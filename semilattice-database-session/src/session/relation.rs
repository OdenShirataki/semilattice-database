use std::{num::NonZeroU32, ops::Deref, path::Path};

use crate::{BinarySet, IdxFile};

use super::{CollectionRow, Depend};

pub struct SessionRelationRows {
    pub(crate) key: IdxFile<u32>,
    pub(crate) session_row: IdxFile<u32>,
    pub(crate) depend: IdxFile<CollectionRow>,
}
pub struct SessionRelation {
    pub(crate) key_names: BinarySet,
    pub(crate) rows: SessionRelationRows,
}
impl SessionRelation {
    pub fn new<P: AsRef<Path>>(session_dir: P, relation_allocation_lot: u32) -> Self {
        let mut relation_dir = session_dir.as_ref().to_path_buf();
        relation_dir.push("relation");
        if !relation_dir.exists() {
            std::fs::create_dir_all(&relation_dir).unwrap();
        }

        let mut path_key_name = relation_dir.clone();
        path_key_name.push("key_name");

        let mut path_key = relation_dir.clone();
        path_key.push("key.i");

        let mut path_session_row = relation_dir.clone();
        path_session_row.push("session_row.i");

        let mut path_depend = relation_dir.clone();
        path_depend.push("depend.i");

        Self {
            key_names: BinarySet::new(path_key_name, 1),
            rows: SessionRelationRows {
                key: IdxFile::new(path_key, relation_allocation_lot),
                session_row: IdxFile::new(path_session_row, relation_allocation_lot),
                depend: IdxFile::new(path_depend, relation_allocation_lot),
            },
        }
    }

    pub(crate) async fn insert(
        &mut self,
        relation_key: &str,
        session_row: NonZeroU32,
        depend: CollectionRow,
    ) {
        futures::join!(
            self.rows
                .key
                .insert(self.key_names.row_or_insert(relation_key.as_bytes()).get()),
            self.rows.session_row.insert(session_row.get()),
            self.rows.depend.insert(depend)
        );
    }

    pub(crate) async fn from_session_row(
        &mut self,
        session_row: NonZeroU32,
        new_session_row: NonZeroU32,
    ) -> Vec<Depend> {
        let mut ret = vec![];
        for session_relation_row in self
            .rows
            .session_row
            .iter_by(|v| v.cmp(&session_row.get()))
            .collect::<Vec<_>>()
            .into_iter()
        {
            if let (Some(key), Some(depend)) = (
                self.rows.key.get(session_relation_row),
                self.rows.depend.get(session_relation_row),
            ) {
                let key = *key.deref();
                let depend = depend.deref().clone();
                futures::join!(
                    self.rows.key.insert(key),
                    self.rows.session_row.insert(new_session_row.get()),
                    self.rows.depend.insert(depend.clone()),
                    async {
                        ret.push(Depend::new(
                            unsafe {
                                std::str::from_utf8_unchecked(
                                    self.key_names.bytes(NonZeroU32::new(key).unwrap()).unwrap(),
                                )
                            },
                            depend,
                        ));
                    }
                );
            }
        }
        ret
    }

    #[inline(always)]
    pub(crate) async fn delete(&mut self, session_row: NonZeroU32) {
        for relation_row in self
            .rows
            .session_row
            .iter_by(|v| v.cmp(&session_row.get()))
            .collect::<Vec<_>>()
            .into_iter()
        {
            futures::join!(
                async {
                    self.rows.session_row.delete(relation_row);
                },
                async {
                    self.rows.key.delete(relation_row);
                },
                async {
                    self.rows.depend.delete(relation_row);
                },
            );
        }
    }
}
