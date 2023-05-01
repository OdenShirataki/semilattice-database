use std::path::Path;

use idx_binary::IdxBinary;
use versatile_data::IdxSized;

use super::SessionCollectionRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDepend {
    key: String,
    collection_row: SessionCollectionRow,
}
impl SessionDepend {
    pub fn new(key: &str, collection_row: SessionCollectionRow) -> Self {
        Self {
            key: key.to_owned(),
            collection_row,
        }
    }
    pub fn collection_row(&self) -> &SessionCollectionRow {
        &self.collection_row
    }
    pub fn collection_id(&self) -> i32 {
        self.collection_row.collection_id()
    }
    pub fn row(&self) -> i64 {
        self.collection_row.row()
    }
    pub fn key(&self) -> &str {
        &self.key
    }
}

pub struct SessionRelationRows {
    pub(crate) key: IdxSized<u32>,
    pub(crate) session_row: IdxSized<u32>,
    pub(crate) depend: IdxSized<SessionCollectionRow>,
}
pub struct SessionRelation {
    pub(crate) key_names: IdxBinary,
    pub(crate) rows: SessionRelationRows,
}
impl SessionRelation {
    pub fn new<P: AsRef<Path>>(session_dir: P) -> std::io::Result<Self> {
        let mut relation_dir = session_dir.as_ref().to_path_buf();
        relation_dir.push("relation");
        if !relation_dir.exists() {
            std::fs::create_dir_all(&relation_dir)?;
        }

        let mut path_key_name = relation_dir.clone();
        path_key_name.push("key_name");

        let mut path_key = relation_dir.clone();
        path_key.push("key.i");

        let mut path_session_row = relation_dir.clone();
        path_session_row.push("session_row.i");

        let mut path_depend = relation_dir.clone();
        path_depend.push("depend.i");

        Ok(Self {
            key_names: IdxBinary::new(path_key_name)?,
            rows: SessionRelationRows {
                key: IdxSized::new(path_key)?,
                session_row: IdxSized::new(path_session_row)?,
                depend: IdxSized::new(path_depend)?,
            },
        })
    }
    pub fn insert(&mut self, relation_key: &str, session_row: u32, depend: SessionCollectionRow) {
        if let Ok(key_id) = self.key_names.entry(relation_key.as_bytes()) {
            self.rows.key.insert(key_id).unwrap();
            self.rows.session_row.insert(session_row).unwrap();
            self.rows.depend.insert(depend).unwrap();
        }
    }
    pub fn from_session_row(
        &mut self,
        session_row: u32,
        new_session_row: u32,
    ) -> std::io::Result<Vec<SessionDepend>> {
        let mut ret = vec![];
        for session_relation_row in self
            .rows
            .session_row
            .triee()
            .iter_by_value(&session_row)
            .map(|x| x.row())
            .collect::<Vec<u32>>()
        {
            if let (Some(key), Some(depend)) = (
                self.rows.key.value(session_relation_row),
                self.rows.depend.value(session_relation_row),
            ) {
                let key = *key;
                let depend = *depend;
                self.rows.key.insert(key)?;
                self.rows.session_row.insert(new_session_row)?;
                self.rows.depend.insert(depend)?;
                if let Ok(key_name) = std::str::from_utf8(unsafe { self.key_names.bytes(key) }) {
                    ret.push(SessionDepend::new(key_name, depend))
                }
            }
        }
        Ok(ret)
    }
    pub fn delete(&mut self, session_row: u32) -> std::io::Result<()> {
        for relation_row in self
            .rows
            .session_row
            .triee()
            .iter_by_value(&session_row)
            .map(|x| x.row())
            .collect::<Vec<u32>>()
        {
            self.rows.session_row.delete(relation_row)?;
            self.rows.key.delete(relation_row)?;
            self.rows.depend.delete(relation_row)?;
        }
        Ok(())
    }
}
