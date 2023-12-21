use std::{
    num::{NonZeroI32, NonZeroU32},
    path::Path,
};

use binary_set::BinarySet;
use versatile_data::{IdxFile, RowFragment};

use crate::{CollectionRow, Depend};

struct RelationIndexRows {
    key: IdxFile<u32>,
    depend: IdxFile<CollectionRow>,
    pend: IdxFile<CollectionRow>,
}
pub struct RelationIndex {
    fragment: RowFragment,
    key_names: BinarySet,
    rows: RelationIndexRows,
}
impl RelationIndex {
    pub fn new(root_dir: &Path, allocation_lot: u32) -> Self {
        let mut dir = root_dir.to_path_buf();
        dir.push("relation");
        if !dir.exists() {
            std::fs::create_dir_all(&dir).unwrap();
        }
        Self {
            key_names: BinarySet::new(
                {
                    let mut path = dir.clone();
                    path.push("key_name");
                    path
                },
                1,
            ),
            fragment: RowFragment::new({
                let mut path = dir.clone();
                path.push("fragment.f");
                path
            }),
            rows: RelationIndexRows {
                key: IdxFile::new(
                    {
                        let mut path = dir.clone();
                        path.push("key.i");
                        path
                    },
                    allocation_lot,
                ),
                depend: IdxFile::new(
                    {
                        let mut path = dir.clone();
                        path.push("depend.i");
                        path
                    },
                    allocation_lot,
                ),
                pend: IdxFile::new(
                    {
                        let mut path = dir.clone();
                        path.push("pend.i");
                        path
                    },
                    allocation_lot,
                ),
            },
        }
    }

    pub async fn insert(&mut self, relation_key: &str, depend: CollectionRow, pend: CollectionRow) {
        let key_id = self.key_names.row_or_insert(relation_key.as_bytes()).get();
        if let Some(row) = self.fragment.pop() {
            futures::join!(
                self.rows.key.update_with_allocate(row, key_id),
                self.rows.depend.update_with_allocate(row, depend),
                self.rows.pend.update_with_allocate(row, pend)
            );
        } else {
            futures::join!(
                self.rows.key.insert(key_id),
                self.rows.depend.insert(depend),
                self.rows.pend.insert(pend)
            );
        }
    }

    pub async fn delete(&mut self, row: NonZeroU32) {
        futures::join!(
            async {
                self.rows.key.delete(row);
            },
            async {
                self.rows.depend.delete(row);
            },
            async {
                self.rows.pend.delete(row);
            },
            async {
                self.fragment.insert_blank(row);
            },
        );
    }

    pub async fn delete_pends_by_collection_row(&mut self, collection_row: &CollectionRow) {
        for row in self
            .rows
            .pend
            .iter_by(|v| v.cmp(collection_row))
            .collect::<Vec<_>>()
            .into_iter()
        {
            self.delete(row).await;
        }
    }

    pub fn pends(
        &self,
        key: Option<&str>,
        depend: &CollectionRow,
        pend_collection_id: Option<NonZeroI32>,
    ) -> Vec<&CollectionRow> {
        if let Some(pend_collection_id) = pend_collection_id {
            key.as_ref().map_or_else(
                || {
                    self.rows
                        .depend
                        .iter_by(|v| v.cmp(depend))
                        .filter_map(|row| {
                            if let Some(v) = self.rows.pend.value(row) {
                                if v.collection_id() == pend_collection_id {
                                    return Some(v);
                                }
                            }
                            None
                        })
                        .collect()
                },
                |key| {
                    self.key_names.row(key.as_bytes()).map_or(vec![], |key| {
                        self.rows
                            .depend
                            .iter_by(|v| v.cmp(depend))
                            .filter_map(|row| {
                                if let (Some(key_row), Some(collection_row)) =
                                    (self.rows.key.value(row), self.rows.pend.value(row))
                                {
                                    if *key_row == key.get() {
                                        if collection_row.collection_id() == pend_collection_id {
                                            return Some(collection_row);
                                        }
                                    }
                                }
                                None
                            })
                            .collect()
                    })
                },
            )
        } else {
            key.as_ref().map_or_else(
                || {
                    self.rows
                        .depend
                        .iter_by(|v| v.cmp(depend))
                        .filter_map(|row| self.rows.pend.value(row))
                        .collect()
                },
                |key| {
                    self.key_names.row(key.as_bytes()).map_or(vec![], |key| {
                        self.rows
                            .depend
                            .iter_by(|v| v.cmp(depend))
                            .filter_map(|row| {
                                if let (Some(key_row), Some(collection_row)) =
                                    (self.rows.key.value(row), self.rows.pend.value(row))
                                {
                                    (*key_row == key.get()).then_some(collection_row)
                                } else {
                                    None
                                }
                            })
                            .collect()
                    })
                },
            )
        }
    }

    pub fn depends(&self, key: Option<&str>, pend: &CollectionRow) -> Vec<Depend> {
        key.map_or_else(
            || {
                self.rows
                    .pend
                    .iter_by(|v| v.cmp(pend))
                    .filter_map(|row| {
                        if let (Some(key), Some(collection_row)) =
                            (self.rows.key.value(row), self.rows.depend.value(row))
                        {
                            Some(Depend::new(
                                unsafe {
                                    std::str::from_utf8_unchecked(
                                        self.key_names.bytes(NonZeroU32::new(*key).unwrap()),
                                    )
                                },
                                collection_row.clone(),
                            ))
                        } else {
                            None
                        }
                    })
                    .collect()
            },
            |key_name| {
                self.key_names
                    .row(key_name.as_bytes())
                    .map_or(vec![], |key| {
                        self.rows
                            .pend
                            .iter_by(|v| v.cmp(pend))
                            .filter_map(|row| {
                                if let (Some(key_row), Some(collection_row)) =
                                    (self.rows.key.value(row), self.rows.depend.value(row))
                                {
                                    (*key_row == key.get())
                                        .then_some(Depend::new(key_name, collection_row.clone()))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    })
            },
        )
    }

    pub fn index_depend(&self) -> &IdxFile<CollectionRow> {
        &self.rows.depend
    }

    pub fn index_pend(&self) -> &IdxFile<CollectionRow> {
        &self.rows.pend
    }

    pub fn depend(&self, row: NonZeroU32) -> Option<&CollectionRow> {
        self.rows.depend.value(row)
    }

    pub unsafe fn key(&self, row: NonZeroU32) -> &str {
        self.rows.key.value(row).map_or("", |key_row| {
            std::str::from_utf8_unchecked(self.key_names.bytes(NonZeroU32::new(*key_row).unwrap()))
        })
    }
}
