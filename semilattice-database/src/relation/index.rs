use std::path::Path;

use binary_set::BinarySet;
use futures::executor::block_on;
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
    pub fn new(root_dir: &Path) -> Self {
        let mut dir = root_dir.to_path_buf();
        dir.push("relation");
        if !dir.exists() {
            std::fs::create_dir_all(&dir).unwrap();
        }
        Self {
            key_names: BinarySet::new({
                let mut path = dir.clone();
                path.push("key_name");
                path
            }),
            fragment: RowFragment::new({
                let mut path = dir.clone();
                path.push("fragment.f");
                path
            }),
            rows: RelationIndexRows {
                key: IdxFile::new({
                    let mut path = dir.clone();
                    path.push("key.i");
                    path
                }),
                depend: IdxFile::new({
                    let mut path = dir.clone();
                    path.push("depend.i");
                    path
                }),
                pend: IdxFile::new({
                    let mut path = dir.clone();
                    path.push("pend.i");
                    path
                }),
            },
        }
    }
    pub fn insert(&mut self, relation_key: &str, depend: CollectionRow, pend: CollectionRow) {
        let key_id = self.key_names.row_or_insert(relation_key.as_bytes());
        if let Some(row) = self.fragment.pop() {
            block_on(async {
                futures::join!(
                    async {
                        self.rows.key.update(row, key_id);
                    },
                    async {
                        self.rows.depend.update(row, depend);
                    },
                    async {
                        self.rows.pend.update(row, pend);
                    },
                )
            });
        } else {
            block_on(async {
                futures::join!(
                    async {
                        self.rows.key.insert(key_id);
                    },
                    async {
                        self.rows.depend.insert(depend);
                    },
                    async {
                        self.rows.pend.insert(pend);
                    }
                );
            });
        }
    }
    pub fn delete(&mut self, row: u32) -> u64 {
        block_on(async {
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
            );
        });
        self.fragment.insert_blank(row)
    }
    pub fn delete_pends_by_collection_row(&mut self, collection_row: &CollectionRow) {
        for row in self
            .rows
            .pend
            .iter_by(|v| v.cmp(collection_row))
            .map(|x| x.row())
            .collect::<Vec<u32>>()
        {
            self.delete(row);
        }
    }
    pub fn pends(&self, key: &Option<String>, depend: &CollectionRow) -> Vec<CollectionRow> {
        if let Some(key) = key {
            if let Some(key) = self.key_names.row(key.as_bytes()) {
                self.rows
                    .depend
                    .iter_by(|v| v.cmp(depend))
                    .map(|x| x.row())
                    .filter_map(|i| {
                        if let (Some(key_row), Some(collection_row)) =
                            (self.rows.key.value(i), self.rows.pend.value(i))
                        {
                            if *key_row == key {
                                return Some(collection_row.clone());
                            }
                        }
                        None
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            self.rows
                .depend
                .iter_by(|v| v.cmp(depend))
                .map(|x| x.row())
                .filter_map(|i| {
                    if let Some(collection_row) = self.rows.pend.value(i) {
                        Some(collection_row.clone())
                    } else {
                        None
                    }
                })
                .collect()
        }
    }
    pub fn depends(&self, key: Option<&str>, pend: &CollectionRow) -> Vec<Depend> {
        if let Some(key_name) = key {
            if let Some(key) = self.key_names.row(key_name.as_bytes()) {
                self.rows
                    .pend
                    .iter_by(|v| v.cmp(pend))
                    .map(|x| x.row())
                    .filter_map(|i| {
                        if let (Some(key_row), Some(collection_row)) =
                            (self.rows.key.value(i), self.rows.depend.value(i))
                        {
                            if *key_row == key {
                                return Some(Depend::new(key_name, collection_row.clone()));
                            }
                        }
                        None
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            self.rows
                .pend
                .iter_by(|v| v.cmp(pend))
                .map(|x| x.row())
                .filter_map(|i| {
                    if let (Some(key), Some(collection_row)) =
                        (self.rows.key.value(i), self.rows.depend.value(i))
                    {
                        Some(Depend::new(
                            unsafe { std::str::from_utf8_unchecked(self.key_names.bytes(*key)) },
                            collection_row.clone(),
                        ))
                    } else {
                        None
                    }
                })
                .collect()
        }
    }
    pub fn index_depend(&self) -> &IdxFile<CollectionRow> {
        &self.rows.depend
    }
    pub fn index_pend(&self) -> &IdxFile<CollectionRow> {
        &self.rows.pend
    }
    pub fn depend(&self, row: u32) -> Option<&CollectionRow> {
        self.rows.depend.value(row)
    }
    pub unsafe fn key(&self, row: u32) -> &str {
        if let Some(key_row) = self.rows.key.value(row) {
            std::str::from_utf8_unchecked(self.key_names.bytes(*key_row))
        } else {
            ""
        }
    }
}
