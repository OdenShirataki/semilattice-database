use std::{num::NonZeroU32, path::Path};

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

    #[inline(always)]
    pub fn insert(&mut self, relation_key: &str, depend: CollectionRow, pend: CollectionRow) {
        let key_id = self.key_names.row_or_insert(relation_key.as_bytes()).get();
        block_on(async {
            if let Some(row) = self.fragment.pop() {
                let row = row.get();
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
            } else {
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
                )
            }
        });
    }

    #[inline(always)]
    pub fn delete(&mut self, row: u32) {
        assert!(row > 0);
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
                async {
                    self.fragment.insert_blank(NonZeroU32::new(row).unwrap());
                },
            )
        });
    }

    #[inline(always)]
    pub fn delete_pends_by_collection_row(&mut self, collection_row: &CollectionRow) {
        for row in self
            .rows
            .pend
            .iter_by(|v| v.cmp(collection_row))
            .collect::<Vec<_>>()
        {
            self.delete(row.get());
        }
    }

    #[inline(always)]
    pub fn pends(&self, key: &Option<String>, depend: &CollectionRow) -> Vec<CollectionRow> {
        key.as_ref().map_or_else(
            || {
                self.rows
                    .depend
                    .iter_by(|v| v.cmp(depend))
                    .filter_map(|row| self.rows.pend.value(row.get()).cloned())
                    .collect()
            },
            |key| {
                self.key_names.row(key.as_bytes()).map_or(vec![], |key| {
                    self.rows
                        .depend
                        .iter_by(|v| v.cmp(depend))
                        .filter_map(|row| {
                            if let (Some(key_row), Some(collection_row)) = (
                                self.rows.key.value(row.get()),
                                self.rows.pend.value(row.get()),
                            ) {
                                (*key_row == key.get()).then_some(collection_row.clone())
                            } else {
                                None
                            }
                        })
                        .collect()
                })
            },
        )
    }

    #[inline(always)]
    pub fn depends(&self, key: Option<&str>, pend: &CollectionRow) -> Vec<Depend> {
        key.map_or_else(
            || {
                self.rows
                    .pend
                    .iter_by(|v| v.cmp(pend))
                    .filter_map(|row| {
                        if let (Some(key), Some(collection_row)) = (
                            self.rows.key.value(row.get()),
                            self.rows.depend.value(row.get()),
                        ) {
                            Some(Depend::new(
                                unsafe {
                                    std::str::from_utf8_unchecked(self.key_names.bytes(*key))
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
                                if let (Some(key_row), Some(collection_row)) = (
                                    self.rows.key.value(row.get()),
                                    self.rows.depend.value(row.get()),
                                ) {
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

    #[inline(always)]
    pub fn index_depend(&self) -> &IdxFile<CollectionRow> {
        &self.rows.depend
    }

    #[inline(always)]
    pub fn index_pend(&self) -> &IdxFile<CollectionRow> {
        &self.rows.pend
    }

    #[inline(always)]
    pub fn depend(&self, row: u32) -> Option<&CollectionRow> {
        self.rows.depend.value(row)
    }

    #[inline(always)]
    pub unsafe fn key(&self, row: u32) -> &str {
        self.rows.key.value(row).map_or("", |key_row| {
            std::str::from_utf8_unchecked(self.key_names.bytes(*key_row))
        })
    }
}
