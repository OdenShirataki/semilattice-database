use idx_binary::IdxBinary;
use std::{io, path::Path};
use versatile_data::{IdxSized, RowFragment};

use crate::{collection::CollectionRow, session::SessionDepend, SessionCollectionRow};

#[derive(Clone, Debug)]
pub struct Depend {
    key: String,
    collection_row: CollectionRow,
}
impl Depend {
    pub fn new(key: impl Into<String>, collection_row: CollectionRow) -> Self {
        Self {
            key: key.into(),
            collection_row,
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn collection_row(&self) -> &CollectionRow {
        &self.collection_row
    }
}

struct RelationIndexRows {
    key: IdxSized<u32>,
    depend: IdxSized<CollectionRow>,
    pend: IdxSized<CollectionRow>,
}
pub struct RelationIndex {
    fragment: RowFragment,
    key_names: IdxBinary,
    rows: RelationIndexRows,
}
impl RelationIndex {
    pub fn new(root_dir: &Path) -> io::Result<Self> {
        let mut dir = root_dir.to_path_buf();
        dir.push("relation");
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(Self {
            key_names: IdxBinary::new({
                let mut path = dir.clone();
                path.push("key_name");
                path
            })?,
            fragment: RowFragment::new({
                let mut path = dir.clone();
                path.push("fragment.f");
                path
            })?,
            rows: RelationIndexRows {
                key: IdxSized::new({
                    let mut path = dir.clone();
                    path.push("key.i");
                    path
                })?,
                depend: IdxSized::new({
                    let mut path = dir.clone();
                    path.push("depend.i");
                    path
                })?,
                pend: IdxSized::new({
                    let mut path = dir.clone();
                    path.push("pend.i");
                    path
                })?,
            },
        })
    }
    pub fn insert(
        &mut self,
        relation_key: &str,
        depend: CollectionRow,
        pend: CollectionRow,
    ) -> io::Result<()> {
        if let Ok(key_id) = self.key_names.entry(relation_key.as_bytes()) {
            if let Some(row) = self.fragment.pop()? {
                self.rows.key.update(row, key_id)?;
                self.rows.depend.update(row, depend)?;
                self.rows.pend.update(row, pend)?;
            } else {
                self.rows.key.insert(key_id)?;
                self.rows.depend.insert(depend)?;
                self.rows.pend.insert(pend)?;
            }
        }
        Ok(())
    }
    pub fn delete(&mut self, row: u32) -> io::Result<u64> {
        self.rows.key.delete(row)?;
        self.rows.depend.delete(row)?;
        self.rows.pend.delete(row)?;
        self.fragment.insert_blank(row)
    }
    pub fn delete_pends_by_collection_row(
        &mut self,
        collection_row: &CollectionRow,
    ) -> io::Result<()> {
        for row in self
            .rows
            .pend
            .triee()
            .iter_by_value(collection_row)
            .map(|x| x.row())
            .collect::<Vec<u32>>()
        {
            self.delete(row)?;
        }
        Ok(())
    }
    pub fn pends(&self, key: Option<&str>, depend: &CollectionRow) -> Vec<CollectionRow> {
        let mut ret: Vec<CollectionRow> = Vec::new();
        if let Some(key) = key {
            if let Some(key) = self.key_names.find_row(key.as_bytes()) {
                for i in self
                    .rows
                    .depend
                    .triee()
                    .iter_by_value(depend)
                    .map(|x| x.row())
                {
                    if let (Some(key_row), Some(collection_row)) =
                        (self.rows.key.value(i), self.rows.pend.value(i))
                    {
                        if *key_row == key {
                            ret.push(*collection_row);
                        }
                    }
                }
            }
        } else {
            for i in self
                .rows
                .depend
                .triee()
                .iter_by_value(depend)
                .map(|x| x.row())
            {
                if let Some(collection_row) = self.rows.pend.value(i) {
                    ret.push(*collection_row);
                }
            }
        }
        ret
    }
    pub fn depends(&self, key: Option<&str>, pend: &CollectionRow) -> Vec<SessionDepend> {
        let mut ret: Vec<SessionDepend> = Vec::new();
        if let Some(key_name) = key {
            if let Some(key) = self.key_names.find_row(key_name.as_bytes()) {
                for i in self.rows.pend.triee().iter_by_value(pend).map(|x| x.row()) {
                    if let (Some(key_row), Some(collection_row)) =
                        (self.rows.key.value(i), self.rows.depend.value(i))
                    {
                        if *key_row == key {
                            ret.push(SessionDepend::new(
                                key_name,
                                SessionCollectionRow::new(
                                    collection_row.collection_id(),
                                    collection_row.row() as i64,
                                ),
                            ));
                        }
                    }
                }
            }
        } else {
            for i in self.rows.pend.triee().iter_by_value(pend).map(|x| x.row()) {
                if let (Some(key), Some(collection_row)) =
                    (self.rows.key.value(i), self.rows.pend.value(i))
                {
                    ret.push(SessionDepend::new(
                        unsafe { std::str::from_utf8_unchecked(self.key_names.bytes(*key)) },
                        SessionCollectionRow::new(
                            collection_row.collection_id(),
                            collection_row.row() as i64,
                        ),
                    ));
                }
            }
        }
        ret
    }
    pub fn index_depend(&self) -> &IdxSized<CollectionRow> {
        &self.rows.depend
    }
    pub fn index_pend(&self) -> &IdxSized<CollectionRow> {
        &self.rows.pend
    }
    pub fn depend(&self, row: u32) -> Option<&CollectionRow> {
        self.rows.depend.value(row)
    }
    pub unsafe fn key(&self, row: u32) -> Result<&str, std::str::Utf8Error> {
        Ok(if let Some(key_row) = self.rows.key.value(row) {
            std::str::from_utf8(self.key_names.bytes(*key_row))?
        } else {
            ""
        })
    }
}
