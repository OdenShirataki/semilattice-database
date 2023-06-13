use std::{io, ops::Deref, path::Path};

use serde::{ser::SerializeStruct, Serialize};
use versatile_data::{anyhow::Result, IdxFile, RowFragment};

use crate::{collection::CollectionRow, BinarySet};

#[derive(Clone, PartialEq, Eq, Debug)]
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
}
impl Deref for Depend {
    type Target = CollectionRow;
    fn deref(&self) -> &Self::Target {
        &self.collection_row
    }
}
impl Serialize for Depend {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Depend", 3)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("collection_id", &self.collection_row.collection_id())?;
        state.serialize_field("row", &self.collection_row.row())?;
        state.end()
    }
}

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
    pub fn new(root_dir: &Path) -> io::Result<Self> {
        let mut dir = root_dir.to_path_buf();
        dir.push("relation");
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(Self {
            key_names: BinarySet::new({
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
                key: IdxFile::new({
                    let mut path = dir.clone();
                    path.push("key.i");
                    path
                })?,
                depend: IdxFile::new({
                    let mut path = dir.clone();
                    path.push("depend.i");
                    path
                })?,
                pend: IdxFile::new({
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
    ) -> Result<()> {
        if let Ok(key_id) = self.key_names.row_or_insert(relation_key.as_bytes()) {
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
            .iter_by(|v| v.cmp(collection_row))
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
            if let Some(key) = self.key_names.row(key.as_bytes()) {
                for i in self.rows.depend.iter_by(|v| v.cmp(depend)).map(|x| x.row()) {
                    if let (Some(key_row), Some(collection_row)) =
                        (self.rows.key.value(i), self.rows.pend.value(i))
                    {
                        if *key_row == key {
                            ret.push(collection_row.clone());
                        }
                    }
                }
            }
        } else {
            for i in self.rows.depend.iter_by(|v| v.cmp(depend)).map(|x| x.row()) {
                if let Some(collection_row) = self.rows.pend.value(i) {
                    ret.push(collection_row.clone());
                }
            }
        }
        ret
    }
    pub fn depends(&self, key: Option<&str>, pend: &CollectionRow) -> Vec<Depend> {
        let mut ret: Vec<Depend> = Vec::new();
        if let Some(key_name) = key {
            if let Some(key) = self.key_names.row(key_name.as_bytes()) {
                for i in self.rows.pend.iter_by(|v| v.cmp(pend)).map(|x| x.row()) {
                    if let (Some(key_row), Some(collection_row)) =
                        (self.rows.key.value(i), self.rows.depend.value(i))
                    {
                        if *key_row == key {
                            ret.push(Depend::new(key_name, collection_row.clone()));
                        }
                    }
                }
            }
        } else {
            for i in self.rows.pend.iter_by(|v| v.cmp(pend)).map(|x| x.row()) {
                if let (Some(key), Some(collection_row)) =
                    (self.rows.key.value(i), self.rows.pend.value(i))
                {
                    ret.push(Depend::new(
                        unsafe { std::str::from_utf8_unchecked(self.key_names.bytes(*key)) },
                        collection_row.clone(),
                    ));
                }
            }
        }
        ret
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
    pub unsafe fn key(&self, row: u32) -> Result<&str, std::str::Utf8Error> {
        Ok(if let Some(key_row) = self.rows.key.value(row) {
            std::str::from_utf8(self.key_names.bytes(*key_row))?
        } else {
            ""
        })
    }
}
