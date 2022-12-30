use file_mmap::FileMmap;
use idx_binary::IdxBinary;
use std::{
    io,
    path::{Path, PathBuf},
};
use versatile_data::IdxSized;

use crate::{collection::CollectionRow, session::SessionDepend, SessionCollectionRow};

#[derive(Clone)]
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
    fragment: Fragment,
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
            fragment: Fragment::new({
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
            if let Some(row) = self.fragment.pop() {
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
    pub fn delete(&mut self, row: u32) -> io::Result<()> {
        self.rows.key.delete(row);
        self.rows.depend.delete(row);
        self.rows.pend.delete(row);
        self.fragment.insert_blank(row)
    }
    pub fn delete_by_collection_row(&mut self, collection_row: CollectionRow) -> io::Result<()> {
        for i in self.rows.pend.select_by_value(&collection_row) {
            self.delete(i)?;
        }
        Ok(())
    }
    pub fn pends(&self, key: &str, depend: &CollectionRow) -> Vec<CollectionRow> {
        let mut ret: Vec<CollectionRow> = Vec::new();
        if let Some(key) = self.key_names.find_row(key.as_bytes()) {
            let c = self.rows.depend.select_by_value(depend);
            for i in c {
                if let (Some(key_row), Some(collection_row)) =
                    (self.rows.key.value(i), self.rows.pend.value(i))
                {
                    if key_row == key {
                        ret.push(collection_row);
                    }
                }
            }
        }
        ret
    }
    pub fn depends(&self, key: Option<&str>, pend: &CollectionRow) -> Vec<SessionDepend> {
        let mut ret: Vec<SessionDepend> = Vec::new();
        if let Some(key_name) = key {
            if let Some(key) = self.key_names.find_row(key_name.as_bytes()) {
                let c = self.rows.pend.select_by_value(pend);
                for i in c {
                    if let (Some(key_row), Some(collection_row)) =
                        (self.rows.key.value(i), self.rows.depend.value(i))
                    {
                        if key_row == key {
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
            let c = self.rows.pend.select_by_value(pend);
            for i in c {
                if let (Some(key), Some(collection_row)) =
                    (self.rows.key.value(i), self.rows.pend.value(i))
                {
                    ret.push(SessionDepend::new(
                        unsafe { self.key_names.str(key) }.unwrap(),
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
    pub fn depend(&self, row: u32) -> Option<CollectionRow> {
        self.rows.depend.value(row)
    }
    pub unsafe fn key(&self, row: u32) -> Result<&str, std::str::Utf8Error> {
        Ok(if let Some(key_row) = self.rows.key.value(row) {
            self.key_names.str(key_row)?
        } else {
            ""
        })
    }
}

const U32SIZE: usize = std::mem::size_of::<u32>();
struct Fragment {
    filemmap: FileMmap,
    blank_list: Vec<u32>,
    blank_count: u32,
}
impl Fragment {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(U32SIZE as u64)?;
        }
        let blank_list = filemmap.as_ptr() as *mut u32;
        let blank_count = (filemmap.len()? / U32SIZE as u64 - 1) as u32;
        Ok(Self {
            filemmap,
            blank_list: unsafe { Vec::from_raw_parts(blank_list, 1, 0) },
            blank_count,
        })
    }
    pub fn insert_blank(&mut self, id: u32) -> io::Result<()> {
        self.filemmap.append(&[0, 0, 0, 0])?;
        unsafe {
            *(self.blank_list.as_ptr() as *mut u32).offset(self.blank_count as isize) = id;
        }
        self.blank_count += 1;
        Ok(())
    }
    pub fn pop(&mut self) -> Option<u32> {
        if self.blank_count > 0 {
            let p = unsafe {
                (self.blank_list.as_ptr() as *mut u32).offset(self.blank_count as isize - 1)
            };
            let last = unsafe { *p };
            unsafe {
                *p = 0;
            }
            if let Ok(len) = self.filemmap.len() {
                if let Ok(()) = self.filemmap.set_len(len - U32SIZE as u64) {
                    self.blank_count -= 1;
                    return Some(last);
                }
            }
        }
        None
    }
}
