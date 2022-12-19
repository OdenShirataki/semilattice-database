use file_mmap::FileMmap;
use idx_binary::IdxBinary;
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
    pub fn new(root_dir: &str) -> Result<Self, std::io::Error> {
        let dir = root_dir.to_string() + "/relation/";
        if !std::path::Path::new(&dir).exists() {
            std::fs::create_dir_all(&dir).unwrap();
        }
        Ok(Self {
            key_names: IdxBinary::new(&(dir.to_string() + "/key_name"))?,
            fragment: Fragment::new(&(dir.to_string() + "/fragment.f"))?,
            rows: RelationIndexRows {
                key: IdxSized::new(&(dir.to_string() + "/key.i"))?,
                depend: IdxSized::new(&(dir.to_string() + "/depend.i"))?,
                pend: IdxSized::new(&(dir.to_string() + "/pend.i"))?,
            },
        })
    }
    pub fn insert(
        &mut self,
        relation_key: &str,
        depend: CollectionRow,
        pend: CollectionRow,
    ) -> Result<(), std::io::Error> {
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
    pub fn delete(&mut self, row: u32) {
        self.rows.key.delete(row);
        self.rows.depend.delete(row);
        self.rows.pend.delete(row);
        self.fragment.insert_blank(row);
    }
    pub fn delete_by_collection_row(&mut self, collection_row: CollectionRow) {
        for i in self.rows.pend.select_by_value(&collection_row) {
            self.delete(i);
        }
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
                        unsafe { self.key_names.str(key) },
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
    pub unsafe fn key(&self, row: u32) -> &str {
        if let Some(key_row) = self.rows.key.value(row) {
            self.key_names.str(key_row)
        } else {
            ""
        }
    }
}

const U32SIZE: usize = std::mem::size_of::<u32>();
struct Fragment {
    filemmap: FileMmap,
    blank_list: Vec<u32>,
    blank_count: u32,
}
impl Fragment {
    pub fn new(path: &str) -> Result<Self, std::io::Error> {
        let filemmap = FileMmap::new(path, U32SIZE as u64)?;
        let blank_list = filemmap.as_ptr() as *mut u32;
        let blank_count: u32 = (filemmap.len() / U32SIZE as u64 - 1) as u32;
        Ok(Self {
            filemmap,
            blank_list: unsafe { Vec::from_raw_parts(blank_list, 1, 0) },
            blank_count,
        })
    }
    pub fn insert_blank(&mut self, id: u32) {
        self.filemmap.append(&[0, 0, 0, 0]).unwrap();
        unsafe {
            *(self.blank_list.as_ptr() as *mut u32).offset(self.blank_count as isize) = id;
        }
        self.blank_count += 1;
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
            let _ = self.filemmap.set_len(self.filemmap.len() - U32SIZE as u64);
            self.blank_count -= 1;
            Some(last)
        } else {
            None
        }
    }
}
