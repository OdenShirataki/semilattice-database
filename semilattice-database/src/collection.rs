mod row;

pub use row::CollectionRow;

use std::{
    io,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use versatile_data::{anyhow::Result, Data, DataOption, Operation};

use crate::Database;

pub struct Collection {
    pub(crate) data: Data,
    id: i32,
    name: String,
}
impl Collection {
    pub fn new(data: Data, id: i32, name: impl Into<String>) -> Self {
        Self {
            data,
            id,
            name: name.into(),
        }
    }
    pub fn id(&self) -> i32 {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}
impl Deref for Collection {
    type Target = Data;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl DerefMut for Collection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl Database {
    pub fn collections(&self) -> Vec<String> {
        let mut r = Vec::new();
        for (_, collection) in self.collections.iter() {
            r.push(collection.name().to_owned());
        }
        r
    }

    pub fn collection(&self, id: i32) -> Option<&Collection> {
        self.collections.get(&id)
    }
    pub fn collection_mut(&mut self, id: i32) -> Option<&mut Collection> {
        self.collections.get_mut(&id)
    }
    pub fn collection_id(&self, name: &str) -> Option<i32> {
        if self.collections_map.contains_key(name) {
            Some(*self.collections_map.get(name).unwrap())
        } else {
            None
        }
    }
    pub fn collection_id_or_create(&mut self, name: &str) -> io::Result<i32> {
        if self.collections_map.contains_key(name) {
            Ok(*self.collections_map.get(name).unwrap())
        } else {
            self.collection_by_name_or_create(name)
        }
    }

    pub fn delete_collection(&mut self, name: &str) -> Result<()> {
        let collection_id = if let Some(collection_id) = self.collections_map.get(name) {
            *collection_id
        } else {
            0
        };
        if collection_id > 0 {
            let rows = {
                let mut rows = Default::default();
                if let Some(collection) = self.collections.get(&collection_id) {
                    rows = collection.data.all();
                }
                rows
            };
            for row in rows {
                self.delete_recursive(&CollectionRow::new(collection_id, row))?;
                if let Some(collection) = self.collection_mut(collection_id) {
                    collection.update(&Operation::Delete { row })?;
                }
            }
            self.collections_map.remove(name);
            self.collections.remove(&collection_id);

            let mut dir = self.collections_dir.clone();
            dir.push(collection_id.to_string() + "_" + name);
            std::fs::remove_dir_all(&dir)?;
        }

        Ok(())
    }

    pub(super) fn create_collection(
        &mut self,
        id: i32,
        name: &str,
        dir: PathBuf,
    ) -> io::Result<()> {
        let collection = Collection::new(
            Data::new(
                dir,
                if let Some(option) = self.collection_settings.get(name) {
                    option.clone()
                } else {
                    DataOption::default()
                },
            )?,
            id,
            name,
        );
        self.collections_map.insert(name.to_string(), id);
        self.collections.insert(id, collection);
        Ok(())
    }
    fn collection_by_name_or_create(&mut self, name: &str) -> io::Result<i32> {
        let mut max_id = 0;
        if self.collections_dir.exists() {
            for d in self.collections_dir.read_dir()? {
                let d = d?;
                if d.file_type()?.is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        let s: Vec<&str> = fname.split("_").collect();
                        if s.len() > 1 {
                            if let Ok(i) = s[0].parse() {
                                max_id = std::cmp::max(max_id, i);
                            }
                            if s[1] == name {
                                self.create_collection(max_id, name, d.path())?;
                                return Ok(max_id);
                            }
                        }
                    }
                }
            }
        }
        let collection_id = max_id + 1;
        self.create_collection(collection_id, name, {
            let mut collecion_dir = self.collections_dir.clone();
            collecion_dir.push(&(collection_id.to_string() + "_" + name));
            collecion_dir
        })?;
        Ok(collection_id)
    }
}
