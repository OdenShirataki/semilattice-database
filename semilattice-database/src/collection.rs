mod row;

pub use row::CollectionRow;

use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use versatile_data::{Data, DataOption, Operation};

use crate::Database;

pub struct Collection {
    pub(crate) data: Data,
    id: i32,
    name: String,
}
impl Collection {
    #[inline(always)]
    pub fn new(data: Data, id: i32, name: impl Into<String>) -> Self {
        Self {
            data,
            id,
            name: name.into(),
        }
    }

    #[inline(always)]
    pub fn id(&self) -> i32 {
        self.id
    }

    #[inline(always)]
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
    #[inline(always)]
    pub fn collections(&self) -> Vec<String> {
        self.collections
            .iter()
            .map(|(_, x)| x.name().to_owned())
            .collect()
    }

    #[inline(always)]
    pub fn collection(&self, id: i32) -> Option<&Collection> {
        self.collections.get(&id)
    }

    #[inline(always)]
    pub fn collection_mut(&mut self, id: i32) -> Option<&mut Collection> {
        self.collections.get_mut(&id)
    }

    #[inline(always)]
    pub fn collection_id(&self, name: &str) -> Option<i32> {
        self.collections_map
            .contains_key(name)
            .then(|| *self.collections_map.get(name).unwrap())
    }

    #[inline(always)]
    pub fn collection_id_or_create(&mut self, name: &str) -> i32 {
        if self.collections_map.contains_key(name) {
            *self.collections_map.get(name).unwrap()
        } else {
            self.collection_by_name_or_create(name)
        }
    }

    pub fn delete_collection(&mut self, name: &str) {
        let collection_id = self.collections_map.get(name).map_or(0, |x| *x);
        if collection_id > 0 {
            if let Some(collection) = self.collections.get(&collection_id) {
                collection.data.all().iter().for_each(|row| {
                    self.delete_recursive(&CollectionRow::new(collection_id, *row));
                    if let Some(collection) = self.collection_mut(collection_id) {
                        collection.update(&Operation::Delete { row: *row });
                    }
                });
            }
            self.collections_map.remove(name);
            self.collections.remove(&collection_id);

            let mut dir = self.collections_dir.clone();
            dir.push(collection_id.to_string() + "_" + name);
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    #[inline(always)]
    pub(super) fn create_collection(&mut self, id: i32, name: &str, dir: PathBuf) {
        let collection = Collection::new(
            Data::new(
                dir,
                self.collection_settings
                    .get(name)
                    .map_or(DataOption::default(), |f| f.clone()),
            ),
            id,
            name,
        );
        self.collections_map.insert(name.to_string(), id);
        self.collections.insert(id, collection);
    }

    #[inline(always)]
    fn collection_by_name_or_create(&mut self, name: &str) -> i32 {
        let mut max_id = 0;
        if self.collections_dir.exists() {
            for d in self.collections_dir.read_dir().unwrap() {
                let d = d.unwrap();
                if d.file_type().unwrap().is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        let s: Vec<&str> = fname.split("_").collect();
                        if s.len() > 1 {
                            if let Ok(i) = s[0].parse() {
                                max_id = std::cmp::max(max_id, i);
                            }
                            if s[1] == name {
                                self.create_collection(max_id, name, d.path());
                                return max_id;
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
        });
        collection_id
    }
}
