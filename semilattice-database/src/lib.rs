pub mod search;

mod collection;
mod relation;

pub use binary_set::BinarySet;
pub use collection::{Collection, CollectionRow};
pub use relation::{Depend, RelationIndex};
pub use search::{Condition, Search};
pub use versatile_data::{
    anyhow, create_uuid, idx_binary, uuid_string, Activity, DataOption, Field, FileMmap, IdxBinary,
    IdxFile, KeyValue, Operation, Order, OrderKey, Record, RowSet, Term, Uuid,
};

use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use versatile_data::Data;

pub struct Database {
    collections_dir: PathBuf,
    collections_map: HashMap<String, i32>,
    collections: BTreeMap<i32, Collection>,
    relation: Arc<RwLock<RelationIndex>>,
    collection_settings: HashMap<String, DataOption>,
}
impl Database {
    pub fn new(
        dir: PathBuf,
        collection_settings: Option<HashMap<String, DataOption>>,
    ) -> io::Result<Self> {
        let mut collections_dir = dir.to_path_buf();
        collections_dir.push("collection");

        let mut db = Self {
            collections_dir,
            collections: BTreeMap::new(),
            collections_map: HashMap::new(),
            relation: Arc::new(RwLock::new(RelationIndex::new(&dir)?)),
            collection_settings: collection_settings.unwrap_or(HashMap::new()),
        };
        if db.collections_dir.exists() {
            let dir = db.collections_dir.read_dir()?;
            for d in dir.into_iter() {
                let d = d?;
                if d.file_type()?.is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        if let Some(pos) = fname.find("_") {
                            if let Ok(collection_id) = (&fname[..pos]).parse::<i32>() {
                                let name = &fname[(pos + 1)..];
                                db.create_collection(collection_id, name, d.path())?;
                            }
                        }
                    }
                }
            }
        }

        Ok(db)
    }

    pub fn relation(&self) -> Arc<RwLock<RelationIndex>> {
        self.relation.clone()
    }

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

    pub fn register_relation(
        &mut self,
        key_name: &str,
        depend: &CollectionRow,
        pend: CollectionRow,
    ) -> Result<()> {
        self.relation
            .write()
            .unwrap()
            .insert(key_name, depend.clone(), pend)
    }
    pub fn register_relations(
        &mut self,
        depend: &CollectionRow,
        pends: Vec<(String, CollectionRow)>,
    ) -> Result<()> {
        for (key_name, pend) in pends {
            self.register_relation(&key_name, depend, pend)?;
        }
        Ok(())
    }

    pub fn delete_recursive(&mut self, target: &CollectionRow) -> Result<()> {
        let rows = self
            .relation
            .read()
            .unwrap()
            .index_depend()
            .iter_by(|v| v.cmp(&target))
            .map(|x| x.row())
            .collect::<Vec<u32>>();
        for relation_row in rows {
            let collection_row = self
                .relation
                .read()
                .unwrap()
                .index_pend()
                .value(relation_row)
                .and_then(|v| Some(v.clone()));
            if let Some(collection_row) = collection_row {
                self.delete_recursive(&collection_row)?;
            }
        }
        let rows = self
            .relation
            .read()
            .unwrap()
            .index_pend()
            .iter_by(|v| v.cmp(&target))
            .map(|x| x.row())
            .collect::<Vec<u32>>();
        for relation_row in rows {
            self.relation.write().unwrap().delete(relation_row)?;
        }
        if let Some(collection) = self.collection_mut(target.collection_id()) {
            collection.update(&Operation::Delete { row: target.row() })?;
        }
        Ok(())
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

    pub fn search(&self, colletion: &Collection) -> Search {
        Search::new(colletion)
    }
    pub fn result(
        &self,
        search: Search,
        orders: &[Order],
    ) -> Result<Vec<u32>, std::sync::mpsc::SendError<RowSet>> {
        search.result(self, orders)
    }

    pub fn depends(
        &self,
        key: Option<&str>,
        pend_collection_id: i32,
        pend_row: u32,
    ) -> Vec<Depend> {
        let mut r: Vec<Depend> = vec![];
        let depends = self.relation.read().unwrap().depends(
            key,
            &CollectionRow::new(pend_collection_id, pend_row as u32),
        );
        for i in depends {
            r.push(i.into());
        }
        r
    }

    fn create_collection(&mut self, id: i32, name: &str, dir: PathBuf) -> io::Result<()> {
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
