pub mod search;

mod collection;
mod relation;

pub use binary_set::BinarySet;
pub use collection::{Collection, CollectionRow};
pub use relation::{Depend, RelationIndex};
pub use search::{Condition, Join, JoinCondition, Search};
pub use versatile_data::{
    create_uuid, idx_binary, uuid_string, Activity, CustomSort, DataOption, Field, FileMmap,
    IdxBinary, IdxFile, KeyValue, Operation, Order, OrderKey, Record, RowSet, Term, Uuid,
};

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use hashbrown::HashMap;

pub struct Database {
    collections_dir: PathBuf,
    collections_map: HashMap<String, i32>,
    collections: BTreeMap<i32, Collection>,
    relation: Arc<RwLock<RelationIndex>>,
    collection_settings: std::collections::HashMap<String, DataOption>,
}
impl Database {
    pub fn new(
        dir: PathBuf,
        collection_settings: Option<std::collections::HashMap<String, DataOption>>,
    ) -> Self {
        let mut collections_dir = dir.to_path_buf();
        collections_dir.push("collection");

        let mut db = Self {
            collections_dir,
            collections: BTreeMap::new(),
            collections_map: HashMap::new(),
            relation: Arc::new(RwLock::new(RelationIndex::new(&dir))),
            collection_settings: collection_settings.unwrap_or(std::collections::HashMap::new()),
        };
        if db.collections_dir.exists() {
            let dir = db.collections_dir.read_dir().unwrap();
            for d in dir.into_iter() {
                let d = d.unwrap();
                if d.file_type().unwrap().is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        if let Some(pos) = fname.find("_") {
                            if let Ok(collection_id) = (&fname[..pos]).parse::<i32>() {
                                let name = &fname[(pos + 1)..];
                                db.create_collection(collection_id, name, d.path());
                            }
                        }
                    }
                }
            }
        }
        db
    }

    pub fn delete_recursive(&mut self, target: &CollectionRow) {
        let rows = self
            .relation
            .read()
            .unwrap()
            .index_depend()
            .iter_by(|v| v.cmp(target))
            .collect::<Vec<_>>();
        for relation_row in rows {
            let collection_row = self
                .relation
                .read()
                .unwrap()
                .index_pend()
                .value(relation_row.get())
                .cloned();
            if let Some(collection_row) = collection_row {
                self.delete_recursive(&collection_row);
            }
        }
        let rows = self
            .relation
            .read()
            .unwrap()
            .index_pend()
            .iter_by(|v| v.cmp(target))
            .collect::<Vec<_>>();
        for relation_row in rows {
            self.relation.write().unwrap().delete(relation_row.get());
        }
        if let Some(collection) = self.collection_mut(target.collection_id()) {
            collection.update(&Operation::Delete {
                row: target.row().get(),
            });
        }
    }
}
