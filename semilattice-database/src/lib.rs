#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub mod search;

mod collection;
mod relation;

pub use collection::{Collection, CollectionRow};
pub use relation::{Depend, RelationIndex};
pub use search::{Condition, Search, SearchJoin, SearchResult};
use versatile_data::idx_binary::AvltrieeSearch;
pub use versatile_data::{
    Activity, CustomOrderKey, CustomSort, DataOption, Field, FieldName, Fields, FileMmap, IdxFile,
    Order, OrderKey, RowSet, Term, Uuid, create_uuid, idx_binary, uuid_string,
};

use std::{collections::BTreeMap, num::NonZeroI32, path::PathBuf};

use hashbrown::HashMap;

pub struct Database {
    collections_dir: PathBuf,
    collections_map: HashMap<String, NonZeroI32>,
    collections: BTreeMap<NonZeroI32, Collection>,
    relation: RelationIndex,
    collection_settings: std::collections::HashMap<String, DataOption>,
}
impl Database {
    pub fn new(
        dir: PathBuf,
        collection_settings: Option<std::collections::HashMap<String, DataOption>>,
        relation_allocation_lot: u32,
    ) -> Self {
        let mut collections_dir = dir.to_path_buf();
        collections_dir.push("collection");

        let mut db = Self {
            collections_dir,
            collections: BTreeMap::new(),
            collections_map: HashMap::new(),
            relation: RelationIndex::new(&dir, relation_allocation_lot),
            collection_settings: collection_settings.unwrap_or_default(),
        };
        if db.collections_dir.exists() {
            let dir = db.collections_dir.read_dir().unwrap();
            for d in dir.into_iter() {
                let d = d.unwrap();
                if d.file_type().unwrap().is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        if let Some(pos) = fname.find("_") {
                            if let Ok(collection_id) = fname[..pos].parse::<NonZeroI32>() {
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

    /// Deletes the row and all rows that depend on it, recursively.
    ///
    /// Rows to delete are collected first, visiting each row only once, so
    /// this terminates even if the relations form a cycle.
    pub async fn delete(&mut self, target: &CollectionRow) {
        let mut targets = vec![];
        let mut visited = hashbrown::HashSet::new();
        let mut stack = vec![target.clone()];
        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            for relation_row in self.relation.index_depend().iter_by(&current) {
                if let Some(pend) = self.relation.index_pend().value(relation_row) {
                    stack.push(pend.clone());
                }
            }
            targets.push(current);
        }

        for target in targets {
            for relation_row in self
                .relation
                .index_pend()
                .iter_by(&target)
                .collect::<Vec<_>>()
            {
                self.relation.delete(relation_row).await;
            }
            if let Some(collection) = self.collection_mut(target.collection_id()) {
                collection.delete(target.row()).await;
            }
        }
    }
}
