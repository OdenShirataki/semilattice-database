use std::{
    collections::HashMap,
    sync::{
        mpsc::{channel, SendError},
        Arc, RwLock,
    },
};

use versatile_data::{Order, RowSet};

use crate::{CollectionRow, Database, Search};

#[derive(Clone, Debug)]
pub struct SearchResult {
    collection_id: i32,
    rows: RowSet,
    join: HashMap<String, HashMap<u32, Vec<CollectionRow>>>,
}
impl SearchResult {
    pub fn collection_id(&self) -> i32 {
        self.collection_id
    }
    pub fn rows(&self) -> &RowSet {
        &self.rows
    }
    pub fn join(&self) -> &HashMap<String, HashMap<u32, Vec<CollectionRow>>> {
        &self.join
    }

    pub fn sort(&self, database: &Database, orders: &[Order]) -> Vec<u32> {
        if let Some(collection) = database.collection(self.collection_id) {
            if orders.len() > 0 {
                collection.data.sort(&self.rows, &orders)
            } else {
                self.rows.iter().map(|&x| x).collect()
            }
        } else {
            vec![]
        }
    }
}

impl Search {
    pub fn get_result(&self) -> Arc<RwLock<Option<SearchResult>>> {
        Arc::clone(&self.result)
    }

    pub fn result(
        &mut self,
        database: &Database,
    ) -> Result<Arc<RwLock<Option<SearchResult>>>, SendError<RowSet>> {
        if let Some(collection) = database.collection(self.collection_id) {
            let mut rows = RowSet::default();
            if self.conditions.len() > 0 {
                let (tx, rx) = channel();
                for c in &self.conditions {
                    c.result(collection, &database.relation, tx.clone())?;
                }
                drop(tx);
                let mut fst = true;
                for rs in rx {
                    if fst {
                        rows = rs;
                        fst = false;
                    } else {
                        rows = rows.intersection(&rs).map(|&x| x).collect()
                    }
                }
            } else {
                for row in collection.data.all() {
                    rows.insert(row);
                }
            }
            let mut join_result: HashMap<String, HashMap<u32, Vec<CollectionRow>>> = HashMap::new();
            for (name, join) in &self.join {
                join_result.insert(
                    name.clone(),
                    join.result(database, self.collection_id, &rows),
                );
            }
            *self.result.write().unwrap() = Some(SearchResult {
                collection_id: self.collection_id,
                rows,
                join: join_result,
            });
        }
        Ok(Arc::clone(&self.result))
    }
}
