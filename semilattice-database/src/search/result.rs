use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use futures::{executor::block_on, future};
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

    pub fn result(&mut self, database: &Database) -> Arc<RwLock<Option<SearchResult>>> {
        if let Some(collection) = database.collection(self.collection_id) {
            let rows = if self.conditions.len() > 0 {
                block_on(async {
                    let mut fs = self
                        .conditions
                        .iter()
                        .map(|c| c.result(collection, &database.relation))
                        .collect();
                    let (ret, _index, remaining) = future::select_all(fs).await;
                    let mut rows = ret;
                    fs = remaining;
                    while !fs.is_empty() {
                        let (ret, _index, remaining) = future::select_all(fs).await;
                        rows = rows.intersection(&ret).map(|&x| x).collect();
                        fs = remaining;
                    }
                    rows
                })
            } else {
                collection.data.all()
            };

            let join_result = block_on(async {
                let mut join_result: HashMap<String, HashMap<u32, Vec<CollectionRow>>> =
                    HashMap::new();
                let mut fs: Vec<_> = self
                    .join
                    .iter()
                    .map(|(name, join)| {
                        Box::pin(async {
                            (
                                name.to_owned(),
                                join.result(database, self.collection_id, &rows),
                            )
                        })
                    })
                    .collect();
                while !fs.is_empty() {
                    let (ret, _index, remaining) = future::select_all(fs).await;
                    join_result.insert(ret.0, ret.1.await);
                    fs = remaining;
                }
                join_result
            });
            *self.result.write().unwrap() = Some(SearchResult {
                collection_id: self.collection_id,
                rows,
                join: join_result,
            });
        }
        Arc::clone(&self.result)
    }
}
