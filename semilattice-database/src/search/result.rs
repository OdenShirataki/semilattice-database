use std::{
    num::NonZeroU32,
    sync::{Arc, RwLock},
};

use futures::{executor::block_on, future};
use hashbrown::HashMap;
use versatile_data::{Order, RowSet};

use crate::{Collection, Condition, Database, RelationIndex, Search};

#[derive(Clone, Debug)]
pub struct SearchResult {
    collection_id: i32,
    rows: RowSet,
    join: HashMap<String, HashMap<NonZeroU32, SearchResult>>,
}
impl SearchResult {
    #[inline(always)]
    pub fn new(
        collection_id: i32,
        rows: RowSet,
        join: HashMap<String, HashMap<NonZeroU32, SearchResult>>,
    ) -> Self {
        Self {
            collection_id,
            rows,
            join,
        }
    }

    #[inline(always)]
    pub fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[inline(always)]
    pub fn rows(&self) -> &RowSet {
        &self.rows
    }

    #[inline(always)]
    pub fn join(&self) -> &HashMap<String, HashMap<NonZeroU32, SearchResult>> {
        &self.join
    }

    #[inline(always)]
    pub fn sort(&self, database: &Database, orders: &[Order]) -> Vec<NonZeroU32> {
        database.collection(self.collection_id).map_or(vec![], |c| {
            if orders.len() > 0 {
                c.data.sort(&self.rows, &orders)
            } else {
                self.rows.iter().cloned().collect()
            }
        })
    }
}

impl Search {
    #[inline(always)]
    pub fn get_result(&self) -> Arc<RwLock<Option<SearchResult>>> {
        Arc::clone(&self.result)
    }

    #[inline(always)]
    pub(crate) async fn result_conditions(
        collection: &Collection,
        conditions: &Vec<Condition>,
        relation: &Arc<RwLock<RelationIndex>>,
    ) -> RowSet {
        let mut fs = conditions
            .iter()
            .map(|c| c.result(collection, relation))
            .collect();
        let (ret, _index, remaining) = future::select_all(fs).await;
        let mut rows = ret;
        fs = remaining;
        while !fs.is_empty() {
            let (ret, _index, remaining) = future::select_all(fs).await;
            rows = rows.intersection(&ret).cloned().collect();
            fs = remaining;
        }
        rows
    }

    #[inline(always)]
    pub fn result(&mut self, database: &Database) -> Arc<RwLock<Option<SearchResult>>> {
        if let Some(collection) = database.collection(self.collection_id) {
            let rows = if self.conditions.len() > 0 {
                block_on(Self::result_conditions(
                    collection,
                    &self.conditions,
                    &database.relation,
                ))
            } else {
                collection.data.all()
            };

            let join_result = block_on(async {
                let mut join_result = HashMap::new();
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
