use std::{
    num::{NonZeroI32, NonZeroU32},
    sync::Arc,
};

use futures::future;
use hashbrown::HashMap;
use parking_lot::RwLock;
use versatile_data::{Order, RowSet};

use crate::{Collection, Condition, Database, RelationIndex, Search};

#[derive(Clone, Debug)]
pub struct SearchResult {
    collection_id: NonZeroI32,
    rows: RowSet,
    join: HashMap<String, HashMap<NonZeroU32, SearchResult>>,
}
impl SearchResult {
    #[inline(always)]
    pub fn new(
        collection_id: NonZeroI32,
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
    pub fn collection_id(&self) -> NonZeroI32 {
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
        if let Some(collection) = database.collection(self.collection_id) {
            if orders.len() > 0 {
                collection.data.sort(&self.rows, &orders)
            } else {
                self.rows.iter().cloned().collect()
            }
        } else {
            vec![]
        }
    }
}

impl Search {
    #[inline(always)]
    pub fn get_result(&self) -> &Arc<RwLock<Option<SearchResult>>> {
        &self.result
    }

    pub(crate) async fn result_conditions(
        collection: &Collection,
        conditions: &Vec<Condition>,
        relation: &RelationIndex,
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

    pub async fn result(&mut self, database: &Database) -> Arc<RwLock<Option<SearchResult>>> {
        if let Some(collection) = database.collection(self.collection_id) {
            let rows = if self.conditions.len() > 0 {
                Self::result_conditions(collection, &self.conditions, &database.relation).await
            } else {
                collection.data.all()
            };

            let mut join_result = HashMap::new();
            {
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
            }
            *self.result.write() = Some(SearchResult {
                collection_id: self.collection_id,
                rows,
                join: join_result,
            });
        }
        Arc::clone(&self.result)
    }
}
