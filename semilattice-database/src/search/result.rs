use std::num::{NonZeroI32, NonZeroU32};

use futures::future;
use hashbrown::HashMap;
use versatile_data::{Order, RowSet};

use crate::{Collection, Condition, Database, RelationIndex, Search};

#[derive(Clone, Debug, PartialEq)]
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
    pub(crate) async fn result_conditions(
        collection: &Collection,
        conditions: &Vec<Condition>,
        relation: &RelationIndex,
    ) -> RowSet {
        let (mut rows, _index, fs) = future::select_all(
            conditions
                .into_iter()
                .map(|c| c.result(collection, relation)),
        )
        .await;
        for r in future::join_all(fs).await.into_iter() {
            rows.retain(|v| r.contains(v));
        }
        rows
    }

    pub async fn result(&self, database: &Database) -> SearchResult {
        if let Some(collection) = database.collection(self.collection_id) {
            let rows = if self.conditions.len() > 0 {
                Self::result_conditions(collection, &self.conditions, &database.relation).await
            } else {
                collection.data.all()
            };

            let join = future::join_all(self.join.iter().map(|(name, join)| async {
                (
                    name.to_owned(),
                    join.result(database, self.collection_id, &rows).await,
                )
            }))
            .await
            .into_iter()
            .collect();

            SearchResult {
                collection_id: self.collection_id,
                rows,
                join,
            }
        } else {
            SearchResult {
                collection_id: self.collection_id,
                rows: Default::default(),
                join: Default::default(),
            }
        }
    }
}
