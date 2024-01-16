use std::num::NonZeroU32;

use futures::future;
use hashbrown::HashMap;
use versatile_data::{Order, RowSet};

use crate::{Collection, Condition, Database, RelationIndex, Search};

#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult {
    search: Option<Search>,
    rows: RowSet,
    join: HashMap<String, HashMap<NonZeroU32, SearchResult>>,
}

impl SearchResult {
    pub fn new(
        search: Option<Search>,
        rows: RowSet,
        join: HashMap<String, HashMap<NonZeroU32, SearchResult>>,
    ) -> Self {
        Self { search, rows, join }
    }

    pub fn search(&self) -> Option<&Search> {
        self.search.as_ref()
    }

    pub fn rows(&self) -> &RowSet {
        &self.rows
    }

    pub fn join(&self) -> &HashMap<String, HashMap<NonZeroU32, SearchResult>> {
        &self.join
    }

    pub fn sort(&self, database: &Database, orders: &[Order]) -> Vec<NonZeroU32> {
        if let Some(search) = self.search() {
            if let Some(collection) = database.collection(search.collection_id) {
                return if orders.len() > 0 {
                    collection.data().sort(&self.rows, &orders)
                } else {
                    self.rows.iter().cloned().collect()
                };
            }
        }
        vec![]
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

    pub async fn result(self, database: &Database) -> SearchResult {
        let collection_id = self.collection_id;
        if let Some(collection) = database.collection(collection_id) {
            let rows = if self.conditions.len() > 0 {
                Self::result_conditions(collection, &self.conditions, &database.relation).await
            } else {
                collection.data().all()
            };

            let join = future::join_all(self.join.iter().map(|(name, join)| async {
                (
                    name.to_owned(),
                    join.join_result(database, self.collection_id, &rows).await,
                )
            }))
            .await
            .into_iter()
            .collect();

            SearchResult {
                search: Some(self),
                rows,
                join,
            }
        } else {
            SearchResult {
                search: Some(self),
                rows: Default::default(),
                join: Default::default(),
            }
        }
    }
}
