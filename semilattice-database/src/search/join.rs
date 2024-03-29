use std::{
    num::{NonZeroI32, NonZeroU32},
    sync::Arc,
};

use async_recursion::async_recursion;
use futures::{future, FutureExt};
use hashbrown::HashMap;
use versatile_data::RowSet;

use crate::{CollectionRow, Condition, Database, Search};

use super::SearchResult;

#[derive(Debug, Clone, PartialEq)]
pub struct SearchJoin {
    collection_id: NonZeroI32,
    relation_key: Option<Arc<String>>,
    conditions: Vec<Condition>,
    join: HashMap<Arc<String>, SearchJoin>,
}

impl SearchJoin {
    pub fn new(
        collection_id: NonZeroI32,
        conditions: Vec<Condition>,
        relation_key: Option<Arc<String>>,
        join: HashMap<Arc<String>, SearchJoin>,
    ) -> Self {
        Self {
            collection_id,
            conditions,
            relation_key,
            join,
        }
    }

    #[async_recursion(?Send)]
    async fn join_result_row(
        &self,
        database: &Database,
        parent_collection_id: NonZeroI32,
        parent_row: NonZeroU32,
    ) -> SearchResult {
        let mut futs = vec![];
        if let Some(key) = &self.relation_key {
            futs.push(
                async {
                    database
                        .relation
                        .pends(
                            Some(Arc::clone(key)),
                            &CollectionRow::new(parent_collection_id, parent_row),
                            Some(self.collection_id),
                        )
                        .into_iter()
                        .map(|r| r.row())
                        .collect()
                }
                .boxed_local(),
            );
        }
        if self.conditions.len() > 0 {
            if let Some(collection) = database.collection(self.collection_id) {
                futs.push(
                    async {
                        Search::result_conditions(collection, &self.conditions, &database.relation)
                            .await
                    }
                    .boxed_local(),
                );
            }
        }

        let (mut rows, _index, futs) = future::select_all(futs).await;
        for r in future::join_all(futs).await.into_iter() {
            rows.retain(|v| r.contains(v));
        }

        let join_nest = future::join_all(self.join.iter().map(|(key, join)| async {
            (
                Arc::clone(key),
                join.join_result(database, self.collection_id, &rows).await,
            )
        }))
        .await
        .into_iter()
        .collect();

        SearchResult::new(None, rows, join_nest)
    }

    pub async fn join_result(
        &self,
        database: &Database,
        parent_collection_id: NonZeroI32,
        parent_rows: &RowSet,
    ) -> HashMap<NonZeroU32, SearchResult> {
        future::join_all(parent_rows.into_iter().map(|parent_row| async {
            (
                *parent_row,
                self.join_result_row(database, parent_collection_id, *parent_row)
                    .await,
            )
        }))
        .await
        .into_iter()
        .collect()
    }
}
