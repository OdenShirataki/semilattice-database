use std::num::{NonZeroI32, NonZeroU32};

use async_recursion::async_recursion;
use futures::{future, FutureExt};
use hashbrown::HashMap;
use versatile_data::{RowSet, Search};

use crate::{CollectionRow, Database};

use super::SearchResult;

#[derive(Debug, Clone)]
pub enum JoinCondition {
    Pends { key: Option<String> },
    Field(String, versatile_data::search::Field),
}

#[derive(Debug, Clone)]
pub struct Join {
    collection_id: NonZeroI32,
    conditions: Vec<JoinCondition>,
    join: HashMap<String, Join>,
}
impl Join {
    #[inline(always)]
    pub fn new(collection_id: NonZeroI32, conditions: Vec<JoinCondition>) -> Self {
        Self {
            collection_id,
            conditions,
            join: HashMap::new(),
        }
    }

    #[async_recursion]
    async fn result_row(
        &self,
        database: &Database,
        parent_collection_id: NonZeroI32,
        parent_row: NonZeroU32,
    ) -> SearchResult {
        let mut fs: Vec<_> = vec![];
        for condition in &self.conditions {
            match condition {
                JoinCondition::Pends { key } => {
                    fs.push(
                        async {
                            database
                                .relation
                                .pends(key, &CollectionRow::new(parent_collection_id, parent_row))
                                .iter()
                                .filter_map(|r| {
                                    (r.collection_id() == self.collection_id).then_some(r.row())
                                })
                                .collect::<RowSet>()
                        }
                        .boxed(),
                    );
                }
                JoinCondition::Field(name, condition) => {
                    if let Some(collection) = database.collection(parent_collection_id) {
                        fs.push(
                            async { Search::result_field(collection, name, condition) }.boxed(),
                        );
                    }
                }
            }
        }

        let (mut rows, _index, fs) = future::select_all(fs).await;
        for r in future::join_all(fs).await {
            rows = rows.intersection(&r).cloned().collect();
        }

        let join_nest = future::join_all(self.join.iter().map(|(key, join)| async {
            (
                key.to_owned(),
                join.result(database, self.collection_id, &rows).await,
            )
        }))
        .await
        .iter()
        .cloned()
        .collect::<HashMap<String, HashMap<_, _>>>();

        SearchResult::new(self.collection_id, rows, join_nest)
    }

    pub async fn result(
        &self,
        database: &Database,
        parent_collection_id: NonZeroI32,
        parent_rows: &RowSet,
    ) -> HashMap<NonZeroU32, SearchResult> {
        future::join_all(parent_rows.iter().map(|parent_row| {
            Box::pin(async {
                (
                    *parent_row,
                    self.result_row(database, parent_collection_id, *parent_row)
                        .await,
                )
            })
        }))
        .await
        .iter()
        .cloned()
        .collect::<HashMap<NonZeroU32, SearchResult>>()
    }
}
