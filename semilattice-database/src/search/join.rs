use std::num::{NonZeroI32, NonZeroU32};

use async_recursion::async_recursion;
use futures::{future, FutureExt};
use hashbrown::HashMap;
use versatile_data::RowSet;

use crate::{CollectionRow, Condition, Database, Search};

use super::SearchResult;

impl Search {
    #[async_recursion]
    async fn join_result_row(
        &self,
        database: &Database,
        parent_collection_id: NonZeroI32,
        parent_row: NonZeroU32,
    ) -> SearchResult {
        let mut futs = vec![];
        for condition in &self.conditions {
            match condition {
                Condition::JoinPends { key } => {
                    futs.push(
                        async {
                            database
                                .relation
                                .pends(key, &CollectionRow::new(parent_collection_id, parent_row))
                                .into_iter()
                                .filter_map(|r| {
                                    (r.collection_id() == self.collection_id).then_some(r.row())
                                })
                                .collect()
                        }
                        .boxed(),
                    );
                }
                Condition::JoinField(name, condition) => {
                    if let Some(collection) = database.collection(parent_collection_id) {
                        futs.push(
                            async {
                                versatile_data::Search::result_field(collection, name, condition)
                            }
                            .boxed(),
                        );
                    }
                }
                _ => {}
            }
        }

        let (mut rows, _index, futs) = future::select_all(futs).await;
        for r in future::join_all(futs).await.into_iter() {
            rows.retain(|v| r.contains(v));
        }

        let join_nest = future::join_all(self.join.iter().map(|(key, join)| async {
            (
                key.to_owned(),
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
