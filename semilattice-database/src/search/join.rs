use std::num::NonZeroU32;

use async_recursion::async_recursion;
use futures::{future, FutureExt};
use hashbrown::HashMap;
use versatile_data::{RowSet, Search};

use crate::{CollectionRow, Database};

use super::SearchResult;

#[derive(Debug)]
pub enum JoinCondition {
    Pends { key: Option<String> },
    Field(String, versatile_data::search::Field),
}

#[derive(Debug)]
pub struct Join {
    collection_id: i32,
    conditions: Vec<JoinCondition>,
    join: HashMap<String, Join>,
}
impl Join {
    #[inline(always)]
    pub fn new(collection_id: i32, conditions: Vec<JoinCondition>) -> Self {
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
        parent_collection_id: i32,
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
                                .read()
                                .unwrap()
                                .pends(
                                    key,
                                    &CollectionRow::new(parent_collection_id, parent_row.get()),
                                )
                                .iter()
                                .filter(|r| r.collection_id() == self.collection_id)
                                .map(|v| v.row())
                                .collect::<RowSet>()
                        }
                        .boxed(),
                    );
                }
                JoinCondition::Field(name, condition) => {
                    if let Some(collection) = database.collection(parent_collection_id) {
                        fs.push(Search::result_field(collection, name, condition).boxed());
                    }
                }
            }
        }

        let (ret, _index, remaining) = future::select_all(fs).await;
        let mut rows = ret;
        fs = remaining;
        while !fs.is_empty() {
            let (ret, _index, remaining) = future::select_all(fs).await;
            rows = rows.intersection(&ret).cloned().collect();
            fs = remaining;
        }

        //TODO: optimize , multithreading
        let mut join_nest = HashMap::new();
        for (key, join) in &self.join {
            join_nest.insert(
                key.to_owned(),
                join.result(database, self.collection_id, &rows).await,
            );
        }
        SearchResult::new(self.collection_id, rows, join_nest)
    }

    #[async_recursion]
    pub async fn result(
        &self,
        database: &Database,
        parent_collection_id: i32,
        parent_rows: &RowSet,
    ) -> HashMap<NonZeroU32, SearchResult> {
        let mut r = HashMap::new();

        let mut fs: Vec<_> = parent_rows
            .iter()
            .map(|parent_row| {
                Box::pin(async {
                    (
                        *parent_row,
                        self.result_row(database, parent_collection_id, *parent_row)
                            .await,
                    )
                })
            })
            .collect();
        while !fs.is_empty() {
            let (ret, _index, remaining) = future::select_all(fs).await;
            r.insert(ret.0, ret.1);
            fs = remaining;
        }
        r
    }
}
