use std::collections::HashMap;

use async_recursion::async_recursion;
use futures::future;
use versatile_data::RowSet;

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
        parent_row: &u32,
    ) -> SearchResult {
        let parent_row = *parent_row;
        let mut result = RowSet::default();
        for condition in &self.conditions {
            match condition {
                JoinCondition::Pends { key } => {
                    result.append(
                        &mut database
                            .relation
                            .read()
                            .unwrap()
                            .pends(key, &CollectionRow::new(parent_collection_id, parent_row))
                            .iter()
                            .filter(|r| r.collection_id() == self.collection_id)
                            .map(|v| v.row())
                            .collect(),
                    );
                }
                JoinCondition::Field(name, condition) => {
                    if let Some(collection) = database.collection(parent_collection_id) {
                        collection.search_field(name, condition);
                    }
                }
            }
        }
        let mut join_inner = HashMap::new();
        for (key, join) in &self.join {
            join_inner.insert(
                key.to_owned(),
                join.result(database, self.collection_id, &result).await,
            );
        }
        SearchResult::new(self.collection_id, result, join_inner)
    }

    #[async_recursion]
    pub async fn result(
        &self,
        database: &Database,
        parent_collection_id: i32,
        parent_rows: &RowSet,
    ) -> HashMap<u32, SearchResult> {
        let mut r = HashMap::new();

        let mut fs: Vec<_> = parent_rows
            .iter()
            .map(|parent_row| {
                Box::pin(async {
                    (
                        *parent_row,
                        self.result_row(database, parent_collection_id, parent_row)
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
