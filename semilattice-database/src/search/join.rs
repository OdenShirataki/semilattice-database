use std::collections::HashMap;

use futures::future;
use versatile_data::RowSet;

use crate::{CollectionRow, Database};

#[derive(Clone, Debug)]
pub struct Join {
    collection_id: i32,
    conditions: Vec<JoinCondition>,
}
impl Join {
    pub fn new(collection_id: i32, conditions: Vec<JoinCondition>) -> Self {
        Self {
            collection_id,
            conditions,
        }
    }

    fn result_row(
        &self,
        database: &Database,
        parent_collection_id: i32,
        parent_row: &u32,
    ) -> Vec<CollectionRow> {
        let parent_row = *parent_row;
        let mut result = vec![];
        for condition in &self.conditions {
            match condition {
                JoinCondition::Depend { key } => {
                    result.extend(
                        database
                            .relation
                            .read()
                            .unwrap()
                            .pends(key, &CollectionRow::new(parent_collection_id, parent_row))
                            .iter()
                            .filter(|r| r.collection_id() == self.collection_id)
                            .cloned()
                            .collect::<Vec<_>>(),
                    );
                }
            }
        }
        result
    }

    pub async fn result(
        &self,
        database: &Database,
        parent_collection_id: i32,
        parent_rows: &RowSet,
    ) -> HashMap<u32, Vec<CollectionRow>> {
        let mut r = HashMap::new();

        let mut fs: Vec<_> = parent_rows
            .iter()
            .map(|parent_row| {
                Box::pin(async {
                    (
                        *parent_row,
                        self.result_row(database, parent_collection_id, parent_row),
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

#[derive(Clone, Debug)]
pub enum JoinCondition {
    Depend { key: Option<String> },
}
