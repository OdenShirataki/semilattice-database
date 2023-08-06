use std::collections::HashMap;

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

    pub fn result_row(
        &self,
        database: &Database,
        parent_collection_id: i32,
        parent_row: u32,
    ) -> Vec<CollectionRow> {
        let mut result = vec![];
        for condition in &self.conditions {
            match condition {
                JoinCondition::Depend { key } => {
                    let rel = database
                        .relation
                        .read()
                        .unwrap()
                        .pends(key, &CollectionRow::new(parent_collection_id, parent_row));
                    for r in &rel {
                        if r.collection_id() == self.collection_id {
                            result.push(r.clone());
                        }
                    }
                }
            }
        }
        result
    }

    pub fn result(
        &self,
        database: &Database,
        parent_collection_id: i32,
        parent_rows: &RowSet,
    ) -> HashMap<u32, Vec<CollectionRow>> {
        let mut r = HashMap::new();

        for parent_row in parent_rows {
            let parent_row = *parent_row;
            r.insert(
                parent_row,
                self.result_row(database, parent_collection_id, parent_row),
            );
        }

        r
    }
}

#[derive(Clone, Debug)]
pub enum JoinCondition {
    Depend { key: Option<String> },
}
