use serde::Serialize;
use std::cmp::Ordering;
use versatile_data::{Activity, Data, Operation};

use crate::anyhow::Result;

pub struct Collection {
    pub(crate) data: Data,
    id: i32,
    name: String,
}
impl Collection {
    pub fn new(data: Data, id: i32, name: impl Into<String>) -> Self {
        Self {
            data,
            id,
            name: name.into(),
        }
    }
    pub fn id(&self) -> i32 {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn activity(&self, row: u32) -> Activity {
        self.data.activity(row)
    }
    pub fn serial(&self, row: u32) -> u32 {
        self.data.serial(row)
    }
    pub fn uuid(&self, row: u32) -> u128 {
        self.data.uuid(row)
    }
    pub fn uuid_string(&self, row: u32) -> String {
        self.data.uuid_string(row)
    }
    pub fn last_updated(&self, row: u32) -> u64 {
        self.data.last_updated(row)
    }
    pub fn term_begin(&self, row: u32) -> u64 {
        self.data.term_begin(row)
    }
    pub fn term_end(&self, row: u32) -> u64 {
        self.data.term_end(row)
    }
    pub fn field_bytes(&self, row: u32, field_name: &str) -> &[u8] {
        self.data.field_bytes(row, field_name)
    }
    pub fn field_num(&self, row: u32, field_name: &str) -> f64 {
        self.data.field_num(row, field_name)
    }
    pub fn update(&mut self, operation: &Operation) -> Result<u32> {
        self.data.update(operation)
    }
}

#[derive(Clone, Copy, Default, Debug, Serialize)]
pub struct CollectionRow {
    collection_id: i32,
    row: u32,
}
impl PartialOrd for CollectionRow {
    fn partial_cmp(&self, other: &CollectionRow) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CollectionRow {
    fn cmp(&self, other: &CollectionRow) -> Ordering {
        if self.collection_id == other.collection_id {
            if self.row == other.row {
                Ordering::Equal
            } else if self.row > other.row {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        } else if self.collection_id > other.collection_id {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}
impl PartialEq for CollectionRow {
    fn eq(&self, other: &CollectionRow) -> bool {
        self.collection_id == other.collection_id && self.row == other.row
    }
}
impl Eq for CollectionRow {}

impl CollectionRow {
    pub fn new(collection_id: i32, row: u32) -> Self {
        Self { collection_id, row }
    }
    pub fn collection_id(&self) -> i32 {
        self.collection_id
    }
    pub fn row(&self) -> u32 {
        self.row
    }
}
