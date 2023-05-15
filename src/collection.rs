use serde::Serialize;
use std::{
    cmp::Ordering,
    ops::{Deref, DerefMut},
};
use versatile_data::Data;

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
}
impl Deref for Collection {
    type Target = Data;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl DerefMut for Collection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[derive(Clone, Debug, Serialize, Hash)]
pub struct CollectionRow {
    collection_id: i32, //Negative values ​​contain session rows
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
