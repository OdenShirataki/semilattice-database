use std::{
    cmp::Ordering,
    num::{NonZeroI32, NonZeroU32},
};

use serde::Serialize;

#[derive(Clone, Debug, Serialize, Hash, Default)]
pub struct CollectionRow {
    collection_id: Option<NonZeroI32>,
    row: Option<NonZeroU32>,
}
impl PartialOrd for CollectionRow {
    #[inline(always)]
    fn partial_cmp(&self, other: &CollectionRow) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CollectionRow {
    fn cmp(&self, other: &CollectionRow) -> Ordering {
        if self.collection_id == other.collection_id {
            self.row.cmp(&other.row)
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
    pub fn new(collection_id: NonZeroI32, row: NonZeroU32) -> Self {
        Self {
            collection_id: Some(collection_id),
            row: Some(row),
        }
    }

    pub fn collection_id(&self) -> NonZeroI32 {
        self.collection_id.unwrap()
    }

    pub fn row(&self) -> NonZeroU32 {
        self.row.unwrap()
    }
}
