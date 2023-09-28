use std::{
    cmp::Ordering,
    num::{NonZeroI32, NonZeroU32},
};

use serde::Serialize;

#[derive(Clone, Debug, Serialize, Hash)]
pub struct CollectionRow {
    collection_id: NonZeroI32,
    row: NonZeroU32,
}
impl PartialOrd for CollectionRow {
    #[inline(always)]
    fn partial_cmp(&self, other: &CollectionRow) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CollectionRow {
    #[inline(always)]
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
    #[inline(always)]
    fn eq(&self, other: &CollectionRow) -> bool {
        self.collection_id == other.collection_id && self.row == other.row
    }
}
impl Eq for CollectionRow {}

impl CollectionRow {
    #[inline(always)]
    pub fn new(collection_id: NonZeroI32, row: u32) -> Self {
        Self {
            collection_id,
            row: NonZeroU32::new(row).unwrap(),
        }
    }

    #[inline(always)]
    pub fn collection_id(&self) -> NonZeroI32 {
        self.collection_id
    }

    #[inline(always)]
    pub fn row(&self) -> NonZeroU32 {
        self.row
    }
}
