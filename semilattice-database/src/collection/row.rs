use std::num::{NonZeroI32, NonZeroU32};

use serde::Serialize;

/// Ordered by collection_id, then row.
#[derive(Clone, Debug, Serialize, Hash, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollectionRow {
    collection_id: Option<NonZeroI32>,
    row: Option<NonZeroU32>,
}

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
