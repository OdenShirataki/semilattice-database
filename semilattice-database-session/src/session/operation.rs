use std::num::{NonZeroI32, NonZeroU32};

use crate::{CollectionRow, Record};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum SessionOperation {
    #[default]
    New,
    Update,
    Delete,
}

#[derive(Debug)]
pub enum Depends {
    Default,
    Overwrite(Vec<(String, CollectionRow)>),
}

#[derive(Debug)]
pub struct Pend {
    pub key: String,
    pub records: Vec<SessionRecord>,
}

#[derive(Debug)]
pub enum SessionRecord {
    New {
        collection_id: NonZeroI32,
        record: Record,
        depends: Depends,
        pends: Vec<Pend>,
    },
    Update {
        collection_id: NonZeroI32,
        row: NonZeroU32,
        record: Record,
        depends: Depends,
        pends: Vec<Pend>,
    },
    Delete {
        collection_id: NonZeroI32,
        row: NonZeroU32,
    },
}
