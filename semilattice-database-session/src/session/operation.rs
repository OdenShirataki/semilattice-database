use std::num::{NonZeroI32, NonZeroU32};

use crate::{CollectionRow, Record};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SessionOperation {
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
    key: String,
    records: Vec<SessionRecord>,
}
impl Pend {
    #[inline(always)]
    pub fn new(key: impl Into<String>, records: Vec<SessionRecord>) -> Pend {
        Pend {
            key: key.into(),
            records,
        }
    }

    #[inline(always)]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[inline(always)]
    pub fn records(&self) -> &Vec<SessionRecord> {
        &self.records
    }
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
