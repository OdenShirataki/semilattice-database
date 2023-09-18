use crate::{CollectionRow, Record};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
        collection_id: i32,
        record: Record,
        depends: Depends,
        pends: Vec<Pend>,
    },
    Update {
        collection_id: i32,
        row: u32,
        record: Record,
        depends: Depends,
        pends: Vec<Pend>,
    },
    Delete {
        collection_id: i32,
        row: u32,
    },
}
