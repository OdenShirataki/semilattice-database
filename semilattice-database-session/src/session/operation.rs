use std::{
    num::{NonZeroI32, NonZeroU32},
    sync::Arc,
};

use hashbrown::HashMap;
use semilattice_database::{Activity, FieldName, Term};

use crate::CollectionRow;

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
    Overwrite(Vec<(Arc<String>, CollectionRow)>),
}

#[derive(Debug)]
pub struct Pend {
    pub key: Arc<String>,
    pub records: Vec<SessionRecord>,
}

#[derive(Debug)]
pub enum SessionRecord {
    Update {
        collection_id: NonZeroI32,
        row: Option<NonZeroU32>,
        activity: Activity,
        term_begin: Term,
        term_end: Term,
        fields: HashMap<FieldName, Vec<u8>>,
        depends: Depends,
        pends: Vec<Pend>,
    },
    Delete {
        collection_id: NonZeroI32,
        row: NonZeroU32,
    },
}
