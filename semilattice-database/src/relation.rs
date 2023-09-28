mod index;

pub use index::RelationIndex;

use std::{
    num::{NonZeroI32, NonZeroU32},
    ops::Deref,
    sync::{Arc, RwLock},
};

use serde::{ser::SerializeStruct, Serialize};

use crate::{collection::CollectionRow, Database};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Depend {
    key: String,
    collection_row: CollectionRow,
}
impl Depend {
    #[inline(always)]
    pub fn new(key: impl Into<String>, collection_row: CollectionRow) -> Self {
        Self {
            key: key.into(),
            collection_row,
        }
    }

    #[inline(always)]
    pub fn key(&self) -> &str {
        &self.key
    }
}
impl Deref for Depend {
    type Target = CollectionRow;
    fn deref(&self) -> &Self::Target {
        &self.collection_row
    }
}
impl Serialize for Depend {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Depend", 3)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("collection_id", &self.collection_row.collection_id())?;
        state.serialize_field("row", &self.collection_row.row())?;
        state.end()
    }
}

impl Database {
    #[inline(always)]
    pub fn relation(&self) -> Arc<RwLock<RelationIndex>> {
        self.relation.clone()
    }

    #[inline(always)]
    pub fn register_relation(
        &mut self,
        key_name: &str,
        depend: &CollectionRow,
        pend: CollectionRow,
    ) {
        let depend = depend.clone();
        self.relation
            .write()
            .unwrap()
            .insert(key_name, depend, pend)
    }

    #[inline(always)]
    pub fn register_relations(
        &mut self,
        depend: &CollectionRow,
        pends: Vec<(String, CollectionRow)>,
    ) {
        for (key_name, pend) in pends {
            self.register_relation(&key_name, depend, pend);
        }
    }

    #[inline(always)]
    pub fn depends(
        &self,
        key: Option<&str>,
        pend_collection_id: NonZeroI32,
        pend_row: NonZeroU32,
    ) -> Vec<Depend> {
        self.relation
            .read()
            .unwrap()
            .depends(key, &CollectionRow::new(pend_collection_id, pend_row.get()))
            .iter()
            .cloned()
            .collect()
    }
}
