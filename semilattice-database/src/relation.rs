mod index;

pub use index::RelationIndex;

use std::{
    num::{NonZeroI32, NonZeroU32},
    ops::Deref,
    sync::Arc,
};

use serde::{ser::SerializeStruct, Serialize};

use crate::{collection::CollectionRow, Database};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Depend {
    key: Arc<String>,
    collection_row: CollectionRow,
}
impl Depend {
    pub fn new(key: Arc<String>, collection_row: CollectionRow) -> Self {
        Self {
            key,
            collection_row,
        }
    }

    pub fn key(&self) -> &Arc<String> {
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
        state.serialize_field("key", self.key.as_str())?;
        state.serialize_field("collection_id", &self.collection_row.collection_id())?;
        state.serialize_field("row", &self.collection_row.row())?;
        state.end()
    }
}

impl Database {
    pub fn relation(&self) -> &RelationIndex {
        &self.relation
    }

    pub fn relation_mut(&mut self) -> &mut RelationIndex {
        &mut self.relation
    }

    pub async fn register_relation(
        &mut self,
        key_name: &str,
        depend: &CollectionRow,
        pend: &CollectionRow,
    ) {
        self.relation.insert(key_name, depend, pend).await
    }

    pub async fn register_relations(
        &mut self,
        depend: &CollectionRow,
        pends: Vec<(Arc<String>, CollectionRow)>,
    ) {
        for (key_name, pend) in pends.iter() {
            self.register_relation(key_name.as_str(), depend, pend)
                .await;
        }
    }

    pub fn depends(
        &self,
        key: Option<Arc<String>>,
        pend_collection_id: NonZeroI32,
        pend_row: NonZeroU32,
    ) -> Vec<Depend> {
        self.relation
            .depends(key, &CollectionRow::new(pend_collection_id, pend_row))
            .into_iter()
            .collect()
    }
}
