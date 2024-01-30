use std::sync::Arc;

use async_recursion::async_recursion;
use futures::future;

use versatile_data::{
    search::{Field, Number, Term},
    Activity, Condition as VersatileDataCondition, FieldName, RowSet,
};

use crate::{Collection, CollectionRow, RelationIndex, Search};

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Activity(Activity),
    Term(Term),
    Row(Number),
    Uuid(Vec<u128>),
    LastUpdated(Number),
    Field(FieldName, Field),
    Narrow(Vec<Condition>),
    Wide(Vec<Condition>),
    Depend(Option<Arc<String>>, CollectionRow),
}
impl Condition {
    #[async_recursion(?Send)]
    pub(crate) async fn result(&self, collection: &Collection, relation: &RelationIndex) -> RowSet {
        match self {
            Self::Activity(c) => {
                collection
                    .data()
                    .result_condition(&VersatileDataCondition::Activity(*c))
                    .await
            }
            Self::Term(c) => {
                collection
                    .data()
                    .result_condition(&VersatileDataCondition::Term(c.clone()))
                    .await
            }
            Self::Row(c) => {
                collection
                    .data()
                    .result_condition(&VersatileDataCondition::Row(c))
                    .await
            }
            Self::Uuid(c) => {
                collection
                    .data()
                    .result_condition(&VersatileDataCondition::Uuid(c))
                    .await
            }
            Self::LastUpdated(c) => {
                collection
                    .data()
                    .result_condition(&VersatileDataCondition::LastUpdated(c))
                    .await
            }
            Self::Field(name, condition) => {
                collection
                    .data()
                    .result_condition(&VersatileDataCondition::Field(Arc::clone(name), condition))
                    .await
            }
            Self::Depend(key, collection_row) => {
                let collection_id = collection.id();
                relation
                    .pends(key.clone(), collection_row, Some(collection_id))
                    .into_iter()
                    .map(|r| r.row())
                    .collect()
            }
            Self::Narrow(conditions) => {
                Search::result_conditions(collection, conditions, relation).await
            }
            Self::Wide(conditions) => future::join_all(
                conditions
                    .into_iter()
                    .map(|c| c.result(collection, relation)),
            )
            .await
            .into_iter()
            .flatten()
            .collect(),
        }
    }
}
