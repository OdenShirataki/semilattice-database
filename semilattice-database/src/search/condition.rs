use async_recursion::async_recursion;
use futures::future;

use versatile_data::{
    search::{Field, Number, Term},
    Activity, Condition as VersatileDataCondition, RowSet, Search as VersatileDataSearch,
};

use crate::{Collection, CollectionRow, RelationIndex, Search};

#[derive(Debug, Clone)]
pub enum Condition {
    Activity(Activity),
    Term(Term),
    Row(Number),
    Uuid(Vec<u128>),
    LastUpdated(Number),
    Field(String, Field),
    Narrow(Vec<Condition>),
    Wide(Vec<Condition>),
    Depend(Option<String>, CollectionRow),
}
impl Condition {
    #[async_recursion]
    pub(crate) async fn result(&self, collection: &Collection, relation: &RelationIndex) -> RowSet {
        match self {
            Self::Activity(c) => {
                VersatileDataSearch::result_condition(
                    &collection.data,
                    &VersatileDataCondition::Activity(*c),
                )
                .await
            }
            Self::Term(c) => {
                VersatileDataSearch::result_condition(
                    &collection.data,
                    &VersatileDataCondition::Term(c.clone()),
                )
                .await
            }
            Self::Row(c) => {
                VersatileDataSearch::result_condition(
                    &collection.data,
                    &VersatileDataCondition::Row(c),
                )
                .await
            }
            Self::Uuid(c) => {
                VersatileDataSearch::result_condition(
                    &collection.data,
                    &VersatileDataCondition::Uuid(c),
                )
                .await
            }
            Self::LastUpdated(c) => {
                VersatileDataSearch::result_condition(
                    &collection.data,
                    &VersatileDataCondition::LastUpdated(c),
                )
                .await
            }
            Self::Field(key, condition) => {
                VersatileDataSearch::result_condition(
                    &collection.data,
                    &VersatileDataCondition::Field(key, condition),
                )
                .await
            }
            Self::Depend(key, collection_row) => {
                let collection_id = collection.id();
                relation
                    .pends(key, collection_row)
                    .iter()
                    .filter_map(|r| (r.collection_id() == collection_id).then(|| r.row()))
                    .collect::<RowSet>()
            }
            Self::Narrow(conditions) => {
                Search::result_conditions(collection, conditions, relation).await
            }
            Self::Wide(conditions) => future::join_all(
                conditions
                    .iter()
                    .map(|c| c.result(collection, relation))
                    .collect::<Vec<_>>(),
            )
            .await
            .iter()
            .flat_map(|v| v)
            .cloned()
            .collect::<RowSet>(),
        }
    }
}
