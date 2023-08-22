use std::sync::{Arc, RwLock};

use async_recursion::async_recursion;
use futures::future;

use versatile_data::{
    search::{Field, Number, Term},
    Activity, Condition as VersatileDataCondition, RowSet, Search as VersatileDataSearch,
};

use crate::{Collection, CollectionRow, RelationIndex};

#[derive(Clone, Debug)]
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
    pub(crate) async fn result(
        &self,
        collection: &Collection,
        relation: &Arc<RwLock<RelationIndex>>,
    ) -> RowSet {
        match self {
            Self::Activity(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Activity(*c),
                )
                .await
            }
            Self::Term(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Term(c.clone()),
                )
                .await
            }
            Self::Row(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Row(c.clone()),
                )
                .await
            }
            Self::Uuid(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Uuid(c.clone()),
                )
                .await
            }
            Self::LastUpdated(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::LastUpdated(c.clone()),
                )
                .await
            }
            Self::Field(key, condition) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Field(key.to_owned(), condition.clone()),
                )
                .await
            }
            Self::Depend(key, collection_row) => {
                let collection_id = collection.id();
                let key = key.clone();
                let collection_row = collection_row.clone();

                let rel = relation.read().unwrap().pends(&key, &collection_row);
                let mut tmp = RowSet::default();
                for r in rel {
                    if r.collection_id() == collection_id {
                        tmp.insert(r.row());
                    }
                }
                tmp
            }
            Self::Narrow(conditions) => {
                let mut fs = conditions
                    .iter()
                    .map(|c| c.result(collection, relation))
                    .collect();
                let (ret, _index, remaining) = future::select_all(fs).await;
                let mut rows = ret;
                fs = remaining;
                while !fs.is_empty() {
                    let (ret, _index, remaining) = future::select_all(fs).await;
                    rows = rows.intersection(&ret).map(|&x| x).collect();
                    fs = remaining;
                }
                rows
            }
            Self::Wide(conditions) => {
                let mut fs: Vec<_> = conditions
                    .iter()
                    .map(|c| c.result(collection, relation))
                    .collect();
                let mut tmp = RowSet::default();
                while !fs.is_empty() {
                    let (ret, _index, remaining) = future::select_all(fs).await;
                    tmp.extend(ret);
                    fs = remaining;
                }
                tmp
            }
        }
    }
}
