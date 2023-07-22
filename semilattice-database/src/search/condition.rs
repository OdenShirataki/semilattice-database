use std::{
    sync::{
        mpsc::{channel, SendError, Sender},
        Arc, RwLock,
    },
    thread::spawn,
};

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
    pub(crate) fn result(
        &self,
        collection: &Collection,
        relation: &Arc<RwLock<RelationIndex>>,
        tx: Sender<RowSet>,
    ) -> Result<(), SendError<RowSet>> {
        match self {
            Self::Activity(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Activity(*c),
                    tx,
                )?;
            }
            Self::Term(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Term(c.clone()),
                    tx,
                )?;
            }
            Self::Row(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Row(c.clone()),
                    tx,
                )?;
            }
            Self::Uuid(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Uuid(c.clone()),
                    tx,
                )?;
            }
            Self::LastUpdated(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::LastUpdated(c.clone()),
                    tx,
                )?;
            }
            Self::Field(key, condition) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Field(key.to_owned(), condition.clone()),
                    tx,
                )?;
            }
            Self::Depend(key, collection_row) => {
                let collection_id = collection.id();
                let relation = Arc::clone(relation);
                let key = key.clone();
                let collection_row = collection_row.clone();
                spawn(move || {
                    let rel = relation.read().unwrap().pends(key, &collection_row);
                    let mut tmp = RowSet::default();
                    for r in rel {
                        if r.collection_id() == collection_id {
                            tmp.insert(r.row());
                        }
                    }
                    let tx = tx.clone();
                    tx.send(tmp).unwrap();
                });
            }
            Self::Narrow(conditions) => {
                let (tx_inner, rx) = channel();
                for c in conditions {
                    let tx_inner = tx_inner.clone();
                    c.result(collection, relation, tx_inner)?;
                }
                drop(tx_inner);
                spawn(move || {
                    let mut is_1st = true;
                    let mut tmp = RowSet::default();
                    for mut rs in rx {
                        if is_1st {
                            tmp = rs;
                            is_1st = false;
                        } else {
                            tmp = tmp.intersection(&mut rs).map(|&x| x).collect();
                        }
                    }
                    tx.send(tmp).unwrap();
                });
            }
            Self::Wide(conditions) => {
                let (tx_inner, rx) = channel();
                for c in conditions {
                    let tx_inner = tx_inner.clone();
                    c.result(collection, relation, tx_inner)?;
                }
                drop(tx_inner);
                spawn(move || {
                    let mut tmp = RowSet::default();
                    for ref mut rs in rx {
                        tmp.append(rs);
                    }
                    tx.send(tmp).unwrap();
                });
            }
        }
        Ok(())
    }
}
