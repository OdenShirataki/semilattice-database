use std::{
    sync::mpsc::{channel, SendError, Sender},
    thread::spawn,
    time::{SystemTime, UNIX_EPOCH},
};

pub use versatile_data::search::{Field, Number, Term};
use versatile_data::{
    Activity, Condition as VersatileDataCondition, Order, RowSet, Search as VersatileDataSearch,
};

use crate::{Collection, CollectionRow, Database, RelationIndex, SessionDepend};

#[derive(Clone, Debug)]
pub enum Condition {
    Activity(Activity),
    Term(Term),
    Row(Number),
    Uuid(u128),
    LastUpdated(Number),
    Field(String, Field),
    Narrow(Vec<Condition>),
    Wide(Vec<Condition>),
    Depend(SessionDepend),
}

#[derive(Debug)]
pub struct Search {
    collection_id: i32,
    conditions: Vec<Condition>,
}
impl Search {
    pub fn new(collection: &Collection) -> Self {
        Self {
            collection_id: collection.id(),
            conditions: Vec::new(),
        }
    }
    pub fn search(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }
    pub fn default(mut self) -> Self {
        self.conditions.push(Condition::Term(Term::In(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
    }
    pub fn depend(mut self, condition: SessionDepend) -> Self {
        self.conditions.push(Condition::Depend(condition));
        self
    }
    pub fn field(self, field_name: impl Into<String>, condition: Field) -> Self {
        self.search(Condition::Field(field_name.into(), condition))
    }

    fn exec_cond(
        collection: &Collection,
        relation: &RelationIndex,
        condtion: &Condition,
        tx: Sender<RowSet>,
    ) -> Result<(), SendError<RowSet>> {
        match condtion {
            Condition::Activity(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Activity(*c),
                    tx,
                )?;
            }
            Condition::Term(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Term(c.clone()),
                    tx,
                )?;
            }
            Condition::Row(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Row(c.clone()),
                    tx,
                )?;
            }
            Condition::Uuid(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Uuid(c.clone()),
                    tx,
                )?;
            }
            Condition::LastUpdated(c) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::LastUpdated(c.clone()),
                    tx,
                )?;
            }
            Condition::Field(key, condition) => {
                VersatileDataSearch::search_exec_cond(
                    &collection.data,
                    &VersatileDataCondition::Field(key.to_owned(), condition.clone()),
                    tx,
                )?;
            }
            Condition::Depend(depend) => {
                let depend_row = depend.row();
                if depend_row > 0 {
                    let rel = relation.pends(
                        Some(depend.key()),
                        &CollectionRow::new(depend.collection_id(), depend_row as u32),
                    );
                    let collection_id = collection.id();
                    spawn(move || {
                        let mut tmp = RowSet::default();
                        for r in rel {
                            if r.collection_id() == collection_id {
                                tmp.insert(r.row());
                            }
                        }
                        let tx = tx.clone();
                        tx.send(tmp).unwrap();
                    });
                } else {
                    //todo? : search session depend
                }
            }
            Condition::Narrow(conditions) => {
                let (tx_inner, rx) = channel();
                for c in conditions {
                    let tx_inner = tx_inner.clone();
                    Self::exec_cond(collection, relation, c, tx_inner)?;
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
            Condition::Wide(conditions) => {
                let (tx_inner, rx) = channel();
                for c in conditions {
                    let tx_inner = tx_inner.clone();
                    Self::exec_cond(collection, relation, c, tx_inner)?;
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
    pub(super) fn result(
        &self,
        database: &Database,
        orders: &[Order],
    ) -> Result<Vec<u32>, SendError<RowSet>> {
        if let Some(collection) = database.collection(self.collection_id) {
            let mut rows = RowSet::default();
            if self.conditions.len() > 0 {
                let (tx, rx) = channel();
                for c in &self.conditions {
                    Self::exec_cond(collection, &database.relation, c, tx.clone())?;
                }
                drop(tx);
                let mut fst = true;
                for rs in rx {
                    if fst {
                        rows = rs;
                        fst = false;
                    } else {
                        rows = rows.intersection(&rs).map(|&x| x).collect()
                    }
                }
            } else {
                for row in collection.data.all() {
                    rows.insert(row);
                }
            }
            if orders.len() > 0 {
                Ok(collection.data.sort(rows, &orders))
            } else {
                Ok(rows.into_iter().collect())
            }
        } else {
            Ok(vec![])
        }
    }
}
