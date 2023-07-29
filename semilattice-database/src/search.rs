mod condition;

pub use self::condition::Condition;
pub use versatile_data::search::{Field, Number, Term};

use std::{
    sync::mpsc::{channel, SendError},
    time::{SystemTime, UNIX_EPOCH},
};

use versatile_data::{Activity, Order, RowSet};

use crate::{Collection, Database};

#[derive(Clone, Debug)]
pub struct Search {
    collection_id: i32,
    conditions: Vec<Condition>,
}
impl Search {
    pub fn begin(collection: &Collection) -> Self {
        Self {
            collection_id: collection.id(),
            conditions: Vec::new(),
        }
    }
    pub fn new(collection_id: i32, conditions: Vec<Condition>) -> Self {
        Self {
            collection_id,
            conditions,
        }
    }
    pub fn collection_id(&self) -> i32 {
        self.collection_id
    }
    pub fn conditions(&self) -> &Vec<Condition> {
        &self.conditions
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

    pub(crate) fn result(
        &self,
        database: &Database,
        orders: &[Order],
    ) -> Result<Vec<u32>, SendError<RowSet>> {
        if let Some(collection) = database.collection(self.collection_id) {
            let mut rows = RowSet::default();
            if self.conditions.len() > 0 {
                let (tx, rx) = channel();
                for c in &self.conditions {
                    c.result(collection, &database.relation, tx.clone())?;
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
