mod condition;
mod join;
mod result;

pub use condition::Condition;
pub use join::{Join, JoinCondition};
pub use result::SearchResult;

pub use versatile_data::search::{Field, Number, Term};

use crate::Database;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use versatile_data::Activity;

#[derive(Clone, Debug)]
pub struct Search {
    collection_id: i32,
    conditions: Vec<Condition>,
    join: HashMap<String, Join>,
    result: Arc<RwLock<Option<SearchResult>>>,
}
impl Search {
    pub fn new(
        collection_id: i32,
        conditions: Vec<Condition>,
        join: HashMap<String, Join>,
    ) -> Self {
        Self {
            collection_id,
            conditions,
            join,
            result: Arc::new(RwLock::new(None)),
        }
    }
    pub fn collection_id(&self) -> i32 {
        self.collection_id
    }
    pub fn conditions(&self) -> &Vec<Condition> {
        &self.conditions
    }
    pub fn search(&mut self, condition: Condition) -> &mut Self {
        self.conditions.push(condition);
        self
    }
    pub fn default(&mut self) -> &mut Self {
        self.conditions.push(Condition::Term(Term::In(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
    }
}

impl Database {
    pub fn search(&self, colletion_id: i32) -> Search {
        Search::new(colletion_id, vec![], HashMap::new())
    }
}
