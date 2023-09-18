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
};

use versatile_data::Activity;

#[derive(Debug)]
pub struct Search {
    collection_id: i32,
    conditions: Vec<Condition>,
    join: HashMap<String, Join>,
    result: Arc<RwLock<Option<SearchResult>>>,
}

impl Search {
    #[inline(always)]
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

    #[inline(always)]
    pub fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[inline(always)]
    pub fn conditions(&self) -> &Vec<Condition> {
        &self.conditions
    }

    #[inline(always)]
    pub fn search(&mut self, condition: Condition) -> &mut Self {
        self.conditions.push(condition);
        self
    }

    #[inline(always)]
    pub fn default(&mut self) -> &mut Self {
        self.conditions.push(Condition::Term(Term::default()));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
    }
}

impl Database {
    #[inline(always)]
    pub fn search(&self, colletion_id: i32) -> Search {
        Search::new(colletion_id, vec![], HashMap::new())
    }
}
