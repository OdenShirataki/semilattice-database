mod condition;
mod join;
mod result;

pub use condition::Condition;
pub use join::{Join, JoinCondition};
pub use result::SearchResult;

pub use versatile_data::search::{Field, Number, Term};

use crate::Database;

use std::num::NonZeroI32;

use hashbrown::HashMap;

use versatile_data::Activity;

#[derive(Debug, Clone, PartialEq)]
pub struct Search {
    collection_id: NonZeroI32,
    conditions: Vec<Condition>,
    join: HashMap<String, Join>,
}

impl Search {
    #[inline(always)]
    pub fn new(
        collection_id: NonZeroI32,
        conditions: Vec<Condition>,
        join: HashMap<String, Join>,
    ) -> Self {
        Self {
            collection_id,
            conditions,
            join,
        }
    }

    #[inline(always)]
    pub fn collection_id(&self) -> NonZeroI32 {
        self.collection_id
    }

    #[inline(always)]
    pub fn conditions(&self) -> &Vec<Condition> {
        &self.conditions
    }

    #[inline(always)]
    pub fn search(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }

    #[inline(always)]
    pub fn default(mut self) -> Self {
        self.conditions.push(Condition::Term(Term::default()));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
    }
}

impl Database {
    #[inline(always)]
    pub fn search(&self, colletion_id: NonZeroI32) -> Search {
        Search::new(colletion_id, vec![], HashMap::new())
    }
}
