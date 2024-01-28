mod condition;
mod join;
mod result;

pub use self::join::SearchJoin;
pub use condition::Condition;
pub use result::SearchResult;

pub use versatile_data::search::{Field, Number, Term};

use crate::Database;

use std::num::NonZeroI32;

use hashbrown::HashMap;

use versatile_data::{Activity, FieldName};

#[derive(Debug, Clone, PartialEq)]
pub struct Search {
    collection_id: NonZeroI32,
    conditions: Vec<Condition>,
    join: HashMap<String, SearchJoin>,
}

impl Search {
    pub fn new(
        collection_id: NonZeroI32,
        conditions: Vec<Condition>,
        join: HashMap<String, SearchJoin>,
    ) -> Self {
        Self {
            collection_id,
            conditions,
            join,
        }
    }

    pub fn collection_id(&self) -> NonZeroI32 {
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
        self.conditions.push(Condition::Term(Term::default()));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
    }

    pub fn search_field(self, field_name: FieldName, condition: Field) -> Self {
        self.search(Condition::Field(field_name, condition))
    }

    pub fn search_row(self, condition: Number) -> Self {
        self.search(Condition::Row(condition))
    }

    pub fn search_activity(self, condition: Activity) -> Self {
        self.search(Condition::Activity(condition))
    }
}

impl Database {
    pub fn search(&self, colletion_id: NonZeroI32) -> Search {
        Search::new(colletion_id, vec![], HashMap::new())
    }
}
