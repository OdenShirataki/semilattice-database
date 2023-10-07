use std::num::NonZeroI64;

use semilattice_database::Search;

use crate::{search, Activity, Condition, Database, Order, Session};

pub struct SessionSearch<'a> {
    session: &'a mut Session,
    search: Search,
}
impl<'a> SessionSearch<'a> {
    #[inline(always)]
    pub fn new(session: &'a mut Session, search: Search) -> Self {
        Self { session, search }
    }

    #[inline(always)]
    pub fn search_default(self) -> Result<Self, std::time::SystemTimeError> {
        Ok(self
            .search_term(search::Term::default())
            .search(Condition::Activity(Activity::Active)))
    }

    #[inline(always)]
    pub fn search_field(self, field_name: impl Into<String>, condition: search::Field) -> Self {
        self.search(Condition::Field(field_name.into(), condition))
    }

    #[inline(always)]
    pub fn search_term(self, condition: search::Term) -> Self {
        self.search(Condition::Term(condition))
    }

    #[inline(always)]
    pub fn search_activity(self, condition: Activity) -> Self {
        self.search(Condition::Activity(condition))
    }

    #[inline(always)]
    pub fn search_row(self, condition: search::Number) -> Self {
        self.search(Condition::Row(condition))
    }

    #[inline(always)]
    pub fn search(mut self, condition: Condition) -> Self {
        self.search = self.search.search(condition);
        self
    }

    pub async fn result(&mut self, database: &Database, orders: &Vec<Order>) -> Vec<NonZeroI64> {
        self.session
            .result_with(&mut self.search, database, orders)
            .await
    }
}
