use std::{
    collections::BTreeSet,
    num::NonZeroI64,
    ops::Deref,
    sync::{Arc, RwLock},
};

use semilattice_database::Search;

use super::{SessionOperation, TemporaryDataEntity};
use crate::{search, Activity, Condition, Database, Order, Session};

pub struct SessionSearch<'a> {
    session: &'a Session,
    search: Arc<RwLock<Search>>,
}
impl<'a> SessionSearch<'a> {
    #[inline(always)]
    pub fn new(session: &'a Session, search: Arc<RwLock<Search>>) -> Self {
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
    pub fn search(self, condition: Condition) -> Self {
        self.search.write().unwrap().search(condition);
        self
    }

    #[inline(always)]
    fn temporary_data_match(
        &self,
        row: NonZeroI64,
        ent: &TemporaryDataEntity,
        condition: &Condition,
    ) -> bool {
        match condition {
            Condition::Row(cond) => match cond {
                search::Number::In(c) => c.contains(&(row.get() as isize)),
                search::Number::Max(c) => row.get() <= *c as i64,
                search::Number::Min(c) => row.get() >= *c as i64,
                search::Number::Range(c) => c.contains(&(row.get() as isize)),
            },
            Condition::Uuid(uuid) => uuid.contains(&ent.uuid),
            Condition::Activity(activity) => ent.activity == *activity,
            Condition::Term(cond) => match cond {
                search::Term::In(c) => {
                    ent.term_begin < *c && (ent.term_end == 0 || ent.term_end > *c)
                }
                search::Term::Past(c) => ent.term_end >= *c,
                search::Term::Future(c) => ent.term_begin >= *c,
            },
            Condition::Field(key, cond) => ent.fields.get(key).map_or(false, |f| match cond {
                search::Field::Match(v) => f == v,
                search::Field::Range(min, max) => min <= f && max >= f,
                search::Field::Min(min) => min <= f,
                search::Field::Max(max) => max >= f,
                search::Field::Forward(v) => {
                    unsafe { std::str::from_utf8_unchecked(f) }.starts_with(v.as_ref())
                }
                search::Field::Partial(v) => {
                    unsafe { std::str::from_utf8_unchecked(f) }.contains(v.as_ref())
                }
                search::Field::Backward(v) => {
                    unsafe { std::str::from_utf8_unchecked(f) }.ends_with(v.as_ref())
                }
                search::Field::ValueForward(v) => {
                    v.starts_with(unsafe { std::str::from_utf8_unchecked(f) })
                }
                search::Field::ValueBackward(v) => {
                    v.ends_with(unsafe { std::str::from_utf8_unchecked(f) })
                }
                search::Field::ValuePartial(v) => {
                    v.contains(unsafe { std::str::from_utf8_unchecked(f) })
                }
            }),
            Condition::Narrow(conditions) => {
                let mut is_match = true;
                for c in conditions {
                    is_match &= self.temporary_data_match(row, ent, c);
                    if !is_match {
                        break;
                    }
                }
                is_match
            }
            Condition::Wide(conditions) => {
                let mut is_match = false;
                for c in conditions {
                    is_match |= self.temporary_data_match(row, ent, c);
                    if is_match {
                        break;
                    }
                }
                is_match
            }
            Condition::Depend(key, collection_row) => {
                let mut is_match = true;
                for depend in &ent.depends {
                    is_match = key.as_ref().map_or(true, |key| key == depend.key())
                        && collection_row == depend.deref();
                    if is_match {
                        break;
                    }
                }
                is_match
            }
            Condition::LastUpdated(_) => true,
        }
    }

    #[inline(always)]
    fn temprary_data_match_conditions(&self, row: NonZeroI64, ent: &TemporaryDataEntity) -> bool {
        for c in self.search.read().unwrap().conditions() {
            if !self.temporary_data_match(row, ent, c) {
                return false;
            }
        }
        true
    }

    //TODO : Supports join for session data. overwrite result data by session datas.
    #[inline(always)]
    pub fn result(self, database: &Database, orders: &Vec<Order>) -> Vec<NonZeroI64> {
        let collection_id = self.search.read().unwrap().collection_id();
        if let Some(collection) = database.collection(collection_id) {
            let result = self.search.write().unwrap().result(database);
            if let Some(tmp) = self.session.temporary_data.get(&collection_id) {
                let mut tmp_rows: BTreeSet<NonZeroI64> = BTreeSet::new();
                if let Some(result) = result.read().unwrap().as_ref() {
                    for row in result.rows() {
                        let row = NonZeroI64::from(*row);
                        if let Some(ent) = tmp.get(&row) {
                            if ent.operation != SessionOperation::Delete {
                                if self.temprary_data_match_conditions(row, ent) {
                                    tmp_rows.insert(row);
                                }
                            }
                        } else {
                            tmp_rows.insert(row);
                        }
                    }
                }
                for (row, _) in tmp {
                    //session new data
                    if row.get() < 0 {
                        if let Some(ent) = tmp.get(row) {
                            if ent.operation != SessionOperation::Delete {
                                if self.temprary_data_match_conditions(*row, ent) {
                                    tmp_rows.insert(*row);
                                }
                            }
                        }
                    }
                }
                let mut new_rows = tmp_rows.into_iter().collect();
                if orders.len() > 0 {
                    super::sort::sort(&mut new_rows, orders, collection, tmp);
                }
                return new_rows;
            } else {
                if let Some(result) = result.read().unwrap().as_ref() {
                    return result
                        .sort(database, orders)
                        .into_iter()
                        .map(|x| x.into())
                        .collect();
                }
            }
        }

        vec![]
    }
}
