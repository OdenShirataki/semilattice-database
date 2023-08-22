use std::{
    collections::BTreeSet,
    ops::Deref,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use semilattice_database::Search;

use super::{SessionOperation, TemporaryDataEntity};
use crate::{search, Activity, Condition, Database, Order, RowSet, Session};

pub struct SessionSearch<'a> {
    session: &'a Session,
    search: Arc<RwLock<Search>>,
}
impl<'a> SessionSearch<'a> {
    pub fn new(session: &'a Session, search: Arc<RwLock<Search>>) -> Self {
        Self { session, search }
    }
    pub fn search_default(self) -> Result<Self, std::time::SystemTimeError> {
        Ok(self
            .search_term(search::Term::In(
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            ))
            .search(Condition::Activity(Activity::Active)))
    }
    pub fn search_field(self, field_name: impl Into<String>, condition: search::Field) -> Self {
        self.search(Condition::Field(field_name.into(), condition))
    }
    pub fn search_term(self, condition: search::Term) -> Self {
        self.search(Condition::Term(condition))
    }
    pub fn search_activity(self, condition: Activity) -> Self {
        self.search(Condition::Activity(condition))
    }
    pub fn search_row(self, condition: search::Number) -> Self {
        self.search(Condition::Row(condition))
    }

    pub fn search(self, condition: Condition) -> Self {
        self.search.write().unwrap().search(condition);
        self
    }

    fn temporary_data_match(
        &self,
        row: i64,
        ent: &TemporaryDataEntity,
        condition: &Condition,
    ) -> bool {
        match condition {
            Condition::Row(cond) => match cond {
                search::Number::In(c) => c.contains(&(row as isize)),
                search::Number::Max(c) => row <= *c as i64,
                search::Number::Min(c) => row >= *c as i64,
                search::Number::Range(c) => c.contains(&(row as isize)),
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
            Condition::Field(key, cond) => {
                if let Some(field_tmp) = ent.fields.get(key) {
                    match cond {
                        search::Field::Match(v) => field_tmp == v,
                        search::Field::Range(min, max) => {
                            min <= field_tmp && max >= field_tmp
                        }
                        search::Field::Min(min) => min <= field_tmp,
                        search::Field::Max(max) => max >= field_tmp,
                        search::Field::Forward(v) => {
                            unsafe { std::str::from_utf8_unchecked(field_tmp) }
                                .starts_with(v.as_ref())
                        }
                        search::Field::Partial(v) => {
                            unsafe { std::str::from_utf8_unchecked(field_tmp) }.contains(v.as_ref())
                        }
                        search::Field::Backward(v) => {
                            unsafe { std::str::from_utf8_unchecked(field_tmp) }
                                .ends_with(v.as_ref())
                        }
                        search::Field::ValueForward(v) => {
                            v.starts_with(unsafe { std::str::from_utf8_unchecked(field_tmp) })
                        }
                        search::Field::ValueBackward(v) => {
                            v.ends_with(unsafe { std::str::from_utf8_unchecked(field_tmp) })
                        }
                        search::Field::ValuePartial(v) => {
                            v.contains(unsafe { std::str::from_utf8_unchecked(field_tmp) })
                        }
                    }
                } else {
                    false
                }
            }
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
                    is_match = if let Some(key) = key {
                        key == depend.key()
                    } else {
                        true
                    } && collection_row == depend.deref();
                    if is_match {
                        break;
                    }
                }
                is_match
            }
            Condition::LastUpdated(_) => true,
        }
    }

    fn temprary_data_match_conditions(&self, row: i64, ent: &TemporaryDataEntity) -> bool {
        for c in self.search.read().unwrap().conditions() {
            if !self.temporary_data_match(row, ent, c) {
                return false;
            }
        }
        true
    }
    pub fn result(
        self,
        database: &Database,
        orders: &Vec<Order>,
    ) -> Result<Vec<i64>, std::sync::mpsc::SendError<RowSet>> {
        let collection_id = self.search.read().unwrap().collection_id();
        if let Some(collection) = database.collection(collection_id) {
            let result = self.search.write().unwrap().result(database);
            if let Some(tmp) = self.session.temporary_data.get(&collection_id) {
                let mut tmp_rows: BTreeSet<i64> = BTreeSet::new();
                if let Some(result) = result.read().unwrap().as_ref() {
                    for row in result.rows() {
                        if let Some(ent) = tmp.get(&(*row as i64)) {
                            if ent.operation != SessionOperation::Delete {
                                if self.temprary_data_match_conditions(*row as i64, ent) {
                                    tmp_rows.insert(*row as i64);
                                }
                            }
                        } else {
                            tmp_rows.insert(*row as i64);
                        }
                    }
                }
                for (row, _) in tmp {
                    //session new data
                    let row = *row;
                    if row < 0 {
                        if let Some(ent) = tmp.get(&(row as i64)) {
                            if ent.operation != SessionOperation::Delete {
                                if self.temprary_data_match_conditions(row, ent) {
                                    tmp_rows.insert(row as i64);
                                }
                            }
                        }
                    }
                }
                let mut new_rows = tmp_rows.into_iter().collect();
                if orders.len() > 0 {
                    super::sort::sort(&mut new_rows, orders, collection, tmp);
                }
                return Ok(new_rows);
            } else {
                if let Some(result) = result.read().unwrap().as_ref() {
                    return Ok(result
                        .sort(database, orders)
                        .into_iter()
                        .map(|x| x as i64)
                        .collect());
                }
            }
        }

        Ok(vec![])
    }
}
