use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};
use versatile_data::{Order, RowSet};

use super::{Session, SessionOperation, TemporaryDataEntity};
use crate::{search, Activity, Condition, Database};

pub struct SessionSearch<'a> {
    session: &'a Session,
    collection_id: i32,
    conditions: Vec<Condition>,
}
impl<'a> SessionSearch<'a> {
    pub fn new(session: &'a Session, collection_id: i32) -> Self {
        Self {
            session,
            collection_id,
            conditions: Vec::new(),
        }
    }
    pub fn search_default(mut self) -> Self {
        self.conditions.push(Condition::Term(search::Term::In(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
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

    pub fn search(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }

    fn temporary_data_match(
        &self,
        row: i64,
        ent: &TemporaryDataEntity,
        condition: &Condition,
    ) -> bool {
        let mut is_match = true;
        match condition {
            Condition::Row(cond) => {
                is_match = match cond {
                    search::Number::In(c) => c.contains(&(row as isize)),
                    search::Number::Max(c) => row <= *c as i64,
                    search::Number::Min(c) => row >= *c as i64,
                    search::Number::Range(c) => c.contains(&(row as isize)),
                }
            }
            Condition::Activity(activity) => {
                if ent.activity != *activity {
                    is_match = false;
                }
            }
            Condition::Term(cond) => match cond {
                search::Term::In(c) => {
                    if !(ent.term_begin < *c && (ent.term_end == 0 || ent.term_end > *c)) {
                        is_match = false;
                    }
                }
                search::Term::Past(c) => {
                    if ent.term_end > *c {
                        is_match = false;
                    }
                }
                search::Term::Future(c) => {
                    if ent.term_begin < *c {
                        is_match = false;
                    }
                }
            },
            Condition::Field(key, cond) => {
                if let Some(field_tmp) = ent.fields.get(key) {
                    match cond {
                        search::Field::Match(v) => {
                            if field_tmp != v {
                                is_match = false;
                            }
                        }
                        search::Field::Range(min, max) => {
                            if min < field_tmp || max > field_tmp {
                                is_match = false;
                            }
                        }
                        search::Field::Min(min) => {
                            if min < field_tmp {
                                is_match = false;
                            }
                        }
                        search::Field::Max(max) => {
                            if max > field_tmp {
                                is_match = false;
                            }
                        }
                        search::Field::Forward(v) => {
                            if let Ok(str) = std::str::from_utf8(field_tmp) {
                                if !str.starts_with(v) {
                                    is_match = false;
                                }
                            } else {
                                is_match = false;
                            }
                        }
                        search::Field::Partial(v) => {
                            if let Ok(str) = std::str::from_utf8(field_tmp) {
                                if !str.contains(v) {
                                    is_match = false;
                                }
                            } else {
                                is_match = false;
                            }
                        }
                        search::Field::Backward(v) => {
                            if let Ok(str) = std::str::from_utf8(field_tmp) {
                                if !str.ends_with(v) {
                                    is_match = false;
                                }
                            } else {
                                is_match = false;
                            }
                        }
                    }
                } else {
                    is_match = false;
                }
            }
            Condition::Narrow(conditions) => {
                is_match = true;
                for c in conditions {
                    is_match &= self.temporary_data_match(row, ent, c);
                    if !is_match {
                        break;
                    }
                }
            }
            Condition::Wide(conditions) => {
                is_match = false;
                for c in conditions {
                    is_match |= self.temporary_data_match(row, ent, c);
                    if is_match {
                        break;
                    }
                }
            }
            Condition::Depend(condition) => {
                for depend in &ent.depends {
                    is_match = *condition == *depend
                }
            }
            Condition::LastUpdated(_) => {}
            Condition::Uuid(_) => {}
        }
        is_match
    }

    fn temprary_data_match_conditions(&self, row: i64, ent: &TemporaryDataEntity) -> bool {
        let mut is_match = true;
        for c in &self.conditions {
            is_match = self.temporary_data_match(row, ent, c);
            if !is_match {
                break;
            }
        }
        is_match
    }
    pub(crate) fn result(
        self,
        database: &Database,
        orders: Vec<Order>,
    ) -> Result<Vec<i64>, std::sync::mpsc::SendError<RowSet>> {
        if let Some(collection) = database.collection(self.collection_id) {
            let mut search = database.search(collection);
            for c in &self.conditions {
                search = search.search(c.clone());
            }
            let r = database.result(search, &orders)?;
            if let Some(tmp) = self.session.temporary_data.get(&self.collection_id) {
                let mut tmp_rows: BTreeSet<i64> = BTreeSet::new();
                for row in r {
                    if let Some(ent) = tmp.get(&(row as i64)) {
                        if ent.operation != SessionOperation::Delete {
                            if self.temprary_data_match_conditions(row as i64, ent) {
                                tmp_rows.insert(row as i64);
                            }
                        }
                    } else {
                        tmp_rows.insert(row as i64);
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
                Ok(new_rows)
            } else {
                Ok(r.into_iter().map(|x| x as i64).collect())
            }
        } else {
            Ok(vec![])
        }
    }
}
