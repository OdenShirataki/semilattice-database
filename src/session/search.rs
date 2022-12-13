use std::collections::BTreeSet;

use super::{Session, TemporaryDataEntity};
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
            chrono::Local::now().timestamp(),
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

    fn temporary_data_match(ent: &TemporaryDataEntity, condition: &Condition) -> bool {
        let mut is_match = true;
        match condition {
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
                    is_match &= Self::temporary_data_match(ent, c);
                    if !is_match {
                        break;
                    }
                }
            }
            Condition::Wide(conditions) => {
                is_match = false;
                for c in conditions {
                    is_match |= Self::temporary_data_match(ent, c);
                    if is_match {
                        break;
                    }
                }
            }
            Condition::Depend(_) => {}
            Condition::Row(_) => {}
            Condition::LastUpdated(_) => {}
            Condition::Uuid(_) => {}
        }
        is_match
    }

    pub(crate) fn result(self, database: &Database) -> BTreeSet<i64> {
        let mut new_rows: BTreeSet<i64> = BTreeSet::new();
        if let Some(collection) = database.collection(self.collection_id) {
            let mut search = database.search(collection);
            for c in &self.conditions {
                search = search.search(c.clone());
            }
            let r = database.result(search);
            if let Some(tmp) = self.session.temporary_data.get(&self.collection_id) {
                for row in r {
                    if let Some(ent) = tmp.get(&(row as i64)) {
                        let mut is_match = true;
                        for c in &self.conditions {
                            is_match = Self::temporary_data_match(ent, c);
                            if !is_match {
                                break;
                            }
                        }
                        if is_match {
                            new_rows.insert(row as i64);
                        }
                    } else {
                        new_rows.insert(row as i64);
                    }
                }
                for (row, _) in tmp {
                    //セッション中に新規作成されたデータ
                    let row = *row;
                    if row < 0 {
                        new_rows.insert(row);
                    }
                }
            }
        }
        new_rows
    }
}
