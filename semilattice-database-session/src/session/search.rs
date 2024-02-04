use std::{
    collections::BTreeSet,
    num::{NonZeroI32, NonZeroI64, NonZeroU32},
    sync::Arc,
};

use async_recursion::async_recursion;
use hashbrown::HashMap;
use semilattice_database::{search, Collection, Condition, Depend, FieldName, SearchResult};

use crate::{Session, SessionDatabase};

use super::{SessionOperation, TemporaryDataEntity};

#[derive(Debug, Clone, PartialEq)]
pub struct SessionSearchResult {
    collection_id: i32,
    rows: BTreeSet<NonZeroI64>,
    join: HashMap<Arc<String>, HashMap<NonZeroI64, SessionSearchResult>>,
}

impl SessionSearchResult {
    pub fn rows(&self) -> &BTreeSet<NonZeroI64> {
        &self.rows
    }

    pub fn join(&self) -> &HashMap<Arc<String>, HashMap<NonZeroI64, SessionSearchResult>> {
        &self.join
    }
}

impl Session {
    fn temporary_data_match(
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
            Condition::Field(field_id, cond) => {
                ent.fields.get(field_id).map_or(false, |f| match cond {
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
                })
            }
            Condition::Narrow(conditions) => {
                let mut is_match = true;
                for c in conditions.into_iter() {
                    is_match &= Self::temporary_data_match(row, ent, c);
                    if !is_match {
                        break;
                    }
                }
                is_match
            }
            Condition::Wide(conditions) => {
                let mut is_match = false;
                for c in conditions.into_iter() {
                    is_match |= Self::temporary_data_match(row, ent, c);
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
                        && collection_row == &**depend;
                    if is_match {
                        break;
                    }
                }
                is_match
            }
            Condition::LastUpdated(_) => true,
        }
    }

    fn temprary_data_match_conditions(
        conditions: &Vec<Condition>,
        row: NonZeroI64,
        ent: &TemporaryDataEntity,
    ) -> bool {
        for c in conditions.into_iter() {
            if !Self::temporary_data_match(row, ent, c) {
                return false;
            }
        }
        true
    }

    #[async_recursion(?Send)]
    async fn join(
        &self,
        join_result: &HashMap<Arc<String>, HashMap<NonZeroU32, SearchResult>>,
    ) -> HashMap<Arc<String>, HashMap<NonZeroI64, SessionSearchResult>> {
        let mut map = HashMap::new();
        for (name, row_join) in join_result {
            let mut map_inner = HashMap::new();
            for (row, result) in row_join {
                let result = self.result_with(result).await;
                map_inner.insert((*row).into(), result);
            }
            map.insert(name.to_owned(), map_inner);
        }
        map
    }

    pub async fn result_with(&self, search_result: &SearchResult) -> SessionSearchResult {
        let (collection_id, rows) = if let Some(search) = search_result.search() {
            let collection_id = search.collection_id();
            (
                collection_id.get(),
                if let Some(tmp) = self.temporary_data.get(&collection_id) {
                    let mut rows: BTreeSet<NonZeroI64> = BTreeSet::new();
                    for row in search_result.rows().into_iter() {
                        let row = NonZeroI64::from(*row);
                        if let Some(ent) = tmp.get(&row) {
                            if ent.operation != SessionOperation::Delete {
                                if Self::temprary_data_match_conditions(
                                    search.conditions(),
                                    row,
                                    ent,
                                ) {
                                    rows.insert(row);
                                }
                            }
                        } else {
                            rows.insert(row);
                        }
                    }
                    for (row, _) in tmp.into_iter() {
                        if row.get() < 0 {
                            if let Some(ent) = tmp.get(row) {
                                if ent.operation != SessionOperation::Delete {
                                    if Self::temprary_data_match_conditions(
                                        search.conditions(),
                                        *row,
                                        ent,
                                    ) {
                                        rows.insert(*row);
                                    }
                                }
                            }
                        }
                    }
                    rows
                } else {
                    search_result
                        .rows()
                        .into_iter()
                        .map(|x| NonZeroI64::from(*x))
                        .collect()
                },
            )
        } else {
            (0, BTreeSet::new())
        };
        let join = self.join(search_result.join()).await;

        SessionSearchResult {
            collection_id,
            rows,
            join,
        }
    }

    pub fn field_bytes<'a>(
        &'a self,
        database: &'a SessionDatabase,
        collection_id: NonZeroI32,
        row: NonZeroI64,
        field_name: &FieldName,
    ) -> &[u8] {
        if let Some(temporary_collection) = self.temporary_data.get(&collection_id) {
            if let Some(tmp_row) = temporary_collection.get(&row) {
                if let Some(val) = tmp_row.fields.get(field_name) {
                    return val;
                }
            }
        }
        if row.get() > 0 {
            if let Some(collection) = database.collection(collection_id) {
                return collection.field_bytes(row.try_into().unwrap(), field_name);
            }
        }
        b""
    }

    pub fn collection_field_bytes<'a>(
        &'a self,
        collection: &'a Collection,
        row: NonZeroI64,
        field_name: &FieldName,
    ) -> &[u8] {
        if let Some(temprary_collection) = self.temporary_data.get(&collection.id()) {
            if let Some(temprary_row) = temprary_collection.get(&row) {
                if let Some(val) = temprary_row.fields.get(field_name) {
                    return val;
                }
            }
        }
        if row.get() > 0 {
            collection.field_bytes(row.try_into().unwrap(), field_name)
        } else {
            b""
        }
    }

    pub fn temporary_collection(
        &self,
        collection_id: NonZeroI32,
    ) -> Option<&HashMap<NonZeroI64, TemporaryDataEntity>> {
        self.temporary_data.get(&collection_id)
    }

    pub fn depends(&self, key: Option<Arc<String>>, pend_row: NonZeroU32) -> Option<Vec<Depend>> {
        self.session_data.as_ref().and_then(move |session_data| {
            key.map_or_else(
                || {
                    Some(
                        session_data
                            .relation
                            .rows
                            .session_row
                            .iter_by(&pend_row.get())
                            .filter_map(|relation_row| {
                                if let (Some(key), Some(depend)) = (
                                    session_data.relation.rows.key.get(relation_row),
                                    session_data.relation.rows.depend.get(relation_row),
                                ) {
                                    return Some(Depend::new(
                                        Arc::new(
                                            unsafe {
                                                std::str::from_utf8_unchecked(
                                                    session_data
                                                        .relation
                                                        .key_names
                                                        .bytes(NonZeroU32::new(**key).unwrap())
                                                        .unwrap(),
                                                )
                                            }
                                            .into(),
                                        ),
                                        (**depend).clone(),
                                    ));
                                }
                                None
                            })
                            .collect(),
                    )
                },
                |key_name| {
                    session_data
                        .relation
                        .key_names
                        .row(key_name.as_bytes())
                        .map(|key_id| {
                            session_data
                                .relation
                                .rows
                                .session_row
                                .iter_by(&pend_row.get())
                                .filter_map(|relation_row| {
                                    if let (Some(key), Some(depend)) = (
                                        session_data.relation.rows.key.get(relation_row),
                                        session_data.relation.rows.depend.get(relation_row),
                                    ) {
                                        if **key == key_id.get() {
                                            return Some(Depend::new(
                                                Arc::clone(&key_name),
                                                (**depend).clone(),
                                            ));
                                        }
                                    }
                                    None
                                })
                                .collect()
                        })
                },
            )
        })
    }
}
