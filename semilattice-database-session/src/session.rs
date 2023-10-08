pub mod search;

mod data;
mod operation;
mod relation;
mod sequence;
mod sort;

pub use data::SessionData;
pub use operation::{Depends, Pend, SessionOperation, SessionRecord};

use std::{
    collections::BTreeSet,
    io::Write,
    num::{NonZeroI32, NonZeroI64, NonZeroU32},
    ops::Deref,
    path::Path,
};

use hashbrown::HashMap;

use crate::{Activity, Collection, CollectionRow, Depend, Field, IdxFile, SessionDatabase};

use relation::SessionRelation;
use search::SessionSearch;
use semilattice_database::{Condition, Database, Order, Search};
use sequence::SequenceNumber;
use serde::Serialize;

use self::sequence::SequenceCursor;

#[derive(Serialize)]
pub struct SessionInfo {
    pub(super) name: String,
    pub(super) access_at: u64,
    pub(super) expire: i64,
}

impl SessionInfo {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn access_at(&self) -> u64 {
        self.access_at
    }
    pub fn expire(&self) -> i64 {
        self.expire
    }
}

#[derive(Debug)]
pub struct TemporaryDataEntity {
    pub(super) activity: Activity,
    pub(super) term_begin: u64,
    pub(super) term_end: u64,
    pub(super) uuid: u128,
    pub(super) operation: SessionOperation,
    pub(super) fields: HashMap<String, Vec<u8>>,
    pub(super) depends: Vec<Depend>,
}
impl TemporaryDataEntity {
    #[inline(always)]
    pub fn activity(&self) -> Activity {
        self.activity
    }

    #[inline(always)]
    pub fn term_begin(&self) -> u64 {
        self.term_begin
    }

    #[inline(always)]
    pub fn term_end(&self) -> u64 {
        self.term_end
    }

    #[inline(always)]
    pub fn uuid(&self) -> u128 {
        self.uuid
    }

    #[inline(always)]
    pub fn uuid_string(&self) -> String {
        semilattice_database::uuid_string(self.uuid)
    }

    #[inline(always)]
    pub fn fields(&self) -> &HashMap<String, Vec<u8>> {
        &self.fields
    }

    #[inline(always)]
    pub fn depends(&self) -> &Vec<Depend> {
        &self.depends
    }
}
pub type TemporaryData = HashMap<NonZeroI32, HashMap<NonZeroI64, TemporaryDataEntity>>;

pub struct Session {
    name: String,
    pub(super) session_data: Option<SessionData>,
    pub(super) temporary_data: TemporaryData,
}
impl Session {
    pub(super) fn new(
        main_database: &SessionDatabase,
        name: impl Into<String>,
        expire_interval_sec: Option<i64>,
    ) -> Self {
        let mut name: String = name.into();
        assert!(name != "");
        if name == "" {
            name = "untitiled".to_owned();
        }
        let session_dir = main_database.session_dir(&name);
        if !session_dir.exists() {
            std::fs::create_dir_all(&session_dir).unwrap();
        }
        let session_data = Self::new_data(&session_dir, expire_interval_sec);
        let temporary_data = session_data.init_temporary_data();
        Self {
            name,
            session_data: Some(session_data),
            temporary_data,
        }
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    pub fn set_sequence_cursor(&mut self, current: usize) {
        if let Some(session_data) = &mut self.session_data {
            session_data.sequence_number.set_current(current);
        }
    }

    #[inline(always)]
    pub fn sequence_cursor(&self) -> Option<SequenceCursor> {
        self.session_data
            .as_ref()
            .map(|session_data| SequenceCursor {
                max: session_data.sequence_number.max(),
                current: session_data.sequence_number.current(),
            })
    }

    pub fn new_data(session_dir: &Path, expire_interval_sec: Option<i64>) -> SessionData {
        let mut access = session_dir.to_path_buf();
        access.push("expire");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(access)
            .unwrap();
        let expire = expire_interval_sec.unwrap_or(-1);
        file.write(&expire.to_be_bytes()).unwrap();

        let mut fields = HashMap::new();
        let mut fields_dir = session_dir.to_path_buf();
        fields_dir.push("fields");
        if !fields_dir.exists() {
            std::fs::create_dir_all(&fields_dir.to_owned()).unwrap();
        }
        for p in fields_dir.read_dir().unwrap() {
            let p = p.unwrap();
            let path = p.path();
            if path.is_dir() {
                if let Some(fname) = p.file_name().to_str() {
                    let field = Field::new(path, 1);
                    fields.insert(fname.to_owned(), field);
                }
            }
        }

        SessionData {
            sequence_number: SequenceNumber::new({
                let mut path = session_dir.to_path_buf();
                path.push("sequence_number.i");
                path
            }),
            sequence: IdxFile::new(
                {
                    let mut path = session_dir.to_path_buf();
                    path.push("sequence.i");
                    path
                },
                1,
            ),
            collection_id: IdxFile::new(
                {
                    let mut path = session_dir.to_path_buf();
                    path.push("collection_id.i");
                    path
                },
                1,
            ),
            row: IdxFile::new(
                {
                    let mut path = session_dir.to_path_buf();
                    path.push("row.i");
                    path
                },
                1,
            ),
            operation: IdxFile::new(
                {
                    let mut path = session_dir.to_path_buf();
                    path.push("operation.i");
                    path
                },
                1,
            ),
            activity: IdxFile::new(
                {
                    let mut path = session_dir.to_path_buf();
                    path.push("activity.i");
                    path
                },
                1,
            ),
            term_begin: IdxFile::new(
                {
                    let mut path = session_dir.to_path_buf();
                    path.push("term_begin.i");
                    path
                },
                1,
            ),
            term_end: IdxFile::new(
                {
                    let mut path = session_dir.to_path_buf();
                    path.push("term_end.i");
                    path
                },
                1,
            ),
            uuid: IdxFile::new(
                {
                    let mut path = session_dir.to_path_buf();
                    path.push("uuid.i");
                    path
                },
                1,
            ),
            fields,
            relation: SessionRelation::new(session_dir, 1),
        }
    }

    #[inline(always)]
    pub fn begin_search(&mut self, collection_id: NonZeroI32) -> SessionSearch {
        self.search(Search::new(collection_id, vec![], HashMap::new()))
    }

    #[inline(always)]
    pub fn search(&mut self, search: Search) -> SessionSearch {
        SessionSearch::new(self, search)
    }

    fn temporary_data_match(
        row: NonZeroI64,
        ent: &TemporaryDataEntity,
        condition: &Condition,
    ) -> bool {
        match condition {
            Condition::Row(cond) => match cond {
                semilattice_database::search::Number::In(c) => c.contains(&(row.get() as isize)),
                semilattice_database::search::Number::Max(c) => row.get() <= *c as i64,
                semilattice_database::search::Number::Min(c) => row.get() >= *c as i64,
                semilattice_database::search::Number::Range(c) => c.contains(&(row.get() as isize)),
            },
            Condition::Uuid(uuid) => uuid.contains(&ent.uuid),
            Condition::Activity(activity) => ent.activity == *activity,
            Condition::Term(cond) => match cond {
                semilattice_database::search::Term::In(c) => {
                    ent.term_begin < *c && (ent.term_end == 0 || ent.term_end > *c)
                }
                semilattice_database::search::Term::Past(c) => ent.term_end >= *c,
                semilattice_database::search::Term::Future(c) => ent.term_begin >= *c,
            },
            Condition::Field(key, cond) => ent.fields.get(key).map_or(false, |f| match cond {
                semilattice_database::search::Field::Match(v) => f == v,
                semilattice_database::search::Field::Range(min, max) => min <= f && max >= f,
                semilattice_database::search::Field::Min(min) => min <= f,
                semilattice_database::search::Field::Max(max) => max >= f,
                semilattice_database::search::Field::Forward(v) => {
                    unsafe { std::str::from_utf8_unchecked(f) }.starts_with(v.as_ref())
                }
                semilattice_database::search::Field::Partial(v) => {
                    unsafe { std::str::from_utf8_unchecked(f) }.contains(v.as_ref())
                }
                semilattice_database::search::Field::Backward(v) => {
                    unsafe { std::str::from_utf8_unchecked(f) }.ends_with(v.as_ref())
                }
                semilattice_database::search::Field::ValueForward(v) => {
                    v.starts_with(unsafe { std::str::from_utf8_unchecked(f) })
                }
                semilattice_database::search::Field::ValueBackward(v) => {
                    v.ends_with(unsafe { std::str::from_utf8_unchecked(f) })
                }
                semilattice_database::search::Field::ValuePartial(v) => {
                    v.contains(unsafe { std::str::from_utf8_unchecked(f) })
                }
            }),
            Condition::Narrow(conditions) => {
                let mut is_match = true;
                for c in conditions {
                    is_match &= Self::temporary_data_match(row, ent, c);
                    if !is_match {
                        break;
                    }
                }
                is_match
            }
            Condition::Wide(conditions) => {
                let mut is_match = false;
                for c in conditions {
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
    fn temprary_data_match_conditions(
        search: &Vec<Condition>,
        row: NonZeroI64,
        ent: &TemporaryDataEntity,
    ) -> bool {
        for c in search {
            if !Self::temporary_data_match(row, ent, c) {
                return false;
            }
        }
        true
    }

    //TODO : Supports join for session data. overwrite result data by session datas.
    pub async fn result_with(
        &mut self,
        search: &mut Search,
        database: &Database,
        orders: &Vec<Order>,
    ) -> Vec<NonZeroI64> {
        let collection_id = search.collection_id();
        if let Some(collection) = database.collection(collection_id) {
            let conditions = search.conditions().clone();
            let result = search.result(database).await;
            if let Some(tmp) = self.temporary_data.get(&collection_id) {
                let mut tmp_rows: BTreeSet<NonZeroI64> = BTreeSet::new();
                if let Some(result) = result.read().unwrap().deref() {
                    for row in result.rows() {
                        let row = NonZeroI64::from(*row);
                        if let Some(ent) = tmp.get(&row) {
                            if ent.operation != SessionOperation::Delete {
                                if Self::temprary_data_match_conditions(&conditions, row, ent) {
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
                                if Self::temprary_data_match_conditions(&conditions, *row, ent) {
                                    tmp_rows.insert(*row);
                                }
                            }
                        }
                    }
                }
                let mut new_rows = tmp_rows.into_iter().collect();
                if orders.len() > 0 {
                    sort::sort(&mut new_rows, orders, collection, tmp);
                }
                return new_rows;
            } else {
                if let Some(result) = result.read().unwrap().deref() {
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

    #[inline(always)]
    pub fn field_bytes<'a>(
        &'a self,
        database: &'a SessionDatabase,
        collection_id: NonZeroI32,
        row: NonZeroI64,
        key: &str,
    ) -> &[u8] {
        if let Some(temporary_collection) = self.temporary_data.get(&collection_id) {
            if let Some(tmp_row) = temporary_collection.get(&row) {
                if let Some(val) = tmp_row.fields.get(key) {
                    return val;
                }
            }
        }
        if row.get() > 0 {
            if let Some(collection) = database.collection(collection_id) {
                return collection.field_bytes(row.try_into().unwrap(), key);
            }
        }
        b""
    }

    #[inline(always)]
    pub fn collection_field_bytes<'a>(
        &'a self,
        collection: &'a Collection,
        row: NonZeroI64,
        key: &str,
    ) -> &[u8] {
        if let Some(temprary_collection) = self.temporary_data.get(&collection.id()) {
            if let Some(temprary_row) = temprary_collection.get(&row) {
                if let Some(val) = temprary_row.fields.get(key) {
                    return val;
                }
            }
        }
        if row.get() > 0 {
            collection.field_bytes(row.try_into().unwrap(), key)
        } else {
            b""
        }
    }

    #[inline(always)]
    pub fn temporary_collection(
        &self,
        collection_id: NonZeroI32,
    ) -> Option<&HashMap<NonZeroI64, TemporaryDataEntity>> {
        self.temporary_data.get(&collection_id)
    }

    #[inline(always)]
    pub fn depends(&self, key: Option<&str>, pend_row: NonZeroU32) -> Option<Vec<Depend>> {
        self.session_data.as_ref().and_then(|session_data| {
            key.map_or_else(
                || {
                    Some(
                        session_data
                            .relation
                            .rows
                            .session_row
                            .iter_by(|v| v.cmp(&pend_row.get()))
                            .filter_map(|relation_row| {
                                if let (Some(key), Some(depend)) = (
                                    session_data.relation.rows.key.value(relation_row),
                                    session_data.relation.rows.depend.value(relation_row),
                                ) {
                                    return Some(Depend::new(
                                        unsafe {
                                            std::str::from_utf8_unchecked(
                                                session_data
                                                    .relation
                                                    .key_names
                                                    .bytes(NonZeroU32::new(*key).unwrap()),
                                            )
                                        },
                                        depend.clone(),
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
                                .iter_by(|v| v.cmp(&pend_row.get()))
                                .filter_map(|relation_row| {
                                    if let (Some(key), Some(depend)) = (
                                        session_data.relation.rows.key.value(relation_row),
                                        session_data.relation.rows.depend.value(relation_row),
                                    ) {
                                        if *key == key_id.get() {
                                            return Some(Depend::new(key_name, depend.clone()));
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
