pub mod search;

mod data;
mod operation;
mod relation;
mod sequence;
mod sort;

pub use data::SessionData;
pub use operation::{Depends, Pend, SessionOperation, SessionRecord};

use std::{
    io::Write,
    path::Path,
    sync::{Arc, RwLock},
};

use hashbrown::HashMap;

use crate::{Activity, Collection, CollectionRow, Depend, Field, IdxFile, SessionDatabase};

use relation::SessionRelation;
use search::SessionSearch;
use semilattice_database::Search;
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
pub type TemporaryData = HashMap<i32, HashMap<i64, TemporaryDataEntity>>;

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
                    let field = Field::new(path);
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
            sequence: IdxFile::new({
                let mut path = session_dir.to_path_buf();
                path.push("sequence.i");
                path
            }),
            collection_id: IdxFile::new({
                let mut path = session_dir.to_path_buf();
                path.push("collection_id.i");
                path
            }),
            row: IdxFile::new({
                let mut path = session_dir.to_path_buf();
                path.push("row.i");
                path
            }),
            operation: IdxFile::new({
                let mut path = session_dir.to_path_buf();
                path.push("operation.i");
                path
            }),
            activity: IdxFile::new({
                let mut path = session_dir.to_path_buf();
                path.push("activity.i");
                path
            }),
            term_begin: IdxFile::new({
                let mut path = session_dir.to_path_buf();
                path.push("term_begin.i");
                path
            }),
            term_end: IdxFile::new({
                let mut path = session_dir.to_path_buf();
                path.push("term_end.i");
                path
            }),
            uuid: IdxFile::new({
                let mut path = session_dir.to_path_buf();
                path.push("uuid.i");
                path
            }),
            fields,
            relation: SessionRelation::new(session_dir),
        }
    }

    #[inline(always)]
    pub fn begin_search(&self, collection_id: i32) -> SessionSearch {
        self.search(&Arc::new(RwLock::new(Search::new(
            collection_id,
            vec![],
            HashMap::new(),
        ))))
    }

    #[inline(always)]
    pub fn search(&self, search: &Arc<RwLock<Search>>) -> SessionSearch {
        SessionSearch::new(self, Arc::clone(search))
    }

    #[inline(always)]
    pub fn field_bytes<'a>(
        &'a self,
        database: &'a SessionDatabase,
        collection_id: i32,
        row: i64,
        key: &str,
    ) -> &[u8] {
        if let Some(temporary_collection) = self.temporary_data.get(&collection_id) {
            if let Some(tmp_row) = temporary_collection.get(&row) {
                if let Some(val) = tmp_row.fields.get(key) {
                    return val;
                }
            }
        }
        if row > 0 {
            if let Some(collection) = database.collection(collection_id) {
                return collection.field_bytes(row as u32, key);
            }
        }
        b""
    }

    #[inline(always)]
    pub fn collection_field_bytes<'a>(
        &'a self,
        collection: &'a Collection,
        row: i64,
        key: &str,
    ) -> &[u8] {
        if let Some(temprary_collection) = self.temporary_data.get(&collection.id()) {
            if let Some(temprary_row) = temprary_collection.get(&row) {
                if let Some(val) = temprary_row.fields.get(key) {
                    return val;
                }
            }
        }
        if row > 0 {
            collection.field_bytes(row as u32, key)
        } else {
            b""
        }
    }

    #[inline(always)]
    pub fn temporary_collection(
        &self,
        collection_id: i32,
    ) -> Option<&HashMap<i64, TemporaryDataEntity>> {
        self.temporary_data.get(&collection_id)
    }

    #[inline(always)]
    pub fn depends(&self, key: Option<&str>, pend_row: u32) -> Option<Vec<Depend>> {
        self.session_data.as_ref().and_then(|session_data| {
            key.map_or_else(
                || {
                    Some(
                        session_data
                            .relation
                            .rows
                            .session_row
                            .iter_by(|v| v.cmp(&pend_row))
                            .filter_map(|relation_row| {
                                if let (Some(key), Some(depend)) = (
                                    session_data.relation.rows.key.value(relation_row.get()),
                                    session_data.relation.rows.depend.value(relation_row.get()),
                                ) {
                                    return Some(Depend::new(
                                        unsafe {
                                            std::str::from_utf8_unchecked(
                                                session_data.relation.key_names.bytes(*key),
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
                                .iter_by(|v| v.cmp(&pend_row))
                                .filter_map(|relation_row| {
                                    if let (Some(key), Some(depend)) = (
                                        session_data.relation.rows.key.value(relation_row.get()),
                                        session_data.relation.rows.depend.value(relation_row.get()),
                                    ) {
                                        if *key == key_id {
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
