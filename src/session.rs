use std::{
    collections::HashMap,
    io::{self, Write},
    path::Path,
};
use versatile_data::{Activity, FieldData, IdxSized};

use crate::{Collection, CollectionRow, Condition};

use super::Database;

mod operation;
pub use operation::{Depends, Pend, Record, SessionOperation};

mod sequence_number;
use sequence_number::SequenceNumber;

use serde::Serialize;

mod relation;
pub use relation::SessionDepend;
use relation::SessionRelation;

pub mod search;
use search::SessionSearch;

mod sort;

pub struct SessionSequenceCursor {
    pub max: usize,
    pub current: usize,
}

#[derive(Serialize)]
pub struct SessionInfo {
    pub(super) name: String,
    pub(super) access_at: u64,
    pub(super) expire: i64,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Debug)]
pub struct SessionCollectionRow {
    pub(crate) collection_id: i32,
    pub(crate) row: i64, //-の場合はセッションの行が入る
}
impl SessionCollectionRow {
    pub fn new(collection_id: i32, row: i64) -> Self {
        Self { collection_id, row }
    }
    pub fn collection_id(&self) -> i32 {
        self.collection_id
    }
    pub fn row(&self) -> i64 {
        self.row
    }
}
impl From<CollectionRow> for SessionCollectionRow {
    fn from(item: CollectionRow) -> Self {
        SessionCollectionRow {
            collection_id: item.collection_id(),
            row: item.row() as i64,
        }
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
    pub(super) depends: Vec<SessionDepend>,
}
impl TemporaryDataEntity {
    pub fn activity(&self) -> Activity {
        self.activity
    }
    pub fn term_begin(&self) -> u64 {
        self.term_begin
    }
    pub fn term_end(&self) -> u64 {
        self.term_end
    }
    pub fn uuid(&self) -> u128 {
        self.uuid
    }
    pub fn uuid_string(&self) -> String {
        versatile_data::uuid_string(self.uuid)
    }
    pub fn fields(&self) -> &HashMap<String, Vec<u8>> {
        &self.fields
    }
}
pub type TemporaryData = HashMap<i32, HashMap<i64, TemporaryDataEntity>>;

pub struct SessionData {
    pub(super) sequence_number: SequenceNumber,
    pub(super) sequence: IdxSized<usize>,
    pub(super) collection_id: IdxSized<i32>,
    pub(super) row: IdxSized<i64>,
    pub(super) operation: IdxSized<SessionOperation>,
    pub(super) activity: IdxSized<u8>,
    pub(super) term_begin: IdxSized<u64>,
    pub(super) term_end: IdxSized<u64>,
    pub(super) uuid: IdxSized<u128>,
    pub(super) fields: HashMap<String, FieldData>,
    pub(super) relation: SessionRelation,
}

pub struct Session {
    name: String,
    pub(super) session_data: Option<SessionData>,
    pub(super) temporary_data: TemporaryData,
}
impl Session {
    pub fn new(
        main_database: &Database,
        name: impl Into<String>,
        expire_interval_sec: Option<i64>,
    ) -> io::Result<Self> {
        let mut name: String = name.into();
        assert!(name != "");
        if name == "" {
            name = "untitiled".to_owned();
        }
        let session_dir = main_database.session_dir(&name);
        if !session_dir.exists() {
            std::fs::create_dir_all(&session_dir)?;
        }
        let session_data = Self::new_data(&session_dir, expire_interval_sec)?;
        let temporary_data = Self::init_temporary_data(&session_data)?;
        Ok(Self {
            name,
            session_data: Some(session_data),
            temporary_data,
        })
    }
    pub fn name(&mut self) -> &str {
        &self.name
    }
    pub fn set_sequence_cursor(&mut self, current: usize) {
        if let Some(session_data) = &mut self.session_data {
            session_data.sequence_number.set_current(current);
        }
    }
    pub fn sequence_cursor(&self) -> Option<SessionSequenceCursor> {
        if let Some(session_data) = &self.session_data {
            Some(SessionSequenceCursor {
                max: session_data.sequence_number.max(),
                current: session_data.sequence_number.current(),
            })
        } else {
            None
        }
    }
    pub(super) fn init_temporary_data(session_data: &SessionData) -> io::Result<TemporaryData> {
        let mut temporary_data = HashMap::new();
        let current = session_data.sequence_number.current();
        if current > 0 {
            let mut fields_overlaps: HashMap<SessionCollectionRow, HashMap<String, Vec<u8>>> =
                HashMap::new();
            for sequence in 1..=current {
                for session_row in session_data.sequence.select_by_value(&sequence) {
                    if let Some(collection_id) = session_data.collection_id.value(session_row) {
                        if collection_id > 0 {
                            let col = temporary_data
                                .entry(collection_id)
                                .or_insert(HashMap::new());
                            let row = session_data.row.value(session_row).unwrap();

                            let temporary_row = if row == 0 { -(session_row as i64) } else { row };

                            let operation = session_data.operation.value(session_row).unwrap();
                            if operation == SessionOperation::Delete {
                                col.insert(
                                    temporary_row,
                                    TemporaryDataEntity {
                                        activity: Activity::Inactive,
                                        term_begin: 0,
                                        term_end: 0,
                                        uuid: 0,
                                        operation,
                                        fields: HashMap::new(),
                                        depends: vec![],
                                    },
                                );
                            } else {
                                let row_fields = fields_overlaps
                                    .entry(SessionCollectionRow::new(collection_id, temporary_row))
                                    .or_insert(HashMap::new());
                                for (key, val) in &session_data.fields {
                                    if let Some(v) = val.get(session_row) {
                                        row_fields.insert(key.to_string(), v.to_vec());
                                    }
                                }
                                col.insert(
                                    temporary_row,
                                    TemporaryDataEntity {
                                        activity: if session_data
                                            .activity
                                            .value(session_row)
                                            .unwrap()
                                            == 1
                                        {
                                            Activity::Active
                                        } else {
                                            Activity::Inactive
                                        },
                                        term_begin: session_data
                                            .term_begin
                                            .value(session_row)
                                            .unwrap(),
                                        term_end: session_data.term_end.value(session_row).unwrap(),
                                        uuid: if let Some(uuid) =
                                            session_data.uuid.value(session_row)
                                        {
                                            uuid
                                        } else {
                                            0
                                        },
                                        operation,
                                        fields: row_fields.clone(),
                                        depends: {
                                            let mut depends = vec![];
                                            for relation_row in session_data
                                                .relation
                                                .rows
                                                .session_row
                                                .select_by_value(&session_row)
                                                .iter()
                                            {
                                                if let (Some(key), Some(depend)) = (
                                                    session_data
                                                        .relation
                                                        .rows
                                                        .key
                                                        .value(*relation_row),
                                                    session_data
                                                        .relation
                                                        .rows
                                                        .depend
                                                        .value(*relation_row),
                                                ) {
                                                    if let Ok(key_name) = unsafe {
                                                        session_data.relation.key_names.str(key)
                                                    } {
                                                        depends.push(SessionDepend::new(
                                                            key_name, depend,
                                                        ));
                                                    }
                                                }
                                            }
                                            depends
                                        },
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(temporary_data)
    }
    pub fn new_data(
        session_dir: &Path,
        expire_interval_sec: Option<i64>,
    ) -> io::Result<SessionData> {
        let mut access = session_dir.to_path_buf();
        access.push("expire");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(access)?;
        let expire = if let Some(expire) = expire_interval_sec {
            expire
        } else {
            -1
        };
        file.write(&expire.to_be_bytes())?;

        let mut fields = HashMap::new();
        let mut fields_dir = session_dir.to_path_buf();
        fields_dir.push("fields");
        if !fields_dir.exists() {
            std::fs::create_dir_all(&fields_dir.to_owned())?;
        }
        for p in fields_dir.read_dir()? {
            let p = p?;
            let path = p.path();
            if path.is_dir() {
                if let Some(fname) = p.file_name().to_str() {
                    let field = FieldData::new(path)?;
                    fields.insert(fname.to_owned(), field);
                }
            }
        }

        Ok(SessionData {
            sequence_number: SequenceNumber::new({
                let mut path = session_dir.to_path_buf();
                path.push("sequence_number.i");
                path
            })?,
            sequence: IdxSized::new({
                let mut path = session_dir.to_path_buf();
                path.push("sequence.i");
                path
            })?,
            collection_id: IdxSized::new({
                let mut path = session_dir.to_path_buf();
                path.push("collection_id.i");
                path
            })?,
            row: IdxSized::new({
                let mut path = session_dir.to_path_buf();
                path.push("row.i");
                path
            })?,
            operation: IdxSized::new({
                let mut path = session_dir.to_path_buf();
                path.push("operation.i");
                path
            })?,
            activity: IdxSized::new({
                let mut path = session_dir.to_path_buf();
                path.push("activity.i");
                path
            })?,
            term_begin: IdxSized::new({
                let mut path = session_dir.to_path_buf();
                path.push("term_begin.i");
                path
            })?,
            term_end: IdxSized::new({
                let mut path = session_dir.to_path_buf();
                path.push("term_end.i");
                path
            })?,
            uuid: IdxSized::new({
                let mut path = session_dir.to_path_buf();
                path.push("uuid.i");
                path
            })?,
            fields,
            relation: SessionRelation::new(session_dir)?,
        })
    }

    pub fn begin_search(&self, collection_id: i32) -> SessionSearch {
        SessionSearch::new(self, collection_id)
    }
    pub fn search(&self, collection_id: i32, condtions: &Vec<Condition>) -> SessionSearch {
        let mut search = SessionSearch::new(self, collection_id);
        for c in condtions {
            search = search.search(c.clone());
        }
        search
    }

    pub fn field_bytes<'a>(
        &'a self,
        database: &'a Database,
        collection_id: i32,
        row: i64,
        key: &str,
    ) -> &[u8] {
        if let Some(tmp_col) = self.temporary_data.get(&collection_id) {
            if let Some(tmp_row) = tmp_col.get(&row) {
                if let Some(val) = tmp_row.fields.get(key) {
                    return val;
                }
            }
        }
        if row > 0 {
            if let Some(col) = database.collection(collection_id) {
                return col.field_bytes(row as u32, key);
            }
        }
        b""
    }

    pub fn collection_field_bytes<'a>(
        &'a self,
        collection: &'a Collection,
        row: i64,
        key: &str,
    ) -> &[u8] {
        if let Some(tmp_col) = self.temporary_data.get(&collection.id()) {
            if let Some(tmp_row) = tmp_col.get(&row) {
                if let Some(val) = tmp_row.fields.get(key) {
                    return val;
                }
            }
        }
        if row > 0 {
            return collection.field_bytes(row as u32, key);
        }
        b""
    }
    pub fn temporary_collection(
        &self,
        collection_id: i32,
    ) -> Option<&HashMap<i64, TemporaryDataEntity>> {
        self.temporary_data.get(&collection_id)
    }

    pub fn depends(&self, key: Option<&str>, pend_row: u32) -> Option<Vec<SessionDepend>> {
        let mut r = vec![];
        if let Some(ref session_data) = self.session_data {
            if let Some(key_name) = key {
                if let Some(key_id) = session_data
                    .relation
                    .key_names
                    .find_row(key_name.as_bytes())
                {
                    for relation_row in session_data
                        .relation
                        .rows
                        .session_row
                        .select_by_value(&pend_row)
                        .iter()
                    {
                        if let (Some(key), Some(depend)) = (
                            session_data.relation.rows.key.value(*relation_row),
                            session_data.relation.rows.depend.value(*relation_row),
                        ) {
                            if key == key_id {
                                r.push(SessionDepend::new(key_name, depend));
                            }
                        }
                    }
                    return Some(r);
                }
            } else {
                for relation_row in session_data
                    .relation
                    .rows
                    .session_row
                    .select_by_value(&pend_row)
                    .iter()
                {
                    if let (Some(key), Some(depend)) = (
                        session_data.relation.rows.key.value(*relation_row),
                        session_data.relation.rows.depend.value(*relation_row),
                    ) {
                        r.push(SessionDepend::new(
                            unsafe { session_data.relation.key_names.str(key) }.unwrap(),
                            depend,
                        ));
                    }
                }
                return Some(r);
            }
        }
        None
    }
}
