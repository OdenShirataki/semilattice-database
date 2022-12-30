use std::{collections::HashMap, io, path::Path};
use versatile_data::{Activity, FieldData, IdxSized};

use crate::{Collection, CollectionRow, Condition};

use super::Database;

mod operation;
pub use operation::{Depends, Pend, Record, SessionOperation};

mod sequence_number;
use sequence_number::SequenceNumber;

mod relation;
pub use relation::SessionDepend;
use relation::SessionRelation;

pub mod search;
use search::SessionSearch;

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
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
pub struct TemporaryDataEntity {
    pub(super) activity: Activity,
    pub(super) term_begin: i64,
    pub(super) term_end: i64,
    pub(super) fields: HashMap<String, Vec<u8>>,
}
impl TemporaryDataEntity {
    pub fn activity(&self) -> Activity {
        self.activity
    }
    pub fn term_begin(&self) -> i64 {
        self.term_begin
    }
    pub fn term_end(&self) -> i64 {
        self.term_end
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
    pub(super) term_begin: IdxSized<i64>,
    pub(super) term_end: IdxSized<i64>,
    pub(super) fields: HashMap<String, FieldData>,
    pub(super) relation: SessionRelation,
}
pub struct Session {
    name: String,
    pub(super) session_data: Option<SessionData>,
    pub(super) temporary_data: TemporaryData,
}
impl Session {
    pub fn new(main_database: &Database, name: impl Into<String>) -> io::Result<Self> {
        let mut name: String = name.into();
        assert!(name != "");
        if name == "" {
            name = "untitiled".to_owned();
        }
        let session_dir = main_database.session_dir(&name);
        if !std::path::Path::new(&session_dir).exists() {
            std::fs::create_dir_all(&session_dir)?;
        }
        let session_data = Self::new_data(&session_dir)?;
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
    fn init_temporary_data(session_data: &SessionData) -> io::Result<TemporaryData> {
        let mut temporary_data = HashMap::new();
        for session_row in 1..session_data.sequence.max_rows()? {
            let collection_id = session_data.collection_id.value(session_row).unwrap();
            if collection_id > 0 {
                let col = temporary_data
                    .entry(collection_id)
                    .or_insert(HashMap::new());
                let row = session_data.row.value(session_row).unwrap();

                let temporary_row: i64 = if row == 0 {
                    -(session_row as i64)
                } else {
                    row as i64
                };
                let mut fields = HashMap::new();
                for (key, val) in &session_data.fields {
                    if let Some(v) = val.get(session_row) {
                        fields.insert(key.to_string(), v.to_vec());
                    }
                }
                col.insert(
                    temporary_row,
                    TemporaryDataEntity {
                        activity: if session_data.activity.value(session_row).unwrap() == 1 {
                            Activity::Active
                        } else {
                            Activity::Inactive
                        },
                        term_begin: session_data.term_begin.value(session_row).unwrap(),
                        term_end: session_data.term_end.value(session_row).unwrap(),
                        fields,
                    },
                );
            }
        }
        Ok(temporary_data)
    }
    pub fn new_data(session_dir: &Path) -> io::Result<SessionData> {
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
                path.push("sequece_number.i");
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
