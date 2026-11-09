mod data;
mod operation;
mod relation;
mod search;
mod sequence;
mod sort;
mod temporary_data;

pub use data::SessionData;
pub use operation::{Depends, Pend, SessionOperation, SessionRecord};
pub use search::SessionSearchResult;
pub use sort::{SessionCustomCmp, SessionCustomOrder, SessionOrder, SessionOrderKey};
pub use temporary_data::{TemporaryData, TemporaryDataEntity, TemporaryDataRow};

use semilattice_database::{Collection, FieldName, Fields};

use std::{
    io::Write,
    num::{NonZeroI64, NonZeroU32},
    path::Path,
};

use crate::{CollectionRow, Depend, Field, IdxFile, SessionDatabase};

use relation::SessionRelation;
use sequence::SequenceNumber;
use serde::Serialize;

use self::sequence::SequenceCursor;

#[derive(Serialize)]
pub struct SessionInfo {
    pub(crate) name: String,
    pub(crate) access_at: u64,
    pub(crate) expire: i64,
}

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
        assert!(!name.is_empty());
        if name.is_empty() {
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_sequence_cursor(&mut self, current: usize) {
        if let Some(session_data) = &mut self.session_data {
            session_data.sequence_number.set_current(current);
        }
    }

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
            .truncate(true)
            .open(access)
            .unwrap();
        let expire = expire_interval_sec.unwrap_or(-1);
        file.write_all(&expire.to_be_bytes()).unwrap();

        let mut fields = Fields::new();
        let mut fields_dir = session_dir.to_path_buf();
        fields_dir.push("fields");
        if !fields_dir.exists() {
            std::fs::create_dir_all(&fields_dir).unwrap();
        }
        for p in fields_dir.read_dir().unwrap() {
            let p = p.unwrap();
            let path = p.path();
            if path.is_dir() {
                if let Some(fname) = p.file_name().to_str() {
                    let field = Field::new(path, 1);
                    fields.insert(FieldName::new(fname.into()), field);
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

    /// Converts a row of the main data to [NonZeroU32].
    /// Returns None for rows of the session (negative rows) and out of range rows.
    fn data_row(row: NonZeroI64) -> Option<NonZeroU32> {
        u32::try_from(row.get()).ok().and_then(NonZeroU32::new)
    }

    /// Returns the serial number of the row, or 0 if the row is a session row or does not exist.
    pub fn serial(collection: &Collection, row: NonZeroI64) -> u32 {
        Self::data_row(row)
            .and_then(|row| collection.serial(row))
            .copied()
            .unwrap_or(0)
    }

    /// Returns term_begin of the row, or 0 if it does not exist.
    pub fn term_begin(
        temporary_collection: &TemporaryDataRow,
        collection: &Collection,
        row: NonZeroI64,
    ) -> u64 {
        if row.get() < 0 {
            temporary_collection.get(&row).map_or(0, |e| e.term_begin())
        } else {
            Self::data_row(row)
                .and_then(|row| collection.term_begin(row))
                .copied()
                .unwrap_or(0)
        }
    }

    /// Returns term_end of the row, or 0 if it does not exist.
    pub fn term_end(
        temporary_collection: &TemporaryDataRow,
        collection: &Collection,
        row: NonZeroI64,
    ) -> u64 {
        if row.get() < 0 {
            temporary_collection.get(&row).map_or(0, |e| e.term_end())
        } else {
            Self::data_row(row)
                .and_then(|row| collection.term_end(row))
                .copied()
                .unwrap_or(0)
        }
    }

    /// Returns last_updated of the row, or 0 if the row is a session row or does not exist.
    pub fn last_updated(collection: &Collection, row: NonZeroI64) -> u64 {
        Self::data_row(row)
            .and_then(|row| collection.last_updated(row))
            .copied()
            .unwrap_or(0)
    }

    /// Returns the field value of the row, or an empty slice if it does not exist.
    pub fn field<'a>(
        temporary_collection: &'a TemporaryDataRow,
        collection: &'a Collection,
        row: NonZeroI64,
        field_name: &FieldName,
    ) -> &'a [u8] {
        if row.get() < 0 {
            temporary_collection
                .get(&row)
                .and_then(|e| e.fields().get(field_name))
                .map_or(b"", |v| v)
        } else {
            Self::data_row(row).map_or(b"", |row| collection.field_bytes(row, field_name))
        }
    }
}
