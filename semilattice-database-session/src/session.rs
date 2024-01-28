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
use semilattice_database::Fields;
pub use sort::{SessionCustomOrder, SessionOrder, SessionOrderKey};
pub use temporary_data::{TemporaryData, TemporaryDataEntity};

use std::{io::Write, path::Path};

use crate::{CollectionRow, Depend, Field, IdxFile, SessionDatabase};

use relation::SessionRelation;
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
            .open(access)
            .unwrap();
        let expire = expire_interval_sec.unwrap_or(-1);
        file.write(&expire.to_be_bytes()).unwrap();

        let mut fields = Fields::new();
        let mut fields_dir = session_dir.to_path_buf();
        fields_dir.push("fields");
        if !fields_dir.exists() {
            std::fs::create_dir_all(&fields_dir.to_owned()).unwrap();
        }
        for p in fields_dir.read_dir().unwrap().into_iter() {
            let p = p.unwrap();
            let path = p.path();
            if path.is_dir() {
                if let Some(fname) = p.file_name().to_str() {
                    let field = Field::new(path, 1);
                    fields.insert(fname.into(), field);
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
}
