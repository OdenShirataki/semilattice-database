mod commit;
mod session;
mod update;

pub use semilattice_database::{
    search, Activity, Collection, CollectionRow, Condition, CustomOrderKey, CustomSort, DataOption,
    Depend, FieldName, Order, OrderKey, SearchResult, Term, Uuid,
};
pub use session::{
    Depends, Pend, Session, SessionCustomOrder, SessionOrder, SessionOrderKey, SessionRecord,
    SessionSearchResult,
};

use std::{
    io::Read,
    num::{NonZeroI32, NonZeroI64, NonZeroU32},
    path::PathBuf,
    sync::Arc,
    time::{self, UNIX_EPOCH},
};

use hashbrown::HashMap;
use semilattice_database::{idx_binary, BinarySet, Database, Field, FileMmap, IdxFile};
use session::SessionInfo;

pub struct SessionDatabase {
    database: Database,
    sessions_dir: PathBuf,
}

impl std::ops::Deref for SessionDatabase {
    type Target = Database;
    fn deref(&self) -> &Self::Target {
        &self.database
    }
}
impl std::ops::DerefMut for SessionDatabase {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.database
    }
}

impl SessionDatabase {
    pub fn new(
        dir: PathBuf,
        collection_settings: Option<std::collections::HashMap<String, DataOption>>,
        relation_reserve_unit: u32,
    ) -> Self {
        let database = Database::new(dir.clone(), collection_settings, relation_reserve_unit);
        let mut sessions_dir = dir.to_path_buf();
        sessions_dir.push("sessions");
        Self {
            database,
            sessions_dir,
        }
    }
    pub fn sessions(&self) -> Vec<SessionInfo> {
        let mut sessions = Vec::new();
        if self.sessions_dir.exists() {
            let dir = self.sessions_dir.read_dir().unwrap();
            for d in dir.into_iter() {
                let d = d.unwrap();
                if d.file_type().unwrap().is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        let mut access_at = 0;
                        let mut expire = 0;

                        let mut expire_file = d.path().to_path_buf();
                        expire_file.push("expire");
                        if expire_file.exists() {
                            if let Ok(md) = expire_file.metadata() {
                                if let Ok(m) = md.modified() {
                                    access_at = m.duration_since(UNIX_EPOCH).unwrap().as_secs();
                                    let mut file = std::fs::File::open(expire_file).unwrap();
                                    let mut buf = [0u8; 8];
                                    file.read(&mut buf).unwrap();
                                    expire = i64::from_be_bytes(buf);
                                }
                            }
                        }
                        sessions.push(SessionInfo {
                            name: fname.to_owned(),
                            access_at,
                            expire,
                        });
                    }
                }
            }
        }
        sessions
    }
    pub fn session_gc(&self, default_expire_interval_sec: i64) {
        for session in self.sessions().into_iter() {
            let expire = if session.expire < 0 {
                default_expire_interval_sec
            } else {
                session.expire
            };
            if session.access_at
                < (time::SystemTime::now() - time::Duration::new(expire as u64, 0))
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            {
                let mut path = self.sessions_dir.clone();
                path.push(session.name);
                std::fs::remove_dir_all(&path).unwrap();
            }
        }
    }

    pub fn session(&self, session_name: &str, expire_interval_sec: Option<i64>) -> Session {
        let session_dir = self.session_dir(session_name);
        if !session_dir.exists() {
            std::fs::create_dir_all(&session_dir).unwrap();
        }
        Session::new(self, session_name, expire_interval_sec)
    }
    pub fn session_dir(&self, session_name: &str) -> PathBuf {
        let mut dir = self.sessions_dir.clone();
        dir.push(session_name);
        dir
    }
    fn delete_dir(dir: PathBuf) {
        for d in dir.read_dir().unwrap().into_iter() {
            let d = d.unwrap();
            if d.file_type().unwrap().is_dir() {
                let dir = d.path();
                Self::delete_dir(dir);
            } else {
                let file = d.path();
                std::fs::remove_file(file).unwrap();
            }
        }
        let _ = std::fs::remove_dir_all(dir);
    }
    pub fn session_clear(&self, session: &mut Session) {
        let session_dir = self.session_dir(session.name());
        session.session_data = None;
        if session_dir.exists() {
            Self::delete_dir(session_dir);
        }
        session.temporary_data.clear();
    }

    pub fn session_restart(&self, session: &mut Session, expire_interval_sec: Option<i64>) {
        self.session_clear(session);
        self.init_session(session, expire_interval_sec)
    }

    fn init_session(&self, session: &mut Session, expire_interval_sec: Option<i64>) {
        let session_dir = self.session_dir(session.name());
        std::fs::create_dir_all(&session_dir).unwrap();
        let session_data = Session::new_data(&session_dir, expire_interval_sec);
        let temporary_data = session_data.init_temporary_data();
        session.session_data = Some(session_data);
        session.temporary_data = temporary_data;
    }

    pub async fn update(
        &self,
        session: &mut Session,
        records: Vec<SessionRecord>,
    ) -> Vec<CollectionRow> {
        let mut ret = vec![];
        let session_dir = self.session_dir(session.name());
        if let None = session.session_data {
            self.init_session(session, None);
        }
        if let Some(ref mut session_data) = session.session_data {
            let current = session_data.sequence_number.current();
            let max = session_data.sequence_number.max();
            if current < max {
                for row in ((current + 1)..=max).rev() {
                    for session_row in session_data
                        .sequence
                        .iter_by(|v| v.cmp(&row))
                        .collect::<Vec<_>>()
                        .into_iter()
                    {
                        futures::join!(
                            session_data.relation.delete(session_row),
                            async {
                                session_data.collection_id.delete(session_row);
                            },
                            async {
                                session_data.row.delete(session_row);
                            },
                            async {
                                session_data.operation.delete(session_row);
                            },
                            async {
                                session_data.activity.delete(session_row);
                            },
                            async {
                                session_data.term_begin.delete(session_row);
                            },
                            async {
                                session_data.term_end.delete(session_row);
                            },
                            async {
                                session_data.uuid.delete(session_row);
                            },
                            {
                                let mut fs = vec![];
                                for (_field_name, field_data) in session_data.fields.iter_mut() {
                                    fs.push(async { field_data.delete(session_row) });
                                }
                                futures::future::join_all(fs)
                            },
                            async {
                                session_data.sequence.delete(session_row);
                            }
                        );
                    }
                }
            }

            let sequence = session_data.sequence_number.next();
            ret.extend(
                self.update_recursive(
                    session_data,
                    &mut session.temporary_data,
                    &session_dir,
                    sequence,
                    &records,
                    None,
                )
                .await,
            );
        }
        ret
    }

    pub fn depends_with_session(
        &self,
        key: Option<Arc<String>>,
        pend_collection_id: NonZeroI32,
        pend_row: NonZeroI64,
        session: Option<&Session>,
    ) -> Vec<Depend> {
        let pend_row = pend_row.get();
        if pend_row < 0 {
            if let Some(session) = session {
                if let Some(session_depends) = session.depends(key, unsafe {
                    NonZeroU32::new_unchecked((-pend_row) as u32)
                }) {
                    return session_depends;
                }
            }
            vec![]
        } else if pend_collection_id.get() > 0 {
            self.relation()
                .depends(
                    key,
                    &CollectionRow::new(pend_collection_id, unsafe {
                        NonZeroU32::new_unchecked(pend_row as u32)
                    }),
                )
                .into_iter()
                .collect()
        } else {
            self.relation()
                .depends(
                    key,
                    &CollectionRow::new(-pend_collection_id, unsafe {
                        NonZeroU32::new_unchecked(pend_row as u32)
                    }),
                )
                .into_iter()
                .collect()
        }
    }

    pub async fn register_relations_with_session(
        &mut self,
        depend: &CollectionRow,
        pends: Vec<(Arc<String>, CollectionRow)>,
        row_map: &HashMap<CollectionRow, CollectionRow>,
    ) {
        for (key_name, pend) in pends.into_iter() {
            if pend.collection_id().get() < 0 {
                if let Some(pend) = row_map.get(&pend) {
                    self.register_relation(&key_name, depend, pend.clone())
                        .await;
                }
            } else {
                self.register_relation(&key_name, depend, pend).await;
            }
        }
    }
}
