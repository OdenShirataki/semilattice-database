mod commit;
mod session;
mod update;

pub use semilattice_database::{
    anyhow, search, Activity, Collection, CollectionRow, Condition, Depend, KeyValue, Order,
    OrderKey, Term, Uuid,
};
pub use session::{Depends, Pend, Record, Session};

use std::{
    collections::HashMap,
    io::{self, Read},
    path::{Path, PathBuf},
    time::{self, UNIX_EPOCH},
};

use anyhow::Result;
use semilattice_database::{natord, BinarySet, Database, Field, FileMmap, IdxFile, RowSet};
use session::{search::SessionSearch, SessionInfo};

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
    pub fn new<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let dir = dir.as_ref();
        let database = Database::new(dir)?;
        let mut sessions_dir = dir.to_path_buf();
        sessions_dir.push("sessions");
        Ok(Self {
            database,
            sessions_dir,
        })
    }
    pub fn sessions(&self) -> io::Result<Vec<SessionInfo>> {
        let mut sessions = Vec::new();
        if self.sessions_dir.exists() {
            let dir = self.sessions_dir.read_dir()?;
            for d in dir.into_iter() {
                let d = d?;
                if d.file_type()?.is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        let mut access_at = 0;
                        let mut expire = 0;

                        let mut expire_file = d.path().to_path_buf();
                        expire_file.push("expire");
                        if expire_file.exists() {
                            if let Ok(md) = expire_file.metadata() {
                                if let Ok(m) = md.modified() {
                                    access_at = m.duration_since(UNIX_EPOCH).unwrap().as_secs();
                                    let mut file = std::fs::File::open(expire_file)?;
                                    let mut buf = [0u8; 8];
                                    file.read(&mut buf)?;
                                    expire = i64::from_be_bytes(buf);
                                }
                            }
                        }
                        sessions.push(SessionInfo {
                            name: fname.to_owned(),
                            access_at: access_at,
                            expire: expire,
                        });
                    }
                }
            }
        }
        Ok(sessions)
    }
    pub fn session_gc(&self, default_expire_interval_sec: i64) -> io::Result<()> {
        for session in self.sessions()? {
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
                std::fs::remove_dir_all(&path)?;
            }
        }
        Ok(())
    }

    pub fn session(
        &self,
        session_name: &str,
        expire_interval_sec: Option<i64>,
    ) -> io::Result<Session> {
        let session_dir = self.session_dir(session_name);
        if !session_dir.exists() {
            std::fs::create_dir_all(&session_dir)?;
        }
        Session::new(self, session_name, expire_interval_sec)
    }
    pub fn session_dir(&self, session_name: &str) -> PathBuf {
        let mut dir = self.sessions_dir.clone();
        dir.push(session_name);
        dir
    }
    pub fn session_clear(&self, session: &mut Session) -> io::Result<()> {
        let session_dir = self.session_dir(session.name());
        session.session_data = None;
        if session_dir.exists() {
            std::fs::remove_dir_all(&session_dir)?;
        }
        Ok(())
    }

    pub fn session_restart(
        &self,
        session: &mut Session,
        expire_interval_sec: Option<i64>,
    ) -> io::Result<()> {
        self.session_clear(session)?;

        let session_dir = self.session_dir(session.name());
        std::fs::create_dir_all(&session_dir)?;
        let session_data = Session::new_data(&session_dir, expire_interval_sec)?;
        let temporary_data = session_data.init_temporary_data()?;
        session.session_data = Some(session_data);
        session.temporary_data = temporary_data;

        Ok(())
    }

    pub fn update(
        &self,
        session: &mut Session,
        records: Vec<Record>,
    ) -> Result<Vec<CollectionRow>> {
        let mut ret = vec![];
        let session_dir = self.session_dir(session.name());
        if let Some(ref mut session_data) = session.session_data {
            let current = session_data.sequence_number.current();
            let max = session_data.sequence_number.max();
            if current < max {
                for row in ((current + 1)..=max).rev() {
                    for session_row in session_data
                        .sequence
                        .iter_by(|v| v.cmp(&row))
                        .map(|x| x.row())
                        .collect::<Vec<u32>>()
                    {
                        session_data.collection_id.delete(session_row)?;
                        session_data.row.delete(session_row)?;
                        session_data.operation.delete(session_row)?;
                        session_data.activity.delete(session_row)?;
                        session_data.term_begin.delete(session_row)?;
                        session_data.term_end.delete(session_row)?;
                        session_data.uuid.delete(session_row)?;

                        for (_field_name, field_data) in session_data.fields.iter_mut() {
                            field_data.delete(session_row)?;
                        }

                        session_data.relation.delete(session_row)?;

                        session_data.sequence.delete(session_row)?;
                    }
                }
            }

            let sequence = session_data.sequence_number.next();
            ret.append(&mut self.update_recursive(
                session_data,
                &mut session.temporary_data,
                &session_dir,
                sequence,
                &records,
                None,
            )?);
        }
        Ok(ret)
    }

    pub fn result_session(
        &self,
        search: SessionSearch,
        orders: Vec<Order>,
    ) -> Result<Vec<i64>, std::sync::mpsc::SendError<RowSet>> {
        search.result(self, orders)
    }

    pub fn depends_with_session(
        &self,
        key: Option<&str>,
        pend_collection_id: i32,
        pend_row: u32,
        session: Option<&Session>,
    ) -> Vec<Depend> {
        let mut r: Vec<Depend> = vec![];
        if pend_collection_id > 0 {
            let depends = self.relation().depends(
                key,
                &CollectionRow::new(pend_collection_id, pend_row as u32),
            );
            for i in depends {
                r.push(i.into());
            }
        } else {
            if let Some(session) = session {
                if let Some(session_depends) = session.depends(key, pend_row) {
                    r = session_depends;
                }
            }
        }
        r
    }

    pub fn register_relations_with_session(
        &mut self,
        depend: &CollectionRow,
        pends: Vec<(String, CollectionRow)>,
        row_map: &HashMap<CollectionRow, CollectionRow>,
    ) -> Result<()> {
        for (key_name, pend) in pends {
            if pend.collection_id() < 0 {
                if let Some(pend) = row_map.get(&pend) {
                    self.register_relation(&key_name, depend, pend.clone())?;
                }
            } else {
                self.register_relation(&key_name, depend, pend)?;
            }
        }
        Ok(())
    }
}
