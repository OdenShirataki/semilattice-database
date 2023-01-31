use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Read},
    path::{Path, PathBuf},
    time::{self, UNIX_EPOCH},
};

pub use idx_binary::IdxBinary;

use session::{search::SessionSearch, SessionInfo};
pub use versatile_data::{natord, Activity, IdxSized, KeyValue, Order, OrderKey, RowSet, Term};
use versatile_data::{Data, Operation};

mod collection;
pub use collection::{Collection, CollectionRow};

mod relation;
pub use relation::{Depend, RelationIndex};

mod session;
use session::TemporaryDataEntity;
pub use session::{
    search as session_search, Depends, Pend, Record, Session, SessionCollectionRow, SessionDepend,
};

pub mod search;
pub use search::{Condition, Search};

mod commit;

mod update;

pub mod prelude;

pub struct Database {
    root_dir: PathBuf,
    sessions_dir: PathBuf,
    collections_dir: PathBuf,
    collections_map: HashMap<String, i32>,
    collections: BTreeMap<i32, Collection>,
    relation: RelationIndex,
}
impl Database {
    pub fn new<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let dir = dir.as_ref();

        let mut collections_dir = dir.to_path_buf();
        collections_dir.push("collection");

        let mut collections_map = HashMap::new();
        let mut collections = BTreeMap::new();

        if collections_dir.exists() {
            let dir = collections_dir.read_dir()?;
            for d in dir.into_iter() {
                let d = d?;
                if d.file_type()?.is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        if let Some(pos) = fname.find("_") {
                            if let Ok(collection_id) = (&fname[..pos]).parse::<i32>() {
                                let name = &fname[(pos + 1)..];
                                let data =
                                    Collection::new(Data::new(d.path())?, collection_id, name);
                                collections_map.insert(name.to_string(), collection_id);
                                collections.insert(collection_id, data);
                            }
                        }
                    }
                }
            }
        }

        let mut sessions_dir = dir.to_path_buf();
        sessions_dir.push("sessions");

        Ok(Self {
            root_dir: dir.to_path_buf(),
            sessions_dir,
            collections_dir,
            collections,
            collections_map,
            relation: RelationIndex::new(dir)?,
        })
    }
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }
    fn session_dir(&self, session_name: &str) -> PathBuf {
        let mut dir = self.sessions_dir.clone();
        dir.push(session_name);
        dir
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
    pub fn commit(&mut self, session: &mut Session) -> Result<(), anyhow::Error> {
        if let Some(ref mut data) = session.session_data {
            commit::commit(self, data)?;
            self.session_clear(session)?;
        }
        Ok(())
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
        let temporary_data = Session::init_temporary_data(&session_data)?;
        session.session_data = Some(session_data);
        session.temporary_data = temporary_data;

        Ok(())
    }
    pub fn update(&self, session: &mut Session, records: Vec<Record>) -> io::Result<()> {
        let session_dir = self.session_dir(session.name());
        if let Some(ref mut session_data) = session.session_data {
            let sequence = session_data.sequence_number.next();
            update::update_recursive(
                self,
                session_data,
                &mut session.temporary_data,
                &session_dir,
                sequence,
                &records,
                None,
            )?;
        }
        Ok(())
    }
    fn collection_by_name_or_create(&mut self, name: &str) -> io::Result<i32> {
        let mut max_id = 0;
        if self.collections_dir.exists() {
            for d in self.collections_dir.read_dir()? {
                let d = d?;
                if d.file_type()?.is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        let s: Vec<&str> = fname.split("_").collect();
                        if s.len() > 1 {
                            if let Ok(i) = s[0].parse() {
                                max_id = std::cmp::max(max_id, i);
                            }
                            if s[1] == name {
                                let data = Collection::new(Data::new(d.path())?, max_id, name);
                                self.collections_map.insert(name.to_string(), max_id);
                                self.collections.insert(max_id, data);
                                return Ok(max_id);
                            }
                        }
                    }
                }
            }
        }
        let collection_id = max_id + 1;
        let data = Collection::new(
            Data::new({
                let mut collecion_dir = self.collections_dir.clone();
                collecion_dir.push(&(collection_id.to_string() + "_" + name));
                collecion_dir
            })?,
            collection_id,
            name,
        );
        self.collections_map.insert(name.to_string(), collection_id);
        self.collections.insert(collection_id, data);
        Ok(collection_id)
    }
    pub fn collections(&self) -> Vec<String> {
        let mut r = Vec::new();
        for (_, collection) in self.collections.iter() {
            r.push(collection.name().to_owned());
        }
        r
    }

    pub fn collection(&self, id: i32) -> Option<&Collection> {
        self.collections.get(&id)
    }
    pub fn collection_mut(&mut self, id: i32) -> Option<&mut Collection> {
        self.collections.get_mut(&id)
    }
    pub fn collection_id(&self, name: &str) -> Option<i32> {
        if self.collections_map.contains_key(name) {
            Some(*self.collections_map.get(name).unwrap())
        } else {
            None
        }
    }
    pub fn collection_id_or_create(&mut self, name: &str) -> io::Result<i32> {
        if self.collections_map.contains_key(name) {
            Ok(*self.collections_map.get(name).unwrap())
        } else {
            self.collection_by_name_or_create(name)
        }
    }

    pub fn delete_collection(&mut self, name: &str) -> std::io::Result<()> {
        let collection_id = if let Some(collection_id) = self.collections_map.get(name) {
            *collection_id
        } else {
            0
        };
        if collection_id > 0 {
            let rows = {
                let mut rows = Default::default();
                if let Some(collection) = self.collections.get(&collection_id) {
                    rows = collection.data.all();
                }
                rows
            };
            for row in rows {
                let collection_row = SessionCollectionRow::new(collection_id, row as i64);
                commit::delete_recursive(self, &collection_row)?;
                if let Some(collection) = self.collection_mut(collection_id) {
                    collection.update(&Operation::Delete { row: row as u32 })?;
                }
            }
            self.collections_map.remove(name);
            self.collections.remove(&collection_id);

            let mut dir = self.collections_dir.clone();
            dir.push(collection_id.to_string() + "_" + name);
            std::fs::remove_dir_all(&dir)?;
        }

        Ok(())
    }

    pub fn relation(&self) -> &RelationIndex {
        &self.relation
    }
    pub fn search(&self, colletion: &Collection) -> Search {
        Search::new(colletion)
    }
    pub fn result(&self, search: Search) -> Result<RowSet, std::sync::mpsc::SendError<RowSet>> {
        search.result(self)
    }
    pub fn result_session(
        &self,
        search: SessionSearch,
        orders: Vec<Order>,
    ) -> Result<Vec<i64>, std::sync::mpsc::SendError<RowSet>> {
        search.result(self, orders)
    }

    pub fn depends(
        &self,
        key: Option<&str>,
        pend_collection_id: i32,
        pend_row: i64,
        session: Option<&Session>,
    ) -> Vec<SessionDepend> {
        let mut r: Vec<SessionDepend> = vec![];
        if pend_row > 0 {
            let depends = self.relation.depends(
                key,
                &CollectionRow::new(pend_collection_id, pend_row as u32),
            );
            for i in depends {
                r.push(i.into());
            }
        } else {
            if let Some(session) = session {
                if let Some(session_depends) = session.depends(key, (-pend_row) as u32) {
                    r = session_depends;
                }
            }
        }
        r
    }
}
