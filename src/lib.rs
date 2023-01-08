use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io,
    path::{Path, PathBuf},
};

pub use idx_binary::IdxBinary;

use session::search::SessionSearch;
use versatile_data::Data;
pub use versatile_data::{Activity, IdxSized, KeyValue, Order, OrderKey, RowSet, Term};

mod collection;
pub use collection::{Collection, CollectionRow};

mod relation;
pub use relation::{Depend, RelationIndex};

mod session;
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
    pub fn session(&self, session_name: &str) -> io::Result<Session> {
        let session_dir = self.session_dir(session_name);
        if !std::path::Path::new(&session_dir).exists() {
            std::fs::create_dir_all(&session_dir)?;
        }
        Session::new(self, session_name)
    }
    pub fn commit(&mut self, session: &mut Session) -> io::Result<()> {
        if let Some(ref mut data) = session.session_data {
            commit::commit(self, data)?;
            self.session_clear(session)?;
        }
        Ok(())
    }
    pub fn session_clear(&self, session: &mut Session) -> io::Result<()> {
        let session_dir = self.session_dir(session.name());
        session.session_data = None;
        if std::path::Path::new(&session_dir).exists() {
            std::fs::remove_dir_all(&session_dir)?;
        }
        Ok(())
    }
    pub fn session_start(&self, session: &mut Session) {
        let session_dir = self.session_dir(session.name());
        if let Ok(session_data) = Session::new_data(&session_dir) {
            session.session_data = Some(session_data);
        }
    }
    pub fn session_restart(&self, session: &mut Session) -> io::Result<()> {
        self.session_clear(session)?;
        self.session_start(session);
        Ok(())
    }
    pub fn update(&self, session: &mut Session, records: Vec<Record>) -> io::Result<()> {
        let session_dir = self.session_dir(session.name());
        if let None = session.session_data {
            if let Ok(session_data) = Session::new_data(&session_dir) {
                session.session_data = Some(session_data);
            }
        }
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
    ) -> Result<BTreeSet<i64>, std::sync::mpsc::SendError<RowSet>> {
        search.result(self)
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
