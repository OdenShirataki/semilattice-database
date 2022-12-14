use idx_binary::IdxBinary;
use versatile_data::IdxSized;

use super::SessionCollectionRow;

pub struct SessionRelationRows {
    pub(crate) key: IdxSized<u32>,
    pub(crate) session_row: IdxSized<u32>,
    pub(crate) depend: IdxSized<SessionCollectionRow>,
}
pub struct SessionRelation {
    pub(crate) key_names: IdxBinary,
    pub(crate) rows: SessionRelationRows,
}
impl SessionRelation {
    pub fn new(session_dir: &str) -> Self {
        let relation_dir = session_dir.to_string() + "/relation/";
        if !std::path::Path::new(&relation_dir).exists() {
            std::fs::create_dir_all(&relation_dir).unwrap();
        }
        Self {
            key_names: IdxBinary::new(&(relation_dir.to_string() + "/key_name")).unwrap(),
            rows: SessionRelationRows {
                key: IdxSized::new(&(relation_dir.to_string() + "/key.i")).unwrap(),
                session_row: IdxSized::new(&(relation_dir.to_string() + "/session_row.i")).unwrap(),
                depend: IdxSized::new(&(relation_dir.to_string() + "/depend.i")).unwrap(),
            },
        }
    }
    pub fn insert(&mut self, relation_key: &str, session_row: u32, depend: SessionCollectionRow) {
        if let Ok(key_id) = self.key_names.entry(relation_key.as_bytes()) {
            self.rows.key.insert(key_id).unwrap();
            self.rows.session_row.insert(session_row).unwrap();

            self.rows.depend.insert(depend).unwrap();
        }
    }
    /*
    pub fn depends(&self,pend_collection_id:i32,pend_row:i64){
        //self.rows.
    } */
}
