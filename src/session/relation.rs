use idx_binary::IdxBinary;
use versatile_data::IdxSized;
use crate::CollectionRow;

pub struct SessionRelationRows{
    pub(super) sequence:IdxSized<usize>
    ,pub(super) key:IdxSized<u32>
    ,pub(super) child_session_row:IdxSized<u32>
    ,pub(super) parent_session_row:IdxSized<u32>
    ,pub(super) parent:IdxSized<CollectionRow>
}
pub struct SessionRelation{
    pub(super) key_names:IdxBinary
    ,pub(super) rows:SessionRelationRows
}
impl SessionRelation{
    pub fn new(session_dir:&str)->SessionRelation{
        let relation_dir=session_dir.to_string()+"/relation/";
        if !std::path::Path::new(&relation_dir).exists(){
            std::fs::create_dir_all(&relation_dir).unwrap();
        }
        SessionRelation{
            key_names:IdxBinary::new(&(relation_dir.to_string()+"/key_name")).unwrap()
            ,rows:SessionRelationRows{
                sequence:IdxSized::new(&(relation_dir.to_string()+"/sequence.i")).unwrap()
                ,key:IdxSized::new(&(relation_dir.to_string()+"/key.i")).unwrap()
                ,child_session_row:IdxSized::new(&(relation_dir.to_string()+"/child_session_row.i")).unwrap()
                ,parent_session_row:IdxSized::new(&(relation_dir.to_string()+"/parent_session_row.i")).unwrap()
                ,parent:IdxSized::new(&(relation_dir.to_string()+"/parent.i")).unwrap()
            }
        }
    }
    pub fn insert(
        &mut self
        ,sequence:usize
        ,relation_key:&str
        ,child_session_row:u32
        ,parent_session_row:u32
        ,parent:CollectionRow
    ){
        if let Some(key_id)=self.key_names.entry(relation_key.as_bytes()){
            self.rows.sequence.insert(sequence);
            self.rows.key.insert(key_id);
            self.rows.child_session_row.insert(child_session_row);
            self.rows.parent_session_row.insert(parent_session_row);
            self.rows.parent.insert(parent);
        }
    }
}
