use idx_binary::IdxBinary;
use versatile_data::IdxSized;
use crate::CollectionRow;

pub struct SessionRelationRows{
    pub(crate) sequence:IdxSized<usize>
    ,pub(crate) key:IdxSized<u32>
    ,pub(crate) session_row:IdxSized<u32>
    ,pub(crate) depend_session_row:IdxSized<u32>
    ,pub(crate) depend:IdxSized<CollectionRow>
}
pub struct SessionRelation{
    pub(crate) key_names:IdxBinary
    ,pub(crate) rows:SessionRelationRows
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
                ,session_row:IdxSized::new(&(relation_dir.to_string()+"/session_row.i")).unwrap()
                ,depend_session_row:IdxSized::new(&(relation_dir.to_string()+"/depend_session_row.i")).unwrap()
                ,depend:IdxSized::new(&(relation_dir.to_string()+"/depend.i")).unwrap()
            }
        }
    }
    pub fn insert(
        &mut self
        ,sequence:usize
        ,relation_key:&str
        ,session_row:u32
        ,depend_session_row:u32
        ,depend:CollectionRow
    ){
        if let Some(key_id)=self.key_names.entry(relation_key.as_bytes()){
            self.rows.sequence.insert(sequence);
            self.rows.key.insert(key_id);
            self.rows.session_row.insert(session_row);
            self.rows.depend_session_row.insert(depend_session_row);
            self.rows.depend.insert(depend);
        }
    }
}
