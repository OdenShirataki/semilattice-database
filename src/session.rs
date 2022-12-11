//TODO:relationの処理を検証する事

use std::collections::HashMap;
use versatile_data::{
    IdxSized
    ,FieldData
    ,Activity
};

use crate::{Condition, Collection};

use super::Database;

mod operation;
pub use operation::{
    Record
    ,SessionOperation
    ,Depends
    ,Pend
};

mod sequence_number;
use sequence_number::SequenceNumber;

mod relation;
use relation::SessionRelation;

pub mod search;
use search::SessionSearch;

pub struct TemporaryDataEntity{
    pub(super) activity:Activity
    ,pub(super) term_begin:i64
    ,pub(super) term_end:i64
    ,pub(super) fields:HashMap<String,Vec<u8>>
}
pub type TemporaryData=HashMap<i32,HashMap<i64,TemporaryDataEntity>>;

pub struct SessionData{
    pub(super) sequence_number:SequenceNumber
    ,pub(super) sequence:IdxSized<usize>
    ,pub(super) collection_id:IdxSized<i32>
    ,pub(super) collection_row:IdxSized<u32>
    ,pub(super) operation:IdxSized<SessionOperation>
    ,pub(super) activity: IdxSized<u8>
    ,pub(super) term_begin: IdxSized<i64>
    ,pub(super) term_end: IdxSized<i64>
    ,pub(super) fields:HashMap<String,FieldData>
    ,pub(super) relation:SessionRelation
}
pub struct Session{
    name:String
    ,pub(super) session_data:Option<SessionData>
    ,pub(super) temporary_data:TemporaryData
}
impl Session{
    pub fn new(
        main_database:&Database
        ,name:impl Into<String>
    )->Result<Self,std::io::Error>{
        let mut name:String=name.into();
        assert!(name!="");
        if name==""{
            name="untitiled".to_owned();
        }
        let session_dir=main_database.root_dir().to_string()+"/sessions/"+&name;
        if !std::path::Path::new(&session_dir).exists(){
            std::fs::create_dir_all(&session_dir).unwrap();
        }
        let session_data=Self::new_data(&session_dir)?;
        let temporary_data=Self::init_temporary_data(&session_data);
        Ok(Self{
            name
            ,session_data:Some(session_data)
            ,temporary_data
        })
    }
    pub fn name(&mut self)->&str{
        &self.name
    }
    fn init_temporary_data(session_data:&SessionData) -> TemporaryData {
        let mut temporary_data=HashMap::new();
        for session_row in 1..session_data.sequence.max_rows(){
            let collection_id=session_data.collection_id.value(session_row).unwrap();
            if collection_id>0{
                let col=temporary_data.entry(collection_id).or_insert(HashMap::new());
                let collection_row=session_data.collection_row.value(session_row).unwrap();

                let temporary_row:i64=if collection_row==0{
                    -(session_row as i64)
                }else{
                    collection_row as i64
                };
                let mut fields=HashMap::new();
                for (key,val) in &session_data.fields{
                    if let Some(v)=val.get(session_row){
                        fields.insert(key.to_string(), v.to_vec());
                    }
                }
                col.insert(temporary_row,TemporaryDataEntity{
                    activity:if session_data.activity.value(session_row).unwrap()==1{
                        Activity::Active
                    }else{
                        Activity::Inactive
                    }
                    ,term_begin:session_data.term_begin.value(session_row).unwrap()
                    ,term_end:session_data.term_end.value(session_row).unwrap()
                    ,fields
                });
            }
        }
        temporary_data
    }
    pub fn new_data(session_dir:&str)->Result<SessionData,std::io::Error>{
        let mut fields=HashMap::new();

        let fields_dir=session_dir.to_string()+"/fields/";
        if !std::path::Path::new(&fields_dir).exists(){
            std::fs::create_dir_all(fields_dir.to_owned()).unwrap();
        }
        let d=std::fs::read_dir(fields_dir).unwrap();
        for p in d{
            if let Ok(p)=p{
                let path=p.path();
                if path.is_dir(){
                    if let Some(fname)=path.file_name(){
                        if let Some(str_fname)=fname.to_str(){
                            if let Some(p)=path.to_str(){
                                let field=FieldData::new(&(p.to_string()+"/")).unwrap();
                                fields.insert(String::from(str_fname),field);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(SessionData{
            sequence_number:SequenceNumber::new(&(session_dir.to_string()+"/sequece_number.i"))?
            ,sequence:IdxSized::new(&(session_dir.to_string()+"/sequence.i"))?
            ,collection_id:IdxSized::new(&(session_dir.to_string()+"/collection_id.i"))?
            ,collection_row:IdxSized::new(&(session_dir.to_string()+"/collection_row.i"))?
            ,operation:IdxSized::new(&(session_dir.to_string()+"/operation.i"))?
            ,activity:IdxSized::new(&(session_dir.to_string()+"/activity.i"))?
            ,term_begin:IdxSized::new(&(session_dir.to_string()+"/term_begin.i"))?
            ,term_end:IdxSized::new(&(session_dir.to_string()+"/term_end.i"))?
            ,fields
            ,relation:SessionRelation::new(&session_dir)
        })
    }

    pub fn begin_search(&self,collection_id:i32)->SessionSearch{
        SessionSearch::new(self,collection_id)
    }
    pub fn search(&self,collection_id:i32,condtions:&Vec<Condition>)->SessionSearch{
        let mut search=SessionSearch::new(self,collection_id);
        for c in condtions{
            search=search.search(c.clone());
        }
        search
    }
    
    pub fn field_bytes<'a>(&'a self,database:&'a Database,collection_id:i32,row:i64,key:&str)->&[u8]{
        if let Some(tmp_col)=self.temporary_data.get(&collection_id){
            if let Some(tmp_row)=tmp_col.get(&row){
                if let Some(val)=tmp_row.fields.get(key){
                    return val;
                }
            }
        }
        if row>0{
            if let Some(col)=database.collection(collection_id){
                return col.field_bytes(row as u32,key)
            } 
        }
        b""
    }

    pub fn collection_field_bytes<'a>(&'a self,collection:&'a Collection,row:i64,key:&str)->&[u8]{
        if let Some(tmp_col)=self.temporary_data.get(&collection.id()){
            if let Some(tmp_row)=tmp_col.get(&row){
                if let Some(val)=tmp_row.fields.get(key){
                    return val;
                }
            }
        }
        if row>0{
            return collection.field_bytes(row as u32,key);
        }
        b""
    }
    pub fn temporary_collection(&self,collection_id:i32)->Option<&HashMap<i64,TemporaryDataEntity>>{
        self.temporary_data.get(&collection_id)
    }
}
