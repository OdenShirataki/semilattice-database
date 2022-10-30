use std::collections::HashMap;
use versatile_data::{
    IdxSized
    ,FieldData
    ,Activity
};

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

#[derive(Debug)]
pub struct TemporaryDataEntity{
    pub(super) activity:Activity
    ,pub(super) term_begin:i64
    ,pub(super) term_end:i64
    ,pub(super) fields:HashMap<String,Vec<u8>>
}
pub type TemporaryData=HashMap<i32,HashMap<u32,TemporaryDataEntity>>;

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
        let name:String=name.into();
        if name==""{
            Ok(Self::new_blank())
        }else{
            let session_dir=main_database.root_dir().to_string()+"/sessions/"+&name;
            if !std::path::Path::new(&session_dir).exists(){
                std::fs::create_dir_all(&session_dir).unwrap();
            }
            let session_data=Self::new_data(&session_dir)?;
            let temporary_data=Self::make_session_data(&session_data);
            Ok(Self{
                name
                ,session_data:Some(session_data)
                ,temporary_data
            })
        }
    }
    pub fn new_blank()->Self{
        Self{
            name:"".to_string()
            ,session_data:None
            ,temporary_data:HashMap::new()
        }
    }
    pub fn name(&mut self)->&str{
        &self.name
    }
    fn make_session_data(session_data:&SessionData) -> HashMap<i32, HashMap<u32, TemporaryDataEntity>> {
        let mut temporary_data=HashMap::new();
        for i in session_data.sequence.triee().iter(){
            let row=i.row();

            let collection_id=session_data.collection_id.value(row).unwrap();
            if collection_id>0{
                let col=temporary_data.entry(collection_id).or_insert(HashMap::new());
                let collection_row=session_data.collection_row.value(row).unwrap();
                let mut fields=HashMap::new();

                for (key,val) in &session_data.fields{
                    if let Some(v)=val.get(collection_row){
                        fields.insert(key.to_string(), v.to_vec());
                    }
                }
                col.insert(collection_row,TemporaryDataEntity{
                    activity:if session_data.activity.value(row).unwrap()==1{
                        Activity::Active
                    }else{
                        Activity::Inactive
                    }
                    ,term_begin:session_data.term_begin.value(row).unwrap()
                    ,term_end:session_data.term_end.value(row).unwrap()
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

    pub fn is_blank(&self)->bool{
        if let None=self.session_data{
            true
        }else{
            false
        }
    }

    pub fn begin_search(&self,collection_id:i32)->SessionSearch{
        SessionSearch::new(self,collection_id)
    }
    pub fn field_str<'a>(&'a self,database:&'a Database,collection_id:i32,row:u32,key:&str)->&str{
        if let Some(tmp_col)=self.temporary_data.get(&collection_id){
            if let Some(tmp_row)=tmp_col.get(&row){
                if let Some(val)=tmp_row.fields.get(key){
                    if let Ok(str)=std::str::from_utf8(val){
                        return str;
                    }
                }
            }
        }
        if let Some(col)=database.collection(collection_id){
            col.field_str(row,key)
        }else{
            ""
        }
    }
}
