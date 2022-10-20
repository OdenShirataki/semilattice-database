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

mod update;
use update::*;

pub mod search;
use search::SessionSearch;

#[derive(Debug)]
struct TemporaryDataEntity{
    activity:Activity
    ,term_begin:i64
    ,term_end:i64
    ,fields:HashMap<String,Vec<u8>>
}
type TemporaryData=HashMap<i32,HashMap<u32,TemporaryDataEntity>>;

struct SessionData{
    sequence_number:SequenceNumber
    ,sequence:IdxSized<usize>
    ,collection_id:IdxSized<i32>
    ,collection_row:IdxSized<u32>
    ,operation:IdxSized<SessionOperation>
    ,activity: IdxSized<u8>
    ,term_begin: IdxSized<i64>
    ,term_end: IdxSized<i64>
    ,fields:HashMap<String,FieldData>
    ,relation:SessionRelation
}
pub struct Session<'a>{
    main_database:&'a mut Database
    ,session_dir:String
    ,session_data:Option<SessionData>
    ,temporary_data:TemporaryData
}
impl<'a> Session<'a>{
    pub fn new(
        main_database:&'a mut Database
        ,session_name:&'a str
    )->Result<Session,std::io::Error>{
        if session_name==""{
            Ok(Session{
                main_database
                ,session_dir:"".to_string()
                ,session_data:None
                ,temporary_data:HashMap::new()
            })
        }else{
            let session_dir=main_database.root_dir().to_string()+"/sessions/"+session_name;
            if !std::path::Path::new(&session_dir).exists(){
                std::fs::create_dir_all(&session_dir).unwrap();
            }
            let session_data=Self::new_data(&session_dir)?;
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
            
            Ok(Session{
                main_database
                ,session_dir:session_dir.to_string()
                ,session_data:Some(session_data)
                ,temporary_data
            })
        }
    }
    
    fn new_data(session_dir:&str)->Result<SessionData,std::io::Error>{
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

    pub fn is_none(&self)->bool{
        if let None=self.session_data{
            true
        }else{
            false
        }
    }
    pub fn clear(&mut self){
        self.session_data=None;
        if std::path::Path::new(&self.session_dir).exists(){
            std::fs::remove_dir_all(&self.session_dir).unwrap();
        }
    }
    pub fn commit(&mut self){
        if let Some(ref mut data)=self.session_data{
            commit(data,self.main_database);
            self.clear();
        }
    }
    pub fn update(&mut self,records:Vec::<Record>){
        match self.session_data{
            Some(ref mut data)=>{
                let sequence=data.sequence_number.next();
                update_recursive(&self.main_database,data,&mut self.temporary_data,&self.session_dir,sequence,&records,None);
            }
            ,None=>{
                if let Ok(data)=Self::new_data(&self.session_dir){
                    self.session_data=Some(data);
                    if let Some(ref mut data)=self.session_data{
                        let sequence=data.sequence_number.next();
                        update_recursive(&self.main_database,data,&mut self.temporary_data,&self.session_dir,sequence,&records,None);
                    }
                }
            }
        }
    }
    pub fn begin_search(&self,collection_id:i32)->SessionSearch{
        SessionSearch::new(self,collection_id)
    }
    pub fn field_str(&self,collection_id:i32,row:u32,key:&str)->&str{
        if let Some(tmp_col)=self.temporary_data.get(&collection_id){
            if let Some(tmp_row)=tmp_col.get(&row){
                if let Some(val)=tmp_row.fields.get(key){
                    if let Ok(str)=std::str::from_utf8(val){
                        return str;
                    }
                }
            }
        }
        if let Some(col)=self.main_database.collection(collection_id){
            col.field_str(row,key)
        }else{
            ""
        }
    }
    pub fn collection_id(&mut self,collection_name:&str)->Result<i32,std::io::Error>{
        self.main_database.collection_id(collection_name)
    }
}