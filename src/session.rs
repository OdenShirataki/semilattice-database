use std::collections::HashMap;
use versatile_data::{
    IdxSized
    ,Activity
    ,FieldData
    ,Condition
    ,Term
};
use super::Database;

mod operation;
pub use operation::{
    Record
    ,SessionOperation
    ,UpdateDepends
};

mod sequence_number;
use sequence_number::SequenceNumber;

mod relation;
use relation::SessionRelation;

mod update;
use update::*;

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
    ,data:Option<SessionData>
    ,search_condition:Vec<Condition>
}
impl<'a> Session<'a>{
    pub fn new(
        main_database:&'a mut Database
        ,session_name:&'a str
    )->Result<Session,std::io::Error>{
        let session_dir=main_database.root_dir().to_string()+"/sessions/"+session_name;
        if !std::path::Path::new(&session_dir).exists(){
            std::fs::create_dir_all(&session_dir).unwrap();
        }
        Ok(Session{
            main_database
            ,session_dir:session_dir.to_string()
            ,data:Some(Self::new_data(&session_dir)?)
            ,search_condition:Vec::new()
        })
    }
    
    fn new_data(session_dir:&str)->Result<SessionData,std::io::Error>{
        Ok(SessionData{
            sequence_number:SequenceNumber::new(&(session_dir.to_string()+"/sequece_number.i"))?
            ,sequence:IdxSized::new(&(session_dir.to_string()+"/sequence.i"))?
            ,collection_id:IdxSized::new(&(session_dir.to_string()+"/collection_id.i"))?
            ,collection_row:IdxSized::new(&(session_dir.to_string()+"/collection_row.i"))?
            ,operation:IdxSized::new(&(session_dir.to_string()+"/operation.i"))?
            ,activity:IdxSized::new(&(session_dir.to_string()+"/activity.i"))?
            ,term_begin:IdxSized::new(&(session_dir.to_string()+"/term_begin.i"))?
            ,term_end:IdxSized::new(&(session_dir.to_string()+"/term_end.i"))?
            ,fields:HashMap::new()
            ,relation:SessionRelation::new(&session_dir)
        })
    }
    pub fn clear(&mut self){
        self.data=None;
        if std::path::Path::new(&self.session_dir).exists(){
            std::fs::remove_dir_all(&self.session_dir).unwrap();
        }
    }
    pub fn public(&mut self){
        if let Some(ref mut data)=self.data{
            public(data,self.main_database);
            self.clear();
        }
    }
    pub fn update(&mut self,records:Vec::<Record>){
        match self.data{
            Some(ref mut data)=>{
                let sequence=data.sequence_number.next();
                update_recursive(&self.main_database,data,&self.session_dir,sequence,&records,None);
            }
            ,None=>{
                if let Ok(data)=Self::new_data(&self.session_dir){
                    self.data=Some(data);
                    if let Some(ref mut data)=self.data{
                        let sequence=data.sequence_number.next();
                        update_recursive(&self.main_database,data,&self.session_dir,sequence,&records,None);
                    }
                }
            }
        }
    }
    pub fn search_default(mut self)->Self{
        self.search_condition.push(Condition::Term(Term::In(chrono::Local::now().timestamp())));
        self.search_condition.push(Condition::Activity(Activity::Active));
        self
    }
    pub fn search(mut self,condition:Condition)->Self{
        self.search_condition.push(condition);
        self
    }
}