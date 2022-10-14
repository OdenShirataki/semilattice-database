use versatile_data::{Condition, RowSet, Activity, search::Term, Field, Number};

use super::Session;

pub struct SessionSearch<'a>{
    session:&'a Session<'a>
    ,collection_id:i32
    ,conditions:Vec<Condition>
    ,result:Option<RowSet>
}
impl<'a> SessionSearch<'a>{
    pub fn new(session:&'a Session<'a>,collection_id:i32)->SessionSearch{
        SessionSearch{
            session
            ,collection_id
            ,conditions:Vec::new()
            ,result:None
        }
    }
    pub fn search_default(mut self)->Self{
        self.conditions.push(Condition::Term(Term::In(chrono::Local::now().timestamp())));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
    }
    pub fn search_field(self,field_name:impl Into<String>,condition:Field)->Self{
        self.search(Condition::Field(field_name.into(),condition))
    }
    pub fn search_term(self,condition:Term)->Self{
        self.search(Condition::Term(condition))
    }
    pub fn search_activity(self,condition:Activity)->Self{
        self.search(Condition::Activity(condition))
    }
    pub fn search_row(self,condition:Number)->Self{
        self.search(Condition::Row(condition))
    }

    fn search(mut self,condition:Condition)->Self{
        self.conditions.push(condition);
        self
    }

    pub fn result(self)->RowSet{
        if let Some(collection)=self.session.main_database.collections.get(&self.collection_id){
            let mut search=collection.begin_search();
            for c in &self.conditions{
                search=search.search(c.clone());
            }
            let r=search.result();
            if let Some(t)=self.session.temporary_data.get(&self.collection_id){
                for c in &self.conditions{
                    
                    //search=search.search(c);
                }
                println!("{:?}",t)
            }
            println!("result:{:?}",r);
        }
        
        /*
        self.search_exec();
        if let Some(r)=self.result{
            r
        }else{
            //self.data.all()
            RowSet::default()
        } */
        RowSet::default()
    }
    fn search_exec(&mut self){
        /*
        let (tx, rx) = std::sync::mpsc::channel();
        for c in &self.conditions{
            let tx=tx.clone();
            match c{
                Condition::Activity(condition)=>{
                    self.search_exec_activity(condition,tx)
                }
                ,Condition::Term(condition)=>{
                    self.search_exec_term(condition,tx)
                }
                ,Condition::Field(field_name,condition)=>{
                    self.search_exec_field(field_name,condition,tx)
                }
                ,Condition::Row(condition)=>{
                    self.search_exec_row(condition,tx)
                }
                ,Condition::LastUpdated(condition)=>{
                    self.search_exec_last_updated(condition,tx)
                }
                ,Condition::Uuid(uuid)=>{
                    self.search_exec_uuid(uuid,tx)
                }
            };
        }
        drop(tx);
        for rs in rx{
            self.reduce(rs);
        } */
    }
}