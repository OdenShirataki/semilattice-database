use versatile_data::{
    Condition
    ,RowSet
    ,Activity
    ,Field
    ,Number
    ,search::Term
};

use super::{Session,TemporaryDataEntity};

pub struct SessionSearch<'a>{
    session:&'a Session<'a>
    ,collection_id:i32
    ,conditions:Vec<Condition>
}
impl<'a> SessionSearch<'a>{
    pub fn new(session:&'a Session<'a>,collection_id:i32)->SessionSearch{
        SessionSearch{
            session
            ,collection_id
            ,conditions:Vec::new()
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

    fn temporary_data_match(ent:&TemporaryDataEntity,conditions:&Vec<Condition>)->bool{
        let mut is_match=true;
        for c in conditions{
            match c{
                Condition::Activity(activity)=>{
                    if ent.activity!=*activity{
                        is_match=false;
                        break;
                    }
                }
                ,Condition::Term(cond)=>{
                    match cond{
                        Term::In(c)=>{
                            if !(ent.term_begin<*c && (ent.term_end==0||ent.term_end>*c)){
                                is_match=false;
                                break;
                            }
                        }
                        ,Term::Past(c)=>{
                            if ent.term_end>*c{
                                is_match=false;
                                break;
                            }
                        }
                        ,Term::Future(c)=>{
                            if ent.term_begin<*c{
                                is_match=false;
                                break;
                            }
                        }
                    }
                }
                ,Condition::Field(key,cond)=>{
                    if let Some(field_tmp)=ent.fields.get(key){
                        match cond{
                            Field::Match(v)=>{
                                if field_tmp!=v{
                                    is_match=false;
                                    break;
                                }
                            }
                            ,Field::Range(min,max)=>{
                                if min<field_tmp||max>field_tmp{
                                    is_match=false;
                                    break;
                                }
                            }
                            ,Field::Min(min)=>{
                                if min<field_tmp{
                                    is_match=false;
                                    break;
                                }
                            }
                            ,Field::Max(max)=>{
                                if max>field_tmp{
                                    is_match=false;
                                    break;
                                }
                            }
                            ,Field::Forward(v)=>{
                                if let Ok(str)=std::str::from_utf8(field_tmp){
                                    if !str.starts_with(v){
                                        is_match=false;
                                        break;
                                    }
                                }else{
                                    is_match=false;
                                    break;
                                }
                            }
                            ,Field::Partial(v)=>{
                                if let Ok(str)=std::str::from_utf8(field_tmp){
                                    if !str.contains(v){
                                        is_match=false;
                                        break;
                                    }
                                }else{
                                    is_match=false;
                                    break;
                                }
                            }
                            ,Field::Backward(v)=>{
                                if let Ok(str)=std::str::from_utf8(field_tmp){
                                    if !str.ends_with(v){
                                        is_match=false;
                                        break;
                                    }
                                }else{
                                    is_match=false;
                                    break;
                                }
                            }
                        }
                    }else{
                        is_match=false;
                        break;
                    }
                    
                }
                ,Condition::Or(conditions)=>{
                    is_match=Self::temporary_data_match(ent, conditions);
                    if !is_match{
                        break;
                    }
                }
                ,Condition::Row(_)=>{}
                ,Condition::LastUpdated(_)=>{}
                ,Condition::Uuid(_)=>{}
            }
        }
        is_match
    }
    pub fn result(self)->RowSet{
        if let Some(collection)=self.session.main_database.collections.get(&self.collection_id){
            let mut search=collection.begin_search();
            for c in &self.conditions{
                search=search.search(c.clone());
            }
            let mut r=search.result();
            if let Some(tmp)=self.session.temporary_data.get(&self.collection_id){
                let mut new_rows=RowSet::new();
                for row in r{
                    if let Some(ent)=tmp.get(&row){
                        if Self::temporary_data_match(&ent,&self.conditions){
                            new_rows.insert(row);
                        }
                    }else{
                        new_rows.insert(row);
                    }
                }
                r=new_rows;
            }
            return r;
        }
        RowSet::default()
    }
}