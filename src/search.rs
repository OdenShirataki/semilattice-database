use std::sync::mpsc::Sender;

use versatile_data::{
    Condition as VersatileDataCondition
    ,Activity
    ,RowSet
    ,Search as VersatileDataSearch
};
pub use versatile_data::search::{
    Term
    ,Field
    ,Number
};

use crate::{
    Database
    ,Collection
    ,RelationIndex
    ,Depend
};

#[derive(Clone)]
pub enum Condition{
    Activity(Activity)
    ,Term(Term)
    ,Row(Number)
    ,Uuid(u128)
    ,LastUpdated(Number)
    ,Field(String,Field)
    ,Narrow(Vec<Condition>)
    ,Wide(Vec<Condition>)
    ,Depend(Depend)
}

pub struct Search{
    collection_id:i32
    ,conditions:Vec<Condition>
}
impl Search{
    pub fn new(collection:&Collection)->Self{
        Search{
            collection_id:collection.id()
            ,conditions:Vec::new()
        }
    }
    pub fn search(mut self,condition:Condition)->Self{
        self.conditions.push(condition);
        self
    }
    pub fn default(mut self)->Self{
        self.conditions.push(Condition::Term(Term::In(chrono::Local::now().timestamp())));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
    }
    pub fn depend(mut self,condition:Depend)->Self{
        self.conditions.push(Condition::Depend(condition));
        self
    }
    pub fn field(self,field_name:impl Into<String>,condition:Field)->Self{
        self.search(Condition::Field(field_name.into(),condition))
    }
    
    fn exec_cond(collection:&Collection,relation:&RelationIndex,condtion:&Condition,tx:Sender<RowSet>){
        match condtion{
            Condition::Activity(c)=>{
                VersatileDataSearch::search_exec_cond(&collection.data,&VersatileDataCondition::Activity(*c),tx);
            }
            ,Condition::Term(c)=>{
                VersatileDataSearch::search_exec_cond(&collection.data,&VersatileDataCondition::Term(c.clone()),tx);
            }
            ,Condition::Row(c)=>{
                VersatileDataSearch::search_exec_cond(&collection.data,&VersatileDataCondition::Row(c.clone()),tx);
            }
            ,Condition::Uuid(c)=>{
                VersatileDataSearch::search_exec_cond(&collection.data,&VersatileDataCondition::Uuid(c.clone()),tx);
            }
            ,Condition::LastUpdated(c)=>{
                VersatileDataSearch::search_exec_cond(&collection.data,&VersatileDataCondition::LastUpdated(c.clone()),tx);
            }
            ,Condition::Field(key,condition)=>{
                VersatileDataSearch::search_exec_cond(&collection.data,&VersatileDataCondition::Field(key.to_owned(),condition.clone()),tx);
            }
            ,Condition::Depend(depend)=>{
                let rel=relation.pends(depend.key(),depend.collection_row());
                let collection_id=collection.id();
                std::thread::spawn(move||{
                    let mut tmp=RowSet::default();
                    for r in rel{
                        if r.collection_id()==collection_id{
                            tmp.insert(r.row());
                        }
                    }
                    let tx=tx.clone();
                    tx.send(tmp).unwrap();
                });
            }
            ,Condition::Narrow(conditions)=>{
                let (tx_inner, rx) = std::sync::mpsc::channel();
                for c in conditions{
                    let tx_inner=tx_inner.clone();
                    Self::exec_cond(collection,relation,c,tx_inner);
                }
                drop(tx_inner);
                std::thread::spawn(move||{
                    let mut is_1st=true;
                    let mut tmp=RowSet::default();
                    for mut rs in rx{
                        if is_1st{
                            tmp=rs;
                            is_1st=false;
                        }else{
                            tmp=tmp.intersection(&mut rs).map(|&x|x).collect();
                        }
                    }
                    tx.send(tmp).unwrap();
                });
            }
            ,Condition::Wide(conditions)=>{
                let (tx_inner, rx) = std::sync::mpsc::channel();
                for c in conditions{
                    let tx_inner=tx_inner.clone();
                    Self::exec_cond(collection,relation,c,tx_inner);
                }
                drop(tx_inner);
                std::thread::spawn(move||{
                    let mut tmp=RowSet::default();
                    for ref mut rs in rx{
                        tmp.append(rs);
                    }
                    tx.send(tmp).unwrap();
                });
            }
            
        }
    }
    pub(super) fn exec(&self,database:&Database)->RowSet{
        let mut rows=RowSet::default();
        if let Some(collection)=database.collection(self.collection_id){
            if self.conditions.len()>0{
                let (tx, rx) = std::sync::mpsc::channel();
                for c in &self.conditions{
                    Self::exec_cond(collection,&database.relation,c,tx.clone());
                }
                drop(tx);
                let mut fst=true;
                for rs in rx{
                    if fst{
                        rows=rs;
                        fst=false;
                    }else{
                        rows=rows.intersection(&rs).map(|&x|x).collect()
                    }
                }
            }else{
                for row in collection.data.all(){
                    rows.insert(row);
                }
            }
        }
        
        rows
    }
}