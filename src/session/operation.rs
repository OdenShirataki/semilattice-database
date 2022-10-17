use versatile_data::{
    KeyValue
    ,Term
    ,Activity
};
use crate::CollectionRow;

#[derive(Clone,Copy,Default,PartialEq,Eq,PartialOrd,Ord)]
pub enum SessionOperation{
    #[default]
    New
    ,Update
    ,Delete
}

pub enum Depends{
    Inherit
    ,Overwrite(Vec<(String,CollectionRow)>)
}

pub struct Pend{
    key:String
    ,records:Vec<Record>
}
impl Pend{
    pub fn new(key:impl Into<String>,records:Vec<Record>)->Pend{
        Pend{
            key:key.into()
            ,records
        }
    }
    pub fn key(&self)->&str{
        &self.key
    }
    pub fn records(&self)->&Vec<Record>{
        &self.records
    }
}
pub enum Record{
    New{
        collection_id:i32
        ,activity:Activity
        ,term_begin:Term
        ,term_end:Term
        ,fields:Vec<KeyValue>
        ,depends:Depends
        ,pends:Vec<Pend>
    }
    ,Update{
        collection_id:i32
        ,row:u32
        ,activity:Activity
        ,term_begin:Term
        ,term_end:Term
        ,fields:Vec<KeyValue>
        ,depends:Depends
        ,pends:Vec<Pend>
    }
    ,Delete{
        collection_id:i32
        ,row:u32
    }
}