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

pub enum Record{
    New{
        collection_id:i32
        ,activity:Activity
        ,term_begin:Term
        ,term_end:Term
        ,fields:Vec<KeyValue>
        ,depends:Depends
        ,pends:Vec<(String,Vec<Record>)>
    }
    ,Update{
        collection_id:i32
        ,row:u32
        ,activity:Activity
        ,term_begin:Term
        ,term_end:Term
        ,fields:Vec<KeyValue>
        ,depends:Depends
        ,pends:Vec<(String,Vec<Record>)>
    }
    ,Delete{
        collection_id:i32
        ,row:u32
    }
}