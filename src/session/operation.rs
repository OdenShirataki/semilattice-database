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

pub enum Depends<'a>{
    Inherit
    ,Overwrite(Vec<(&'a str,CollectionRow)>)
}

pub enum Record<'a>{
    New{
        collection_id:i32
        ,activity:Activity
        ,term_begin:Term
        ,term_end:Term
        ,fields:Vec<KeyValue<'a>>
        ,depends:Depends<'a>
        ,pends:Vec<(&'a str,Vec<Record<'a>>)>
    }
    ,Update{
        collection_id:i32
        ,row:u32
        ,activity:Activity
        ,term_begin:Term
        ,term_end:Term
        ,fields:Vec<KeyValue<'a>>
        ,depends:Depends<'a>
        ,pends:Vec<(&'a str,Vec<Record<'a>>)>
    }
    ,Delete{
        collection_id:i32
        ,row:u32
    }
}