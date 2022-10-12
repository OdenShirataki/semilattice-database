use versatile_data::{
    KeyValue
    ,UpdateTerm
    ,Activity
};
use crate::{
    CollectionRow
};

#[derive(Clone,Copy,Default,PartialEq,Eq,PartialOrd,Ord)]
pub enum SessionOperation{
    #[default]
    New
    ,Update
    ,Delete
}

pub enum UpdateParent<'a>{
    Inherit
    ,Overwrite(Vec<(&'a str,CollectionRow)>)
}

pub enum Record<'a>{
    New{
        collection_id:i32
        ,activity:Activity
        ,term_begin:UpdateTerm
        ,term_end:UpdateTerm
        ,fields:Vec<KeyValue<'a>>
        ,parents:UpdateParent<'a>
        ,childs:Vec<(&'a str,Vec<Record<'a>>)>
    }
    ,Update{
        collection_id:i32
        ,row:u32
        ,activity:Activity
        ,term_begin:UpdateTerm
        ,term_end:UpdateTerm
        ,fields:Vec<KeyValue<'a>>
        ,parents:UpdateParent<'a>
        ,childs:Vec<(&'a str,Vec<Record<'a>>)>
    }
    ,Delete{
        collection_id:i32
        ,row:u32
    }
}