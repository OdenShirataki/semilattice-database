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

pub struct SessionRecord<'a>{
    collection_id:i32
    ,operation:Operation<'a>
}
impl<'a> SessionRecord<'a>{
    pub fn new(
        collection_id:i32
        ,operation:Operation<'a>
    )->SessionRecord<'a>{
        SessionRecord{
            collection_id
            ,operation
        }
    }
    pub fn collection_id(&self)->i32{
        self.collection_id
    }
    pub fn operation(&self)->&Operation{
        &self.operation
    }
}

pub enum Operation<'a>{
    New{
        activity:Activity
        ,term_begin:UpdateTerm
        ,term_end:UpdateTerm
        ,fields:Vec<KeyValue<'a>>
        ,parents:UpdateParent<'a>
        ,childs:Vec<(&'a str,Vec<SessionRecord<'a>>)>
    }
    ,Update{
        row:u32
        ,activity:Activity
        ,term_begin:UpdateTerm
        ,term_end:UpdateTerm
        ,fields:Vec<KeyValue<'a>>
        ,parents:UpdateParent<'a>
        ,childs:Vec<(&'a str,Vec<SessionRecord<'a>>)>
    }
    ,Delete{row:u32}
}