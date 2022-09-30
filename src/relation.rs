use idx_binary::IdxBinary;
use versatile_data::IdxSized;

use crate::collection::CollectionRow;

pub struct RelationIndexes{
    key_names:IdxBinary
    ,key:IdxSized<u32>
    ,parent:IdxSized<CollectionRow>
    ,child:IdxSized<CollectionRow>
}
impl RelationIndexes{
    pub fn new(
        key_names:IdxBinary
        ,key:IdxSized<u32>
        ,parent:IdxSized<CollectionRow>
        ,child:IdxSized<CollectionRow>
    )->RelationIndexes{
        RelationIndexes{
            key_names
            ,key
            ,parent
            ,child
        }
    }
    pub fn insert(&mut self,relation_key:&str,parent:CollectionRow,child:CollectionRow){
        if let Some(key_id)=self.key_names.entry(relation_key.as_bytes()){
            self.key.insert(key_id);
            self.parent.insert(parent);
            self.child.insert(child);
        }
    }
    pub fn childs(&self,parent:&CollectionRow)->Vec<CollectionRow>{
        let mut ret:Vec<CollectionRow>=Vec::new();
        let c=self.parent.select_by_value(parent);
        for i in c{
            if let Some(child)=self.child.value(i){
                ret.push(child);
            }   
        }
        ret
    }
}