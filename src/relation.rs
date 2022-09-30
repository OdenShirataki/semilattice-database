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
    pub fn childs(&self,key:&str,parent:&CollectionRow)->Vec<CollectionRow>{
        let mut ret:Vec<CollectionRow>=Vec::new();
        if let Some(key)=self.key_names.row(key.as_bytes()){
            let c=self.parent.select_by_value(parent);
            for i in c{
                if let (
                    Some(key_row)
                    ,Some(child)
                )=(
                    self.key.value(i)
                    ,self.child.value(i)
                ){
                    if key_row==key{
                        ret.push(child);
                    }
                }
                
            }
        }
        ret
    }
}