use idx_binary::IdxBinary;
use versatile_data::IdxSized;

use crate::collection::CollectionRow;

struct RelationIndexRows{
    key:IdxSized<u32>
    ,parent:IdxSized<CollectionRow>
    ,child:IdxSized<CollectionRow>
}
pub struct RelationIndex{
    key_names:IdxBinary
    ,rows:RelationIndexRows
}
impl RelationIndex{
    pub fn new(
        key_names:IdxBinary
        ,key:IdxSized<u32>
        ,parent:IdxSized<CollectionRow>
        ,child:IdxSized<CollectionRow>
    )->RelationIndex{
        RelationIndex{
            key_names
            ,rows:RelationIndexRows{
                key
                ,parent
                ,child
            }
        }
    }
    pub fn insert(&mut self,relation_key:&str,parent:CollectionRow,child:CollectionRow){
        if let Some(key_id)=self.key_names.entry(relation_key.as_bytes()){
            self.rows.key.insert(key_id);
            self.rows.parent.insert(parent);
            self.rows.child.insert(child);
        }
    }
    pub fn delete(&mut self,row:u32){
        self.rows.key.delete(row);
        self.rows.parent.delete(row);
        self.rows.child.delete(row);
    }
    pub fn childs_all(&self,parent:&CollectionRow)->Vec<CollectionRow>{
        let mut ret:Vec<CollectionRow>=Vec::new();
        let c=self.rows.parent.select_by_value(parent);
        for i in c{
            if let Some(child)=self.rows.child.value(i){
                ret.push(child);
            }
        }
        ret
    }
    pub fn childs(&self,key:&str,parent:&CollectionRow)->Vec<CollectionRow>{
        let mut ret:Vec<CollectionRow>=Vec::new();
        if let Some(key)=self.key_names.row(key.as_bytes()){
            let c=self.rows.parent.select_by_value(parent);
            for i in c{
                if let (
                    Some(key_row)
                    ,Some(child)
                )=(
                    self.rows.key.value(i)
                    ,self.rows.child.value(i)
                ){
                    if key_row==key{
                        ret.push(child);
                    }
                }
                
            }
        }
        ret
    }
    pub fn index_parent(&self)->&IdxSized<CollectionRow>{
        &self.rows.parent
    }
    pub fn index_child(&self)->&IdxSized<CollectionRow>{
        &self.rows.child
    }
    
}