use std::cmp::Ordering;
use idx_binary::IdxBinary;
use versatile_data::IdxSized;

#[derive(Clone,Copy,Default,Debug)]
pub struct CollectionRow{
    collection_id:u32
    ,row:u32
}
impl PartialOrd for CollectionRow {
    fn partial_cmp(&self, other: &CollectionRow) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CollectionRow{
    fn cmp(&self,other:&CollectionRow)->Ordering{
        if self.collection_id==other.collection_id{
            if self.row==other.row{
                Ordering::Equal
            }else if self.row>other.row{
                Ordering::Greater
            }else{
                Ordering::Less
            }
        }else if self.collection_id>other.collection_id{
            Ordering::Greater
        }else{
            Ordering::Less
        }
    }
}
impl PartialEq for CollectionRow {
    fn eq(&self, other: &CollectionRow) -> bool {
        self.collection_id == other.collection_id && self.row == other.row
    }
}
impl Eq for CollectionRow {}

impl CollectionRow{
    pub fn new(
        collection_id:u32
        ,row:u32
    )->CollectionRow{
        CollectionRow{
            collection_id
            ,row
        }
    }
    pub fn collection_id(&self)->u32{
        self.collection_id
    }
    pub fn row(&self)->u32{
        self.row
    }
}
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
        println!("Relation insert {},{:?},{:?}",relation_key,parent,child);
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