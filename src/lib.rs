
use std::collections::BTreeMap;
use std::collections::HashMap;

pub use versatile_data::{
    Data
    ,IdxSized
};
pub use idx_binary::IdxBinary;

mod collection;
pub use collection::Collection;

mod relation;
pub use relation::RelationIndexes;

mod transaction;
use transaction::Transaction;

pub struct Database{
    root_dir:String
    ,collections_map:HashMap<String,u32>
    ,collections:BTreeMap<u32,Collection>
    ,relation:RelationIndexes
}
impl Database{
    pub fn new(dir:&str)->Database{
        let root_dir=if dir.ends_with("/") || dir.ends_with("\\"){
            let mut d=dir.to_string();
            d.pop();
            d
        }else{
            dir.to_string()
        };
        let db=if let (
            Ok(relation_key_names)
            ,Ok(relation_key)
            ,Ok(relation_parent)
            ,Ok(relation_child)
        )=(
            IdxBinary::new(&(root_dir.to_string()+"/relation_key_name"))
            ,IdxSized::new(&(root_dir.to_string()+"/relation_key.i"))
            ,IdxSized::new(&(root_dir.to_string()+"/relation_parent.i"))
            ,IdxSized::new(&(root_dir.to_string()+"/relation_child.i"))
        ){
            Some(Database{
                root_dir
                ,collections:BTreeMap::new()
                ,collections_map:HashMap::new()
                ,relation:RelationIndexes::new(
                    relation_key_names
                    ,relation_key
                    ,relation_parent
                    ,relation_child
                )
            })
        }else{
            None
        };
        db.expect("Fatal error: Can't Create/Open database")
    }

    fn collection_by_name_or_create(&mut self,name:&str)->u32{
        let mut max_id=0;
        let collections_dir=self.root_dir.to_string()+"/collection/";
        if let Ok(dir)=std::fs::read_dir(&collections_dir){
            for d in dir.into_iter(){
                if let Ok(d)=d{
                    if let Ok(dt)=d.file_type(){
                        if dt.is_dir(){
                            if let Some(fname)=d.path().file_name(){
                                if let Some(fname)=fname.to_str(){
                                    let s: Vec<&str>=fname.split("_").collect();
                                    if s.len()>1{
                                        if let Ok(i)=s[0].parse(){
                                            max_id=std::cmp::max(max_id,i);
                                        }
                                        if s[1]==name{
                                            if let Some(path)=d.path().to_str(){
                                                if let Some(data)=Data::new(path){
                                                    self.collections_map.insert(name.to_string(),max_id);
                                                    self.collections.insert(
                                                        max_id
                                                        ,Collection::new(max_id,data)
                                                    );
                                                    return max_id;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let collection_id=max_id+1;
        if let Some(data)=Data::new(&(collections_dir+"/"+&collection_id.to_string()+"_"+name)){
            self.collections_map.insert(name.to_string(),collection_id);
            self.collections.insert(
                collection_id
                ,Collection::new(collection_id,data)
            );
        }
        collection_id
    }
    pub fn collection(&self,id:u32)->Option<&Collection>{
        self.collections.get(&id)
    }
    pub fn collection_mut(&mut self,id:u32)->Option<&mut Collection>{
        self.collections.get_mut(&id)
    }
    pub fn collection_id(&mut self,name:&str)->u32{
        if !self.collections_map.contains_key(name){
            self.collection_by_name_or_create(name)
        }else{
            if let Some(id)=self.collections_map.get(name){
                *id
            }else{
                0
            }
        }
    }
    pub fn begin_transaction(&mut self)->Transaction{
        Transaction::new(self)
    }
}
