use std::collections::BTreeMap;
use std::collections::HashMap;

pub use versatile_data::{
    Data as Collection
    ,IdxSized
    ,Update
};
pub use idx_binary::IdxBinary;

mod collection;
pub use collection::{
    CollectionRow
};

mod relation;
pub use relation::{
    RelationIndex
};

mod transaction;
use transaction::Transaction;
pub use transaction::{
    TransactionRecord
    ,UpdateParent
};

pub struct Database{
    root_dir:String
    ,collections_map:HashMap<String,u32>
    ,collections:BTreeMap<u32,Collection>
    ,relation:RelationIndex
}
impl Database{
    pub fn new(dir:&str)->Result<Database,std::io::Error>{
        let root_dir=if dir.ends_with("/") || dir.ends_with("\\"){
            let mut d=dir.to_string();
            d.pop();
            d
        }else{
            dir.to_string()
        };
        Ok(Database{
            root_dir:root_dir.to_string()
            ,collections:BTreeMap::new()
            ,collections_map:HashMap::new()
            ,relation:RelationIndex::new(&root_dir)?
        })
    }

    fn collection_by_name_or_create(&mut self,name:&str)->Result<u32,std::io::Error>{
        let mut max_id=0;
        let collections_dir=self.root_dir.to_string()+"/collection/";
        if let Ok(dir)=std::fs::read_dir(&collections_dir){
            for d in dir.into_iter(){
                let d=d.unwrap();
                let dt=d.file_type().unwrap();
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
                                        let data=Collection::new(path)?;
                                        self.collections_map.insert(name.to_string(),max_id);
                                        self.collections.insert(
                                            max_id
                                            ,data
                                        );
                                        return Ok(max_id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let collection_id=max_id+1;
        let data=Collection::new(&(collections_dir+"/"+&collection_id.to_string()+"_"+name))?;
        self.collections_map.insert(name.to_string(),collection_id);
        self.collections.insert(
            collection_id
            ,data
        );
        Ok(collection_id)
    }
    pub fn collection(&self,id:u32)->Option<&Collection>{
        self.collections.get(&id)
    }
    pub fn collection_mut(&mut self,id:u32)->Option<&mut Collection>{
        self.collections.get_mut(&id)
    }
    pub fn collection_id(&mut self,name:&str)->Result<u32,std::io::Error>{
        if self.collections_map.contains_key(name){
            Ok(*self.collections_map.get(name).unwrap())
        }else{
            self.collection_by_name_or_create(name)
        }
    }
    pub fn begin_transaction(&mut self)->Transaction{
        Transaction::new(self)
    }
    pub fn relation_mut(&mut self)->&mut RelationIndex{
        &mut self.relation
    }
    pub fn relation(&self)->&RelationIndex{
        &self.relation
    }
    pub fn childs(&self,key:&str,parent:&CollectionRow){
        self.relation.childs(key,parent);
    }
}
