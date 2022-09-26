use std::collections::BTreeMap;
use std::collections::HashMap;

pub use versatile_data::{
    Data
};

pub struct Collection{
    id:u32
    ,data:Data
}
impl Collection{
    pub fn new(id:u32,data:Data)->Collection{
        Collection{
            id
            ,data
        }
    }
    pub fn data(&self)->&Data{
        &self.data
    }
    pub fn data_mut(&mut self)->&mut Data{
        &mut self.data
    }
}

pub struct Database{
    root_dir:String
    ,collections:HashMap<String,Collection>
    ,collections_id_map:BTreeMap<u32,String>
}
impl Database{
    pub fn new(dir:&str)->Database{
        Database{
            root_dir:if dir.ends_with("/") || dir.ends_with("\\"){
                let mut d=dir.to_string();
                d.pop();
                d
            }else{
                dir.to_string()
            }
            ,collections:HashMap::new()
            ,collections_id_map:BTreeMap::new()
        }
    }
    pub fn collection(&mut self,name:&str)->Option<&mut Collection>{
        if !self.collections.contains_key(name){
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
                self.collections_id_map.insert(collection_id,name.to_string());
                self.collections.insert(name.to_string(),Collection::new(collection_id,data));
            }
        }
        self.collections.get_mut(name)
    }
}
