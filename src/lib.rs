use std::collections::HashMap;

pub use versatile_data::{
    Data
};

pub struct Database{
    root_dir:String
    ,collections:HashMap<String,Data>
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
        }
    }
    pub fn create_collection(&mut self,name:&str)->Option<&mut Data>{
        if let Some(data)=Data::new(&(self.root_dir.to_string()+"/"+name)){
            self.collections.insert(name.to_string(),data);
        }
        self.collections.get_mut(name)
    }
}
