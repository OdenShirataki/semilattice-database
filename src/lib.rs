use std::collections::BTreeMap;
use std::collections::HashMap;

pub use idx_binary::IdxBinary;

pub use versatile_data::{
    KeyValue
    ,IdxSized
    ,Activity
    ,Term
    ,RowSet
};
use versatile_data::Data;

mod collection;
pub use collection::{
    Collection
    ,CollectionRow
};

mod relation;
pub use relation::RelationIndex;

mod session;
pub use session::{
    Session
    ,Record
    ,Depends
    ,Pend
    ,search as session_search
};

pub mod search;
pub use search::{
    Condition
    ,Search
};

mod commit;

mod update;

pub mod prelude;

pub struct Database{
    root_dir:String
    ,collections_map:HashMap<String,i32>
    ,collections:BTreeMap<i32,Collection>
    ,relation:RelationIndex
}
impl Database{
    pub fn new(dir:&str)->Result<Self,std::io::Error>{
        let root_dir=if dir.ends_with("/") || dir.ends_with("\\"){
            let mut d=dir.to_string();
            d.pop();
            d
        }else{
            dir.to_string()
        };
        let mut collections_map=HashMap::new();
        let mut collections=BTreeMap::new();
        let collections_dir=root_dir.to_string()+"/collection/";
        if let Ok(dir)=std::fs::read_dir(&collections_dir){
            for d in dir.into_iter(){
                let d=d.unwrap();
                let dt=d.file_type().unwrap();
                if dt.is_dir(){
                    if let Some(fname)=d.path().file_name(){
                        if let Some(fname)=fname.to_str(){
                            let s: Vec<&str>=fname.split("_").collect();
                            if s.len()>1{
                                if let Some(path)=d.path().to_str(){
                                    if let Ok(collection_id)=s[0].parse::<i32>(){
                                        let name=s[1];
                                        let data=Collection::new(Data::new(path)?,collection_id,name);
                                        collections_map.insert(name.to_string(),collection_id);
                                        collections.insert(
                                            collection_id
                                            ,data
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(Self{
            root_dir:root_dir.to_string()
            ,collections
            ,collections_map
            ,relation:RelationIndex::new(&root_dir)?
        })
    }
    pub fn root_dir(&self)->&str{
        &self.root_dir
    }
    fn session_dir(&self,session_name:&str)->String{
        self.root_dir.to_string()+"/sessions/"+session_name
    }
    pub fn session(&self,session_name:&str)->Result<Session,std::io::Error>{
        let session_dir=self.session_dir(session_name);
        if !std::path::Path::new(&session_dir).exists(){
            std::fs::create_dir_all(&session_dir)?;
        }
        Session::new(self,session_name)
    }
    pub fn blank_session(&self)->Result<Session,std::io::Error>{
       self.session("")
    }
    pub fn commit(&mut self,session:&mut Session)->Result<(),std::io::Error>{
        if let Some(ref mut data)=session.session_data{
            commit::commit(self,data)?;
            self.session_clear(session)?;
        }
        Ok(())
    }
    pub fn session_clear(&self,session:&mut Session)->Result<(),std::io::Error>{
        let session_dir=self.session_dir(session.name());
        session.session_data=None;
        if std::path::Path::new(&session_dir).exists(){
            std::fs::remove_dir_all(&session_dir)?;
        }
        Ok(())
    }
    pub fn session_start(&self,session:&mut Session){
        let session_dir=self.session_dir(session.name());
        if let Ok(session_data)=Session::new_data(&session_dir){
            session.session_data=Some(session_data);
        }
    }
    pub fn session_restart(&self,session:&mut Session)->Result<(),std::io::Error>{
        self.session_clear(session)?;
        self.session_start(session);
        Ok(())
    }
    pub fn update(&self,session:&mut Session,records:Vec::<Record>)->Result<(),std::io::Error>{
        let session_dir=self.session_dir(session.name());
        match session.session_data{
            Some(ref mut data)=>{
                let sequence=data.sequence_number.next();
                update::update_recursive(self,data,&mut session.temporary_data,&session_dir,sequence,&records,None)?;
            }
            ,None=>{
                if let Ok(data)=Session::new_data(&session_dir){
                    session.session_data=Some(data);
                    if let Some(ref mut data)=session.session_data{
                        let sequence=data.sequence_number.next();
                        update::update_recursive(self,data,&mut session.temporary_data,&session_dir,sequence,&records,None)?;
                    }
                }
            }
        }
        Ok(())
    }
    fn collection_by_name_or_create(&mut self,name:&str)->Result<i32,std::io::Error>{
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
                                        let data=Collection::new(Data::new(path)?,max_id,name);
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
        let data=Collection::new(Data::new(&(collections_dir+"/"+&collection_id.to_string()+"_"+name))?,collection_id,name);
        self.collections_map.insert(name.to_string(),collection_id);
        self.collections.insert(
            collection_id
            ,data
        );
        Ok(collection_id)
    }
    pub fn collection(&self,id:i32)->Option<&Collection>{
        self.collections.get(&id)
    }
    pub fn collection_mut(&mut self,id:i32)->Option<&mut Collection>{
        self.collections.get_mut(&id)
    }
    pub fn collection_id(&self,name:&str)->Option<i32>{
        if self.collections_map.contains_key(name){
            Some(*self.collections_map.get(name).unwrap())
        }else{
            None
        }
    }
    pub fn collection_id_or_create(&mut self,name:&str)->Result<i32,std::io::Error>{
        if self.collections_map.contains_key(name){
            Ok(*self.collections_map.get(name).unwrap())
        }else{
            self.collection_by_name_or_create(name)
        }
    }
    pub fn relation(&self)->&RelationIndex{
        &self.relation
    }
    pub fn search(&self,colletion:&Collection)->Search{
        Search::new(colletion)
    }
    pub fn result(&self,search:&Search)->RowSet{
        search.exec(self)
    }
}
