use file_mmap::FileMmap;
use idx_binary::IdxBinary;
use versatile_data::IdxSized;

use crate::collection::CollectionRow;

struct RelationIndexRows{
    key:IdxSized<u32>
    ,parent:IdxSized<CollectionRow>
    ,child:IdxSized<CollectionRow>
}
pub struct RelationIndex{
    fragment:Fragment
    ,key_names:IdxBinary
    ,rows:RelationIndexRows
}
impl RelationIndex{
    pub fn new(
        root_dir:&str
    )->Result<RelationIndex,std::io::Error>{
        Ok(RelationIndex{
            key_names:IdxBinary::new(&(root_dir.to_string()+"/relation_key_name"))?
            ,fragment:Fragment::new(&(root_dir.to_string()+"/relation.f"))?
            ,rows:RelationIndexRows{
                key:IdxSized::new(&(root_dir.to_string()+"/relation_key.i"))?
                ,parent:IdxSized::new(&(root_dir.to_string()+"/relation_parent.i"))?
                ,child:IdxSized::new(&(root_dir.to_string()+"/relation_child.i"))?
            }
        })
    }
    pub fn insert(&mut self,relation_key:&str,parent:CollectionRow,child:CollectionRow){
        if let Some(key_id)=self.key_names.entry(relation_key.as_bytes()){
            if let Some(row)=self.fragment.pop(){
                self.rows.key.update(row,key_id);
                self.rows.parent.update(row,parent);
                self.rows.child.update(row,child);
            }else{
                self.rows.key.insert(key_id);
                self.rows.parent.insert(parent);
                self.rows.child.insert(child);
            }
        }
    }
    pub fn delete(&mut self,row:u32){
        self.rows.key.delete(row);
        self.rows.parent.delete(row);
        self.rows.child.delete(row);
        self.fragment.insert_blank(row);
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

const U32SIZE:usize=std::mem::size_of::<u32>();
struct Fragment{
    filemmap:FileMmap
    ,blank_list: Vec<u32>
    ,blank_count: u32
}
impl Fragment{
    pub fn new(path:&str) -> Result<Fragment,std::io::Error>{
        let filemmap=FileMmap::new(path,U32SIZE as u64)?;
        let blank_list=filemmap.offset(0) as *mut u32;
        let blank_count:u32=(filemmap.len() / U32SIZE as u64 - 1) as u32;       
        Ok(Fragment{
            filemmap
            ,blank_list:unsafe {Vec::from_raw_parts(blank_list,1,0)}
            ,blank_count
        })
    }
    pub fn insert_blank(&mut self,id:u32){
        self.filemmap.append(
            &[0,0,0,0]
        );
        unsafe{
            *(self.blank_list.as_ptr() as *mut u32).offset(self.blank_count as isize)=id;
        }
        self.blank_count+=1;
    }
    pub fn pop(&mut self)->Option<u32>{
        if self.blank_count>0{
            let p=unsafe{
                (self.blank_list.as_ptr() as *mut u32).offset(self.blank_count as isize - 1)
            };
            let last=unsafe{*p};
            unsafe{*p=0;}
            let _=self.filemmap.set_len(self.filemmap.len()-U32SIZE as u64);
            self.blank_count-=1;
            Some(last)
        }else{
            None
        }
    }
}