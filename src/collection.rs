use std::cmp::Ordering;

use versatile_data::Data;

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
    pub fn id(&self)->u32{
        self.id
    }
    pub fn data(&self)->&Data{
        &self.data
    }
    pub fn data_mut(&mut self)->&mut Data{
        &mut self.data
    }
}

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