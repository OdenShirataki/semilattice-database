use std::cmp::Ordering;

#[derive(Clone,Copy,Default,Debug)]
pub struct CollectionRow{
    collection_id:i32
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
        collection_id:i32
        ,row:u32
    )->CollectionRow{
        CollectionRow{
            collection_id
            ,row
        }
    }
    pub fn collection_id(&self)->i32{
        self.collection_id
    }
    pub fn row(&self)->u32{
        self.row
    }
}