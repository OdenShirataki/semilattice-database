use idx_binary::IdxBinary;
use versatile_data::IdxSized;

#[derive(Clone,Copy,Default)]
pub struct CollectionRow{
    collection:u32
    ,row:u32
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
}