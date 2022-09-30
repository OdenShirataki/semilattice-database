
use versatile_data::{
    Activity
    ,KeyValue
};

use crate::CollectionRow;

#[derive(Debug)]
pub enum UpdateOrNew{
    New
    ,Update(u32)
}
pub struct TransactionRecord<'a>{
    collection_id:u32
    ,update_or_new:UpdateOrNew
    ,activity:Activity
    ,term_begin:i64
    ,term_end:i64
    ,fields:Vec<KeyValue<'a>>
    ,childs:Vec<(&'a str,Vec<TransactionRecord<'a>>)>
}
impl<'a> TransactionRecord<'a>{
    pub fn new(
        collection_id:u32
        ,update_or_new:UpdateOrNew
        ,activity:Activity
        ,term_begin:i64
        ,term_end:i64
        ,fields:Vec<KeyValue<'a>>
        ,childs:Vec<(&'a str,Vec<TransactionRecord<'a>>)>
    )->TransactionRecord<'a>{
        TransactionRecord{
            collection_id
            ,update_or_new
            ,activity
            ,term_begin
            ,term_end
            ,fields
            ,childs
        }
    }
}

pub struct Transaction<'a>{
    database:&'a mut super::Database
    ,records:Vec<TransactionRecord<'a>>
}
impl<'a> Transaction<'a>{
    pub fn new(database:&'a mut super::Database)->Transaction{
        Transaction{
            database
            ,records:Vec::new()
        }
    }
    pub fn update(&mut self,records:&mut Vec::<TransactionRecord<'a>>){
        self.records.append(records);
    }
 
    pub fn commit(&mut self){
        Self::marge_data(&mut self.database,&self.records,None);
    }

    fn marge_data(database:&mut super::Database,records:&Vec::<TransactionRecord>,parent:Option<(&str,CollectionRow)>){
        for r in records.iter(){
            if let Some(collection)=database.collection_mut(r.collection_id){
                let data=collection.data_mut();
                if let Some(row)=if let UpdateOrNew::Update(original_row)=r.update_or_new{
                    data.update(original_row,r.activity,r.term_begin,r.term_end,&r.fields);
                    Some(original_row)
                }else{
                    data.insert(r.activity,r.term_begin,r.term_end,&r.fields)
                }{
                    if let Some((relation_key,parent_collection_row))=parent{
                        database.relation_mut().insert(
                            relation_key
                            ,parent_collection_row
                            ,CollectionRow::new(r.collection_id,row)
                        );
                    }
                    for (relation_key,childs) in &r.childs{
                        Self::marge_data(
                            database
                            ,&childs
                            ,Some((
                                relation_key
                                ,CollectionRow::new(r.collection_id,row)
                            ))
                        );
                    }
                }
            }
        }
    }
}