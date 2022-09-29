
use versatile_data::{
    Activity
    ,KeyValue
};

pub struct TransactionRecord<'a>{
    collection_id:u32
    ,original_row:u32
    ,activity:Activity
    ,term_begin:i64
    ,term_end:i64
    ,fields:Vec<KeyValue<'a>>
    ,childs:Vec<TransactionRecord<'a>>
}
impl TransactionRecord<'_>{
    pub fn new(
        collection_id:u32
        ,original_row:u32
        ,activity:Activity
        ,term_begin:i64
        ,term_end:i64
        ,fields:Vec<KeyValue>
    )->TransactionRecord{
        TransactionRecord{
            collection_id
            ,original_row
            ,activity
            ,term_begin
            ,term_end
            ,fields
            ,childs:Vec::new()
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
    pub fn insert(&mut self,collection_id:u32,activity:Activity,term_begin:i64,term_end:i64,fields:Vec<KeyValue<'a>>){
        let t=TransactionRecord::new(collection_id,0,activity,term_begin,term_end,fields);
        self.records.push(t);
    }
    pub fn update(&mut self,collection_id:u32,row:u32,activity:Activity,term_begin:i64,term_end:i64,fields:Vec<KeyValue<'a>>){
        let t=TransactionRecord::new(collection_id,row,activity,term_begin,term_end,fields);
        self.records.push(t);
    }
    pub fn commit(&mut self){
        for r in self.records.iter(){
            if let Some(collection)=self.database.collection_mut(r.collection_id){
                let data=collection.data_mut();
                if r.original_row==0{
                    data.insert(r.activity,r.term_begin,r.term_end,&r.fields);
                }else{
                    if let Some(row)=data.update(r.original_row,r.activity,r.term_begin,r.term_end,&r.fields)
                    {
                        for (fk,fv) in &r.fields{
                            data.update_field(row,&fk,fv);
                        }
                    }
                }
            }
        }
    }
}