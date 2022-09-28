use versatile_data::Activity;
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

pub struct TransactionRecord{
    original_row:u32
    ,activity:Activity
    ,term_begin:i64
    ,term_end:i64
    ,fields:Vec<(String,String)>
}
impl TransactionRecord{
    pub fn new(
        original_row:u32
        ,activity:Activity
        ,term_begin:i64
        ,term_end:i64
        ,fields:Vec<(String,String)>
    )->TransactionRecord{
            TransactionRecord{
                original_row
                ,activity
                ,term_begin
                ,term_end
                ,fields
            }
    }
}

pub struct Transaction<'a>{
    collection_id:u32
    ,database:&'a mut super::Database
    ,records:Vec<TransactionRecord>
}
impl<'a> Transaction<'a>{
    pub fn new(collection_id:u32,database:&'a mut super::Database)->Transaction{
        Transaction{
            collection_id
            ,database
            ,records:Vec::new()
        }
    }
    pub fn insert(&mut self,activity:Activity,term_begin:i64,term_end:i64,fields:Vec<(String,String)>){
        let t=TransactionRecord::new(0,activity,term_begin,term_end,fields);
        self.records.push(t);
    }
    pub fn update(&mut self,row:u32,activity:Activity,term_begin:i64,term_end:i64,fields:Vec<(String,String)>){
        let t=TransactionRecord::new(row,activity,term_begin,term_end,fields);
        self.records.push(t);
    }
    pub fn commit(&mut self){
        if let Some(mut collection)=self.database.collection_by_id_mut(self.collection_id){
            let data=collection.data_mut();
            for record in self.records.iter(){
                if record.original_row==0{
                    data.insert(record.activity,record.term_begin,record.term_end,&record.fields);
                }else{
                    if let Some(row)=data.update(record.original_row,record.activity,record.term_begin,record.term_end)
                    {
                        for (fk,fv) in &record.fields{
                            data.update_field(row,&fk,fv);
                        }
                    }
                }
            }
        }
    }
}