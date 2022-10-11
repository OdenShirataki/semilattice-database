use versatile_data::{
    Operation
    ,Activity
    ,UpdateTerm
    ,KeyValue
};
use crate::{
    Database
    ,CollectionRow
};

pub enum UpdateParent<'a>{
    Inherit
    ,Overwrite(Vec<(&'a str,CollectionRow)>)
}

pub enum TransactionOperation<'a>{
    New{
        activity:Activity
        ,term_begin:UpdateTerm
        ,term_end:UpdateTerm
        ,fields:Vec<KeyValue<'a>>
        ,parents:UpdateParent<'a>
        ,childs:Vec<(&'a str,Vec<TransactionRecord<'a>>)>
    }
    ,Update{
        row:u32
        ,activity:Activity
        ,term_begin:UpdateTerm
        ,term_end:UpdateTerm
        ,fields:Vec<KeyValue<'a>>
        ,parents:UpdateParent<'a>
        ,childs:Vec<(&'a str,Vec<TransactionRecord<'a>>)>
    }
    ,Delete{row:u32}
}
impl<'a> TransactionOperation<'a>{
    pub fn data_operation(&self)->Operation{
        match self{
            TransactionOperation::New{
                activity, term_begin, term_end, fields, parents, childs
            }=>{
                Operation::New{
                    activity:*activity
                    ,term_begin:*term_begin
                    ,term_end:*term_end
                    ,fields:fields.to_vec()
                }
            }
            ,TransactionOperation::Update{
                row, activity, term_begin, term_end, fields, parents, childs
            } =>{
                Operation::Update{
                    row:*row
                    ,activity:*activity
                    ,term_begin:*term_begin
                    ,term_end:*term_end
                    ,fields:fields.to_vec()
                }
            }
            ,TransactionOperation::Delete{row}=>{
                Operation::Delete{
                    row:*row
                }
            }
        }
    }
    pub fn parents(&self)->Option<&UpdateParent<'a>>{
        match self{
            TransactionOperation::New{
                activity, term_begin, term_end, fields, parents, childs
            }=>{
                Some(parents)
            }
            ,TransactionOperation::Update{
                row, activity, term_begin, term_end, fields, parents, childs
            } =>{
                Some(parents)
            }
            ,TransactionOperation::Delete{row}=>{
                None
            }
        }
    }
    pub fn childs(&self)->Option<&Vec<(&'a str,Vec<TransactionRecord<'a>>)>>{
        match self{
            TransactionOperation::New{
                activity, term_begin, term_end, fields, parents, childs
            }=>{
                Some(childs)
            }
            ,TransactionOperation::Update{
                row, activity, term_begin, term_end, fields, parents, childs
            } =>{
                Some(childs)
            }
            ,TransactionOperation::Delete{row}=>{
                None
            }
        }
    }
}
pub struct TransactionRecord<'a>{
    collection_id:i32
    ,operation:TransactionOperation<'a>
}
impl<'a> TransactionRecord<'a>{
    pub fn new(
        collection_id:i32
        ,operation:TransactionOperation<'a>
    )->TransactionRecord<'a>{
        TransactionRecord{
            collection_id
            ,operation
        }
    }
    pub fn collection_id(&self)->i32{
        self.collection_id
    }
    pub fn operation(&self)->&TransactionOperation{
        &self.operation
    }
}

pub struct Transaction<'a>{
    database:&'a mut Database
    ,records:Vec<TransactionRecord<'a>>
    ,deletes:Vec<CollectionRow>
}
impl<'a> Transaction<'a>{
    pub fn new(database:&'a mut Database)->Transaction{
        Transaction{
            database
            ,records:Vec::new()
            ,deletes:Vec::new()
        }
    }
    pub fn update(&mut self,records:&mut Vec::<TransactionRecord<'a>>){
        self.records.append(records);
    }
    pub fn delete(&mut self,collection_id:i32,row:u32){
        self.deletes.push(CollectionRow::new(collection_id,row));
    }
    pub fn commit(&mut self){
        Self::register_recursive(&mut self.database,&self.records,None);
        for i in &self.deletes{
            Self::delete_recursive(&mut self.database,&i);
        }
        self.records=Vec::new();
        self.deletes=Vec::new();
    }

    fn delete_recursive(database:&mut Database,target:&CollectionRow){
        let c=database.relation.index_parent().select_by_value(target);
        for relation_row in c{
            if let Some(child)=database.relation.index_child().value(relation_row){
                Self::delete_recursive(database,&child);
                if let Some(collection)=database.collection_mut(child.collection_id()){
                    collection.update(&Operation::Delete{row:child.row()});
                }
            }
            database.relation.delete(relation_row);
        }
    }
    fn register_recursive(
        database:&mut Database
        ,records:&Vec::<TransactionRecord>
        ,incidentally_parent:Option<(&str,CollectionRow)>
    ){
        for r in records.iter(){
            if let Some(collection)=database.collection_mut(r.collection_id){
                //TODO:処理が雑い。
                let data_operation=r.operation.data_operation();
                let row=collection.update(&data_operation);
                if let Some(UpdateParent::Overwrite(ref parents))=r.operation.parents(){
                    if let Operation::Update{
                        row,activity,term_begin,term_end,ref fields
                    }=data_operation{
                        let relations=database.relation().index_child().select_by_value(
                            &CollectionRow::new(r.collection_id,row)
                        );
                        for i in relations{
                            database.relation_mut().delete(i);
                        }
                    }
                    for (relation_key,parent) in parents{
                        database.relation_mut().insert(
                            relation_key
                            ,*parent
                            ,CollectionRow::new(r.collection_id,row)
                        );
                    }
                }
                
                if let Some((relation_key,parent_collection_row))=incidentally_parent{
                    database.relation_mut().insert(
                        relation_key
                        ,parent_collection_row
                        ,CollectionRow::new(r.collection_id,row)
                    );
                }
                if let Some(childs)=r.operation.childs(){
                    for (relation_key,childs) in childs{
                        Self::register_recursive(
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