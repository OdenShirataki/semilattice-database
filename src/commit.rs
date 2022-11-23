use std::collections::HashMap;

use versatile_data::{
    KeyValue
    ,Activity
    ,Term
    ,Operation
};

use crate::{
    Database
    ,CollectionRow
    ,session::{
        SessionOperation
        ,SessionData
    }
};

pub fn commit(
    main_database:&mut Database
    ,session_data:&SessionData
)->Result<(),std::io::Error>{
    let mut session_collection_row_map:HashMap<u32,CollectionRow>=HashMap::new();

    let mut relation:HashMap<u32,Vec<(u32,u32,CollectionRow)>>=HashMap::new();
    for row in 1..session_data.relation.rows.sequence.max_rows(){
        if let (
            Some(session_row)
            ,Some(depend_session_row)
            ,Some(depend)
        )=(
            session_data.relation.rows.session_row.value(row)
            ,session_data.relation.rows.depend_session_row.value(row)
            ,session_data.relation.rows.depend.value(row)
        ){
            let m=relation.entry(session_row).or_insert(Vec::new());
            m.push((row,depend_session_row,depend));
        }
    }
    for session_row in 1..session_data.sequence.max_rows(){
        if let(
            Some(op)
            ,Some(collection_id)
            ,Some(collection_row)
        )=(
            session_data.operation.value(session_row)
            ,session_data.collection_id.value(session_row)
            ,session_data.collection_row.value(session_row)
        ){
            if let Some(collection)=main_database.collection_mut(collection_id){
                match op{
                    SessionOperation::New | SessionOperation::Update=>{
                        let mut fields:Vec<KeyValue>=Vec::new();
                        for (key,ref field_data) in &session_data.fields{
                            if let Some(val)=field_data.get(session_row){
                                fields.push(
                                    KeyValue::new(key,val)
                                );
                            }
                        }
                        let activity=if session_data.activity.value(session_row).unwrap()==1{
                            Activity::Active
                        }else{
                            Activity::Inactive
                        };
                        let term_begin=Term::Overwrite(
                            session_data.term_begin.value(session_row).unwrap()
                        );
                        let term_end=Term::Overwrite(
                            session_data.term_end.value(session_row).unwrap()
                        );
                        let collection_row=if collection_row==0{
                            collection.create_row(
                                &activity
                                ,&term_begin
                                ,&term_end
                                ,&fields
                            )
                        }else{
                            collection.update_row(
                                collection_row
                                ,&activity
                                ,&term_begin
                                ,&term_end
                                ,&fields
                            );
                            collection_row
                        };
                        let cr=CollectionRow::new(
                            collection_id
                            ,collection_row
                        );
                        session_collection_row_map.insert(session_row,cr);
                        if let Some(depend_rows)=relation.get(&session_row){
                            for (session_relation_row,depend_session_row,depend) in depend_rows{
                                let key=session_data.relation.rows.key.value(*session_relation_row).unwrap();
                                let key=unsafe{
                                    session_data.relation.key_names.str(key)
                                };
                                let depend_session_row=*depend_session_row;
                                if depend_session_row==0{
                                    main_database.relation.insert(
                                        key
                                        ,*depend
                                        ,cr
                                    )?;
                                }else{
                                    main_database.relation.insert(
                                        key
                                        ,*session_collection_row_map.get(&depend_session_row).unwrap()
                                        ,cr
                                    )?;
                                };
                            }
                        }
                    }
                    ,SessionOperation::Delete=>{
                        delete_recursive(main_database,&CollectionRow::new(collection_id,collection_row));
                    }
                }
            }
        }  
    }
    Ok(())
}

pub(super) fn delete_recursive(database:&mut Database,target:&CollectionRow){
    let c=database.relation.index_depend().select_by_value(target);
    for relation_row in c{
        if let Some(collection_row)=database.relation.index_pend().value(relation_row){
            delete_recursive(database,&collection_row);
            if let Some(collection)=database.collection_mut(collection_row.collection_id()){
                collection.update(&Operation::Delete{row:collection_row.row()});
            }
        }
        database.relation.delete(relation_row);
    }
}