use std::collections::HashMap;

use versatile_data::{
    Term
    ,Activity
    ,KeyValue
    ,FieldData
};

use crate::{
    Database
    ,session::{
        SessionData
        ,TemporaryData
        ,SessionOperation
        ,TemporaryDataEntity
    }
    ,Record
    ,Depends
    ,CollectionRow
};

pub fn fn_incidentally_depend(
    data:&mut SessionData
    ,sequence:usize
    ,pend_session_row:u32
    ,incidentally_depend:Option<(&str,u32)>
){
    if let Some((relation_key,depend_session_row))=incidentally_depend{
        let depend=CollectionRow::new(
            data.collection_id.value(depend_session_row).unwrap()
            ,data.collection_row.value(depend_session_row).unwrap()
        );
        data.relation.insert(
            sequence
            ,relation_key
            ,pend_session_row
            ,depend_session_row
            ,depend
        );
    }
}

pub fn update_row(
    session_dir:&str
    ,data:&mut SessionData
    ,session_row:u32
    ,collection_row:u32
    ,activity:&Activity
    ,term_begin:i64
    ,term_end:i64
    ,fields:&Vec<KeyValue>
){
    data.collection_row.update(session_row,collection_row);
    data.activity.update(session_row,*activity as u8);
    data.term_begin.update(session_row,term_begin);
    data.term_end.update(session_row,term_end);
    for kv in fields{
        let key=kv.key();
        let field=if data.fields.contains_key(key){
            data.fields.get_mut(key).unwrap()
        }else{
            let dir_name=session_dir.to_string()+"/fields/"+key+"/";
            std::fs::create_dir_all(dir_name.to_owned()).unwrap();
            if std::path::Path::new(&dir_name).exists(){
                let field=FieldData::new(&dir_name).unwrap();
                data.fields.entry(String::from(key)).or_insert(
                    field
                );
            }
            data.fields.get_mut(key).unwrap()
        };
        field.update(session_row,kv.value());
    }
}

pub(super) fn update_recursive(
    master_database:&Database
    ,data:&mut SessionData
    ,temporary_data:&mut TemporaryData
    ,session_dir:&str
    ,sequence:usize
    ,records:&Vec::<Record>
    ,incidentally_depend:Option<(&str,u32)>
){
    for record in records{
        if let Some(session_row)=data.sequence.insert(sequence){
            data.collection_row.resize_to(session_row).unwrap();
            data.operation.resize_to(session_row).unwrap();
            data.activity.resize_to(session_row).unwrap();
            data.term_begin.resize_to(session_row).unwrap();
            data.term_end.resize_to(session_row).unwrap();

            match record{
                Record::New{
                    collection_id,activity,term_begin,term_end,fields,depends,pends
                }=>{
                    let term_begin=if let Term::Overwrite(term_begin)=term_begin{
                        *term_begin
                    }else{
                        chrono::Local::now().timestamp()
                    };
                    let term_end=if let Term::Overwrite(term_end)=term_end{
                        *term_end
                    }else{
                        0
                    };
                    data.collection_id.update(session_row,*collection_id);
                    data.operation.update(session_row,SessionOperation::New);
                    update_row(
                        session_dir
                        ,data
                        ,session_row
                        ,0
                        ,activity
                        ,term_begin
                        ,term_end
                        ,fields
                    );
                    if let Depends::Overwrite(depends)=depends{
                        for (key,depend) in depends{
                            data.relation.insert(
                                sequence
                                ,key
                                ,session_row
                                ,0
                                ,*depend
                            );
                        }
                    }
                    fn_incidentally_depend(
                        data
                        ,sequence
                        ,session_row
                        ,incidentally_depend
                    );
                    for pend in pends{
                        update_recursive(master_database,data,temporary_data,session_dir,sequence,pend.records(),Some((pend.key(),session_row)));
                    }
                }
                ,Record::Update{
                    collection_id,row,activity,term_begin,term_end,fields,depends,pends
                }=>{
                    let term_begin=if let Term::Overwrite(term_begin)=term_begin{
                        *term_begin
                    }else{
                        chrono::Local::now().timestamp()
                    };
                    let term_end=if let Term::Overwrite(term_end)=term_end{
                        *term_end
                    }else{
                        0
                    };
                    let col=temporary_data.entry(*collection_id).or_insert(HashMap::new());
                    let entity=col.entry(*row).or_insert(TemporaryDataEntity{
                        activity:*activity
                        ,term_begin
                        ,term_end
                        ,fields:HashMap::new()
                    });
                    for kv in fields{
                        entity.fields.insert(kv.key().into(),kv.value().into());
                    }

                    data.collection_id.update(session_row,*collection_id);
                    data.operation.update(session_row,SessionOperation::Update);
                    update_row(
                        session_dir
                        ,data
                        ,session_row
                        ,*row
                        ,activity
                        ,term_begin
                        ,term_end
                        ,fields
                    );
                    match depends{
                        Depends::Default=>{
                            let depends=master_database.relation().index_pend().select_by_value(&CollectionRow::new(*collection_id,*row));
                            for i in depends{
                                let depend=master_database.relation().depend(i).unwrap();
                                data.relation.insert(
                                    sequence
                                    ,master_database.relation().key(i)
                                    ,session_row
                                    ,0
                                    ,depend
                                );
                            }
                        }
                        ,Depends::Overwrite(depends)=>{   
                            for (key,depend) in depends{
                                data.relation.insert(
                                    sequence
                                    ,key
                                    ,session_row
                                    ,0
                                    ,*depend
                                );
                            }
                        }
                    }
                    fn_incidentally_depend(
                        data
                        ,sequence
                        ,session_row
                        ,incidentally_depend
                    );
                    for pend in pends{
                        update_recursive(master_database,data,temporary_data,session_dir,sequence,pend.records(),Some((pend.key(),session_row)));
                    }
                }
                ,Record::Delete{collection_id,row}=>{
                    data.collection_id.update(session_row,*collection_id);
                    data.collection_row.update(session_row,*row);
                    data.operation.update(session_row,SessionOperation::Delete);
                }
            }
        }
    }
}