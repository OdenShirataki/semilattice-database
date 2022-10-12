use std::collections::HashMap;
use versatile_data::{
    IdxSized
    ,KeyValue
    ,UpdateTerm
    ,Activity
    ,FieldData
    ,Operation
};
use super::{
    Database
    ,CollectionRow
};

mod operation;
pub use operation::{
    Record
    ,SessionOperation
    ,UpdateDepends
};

mod sequence_number;
use sequence_number::SequenceNumber;

mod relation;
use relation::SessionRelation;

struct SessionData{
    sequence_number:SequenceNumber
    ,sequence:IdxSized<usize>
    ,collection_id:IdxSized<i32>
    ,collection_row:IdxSized<u32>
    ,operation:IdxSized<SessionOperation>
    ,activity: IdxSized<u8>
    ,term_begin: IdxSized<i64>
    ,term_end: IdxSized<i64>
    ,fields:HashMap<String,FieldData>
    ,relation:SessionRelation
}
pub struct Session<'a>{
    main_database:&'a mut Database
    ,session_dir:String
    ,data:Option<SessionData>
}
impl<'a> Session<'a>{
    pub fn new(
        main_database:&'a mut Database
        ,session_name:&'a str
    )->Result<Session,std::io::Error>{
        let session_dir=main_database.root_dir().to_string()+"/sessions/"+session_name;
        if !std::path::Path::new(&session_dir).exists(){
            std::fs::create_dir_all(&session_dir).unwrap();
        }
        Ok(Session{
            main_database
            ,session_dir:session_dir.to_string()
            ,data:Some(Self::new_data(&session_dir)?)
        })
    }
    
    fn new_data(session_dir:&str)->Result<SessionData,std::io::Error>{
        Ok(SessionData{
            sequence_number:SequenceNumber::new(&(session_dir.to_string()+"/sequece_number.i"))?
            ,sequence:IdxSized::new(&(session_dir.to_string()+"/sequence.i"))?
            ,collection_id:IdxSized::new(&(session_dir.to_string()+"/collection_id.i"))?
            ,collection_row:IdxSized::new(&(session_dir.to_string()+"/collection_row.i"))?
            ,operation:IdxSized::new(&(session_dir.to_string()+"/operation.i"))?
            ,activity:IdxSized::new(&(session_dir.to_string()+"/activity.i"))?
            ,term_begin:IdxSized::new(&(session_dir.to_string()+"/term_begin.i"))?
            ,term_end:IdxSized::new(&(session_dir.to_string()+"/term_end.i"))?
            ,fields:HashMap::new()
            ,relation:SessionRelation::new(&session_dir)
        })
    }
    pub fn public(&mut self){
        if let Some(ref data)=self.data{
            let mut session_collection_row_map:HashMap<u32,CollectionRow>=HashMap::new();

            let mut relation:HashMap<u32,Vec<(u32,u32,CollectionRow)>>=HashMap::new();
            for row in 1..data.relation.rows.sequence.max_rows(){
                if let (
                    Some(session_row)
                    ,Some(depend_session_row)
                    ,Some(depend)
                )=(
                    data.relation.rows.session_row.value(row)
                    ,data.relation.rows.depend_session_row.value(row)
                    ,data.relation.rows.depend.value(row)
                ){
                    let m=relation.entry(session_row).or_insert(Vec::new());
                    m.push((row,depend_session_row,depend));
                }
            }
            for session_row in 1..data.sequence.max_rows(){
                if let (
                    Some(op)
                    ,Some(collection_id)
                    ,Some(collection_row)
                )=(
                    data.operation.value(session_row)
                    ,data.collection_id.value(session_row)
                    ,data.collection_row.value(session_row)
                ){
                    if let Some(collection)=self.main_database.collection_mut(collection_id){
                        match op{
                            SessionOperation::New | SessionOperation::Update=>{
                                let mut fields:Vec<KeyValue>=Vec::new();
                                for (ref key,ref field_data) in &data.fields{
                                    if let Some(val)=field_data.get(session_row){
                                        fields.push((
                                            &key
                                            ,val.to_owned()
                                        ));
                                    }
                                }
                                let activity=if data.activity.value(session_row).unwrap()==1{
                                    Activity::Active
                                }else{
                                    Activity::Inactive
                                };
                                let term_begin=UpdateTerm::Overwrite(
                                    data.term_begin.value(session_row).unwrap()
                                );
                                let term_end=UpdateTerm::Overwrite(
                                    data.term_end.value(session_row).unwrap()
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
                                        let key=data.relation.rows.key.value(*session_relation_row).unwrap();
                                        let key=data.relation.key_names.str(key);
                                        let depend_session_row=*depend_session_row;
                                        if depend_session_row==0{
                                            self.main_database.relation.insert(
                                                key
                                                ,*depend
                                                ,cr
                                            );
                                        }else{
                                            self.main_database.relation.insert(
                                                key
                                                ,*session_collection_row_map.get(&depend_session_row).unwrap()
                                                ,cr
                                            );
                                        };
                                    }
                                }
                            }
                            ,SessionOperation::Delete=>{
                                Self::delete_recursive(self.main_database,&CollectionRow::new(collection_id,collection_row));
                            }
                        }
                    }
                }    
            }
            self.data=None;
            if std::path::Path::new(&self.session_dir).exists(){
                std::fs::remove_dir_all(&self.session_dir).unwrap();
            }
        }
    }
    fn delete_recursive(database:&mut Database,target:&CollectionRow){
        let c=database.relation.index_depend().select_by_value(target);
        for relation_row in c{
            if let Some(collection_row)=database.relation.index_pend().value(relation_row){
                Self::delete_recursive(database,&collection_row);
                if let Some(collection)=database.collection_mut(collection_row.collection_id()){
                    collection.update(&Operation::Delete{row:collection_row.row()});
                }
            }
            database.relation.delete(relation_row);
        }
    }
    fn update_row(
        session_dir:&str
        ,data:&mut SessionData
        ,session_row:u32
        ,collection_row:u32
        ,activity:&Activity
        ,term_begin:&UpdateTerm
        ,term_end:&UpdateTerm
        ,fields:&Vec<KeyValue>
    ){
        data.collection_row.update(session_row,collection_row);
        data.activity.update(session_row,*activity as u8);
        data.term_begin.update(session_row,if let UpdateTerm::Overwrite(term_begin)=term_begin{
            *term_begin
        }else{
            chrono::Local::now().timestamp()
        });
        data.term_end.update(session_row,if let UpdateTerm::Overwrite(term_end)=term_end{
            *term_end
        }else{
            0
        });
        for (key,value) in fields{
            let field=if data.fields.contains_key(*key){
                data.fields.get_mut(*key).unwrap()
            }else{
                let dir_name=session_dir.to_string()+"/fields/"+key+"/";
                std::fs::create_dir_all(dir_name.to_owned()).unwrap();
                if std::path::Path::new(&dir_name).exists(){
                    let field=FieldData::new(&dir_name).unwrap();
                    data.fields.entry(String::from(*key)).or_insert(
                        field
                    );
                }
                data.fields.get_mut(*key).unwrap()
            };
            field.update(session_row,value);
        }
    }
    fn incidentally_depend(
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
    fn update_recursive(
        master_database:&Database
        ,data:&mut SessionData
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
                        data.collection_id.update(session_row,*collection_id);
                        data.operation.update(session_row,SessionOperation::New);
                        Self::update_row(
                            session_dir
                            ,data
                            ,session_row
                            ,0
                            ,activity
                            ,term_begin
                            ,term_end
                            ,fields
                        );
                        if let UpdateDepends::Overwrite(depends)=depends{
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
                        Self::incidentally_depend(
                            data
                            ,sequence
                            ,session_row
                            ,incidentally_depend
                        );
                        for (key,records) in pends{
                            Self::update_recursive(master_database,data,session_dir,sequence,records,Some((*key,session_row)));
                        }
                    }
                    ,Record::Update{
                        collection_id,row,activity,term_begin,term_end,fields,depends,pends
                    }=>{
                        data.collection_id.update(session_row,*collection_id);
                        data.operation.update(session_row,SessionOperation::Update);
                        Self::update_row(
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
                            UpdateDepends::Inherit=>{
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
                            ,UpdateDepends::Overwrite(depends)=>{   
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
                        Self::incidentally_depend(
                            data
                            ,sequence
                            ,session_row
                            ,incidentally_depend
                        );
                        for (key,records) in pends{
                            Self::update_recursive(master_database,data,session_dir,sequence,records,Some((*key,session_row)));
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
    pub fn update(&mut self,records:Vec::<Record>){
        match self.data{
            Some(ref mut data)=>{
                let sequence=data.sequence_number.next();
                Self::update_recursive(&self.main_database,data,&self.session_dir,sequence,&records,None);
            }
            ,None=>{
                if let Ok(data)=Self::new_data(&self.session_dir){
                    self.data=Some(data);
                    if let Some(ref mut data)=self.data{
                        let sequence=data.sequence_number.next();
                        Self::update_recursive(&self.main_database,data,&self.session_dir,sequence,&records,None);
                    }
                }
            }
        }
    }
}