#[cfg(test)]

#[test]
fn it_works() {
    use semilattice_database::prelude::*;

    let dir="./sl-test/";

    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::create_dir_all(dir).unwrap();
    }else{
        std::fs::create_dir_all(dir).unwrap();
    }
    let mut database=Database::new(dir).unwrap();

    let collection_person=database.collection_id_or_create("person").unwrap();
    let collection_history=database.collection_id_or_create("history").unwrap();

    if let Ok(mut sess)=database.session("test"){
        database.update(&mut sess,vec![
            Record::New{
                collection_id:collection_person
                ,activity:Activity::Active
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![
                    KeyValue::new("name","Joe")
                    ,KeyValue::new("birthday","1972-08-02")
                ]
                ,depends:Depends::Overwrite(vec![])
                ,pends:vec![Pend::new("history",vec![
                    Record::New{
                        collection_id:collection_history
                        ,activity:Activity::Active
                        ,term_begin:Term::Defalut
                        ,term_end:Term::Defalut
                        ,fields:vec![
                            KeyValue::new("date","1972-08-02")
                            ,KeyValue::new("event","Birth")
                        ]
                        ,depends:Depends::Default
                        ,pends:vec![]
                    }
                    ,Record::New{
                        collection_id:collection_history
                        ,activity:Activity::Active
                        ,term_begin:Term::Defalut
                        ,term_end:Term::Defalut
                        ,fields:vec![
                            KeyValue::new("date","1999-12-31")
                            ,KeyValue::new("event","Mariage")
                        ]
                        ,depends:Depends::Default
                        ,pends:vec![]
                    }
                ])]
            }
            ,Record::New{
                collection_id:collection_person
                ,activity:Activity::Active
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![
                    KeyValue::new("name","Tom")
                    ,KeyValue::new("birthday","2000-12-12")
                ]
                ,depends:Depends::Default
                ,pends:vec![Pend::new("history",vec![
                    Record::New{
                        collection_id:collection_history
                        ,activity:Activity::Active
                        ,term_begin:Term::Defalut
                        ,term_end:Term::Defalut
                        ,fields:vec![
                            KeyValue::new("date","2000-12-12")
                            ,KeyValue::new("event","Birth")
                        ]
                        ,depends:Depends::Default
                        ,pends:vec![]
                    }
                ])]
            }
            ,Record::New{
                collection_id:collection_person
                ,activity:Activity::Active
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![
                    KeyValue::new("name","Billy")
                    ,KeyValue::new("birthday","1982-03-03")
                ]
                ,depends:Depends::Default
                ,pends:vec![]
            }
        ]).unwrap();
        database.commit(&mut sess).unwrap();
    }
    
    if let (
        Some(person)
        ,Some(history)
    )=(
        database.collection(collection_person)
        ,database.collection(collection_history)
    ){
        let search=database.search(person);
        for i in database.result(&search){
            println!(
                "{},{}"
                ,std::str::from_utf8(person.field_bytes(i,"name")).unwrap()
                ,std::str::from_utf8(person.field_bytes(i,"birthday")).unwrap()
            );
            let search=database.search(history).depend(
                search::Depend::new("history",CollectionRow::new(collection_person,i))
            );
            for h in database.result(&search){
                println!(
                    " {} : {}"
                    ,std::str::from_utf8(history.field_bytes(h,"date")).unwrap()
                    ,std::str::from_utf8(history.field_bytes(h,"event")).unwrap()
                );
            }
        }
    }
    if let Ok(mut sess)=database.session("test"){
        database.update(&mut sess,vec![
            Record::Update{
                collection_id:collection_person
                ,row:1
                ,activity:Activity::Active
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![KeyValue::new("name","Renamed Joe")]
                ,depends:Depends::Default
                ,pends:vec![]
            }
        ]).unwrap();
    }
    if let Ok(mut sess)=database.session("test"){
        let search=sess.begin_search(collection_person).search_activity(Activity::Active);
        for r in search.result(&database){
            println!(
                "{},{}"
                ,std::str::from_utf8(sess.field_bytes(&database,collection_person,r,"name")).unwrap()
                ,std::str::from_utf8(sess.field_bytes(&database,collection_person,r,"birthday")).unwrap()
            );
        }
        database.commit(&mut sess).unwrap();
    }

    let test1=database.collection_id_or_create("test1").unwrap();
    let range=1..=10;
    if let Ok(mut sess)=database.session("test"){
        for i in range.clone(){
            database.update(&mut sess,vec![
                Record::New{
                    collection_id:test1
                    ,activity:Activity::Active
                    ,term_begin:Term::Defalut
                    ,term_end:Term::Defalut
                    ,fields:vec![
                        KeyValue::new("num",i.to_string())
                        ,KeyValue::new("num_by3",(i*3).to_string())
                    ]
                    ,depends:Depends::Overwrite(vec![])
                    ,pends:vec![]
                }
            ]).unwrap();
        }
        database.commit(&mut sess).unwrap();
    }
    
    if let Ok(mut sess)=database.session("test"){
        database.update(&mut sess,vec![
            Record::Update{
                collection_id:test1
                ,row:3
                ,activity:Activity::Inactive
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![]
                ,depends:Depends::Overwrite(vec![])
                ,pends:vec![]
            }
        ]).unwrap();
        database.commit(&mut sess).unwrap();
    }

    if let Some(t1)=database.collection(test1){
        let mut sum=0.0;
        for i in range.clone(){
            sum+=t1.field_num(i,"num");
            println!(
                "{},{},{},{},{},{},{},{}"
                ,t1.serial(i)
                ,if t1.activity(i)==Activity::Active{
                    "Active"
                }else{
                    "Inactive"
                }
                ,t1.uuid_str(i)
                ,t1.last_updated(i)
                ,t1.term_begin(i)
                ,t1.term_end(i)
                ,std::str::from_utf8(t1.field_bytes(i,"num")).unwrap()
                ,std::str::from_utf8(t1.field_bytes(i,"num_by3")).unwrap()
            );
        }
        assert_eq!(sum,55.0);
    }
}
