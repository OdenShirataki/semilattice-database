#[cfg(test)]

#[test]
fn it_works() {
    use versatile_data::prelude::*;
    use semilattice_database::{
        Database
        ,TransactionRecord
        ,CollectionRow
    };

    let dir="D:/sl-test/";

    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::create_dir_all(dir).unwrap();
    }else{
        std::fs::create_dir_all(dir).unwrap();
    }
    let mut database=Database::new(dir);
    
    let collection_person=database.collection_id("person");
    let collection_history=database.collection_id("history");

    let mut t=database.begin_transaction();
    t.update(&mut vec![
        TransactionRecord::new(
            collection_person
            ,Update::New
            ,Activity::Active
            ,0
            ,0
            ,vec![
                ("name","Joe".to_string())
                ,("birthday","1972-08-02".to_string())
            ]
            ,vec![]
            ,vec![("history",vec![
                TransactionRecord::new(
                    collection_history
                    ,Update::New
                    ,Activity::Active
                    ,0
                    ,0
                    ,vec![
                        ("date","1972-08-02".to_string())
                        ,("event","Birth".to_string())
                    ]
                    ,vec![]
                    ,vec![]
                )
                ,TransactionRecord::new(
                    collection_history
                    ,Update::New
                    ,Activity::Active
                    ,0
                    ,0
                    ,vec![
                        ("date","1999-12-31".to_string())
                        ,("event","Mariage".to_string())
                    ]
                    ,vec![]
                    ,vec![]
                )
            ])]
        )
        ,TransactionRecord::new(
            collection_person
            ,Update::New
            ,Activity::Active
            ,0
            ,0
            ,vec![
                ("name","Tom".to_string())
                ,("birthday","2000-12-12".to_string())
            ]
            ,vec![]
            ,vec![("history",vec![
                TransactionRecord::new(
                    collection_history
                    ,Update::New
                    ,Activity::Active
                    ,0
                    ,0
                    ,vec![
                        ("date","2000-12-12".to_string())
                        ,("event","Birth".to_string())
                    ]
                    ,vec![]
                    ,vec![]
                )
            ])]
        )
        ,TransactionRecord::new(
            collection_person
            ,Update::New
            ,Activity::Active
            ,0
            ,0
            ,vec![
                ("name","Billy".to_string())
                ,("birthday","1982-03-03".to_string())
            ]
            ,vec![]
            ,vec![]
        )
    ]);
    t.commit();

    t.delete(collection_person,2);
    t.commit();

    let relation=database.relation();
    if let Some(p)=database.collection(collection_person){
        for i in 1..=3{
            println!(
                "{},{}"
                ,p.field_str(i,"name")
                ,p.field_str(i,"birthday")
            );
            for h in relation.childs("history",&CollectionRow::new(collection_person,i)){
                if let Some(col)=database.collection(h.collection_id()){
                    let row=h.row();
                    println!(
                        " {} : {}"
                        ,col.field_str(row,"date")
                        ,col.field_str(row,"event")
                    );
                    
                }
            }
        }
    }
    
    let test1=database.collection_id("test1");
    let mut t=database.begin_transaction();
    let range=1..=10;
    for i in range.clone(){
        t.update(&mut vec![
            TransactionRecord::new(
                test1
                ,Update::New
                ,Activity::Active
                ,0
                ,0
                ,vec![
                    ("num",i.to_string())
                    ,("num_by3",(i*3).to_string())
                ]
                ,vec![]
                ,vec![]
            )
        ]);
    }
    t.update(&mut vec![
        TransactionRecord::new(test1,Update::Row(3),Activity::Inactive,0,0,vec![],vec![],vec![])
    ]);
    t.commit();
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
                ,t1.field_str(i,"num")
                ,t1.field_str(i,"num_by3")
            );
        }
        assert_eq!(sum,55.0);

        let r=t1
            .search(Condition::Field("num".to_string(),Field::Range(b"3".to_vec(),b"8".to_vec())))
            .search_default()   //Automatic execution of the following two lines
            //.search(SearchCondition::Term(Term::In(chrono::Local::now().timestamp())))
            //.search(SearchCondition::Activity(Activity::Active))
            .result()
        ;
        println!("{:?}",r);
    }
}
