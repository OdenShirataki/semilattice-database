#[cfg(test)]

#[test]
fn it_works() {
    use versatile_data::prelude::*;
    use semilattice_database::{
        Database
        ,Record
        ,CollectionRow
        ,Depends
    };

    let dir="./sl-test/";

    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::create_dir_all(dir).unwrap();
    }else{
        std::fs::create_dir_all(dir).unwrap();
    }
    let mut database=Database::new(dir).unwrap();

    let collection_person=database.collection_id("person").unwrap();
    let collection_history=database.collection_id("history").unwrap();

    if let Ok(mut sess)=database.session("test"){
        sess.update(vec![
            Record::New{
                collection_id:collection_person
                ,activity:Activity::Active
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![
                    ("name","Joe".to_string().into_bytes())
                    ,("birthday","1972-08-02".to_string().into_bytes())
                ]
                ,depends:Depends::Overwrite(vec![])
                ,pends:vec![("history",vec![
                    Record::New{
                        collection_id:collection_history
                        ,activity:Activity::Active
                        ,term_begin:Term::Defalut
                        ,term_end:Term::Defalut
                        ,fields:vec![
                            ("date","1972-08-02".to_string().into_bytes())
                            ,("event","Birth".to_string().into_bytes())
                        ]
                        ,depends:Depends::Overwrite(vec![])
                        ,pends:vec![]
                    }
                    ,Record::New{
                        collection_id:collection_history
                        ,activity:Activity::Active
                        ,term_begin:Term::Defalut
                        ,term_end:Term::Defalut
                        ,fields:vec![
                            ("date","1999-12-31".as_bytes().to_vec())
                            ,("event","Mariage".as_bytes().to_vec())
                        ]
                        ,depends:Depends::Overwrite(vec![])
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
                    ("name","Tom".as_bytes().to_vec())
                    ,("birthday","2000-12-12".as_bytes().to_vec())
                ]
                ,depends:Depends::Overwrite(vec![])
                ,pends:vec![("history",vec![
                    Record::New{
                        collection_id:collection_history
                        ,activity:Activity::Active
                        ,term_begin:Term::Defalut
                        ,term_end:Term::Defalut
                        ,fields:vec![
                            ("date","2000-12-12".as_bytes().to_vec())
                            ,("event","Birth".as_bytes().to_vec())
                        ]
                        ,depends:Depends::Overwrite(vec![])
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
                    ("name","Billy".as_bytes().to_vec())
                    ,("birthday","1982-03-03".as_bytes().to_vec())
                ]
                ,depends:Depends::Overwrite(vec![])
                ,pends:vec![]
            }
        ]);
        sess.public();
    }
    
    let relation=database.relation();
    if let Some(p)=database.collection(collection_person){
        for i in 1..=3{
            println!(
                "{},{}"
                ,p.field_str(i,"name")
                ,p.field_str(i,"birthday")
            );
            for h in relation.pends("history",&CollectionRow::new(collection_person,i)){
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
    if let Ok(mut sess)=database.session("test"){
        sess.update(vec![
            Record::Update{
                collection_id:collection_person
                ,row:1
                ,activity:Activity::Active
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![("name","Renamed Joe".to_string().into_bytes())]
                ,depends:Depends::Inherit
                ,pends:vec![]
            }
        ]);
    }
    if let Ok(mut sess)=database.session("test"){
        let search=sess.begin_search(collection_person).search_activity(Activity::Active);
        for r in search.result(){
            println!(
                "{},{}"
                ,sess.field_str(collection_person,r,"name")
                ,sess.field_str(collection_person,r,"birthday")
            );
        }
        sess.public();
    }

    let test1=database.collection_id("test1").unwrap();
    let range=1..=10;
    if let Ok(mut sess)=database.session("test"){
        for i in range.clone(){
            sess.update(vec![
                Record::New{
                    collection_id:test1
                    ,activity:Activity::Active
                    ,term_begin:Term::Defalut
                    ,term_end:Term::Defalut
                    ,fields:vec![
                        ("num",i.to_string().into_bytes())
                        ,("num_by3",(i*3).to_string().into_bytes())
                    ]
                    ,depends:Depends::Overwrite(vec![])
                    ,pends:vec![]
                }
            ]);
        }
        sess.public();
    }
    
    if let Ok(mut sess)=database.session("test"){
        sess.update(vec![
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
        ]);
        sess.public();
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
                ,t1.field_str(i,"num")
                ,t1.field_str(i,"num_by3")
            );
        }
        assert_eq!(sum,55.0);

        let r=t1
            .search_field("num",Field::Range(b"3".to_vec(),b"8".to_vec()))
            .search_default()   //Automatic execution of the following two lines
            //.search_term(Term::In(chrono::Local::now().timestamp()))
            //.search_activity(Activity::Active)
            .result()
        ;
        println!("{:?}",r);
    }
}
