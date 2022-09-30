# semilattice-database

## Example

```rust
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
    )
]);
t.commit();

let relation=database.relation();
if let Some(p)=database.data(collection_person){
    for i in 1..=3{
        println!(
            "{},{}"
            ,p.field_str(i,"name")
            ,p.field_str(i,"birthday")
        );
        for h in relation.childs(&CollectionRow::new(collection_person,i)){
            if let Some(col)=database.data(h.collection_id()){
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
