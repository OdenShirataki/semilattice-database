# semilattice-database-session

## Example

```rust
use std::sync::Arc;

use semilattice_database_session::*;

let dir = "./sl-test/";

if std::path::Path::new(dir).exists() {
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::create_dir_all(dir).unwrap();
} else {
    std::fs::create_dir_all(dir).unwrap();
}
let mut database = SessionDatabase::new(dir.into(), None).unwrap();

let collection_admin = database.collection_id_or_create("admin").unwrap();

if let Ok(mut sess) = database.session("creatre_account_1st", None) {
    database
        .update(
            &mut sess,
            vec![SessionRecord::New {
                collection_id: collection_admin,
                record: Record {
                    fields: vec![
                        KeyValue::new("id", "test".to_owned()),
                        KeyValue::new("password", "test".to_owned()),
                    ],
                    ..Record::default()
                },
                depends: Depends::Overwrite(vec![]),
                pends: vec![],
            }],
        )
        .unwrap();
    database.commit(&mut sess).unwrap();
}

let collection_login = database.collection_id_or_create("login").unwrap();
if let Ok(mut sess) = database.session("login", None) {
    let search = sess
        .begin_search(collection_admin)
        .search_field("id", search::Field::Match(Arc::new(b"test".to_vec())))
        .search_field("password", search::Field::Match(Arc::new(b"test".to_vec())));
    for row in search.result(&database, &vec![]).unwrap() {
        assert!(row >= 0);
        database
            .update(
                &mut sess,
                vec![SessionRecord::New {
                    collection_id: collection_login,
                    record: Record {
                        fields: vec![],
                        ..Record::default()
                    },
                    depends: Depends::Overwrite(vec![(
                        "admin".to_owned(),
                        CollectionRow::new(collection_admin, row as u32),
                    )]),
                    pends: vec![],
                }],
            )
            .unwrap();
    }
}
if let Ok(sess) = database.session("login", None) {
    let search = sess.begin_search(collection_login);
    for row in search.result(&database, &vec![]).unwrap() {
        let depends = database.depends_with_session(
            Some("admin"),
            collection_login,
            row as u32,
            Some(&sess),
        );
        for d in depends {
            let collection_id = d.collection_id();
            if let Some(collection) = database.collection(collection_id) {
                let search = sess
                    .begin_search(collection_id)
                    .search_row(search::Number::In(vec![d.row() as isize]));
                for row in search.result(&database, &vec![]).unwrap() {
                    println!(
                        "login id : {}",
                        std::str::from_utf8(collection.field_bytes(row as u32, "id")).unwrap()
                    );
                }
            }
        }
    }
}

let collection_person = database.collection_id_or_create("person").unwrap();
let collection_history = database.collection_id_or_create("history").unwrap();

if let Ok(mut sess) = database.session("test", None) {
    database
        .update(
            &mut sess,
            vec![
                SessionRecord::New {
                    collection_id: collection_person,
                    record: Record {
                        fields: vec![
                            KeyValue::new("name", "Joe"),
                            KeyValue::new("birthday", "1972-08-02"),
                        ],
                        ..Record::default()
                    },
                    depends: Depends::Overwrite(vec![]),
                    pends: vec![Pend::new(
                        "history",
                        vec![
                            SessionRecord::New {
                                collection_id: collection_history,
                                record: Record {
                                    fields: vec![
                                        KeyValue::new("date", "1972-08-02"),
                                        KeyValue::new("event", "Birth"),
                                    ],
                                    ..Record::default()
                                },
                                depends: Depends::Default,
                                pends: vec![],
                            },
                            SessionRecord::New {
                                collection_id: collection_history,
                                record: Record {
                                    fields: vec![
                                        KeyValue::new("date", "1999-12-31"),
                                        KeyValue::new("event", "Mariage"),
                                    ],
                                    ..Record::default()
                                },
                                depends: Depends::Default,
                                pends: vec![],
                            },
                        ],
                    )],
                },
                SessionRecord::New {
                    collection_id: collection_person,
                    record: Record {
                        fields: vec![
                            KeyValue::new("name", "Tom"),
                            KeyValue::new("birthday", "2000-12-12"),
                        ],
                        ..Record::default()
                    },
                    depends: Depends::Default,
                    pends: vec![Pend::new(
                        "history",
                        vec![SessionRecord::New {
                            collection_id: collection_history,
                            record: Record {
                                fields: vec![
                                    KeyValue::new("date", "2000-12-12"),
                                    KeyValue::new("event", "Birth"),
                                ],
                                ..Record::default()
                            },
                            depends: Depends::Default,
                            pends: vec![],
                        }],
                    )],
                },
                SessionRecord::New {
                    collection_id: collection_person,
                    record: Record {
                        fields: vec![
                            KeyValue::new("name", "Billy"),
                            KeyValue::new("birthday", "1982-03-03"),
                        ],
                        ..Record::default()
                    },
                    depends: Depends::Default,
                    pends: vec![],
                },
            ],
        )
        .unwrap();
    database.commit(&mut sess).unwrap();
}

if let (Some(person), Some(history)) = (
    database.collection(collection_person),
    database.collection(collection_history),
) {
    let mut search = database.search(collection_person);
    let result = search.result(&database).unwrap();
    let person_rows = if let Some(r) = result.read().unwrap().as_ref() {
        r.sort(
            &database,
            &vec![Order::Asc(OrderKey::Field("birthday".to_owned()))],
        )
    } else {
        vec![]
    };
    for i in person_rows {
        println!(
            "{},{}",
            std::str::from_utf8(person.field_bytes(i, "name")).unwrap(),
            std::str::from_utf8(person.field_bytes(i, "birthday")).unwrap()
        );
        let mut seach = database.search(collection_history);
        seach.search(Condition::Depend(
            Some("history".to_owned()),
            CollectionRow::new(collection_person, i),
        ));
        let result = search.result(&database).unwrap();
        if let Some(result) = Arc::clone(&result).read().unwrap().as_ref() {
            for h in result.rows() {
                println!(
                    " {} : {}",
                    std::str::from_utf8(history.field_bytes(*h, "date")).unwrap(),
                    std::str::from_utf8(history.field_bytes(*h, "event")).unwrap()
                );
            }
        }
    }
}
if let Ok(mut sess) = database.session("test", None) {
    database
        .update(
            &mut sess,
            vec![SessionRecord::Update {
                collection_id: collection_person,
                row: 1,
                record: Record {
                    fields: vec![KeyValue::new("name", "Renamed Joe")],
                    ..Record::default()
                },
                depends: Depends::Default,
                pends: vec![],
            }],
        )
        .unwrap();
}
if let Ok(mut sess) = database.session("test", None) {
    let search = sess
        .begin_search(collection_person)
        .search_activity(Activity::Active);
    for r in search.result(&database, &vec![]).unwrap() {
        println!(
            "session_search : {},{}",
            std::str::from_utf8(sess.field_bytes(&database, collection_person, r, "name"))
                .unwrap(),
            std::str::from_utf8(sess.field_bytes(&database, collection_person, r, "birthday"))
                .unwrap()
        );
    }
    database.commit(&mut sess).unwrap();
}
let test1 = database.collection_id_or_create("test1").unwrap();
let range = 1u32..=10;
if let Ok(mut sess) = database.session("test", None) {
    for i in range.clone() {
        database
            .update(
                &mut sess,
                vec![SessionRecord::New {
                    collection_id: test1,
                    record: Record {
                        fields: vec![
                            KeyValue::new("num", i.to_string()),
                            KeyValue::new("num_by3", (i * 3).to_string()),
                        ],
                        ..Record::default()
                    },
                    depends: Depends::Overwrite(vec![]),
                    pends: vec![],
                }],
            )
            .unwrap();
    }
    database.commit(&mut sess).unwrap();
}

if let Ok(mut sess) = database.session("test", None) {
    database
        .update(
            &mut sess,
            vec![SessionRecord::Update {
                collection_id: test1,
                row: 3,
                record: Record {
                    fields: vec![],
                    ..Record::default()
                },
                depends: Depends::Overwrite(vec![]),
                pends: vec![],
            }],
        )
        .unwrap();
    database.commit(&mut sess).unwrap();
}

if let Some(t1) = database.collection(test1) {
    let mut sum = 0.0;
    for i in range.clone() {
        sum += t1.field_num(i, "num");
        println!(
            "{},{},{},{},{},{},{},{}",
            t1.serial(i),
            if let Some(Activity::Active) = t1.activity(i) {
                "Active"
            } else {
                "Inactive"
            },
            t1.uuid_string(i).unwrap_or("".to_string()),
            t1.last_updated(i).unwrap_or(0),
            t1.term_begin(i).unwrap_or(0),
            t1.term_end(i).unwrap_or(0),
            std::str::from_utf8(t1.field_bytes(i, "num")).unwrap(),
            std::str::from_utf8(t1.field_bytes(i, "num_by3")).unwrap()
        );
    }
    assert_eq!(sum, 55.0);
}
```