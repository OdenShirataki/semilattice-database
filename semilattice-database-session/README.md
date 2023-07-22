# semilattice-database-session

## Example

```rust
use semilattice_database_session::*;

let dir = "./sl-test/";

if std::path::Path::new(dir).exists() {
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::create_dir_all(dir).unwrap();
} else {
    std::fs::create_dir_all(dir).unwrap();
}
let mut database = SessionDatabase::new(dir).unwrap();

let collection_admin = database.collection_id_or_create("admin").unwrap();

if let Ok(mut sess) = database.session("creatre_account_1st", None) {
    database
        .update(
            &mut sess,
            vec![Record::New {
                collection_id: collection_admin,
                activity: Activity::Active,
                term_begin: Term::Default,
                term_end: Term::Default,
                fields: vec![
                    KeyValue::new("id", "test".to_owned()),
                    KeyValue::new("password", "test".to_owned()),
                ],
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
    for row in database.result_session(search, vec![]).unwrap() {
        assert!(row >= 0);
        database
            .update(
                &mut sess,
                vec![Record::New {
                    collection_id: collection_login,
                    activity: Activity::Active,
                    term_begin: Term::Default,
                    term_end: Term::Default,
                    fields: vec![],
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
    for row in database.result_session(search, vec![]).unwrap() {
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
                for row in database.result_session(search, vec![]).unwrap() {
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
                Record::New {
                    collection_id: collection_person,
                    activity: Activity::Active,
                    term_begin: Term::Default,
                    term_end: Term::Default,
                    fields: vec![
                        KeyValue::new("name", "Joe"),
                        KeyValue::new("birthday", "1972-08-02"),
                    ],
                    depends: Depends::Overwrite(vec![]),
                    pends: vec![Pend::new(
                        "history",
                        vec![
                            Record::New {
                                collection_id: collection_history,
                                activity: Activity::Active,
                                term_begin: Term::Default,
                                term_end: Term::Default,
                                fields: vec![
                                    KeyValue::new("date", "1972-08-02"),
                                    KeyValue::new("event", "Birth"),
                                ],
                                depends: Depends::Default,
                                pends: vec![],
                            },
                            Record::New {
                                collection_id: collection_history,
                                activity: Activity::Active,
                                term_begin: Term::Default,
                                term_end: Term::Default,
                                fields: vec![
                                    KeyValue::new("date", "1999-12-31"),
                                    KeyValue::new("event", "Mariage"),
                                ],
                                depends: Depends::Default,
                                pends: vec![],
                            },
                        ],
                    )],
                },
                Record::New {
                    collection_id: collection_person,
                    activity: Activity::Active,
                    term_begin: Term::Default,
                    term_end: Term::Default,
                    fields: vec![
                        KeyValue::new("name", "Tom"),
                        KeyValue::new("birthday", "2000-12-12"),
                    ],
                    depends: Depends::Default,
                    pends: vec![Pend::new(
                        "history",
                        vec![Record::New {
                            collection_id: collection_history,
                            activity: Activity::Active,
                            term_begin: Term::Default,
                            term_end: Term::Default,
                            fields: vec![
                                KeyValue::new("date", "2000-12-12"),
                                KeyValue::new("event", "Birth"),
                            ],
                            depends: Depends::Default,
                            pends: vec![],
                        }],
                    )],
                },
                Record::New {
                    collection_id: collection_person,
                    activity: Activity::Active,
                    term_begin: Term::Default,
                    term_end: Term::Default,
                    fields: vec![
                        KeyValue::new("name", "Billy"),
                        KeyValue::new("birthday", "1982-03-03"),
                    ],
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
    let search = database.search(person);
    let person_rows = database
        .result(
            search,
            &vec![Order::Asc(OrderKey::Field("birthday".to_owned()))],
        )
        .unwrap();
    for i in person_rows {
        println!(
            "{},{}",
            std::str::from_utf8(person.field_bytes(i, "name")).unwrap(),
            std::str::from_utf8(person.field_bytes(i, "birthday")).unwrap()
        );
        let search = database
            .search(history)
            .search(Condition::Depend(
                Some("history".to_owned()),
                CollectionRow::new(collection_person, i),
            ));
        for h in database.result(search, &vec![]).unwrap() {
            println!(
                " {} : {}",
                std::str::from_utf8(history.field_bytes(h, "date")).unwrap(),
                std::str::from_utf8(history.field_bytes(h, "event")).unwrap()
            );
        }
    }
}
if let Ok(mut sess) = database.session("test", None) {
    database
        .update(
            &mut sess,
            vec![Record::Update {
                collection_id: collection_person,
                row: 1,
                activity: Activity::Active,
                term_begin: Term::Default,
                term_end: Term::Default,
                fields: vec![KeyValue::new("name", "Renamed Joe")],
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
    for r in database.result_session(search, vec![]).unwrap() {
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
```