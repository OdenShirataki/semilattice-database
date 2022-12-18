#[cfg(test)]
#[test]
fn it_works() {
    use semilattice_database::prelude::*;

    let dir = "./sl-test/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::create_dir_all(dir).unwrap();
    } else {
        std::fs::create_dir_all(dir).unwrap();
    }
    let mut database = Database::new(dir).unwrap();

    let collection_admin = database.collection_id_or_create("admin").unwrap();
    if let Ok(mut sess) = database.session("creatre_account_1st") {
        database
            .update(
                &mut sess,
                vec![Record::New {
                    collection_id: collection_admin,
                    activity: Activity::Active,
                    term_begin: Term::Defalut,
                    term_end: Term::Defalut,
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
    if let Ok(mut sess) = database.session("login") {
        let search = sess
            .begin_search(collection_admin)
            .search_field("id", search::Field::Match(b"test".to_vec()))
            .search_field("password", search::Field::Match(b"test".to_vec()));
        for row in database.result_session(search) {
            println!("session_search : {row}");
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_login,
                        activity: Activity::Active,
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
                        fields: vec![],
                        depends: Depends::Overwrite(vec![(
                            "admin".to_owned(),
                            SessionCollectionRow::new(collection_admin, row),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
        }
    }
    if let Ok(sess) = database.session("login") {
        let search = sess
            .begin_search(collection_login);
        for row in database.result_session(search) {
            let depends=database.depends(Some("admin"), collection_login, row, Some(&sess));
            for d in depends{
                let collection_id=d.collection_id();
                if let Some(collection)=database.collection(collection_id){
                    let search = sess
                        .begin_search(collection_id)
                        .search_row(search::Number::In(vec![d.row() as isize]));
                    for row in database.result_session(search){
                        println!("login id : {}",std::str::from_utf8(collection.field_bytes(row as u32,"id")).unwrap());
                    }
                }
            }
        }
    }
    if let Ok(mut sess) = database.session("login") {
        let search = sess
            .begin_search(collection_login);
        for row in database.result_session(search) {
            database
                .update(
                    &mut sess,
                    vec![Record::Update {
                        collection_id: collection_login,
                        row:row, //-1
                        activity: Activity::Active,
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
                        fields: vec![],
                        depends: Depends::Overwrite(vec![(
                            "admin".to_owned(),
                            SessionCollectionRow::new(collection_admin, 1),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
        }
        database.commit(&mut sess).unwrap();
    }
    return ;
    /*
    if let Ok(sess) = database.session("logintest") {
        let search = sess.begin_search(collection_login).search_default();
        let r = database.result_session(search);
        println!("B session_login : {}", r.len());
        for r in r {
            let accounts = database.depends(Some("account"), collection_login, r, Some(&sess));
            for account in accounts {
                let account_collection_id = account.collection_id();
                let account_row = account.row();
                for account_row in database.result_session(
                    sess.begin_search(account_collection_id)
                        .search_row(search::Number::In(vec![account_row as isize])),
                ) {
                    if let Some(collection) = database.collection(account_collection_id) {
                        println!(
                            "session_login : {} , {}",
                            r,
                            std::str::from_utf8(collection.field_bytes(account_row as u32, "id"))
                                .unwrap()
                        );
                    }
                }
            }
        }
    } */

    let collection_person = database.collection_id_or_create("person").unwrap();
    let collection_history = database.collection_id_or_create("history").unwrap();

    if let Ok(mut sess) = database.session("test") {
        database
            .update(
                &mut sess,
                vec![
                    Record::New {
                        collection_id: collection_person,
                        activity: Activity::Active,
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
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
                                    term_begin: Term::Defalut,
                                    term_end: Term::Defalut,
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
                                    term_begin: Term::Defalut,
                                    term_end: Term::Defalut,
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
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
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
                                term_begin: Term::Defalut,
                                term_end: Term::Defalut,
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
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
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
        let person_rows = database.result(search);
        let person_rows = person.sort(
            person_rows,
            vec![Order::Asc(OrderKey::Field("birthday".to_owned()))],
        );
        for i in person_rows {
            println!(
                "{},{}",
                std::str::from_utf8(person.field_bytes(i, "name")).unwrap(),
                std::str::from_utf8(person.field_bytes(i, "birthday")).unwrap()
            );
            let search = database.search(history).depend(Depend::new(
                "history",
                CollectionRow::new(collection_person, i as u32),
            ));
            for h in database.result(search) {
                println!(
                    " {} : {}",
                    std::str::from_utf8(history.field_bytes(h, "date")).unwrap(),
                    std::str::from_utf8(history.field_bytes(h, "event")).unwrap()
                );
            }
        }
    }
    if let Ok(mut sess) = database.session("test") {
        database
            .update(
                &mut sess,
                vec![Record::Update {
                    collection_id: collection_person,
                    row: 1,
                    activity: Activity::Active,
                    term_begin: Term::Defalut,
                    term_end: Term::Defalut,
                    fields: vec![KeyValue::new("name", "Renamed Joe")],
                    depends: Depends::Default,
                    pends: vec![],
                }],
            )
            .unwrap();
    }
    if let Ok(mut sess) = database.session("test") {
        let search = sess
            .begin_search(collection_person)
            .search_activity(Activity::Active);
        for r in database.result_session(search) {
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
    if let Ok(mut sess) = database.session("test") {
        for i in range.clone() {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: test1,
                        activity: Activity::Active,
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
                        fields: vec![
                            KeyValue::new("num", i.to_string()),
                            KeyValue::new("num_by3", (i * 3).to_string()),
                        ],
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .unwrap();
        }
        database.commit(&mut sess).unwrap();
    }

    if let Ok(mut sess) = database.session("test") {
        database
            .update(
                &mut sess,
                vec![Record::Update {
                    collection_id: test1,
                    row: 3,
                    activity: Activity::Inactive,
                    term_begin: Term::Defalut,
                    term_end: Term::Defalut,
                    fields: vec![],
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
                if t1.activity(i) == Activity::Active {
                    "Active"
                } else {
                    "Inactive"
                },
                t1.uuid_str(i),
                t1.last_updated(i),
                t1.term_begin(i),
                t1.term_end(i),
                std::str::from_utf8(t1.field_bytes(i, "num")).unwrap(),
                std::str::from_utf8(t1.field_bytes(i, "num_by3")).unwrap()
            );
        }
        assert_eq!(sum, 55.0);
    }
}
