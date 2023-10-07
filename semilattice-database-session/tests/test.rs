#[cfg(test)]
#[test]
fn test() {
    use std::ops::Deref;

    use semilattice_database_session::*;

    let dir = "./sl-test/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();

    let mut database = SessionDatabase::new(dir.into(), None);

    let collection_admin = database.collection_id_or_create("admin");

    let mut sess = database.session("creatre_account_1st", None);
    futures::executor::block_on(async {
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
            .await;
        database.commit(&mut sess).await;

        let collection_login = database.collection_id_or_create("login");
        let mut sess = database.session("login", None);
        let mut search = sess
            .begin_search(collection_admin)
            .search_field("id", search::Field::Match(b"test".to_vec()))
            .search_field("password", search::Field::Match(b"test".to_vec()));
        for row in search.result(&database, &vec![]).await {
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
                            CollectionRow::new(collection_admin, row.try_into().unwrap()),
                        )]),
                        pends: vec![],
                    }],
                )
                .await;
        }
        let mut sess = database.session("login", None);
        let mut search = sess.begin_search(collection_login);
        for row in search.result(&database, &vec![]).await {
            println!("depends_with_session {} {}", collection_login, row);
            let depends = database.depends_with_session(
                Some("admin"),
                collection_login,
                row.try_into().unwrap(),
                Some(&sess),
            );
            for d in depends {
                let collection_id = d.collection_id();
                if let Some(collection) = database.collection(collection_id) {
                    let mut search = sess
                        .begin_search(collection_id)
                        .search_row(search::Number::In(vec![d.row().get() as isize]));
                    for row in search.result(&database, &vec![]).await {
                        println!(
                            "login id : {}",
                            std::str::from_utf8(
                                collection.field_bytes(row.try_into().unwrap(), "id")
                            )
                            .unwrap()
                        );
                    }
                }
            }
        }

        let collection_person = database.collection_id_or_create("person");
        let collection_history = database.collection_id_or_create("history");

        let mut sess = database.session("test", None);
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
            .await;
        database.commit(&mut sess).await;

        if let (Some(person), Some(history)) = (
            database.collection(collection_person),
            database.collection(collection_history),
        ) {
            let mut search = database.search(collection_person);
            let result = search.result(&database).await;
            let person_rows = if let Some(r) = result.read().unwrap().deref() {
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
                let mut search = database
                    .search(collection_history)
                    .search(Condition::Depend(
                        Some("history".to_owned()),
                        CollectionRow::new(collection_person, i),
                    ));
                if let Some(result) = search.result(&database).await.read().unwrap().deref() {
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
        let mut sess = database.session("test", None);
        database
            .update(
                &mut sess,
                vec![SessionRecord::Update {
                    collection_id: collection_person,
                    row: 1.try_into().unwrap(),
                    record: Record {
                        fields: vec![KeyValue::new("name", "Renamed Joe")],
                        ..Record::default()
                    },
                    depends: Depends::Default,
                    pends: vec![],
                }],
            )
            .await;

        let mut sess = database.session("test", None);
        let mut search = sess
            .begin_search(collection_person)
            .search_activity(Activity::Active);
        for r in search.result(&database, &vec![]).await {
            println!(
                "session_search : {},{}",
                std::str::from_utf8(sess.field_bytes(&database, collection_person, r, "name"))
                    .unwrap(),
                std::str::from_utf8(sess.field_bytes(&database, collection_person, r, "birthday"))
                    .unwrap()
            );
        }
        database.commit(&mut sess).await;

        let test1 = database.collection_id_or_create("test1");
        let range = 1u32..=10;
        let mut sess = database.session("test", None);
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
                .await;
        }
        database.commit(&mut sess).await;

        let mut sess = database.session("test", None);
        database
            .update(
                &mut sess,
                vec![SessionRecord::Update {
                    collection_id: test1,
                    row: 3.try_into().unwrap(),
                    record: Record {
                        fields: vec![],
                        ..Record::default()
                    },
                    depends: Depends::Overwrite(vec![]),
                    pends: vec![],
                }],
            )
            .await;
        database.commit(&mut sess).await;

        if let Some(t1) = database.collection(test1) {
            let mut sum = 0.0;
            for i in range.clone() {
                sum += t1.field_num(i.try_into().unwrap(), "num");
                println!(
                    "{},{},{},{},{},{},{},{}",
                    t1.serial(i.try_into().unwrap()),
                    if let Some(Activity::Active) = t1.activity(i.try_into().unwrap()) {
                        "Active"
                    } else {
                        "Inactive"
                    },
                    t1.uuid_string(i.try_into().unwrap())
                        .unwrap_or("".to_string()),
                    t1.last_updated(i.try_into().unwrap()).unwrap_or(0),
                    t1.term_begin(i.try_into().unwrap()).unwrap_or(0),
                    t1.term_end(i.try_into().unwrap()).unwrap_or(0),
                    std::str::from_utf8(t1.field_bytes(i.try_into().unwrap(), "num")).unwrap(),
                    std::str::from_utf8(t1.field_bytes(i.try_into().unwrap(), "num_by3")).unwrap()
                );
            }
            assert_eq!(sum, 55.0);
        }
    });
}
