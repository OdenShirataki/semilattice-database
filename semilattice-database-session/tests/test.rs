#[cfg(test)]
#[test]
fn test() {
    use std::{num::NonZeroU32, sync::Arc};

    use hashbrown::HashMap;
    use semilattice_database::FieldName;
    use semilattice_database_session::*;

    let dir = "./sl-test/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();

    let mut database = SessionDatabase::new(dir.into(), None, 10);

    let collection_admin = database.collection_id_or_create("admin");

    let field_id = FieldName::new("id".into());
    let field_password = FieldName::new("password".into());

    let mut sess = database.session("creatre_account_1st", None);
    futures::executor::block_on(async {
        database
            .update(
                &mut sess,
                vec![SessionRecord::Update {
                    collection_id: collection_admin,
                    row: None,
                    activity: Activity::Active,
                    term_begin: Default::default(),
                    term_end: Default::default(),
                    fields: [
                        (field_id.clone(), b"test".to_vec()),
                        (field_password.clone(), b"test".to_vec()),
                    ]
                    .into(),
                    depends: Depends::Overwrite(vec![]),
                    pends: vec![],
                }],
            )
            .await;
        database.commit(&mut sess).await;

        let collection_login = database.collection_id_or_create("login");
        let mut sess = database.session("login", None);

        let search = database
            .search(collection_admin)
            .search_field(field_id.clone(), search::Field::Match(b"test".to_vec()))
            .search_field(
                field_password.clone(),
                search::Field::Match(b"test".to_vec()),
            );

        for row in sess
            .result_with(&search.result(&database).await)
            .await
            .rows()
        {
            database
                .update(
                    &mut sess,
                    vec![SessionRecord::Update {
                        collection_id: collection_login,
                        row: None,
                        activity: Activity::Active,
                        term_begin: Default::default(),
                        term_end: Default::default(),
                        fields: HashMap::new(),
                        depends: Depends::Overwrite(vec![(
                            Arc::new("admin".into()),
                            CollectionRow::new(collection_admin, (*row).try_into().unwrap()),
                        )]),
                        pends: vec![],
                    }],
                )
                .await;
        }
        let search = database.search(collection_login);
        for row in sess
            .result_with(&search.result(&database).await)
            .await
            .rows()
        {
            println!("depends_with_session {} {}", collection_login, row);
            let depends = database.depends_with_session(
                Some(Arc::new("admin".into())),
                collection_login,
                (*row).try_into().unwrap(),
                Some(&sess),
            );
            for d in depends {
                let collection_id = d.collection_id();
                if let Some(collection) = database.collection(collection_id) {
                    let search = database
                        .search(collection_id)
                        .search_row(search::Number::In(vec![d.row().get() as isize]));
                    for row in sess
                        .result_with(&search.result(&database).await)
                        .await
                        .rows()
                    {
                        println!(
                            "login id : {}",
                            std::str::from_utf8(
                                collection.field_bytes((*row).try_into().unwrap(), &field_id)
                            )
                            .unwrap()
                        );
                    }
                }
            }
        }

        let collection_person = database.collection_id_or_create("person");
        let collection_history = database.collection_id_or_create("history");

        let field_name = FieldName::new("name".into());
        let field_birthday = FieldName::new("birthday".into());

        let field_date = FieldName::new("date".into());
        let field_event = FieldName::new("event".into());

        let mut sess = database.session("test", None);
        database
            .update(
                &mut sess,
                vec![
                    SessionRecord::Update {
                        collection_id: collection_person,
                        row: None,
                        activity: Activity::Active,
                        term_begin: Default::default(),
                        term_end: Default::default(),
                        fields: [
                            (field_name.clone(), "Joe".into()),
                            (field_birthday.clone(), "1972-08-02".into()),
                        ]
                        .into(),
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![Pend {
                            key: Arc::new("history".into()),
                            records: vec![
                                SessionRecord::Update {
                                    collection_id: collection_history,
                                    row: None,
                                    activity: Activity::Active,
                                    term_begin: Default::default(),
                                    term_end: Default::default(),
                                    fields: [
                                        (field_date.clone(), "1972-08-02".into()),
                                        (field_event.clone(), "Birth".into()),
                                    ]
                                    .into(),
                                    depends: Depends::Default,
                                    pends: vec![],
                                },
                                SessionRecord::Update {
                                    collection_id: collection_history,
                                    row: None,
                                    activity: Activity::Active,
                                    term_begin: Default::default(),
                                    term_end: Default::default(),
                                    fields: [
                                        (field_date.clone(), "1999-12-31".into()),
                                        (field_event.clone(), "Mariage".into()),
                                    ]
                                    .into(),
                                    depends: Depends::Default,
                                    pends: vec![],
                                },
                            ],
                        }],
                    },
                    SessionRecord::Update {
                        collection_id: collection_person,
                        row: None,
                        activity: Activity::Active,
                        term_begin: Default::default(),
                        term_end: Default::default(),
                        fields: [
                            (field_name.clone(), "Tom".into()),
                            (field_birthday.clone(), "2000-12-12".into()),
                        ]
                        .into(),
                        depends: Depends::Default,
                        pends: vec![Pend {
                            key: Arc::new("history".into()),
                            records: vec![SessionRecord::Update {
                                collection_id: collection_history,
                                row: None,
                                activity: Activity::Active,
                                term_begin: Default::default(),
                                term_end: Default::default(),
                                fields: [
                                    (field_date.clone(), "2000-12-12".into()),
                                    (field_event.clone(), "Birth".into()),
                                ]
                                .into(),
                                depends: Depends::Default,
                                pends: vec![],
                            }],
                        }],
                    },
                    SessionRecord::Update {
                        collection_id: collection_person,
                        row: None,
                        activity: Activity::Active,
                        term_begin: Default::default(),
                        term_end: Default::default(),
                        fields: [
                            (field_name.clone(), "Billy".into()),
                            (field_birthday.clone(), "1982-03-03".into()),
                        ]
                        .into(),
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
            let person_rows = database
                .search(collection_person)
                .result(&database)
                .await
                .sort(
                    &database,
                    &vec![Order::Asc(OrderKey::Field(field_birthday.clone()))],
                );
            for i in person_rows {
                println!(
                    "{},{}",
                    std::str::from_utf8(person.field_bytes(i, &field_name)).unwrap(),
                    std::str::from_utf8(person.field_bytes(i, &field_birthday)).unwrap()
                );
                for h in database
                    .search(collection_history)
                    .search(Condition::Depend(
                        Some(Arc::new("history".into())),
                        CollectionRow::new(collection_person, i),
                    ))
                    .result(&database)
                    .await
                    .rows()
                {
                    println!(
                        " {} : {}",
                        std::str::from_utf8(history.field_bytes(*h, &field_date)).unwrap(),
                        std::str::from_utf8(history.field_bytes(*h, &field_event)).unwrap()
                    );
                }
            }
        }
        let mut sess = database.session("test", None);
        database
            .update(
                &mut sess,
                vec![SessionRecord::Update {
                    collection_id: collection_person,
                    row: NonZeroU32::new(1),
                    activity: Activity::Active,
                    term_begin: Default::default(),
                    term_end: Default::default(),
                    fields: [(field_name.clone(), "Renamed Joe".into())].into(),
                    depends: Depends::Default,
                    pends: vec![],
                }],
            )
            .await;

        let mut sess = database.session("test", None);
        let search = database
            .search(collection_person)
            .search_activity(Activity::Active);
        for r in sess
            .result_with(&search.result(&database).await)
            .await
            .rows()
        {
            println!(
                "session_search : {},{}",
                std::str::from_utf8(sess.field_bytes(
                    &database,
                    collection_person,
                    *r,
                    &field_name
                ))
                .unwrap(),
                std::str::from_utf8(sess.field_bytes(
                    &database,
                    collection_person,
                    *r,
                    &field_birthday
                ))
                .unwrap()
            );
        }
        database.commit(&mut sess).await;

        let test1 = database.collection_id_or_create("test1");

        let field_num = FieldName::new("num".into());
        let field_num_by3 = FieldName::new("num_by3".into());

        let range = 1u32..=10;
        let mut sess = database.session("test", None);
        for i in range.clone() {
            database
                .update(
                    &mut sess,
                    vec![SessionRecord::Update {
                        collection_id: test1,
                        row: None,
                        activity: Activity::Active,
                        term_begin: Default::default(),
                        term_end: Default::default(),
                        fields: [
                            (field_num.clone(), i.to_string().into()),
                            (field_num_by3.clone(), (i * 3).to_string().into()),
                        ]
                        .into(),
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
                    row: NonZeroU32::new(3),
                    activity: Activity::Active,
                    term_begin: Default::default(),
                    term_end: Default::default(),
                    fields: HashMap::new(),
                    depends: Depends::Overwrite(vec![]),
                    pends: vec![],
                }],
            )
            .await;
        database.commit(&mut sess).await;

        if let Some(t1) = database.collection(test1) {
            let mut sum = 0.0;
            for i in range.clone() {
                sum += t1.field_num(i.try_into().unwrap(), &field_num);
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
                    t1.last_updated(i.try_into().unwrap()).unwrap_or(&0),
                    t1.term_begin(i.try_into().unwrap()).unwrap_or(&0),
                    t1.term_end(i.try_into().unwrap()).unwrap_or(&0),
                    std::str::from_utf8(t1.field_bytes(i.try_into().unwrap(), &field_num)).unwrap(),
                    std::str::from_utf8(t1.field_bytes(i.try_into().unwrap(), &field_num_by3))
                        .unwrap()
                );
            }
            assert_eq!(sum, 55.0);
        }
    });
}
