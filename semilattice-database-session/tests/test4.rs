#[cfg(test)]
#[test]
fn test4() {
    use semilattice_database::FieldName;
    use semilattice_database_session::*;

    let dir = "./sl-test4/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();

    let mut database = SessionDatabase::new(dir.into(), None, 10);
    let collection_widget = database.collection_id_or_create("widget");
    let field_name = FieldName::from("name");

    let collection_field = database.collection_id_or_create("field");
    futures::executor::block_on(async {
        {
            let mut sess = database.session("widget", None);
            database
                .update(
                    &mut sess,
                    vec![SessionRecord::Update {
                        collection_id: collection_widget,
                        row: None,
                        activity: Activity::Active,
                        term_begin: Default::default(),
                        term_end: Default::default(),
                        fields: [(field_name.clone(), "test".into())].into(),
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .await;
        }
        let mut sess = database.session("widget", None);
        database
            .update(
                &mut sess,
                vec![SessionRecord::Update {
                    collection_id: collection_field,
                    row: None,
                    activity: Activity::Active,
                    term_begin: Default::default(),
                    term_end: Default::default(),
                    fields: [(field_name.clone(), "1".into())].into(),
                    depends: Depends::Overwrite(vec![(
                        "field".to_owned(),
                        CollectionRow::new(-collection_widget, 1.try_into().unwrap()),
                    )]),
                    pends: vec![],
                }],
            )
            .await;
        database
            .update(
                &mut sess,
                vec![SessionRecord::Update {
                    collection_id: collection_field,
                    row: None,
                    activity: Activity::Active,
                    term_begin: Default::default(),
                    term_end: Default::default(),
                    fields: [(field_name.clone(), "2".into())].into(),
                    depends: Depends::Overwrite(vec![(
                        "field".to_owned(),
                        CollectionRow::new(-collection_widget, 1.try_into().unwrap()),
                    )]),
                    pends: vec![],
                }],
            )
            .await;
        database
            .update(
                &mut sess,
                vec![SessionRecord::Update {
                    collection_id: collection_field,
                    row: None,
                    activity: Activity::Active,
                    term_begin: Default::default(),
                    term_end: Default::default(),
                    fields: [(field_name.clone(), "3".into())].into(),
                    depends: Depends::Overwrite(vec![(
                        "field".to_owned(),
                        CollectionRow::new(-collection_widget, 1.try_into().unwrap()),
                    )]),
                    pends: vec![],
                }],
            )
            .await;
        sess.set_sequence_cursor(3);

        //let mut sess = database.session("widget", None);
        database
            .update(
                &mut sess,
                vec![SessionRecord::Update {
                    collection_id: collection_field,
                    row: None,
                    activity: Activity::Active,
                    term_begin: Default::default(),
                    term_end: Default::default(),
                    fields: [(field_name.clone(), "3-r".into())].into(),
                    depends: Depends::Overwrite(vec![(
                        "field".to_owned(),
                        CollectionRow::new(-collection_widget, 1.try_into().unwrap()),
                    )]),
                    pends: vec![],
                }],
            )
            .await;

        let sess = database.session("widget", None);
        let search = database
            .search(collection_field)
            .search(semilattice_database::Condition::Depend(
                Some("field".to_owned()),
                CollectionRow::new(-collection_widget, 1.try_into().unwrap()),
            ))
            .search_activity(Activity::Active);
        for r in sess
            .result_with(&search.result(&database).await)
            .await
            .rows()
        {
            println!(
                "session_search : {}",
                std::str::from_utf8(sess.field_bytes(&database, collection_field, *r, &field_name))
                    .unwrap()
            );
        }
    });
}
