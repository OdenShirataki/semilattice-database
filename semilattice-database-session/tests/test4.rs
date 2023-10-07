#[cfg(test)]
#[test]
fn test4() {
    use semilattice_database_session::*;

    let dir = "./sl-test4/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();

    let mut database = SessionDatabase::new(dir.into(), None);
    let collection_widget = database.collection_id_or_create("widget");
    let collection_field = database.collection_id_or_create("field");
    futures::executor::block_on(async {
        {
            let mut sess = database.session("widget", None);
            database
                .update(
                    &mut sess,
                    vec![SessionRecord::New {
                        collection_id: collection_widget,
                        record: Record {
                            fields: vec![KeyValue::new("name", "test".to_owned())],
                            ..Record::default()
                        },
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
                vec![SessionRecord::New {
                    collection_id: collection_field,
                    record: Record {
                        fields: vec![KeyValue::new("name", "1".to_owned())],
                        ..Record::default()
                    },
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
                vec![SessionRecord::New {
                    collection_id: collection_field,
                    record: Record {
                        fields: vec![KeyValue::new("name", "2".to_owned())],
                        ..Record::default()
                    },
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
                vec![SessionRecord::New {
                    collection_id: collection_field,
                    record: Record {
                        fields: vec![KeyValue::new("name", "3".to_owned())],
                        ..Record::default()
                    },
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
                vec![SessionRecord::New {
                    collection_id: collection_field,
                    record: Record {
                        fields: vec![KeyValue::new("name", "3-r".to_owned())],
                        ..Record::default()
                    },
                    depends: Depends::Overwrite(vec![(
                        "field".to_owned(),
                        CollectionRow::new(-collection_widget, 1.try_into().unwrap()),
                    )]),
                    pends: vec![],
                }],
            )
            .await;

        let mut sess = database.session("widget", None);
        let mut search = sess
            .begin_search(collection_field)
            .search(semilattice_database::Condition::Depend(
                Some("field".to_owned()),
                CollectionRow::new(-collection_widget, 1.try_into().unwrap()),
            ))
            .search_activity(Activity::Active);
        for r in search.result(&database, &vec![]).await {
            println!(
                "session_search : {}",
                std::str::from_utf8(sess.field_bytes(&database, collection_field, r, "name"))
                    .unwrap()
            );
        }
    });
}
