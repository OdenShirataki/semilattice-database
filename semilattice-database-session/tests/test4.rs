#[cfg(test)]
#[test]
fn test4() {
    use semilattice_database_session::*;

    let dir = "./sl-test4/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();

    {
        let mut database = SessionDatabase::new(dir.into(),None).unwrap();
        let collection_widget = database.collection_id_or_create("widget").unwrap();
        let collection_field = database.collection_id_or_create("field").unwrap();

        if let Ok(mut sess) = database.session("widget", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_widget,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "test".to_owned())],
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .unwrap();
        }
        if let Ok(mut sess) = database.session("widget", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_field,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "1".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            CollectionRow::new(-collection_widget, 1),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_field,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "2".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            CollectionRow::new(-collection_widget, 1),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_field,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "3".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            CollectionRow::new(-collection_widget, 1),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            sess.set_sequence_cursor(3);
        }
        if let Ok(mut sess) = database.session("widget", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_field,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "3-r".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            CollectionRow::new(-collection_widget, 1),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
        }

        if let Ok(sess) = database.session("widget", None) {
            let search = sess
                .begin_search(collection_field)
                .search(semilattice_database::Condition::Depend(
                    Some("field".to_owned()),
                    CollectionRow::new(-collection_widget, 1),
                ))
                .search_activity(Activity::Active);
            for r in database.result_session(search, vec![]).unwrap() {
                println!(
                    "session_search : {}",
                    std::str::from_utf8(sess.field_bytes(&database, collection_field, r, "name"))
                        .unwrap()
                );
            }
        }
    }
}
