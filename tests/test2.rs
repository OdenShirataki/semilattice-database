#[cfg(test)]
#[test]
fn test2() {
    use semilattice_database::prelude::*;

    let dir = "./sl-test2/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();

    {
        let mut database = Database::new(dir).unwrap();
        let collection_setting_article =
            database.collection_id_or_create("setting_article").unwrap();
        if let Ok(mut sess) = database.session("setting_article", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_setting_article,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "test".to_owned())],
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        let collection_field = database.collection_id_or_create("field").unwrap();
        if let Ok(mut sess) = database.session("field", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_field,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "f1".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            SessionCollectionRow::new(collection_setting_article, 1),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        if let Ok(mut sess) = database.session("field", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_field,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "f2".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            SessionCollectionRow::new(collection_setting_article, 1),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        if let Ok(mut sess) = database.session("field", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_field,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "f3".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            SessionCollectionRow::new(collection_field, 2),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        if let Ok(mut sess) = database.session("field", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_field,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "f4".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            SessionCollectionRow::new(collection_field, 2),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        if let Some(field) = database.collection(collection_field) {
            let search = database.search(field).depend(Depend::new(
                "field",
                CollectionRow::new(collection_field, 2),
            ));
            let field_rows = database.result(search, &vec![]).unwrap();
            for i in field_rows {
                println!(
                    "{},{}",
                    i,
                    std::str::from_utf8(field.field_bytes(i, "name")).unwrap(),
                );
            }
        }
        println!("");
        if let Ok(mut sess) = database.session("field", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::Update {
                        collection_id: collection_field,
                        row: 4,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![KeyValue::new("name", "f4_rename".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "field".to_owned(),
                            SessionCollectionRow::new(collection_field, 2),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }

        if let Some(field) = database.collection(collection_field) {
            let search = database.search(field).depend(Depend::new(
                "field",
                CollectionRow::new(collection_field, 2),
            ));
            let field_rows = database.result(search, &vec![]).unwrap();
            for i in field_rows {
                println!(
                    "{},{}",
                    i,
                    std::str::from_utf8(field.field_bytes(i, "name")).unwrap(),
                );
            }
        }
    }
}
