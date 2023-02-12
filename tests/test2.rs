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
        let collection_bbs = database.collection_id_or_create("bbs").unwrap();
        if let Ok(mut sess) = database.session("bbs", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_bbs,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![
                            KeyValue::new("name", "test".to_owned()),
                            KeyValue::new("text", "test".to_owned()),
                            KeyValue::new("image_type", "".to_owned()),
                            KeyValue::new("image_name", "".to_owned()),
                            KeyValue::new("image_data", "".to_owned()),
                        ],
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        if let Ok(mut sess) = database.session("bbs", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::Delete {
                        collection_id: collection_bbs,
                        row: 1,
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        println!("OK1");
        if let Ok(mut sess) = database.session("bbs", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_bbs,
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![
                            KeyValue::new("name", "aa".to_owned()),
                            KeyValue::new("text", "bb".to_owned()),
                            KeyValue::new("image_type", "image/jpge".to_owned()),
                            KeyValue::new("image_name", "hoge.jpg".to_owned()),
                            KeyValue::new("image_data", "awdadadfaefaefawfafd".to_owned()),
                        ],
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        println!("OK2");
    }
}
