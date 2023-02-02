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
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
                        fields: vec![
                            KeyValue::new("name", "A".to_owned()),
                            KeyValue::new("text", "A".to_owned()),
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
                    vec![Record::New {
                        collection_id: collection_bbs,
                        activity: Activity::Active,
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
                        fields: vec![
                            KeyValue::new("name", "B".to_owned()),
                            KeyValue::new("text", "B".to_owned()),
                        ],
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }

        let collection_bbs_comment = database.collection_id_or_create("bbs_comment").unwrap();
        if let Ok(mut sess) = database.session("bbs_comment", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_bbs_comment,
                        activity: Activity::Active,
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
                        fields: vec![KeyValue::new("text", "C1".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "bbs".to_owned(),
                            SessionCollectionRow::new(collection_bbs, 1),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }
        if let Ok(mut sess) = database.session("bbs_comment", None) {
            database
                .update(
                    &mut sess,
                    vec![Record::New {
                        collection_id: collection_bbs_comment,
                        activity: Activity::Active,
                        term_begin: Term::Defalut,
                        term_end: Term::Defalut,
                        fields: vec![KeyValue::new("text", "C2".to_owned())],
                        depends: Depends::Overwrite(vec![(
                            "bbs".to_owned(),
                            SessionCollectionRow::new(collection_bbs, 2),
                        )]),
                        pends: vec![],
                    }],
                )
                .unwrap();
            database.commit(&mut sess).unwrap();
        }

        if let (Some(bbs), Some(bbs_comment)) = (
            database.collection(collection_bbs),
            database.collection(collection_bbs_comment),
        ) {
            let search = database.search(bbs);
            let bbs_rows = database.result(search, &vec![]).unwrap();
            for i in bbs_rows {
                println!(
                    "{},{}",
                    std::str::from_utf8(bbs.field_bytes(i, "name")).unwrap(),
                    std::str::from_utf8(bbs.field_bytes(i, "text")).unwrap()
                );
                let search = database.search(bbs_comment).depend(Depend::new(
                    "bbs",
                    CollectionRow::new(collection_bbs, i as u32),
                ));
                for h in database.result(search, &vec![]).unwrap() {
                    println!(
                        " {}",
                        std::str::from_utf8(bbs_comment.field_bytes(h, "text")).unwrap(),
                    );
                }
            }
        }
    }
    {
        let mut database = Database::new(dir).unwrap();
        let collection_bbs = database.collection_id_or_create("bbs").unwrap();
        let collection_bbs_comment = database.collection_id_or_create("bbs_comment").unwrap();

        println!("{} {}", collection_bbs, collection_bbs_comment);
        if let (Some(bbs), Some(bbs_comment)) = (
            database.collection(collection_bbs),
            database.collection(collection_bbs_comment),
        ) {
            let search = database.search(bbs);
            let bbs_rows = database.result(search, &vec![]).unwrap();
            for i in bbs_rows {
                println!(
                    "POST : {},{}",
                    std::str::from_utf8(bbs.field_bytes(i, "name")).unwrap(),
                    std::str::from_utf8(bbs.field_bytes(i, "text")).unwrap()
                );
                let search = database.search(bbs_comment).depend(Depend::new(
                    "bbs",
                    CollectionRow::new(collection_bbs, i as u32),
                ));
                for h in database.result(search, &vec![]).unwrap() {
                    println!(
                        "COMMENT : {}",
                        std::str::from_utf8(bbs_comment.field_bytes(h, "text")).unwrap(),
                    );
                }
            }
        }
    }
}
