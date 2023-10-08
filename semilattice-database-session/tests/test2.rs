#[cfg(test)]
#[test]
fn test2() {
    use semilattice_database_session::*;

    let dir = "./sl-test2/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();

    futures::executor::block_on(async {
        {
            let mut database = SessionDatabase::new(dir.into(), None, 10);
            let collection_bbs = database.collection_id_or_create("bbs");

            let mut sess = database.session("bbs", None);
            database
                .update(
                    &mut sess,
                    vec![SessionRecord::New {
                        collection_id: collection_bbs,
                        record: Record {
                            fields: vec![
                                KeyValue::new("name", "test".to_owned()),
                                KeyValue::new("text", "test".to_owned()),
                                KeyValue::new("image_type", "application/octet-stream".to_owned()),
                                KeyValue::new("image_name", "".to_owned()),
                                KeyValue::new("image_data", "".to_owned()),
                            ],
                            ..Record::default()
                        },
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .await;
            database.commit(&mut sess).await;
        }
        {
            let mut database = SessionDatabase::new(dir.into(), None, 10);
            let collection_bbs = database.collection_id_or_create("bbs");
            let mut sess = database.session("bbs", None);
            database
                .update(
                    &mut sess,
                    vec![SessionRecord::Delete {
                        collection_id: collection_bbs,
                        row: 1.try_into().unwrap(),
                    }],
                )
                .await;
            database.commit(&mut sess).await;

            println!("OK1");
        }
        {
            let mut database = SessionDatabase::new(dir.into(), None, 10);
            let collection_bbs = database.collection_id_or_create("bbs");
            let mut sess = database.session("bbs", None);
            database
                .update(
                    &mut sess,
                    vec![SessionRecord::New {
                        collection_id: collection_bbs,
                        record: Record {
                            fields: vec![
                                KeyValue::new("name", "aa".to_owned()),
                                KeyValue::new("text", "bb".to_owned()),
                                KeyValue::new("image_type", "image/jpge".to_owned()),
                                KeyValue::new("image_name", "hoge.jpg".to_owned()),
                                KeyValue::new("image_data", "awdadadfaefaefawfafd".to_owned()),
                            ],
                            ..Record::default()
                        },
                        depends: Depends::Overwrite(vec![]),
                        pends: vec![],
                    }],
                )
                .await;
            database.commit(&mut sess).await;

            println!("OK2");
        }
    });
}
