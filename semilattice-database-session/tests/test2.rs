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
                            fields: [
                                ("name".into(), "test".into()),
                                ("text".into(), "test".into()),
                                ("image_type".into(), "application/octet-stream".into()),
                                ("image_name".into(), "".into()),
                                ("image_data".into(), "".into()),
                            ]
                            .into(),
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
                            fields: [
                                ("name".into(), "aa".into()),
                                ("text".into(), "bb".into()),
                                ("image_type".into(), "image/jpge".into()),
                                ("image_name".into(), "hoge.jpg".into()),
                                ("image_data".into(), "awdadadfaefaefawfafd".into()),
                            ]
                            .into(),
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
