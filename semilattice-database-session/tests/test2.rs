#[cfg(test)]
#[test]
fn test2() {
    use semilattice_database::FieldName;
    use semilattice_database_session::*;

    let dir = "./sl-test2/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();

    futures::executor::block_on(async {
        let field_name = FieldName::new("name".into());
        let field_text = FieldName::new("text".into());
        let field_image_type = FieldName::new("image_type".into());
        let field_image_name = FieldName::new("image_name".into());
        let field_image_data = FieldName::new("image_data".into());

        {
            let mut database = SessionDatabase::new(dir.into(), None, 10);
            let collection_bbs = database.collection_id_or_create("bbs");

            let mut sess = database.session("bbs", None);
            database
                .update(
                    &mut sess,
                    vec![SessionRecord::Update {
                        collection_id: collection_bbs,
                        row: None,
                        activity: Activity::Active,
                        term_begin: Default::default(),
                        term_end: Default::default(),
                        fields: [
                            (field_name.clone(), "test".into()),
                            (field_text.clone(), "test".into()),
                            (field_image_type.clone(), "application/octet-stream".into()),
                            (field_image_name.clone(), "".into()),
                            (field_image_data.clone(), "".into()),
                        ]
                        .into(),
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
                    vec![SessionRecord::Update {
                        collection_id: collection_bbs,
                        row: None,
                        activity: Activity::Active,
                        term_begin: Default::default(),
                        term_end: Default::default(),
                        fields: [
                            (field_name, "aa".into()),
                            (field_text, "bb".into()),
                            (field_image_type, "image/jpge".into()),
                            (field_image_name, "hoge.jpg".into()),
                            (field_image_data, "awdadadfaefaefawfafd".into()),
                        ]
                        .into(),
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
