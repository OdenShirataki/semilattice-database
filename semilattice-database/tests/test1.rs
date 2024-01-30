#[cfg(test)]
#[test]
fn test() {
    use std::sync::Arc;

    use semilattice_database::*;
    use versatile_data::FieldName;

    let dir = "./sl-test/";

    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::create_dir_all(dir).unwrap();
    } else {
        std::fs::create_dir_all(dir).unwrap();
    }
    futures::executor::block_on(async {
        let mut database = Database::new(dir.into(), None, 10);

        let collection_person_id = database.collection_id_or_create("person");
        let collection_history_id = database.collection_id_or_create("history");
        let collection_person = database.collection_mut(collection_person_id).unwrap();

        let id_name = FieldName::new("name".into());
        let id_birthday = FieldName::new("birthday".into());

        let row = collection_person
            .insert(
                Activity::Active,
                Term::Default,
                Term::Default,
                [
                    (id_name.clone(), "Joe".into()),
                    (id_birthday.clone(), "1972-08-02".into()),
                ]
                .into(),
            )
            .await;

        let depend = CollectionRow::new(collection_person_id, row);
        let mut pends = vec![];

        let collection_history = database.collection_mut(collection_history_id).unwrap();

        let id_date = FieldName::new("date".into());
        let id_event = FieldName::new("event".into());

        let history_row = collection_history
            .insert(
                Activity::Active,
                Term::Default,
                Term::Default,
                [
                    (id_date.clone(), "1972-08-02".into()),
                    (id_event.clone(), "Birth".into()),
                ]
                .into(),
            )
            .await;
        pends.push((
            Arc::new("history".to_owned()),
            CollectionRow::new(collection_history_id, history_row),
        ));
        let history_row = collection_history
            .insert(
                Activity::Active,
                Term::Default,
                Term::Default,
                [
                    (id_date.clone(), "1999-12-31".into()),
                    (id_event.clone(), "Mariage".into()),
                ]
                .into(),
            )
            .await;
        pends.push((
            Arc::new("history".to_owned()),
            CollectionRow::new(collection_history_id, history_row),
        ));
        database.register_relations(&depend, pends).await;

        if let (Some(person), Some(history)) = (
            database.collection(collection_person_id),
            database.collection(collection_history_id),
        ) {
            let result = database
                .search(collection_person_id)
                .result(&database)
                .await;
            for row in result.rows().into_iter() {
                println!(
                    "{},{}",
                    std::str::from_utf8(person.field_bytes(*row, &id_name)).unwrap(),
                    std::str::from_utf8(person.field_bytes(*row, &id_birthday)).unwrap()
                );
                for h in database
                    .search(collection_history_id)
                    .search(Condition::Depend(
                        Some(Arc::new("history".into())),
                        CollectionRow::new(collection_person_id, *row),
                    ))
                    .result(&database)
                    .await
                    .rows()
                    .into_iter()
                {
                    println!(
                        " {} : {}",
                        std::str::from_utf8(history.field_bytes(*h, &id_date)).unwrap(),
                        std::str::from_utf8(history.field_bytes(*h, &id_event)).unwrap()
                    );
                }
            }
        }
    });
}
