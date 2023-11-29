#[cfg(test)]
#[test]
fn test() {
    use std::num::NonZeroU32;

    use semilattice_database::*;

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
        if let Some(collection_person) = database.collection_mut(collection_person_id) {
            let row = collection_person
                .update(Operation::New(Record {
                    activity: Activity::Active,
                    term_begin: Term::Default,
                    term_end: Term::Default,
                    fields: [
                        ("name".into(), "Joe".into()),
                        ("birthday".into(), "1972-08-02".into()),
                    ]
                    .into(),
                }))
                .await;

            let depend = CollectionRow::new(collection_person_id, NonZeroU32::new(row).unwrap());
            let mut pends = vec![];
            if let Some(collection_history) = database.collection_mut(collection_history_id) {
                let history_row = collection_history
                    .update(Operation::New(Record {
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: [
                            ("date".into(), "1972-08-02".into()),
                            ("event".into(), "Birth".into()),
                        ]
                        .into(),
                    }))
                    .await;
                pends.push((
                    "history".to_owned(),
                    CollectionRow::new(
                        collection_history_id,
                        NonZeroU32::new(history_row).unwrap(),
                    ),
                ));
                let history_row = collection_history
                    .update(Operation::New(Record {
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: [
                            ("date".into(), "1999-12-31".into()),
                            ("event".into(), "Mariage".into()),
                        ]
                        .into(),
                    }))
                    .await;
                pends.push((
                    "history".to_owned(),
                    CollectionRow::new(
                        collection_history_id,
                        NonZeroU32::new(history_row).unwrap(),
                    ),
                ));
            }
            database.register_relations(&depend, pends).await;
        }

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
                    std::str::from_utf8(person.field_bytes(*row, "name")).unwrap(),
                    std::str::from_utf8(person.field_bytes(*row, "birthday")).unwrap()
                );
                for h in database
                    .search(collection_history_id)
                    .search(Condition::Depend(
                        Some("history".to_owned()),
                        CollectionRow::new(collection_person_id, *row),
                    ))
                    .result(&database)
                    .await
                    .rows()
                    .into_iter()
                {
                    println!(
                        " {} : {}",
                        std::str::from_utf8(history.field_bytes(*h, "date")).unwrap(),
                        std::str::from_utf8(history.field_bytes(*h, "event")).unwrap()
                    );
                }
            }
        }
    });
}
