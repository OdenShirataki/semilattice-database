#[cfg(test)]
#[test]
fn test() {
    use std::{num::NonZeroU32, ops::Deref};

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
                .update(&Operation::New(Record {
                    activity: Activity::Active,
                    term_begin: Term::Default,
                    term_end: Term::Default,
                    fields: vec![
                        KeyValue::new("name", "Joe"),
                        KeyValue::new("birthday", "1972-08-02"),
                    ],
                }))
                .await;

            let depend = CollectionRow::new(collection_person_id, NonZeroU32::new(row).unwrap());
            let mut pends = vec![];
            if let Some(collection_history) = database.collection_mut(collection_history_id) {
                let history_row = collection_history
                    .update(&Operation::New(Record {
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![
                            KeyValue::new("date", "1972-08-02"),
                            KeyValue::new("event", "Birth"),
                        ],
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
                    .update(&Operation::New(Record {
                        activity: Activity::Active,
                        term_begin: Term::Default,
                        term_end: Term::Default,
                        fields: vec![
                            KeyValue::new("date", "1999-12-31"),
                            KeyValue::new("event", "Mariage"),
                        ],
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
            let mut search = database.search(collection_person_id);
            if let Some(result) = search.result(&database).await.read().unwrap().deref() {
                for row in result.rows() {
                    println!(
                        "{},{}",
                        std::str::from_utf8(person.field_bytes(*row, "name")).unwrap(),
                        std::str::from_utf8(person.field_bytes(*row, "birthday")).unwrap()
                    );
                    let mut search_history =
                        database
                            .search(collection_history_id)
                            .search(Condition::Depend(
                                Some("history".to_owned()),
                                CollectionRow::new(collection_person_id, *row),
                            ));
                    if let Some(result) = search_history
                        .result(&database)
                        .await
                        .read()
                        .unwrap()
                        .deref()
                    {
                        for h in result.rows() {
                            println!(
                                " {} : {}",
                                std::str::from_utf8(history.field_bytes(*h, "date")).unwrap(),
                                std::str::from_utf8(history.field_bytes(*h, "event")).unwrap()
                            );
                        }
                    }
                }
            }
        }
    });
}
