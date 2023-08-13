# semilattice-database

## Example

```rust
use semilattice_database::*;

let dir = "./sl-test/";

if std::path::Path::new(dir).exists() {
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::create_dir_all(dir).unwrap();
} else {
    std::fs::create_dir_all(dir).unwrap();
}
let mut database = Database::new(dir, None);

let collection_person_id=database.collection_id_or_create("person");
let collection_history_id=database.collection_id_or_create("history");

if let Some(collection_person) = database.collection_mut(collection_person_id) {
    if let Ok(row) = collection_person.update(&Operation::New {
        activity: Activity::Active,
        term_begin: Term::Default,
        term_end: Term::Default,
        fields: vec![
            KeyValue::new("name", "Joe"),
            KeyValue::new("birthday", "1972-08-02"),
        ],
    }) {
        let depend = CollectionRow::new(collection_person_id, row);
        let mut pends = vec![];
        if let Some(collection_history) = database.collection_mut(collection_history_id) {
            let history_row = collection_history.update(&Operation::New {
                activity: Activity::Active,
                term_begin: Term::Default,
                term_end: Term::Default,
                fields: vec![
                    KeyValue::new("date", "1972-08-02"),
                    KeyValue::new("event", "Birth"),
                ],
            });
            pends.push((
                "history".to_owned(),
                CollectionRow::new(collection_history_id, history_row),
            ));

            let history_row = collection_history.update(&Operation::New {
                activity: Activity::Active,
                term_begin: Term::Default,
                term_end: Term::Default,
                fields: vec![
                    KeyValue::new("date", "1999-12-31"),
                    KeyValue::new("event", "Mariage"),
                ],
            });
            pends.push((
                "history".to_owned(),
                CollectionRow::new(collection_history_id, history_row),
            ));
        }
        database.register_relations(&depend, pends).unwrap();
    }
}
if let (Some(person), Some(history)) = (
    database.collection(collection_person_id),
    database.collection(collection_history_id),
) {
    let search = database.search(person);
    if let Ok(result) = search.result(&database) {
        for row in result {
            println!(
                "{},{}",
                std::str::from_utf8(person.field_bytes(row, "name")).unwrap(),
                std::str::from_utf8(person.field_bytes(row, "birthday")).unwrap()
            );
            let search = database
                .search(history)
                .search(Condition::Depend(
                    Some("history".to_owned()),
                    CollectionRow::new(collection_person_id, row),
                ));
            for h in database.result(search, &vec![]).unwrap() {
                println!(
                    " {} : {}",
                    std::str::from_utf8(history.field_bytes(h, "date")).unwrap(),
                    std::str::from_utf8(history.field_bytes(h, "event")).unwrap()
                );
            }
        }
    }
}

```