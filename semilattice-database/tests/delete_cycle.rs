//! Deleting rows whose relations form a cycle must terminate.

use std::{sync::mpsc, time::Duration};

use semilattice_database::*;

fn insert(collection: &mut Collection, value: &str) -> std::num::NonZeroU32 {
    let name = FieldName::new("name".into());
    futures::executor::block_on(collection.insert(
        Activity::Active,
        Term::Default,
        Term::Default,
        [(name, value.as_bytes().to_vec())].into(),
    ))
}

#[test]
fn delete_with_cyclic_relations_terminates() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let dir = std::env::temp_dir().join("semilattice-delete-cycle");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut database = Database::new(dir, None, 10);
        let id = database.collection_id_or_create("c");

        let collection = database.collection_mut(id).unwrap();
        let a = CollectionRow::new(id, insert(collection, "a"));
        let b = CollectionRow::new(id, insert(collection, "b"));
        let c = CollectionRow::new(id, insert(collection, "c"));

        futures::executor::block_on(async {
            // a -> b -> a, and c -> a
            database.register_relation("k", &a, &b).await;
            database.register_relation("k", &b, &a).await;
            database.register_relation("k", &a, &c).await;
            database.delete(&a).await;
        });
        let collection = database.collection(id).unwrap();
        let left = collection.data().all().len();
        tx.send(left).unwrap();
    });
    let left = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("delete did not finish: recursion over a cycle");
    assert_eq!(left, 0);
}
