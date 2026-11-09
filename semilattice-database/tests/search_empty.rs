use semilattice_database::*;

#[test]
fn empty_narrow_condition_returns_all_rows() {
    let dir = std::env::temp_dir().join("semilattice-search-empty");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut database = Database::new(dir, None, 10);
    let id = database.collection_id_or_create("c");
    let name = FieldName::new("name".into());

    futures::executor::block_on(async {
        let collection = database.collection_mut(id).unwrap();
        for v in ["a", "b"] {
            collection
                .insert(
                    Activity::Active,
                    Term::Default,
                    Term::Default,
                    [(name.clone(), v.as_bytes().to_vec())].into(),
                )
                .await;
        }
        let result = database
            .search(id)
            .search(Condition::Narrow(vec![]))
            .result(&database)
            .await;
        assert_eq!(result.rows().len(), 2);
    });
}
