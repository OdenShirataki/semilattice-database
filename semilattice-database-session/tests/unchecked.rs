//! Inputs that used to cause undefined behavior.

use std::num::NonZeroI64;

use semilattice_database::{FieldName, search};
use semilattice_database_session::*;

fn new_database(name: &str) -> SessionDatabase {
    let dir = std::env::temp_dir().join(format!("semilattice-session-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    SessionDatabase::new(dir, None, 10)
}

#[test]
fn depends_with_rows_out_of_u32() {
    let mut database = new_database("depends-range");
    let collection = database.collection_id_or_create("c");
    let session = database.session("s", None);
    for row in [1i64 << 32, -(1i64 << 32), i64::MIN, i64::MAX] {
        let row = NonZeroI64::new(row).unwrap();
        assert!(
            database
                .depends_with_session(None, collection, row, Some(&session))
                .is_empty()
        );
        assert!(
            database
                .depends_with_session(None, collection, row, None)
                .is_empty()
        );
    }
}

#[test]
fn session_search_on_binary_field() {
    let mut database = new_database("binary-field");
    let collection = database.collection_id_or_create("c");
    let field = FieldName::new("data".into());
    futures::executor::block_on(async {
        let mut session = database.session("s", None);
        database
            .update(
                &mut session,
                vec![SessionRecord::Update {
                    collection_id: collection,
                    row: None,
                    activity: Activity::Active,
                    term_begin: Default::default(),
                    term_end: Default::default(),
                    fields: [(field.clone(), vec![0xff, 0xfe, b'a', b'b', 0xc0])].into(),
                    depends: Depends::Overwrite(vec![]),
                    pends: vec![],
                }],
            )
            .await;

        let find = |cond: search::Field| {
            let database = &database;
            let session = &session;
            let field = field.clone();
            async move {
                let search = database.search(collection).search_field(field, cond);
                session
                    .result_with(&search.result(database).await)
                    .await
                    .rows()
                    .len()
            }
        };
        assert_eq!(
            find(search::Field::Partial("ab".to_owned().into())).await,
            1
        );
        assert_eq!(
            find(search::Field::Partial("xy".to_owned().into())).await,
            0
        );
        assert_eq!(
            find(search::Field::Forward("ab".to_owned().into())).await,
            0
        );
        assert_eq!(
            find(search::Field::ValuePartial("ab".to_owned().into())).await,
            0
        );
    });
}
