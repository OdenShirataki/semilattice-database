use serde::Serialize;
use std::collections::HashMap;
use versatile_data::{Activity, KeyValue, Operation, Term};

use crate::{
    session::{SessionCollectionRow, SessionData, SessionOperation},
    CollectionRow, Database,
};

#[derive(Serialize)]
struct LogDepend {
    session_row: u32,
    depend: SessionCollectionRow,
}

pub fn commit(main_database: &mut Database, session_data: &SessionData) -> std::io::Result<()> {
    let mut session_collection_row_map: HashMap<u32, CollectionRow> = HashMap::new();

    let mut session_relation: HashMap<u32, Vec<(u32, SessionCollectionRow)>> = HashMap::new();
    for row in 1..session_data.relation.rows.session_row.max_rows()? {
        if let (Some(session_row), Some(depend)) = (
            session_data.relation.rows.session_row.value(row),
            session_data.relation.rows.depend.value(row),
        ) {
            let m = session_relation.entry(session_row).or_insert(Vec::new());
            m.push((row, depend));
        }
    }
    for session_row in 1..session_data.sequence.max_rows()? {
        if let (Some(op), Some(collection_id), Some(row)) = (
            session_data.operation.value(session_row),
            session_data.collection_id.value(session_row),
            session_data.row.value(session_row),
        ) {
            let fields = if op == SessionOperation::Delete {
                vec![]
            } else {
                let mut fields: Vec<KeyValue> = Vec::new();
                for (key, ref field_data) in &session_data.fields {
                    if let Some(val) = field_data.get(session_row) {
                        fields.push(KeyValue::new(key, val));
                    }
                }
                fields
            };

            if let Some(collection) = main_database.collection_mut(collection_id) {
                match op {
                    SessionOperation::New | SessionOperation::Update => {
                        let activity = if session_data.activity.value(session_row).unwrap() == 1 {
                            Activity::Active
                        } else {
                            Activity::Inactive
                        };
                        let term_begin =
                            Term::Overwrite(session_data.term_begin.value(session_row).unwrap());
                        let term_end =
                            Term::Overwrite(session_data.term_end.value(session_row).unwrap());
                        let collection_row = if row == 0 {
                            //new
                            let row = collection.create_row(
                                &activity,
                                &term_begin,
                                &term_end,
                                &fields,
                            )?;
                            CollectionRow::new(collection_id, row)
                        } else {
                            if row < 0 {
                                //update new data in session.
                                if let Some(master_collection_row) =
                                    session_collection_row_map.get(&(-row as u32))
                                {
                                    let row = master_collection_row.row();
                                    collection.update_row(
                                        row,
                                        &activity,
                                        &term_begin,
                                        &term_end,
                                        &fields,
                                    )?;
                                    CollectionRow::new(master_collection_row.collection_id(), row)
                                } else {
                                    panic!("crash");
                                }
                            } else {
                                //update
                                let row = row as u32;
                                collection.update_row(
                                    row,
                                    &activity,
                                    &term_begin,
                                    &term_end,
                                    &fields,
                                )?;
                                CollectionRow::new(collection_id, row)
                            }
                        };
                        main_database
                            .relation
                            .delete_by_collection_row(collection_row)?;
                        session_collection_row_map.insert(session_row, collection_row);
                        if let Some(depend_rows) = session_relation.get(&session_row) {
                            for (session_row, depend) in depend_rows {
                                let key =
                                    session_data.relation.rows.key.value(*session_row).unwrap();
                                let key =
                                    unsafe { session_data.relation.key_names.str(key) }.unwrap(); //todo:resultにerrorのタイプが複数ある場合はどうしたらいいんだろう

                                if depend.row < 0 {
                                    if let Some(depend) =
                                        session_collection_row_map.get(&((-depend.row) as u32))
                                    {
                                        main_database.relation.insert(
                                            key,
                                            *depend,
                                            collection_row,
                                        )?;
                                    }
                                } else {
                                    main_database.relation.insert(
                                        key,
                                        CollectionRow::new(depend.collection_id, depend.row as u32),
                                        collection_row,
                                    )?;
                                };
                            }
                        }
                    }
                    SessionOperation::Delete => {
                        //todo!("セッション考慮の削除処理");
                        delete_recursive(
                            main_database,
                            &SessionCollectionRow::new(collection_id, row),
                        )?;
                        if row > 0 {
                            if let Some(collection) = main_database.collection_mut(collection_id) {
                                collection.update(&Operation::Delete { row: row as u32 })?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn delete_recursive(
    database: &mut Database,
    target: &SessionCollectionRow,
) -> std::io::Result<()> {
    if target.row > 0 {
        let depend = CollectionRow::new(target.collection_id, target.row as u32);
        let c = database.relation.index_depend().select_by_value(&depend);
        for relation_row in c {
            if let Some(collection_row) = database.relation.index_pend().value(relation_row) {
                delete_recursive(
                    database,
                    &SessionCollectionRow::new(
                        collection_row.collection_id(),
                        collection_row.row() as i64,
                    ),
                )?;
                if let Some(collection) = database.collection_mut(collection_row.collection_id()) {
                    collection.update(&Operation::Delete {
                        row: collection_row.row(),
                    })?;
                }
            }
            database.relation.delete(relation_row)?;
        }
    }

    Ok(())
}
