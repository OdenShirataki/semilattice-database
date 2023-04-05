use std::collections::HashMap;
use versatile_data::{Activity, KeyValue, Operation, Term};

use crate::{
    session::{SessionCollectionRow, SessionData, SessionOperation},
    CollectionRow, Database,
};

pub fn commit(
    main_database: &mut Database,
    session_data: &SessionData,
) -> Result<Vec<CollectionRow>, anyhow::Error> {
    let mut commit_rows = Vec::new();

    let mut session_collection_row_map: HashMap<SessionCollectionRow, CollectionRow> =
        HashMap::new();

    for sequence in 1..=session_data.sequence_number.current() {
        for session_row in session_data.sequence.select_by_value(&sequence) {
            if let (Some(op), Some(collection_id), Some(row)) = (
                session_data.operation.value(session_row),
                session_data.collection_id.value(session_row),
                session_data.row.value(session_row),
            ) {
                let row = if row == 0 { -(session_row as i64) } else { row };
                let fields = if op == SessionOperation::Delete {
                    vec![]
                } else {
                    let mut fields: Vec<KeyValue> = Vec::new();
                    for (key, field_data) in session_data.fields.iter() {
                        if let Some(val) = field_data.get(session_row) {
                            fields.push(KeyValue::new(key, val));
                        }
                    }
                    fields
                };

                if let Some(collection) = main_database.collection_mut(collection_id) {
                    let session_collection_row = SessionCollectionRow::new(collection_id, row);
                    match op {
                        SessionOperation::New | SessionOperation::Update => {
                            let activity = if session_data.activity.value(session_row).unwrap() == 1
                            {
                                Activity::Active
                            } else {
                                Activity::Inactive
                            };
                            let term_begin = Term::Overwrite(
                                session_data.term_begin.value(session_row).unwrap(),
                            );
                            let term_end =
                                Term::Overwrite(session_data.term_end.value(session_row).unwrap());
                            let collection_row = CollectionRow::new(
                                collection_id,
                                if op == SessionOperation::New {
                                    //new
                                    collection.update(&Operation::New {
                                        activity,
                                        term_begin,
                                        term_end,
                                        fields,
                                    })?
                                } else {
                                    if row < 0 {
                                        //update new data in session.
                                        let master_collection_row = session_collection_row_map
                                            .get(&session_collection_row)
                                            .unwrap();
                                        collection.update(&Operation::Update {
                                            row: master_collection_row.row(),
                                            activity,
                                            term_begin,
                                            term_end,
                                            fields,
                                        })?
                                    } else {
                                        //update
                                        collection.update(&Operation::Update {
                                            row: row as u32,
                                            activity,
                                            term_begin,
                                            term_end,
                                            fields,
                                        })?
                                    }
                                },
                            );

                            commit_rows.push(collection_row);
                            main_database
                                .relation
                                .delete_pends_by_collection_row(&collection_row)?; //Delete once and re-register later

                            for relation_row in session_data
                                .relation
                                .rows
                                .session_row
                                .select_by_value(&session_row)
                                .iter()
                            {
                                if let (Some(key), Some(depend)) = (
                                    session_data.relation.rows.key.value(*relation_row),
                                    session_data.relation.rows.depend.value(*relation_row),
                                ) {
                                    if let Ok(key_name) =
                                        unsafe { session_data.relation.key_names.str(key) }
                                    {
                                        if depend.row < 0 {
                                            if let Some(depend) =
                                                session_collection_row_map.get(&depend)
                                            {
                                                main_database.relation.insert(
                                                    key_name,
                                                    *depend,
                                                    collection_row,
                                                )?;
                                            }
                                        } else {
                                            main_database.relation.insert(
                                                key_name,
                                                CollectionRow::new(
                                                    depend.collection_id,
                                                    depend.row as u32,
                                                ),
                                                collection_row,
                                            )?;
                                        };
                                    }
                                }
                            }
                            session_collection_row_map
                                .insert(session_collection_row, collection_row);
                        }
                        SessionOperation::Delete => {
                            //todo!("セッション考慮の削除処理");
                            delete_recursive(
                                main_database,
                                &SessionCollectionRow::new(collection_id, row),
                            )?;
                            if row > 0 {
                                if let Some(collection) =
                                    main_database.collection_mut(collection_id)
                                {
                                    collection.update(&Operation::Delete { row: row as u32 })?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(commit_rows)
}

pub(super) fn delete_recursive(
    database: &mut Database,
    target: &SessionCollectionRow,
) -> std::io::Result<()> {
    if target.row > 0 {
        let depend = CollectionRow::new(target.collection_id, target.row as u32);

        for relation_row in database.relation.index_depend().select_by_value(&depend) {
            let mut chain = None;
            if let Some(collection_row) = database.relation.index_pend().value(relation_row) {
                chain = Some(collection_row);
            }
            database.relation.delete(relation_row)?;
            if let Some(collection_row) = chain {
                let collection_id = collection_row.collection_id();
                let row = collection_row.row();
                delete_recursive(
                    database,
                    &SessionCollectionRow::new(collection_id, row as i64),
                )?;
                if let Some(collection) = database.collection_mut(collection_id) {
                    collection.update(&Operation::Delete { row })?;
                }
            }
        }

        for relation_row in database.relation.index_pend().select_by_value(&depend) {
            database.relation.delete(relation_row)?;
        }
    }

    Ok(())
}
