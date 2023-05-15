use std::collections::HashMap;
use versatile_data::{Activity, KeyValue, Operation, Term};

use crate::{
    anyhow::Result,
    session::{SessionData, SessionOperation},
    CollectionRow, Database,
};

pub fn commit(
    main_database: &mut Database,
    session_data: &SessionData,
) -> Result<Vec<CollectionRow>> {
    let mut commit_rows = Vec::new();

    let mut session_collection_row_map: HashMap<CollectionRow, CollectionRow> = HashMap::new();
    let mut relation_temporary: HashMap<CollectionRow, Vec<(String, CollectionRow)>> =
        HashMap::new();

    for sequence in 1..=session_data.sequence_number.current() {
        for session_row in session_data
            .sequence
            .triee()
            .iter_by(|v| v.cmp(&sequence))
            .map(|r| r.row())
            .collect::<Vec<u32>>()
            .iter()
            .rev()
        {
            let session_row = *session_row;
            if let (Some(op), Some(collection_id), Some(row)) = (
                session_data.operation.value(session_row),
                session_data.collection_id.value(session_row),
                session_data.row.value(session_row),
            ) {
                let in_session = *collection_id < 0;

                let main_collection_id = if in_session {
                    -*collection_id
                } else {
                    *collection_id
                };
                let row = if *row == 0 { session_row } else { *row };
                let fields = if *op == SessionOperation::Delete {
                    vec![]
                } else {
                    let mut fields: Vec<KeyValue> = Vec::new();
                    for (key, field_data) in session_data.fields.iter() {
                        if let Some(val) = field_data.bytes(session_row) {
                            fields.push(KeyValue::new(key, val));
                        }
                    }
                    fields
                };
                if let Some(collection) = main_database.collection_mut(main_collection_id) {
                    let session_collection_row = CollectionRow::new(*collection_id, row);
                    match op {
                        SessionOperation::New | SessionOperation::Update => {
                            let activity =
                                if *session_data.activity.value(session_row).unwrap() == 1 {
                                    Activity::Active
                                } else {
                                    Activity::Inactive
                                };
                            let term_begin = Term::Overwrite(
                                *session_data.term_begin.value(session_row).unwrap(),
                            );
                            let term_end =
                                Term::Overwrite(*session_data.term_end.value(session_row).unwrap());
                            let collection_row = CollectionRow::new(
                                main_collection_id,
                                if *op == SessionOperation::New {
                                    collection.update(&Operation::New {
                                        activity,
                                        term_begin,
                                        term_end,
                                        fields,
                                    })?
                                } else {
                                    //SessionOperation::Update
                                    let row = if in_session {
                                        let main_collection_row = session_collection_row_map
                                            .get(&session_collection_row)
                                            .unwrap();
                                        main_collection_row.row()
                                    } else {
                                        row
                                    };
                                    collection.update(&Operation::Update {
                                        row,
                                        activity,
                                        term_begin,
                                        term_end,
                                        fields,
                                    })?
                                },
                            );
                            commit_rows.push(collection_row.clone());
                            main_database
                                .relation
                                .delete_pends_by_collection_row(&collection_row)?; //Delete once and re-register later

                            for relation_row in session_data
                                .relation
                                .rows
                                .session_row
                                .triee()
                                .iter_by(|v| v.cmp(&session_row))
                                .map(|x| x.row())
                            {
                                if let (Some(key), Some(depend)) = (
                                    session_data.relation.rows.key.value(relation_row),
                                    session_data.relation.rows.depend.value(relation_row),
                                ) {
                                    let key_name = unsafe {
                                        std::str::from_utf8_unchecked(
                                            session_data.relation.key_names.bytes(*key),
                                        )
                                    };
                                    let tmp = relation_temporary
                                        .entry(depend.clone())
                                        .or_insert_with(|| Vec::new());
                                    tmp.push((key_name.to_owned(), session_collection_row.clone()));
                                }
                            }
                            session_collection_row_map
                                .insert(session_collection_row, collection_row);
                        }
                        SessionOperation::Delete => {
                            if in_session {
                                if let Some(registered) =
                                    session_collection_row_map.get(&session_collection_row)
                                {
                                    delete_recursive(main_database, registered)?;
                                }
                                session_collection_row_map.remove(&session_collection_row);
                            } else {
                                delete_recursive(
                                    main_database,
                                    &CollectionRow::new(main_collection_id, row),
                                )?;
                            }
                        }
                    }
                }
            }
        }
    }
    for (depend, pends) in relation_temporary {
        if depend.collection_id() < 0 {
            if let Some(depend) = session_collection_row_map.get(&depend) {
                register_relations(main_database, depend, pends, &session_collection_row_map)?;
            }
        } else {
            register_relations(main_database, &depend, pends, &session_collection_row_map)?;
        }
    }
    Ok(commit_rows)
}
fn register_relations(
    main_database: &mut Database,
    depend: &CollectionRow,
    pends: Vec<(String, CollectionRow)>,
    row_map: &HashMap<CollectionRow, CollectionRow>,
) -> Result<()> {
    for (key_name, pend) in pends {
        if pend.collection_id() < 0 {
            if let Some(pend) = row_map.get(&pend) {
                main_database
                    .relation
                    .insert(&key_name, depend.clone(), pend.clone())?;
            }
        } else {
            main_database
                .relation
                .insert(&key_name, depend.clone(), pend)?;
        }
    }
    Ok(())
}

pub(super) fn delete_recursive(database: &mut Database, target: &CollectionRow) -> Result<()> {
    for relation_row in database
        .relation
        .index_depend()
        .triee()
        .iter_by(|v| v.cmp(&target))
        .map(|x| x.row())
        .collect::<Vec<u32>>()
    {
        let mut chain = None;
        if let Some(collection_row) = database.relation.index_pend().value(relation_row) {
            chain = Some(collection_row.clone());
        }
        database.relation.delete(relation_row)?;
        if let Some(collection_row) = chain {
            let collection_id = collection_row.collection_id();
            let row = collection_row.row();
            delete_recursive(database, &CollectionRow::new(collection_id, row))?;
        }
        if let Some(collection) = database.collection_mut(target.collection_id()) {
            collection.update(&Operation::Delete { row: target.row() })?;
        }
    }

    for relation_row in database
        .relation
        .index_pend()
        .triee()
        .iter_by(|v| v.cmp(&target))
        .map(|x| x.row())
        .collect::<Vec<u32>>()
    {
        database.relation.delete(relation_row)?;
    }

    Ok(())
}
