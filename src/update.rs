use std::{
    collections::HashMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use versatile_data::{Activity, FieldData, KeyValue, Term};

use crate::{
    session::{
        SessionCollectionRow, SessionData, SessionOperation, TemporaryData, TemporaryDataEntity,
    },
    CollectionRow, Database, Depends, Record,
};

pub fn incidentally_depend(
    session_data: &mut SessionData,
    pend_session_row: u32,
    relation_key: &str,
    depend_session_row: u32,
) {
    let depend = SessionCollectionRow::new(
        session_data
            .collection_id
            .value(depend_session_row)
            .unwrap(),
        -(depend_session_row as i64),
    );
    session_data
        .relation
        .insert(relation_key, pend_session_row, depend);
}

pub fn update_row(
    session_dir: &Path,
    session_data: &mut SessionData,
    session_row: u32,
    row: i64,
    activity: &Activity,
    term_begin: u64,
    term_end: u64,
    fields: &Vec<KeyValue>,
) -> std::io::Result<()> {
    session_data.row.update(session_row, row)?;
    session_data.activity.update(session_row, *activity as u8)?;
    session_data.term_begin.update(session_row, term_begin)?;
    session_data.term_end.update(session_row, term_end)?;
    for kv in fields {
        let key = kv.key();
        let field = if session_data.fields.contains_key(key) {
            session_data.fields.get_mut(key).unwrap()
        } else {
            let mut dir = session_dir.to_path_buf();
            dir.push("fields");
            dir.push(key);
            std::fs::create_dir_all(&dir)?;
            if dir.exists() {
                let field = FieldData::new(&dir)?;
                session_data
                    .fields
                    .entry(String::from(key))
                    .or_insert(field);
            }
            session_data.fields.get_mut(key).unwrap()
        };
        field.update(session_row, kv.value())?;
    }
    Ok(())
}

pub(super) fn update_recursive(
    master_database: &Database,
    session_data: &mut SessionData,
    temporary_data: &mut TemporaryData,
    session_dir: &Path,
    sequence_number: usize,
    records: &Vec<Record>,
    depend_by_pend: Option<(&str, u32)>,
) -> std::io::Result<()> {
    for record in records {
        if let Ok(session_row) = session_data.sequence.insert(sequence_number) {
            session_data.row.resize_to(session_row)?;
            session_data.operation.resize_to(session_row)?;
            session_data.activity.resize_to(session_row)?;
            session_data.term_begin.resize_to(session_row)?;
            session_data.term_end.resize_to(session_row)?;

            match record {
                Record::New {
                    collection_id,
                    activity,
                    term_begin,
                    term_end,
                    fields,
                    depends,
                    pends,
                } => {
                    let collection_id = *collection_id;

                    let virtual_row = -(session_row as i64);

                    let term_begin = if let Term::Overwrite(term_begin) = term_begin {
                        *term_begin
                    } else {
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                    };
                    let term_end = if let Term::Overwrite(term_end) = term_end {
                        *term_end
                    } else {
                        0
                    };

                    let col = temporary_data
                        .entry(collection_id)
                        .or_insert(HashMap::new());
                    let mut tmp_fields = HashMap::new();
                    for kv in fields {
                        tmp_fields.insert(kv.key().to_string(), kv.value().to_vec());
                    }
                    col.insert(
                        virtual_row,
                        TemporaryDataEntity {
                            activity: *activity,
                            term_begin,
                            term_end,
                            fields: tmp_fields,
                        },
                    );

                    session_data
                        .collection_id
                        .update(session_row, collection_id)?;
                    session_data
                        .operation
                        .update(session_row, SessionOperation::New)?;
                    update_row(
                        session_dir,
                        session_data,
                        session_row,
                        0,
                        activity,
                        term_begin,
                        term_end,
                        fields,
                    )?;

                    if let Depends::Overwrite(depends) = depends {
                        for (key, depend) in depends {
                            session_data.relation.insert(key, session_row, *depend);
                        }
                    }
                    if let Some((key, depend_session_row)) = depend_by_pend {
                        incidentally_depend(session_data, session_row, key, depend_session_row);
                    }
                    for pend in pends {
                        update_recursive(
                            master_database,
                            session_data,
                            temporary_data,
                            session_dir,
                            sequence_number,
                            pend.records(),
                            Some((pend.key(), session_row)),
                        )?;
                    }
                }
                Record::Update {
                    collection_id,
                    row, //-の場合はセッション新規データの更新
                    activity,
                    term_begin,
                    term_end,
                    fields,
                    depends,
                    pends,
                } => {
                    let collection_id = *collection_id;
                    let row = *row;

                    let term_begin = match term_begin {
                        Term::Overwrite(term_begin) => *term_begin,
                        Term::Defalut => {
                            let mut r = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            if row > 0 {
                                if let Some(collection) = master_database.collection(collection_id)
                                {
                                    r = collection.term_begin(row as u32);
                                }
                            }
                            r
                        }
                    };
                    let term_end = if let Term::Overwrite(term_end) = term_end {
                        *term_end
                    } else {
                        0
                    };

                    let col = temporary_data
                        .entry(collection_id)
                        .or_insert(HashMap::new());
                    let entity = col.entry(row).or_insert(TemporaryDataEntity {
                        activity: *activity,
                        term_begin,
                        term_end,
                        fields: HashMap::new(),
                    });
                    for kv in fields {
                        entity.fields.insert(kv.key().into(), kv.value().into());
                    }

                    session_data
                        .collection_id
                        .update(session_row, collection_id)?;
                    session_data
                        .operation
                        .update(session_row, SessionOperation::Update)?;
                    update_row(
                        session_dir,
                        session_data,
                        session_row,
                        row,
                        activity,
                        term_begin,
                        term_end,
                        fields,
                    )?;
                    match depends {
                        Depends::Default => {
                            if row > 0 {
                                let depends =
                                    master_database.relation().index_pend().select_by_value(
                                        &CollectionRow::new(collection_id, row as u32),
                                    );
                                for i in depends {
                                    if let Some(depend) =
                                        master_database.relation().depend(i as u32)
                                    {
                                        session_data.relation.insert(
                                            unsafe { master_database.relation().key(i as u32) }
                                                .unwrap(),
                                            session_row,
                                            SessionCollectionRow::new(
                                                depend.collection_id(),
                                                depend.row() as i64,
                                            ),
                                        );
                                    }
                                }
                            } else {
                                session_data
                                    .relation
                                    .from_session_row((-row) as u32, session_row);
                            }
                        }
                        Depends::Overwrite(depends) => {
                            for (key, depend) in depends {
                                session_data.relation.insert(key, session_row, *depend);
                            }
                        }
                    }
                    if let Some((key, depend_session_row)) = depend_by_pend {
                        incidentally_depend(session_data, session_row, key, depend_session_row);
                    }
                    for pend in pends {
                        update_recursive(
                            master_database,
                            session_data,
                            temporary_data,
                            session_dir,
                            sequence_number,
                            pend.records(),
                            Some((pend.key(), session_row)),
                        )?;
                    }
                }
                Record::Delete { collection_id, row } => {
                    session_data
                        .collection_id
                        .update(session_row, *collection_id)?;
                    session_data.row.update(session_row, *row as i64)?;
                    session_data
                        .operation
                        .update(session_row, SessionOperation::Delete)?;
                }
            }
        }
    }
    Ok(())
}
