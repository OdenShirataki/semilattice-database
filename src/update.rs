use std::{
    collections::HashMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use versatile_data::{Activity, Field, KeyValue, Term};

use crate::{
    anyhow::Result,
    session::{SessionData, SessionOperation, TemporaryData, TemporaryDataEntity},
    CollectionRow, Database, Depend, Depends, Record,
};

pub fn incidentally_depend(
    session_data: &mut SessionData,
    pend_session_row: u32,
    relation_key: &str,
    depend_session_row: u32,
) {
    let row = *session_data.row.value(depend_session_row).unwrap();
    let depend = CollectionRow::new(
        *session_data
            .collection_id
            .value(depend_session_row)
            .unwrap(),
        if row == 0 { depend_session_row } else { row },
    );
    session_data
        .relation
        .insert(relation_key, pend_session_row, depend);
}

pub fn update_row(
    session_dir: &Path,
    session_data: &mut SessionData,
    session_row: u32,
    row: u32,
    activity: &Activity,
    term_begin: u64,
    term_end: u64,
    uuid: u128,
    fields: &Vec<KeyValue>,
) -> Result<()> {
    session_data.row.update(session_row, row)?;
    session_data.activity.update(session_row, *activity as u8)?;
    session_data.term_begin.update(session_row, term_begin)?;
    session_data.term_end.update(session_row, term_end)?;
    session_data.uuid.update(session_row, uuid)?;
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
                let field = Field::new(dir)?;
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
    main_database: &Database,
    session_data: &mut SessionData,
    temporary_data: &mut TemporaryData,
    session_dir: &Path,
    sequence_number: usize,
    records: &Vec<Record>,
    depend_by_pend: Option<(&str, u32)>,
) -> Result<Vec<CollectionRow>> {
    let mut ret = vec![];
    for record in records {
        if let Ok(session_row) = session_data.sequence.insert(sequence_number) {
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
                    let session_collection_id = -*collection_id;
                    ret.push(CollectionRow::new(session_collection_id, session_row));
                    let term_begin = if let Term::Overwrite(term_begin) = term_begin {
                        *term_begin
                    } else {
                        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
                    };
                    let term_end = if let Term::Overwrite(term_end) = term_end {
                        *term_end
                    } else {
                        0
                    };
                    let uuid = versatile_data::create_uuid();

                    session_data
                        .collection_id
                        .update(session_row, session_collection_id)?;
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
                        uuid,
                        fields,
                    )?;

                    let temprary_collection = temporary_data
                        .entry(session_collection_id)
                        .or_insert(HashMap::new());
                    temprary_collection.insert(
                        session_row as i64,
                        TemporaryDataEntity {
                            activity: *activity,
                            term_begin,
                            term_end,
                            uuid,
                            operation: SessionOperation::New,
                            fields: {
                                let mut tmp = HashMap::new();
                                for kv in fields {
                                    tmp.insert(kv.key().to_string(), kv.value().to_vec());
                                }
                                tmp
                            },
                            depends: if let Depends::Overwrite(depends) = depends {
                                let mut tmp = vec![];
                                for (key, depend) in depends {
                                    session_data
                                        .relation
                                        .insert(key, session_row, depend.clone());
                                    tmp.push(Depend::new(key, depend.clone()));
                                }
                                tmp
                            } else {
                                vec![]
                            },
                        },
                    );

                    if let Some((key, depend_session_row)) = depend_by_pend {
                        incidentally_depend(session_data, session_row, key, depend_session_row);
                    }
                    for pend in pends {
                        update_recursive(
                            main_database,
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
                    collection_id, //Negative values ​​contain session rows
                    row,
                    activity,
                    term_begin,
                    term_end,
                    fields,
                    depends,
                    pends,
                } => {
                    ret.push(CollectionRow::new(*collection_id, *row));

                    let in_session = *collection_id < 0;
                    let collection_id = if in_session {
                        -*collection_id
                    } else {
                        *collection_id
                    };
                    let row = *row;

                    let term_begin = match term_begin {
                        Term::Overwrite(term_begin) => *term_begin,
                        Term::Default => {
                            let mut r = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                            if !in_session {
                                if let Some(collection) = main_database.collection(collection_id) {
                                    r = collection.term_begin(row);
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

                    let uuid = {
                        if in_session {
                            if let Some(uuid) = session_data.uuid.value(row) {
                                *uuid
                            } else {
                                versatile_data::create_uuid()
                            }
                        } else {
                            if let Some(collection) = main_database.collection(collection_id) {
                                let uuid = collection.uuid(row);
                                if uuid == 0 {
                                    versatile_data::create_uuid()
                                } else {
                                    uuid
                                }
                            } else {
                                unreachable!();
                            }
                        }
                    };

                    session_data.collection_id.update(
                        session_row,
                        if in_session {
                            -collection_id
                        } else {
                            collection_id
                        },
                    )?;
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
                        uuid,
                        fields,
                    )?;

                    let mut tmp_depends = vec![];
                    match depends {
                        Depends::Default => {
                            if in_session {
                                session_data.relation.from_session_row(row, session_row)?;
                            } else {
                                for i in main_database
                                    .relation()
                                    .index_pend()
                                    .triee()
                                    .iter_by(|v| v.cmp(&CollectionRow::new(collection_id, row)))
                                    .map(|x| x.row())
                                {
                                    if let Some(depend) = main_database.relation().depend(i) {
                                        let key =
                                            unsafe { main_database.relation().key(i) }.unwrap();
                                        let depend = CollectionRow::new(
                                            depend.collection_id(),
                                            depend.row(),
                                        );
                                        session_data.relation.insert(
                                            key,
                                            session_row,
                                            depend.clone(),
                                        );
                                        tmp_depends.push(Depend::new(key, depend));
                                    }
                                }
                            }
                        }
                        Depends::Overwrite(depends) => {
                            for (key, depend) in depends {
                                session_data
                                    .relation
                                    .insert(key, session_row, depend.clone());
                                tmp_depends.push(Depend::new(key, depend.clone()));
                            }
                        }
                    }
                    col.entry(if in_session {
                        -(row as i64)
                    } else {
                        row as i64
                    })
                    .or_insert(TemporaryDataEntity {
                        activity: *activity,
                        term_begin,
                        term_end,
                        uuid,
                        operation: SessionOperation::Update,
                        fields: {
                            let mut tmp = HashMap::new();
                            for kv in fields {
                                tmp.insert(kv.key().into(), kv.value().into());
                            }
                            tmp
                        },
                        depends: tmp_depends,
                    });
                    if let Some((key, depend_session_row)) = depend_by_pend {
                        incidentally_depend(session_data, session_row, key, depend_session_row);
                    }
                    for pend in pends {
                        update_recursive(
                            main_database,
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
                    session_data.row.update(session_row, *row)?;
                    session_data
                        .operation
                        .update(session_row, SessionOperation::Delete)?;
                }
            }
        }
    }
    Ok(ret)
}
