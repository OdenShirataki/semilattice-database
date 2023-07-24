use std::{
    collections::HashMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    anyhow::Result,
    session::{Depends, SessionData, SessionOperation, TemporaryData, TemporaryDataEntity},
    CollectionRow, Depend, SessionDatabase, SessionRecord, Term,
};

impl SessionDatabase {
    pub(super) fn update_recursive(
        &self,
        session_data: &mut SessionData,
        temporary_data: &mut TemporaryData,
        session_dir: &Path,
        sequence_number: usize,
        records: &Vec<SessionRecord>,
        depend_by_pend: Option<(&str, u32)>,
    ) -> Result<Vec<CollectionRow>> {
        let mut ret = vec![];
        for record in records {
            if let Ok(session_row) = session_data.sequence.insert(sequence_number) {
                match record {
                    SessionRecord::New {
                        collection_id,
                        record,
                        depends,
                        pends,
                    } => {
                        let session_collection_id = -*collection_id;
                        ret.push(CollectionRow::new(session_collection_id, session_row));
                        let term_begin = if let Term::Overwrite(term_begin) = record.term_begin {
                            term_begin
                        } else {
                            SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
                        };
                        let term_end = if let Term::Overwrite(term_end) = record.term_end {
                            term_end
                        } else {
                            0
                        };
                        let uuid = semilattice_database::create_uuid();

                        session_data
                            .collection_id
                            .update(session_row, session_collection_id)?;
                        session_data
                            .operation
                            .update(session_row, SessionOperation::New)?;
                        session_data.update(
                            session_dir,
                            session_row,
                            0,
                            &record.activity,
                            term_begin,
                            term_end,
                            uuid,
                            &record.fields,
                        )?;

                        let temprary_collection = temporary_data
                            .entry(-session_collection_id)
                            .or_insert(HashMap::new());
                        temprary_collection.insert(
                            -(session_row as i64),
                            TemporaryDataEntity {
                                activity: record.activity,
                                term_begin,
                                term_end,
                                uuid,
                                operation: SessionOperation::New,
                                fields: {
                                    let mut tmp = HashMap::new();
                                    for kv in &record.fields {
                                        tmp.insert(kv.key().to_string(), kv.value().to_vec());
                                    }
                                    tmp
                                },
                                depends: if let Depends::Overwrite(depends) = depends {
                                    let mut tmp = vec![];
                                    for (key, depend) in depends {
                                        session_data.relation.insert(
                                            key,
                                            session_row,
                                            depend.clone(),
                                        );
                                        tmp.push(Depend::new(key, depend.clone()));
                                    }
                                    tmp
                                } else {
                                    vec![]
                                },
                            },
                        );

                        if let Some((key, depend_session_row)) = depend_by_pend {
                            session_data.incidentally_depend(session_row, key, depend_session_row);
                        }
                        for pend in pends {
                            self.update_recursive(
                                session_data,
                                temporary_data,
                                session_dir,
                                sequence_number,
                                pend.records(),
                                Some((pend.key(), session_row)),
                            )?;
                        }
                    }
                    SessionRecord::Update {
                        collection_id, //Negative values ​​contain session rows
                        row,
                        record,
                        depends,
                        pends,
                    } => {
                        let row = *row;
                        let collection_id = *collection_id;
                        ret.push(CollectionRow::new(collection_id, row));

                        let in_session = collection_id < 0;
                        let master_collection_id = if in_session {
                            -collection_id
                        } else {
                            collection_id
                        };

                        let term_begin = match record.term_begin {
                            Term::Overwrite(term_begin) => term_begin,
                            Term::Default => {
                                let mut r = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                                if !in_session {
                                    if let Some(collection) = self.collection(master_collection_id)
                                    {
                                        r = collection.term_begin(row).unwrap_or(0);
                                    }
                                }
                                r
                            }
                        };
                        let term_end = if let Term::Overwrite(term_end) = record.term_end {
                            term_end
                        } else {
                            0
                        };

                        let temporary_collection = temporary_data
                            .entry(master_collection_id)
                            .or_insert(HashMap::new());

                        let uuid = {
                            if in_session {
                                if let Some(uuid) = session_data.uuid.value(row) {
                                    *uuid
                                } else {
                                    semilattice_database::create_uuid()
                                }
                            } else {
                                if let Some(collection) = self.collection(master_collection_id) {
                                    let uuid = collection.uuid(row).unwrap_or(0);
                                    if uuid == 0 {
                                        semilattice_database::create_uuid()
                                    } else {
                                        uuid
                                    }
                                } else {
                                    unreachable!();
                                }
                            }
                        };

                        session_data
                            .collection_id
                            .update(session_row, collection_id)?;
                        session_data
                            .operation
                            .update(session_row, SessionOperation::Update)?;
                        session_data.update(
                            session_dir,
                            session_row,
                            row,
                            &record.activity,
                            term_begin,
                            term_end,
                            uuid,
                            &record.fields,
                        )?;

                        let mut tmp_depends = vec![];
                        match depends {
                            Depends::Default => {
                                if in_session {
                                    session_data.relation.from_session_row(row, session_row)?;
                                } else {
                                    for i in self
                                        .relation()
                                        .read()
                                        .unwrap()
                                        .index_pend()
                                        .iter_by(|v| v.cmp(&CollectionRow::new(collection_id, row)))
                                        .map(|x| x.row())
                                    {
                                        if let Some(depend) =
                                            self.relation().read().unwrap().depend(i)
                                        {
                                            let key =
                                                unsafe { self.relation().read().unwrap().key(i) }
                                                    .unwrap()
                                                    .to_owned();
                                            session_data.relation.insert(
                                                &key,
                                                session_row,
                                                depend.clone(),
                                            );
                                            tmp_depends.push(Depend::new(key, depend.clone()));
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
                        temporary_collection
                            .entry(if in_session {
                                -(row as i64)
                            } else {
                                row as i64
                            })
                            .or_insert(TemporaryDataEntity {
                                activity: record.activity,
                                term_begin,
                                term_end,
                                uuid,
                                operation: SessionOperation::Update,
                                fields: {
                                    let mut tmp = HashMap::new();
                                    for kv in &record.fields {
                                        tmp.insert(kv.key().into(), kv.value().into());
                                    }
                                    tmp
                                },
                                depends: tmp_depends,
                            });
                        if let Some((key, depend_session_row)) = depend_by_pend {
                            session_data.incidentally_depend(session_row, key, depend_session_row);
                        }
                        for pend in pends {
                            self.update_recursive(
                                session_data,
                                temporary_data,
                                session_dir,
                                sequence_number,
                                pend.records(),
                                Some((pend.key(), session_row)),
                            )?;
                        }
                    }
                    SessionRecord::Delete { collection_id, row } => {
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
}
