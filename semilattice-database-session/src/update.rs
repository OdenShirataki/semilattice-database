use std::{
    num::{NonZeroI64, NonZeroU32},
    ops::Deref,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use async_recursion::async_recursion;
use hashbrown::HashMap;

use crate::{
    session::{Depends, SessionData, SessionOperation, TemporaryData, TemporaryDataEntity},
    CollectionRow, Depend, SessionDatabase, SessionRecord, Term,
};

impl SessionDatabase {
    #[async_recursion(?Send)]
    pub(super) async fn update_recursive(
        &self,
        session_data: &mut SessionData,
        temporary_data: &mut TemporaryData,
        session_dir: &Path,
        sequence_number: usize,
        records: &Vec<SessionRecord>,
        depend_by_pend: Option<(&'async_recursion str, NonZeroU32)>,
    ) -> Vec<CollectionRow> {
        let mut ret = vec![];
        for record in records.into_iter() {
            let session_row = session_data.sequence.insert(sequence_number);

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
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                    };
                    let term_end = if let Term::Overwrite(term_end) = record.term_end {
                        term_end
                    } else {
                        0
                    };
                    let uuid = semilattice_database::create_uuid();

                    futures::join!(
                        async {
                            session_data
                                .collection_id
                                .update(session_row, session_collection_id.get())
                        },
                        async {
                            session_data
                                .operation
                                .update(session_row, SessionOperation::New)
                        },
                    );
                    session_data
                        .update(
                            session_dir,
                            session_row,
                            0,
                            &record.activity,
                            term_begin,
                            term_end,
                            uuid,
                            &record.fields,
                        )
                        .await;

                    let temprary_collection = temporary_data
                        .entry(-session_collection_id)
                        .or_insert(HashMap::new());
                    temprary_collection.insert(
                        (-(session_row.get() as i64)).try_into().unwrap(),
                        TemporaryDataEntity {
                            activity: record.activity,
                            term_begin,
                            term_end,
                            uuid,
                            operation: SessionOperation::New,
                            fields: record
                                .fields
                                .iter()
                                .map(|(key, value)| (key.into(), value.to_vec()))
                                .collect(),
                            depends: if let Depends::Overwrite(depends) = depends {
                                let mut tmp = vec![];
                                for (key, depend) in depends.into_iter() {
                                    session_data
                                        .relation
                                        .insert(key, session_row, depend.clone())
                                        .await;
                                    tmp.push(Depend::new(key, depend.clone()));
                                }
                                tmp
                            } else {
                                vec![]
                            },
                        },
                    );

                    if let Some((key, depend_session_row)) = depend_by_pend {
                        session_data
                            .incidentally_depend(session_row, key, depend_session_row)
                            .await;
                    }
                    for pend in pends.into_iter() {
                        self.update_recursive(
                            session_data,
                            temporary_data,
                            session_dir,
                            sequence_number,
                            &pend.records,
                            Some((&pend.key, session_row)),
                        )
                        .await;
                    }
                }
                SessionRecord::Update {
                    collection_id, //Negative values ​​contain session rows
                    row,
                    record,
                    depends,
                    pends,
                } => {
                    let collection_id = *collection_id;
                    ret.push(CollectionRow::new(collection_id, *row));

                    let in_session = collection_id.get() < 0;
                    let master_collection_id = if in_session {
                        -collection_id
                    } else {
                        collection_id
                    };

                    let term_begin = match record.term_begin {
                        Term::Overwrite(term_begin) => term_begin,
                        Term::Default => (!in_session)
                            .then(|| {
                                self.collection(master_collection_id)
                                    .map(|collection| collection.term_begin(*row).unwrap_or(0))
                            })
                            .and_then(|v| v)
                            .unwrap_or_else(|| {
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs()
                            }),
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
                            session_data.uuid.get(*row).map_or_else(
                                || semilattice_database::create_uuid(),
                                |uuid| *uuid.deref(),
                            )
                        } else {
                            if let Some(collection) = self.collection(master_collection_id) {
                                let uuid = collection.uuid(*row).unwrap_or(0);
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

                    futures::join!(
                        async {
                            session_data
                                .collection_id
                                .update(session_row, collection_id.get())
                        },
                        async {
                            session_data
                                .operation
                                .update(session_row, SessionOperation::Update)
                        },
                    );
                    session_data
                        .update(
                            session_dir,
                            session_row,
                            row.get(),
                            &record.activity,
                            term_begin,
                            term_end,
                            uuid,
                            &record.fields,
                        )
                        .await;

                    let mut tmp_depends = vec![];
                    match depends {
                        Depends::Default => {
                            if in_session {
                                session_data
                                    .relation
                                    .from_session_row(*row, session_row)
                                    .await;
                            } else {
                                for i in self
                                    .relation()
                                    .index_pend()
                                    .iter_by(|v| v.cmp(&CollectionRow::new(collection_id, *row)))
                                {
                                    if let Some(depend) = self.relation().depend(i) {
                                        let key = self.relation().key(i).to_owned();
                                        session_data
                                            .relation
                                            .insert(&key, session_row, depend.clone())
                                            .await;
                                        tmp_depends.push(Depend::new(key, depend.clone()));
                                    }
                                }
                            }
                        }
                        Depends::Overwrite(depends) => {
                            for (key, depend) in depends.into_iter() {
                                session_data
                                    .relation
                                    .insert(key, session_row, depend.clone())
                                    .await;
                                tmp_depends.push(Depend::new(key, depend.clone()));
                            }
                        }
                    }
                    temporary_collection
                        .entry(
                            NonZeroI64::new(if in_session {
                                -(row.get() as i64)
                            } else {
                                row.get() as i64
                            })
                            .unwrap(),
                        )
                        .or_insert(TemporaryDataEntity {
                            activity: record.activity,
                            term_begin,
                            term_end,
                            uuid,
                            operation: SessionOperation::Update,
                            fields: record
                                .fields
                                .iter()
                                .map(|(key, value)| (key.into(), value.to_vec()))
                                .collect(),
                            depends: tmp_depends,
                        });
                    if let Some((key, depend_session_row)) = depend_by_pend {
                        session_data
                            .incidentally_depend(session_row, key, depend_session_row)
                            .await;
                    }
                    for pend in pends.into_iter() {
                        self.update_recursive(
                            session_data,
                            temporary_data,
                            session_dir,
                            sequence_number,
                            &pend.records,
                            Some((&pend.key, session_row)),
                        )
                        .await;
                    }
                }
                SessionRecord::Delete { collection_id, row } => {
                    futures::join!(
                        async {
                            session_data
                                .collection_id
                                .update(session_row, collection_id.get())
                        },
                        async { session_data.row.update(session_row, row.get()) },
                        async {
                            session_data
                                .operation
                                .update(session_row, SessionOperation::Delete)
                        }
                    );
                }
            }
        }
        ret
    }
}
