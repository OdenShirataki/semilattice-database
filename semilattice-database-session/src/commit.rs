use std::num::NonZeroU32;

use semilattice_database::{Activity, KeyValue, Operation, Record, Term};

use hashbrown::HashMap;

use crate::{
    session::{SessionData, SessionOperation},
    CollectionRow, Session, SessionDatabase,
};

impl SessionDatabase {
    #[inline(always)]
    pub fn commit(&mut self, session: &mut Session) -> Vec<CollectionRow> {
        if let Some(ref mut data) = session.session_data {
            let r = self.commit_inner(data);
            self.session_clear(session);
            r
        } else {
            vec![]
        }
    }

    #[inline(always)]
    fn commit_inner(&mut self, session_data: &SessionData) -> Vec<CollectionRow> {
        let mut commit_rows = Vec::new();

        let mut session_collection_row_map: HashMap<CollectionRow, CollectionRow> = HashMap::new();
        let mut relation_temporary: HashMap<CollectionRow, Vec<(String, CollectionRow)>> =
            HashMap::new();

        for sequence in 1..=session_data.sequence_number.current() {
            for session_row in session_data
                .sequence
                .iter_by(|v| v.cmp(&sequence))
                .collect::<Vec<_>>()
                .iter()
                .rev()
            {
                let session_row = *session_row;
                if let (Some(op), Some(collection_id), Some(row)) = (
                    session_data.operation.value(session_row.get()),
                    session_data.collection_id.value(session_row.get()),
                    session_data.row.value(session_row.get()),
                ) {
                    let in_session = *collection_id < 0;

                    let main_collection_id = if in_session {
                        -*collection_id
                    } else {
                        *collection_id
                    };
                    let row = unsafe {
                        NonZeroU32::new_unchecked(if *row == 0 { session_row.get() } else { *row })
                    };
                    let fields = if *op == SessionOperation::Delete {
                        vec![]
                    } else {
                        session_data
                            .fields
                            .iter()
                            .filter_map(|(key, field_data)| {
                                field_data
                                    .bytes(session_row.get())
                                    .map(|val| KeyValue::new(key, val))
                            })
                            .collect()
                    };
                    if let Some(collection) = self.collection_mut(main_collection_id) {
                        let session_collection_row = CollectionRow::new(*collection_id, row.get());
                        match op {
                            SessionOperation::New | SessionOperation::Update => {
                                let activity = if *session_data
                                    .activity
                                    .value(session_row.get())
                                    .unwrap()
                                    == 1
                                {
                                    Activity::Active
                                } else {
                                    Activity::Inactive
                                };
                                let term_begin = Term::Overwrite(
                                    *session_data.term_begin.value(session_row.get()).unwrap(),
                                );
                                let term_end = Term::Overwrite(
                                    *session_data.term_end.value(session_row.get()).unwrap(),
                                );
                                let collection_row = CollectionRow::new(
                                    main_collection_id,
                                    collection.update(&if *op == SessionOperation::New {
                                        Operation::New(Record {
                                            activity,
                                            term_begin,
                                            term_end,
                                            fields,
                                        })
                                    } else {
                                        //SessionOperation::Update
                                        Operation::Update {
                                            row: if in_session {
                                                let main_collection_row =
                                                    session_collection_row_map
                                                        .get(&session_collection_row)
                                                        .unwrap();
                                                main_collection_row.row()
                                            } else {
                                                row
                                            }
                                            .get(),
                                            record: Record {
                                                activity,
                                                term_begin,
                                                term_end,
                                                fields,
                                            },
                                        }
                                    }),
                                );
                                commit_rows.push(collection_row.clone());
                                self.relation()
                                    .write()
                                    .unwrap()
                                    .delete_pends_by_collection_row(&collection_row); //Delete once and re-register later

                                for relation_row in session_data
                                    .relation
                                    .rows
                                    .session_row
                                    .iter_by(|v| v.cmp(&session_row.get()))
                                {
                                    if let (Some(key), Some(depend)) = (
                                        session_data.relation.rows.key.value(relation_row.get()),
                                        session_data.relation.rows.depend.value(relation_row.get()),
                                    ) {
                                        relation_temporary
                                            .entry(depend.clone())
                                            .or_insert_with(|| Vec::new())
                                            .push((
                                                unsafe {
                                                    std::str::from_utf8_unchecked(
                                                        session_data.relation.key_names.bytes(*key),
                                                    )
                                                }
                                                .to_owned(),
                                                session_collection_row.clone(),
                                            ));
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
                                        self.delete_recursive(registered);
                                    }
                                } else {
                                    self.delete_recursive(&CollectionRow::new(
                                        main_collection_id,
                                        row.get(),
                                    ));
                                }
                                session_collection_row_map.remove(&session_collection_row);
                            }
                        }
                    }
                }
            }
        }
        for (depend, pends) in relation_temporary {
            if depend.collection_id() < 0 {
                if let Some(depend) = session_collection_row_map.get(&depend) {
                    self.register_relations_with_session(
                        depend,
                        pends,
                        &session_collection_row_map,
                    );
                }
            } else {
                self.register_relations_with_session(&depend, pends, &session_collection_row_map);
            }
        }
        commit_rows
    }
}
