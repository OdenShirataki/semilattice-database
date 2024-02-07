use std::{
    num::{NonZeroI32, NonZeroU32},
    sync::Arc,
};

use semilattice_database::{idx_binary::AvltrieeSearch, Activity, Term};

use hashbrown::HashMap;

use crate::{
    session::{SessionData, SessionOperation},
    CollectionRow, Session, SessionDatabase,
};

impl SessionDatabase {
    pub async fn commit(&mut self, session: &mut Session) -> Vec<CollectionRow> {
        if let Some(ref mut data) = session.session_data {
            let r = self.commit_inner(data).await;
            self.session_clear(session);
            r
        } else {
            vec![]
        }
    }

    async fn commit_inner(&mut self, session_data: &SessionData) -> Vec<CollectionRow> {
        let mut commit_rows = Vec::new();

        let mut session_collection_row_map: HashMap<CollectionRow, CollectionRow> = HashMap::new();
        let mut relation_temporary: HashMap<CollectionRow, Vec<(Arc<String>, CollectionRow)>> =
            HashMap::new();

        for sequence in 1..=session_data.sequence_number.current() {
            for session_row in session_data
                .sequence
                .iter_by(&sequence)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
            {
                if let (Some(op), Some(collection_id), Some(row)) = (
                    session_data.operation.value(session_row).cloned(),
                    session_data.collection_id.value(session_row).cloned(),
                    session_data.row.value(session_row).cloned(),
                ) {
                    let in_session = collection_id < 0;

                    let main_collection_id = NonZeroI32::new(if in_session {
                        -collection_id
                    } else {
                        collection_id
                    })
                    .unwrap();

                    if let Some(collection) = self.collection_mut(main_collection_id) {
                        let row = if row == 0 {
                            session_row
                        } else {
                            unsafe { NonZeroU32::new_unchecked(row) }
                        };

                        let fields = if op == SessionOperation::Delete {
                            HashMap::new()
                        } else {
                            session_data
                                .fields
                                .iter()
                                .filter_map(|(field_name, field_data)| {
                                    field_data
                                        .value(session_row)
                                        .map(|val| (field_name.clone(), val.to_owned()))
                                })
                                .collect()
                        };

                        let session_collection_row =
                            CollectionRow::new(NonZeroI32::new(collection_id).unwrap(), row);
                        match op {
                            SessionOperation::New | SessionOperation::Update => {
                                let activity = if *unsafe {
                                    session_data.activity.value_unchecked(session_row)
                                } == 1
                                {
                                    Activity::Active
                                } else {
                                    Activity::Inactive
                                };
                                let term_begin = Term::Overwrite(*unsafe {
                                    session_data.term_begin.value_unchecked(session_row)
                                });
                                let term_end = Term::Overwrite(*unsafe {
                                    session_data.term_end.value_unchecked(session_row)
                                });

                                let collection_row = CollectionRow::new(
                                    main_collection_id,
                                    if op == SessionOperation::New {
                                        collection
                                            .insert(activity, term_begin, term_end, fields)
                                            .await
                                    } else {
                                        let row = if in_session {
                                            let main_collection_row = session_collection_row_map
                                                .get(&session_collection_row)
                                                .unwrap();
                                            main_collection_row.row()
                                        } else {
                                            row
                                        };
                                        collection
                                            .update(row, activity, term_begin, term_end, fields)
                                            .await;
                                        row
                                    },
                                );
                                commit_rows.push(collection_row.clone());
                                self.relation_mut()
                                    .delete_pends_by_collection_row(&collection_row)
                                    .await; //Delete once and re-register later

                                for relation_row in session_data
                                    .relation
                                    .rows
                                    .session_row
                                    .iter_by(&session_row.get())
                                {
                                    if let (Some(key), Some(depend)) = (
                                        session_data.relation.rows.key.value(relation_row),
                                        session_data.relation.rows.depend.value(relation_row),
                                    ) {
                                        relation_temporary
                                            .entry(depend.clone())
                                            .or_insert_with(|| Vec::new())
                                            .push((
                                                Arc::new(
                                                    unsafe {
                                                        std::str::from_utf8_unchecked(
                                                            session_data
                                                                .relation
                                                                .key_names
                                                                .value(
                                                                    NonZeroU32::new(*key).unwrap(),
                                                                )
                                                                .unwrap(),
                                                        )
                                                    }
                                                    .to_owned(),
                                                ),
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
                                        self.delete(registered).await;
                                    }
                                } else {
                                    self.delete(&CollectionRow::new(main_collection_id, row))
                                        .await;
                                }
                                session_collection_row_map.remove(&session_collection_row);
                            }
                        }
                    }
                }
            }
        }
        for (depend, pends) in relation_temporary.into_iter() {
            if depend.collection_id().get() < 0 {
                if let Some(depend) = session_collection_row_map.get(&depend) {
                    self.register_relations_with_session(
                        depend,
                        pends,
                        &session_collection_row_map,
                    )
                    .await;
                }
            } else {
                self.register_relations_with_session(&depend, pends, &session_collection_row_map)
                    .await;
            }
        }
        commit_rows
    }
}
