use std::{
    num::{NonZeroI32, NonZeroI64, NonZeroU32},
    path::Path,
    sync::Arc,
};

use hashbrown::HashMap;

use semilattice_database::{Activity, CollectionRow, Depend, Field, FieldName, Fields, IdxFile};

use super::{
    relation::SessionRelation, sequence::SequenceNumber, SessionOperation, TemporaryData,
    TemporaryDataEntity,
};

pub struct SessionData {
    pub(crate) sequence_number: SequenceNumber,
    pub(crate) sequence: IdxFile<usize>,
    pub(crate) collection_id: IdxFile<i32>,
    pub(crate) row: IdxFile<u32>,
    pub(crate) operation: IdxFile<SessionOperation>,
    pub(crate) activity: IdxFile<u8>,
    pub(crate) term_begin: IdxFile<u64>,
    pub(crate) term_end: IdxFile<u64>,
    pub(crate) uuid: IdxFile<u128>,
    pub(crate) fields: Fields,
    pub(crate) relation: SessionRelation,
}

impl SessionData {
    pub async fn update(
        &mut self,
        session_dir: &Path,
        session_row: NonZeroU32,
        row: u32,
        activity: &Activity,
        term_begin: u64,
        term_end: u64,
        uuid: u128,
        fields: &HashMap<FieldName, Vec<u8>>,
    ) {
        futures::join!(
            async { self.row.update(session_row, row) },
            async { self.activity.update(session_row, *activity as u8) },
            async { self.term_begin.update(session_row, term_begin) },
            async { self.term_end.update(session_row, term_end) },
            async { self.uuid.update(session_row, uuid) }
        );

        for (field_name, value) in fields.into_iter() {
            let field = if self.fields.contains_key(field_name) {
                self.fields.get_mut(field_name).unwrap()
            } else {
                let mut dir = session_dir.to_path_buf();
                dir.push("fields");
                dir.push(field_name.to_string());
                std::fs::create_dir_all(&dir).unwrap();
                if dir.exists() {
                    let field = Field::new(dir, 1);
                    self.fields.entry(field_name.clone()).or_insert(field)
                } else {
                    panic!();
                }
            };
            //TODO: multi thread
            field.update(session_row, value);
        }
    }

    pub(crate) async fn incidentally_depend(
        &mut self,
        pend_session_row: NonZeroU32,
        relation_key: &str,
        depend_session_row: NonZeroU32,
    ) {
        let row = self.row.get(depend_session_row).unwrap();
        self.relation
            .insert(
                relation_key,
                pend_session_row,
                CollectionRow::new(
                    NonZeroI32::new(**self.collection_id.get(depend_session_row).unwrap()).unwrap(),
                    if **row == 0 {
                        depend_session_row
                    } else {
                        unsafe { NonZeroU32::new_unchecked(**row) }
                    },
                ),
            )
            .await;
    }

    #[inline(always)]
    pub(crate) fn init_temporary_data(&self) -> TemporaryData {
        let mut temporary_data = TemporaryData::new();
        let current = self.sequence_number.current();
        if current > 0 {
            let mut fields_overlaps: HashMap<CollectionRow, HashMap<FieldName, Vec<u8>>> =
                HashMap::new();
            for sequence in 1..=current {
                for session_row in self.sequence.iter_by(|v| v.cmp(&sequence)) {
                    if let Some(collection_id) = self.collection_id.get(session_row) {
                        let collection_id = **collection_id;

                        let in_session = collection_id < 0;
                        let main_collection_id = NonZeroI32::new(if in_session {
                            -collection_id
                        } else {
                            collection_id
                        })
                        .unwrap();
                        let temporary_collection = temporary_data
                            .entry(main_collection_id)
                            .or_insert(HashMap::new());
                        let row = **self.row.get(session_row).unwrap();

                        let temporary_row = NonZeroI64::new(if row == 0 {
                            -(session_row.get() as i64)
                        } else if in_session {
                            -(row as i64)
                        } else {
                            row as i64
                        })
                        .unwrap();

                        let operation = (**self.operation.get(session_row).unwrap()).clone();
                        if operation == SessionOperation::Delete {
                            temporary_collection.insert(
                                temporary_row,
                                TemporaryDataEntity {
                                    activity: Activity::Inactive,
                                    term_begin: 0,
                                    term_end: 0,
                                    uuid: 0,
                                    operation,
                                    fields: HashMap::new(),
                                    depends: vec![],
                                },
                            );
                        } else {
                            let row_fields = fields_overlaps
                                .entry(if row == 0 {
                                    CollectionRow::new(-main_collection_id, session_row)
                                } else {
                                    CollectionRow::new(
                                        NonZeroI32::new(collection_id).unwrap(),
                                        unsafe { NonZeroU32::new_unchecked(row) },
                                    )
                                })
                                .or_insert(HashMap::new());
                            self.fields.iter().for_each(|(field_name, val)| {
                                if let Some(v) = val.bytes(session_row) {
                                    row_fields.insert(field_name.clone(), v.into());
                                }
                            });
                            temporary_collection.insert(
                                temporary_row,
                                TemporaryDataEntity {
                                    activity: if **self.activity.get(session_row).unwrap() == 1 {
                                        Activity::Active
                                    } else {
                                        Activity::Inactive
                                    },
                                    term_begin: **self.term_begin.get(session_row).unwrap(),
                                    term_end: **self.term_end.get(session_row).unwrap(),
                                    uuid: self.uuid.get(session_row).map_or(0, |uuid| **uuid),
                                    operation,
                                    fields: row_fields.clone(),
                                    depends: {
                                        self.relation
                                            .rows
                                            .session_row
                                            .iter_by(|v| v.cmp(&session_row.get()))
                                            .filter_map(|relation_row| {
                                                if let (Some(key), Some(depend)) = (
                                                    self.relation.rows.key.get(relation_row),
                                                    self.relation.rows.depend.get(relation_row),
                                                ) {
                                                    Some(Depend::new(
                                                        Arc::new(
                                                            unsafe {
                                                                std::str::from_utf8_unchecked(
                                                                    self.relation
                                                                        .key_names
                                                                        .bytes(
                                                                            NonZeroU32::new(**key)
                                                                                .unwrap(),
                                                                        )
                                                                        .unwrap(),
                                                                )
                                                            }
                                                            .into(),
                                                        ),
                                                        (**depend).clone(),
                                                    ))
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect()
                                    },
                                },
                            );
                        }
                    }
                }
            }
        }
        temporary_data
    }
}
