use std::{collections::HashMap, path::Path};

use semilattice_database::{Activity, CollectionRow, Depend, Field, IdxFile, KeyValue};

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
    pub(crate) fields: HashMap<String, Field>,
    pub(crate) relation: SessionRelation,
}

impl SessionData {
    pub fn update(
        &mut self,
        session_dir: &Path,
        session_row: u32,
        row: u32,
        activity: &Activity,
        term_begin: u64,
        term_end: u64,
        uuid: u128,
        fields: &Vec<KeyValue>,
    ) {
        self.row.update(session_row, row);
        self.activity.update(session_row, *activity as u8);
        self.term_begin.update(session_row, term_begin);
        self.term_end.update(session_row, term_end);
        self.uuid.update(session_row, uuid);
        for kv in fields {
            let key = kv.key();
            let field = if self.fields.contains_key(key) {
                self.fields.get_mut(key).unwrap()
            } else {
                let mut dir = session_dir.to_path_buf();
                dir.push("fields");
                dir.push(key);
                std::fs::create_dir_all(&dir).unwrap();
                if dir.exists() {
                    let field = Field::new(dir);
                    self.fields.entry(String::from(key)).or_insert(field);
                }
                self.fields.get_mut(key).unwrap()
            };
            field.update(session_row, kv.value());
        }
    }

    pub fn incidentally_depend(
        &mut self,
        pend_session_row: u32,
        relation_key: &str,
        depend_session_row: u32,
    ) {
        let row = *self.row.value(depend_session_row).unwrap();
        let depend = CollectionRow::new(
            *self.collection_id.value(depend_session_row).unwrap(),
            if row == 0 { depend_session_row } else { row },
        );
        self.relation.insert(relation_key, pend_session_row, depend);
    }

    pub(crate) fn init_temporary_data(&self) -> TemporaryData {
        let mut temporary_data = HashMap::new();
        let current = self.sequence_number.current();
        if current > 0 {
            let mut fields_overlaps: HashMap<CollectionRow, HashMap<String, Vec<u8>>> =
                HashMap::new();
            for sequence in 1..=current {
                for session_row in self.sequence.iter_by(|v| v.cmp(&sequence)).map(|x| x.row()) {
                    if let Some(collection_id) = self.collection_id.value(session_row) {
                        let collection_id = *collection_id;

                        let in_session = collection_id < 0;
                        let main_collection_id = if in_session {
                            -collection_id
                        } else {
                            collection_id
                        };
                        let temporary_collection = temporary_data
                            .entry(main_collection_id)
                            .or_insert(HashMap::new());
                        let row = *self.row.value(session_row).unwrap();

                        let temporary_row = if row == 0 {
                            -(session_row as i64)
                        } else if in_session {
                            -(row as i64)
                        } else {
                            row as i64
                        };

                        let operation = self.operation.value(session_row).unwrap().clone();
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
                                    CollectionRow::new(collection_id, row)
                                })
                                .or_insert(HashMap::new());
                            for (key, val) in &self.fields {
                                if let Some(v) = val.bytes(session_row) {
                                    row_fields.insert(key.to_string(), v.to_vec());
                                }
                            }
                            temporary_collection.insert(
                                temporary_row,
                                TemporaryDataEntity {
                                    activity: if *self.activity.value(session_row).unwrap() == 1 {
                                        Activity::Active
                                    } else {
                                        Activity::Inactive
                                    },
                                    term_begin: *self.term_begin.value(session_row).unwrap(),
                                    term_end: *self.term_end.value(session_row).unwrap(),
                                    uuid: if let Some(uuid) = self.uuid.value(session_row) {
                                        *uuid
                                    } else {
                                        0
                                    },
                                    operation,
                                    fields: row_fields.clone(),
                                    depends: {
                                        self.relation
                                            .rows
                                            .session_row
                                            .iter_by(|v| v.cmp(&session_row))
                                            .map(|x| x.row())
                                            .filter_map(|relation_row| {
                                                if let (Some(key), Some(depend)) = (
                                                    self.relation.rows.key.value(relation_row),
                                                    self.relation.rows.depend.value(relation_row),
                                                ) {
                                                    let key_name = unsafe {
                                                        std::str::from_utf8_unchecked(
                                                            self.relation.key_names.bytes(*key),
                                                        )
                                                    };
                                                    Some(Depend::new(key_name, depend.clone()))
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
