use std::num::{NonZeroI32, NonZeroI64};

use hashbrown::HashMap;
use semilattice_database::{Activity, Depend, FieldName};

use super::SessionOperation;

#[derive(Debug)]
pub struct TemporaryDataEntity {
    pub(crate) activity: Activity,
    pub(crate) term_begin: u64,
    pub(crate) term_end: u64,
    pub(crate) uuid: u128,
    pub(crate) operation: SessionOperation,
    pub(crate) fields: HashMap<FieldName, Vec<u8>>,
    pub(crate) depends: Vec<Depend>,
}
impl TemporaryDataEntity {
    pub fn activity(&self) -> Activity {
        self.activity
    }

    pub fn term_begin(&self) -> u64 {
        self.term_begin
    }

    pub fn term_end(&self) -> u64 {
        self.term_end
    }

    pub fn uuid(&self) -> u128 {
        self.uuid
    }

    pub fn uuid_string(&self) -> String {
        semilattice_database::uuid_string(self.uuid)
    }

    pub fn fields(&self) -> &HashMap<FieldName, Vec<u8>> {
        &self.fields
    }

    pub fn depends(&self) -> &Vec<Depend> {
        &self.depends
    }
}
pub type TemporaryData = HashMap<NonZeroI32, HashMap<NonZeroI64, TemporaryDataEntity>>;
