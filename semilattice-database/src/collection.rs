mod row;

pub use row::CollectionRow;

use std::ops::{Deref, DerefMut};

use versatile_data::Data;

pub struct Collection {
    pub(crate) data: Data,
    id: i32,
    name: String,
}
impl Collection {
    pub fn new(data: Data, id: i32, name: impl Into<String>) -> Self {
        Self {
            data,
            id,
            name: name.into(),
        }
    }
    pub fn id(&self) -> i32 {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}
impl Deref for Collection {
    type Target = Data;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl DerefMut for Collection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
