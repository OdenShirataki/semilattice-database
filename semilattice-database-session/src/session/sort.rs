use std::{cmp::Ordering, fmt::Debug, num::NonZeroI64};

use hashbrown::HashMap;

use super::TemporaryDataEntity;
use crate::{idx_binary, Collection, Session};

pub trait SessionCustomOrder {
    fn compare(&self, a: NonZeroI64, b: NonZeroI64) -> Ordering;
    fn asc(&self) -> Vec<NonZeroI64>;
    fn desc(&self) -> Vec<NonZeroI64>;
}
impl Debug for dyn SessionCustomOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SessionCustomOrder")?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum SessionOrderKey<C: SessionCustomOrder> {
    Serial,
    Row,
    TermBegin,
    TermEnd,
    LastUpdated,
    Field(String),
    Custom(C),
}

#[derive(Debug)]
pub enum SessionOrder<C: SessionCustomOrder> {
    Asc(SessionOrderKey<C>),
    Desc(SessionOrderKey<C>),
}

impl Session {
    pub fn sort<C: SessionCustomOrder>(
        &self,
        collection: &Collection,
        mut rows: Vec<NonZeroI64>,
        orders: &Vec<SessionOrder<C>>,
    ) -> Vec<NonZeroI64> {
        if orders.len() > 0 {
            let collection_id = collection.id();
            if let Some(tmp) = self.temporary_data.get(&collection_id) {
                rows.sort_by(|a, b| {
                    for i in 0..orders.len() {
                        match &orders[i] {
                            SessionOrder::Asc(order_key) => match order_key {
                                SessionOrderKey::Serial => {
                                    let (a, b) = serial(collection, *a, *b);
                                    let ord = a.cmp(&b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Row => return a.cmp(b),
                                SessionOrderKey::TermBegin => {
                                    let (a, b) = term_begin(tmp, collection, *a, *b);
                                    let ord = a.cmp(&b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::TermEnd => {
                                    let (a, b) = term_end(tmp, collection, *a, *b);
                                    let ord = a.cmp(&b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::LastUpdated => {
                                    let (a, b) = last_updated(collection, *a, *b);
                                    let ord = a.cmp(&b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Field(field_name) => {
                                    let ord = idx_binary::compare(
                                        field(tmp, collection, *a, &field_name),
                                        field(tmp, collection, *b, &field_name),
                                    );
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Custom(custom_order) => {
                                    if a.get() > 0 && b.get() > 0 {
                                        let ord = custom_order.compare(*a, *b);
                                        if ord != Ordering::Equal {
                                            return ord;
                                        }
                                    }
                                }
                            },
                            SessionOrder::Desc(order_key) => match order_key {
                                SessionOrderKey::Serial => {
                                    let (a, b) = serial(collection, *a, *b);
                                    let ord = b.cmp(&a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Row => {
                                    return b.cmp(a);
                                }
                                SessionOrderKey::TermBegin => {
                                    let (a, b) = term_begin(tmp, collection, *a, *b);
                                    let ord = b.cmp(&a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::TermEnd => {
                                    let (a, b) = term_end(tmp, collection, *a, *b);
                                    let ord = b.cmp(&a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::LastUpdated => {
                                    let (a, b) = last_updated(collection, *a, *b);
                                    let ord = b.cmp(&a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Field(field_name) => {
                                    let ord = idx_binary::compare(
                                        field(tmp, collection, *b, &field_name),
                                        field(tmp, collection, *a, &field_name),
                                    );
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Custom(custom_order) => {
                                    let ord = custom_order.compare(*b, *a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                            },
                        }
                    }
                    Ordering::Equal
                });
            }
        }
        rows
    }
}

fn serial(collection: &Collection, a: NonZeroI64, b: NonZeroI64) -> (u32, u32) {
    (
        if a.get() < 0 {
            0
        } else {
            collection.serial(a.try_into().unwrap())
        },
        if b.get() < 0 {
            0
        } else {
            collection.serial(b.try_into().unwrap())
        },
    )
}

fn term_begin(
    temporary_collection: &HashMap<NonZeroI64, TemporaryDataEntity>,
    collection: &Collection,
    a: NonZeroI64,
    b: NonZeroI64,
) -> (u64, u64) {
    (
        if a.get() < 0 {
            temporary_collection.get(&a).unwrap().term_begin()
        } else {
            collection.term_begin(a.try_into().unwrap()).unwrap_or(0)
        },
        if b.get() < 0 {
            temporary_collection.get(&b).unwrap().term_begin()
        } else {
            collection.term_begin(b.try_into().unwrap()).unwrap_or(0)
        },
    )
}

fn term_end(
    temporary_collection: &HashMap<NonZeroI64, TemporaryDataEntity>,
    collection: &Collection,
    a: NonZeroI64,
    b: NonZeroI64,
) -> (u64, u64) {
    (
        if a.get() < 0 {
            temporary_collection.get(&a).unwrap().term_end()
        } else {
            collection.term_end(a.try_into().unwrap()).unwrap_or(0)
        },
        if b.get() < 0 {
            temporary_collection.get(&b).unwrap().term_end()
        } else {
            collection.term_end(b.try_into().unwrap()).unwrap_or(0)
        },
    )
}

fn last_updated(collection: &Collection, a: NonZeroI64, b: NonZeroI64) -> (u64, u64) {
    (
        if a.get() < 0 {
            0
        } else {
            collection.last_updated(a.try_into().unwrap()).unwrap_or(0)
        },
        if b.get() < 0 {
            0
        } else {
            collection.last_updated(b.try_into().unwrap()).unwrap_or(0)
        },
    )
}

fn field<'a>(
    temporary_collection: &'a HashMap<NonZeroI64, TemporaryDataEntity>,
    collection: &'a Collection,
    row: NonZeroI64,
    field_name: &str,
) -> &'a [u8] {
    if row.get() < 0 {
        temporary_collection
            .get(&row)
            .unwrap()
            .fields()
            .get(field_name)
            .map_or(b"", |v| v)
    } else {
        collection.field_bytes(row.try_into().unwrap(), field_name)
    }
}
