use std::{cmp::Ordering, collections::HashMap};

use super::TemporaryDataEntity;
use crate::{idx_binary, Collection, Order, OrderKey};

fn serial(collection: &Collection, a: i64, b: i64) -> (u32, u32) {
    let a = if a < 0 {
        0
    } else {
        collection.serial(a as u32)
    };
    let b = if b < 0 {
        0
    } else {
        collection.serial(b as u32)
    };
    (a, b)
}
fn term_begin(
    temporary_collection: &HashMap<i64, TemporaryDataEntity>,
    collection: &Collection,
    a: i64,
    b: i64,
) -> (u64, u64) {
    let a = if a < 0 {
        temporary_collection.get(&a).unwrap().term_begin()
    } else {
        collection.term_begin(a as u32).unwrap_or(0)
    };
    let b = if b < 0 {
        temporary_collection.get(&b).unwrap().term_begin()
    } else {
        collection.term_begin(b as u32).unwrap_or(0)
    };
    (a, b)
}
fn term_end(
    temporary_collection: &HashMap<i64, TemporaryDataEntity>,
    collection: &Collection,
    a: i64,
    b: i64,
) -> (u64, u64) {
    let a = if a < 0 {
        temporary_collection.get(&a).unwrap().term_end()
    } else {
        collection.term_end(a as u32).unwrap_or(0)
    };
    let b = if b < 0 {
        temporary_collection.get(&b).unwrap().term_end()
    } else {
        collection.term_end(b as u32).unwrap_or(0)
    };
    (a, b)
}
fn last_updated(collection: &Collection, a: i64, b: i64) -> (u64, u64) {
    let a = if a < 0 {
        0
    } else {
        collection.last_updated(a as u32).unwrap_or(0)
    };
    let b = if b < 0 {
        0
    } else {
        collection.last_updated(b as u32).unwrap_or(0)
    };
    (a, b)
}
fn field<'a>(
    temporary_collection: &'a HashMap<i64, TemporaryDataEntity>,
    collection: &'a Collection,
    row: i64,
    field_name: &str,
) -> &'a [u8] {
    if row < 0 {
        if let Some(v) = temporary_collection
            .get(&row)
            .unwrap()
            .fields()
            .get(field_name)
        {
            v
        } else {
            b""
        }
    } else {
        collection.field_bytes(row as u32, field_name)
    }
}

pub fn sort(
    rows: &mut Vec<i64>,
    orders: Vec<Order>,
    collection: &Collection,
    temporary_collection: &HashMap<i64, TemporaryDataEntity>,
) {
    rows.sort_by(|a, b| {
        for i in 0..orders.len() {
            match &orders[i] {
                Order::Asc(order_key) => match order_key {
                    OrderKey::Serial => {
                        let (a, b) = serial(collection, *a, *b);
                        let ord = a.cmp(&b);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    OrderKey::Row => return a.cmp(b),
                    OrderKey::TermBegin => {
                        let (a, b) = term_begin(temporary_collection, collection, *a, *b);
                        let ord = a.cmp(&b);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    OrderKey::TermEnd => {
                        let (a, b) = term_end(temporary_collection, collection, *a, *b);
                        let ord = a.cmp(&b);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    OrderKey::LastUpdated => {
                        let (a, b) = last_updated(collection, *a, *b);
                        let ord = a.cmp(&b);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    OrderKey::Field(field_name) => {
                        let ord = idx_binary::compare(
                            field(temporary_collection, collection, *a, &field_name),
                            field(temporary_collection, collection, *b, &field_name),
                        );
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                },
                Order::Desc(order_key) => match order_key {
                    OrderKey::Serial => {
                        let (a, b) = serial(collection, *a, *b);
                        let ord = b.cmp(&a);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    OrderKey::Row => {
                        return b.cmp(a);
                    }
                    OrderKey::TermBegin => {
                        let (a, b) = term_begin(temporary_collection, collection, *a, *b);
                        let ord = b.cmp(&a);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    OrderKey::TermEnd => {
                        let (a, b) = term_end(temporary_collection, collection, *a, *b);
                        let ord = b.cmp(&a);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    OrderKey::LastUpdated => {
                        let (a, b) = last_updated(collection, *a, *b);
                        let ord = b.cmp(&a);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    OrderKey::Field(field_name) => {
                        let ord = idx_binary::compare(
                            field(temporary_collection, collection, *b, &field_name),
                            field(temporary_collection, collection, *a, &field_name),
                        );
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
