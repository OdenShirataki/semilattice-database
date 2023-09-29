use std::{
    cmp::Ordering,
    num::{NonZeroI64, NonZeroU32},
};

use hashbrown::HashMap;

use super::TemporaryDataEntity;
use crate::{idx_binary, Collection, Order, OrderKey};

#[inline(always)]
fn serial(collection: &Collection, a: NonZeroI64, b: NonZeroI64) -> (u32, u32) {
    let a = if a.get() < 0 {
        0
    } else {
        collection.serial(a.try_into().unwrap())
    };
    let b = if b.get() < 0 {
        0
    } else {
        collection.serial(b.try_into().unwrap())
    };
    (a, b)
}

#[inline(always)]
fn term_begin(
    temporary_collection: &HashMap<NonZeroI64, TemporaryDataEntity>,
    collection: &Collection,
    a: NonZeroI64,
    b: NonZeroI64,
) -> (u64, u64) {
    let a = if a.get() < 0 {
        temporary_collection.get(&a).unwrap().term_begin()
    } else {
        collection.term_begin(a.try_into().unwrap()).unwrap_or(0)
    };
    let b = if b.get() < 0 {
        temporary_collection.get(&b).unwrap().term_begin()
    } else {
        collection.term_begin(b.try_into().unwrap()).unwrap_or(0)
    };
    (a, b)
}

#[inline(always)]
fn term_end(
    temporary_collection: &HashMap<NonZeroI64, TemporaryDataEntity>,
    collection: &Collection,
    a: NonZeroI64,
    b: NonZeroI64,
) -> (u64, u64) {
    let a = if a.get() < 0 {
        temporary_collection.get(&a).unwrap().term_end()
    } else {
        collection.term_end(a.try_into().unwrap()).unwrap_or(0)
    };
    let b = if b.get() < 0 {
        temporary_collection.get(&b).unwrap().term_end()
    } else {
        collection.term_end(b.try_into().unwrap()).unwrap_or(0)
    };
    (a, b)
}

#[inline(always)]
fn last_updated(collection: &Collection, a: NonZeroI64, b: NonZeroI64) -> (u64, u64) {
    let a = if a.get() < 0 {
        0
    } else {
        collection.last_updated(a.try_into().unwrap()).unwrap_or(0)
    };
    let b = if b.get() < 0 {
        0
    } else {
        collection.last_updated(b.try_into().unwrap()).unwrap_or(0)
    };
    (a, b)
}

#[inline(always)]
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

//TODO : Supports session data for OrderKey::Custom
#[inline(always)]
pub fn sort(
    rows: &mut Vec<NonZeroI64>,
    orders: &Vec<Order>,
    collection: &Collection,
    temporary_collection: &HashMap<NonZeroI64, TemporaryDataEntity>,
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
                    OrderKey::Custom(custom_order) => {
                        if a.get() > 0 && b.get() > 0 {
                            let ord = custom_order.compare(
                                unsafe { NonZeroU32::new_unchecked(a.get() as u32) },
                                unsafe { NonZeroU32::new_unchecked(b.get() as u32) },
                            );
                            if ord != Ordering::Equal {
                                return ord;
                            }
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
                    OrderKey::Custom(custom_order) => {
                        if a.get() > 0 && b.get() > 0 {
                            let ord = custom_order.compare(
                                unsafe { NonZeroU32::new_unchecked(b.get() as u32) },
                                unsafe { NonZeroU32::new_unchecked(b.get() as u32) },
                            );
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                    }
                },
            }
        }
        Ordering::Equal
    });
}
