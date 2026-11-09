use std::{cmp::Ordering, fmt::Debug, num::NonZeroI64};

use semilattice_database::{
    FieldName,
    idx_binary::{AvltrieeSearch, IdxBinary},
};

use super::TemporaryDataRow;
use crate::{Collection, Session};

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

pub enum SessionOrderKey<C: SessionCustomOrder> {
    Serial,
    Row,
    TermBegin,
    TermEnd,
    LastUpdated,
    Field(FieldName),
    Custom(C),
    CustomCmp(SessionCustomCmp),
}

/// Compares two rows. Used by [SessionOrderKey::CustomCmp].
pub type SessionCustomCmp =
    Box<dyn Fn(&TemporaryDataRow, &Collection, &NonZeroI64, &NonZeroI64) -> Ordering>;

pub enum SessionOrder<C: SessionCustomOrder> {
    Asc(SessionOrderKey<C>),
    Desc(SessionOrderKey<C>),
}

impl Session {
    pub fn sort<C: SessionCustomOrder>(
        &self,
        collection: &Collection,
        mut rows: Vec<NonZeroI64>,
        orders: &[SessionOrder<C>],
    ) -> Vec<NonZeroI64> {
        if !orders.is_empty() {
            let collection_id = collection.id();
            if let Some(tmp) = self.temporary_data.get(&collection_id) {
                rows.sort_by(|a, b| {
                    for order in orders {
                        match order {
                            SessionOrder::Asc(order_key) => match order_key {
                                SessionOrderKey::Serial => {
                                    let a = Session::serial(collection, *a);
                                    let b = Session::serial(collection, *b);
                                    let ord = a.cmp(&b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Row => return a.cmp(b),
                                SessionOrderKey::TermBegin => {
                                    let a = Session::term_begin(tmp, collection, *a);
                                    let b = Session::term_begin(tmp, collection, *b);
                                    let ord = a.cmp(&b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::TermEnd => {
                                    let a = Session::term_end(tmp, collection, *a);
                                    let b = Session::term_end(tmp, collection, *b);
                                    let ord = a.cmp(&b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::LastUpdated => {
                                    let a = Session::last_updated(collection, *a);
                                    let b = Session::last_updated(collection, *b);
                                    let ord = a.cmp(&b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Field(field_name) => {
                                    let ord = IdxBinary::cmp(
                                        Session::field(tmp, collection, *a, field_name),
                                        Session::field(tmp, collection, *b, field_name),
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
                                SessionOrderKey::CustomCmp(f) => {
                                    let ord = f(tmp, collection, a, b);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                            },
                            SessionOrder::Desc(order_key) => match order_key {
                                SessionOrderKey::Serial => {
                                    let a = Session::serial(collection, *a);
                                    let b = Session::serial(collection, *b);
                                    let ord = b.cmp(&a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Row => {
                                    return b.cmp(a);
                                }
                                SessionOrderKey::TermBegin => {
                                    let a = Session::term_begin(tmp, collection, *a);
                                    let b = Session::term_begin(tmp, collection, *b);
                                    let ord = b.cmp(&a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::TermEnd => {
                                    let a = Session::term_end(tmp, collection, *a);
                                    let b = Session::term_end(tmp, collection, *b);
                                    let ord = b.cmp(&a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::LastUpdated => {
                                    let a = Session::last_updated(collection, *a);
                                    let b = Session::last_updated(collection, *b);
                                    let ord = b.cmp(&a);
                                    if ord != Ordering::Equal {
                                        return ord;
                                    }
                                }
                                SessionOrderKey::Field(field_name) => {
                                    let ord = IdxBinary::cmp(
                                        Session::field(tmp, collection, *b, field_name),
                                        Session::field(tmp, collection, *a, field_name),
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
                                SessionOrderKey::CustomCmp(f) => {
                                    let ord = f(tmp, collection, b, a);
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
