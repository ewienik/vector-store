/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::DbEmbedding;
use crate::IndexName;
use crate::LocalIndexKey;
use crate::PrimaryKey;
use crate::Restriction;
use crate::Timestamp;
use crate::Vector;
use anyhow::anyhow;
use anyhow::bail;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlDate;
use scylla::value::CqlTime;
use scylla::value::CqlTimestamp;
use scylla::value::CqlTimeuuid;
use scylla::value::CqlValue;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::btree_map::Entry;
use std::mem;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tracing::warn;
use uuid::Uuid;

trait Idx {
    fn idx(&self) -> usize;
}

mod row_id {
    use super::*;

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        derive_more::AsRef,
        derive_more::From,
        derive_more::Into,
    )]
    pub struct RowId(u64);

    const _: () = assert!(
        mem::size_of::<RowId>() == mem::size_of::<usize>(),
        "RowId should be the same size as usize"
    );

    impl RowId {
        const EPOCH_SHIFT: usize = (mem::size_of::<u64>() - mem::size_of::<Epoch>()) * 8;
        const MAX: u64 = !((Epoch::MAX as u64) << Self::EPOCH_SHIFT);

        pub(super) fn try_new(idx: usize, epoch: Epoch) -> anyhow::Result<Self> {
            if idx as u64 > Self::MAX {
                bail!("RowId is too large: {idx}");
            }
            Ok(Self(
                (*epoch.as_ref() as u64) << Self::EPOCH_SHIFT | idx as u64,
            ))
        }

        pub(super) fn new_epoch(mut self, epoch: Epoch) -> Self {
            self.0 &= Self::MAX;
            self.0 |= (epoch.0 as u64) << Self::EPOCH_SHIFT;
            self
        }

        pub(super) fn next_epoch(self) -> Self {
            self.new_epoch(self.epoch().next())
        }

        pub(super) fn epoch(&self) -> Epoch {
            Epoch((self.0 >> Self::EPOCH_SHIFT) as u16)
        }
    }

    impl Idx for RowId {
        fn idx(&self) -> usize {
            (self.0 & Self::MAX) as usize
        }
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::AsRef)]
    pub(super) struct Epoch(u16);

    impl Epoch {
        const MIN: u16 = 0;
        const MAX: u16 = u16::MAX;

        pub(super) fn new() -> Self {
            Self(Self::MIN)
        }

        pub(super) fn next(self) -> Self {
            if self.0 == Self::MAX {
                Self(Self::MIN)
            } else {
                Self(self.0 + 1)
            }
        }
    }
}
use row_id::Epoch;
pub use row_id::RowId;

mod partition_id {
    use super::*;
    use std::sync::atomic::AtomicU16;
    use std::sync::atomic::Ordering;

    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::From)]
    pub(crate) struct PartitionId(u64);

    const _: () = assert!(
        mem::size_of::<PartitionId>() == mem::size_of::<usize>(),
        "PartitionId should be the same size as usize"
    );

    impl PartitionId {
        const INDEX_ID_SHIFT: usize = (mem::size_of::<u64>() - mem::size_of::<ViewId>()) * 8;
        const MAX: u64 = !((ViewId::MAX as u64) << Self::INDEX_ID_SHIFT);

        pub(super) fn try_new(idx: usize, view_id: ViewId) -> anyhow::Result<Self> {
            if idx as u64 > Self::MAX {
                bail!("PartitionId is too large: {idx}");
            }
            Ok(Self(
                (*view_id.as_ref() as u64) << Self::INDEX_ID_SHIFT | idx as u64,
            ))
        }

        pub(super) fn view_id(&self) -> ViewId {
            ViewId((self.0 >> Self::INDEX_ID_SHIFT) as u16)
        }
    }

    impl Idx for PartitionId {
        fn idx(&self) -> usize {
            self.0 as usize
        }
    }

    #[derive(
        Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::AsRef, derive_more::From,
    )]
    pub(super) struct ViewId(u16);

    impl ViewId {
        pub(super) const GLOBAL: ViewId = ViewId(0);
        const MAX: u16 = u16::MAX;
    }

    pub(super) struct ViewIdGenerator {
        next: u16,
    }

    impl ViewIdGenerator {
        pub(super) fn new() -> Self {
            Self {
                next: ViewId::GLOBAL.0 + 1,
            }
        }

        pub(super) fn next(&mut self) -> anyhow::Result<ViewId> {
            if self.next > ViewId::MAX {
                bail!("No more ViewIds available");
            }
            let view_id = ViewId(self.next);
            self.next += 1;
            Ok(view_id)
        }
    }
}
use partition_id::PartitionId;
use partition_id::ViewId;
use partition_id::ViewIdGenerator;

#[derive(Clone, Copy, Debug, derive_more::From, derive_more::Into, derive_more::AsRef)]
struct PartitionSize(usize);

mod column_vec {
    use super::*;

    pub(super) struct ColumnVec<I, T> {
        vec: Vec<T>,
        _index: std::marker::PhantomData<I>,
    }

    impl<I: Idx, T> ColumnVec<I, T> {
        pub(super) fn new() -> Self {
            Self {
                vec: Vec::new(),
                _index: std::marker::PhantomData,
            }
        }

        pub(super) fn resize_with(&mut self, size: usize, f: impl FnMut() -> T) {
            self.vec.resize_with(size, f);
        }

        pub(super) fn get(&self, idx: I) -> Option<&T> {
            self.vec.get(idx.idx())
        }

        pub(super) fn get_mut(&mut self, idx: I) -> Option<&mut T> {
            self.vec.get_mut(idx.idx())
        }

        pub(super) fn update(&mut self, idx: I, value: T) -> anyhow::Result<()> {
            *self
                .get_mut(idx)
                .ok_or_else(|| anyhow!("Index out of ColumnVec bounds"))? = value;
            Ok(())
        }
    }

    impl<T> ColumnVec<RowId, ColumnValue<T>> {
        pub(super) fn update_epoch_timestamp(
            &mut self,
            row_id: RowId,
            timestamp: Timestamp,
        ) -> anyhow::Result<()> {
            self.get_mut(row_id)
                .map(|value| {
                    value.update_epoch_timestamp(row_id.epoch(), timestamp);
                })
                .ok_or_else(|| anyhow!("Index out of ColumnVec bounds"))
        }
    }
}
use column_vec::ColumnVec;

enum ColumnValue<T> {
    None(Epoch, Timestamp),
    Some(Epoch, Timestamp, T),
}

const _: () = assert!(
    mem::size_of::<ColumnValue<()>>() <= 2 * mem::size_of::<u64>(),
    "The impact of Epoch, Timestamp and enum in ColumnValue shouldn't be larger that 2 * u64"
);

impl<T> ColumnValue<T> {
    fn update_epoch_timestamp(&mut self, epoch: Epoch, timestamp: Timestamp) {
        match self {
            Self::None(value_epoch, value_timestamp)
            | Self::Some(value_epoch, value_timestamp, _) => {
                *value_epoch = epoch;
                *value_timestamp = timestamp;
            }
        }
    }

    fn get(&self) -> Option<&T> {
        match self {
            Self::None(_, _) => None,
            Self::Some(_, _, value) => Some(value),
        }
    }
}

enum Column {
    Ascii(ColumnVec<RowId, ColumnValue<String>>),
    BigInt(ColumnVec<RowId, ColumnValue<i64>>),
    Blob(ColumnVec<RowId, ColumnValue<Vec<u8>>>),
    Boolean(ColumnVec<RowId, ColumnValue<bool>>),
    Date(ColumnVec<RowId, ColumnValue<CqlDate>>),
    Double(ColumnVec<RowId, ColumnValue<f64>>),
    Float(ColumnVec<RowId, ColumnValue<f32>>),
    Inet(ColumnVec<RowId, ColumnValue<IpAddr>>),
    Int(ColumnVec<RowId, ColumnValue<i32>>),
    SmallInt(ColumnVec<RowId, ColumnValue<i16>>),
    Text(ColumnVec<RowId, ColumnValue<String>>),
    Time(ColumnVec<RowId, ColumnValue<CqlTime>>),
    Timestamp(ColumnVec<RowId, ColumnValue<CqlTimestamp>>),
    Timeuuid(ColumnVec<RowId, ColumnValue<CqlTimeuuid>>),
    TinyInt(ColumnVec<RowId, ColumnValue<i8>>),
    Uuid(ColumnVec<RowId, ColumnValue<Uuid>>),
}

impl Column {
    fn new(native_type: &NativeType) -> anyhow::Result<Self> {
        Ok(match native_type {
            NativeType::Ascii => Self::Ascii(ColumnVec::new()),
            NativeType::BigInt => Self::BigInt(ColumnVec::new()),
            NativeType::Blob => Self::Blob(ColumnVec::new()),
            NativeType::Boolean => Self::Boolean(ColumnVec::new()),
            NativeType::Date => Self::Date(ColumnVec::new()),
            NativeType::Double => Self::Double(ColumnVec::new()),
            NativeType::Float => Self::Float(ColumnVec::new()),
            NativeType::Inet => Self::Inet(ColumnVec::new()),
            NativeType::Int => Self::Int(ColumnVec::new()),
            NativeType::SmallInt => Self::SmallInt(ColumnVec::new()),
            NativeType::Text => Self::Text(ColumnVec::new()),
            NativeType::Time => Self::Time(ColumnVec::new()),
            NativeType::Timestamp => Self::Timestamp(ColumnVec::new()),
            NativeType::Timeuuid => Self::Timeuuid(ColumnVec::new()),
            NativeType::TinyInt => Self::TinyInt(ColumnVec::new()),
            NativeType::Uuid => Self::Uuid(ColumnVec::new()),
            _ => bail!("Unsupported native type: {native_type:?}"),
        })
    }

    fn resize_with(&mut self, size: usize) {
        let epoch = Epoch::new();
        let timestamp = Timestamp::UNIX_EPOCH;
        match self {
            Self::Ascii(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::BigInt(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Blob(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Boolean(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Date(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Double(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Float(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Inet(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Int(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::SmallInt(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Text(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Time(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Timestamp(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Timeuuid(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::TinyInt(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
            Self::Uuid(vec) => vec.resize_with(size, || ColumnValue::None(epoch, timestamp)),
        }
    }

    fn insert_value(
        &mut self,
        row_id: RowId,
        timestamp: Timestamp,
        value: CqlValue,
    ) -> anyhow::Result<()> {
        let epoch = row_id.epoch();
        match self {
            Self::Ascii(vec) => {
                let CqlValue::Ascii(value) = value else {
                    bail!("Failed to convert value to Ascii");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::BigInt(vec) => {
                let CqlValue::BigInt(value) = value else {
                    bail!("Failed to convert value to BigInt");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Blob(vec) => {
                let CqlValue::Blob(value) = value else {
                    bail!("Failed to convert value to Blob");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Boolean(vec) => {
                let CqlValue::Boolean(value) = value else {
                    bail!("Failed to convert value to Boolean");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Date(vec) => {
                let CqlValue::Date(value) = value else {
                    bail!("Failed to convert value to Date");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Double(vec) => {
                let CqlValue::Double(value) = value else {
                    bail!("Failed to convert value to Double");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Float(vec) => {
                let CqlValue::Float(value) = value else {
                    bail!("Failed to convert value to Float");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Inet(vec) => {
                let CqlValue::Inet(value) = value else {
                    bail!("Failed to convert value to Inet");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Int(vec) => {
                let CqlValue::Int(value) = value else {
                    bail!("Failed to convert value to Int");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::SmallInt(vec) => {
                let CqlValue::SmallInt(value) = value else {
                    bail!("Failed to convert value to SmallInt");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Text(vec) => {
                let CqlValue::Text(value) = value else {
                    bail!("Failed to convert value to Text");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Time(vec) => {
                let CqlValue::Time(value) = value else {
                    bail!("Failed to convert value to Time");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Timestamp(vec) => {
                let CqlValue::Timestamp(value) = value else {
                    bail!("Failed to convert value to Timestamp");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Timeuuid(vec) => {
                let CqlValue::Timeuuid(value) = value else {
                    bail!("Failed to convert value to Timeuuid");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::TinyInt(vec) => {
                let CqlValue::TinyInt(value) = value else {
                    bail!("Failed to convert value to TinyInt");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
            Self::Uuid(vec) => {
                let CqlValue::Uuid(value) = value else {
                    bail!("Failed to convert value to Uuid");
                };
                vec.update(row_id, ColumnValue::Some(epoch, timestamp, value))
            }
        }
    }

    fn update_epoch_timestamp(
        &mut self,
        row_id: RowId,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        match self {
            Self::Ascii(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::BigInt(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Blob(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Boolean(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Date(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Double(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Float(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Inet(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Int(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::SmallInt(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Text(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Time(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Timestamp(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Timeuuid(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::TinyInt(vec) => vec.update_epoch_timestamp(row_id, timestamp),
            Self::Uuid(vec) => vec.update_epoch_timestamp(row_id, timestamp),
        }
    }

    fn get(&self, row_id: RowId) -> Option<CqlValue> {
        match self {
            Self::Ascii(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Ascii),
            Self::BigInt(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::BigInt),
            Self::Blob(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Blob),
            Self::Boolean(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Boolean),
            Self::Date(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Date),
            Self::Double(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Double),
            Self::Float(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Float),
            Self::Inet(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Inet),
            Self::Int(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Int),
            Self::SmallInt(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::SmallInt),
            Self::Text(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Text),
            Self::Time(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Time),
            Self::Timestamp(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Timestamp),
            Self::Timeuuid(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Timeuuid),
            Self::TinyInt(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::TinyInt),
            Self::Uuid(vec) => vec
                .get(row_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Uuid),
        }
    }
}

struct Columns {
    primary_key_columns: Arc<Vec<ColumnName>>,
    partition_key_columns: Option<Arc<Vec<ColumnName>>>,

    columns: BTreeMap<ColumnName, Column>,
}

impl Columns {
    fn new(
        primary_key_columns: Arc<Vec<ColumnName>>,
        partition_key_columns: Option<Arc<Vec<ColumnName>>>,
        table_columns: &HashMap<ColumnName, NativeType>,
    ) -> anyhow::Result<Self> {
        let columns = primary_key_columns
            .iter()
            .chain(
                partition_key_columns
                    .as_ref()
                    .map(|vec| vec.as_slice())
                    .unwrap_or(&[])
                    .iter(),
            )
            .map(|name| {
                table_columns
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("Column {name} not found in table columns"))
                    .and_then(Column::new)
                    .map(|column| (name.clone(), column))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            primary_key_columns,
            partition_key_columns,
            columns,
        })
    }

    fn resize_with(&mut self, size: usize) {
        self.columns
            .values_mut()
            .for_each(|column| column.resize_with(size));
    }

    fn update_primary_key_epoch_timestamp(
        &mut self,
        row_id: RowId,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        self.primary_key_columns.iter().try_for_each(|name| {
            self.columns
                .get_mut(name)
                .ok_or_else(|| anyhow::anyhow!("Primary key column should be in columns"))
                .and_then(|column| column.update_epoch_timestamp(row_id, timestamp))
        })
    }
}

fn compare_rows(
    lhs: &Column,
    lhs_row_id: RowId,
    rhs: &Column,
    rhs_row_id: RowId,
) -> Option<Ordering> {
    match (lhs, rhs) {
        (Column::Ascii(lhs), Column::Ascii(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::BigInt(lhs), Column::BigInt(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Blob(lhs), Column::Blob(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Boolean(lhs), Column::Boolean(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Date(lhs), Column::Date(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Double(lhs), Column::Double(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            lhs_value.partial_cmp(rhs_value)
        }
        (Column::Float(lhs), Column::Float(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            lhs_value.partial_cmp(rhs_value)
        }
        (Column::Inet(lhs), Column::Inet(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Int(lhs), Column::Int(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::SmallInt(lhs), Column::SmallInt(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Text(lhs), Column::Text(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Time(lhs), Column::Time(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Timestamp(lhs), Column::Timestamp(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Timeuuid(lhs), Column::Timeuuid(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::TinyInt(lhs), Column::TinyInt(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Uuid(lhs), Column::Uuid(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            let rhs_value = rhs.get(rhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        _ => None,
    }
}

fn compare_row_with_cqlvalue(lhs: &Column, lhs_row_id: RowId, rhs: &CqlValue) -> Option<Ordering> {
    match (lhs, rhs) {
        (Column::Ascii(lhs), CqlValue::Ascii(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::BigInt(lhs), CqlValue::BigInt(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Blob(lhs), CqlValue::Blob(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Boolean(lhs), CqlValue::Boolean(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Date(lhs), CqlValue::Date(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Double(lhs), CqlValue::Double(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            lhs_value.partial_cmp(rhs_value)
        }
        (Column::Float(lhs), CqlValue::Float(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            lhs_value.partial_cmp(rhs_value)
        }
        (Column::Inet(lhs), CqlValue::Inet(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Int(lhs), CqlValue::Int(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::SmallInt(lhs), CqlValue::SmallInt(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Text(lhs), CqlValue::Text(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Time(lhs), CqlValue::Time(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Timestamp(lhs), CqlValue::Timestamp(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Timeuuid(lhs), CqlValue::Timeuuid(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::TinyInt(lhs), CqlValue::TinyInt(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Uuid(lhs), CqlValue::Uuid(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id)?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        _ => None,
    }
}

/*
struct RowMapK {
    columns: Arc<RwLock<Columns>>,
    row_id: RowId,
}

impl PartialEq for RowMapK {
    fn eq(&self, other: &Self) -> bool {
        let columns = self.columns.read().unwrap();
        let other_columns = other.columns.read().unwrap();
        columns
            .primary_key_columns
            .iter()
            .map(|name| {
                columns
                    .columns
                    .get(name)
                    .expect("Primary key column {name} should be in columns")
            })
            .zip(other_columns.primary_key_columns.iter().map(|name| {
                columns
                    .columns
                    .get(name)
                    .expect("Primary key column {name} should be in columns")
            }))
            .all(|(lhs, rhs)| {
                compare_rows(lhs, self.row_id, rhs, other.row_id)
                    .expect("Comparing should be available")
                    == Ordering::Equal
            })
    }
}

impl Eq for RowMapK {}

impl PartialOrd for RowMapK {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let columns = self.columns.read().unwrap();
        let other_columns = other.columns.read().unwrap();
        columns
            .primary_key_columns
            .iter()
            .map(|name| columns.columns.get(name))
            .zip(
                other_columns
                    .primary_key_columns
                    .iter()
                    .map(|name| columns.columns.get(name)),
            )
            .map(|(lhs, rhs)| {
                lhs.and_then(|lhs| rhs.map(|rhs| (lhs, rhs)))
                    .and_then(|(lhs, rhs)| compare_rows(lhs, self.row_id, rhs, other.row_id))
            })
            .find(|ordering| ordering.is_none() || *ordering != Some(Ordering::Equal))
            .unwrap_or(Some(Ordering::Equal))
    }
}

impl Ord for RowMapK {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

struct PartitionMapK {
    columns: Arc<RwLock<Columns>>,
    row_id: RowId,
}

impl PartialEq for PartitionMapK {
    fn eq(&self, other: &Self) -> bool {
        let columns = self.columns.read().unwrap();
        let other_columns = other.columns.read().unwrap();
        columns
            .partition_key_columns
            .as_ref()
            .expect("Partition key columns should be defined for equality check")
            .iter()
            .map(|name| {
                columns
                    .columns
                    .get(name)
                    .expect("Partition key column {name} should be in columns")
            })
            .zip(
                other_columns
                    .partition_key_columns
                    .as_ref()
                    .expect("Partition key columns should be defined for equality check")
                    .iter()
                    .map(|name| {
                        columns
                            .columns
                            .get(name)
                            .expect("Partition key column {name} should be in columns")
                    }),
            )
            .all(|(lhs, rhs)| {
                compare_rows(lhs, self.row_id, rhs, other.row_id)
                    .expect("Values should be comparable")
                    == Ordering::Equal
            })
    }
}

impl Eq for PartitionMapK {}

impl PartialOrd for PartitionMapK {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let columns = self.columns.read().unwrap();
        let other_columns = other.columns.read().unwrap();
        columns
            .partition_key_columns
            .as_ref()
            .expect("Partition key columns should be defined for compare check")
            .iter()
            .map(|name| {
                columns
                    .columns
                    .get(name)
                    .expect("Primary key column {name} should be in columns")
            })
            .zip(
                other_columns
                    .partition_key_columns
                    .as_ref()
                    .expect("Partition key columns should be defined for compare check")
                    .iter()
                    .map(|name| {
                        columns
                            .columns
                            .get(name)
                            .expect("Primary key column {name} should be in columns")
                    }),
            )
            .map(|(lhs, rhs)| compare_rows(lhs, self.row_id, rhs, other.row_id))
            .find(|ordering| ordering.is_none() || *ordering != Some(Ordering::Equal))
            .unwrap_or(Some(Ordering::Equal))
    }
}

impl Ord for PartitionMapK {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
*/

struct FreeRowIds(VecDeque<RowId>);

impl FreeRowIds {
    fn peek_id(&self) -> anyhow::Result<RowId> {
        self.0
            .front()
            .copied()
            .ok_or(anyhow!("Vector ids should be reserved"))
    }

    fn drop_peeked_id(&mut self, peeked_row_id: RowId) {
        if let Some(row_id) = self.0.front()
            && *row_id == peeked_row_id
        {
            self.0.pop_front();
        }
    }
}

struct FreePartitionIds(VecDeque<PartitionId>);

impl FreePartitionIds {
    fn peek_id(&self) -> anyhow::Result<PartitionId> {
        self.0
            .front()
            .copied()
            .ok_or(anyhow!("Partition ids should be reserved"))
    }

    fn drop_peeked_id(&mut self, peeked_partition_id: PartitionId) {
        if let Some(partition_id) = self.0.front()
            && *partition_id == peeked_partition_id
        {
            self.0.pop_front();
        }
    }
}

/*
enum Partition {
    None,
    Some {
        /// A map from RowId::idx to PartitionId
        map: BTreeMap<PartitionMapK, PartitionId>,

        /// A queue for free PartitionIds
        free_ids: FreePartitionIds,

        /// A map from RowId to PartitionId
        ids: ColumnVec<RowId, PartitionId>,

        /// A map from PartitionId to the number of rows in the partition
        sizes: ColumnVec<PartitionId, usize>,
    },
}

impl Partition {
    const INCREMENT_SIZE: usize = 2 ^ 8;

    fn new(some: bool) -> Self {
        if some {
            Self::Some {
                map: BTreeMap::new(),
                free_ids: FreePartitionIds(VecDeque::new()),
                ids: ColumnVec::new(),
                sizes: ColumnVec::new(),
            }
        } else {
            Self::None
        }
    }

    fn reserve_ids(&mut self) -> anyhow::Result<()> {
        match self {
            Self::None => {}
            Self::Some {
                map,
                free_ids,
                sizes,
                ..
            } => {
                if !free_ids.0.is_empty() {
                    return Ok(());
                }
                let start = map.len() + 1; // Start from 1 to avoid using GLOBAL partition id
                let end = start + Self::INCREMENT_SIZE;
                free_ids.0.reserve(Self::INCREMENT_SIZE);
                (start..end)
                    .map(|id| PartitionId::from(id as u64))
                    .for_each(|id| {
                        free_ids.0.push_back(id);
                    });
                sizes.resize_with(end, || 0);
            }
        }
        Ok(())
    }

    fn resize_row_to_partition_map(&mut self, size: usize) {
        match self {
            Self::None => {}
            Self::Some { ids, .. } => {
                ids.resize_with(size, || PartitionId::GLOBAL);
            }
        }
    }

    fn add_row(
        &mut self,
        columns: &Arc<RwLock<Columns>>,
        row_id: RowId,
    ) -> anyhow::Result<PartitionId> {
        match self {
            Self::None => Ok(PartitionId::GLOBAL),
            Self::Some {
                map,
                free_ids,
                ids,
                sizes,
            } => {
                let row_partition_id = ids
                    .get_mut(row_id.idx())
                    .ok_or_else(|| anyhow!("RowId index out of partition ids bounds"))?;
                if *row_partition_id != PartitionId::GLOBAL {
                    warn!(
                        "RowId {row_id:?} already has a partition id {row_partition_id:?}, but it is being tried to be added to partition map again."
                    );
                    return Ok(*row_partition_id);
                }
                let partition_id = match map.entry(PartitionMapK {
                    columns: Arc::clone(columns),
                    row_id,
                }) {
                    Entry::Occupied(entry) => {
                        let partition_id = *entry.get();
                        *sizes.get_mut(partition_id).ok_or_else(|| {
                            anyhow!("PartitionId index out of partition sizes bounds")
                        })? += 1;
                        partition_id
                    }
                    Entry::Vacant(entry) => {
                        let partition_id = free_ids.peek_id()?;
                        *sizes.get_mut(partition_id).ok_or_else(|| {
                            anyhow!("PartitionId index out of partition sizes bounds")
                        })? = 1;
                        entry.insert(partition_id);
                        free_ids.drop_peeked_id(partition_id);
                        partition_id
                    }
                };
                *row_partition_id = partition_id;
                Ok(partition_id)
            }
        }
    }

    /// Removes a row from a partition and returns partition_id if partition was removed
    fn remove_row(&mut self, row_id: RowId) -> anyhow::Result<Option<PartitionId>> {
        match self {
            Self::None => Ok(None),
            Self::Some { ids, sizes, .. } => {
                let partition_id = ids
                    .get_mut(row_id.idx())
                    .ok_or_else(|| anyhow!("RowId index out of partition ids bounds"))?;
                if *partition_id == PartitionId::GLOBAL {
                    warn!(
                        "RowId {row_id:?} has GLOBAL partition id, but it is being tried to be removed from partition map."
                    );
                    return Ok(None);
                }
                let size = sizes
                    .get_mut(partition_id)
                    .ok_or_else(|| anyhow!("RowId index out of partition sizes bounds"))?;
                if *size == 0 {
                    warn!(
                        "PartitionId {partition_id:?} has size 0, but it is being tried to be removed."
                    );
                    return Ok(None);
                }
                *size -= 1;
                Ok((*size == 0).then_some(*partition_id))
            }
        }
    }

    fn partition_id(&self, row_id: RowId) -> anyhow::Result<PartitionId> {
        match self {
            Self::None => Ok(PartitionId::GLOBAL),
            Self::Some { ids, .. } => ids
                .get(row_id)
                .copied()
                .ok_or_else(|| anyhow!("RowId index out of partition ids bounds")),
        }
    }
}
*/

enum IndexData {
    Global {
        keys: ColumnVec<RowId, Option<Arc<PrimaryKey>>>,
    },
    Local {
        view_id: ViewId,

        /// A map from index key to RowId
        map: BTreeMap<LocalIndexKey, PartitionId>,
        free_ids: FreePartitionIds,
        keys: ColumnVec<PartitionId, Option<LocalIndexKey>>,
        ids: ColumnVec<RowId, Option<PartitionId>>,

        /// A size of each partition
        sizes: ColumnVec<PartitionId, usize>,
    },
}

impl IndexData {
    fn partition_id(&self, row_id: RowId) -> Option<PartitionId> {
        match self {
            Self::Global { .. } => PartitionId::try_new(0, ViewId::GLOBAL).ok(),
            Self::Local { ids, .. } => ids.get(row_id).copied().flatten(),
        }
    }

    /// Returns true if partition is empty
    fn remove_row(&mut self, row_id: RowId) -> bool {
        match self {
            Self::Global { .. } => false,
            Self::Local {
                ids, keys, sizes, ..
            } => {
                let Some(Some(partition_id)) = ids.get_mut(row_id).take() else {
                    return false;
                };

                keys.get_mut(*partition_id).take();
                sizes
                    .get_mut(*partition_id)
                    .map(|size| {
                        if *size > 0 {
                            *size -= 1;
                        }
                        *size == 0
                    })
                    .unwrap_or(false)
            }
        }
    }
}

struct Index {
    data: IndexData,

    /// Column names for which the index is built. The order of column names is important, as it
    /// defines the order of values in the index key.
    key_columns: Arc<Vec<ColumnName>>,

    /// Additional filtering columns used for this index.
    filtering_columns: Arc<Vec<ColumnName>>,

    /// All column names that are used in this index (key columns + filtering columns)
    available_columns: BTreeSet<ColumnName>,

    /// Timestamps of the last vector update
    vector_timestamps: ColumnVec<RowId, ColumnValue<()>>,
}

impl Index {
    const INCREMENT_SIZE: usize = 2 ^ 8;

    fn new_global(
        key_columns: Arc<Vec<ColumnName>>,
        filtering_columns: Arc<Vec<ColumnName>>,
    ) -> Self {
        Self {
            data: IndexData::Global {
                keys: ColumnVec::new(),
            },
            available_columns: key_columns
                .iter()
                .chain(filtering_columns.iter())
                .cloned()
                .collect(),
            key_columns,
            filtering_columns,
            vector_timestamps: ColumnVec::new(),
        }
    }

    fn new_partition(
        view_id: ViewId,
        key_columns: Arc<Vec<ColumnName>>,
        filtering_columns: Arc<Vec<ColumnName>>,
    ) -> Self {
        Self {
            data: IndexData::Local {
                view_id,
                map: BTreeMap::new(),
                free_ids: FreePartitionIds(VecDeque::new()),
                keys: ColumnVec::new(),
                ids: ColumnVec::new(),
                sizes: ColumnVec::new(),
            },
            available_columns: key_columns
                .iter()
                .chain(filtering_columns.iter())
                .cloned()
                .collect(),
            key_columns,
            filtering_columns,
            vector_timestamps: ColumnVec::new(),
        }
    }

    fn resize_row_ids_with(&mut self, new_size: usize) {
        match &mut self.data {
            IndexData::Global { keys } => keys.resize_with(new_size, || None),
            IndexData::Local { ids, .. } => ids.resize_with(new_size, || None),
        }
        self.vector_timestamps.resize_with(new_size, || {
            ColumnValue::None(Epoch::new(), Timestamp::UNIX_EPOCH)
        });
    }

    fn resize_partition_ids(&mut self) -> anyhow::Result<()> {
        let IndexData::Local {
            view_id,
            map,
            free_ids,
            keys,
            sizes,
            ..
        } = &mut self.data
        else {
            return Ok(());
        };
        if !free_ids.0.is_empty() {
            return Ok(());
        }
        let start = map.len();
        let end = start + Self::INCREMENT_SIZE;
        free_ids.0.reserve(Self::INCREMENT_SIZE);
        (start..end)
            .map(|id| PartitionId::try_new(id, *view_id))
            .try_for_each(|id| {
                id.map(|id| {
                    free_ids.0.push_back(id);
                })
            })?;
        keys.resize_with(end, || None);
        sizes.resize_with(end, || None);
        Ok(())
    }
}

pub struct Table {
    row_map: BTreeMap<Arc<PrimaryKey>, RowId>,
    free_row_ids: FreeRowIds,

    columns: BTreeMap<ColumnName, Column>,

    view_id_generator: ViewIdGenerator,
    view_ids: BTreeMap<IndexName, ViewId>,
    indexes: BTreeMap<ViewId, Index>,
}

impl Table {
    const ROWS_INCREMENT_SIZE: usize = 2 ^ 10;

    pub(crate) fn new(
        index_name: IndexName,
        primary_key_columns: Arc<Vec<ColumnName>>,
        partition_key_columns: Option<Arc<Vec<ColumnName>>>,
        filtering_columns: Arc<Vec<ColumnName>>,
        table_columns: Arc<HashMap<ColumnName, NativeType>>,
    ) -> anyhow::Result<Self> {
        let mut view_id_generator = ViewIdGenerator::new();
        let mut indexes = BTreeMap::new();
        let mut view_ids = BTreeMap::new();
        indexes.insert(
            ViewId::GLOBAL,
            Index::new_global(
                Arc::clone(&primary_key_columns),
                Arc::clone(&filtering_columns),
            ),
        );
        if let Some(partition_key_columns) = partition_key_columns.as_ref() {
            let view_id = view_id_generator.next()?;
            indexes.insert(
                view_id,
                Index::new_partition(
                    view_id,
                    Arc::clone(partition_key_columns),
                    Arc::clone(&filtering_columns),
                ),
            );
            view_ids.insert(index_name, view_id);
        } else {
            view_ids.insert(index_name, ViewId::GLOBAL);
        }
        let columns = primary_key_columns
            .iter()
            .chain(
                partition_key_columns
                    .as_ref()
                    .map(|vec| vec.as_slice())
                    .unwrap_or(&[])
                    .iter(),
            )
            .map(|name| {
                table_columns
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("Column {name} not found in table columns"))
                    .and_then(Column::new)
                    .map(|column| (name.clone(), column))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let mut table = Self {
            row_map: BTreeMap::new(),
            free_row_ids: FreeRowIds(VecDeque::new()),
            columns,
            view_id_generator,
            view_ids,
            indexes,
        };
        table.reserve_ids()?;
        Ok(table)
    }

    fn reserve_ids(&mut self) -> anyhow::Result<()> {
        if !self.free_row_ids.0.is_empty() {
            return Ok(());
        }
        let start = self.row_map.len();
        let end = start + Self::ROWS_INCREMENT_SIZE;
        self.free_row_ids.0.reserve(Self::ROWS_INCREMENT_SIZE);
        (start..end)
            .map(|idx| RowId::try_new(idx, Epoch::new()))
            .take_while(|id| id.is_ok())
            .for_each(|id| {
                self.free_row_ids.0.push_back(id.unwrap());
            });
        if self.free_row_ids.0.is_empty() {
            bail!("Failed to reserve vector ids: no more ids available");
        }
        let new_size = start + self.free_row_ids.0.len();

        self.columns
            .iter_mut()
            .for_each(|(_, column)| column.resize_with(new_size));
        self.indexes.iter_mut().try_for_each(|(_, index)| {
            index.resize_row_ids_with(new_size);
            index.resize_partition_ids()
        })?;
        Ok(())
    }

    /*
    fn insert_primary_key(
        &mut self,
        row_id: RowId,
        timestamp: Timestamp,
        primary_key: PrimaryKey,
    ) -> anyhow::Result<()> {
        {
            let mut columns = self.columns.write().unwrap();
            let primary_key_columns = Arc::clone(&columns.primary_key_columns);
            let columns = &mut columns.columns;
            for (name, value) in primary_key_columns.iter().zip(primary_key.0.into_iter()) {
                columns
                    .get_mut(name)
                    .ok_or_else(|| anyhow!("Column {name} not found in columns"))?
                    .insert_value(row_id, timestamp, value.clone())?;
            }
        }
        self.vector_timestamps
            .update(row_id, ColumnValue::Some(row_id.epoch(), timestamp, ()))
    }
    */
}

#[cfg_attr(test, mockall::automock)]
pub(crate) trait TableAdd {
    fn add(
        &mut self,
        name: &IndexName,
        db_embedding: DbEmbedding,
    ) -> anyhow::Result<Vec<Operation>>;
}

impl TableAdd for Table {
    fn add(
        &mut self,
        name: &IndexName,
        db_embedding: DbEmbedding,
    ) -> anyhow::Result<Vec<Operation>> {
        self.reserve_ids()?;

        let mut operations = vec![];

        //let row_id = self.free_row_ids.peek_id()?;
        //self.insert_primary_key(row_id, db_embedding.timestamp, db_embedding.primary_key)?;
        //let columns = &self.columns;
        //let partition = &mut self.partition;
        //let vector_timestamps = &mut self.vector_timestamps;

        let row_map = &mut self.row_map;

        match row_map.entry(Arc::new(db_embedding.primary_key)) {
            Entry::Occupied(mut entry) => {
                let row_id = *entry.get();
                self.indexes.iter_mut().try_for_each(|(view_id, index)| {
                    let (epoch, timestamp, vector_already_exists) = match index
                        .vector_timestamps
                        .get(row_id)
                    {
                        Some(ColumnValue::Some(epoch, timestamp, _)) => (*epoch, timestamp, true),
                        Some(ColumnValue::None(epoch, timestamp)) => (*epoch, timestamp, false),
                        None => {
                            bail!(
                                "Failed to update vector: \
                                missing vector timestamp for view {view_id:?} and row_id {row_id:?}"
                            );
                        }
                    };
                    if timestamp.0 >= db_embedding.timestamp.0 {
                        return Ok(());
                    }
                    let row_id = row_id.new_epoch(epoch);
                    let partition_id = index.data.partition_id(row_id).ok_or_else(|| {
                        anyhow!(
                            "Failed to update vector: \
                            missing partition id for view {view_id:?} and row_id {row_id:?}"
                        )
                    })?;
                    if let Some(vector) = db_embedding.embedding {
                        if vector_already_exists {
                            operations.push(Operation::RemoveBeforeAddVector {
                                row_id,
                                partition_id,
                            });
                        }

                        let row_id = row_id.next_epoch();
                        let timestamp = db_embedding.timestamp;
                        index
                            .vector_timestamps
                            .update(row_id, ColumnValue::Some(row_id.epoch(), timestamp, ()))?;
                        operations.push(Operation::AddVector {
                            row_id,
                            partition_id,
                            vector,
                        });
                    } else {
                        let epoch = row_id.epoch().next();
                        index
                            .vector_timestamps
                            .update(row_id, ColumnValue::None(epoch, *timestamp))?;
                        if vector_already_exists {
                            operations.push(Operation::RemoveVector {
                                row_id,
                                partition_id,
                            });
                            if index.data.remove_row(row_id) {
                                operations.push(Operation::RemovePartition { partition_id });
                            }
                        }
                    }
                    Ok(())
                })?;
                Ok(operations)
            }
            Entry::Vacant(entry) => {
                if let Some(vector) = db_embedding.embedding {
                    let row_id = entry.key().row_id;
                    let partition_id = partition.add_row(columns, row_id)?;
                    entry.insert(row_id.epoch());
                    operations[0] = Some(Operation::AddVector {
                        row_id,
                        partition_id,
                        vector,
                    });
                    self.free_row_ids.drop_peeked_id(row_id);
                } else {
                    warn!("Added row with no vector, skipping vector addition");
                }
                Ok(operations)
            }
        }
    }
}

#[cfg_attr(test, mockall::automock)]
pub(crate) trait TableSearch {
    fn primary_key<R: Into<RowId> + 'static>(&self, row_id: R) -> Option<PrimaryKey>;
    fn is_valid_for<R: Into<RowId> + 'static>(&self, row_id: R, restriction: &Restriction) -> bool;
}

impl TableSearch for Table {
    fn primary_key<R: Into<RowId>>(&self, row_id: R) -> Option<PrimaryKey> {
        let row_id = row_id.into();
        let columns = self.columns.read().unwrap();
        let primary_key = columns
            .primary_key_columns
            .iter()
            .map(|name| columns.columns.get(name)?.get(row_id))
            .collect::<Option<Vec<_>>>()?;
        Some(PrimaryKey(primary_key))
    }

    fn is_valid_for<R: Into<RowId> + 'static>(&self, row_id: R, restriction: &Restriction) -> bool {
        let row_id = row_id.into();
        let columns = &self.columns.read().unwrap().columns;
        match restriction {
            Restriction::Eq { lhs, rhs } => columns
                .get(lhs)
                .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs))
                .is_some_and(|ord| ord.is_eq()),

            Restriction::In { lhs, rhs } => columns
                .get(lhs)
                .map(|column| {
                    rhs.iter()
                        .filter_map(|rhs| compare_row_with_cqlvalue(column, row_id, rhs))
                        .any(|ord| ord.is_eq())
                })
                .is_some(),

            Restriction::Lt { lhs, rhs } => columns
                .get(lhs)
                .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs))
                .is_some_and(|ord| ord.is_lt()),

            Restriction::Lte { lhs, rhs } => columns
                .get(lhs)
                .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs))
                .is_some_and(|ord| ord.is_le()),

            Restriction::Gt { lhs, rhs } => columns
                .get(lhs)
                .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs))
                .is_some_and(|ord| ord.is_gt()),

            Restriction::Gte { lhs, rhs } => columns
                .get(lhs)
                .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs))
                .is_some_and(|ord| ord.is_ge()),

            Restriction::EqTuple { lhs, rhs } => lhs.iter().zip(rhs.iter()).all(|(lhs, rhs)| {
                columns
                    .get(lhs)
                    .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs))
                    .is_some_and(|ord| ord.is_eq())
            }),

            Restriction::InTuple { lhs, rhs } => {
                let columns: Option<Vec<_>> = lhs.iter().map(|lhs| columns.get(lhs)).collect();
                columns
                    .map(|columns| {
                        rhs.iter().any(|rhs| {
                            columns.iter().zip(rhs.iter()).all(|(column, rhs)| {
                                compare_row_with_cqlvalue(column, row_id, rhs)
                                    .is_some_and(|ord| ord.is_eq())
                            })
                        })
                    })
                    .is_some()
            }

            Restriction::LtTuple { lhs, rhs } => lhs
                .iter()
                .zip(rhs.iter())
                .map(|(col, rhs_val)| {
                    columns
                        .get(col)
                        .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs_val))
                })
                .find(|ord| ord.is_none() || ord.is_some_and(|ord| !ord.is_eq()))
                .is_some_and(|ord| ord.is_some_and(|ord| ord.is_lt())),

            Restriction::LteTuple { lhs, rhs } => lhs
                .iter()
                .zip(rhs.iter())
                .map(|(col, rhs_val)| {
                    columns
                        .get(col)
                        .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs_val))
                })
                .find(|ord| ord.is_none() || ord.is_some_and(|ord| !ord.is_eq()))
                .is_none_or(|ord| ord.is_some_and(|ord| ord.is_lt())),

            Restriction::GtTuple { lhs, rhs } => lhs
                .iter()
                .zip(rhs.iter())
                .map(|(col, rhs_val)| {
                    columns
                        .get(col)
                        .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs_val))
                })
                .find(|ord| ord.is_none() || ord.is_some_and(|ord| !ord.is_eq()))
                .is_some_and(|ord| ord.is_some_and(|ord| ord.is_gt())),

            Restriction::GteTuple { lhs, rhs } => lhs
                .iter()
                .zip(rhs.iter())
                .map(|(col, rhs_val)| {
                    columns
                        .get(col)
                        .and_then(|column| compare_row_with_cqlvalue(column, row_id, rhs_val))
                })
                .find(|ord| ord.is_none() || ord.is_some_and(|ord| !ord.is_eq()))
                .is_none_or(|ord| ord.is_some_and(|ord| ord.is_ge())),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Operation {
    AddVector {
        row_id: RowId,
        partition_id: PartitionId,
        vector: Vector,
    },
    RemoveBeforeAddVector {
        row_id: RowId,
        partition_id: PartitionId,
    },
    RemoveVector {
        row_id: RowId,
        partition_id: PartitionId,
    },
    RemovePartition {
        partition_id: PartitionId,
    },
}
