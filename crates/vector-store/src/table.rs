/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::DbEmbedding;
use crate::PartitionKey;
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

mod row_id_epoch {
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
use row_id_epoch::Epoch;
pub use row_id_epoch::RowId;

mod partition_id {
    use super::*;

    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::From)]
    pub(crate) struct PartitionId(u64);

    impl PartitionId {
        pub(super) const GLOBAL: PartitionId = PartitionId(0);
    }

    impl Idx for PartitionId {
        fn idx(&self) -> usize {
            self.0 as usize
        }
    }
}
use partition_id::PartitionId;

mod column_vec {
    use super::*;

    pub(super) struct ColumnVec<T> {
        vec: Vec<T>,
    }

    impl<T> ColumnVec<T> {
        pub(super) fn new() -> Self {
            Self { vec: Vec::new() }
        }

        pub(super) fn resize_with(&mut self, size: usize, f: impl FnMut() -> T) {
            self.vec.resize_with(size, f);
        }

        pub(super) fn get(&self, idx: usize) -> Option<&T> {
            self.vec.get(idx)
        }

        pub(super) fn get_mut(&mut self, idx: usize) -> Option<&mut T> {
            self.vec.get_mut(idx)
        }

        pub(super) fn update(&mut self, idx: impl Idx, value: T) -> anyhow::Result<()> {
            *self
                .get_mut(idx.idx())
                .ok_or_else(|| anyhow!("Index out of ColumnVec bounds"))? = value;
            Ok(())
        }
    }

    impl<T> ColumnVec<ColumnValue<T>> {
        pub(super) fn update_epoch_timestamp(
            &mut self,
            row_id: RowId,
            timestamp: Timestamp,
        ) -> anyhow::Result<()> {
            self.get_mut(row_id.idx())
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
    Ascii(ColumnVec<ColumnValue<String>>),
    BigInt(ColumnVec<ColumnValue<i64>>),
    Blob(ColumnVec<ColumnValue<Vec<u8>>>),
    Boolean(ColumnVec<ColumnValue<bool>>),
    Date(ColumnVec<ColumnValue<CqlDate>>),
    Double(ColumnVec<ColumnValue<f64>>),
    Float(ColumnVec<ColumnValue<f32>>),
    Inet(ColumnVec<ColumnValue<IpAddr>>),
    Int(ColumnVec<ColumnValue<i32>>),
    SmallInt(ColumnVec<ColumnValue<i16>>),
    Text(ColumnVec<ColumnValue<String>>),
    Time(ColumnVec<ColumnValue<CqlTime>>),
    Timestamp(ColumnVec<ColumnValue<CqlTimestamp>>),
    Timeuuid(ColumnVec<ColumnValue<CqlTimeuuid>>),
    TinyInt(ColumnVec<ColumnValue<i8>>),
    Uuid(ColumnVec<ColumnValue<Uuid>>),
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
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Ascii),
            Self::BigInt(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::BigInt),
            Self::Blob(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Blob),
            Self::Boolean(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Boolean),
            Self::Date(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Date),
            Self::Double(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Double),
            Self::Float(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Float),
            Self::Inet(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Inet),
            Self::Int(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Int),
            Self::SmallInt(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::SmallInt),
            Self::Text(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Text),
            Self::Time(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Time),
            Self::Timestamp(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Timestamp),
            Self::Timeuuid(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Timeuuid),
            Self::TinyInt(vec) => vec
                .get(row_id.idx())
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::TinyInt),
            Self::Uuid(vec) => vec
                .get(row_id.idx())
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
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::BigInt(lhs), Column::BigInt(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Blob(lhs), Column::Blob(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Boolean(lhs), Column::Boolean(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Date(lhs), Column::Date(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Double(lhs), Column::Double(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            lhs_value.partial_cmp(rhs_value)
        }
        (Column::Float(lhs), Column::Float(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            lhs_value.partial_cmp(rhs_value)
        }
        (Column::Inet(lhs), Column::Inet(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Int(lhs), Column::Int(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::SmallInt(lhs), Column::SmallInt(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Text(lhs), Column::Text(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Time(lhs), Column::Time(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Timestamp(lhs), Column::Timestamp(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Timeuuid(lhs), Column::Timeuuid(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::TinyInt(lhs), Column::TinyInt(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Uuid(lhs), Column::Uuid(rhs)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            let rhs_value = rhs.get(rhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        _ => None,
    }
}

fn compare_row_with_cqlvalue(lhs: &Column, lhs_row_id: RowId, rhs: &CqlValue) -> Option<Ordering> {
    match (lhs, rhs) {
        (Column::Ascii(lhs), CqlValue::Ascii(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::BigInt(lhs), CqlValue::BigInt(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Blob(lhs), CqlValue::Blob(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Boolean(lhs), CqlValue::Boolean(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Date(lhs), CqlValue::Date(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Double(lhs), CqlValue::Double(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            lhs_value.partial_cmp(rhs_value)
        }
        (Column::Float(lhs), CqlValue::Float(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            lhs_value.partial_cmp(rhs_value)
        }
        (Column::Inet(lhs), CqlValue::Inet(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Int(lhs), CqlValue::Int(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::SmallInt(lhs), CqlValue::SmallInt(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Text(lhs), CqlValue::Text(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Time(lhs), CqlValue::Time(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Timestamp(lhs), CqlValue::Timestamp(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.0.cmp(&rhs_value.0))
        }
        (Column::Timeuuid(lhs), CqlValue::Timeuuid(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::TinyInt(lhs), CqlValue::TinyInt(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        (Column::Uuid(lhs), CqlValue::Uuid(rhs_value)) => {
            let lhs_value = lhs.get(lhs_row_id.idx())?.get()?;
            Some(lhs_value.cmp(rhs_value))
        }
        _ => None,
    }
}

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
    // TODO: remove allow
    #[allow(clippy::non_canonical_partial_ord_impl)]
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
    // TODO: remove allow
    #[allow(clippy::non_canonical_partial_ord_impl)]
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

enum Partition {
    None,
    Some {
        /// A map from RowId::idx to PartitionId
        map: BTreeMap<PartitionMapK, PartitionId>,

        /// A queue for free PartitionIds
        free_ids: FreePartitionIds,

        /// A map from RowId to PartitionId
        ids: ColumnVec<PartitionId>,

        /// A map from PartitionId to the number of rows in the partition
        sizes: ColumnVec<usize>,
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
                        *sizes.get_mut(partition_id.idx()).ok_or_else(|| {
                            anyhow!("PartitionId index out of partition sizes bounds")
                        })? += 1;
                        partition_id
                    }
                    Entry::Vacant(entry) => {
                        let partition_id = free_ids.peek_id()?;
                        *sizes.get_mut(partition_id.idx()).ok_or_else(|| {
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
                    .get_mut(partition_id.idx())
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
                .get(row_id.idx())
                .copied()
                .ok_or_else(|| anyhow!("RowId index out of partition ids bounds")),
        }
    }
}

pub struct Table {
    /// A map from RowId::idx to current Epoch
    row_map: BTreeMap<RowMapK, Epoch>,

    /// A queue for free RowIds
    free_row_ids: FreeRowIds,

    /// A storage for column values, indexed by RowId
    columns: Arc<RwLock<Columns>>,

    /// A map from RowId to the Timestamp of the last vector update
    vector_timestamps: ColumnVec<ColumnValue<()>>,

    /// Storage for partition/local index data
    partition: Partition,
}

impl Table {
    const ROWS_INCREMENT_SIZE: usize = 2 ^ 10;

    pub(crate) fn new(
        primary_key_columns: Arc<Vec<ColumnName>>,
        partition_key_columns: Option<Arc<Vec<ColumnName>>>,
        table_columns: Arc<HashMap<ColumnName, NativeType>>,
    ) -> anyhow::Result<Self> {
        let partition = Partition::new(partition_key_columns.is_some());
        let mut table = Self {
            row_map: BTreeMap::new(),
            free_row_ids: FreeRowIds(VecDeque::new()),

            columns: Arc::new(RwLock::new(Columns::new(
                primary_key_columns,
                partition_key_columns,
                &table_columns,
            )?)),
            vector_timestamps: ColumnVec::new(),

            partition,
        };
        table.reserve_row_ids()?;
        table.partition.reserve_ids()?;
        Ok(table)
    }

    fn reserve_row_ids(&mut self) -> anyhow::Result<()> {
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
        let end = start + self.free_row_ids.0.len();

        self.columns.write().unwrap().resize_with(end);
        self.vector_timestamps.resize_with(end, || {
            ColumnValue::None(Epoch::new(), Timestamp::UNIX_EPOCH)
        });
        self.partition.resize_row_to_partition_map(end);
        Ok(())
    }

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
}

#[cfg_attr(test, mockall::automock)]
pub(crate) trait TableAdd {
    fn add(&mut self, db_embedding: DbEmbedding) -> anyhow::Result<[Option<Operation>; 2]>;
}

impl TableAdd for Table {
    fn add(&mut self, db_embedding: DbEmbedding) -> anyhow::Result<[Option<Operation>; 2]> {
        self.reserve_row_ids()?;
        self.partition.reserve_ids()?;

        let mut operations = [None, None];

        let row_id = self.free_row_ids.peek_id()?;
        self.insert_primary_key(row_id, db_embedding.timestamp, db_embedding.primary_key)?;
        let columns = &self.columns;
        let partition = &mut self.partition;
        let vector_timestamps = &mut self.vector_timestamps;

        // TODO: remove this allow
        #[allow(clippy::mutable_key_type)]
        let row_map = &mut self.row_map;

        match row_map.entry(RowMapK {
            columns: Arc::clone(columns),
            row_id,
        }) {
            Entry::Occupied(mut entry) => {
                let row_id = entry.key().row_id;
                let epoch = *entry.get();
                let (vector_epoch, timestamp, vector_already_exists) =
                    match vector_timestamps.get(row_id.idx()) {
                        Some(ColumnValue::Some(epoch, timestamp, _)) => (*epoch, timestamp, true),
                        Some(ColumnValue::None(epoch, timestamp)) => (*epoch, timestamp, false),
                        None => {
                            bail!("Failed to update vector: missing vector timestamp");
                        }
                    };
                if epoch != vector_epoch {
                    bail!("Failed to update vector: vector epoch mismatch");
                }
                if timestamp.0 >= db_embedding.timestamp.0 {
                    return Ok(operations);
                }
                let row_id = row_id.new_epoch(epoch);
                let partition_id = partition.partition_id(row_id)?;
                if let Some(vector) = db_embedding.embedding {
                    if vector_already_exists {
                        operations[0] = Some(Operation::RemoveBeforeAddVector {
                            row_id,
                            partition_id,
                        });
                    }

                    let row_id = row_id.next_epoch();
                    let timestamp = db_embedding.timestamp;
                    vector_timestamps
                        .update(row_id, ColumnValue::Some(row_id.epoch(), timestamp, ()))?;
                    columns
                        .write()
                        .unwrap()
                        .update_primary_key_epoch_timestamp(row_id, timestamp)?;
                    operations[1] = Some(Operation::AddVector {
                        row_id,
                        partition_id,
                        vector,
                    });
                } else {
                    if vector_already_exists {
                        operations[0] = Some(Operation::RemoveVector {
                            row_id,
                            partition_id,
                        });
                    }
                    if let Some(partition_id) = partition.remove_row(row_id)? {
                        operations[1] = Some(Operation::RemovePartition { partition_id });
                    }
                }
                entry.insert(row_id.epoch());
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
