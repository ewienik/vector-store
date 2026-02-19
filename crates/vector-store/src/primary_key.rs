/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

//! CQL primary key type backed by [`InvariantKey`](crate::invariant_key::InvariantKey).

use crate::invariant_key::InvariantKey;

/// A memory-optimized CQL primary key.
///
/// This is a thin newtype around [`InvariantKey`] providing primary-key-specific
/// semantics. Cloning is O(1) because `InvariantKey` uses `Arc` internally.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::From, derive_more::AsRef,
)]
pub struct PrimaryKey(InvariantKey);

// Static assertion: PrimaryKey must be exactly 16 bytes (same as InvariantKey).
const _: () = assert!(std::mem::size_of::<PrimaryKey>() == 16);
