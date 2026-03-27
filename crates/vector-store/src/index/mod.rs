/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub mod actor;
pub mod factory;
pub mod validator;

pub(crate) use actor::IndexModify;
pub(crate) use actor::IndexModifyExt;
pub(crate) use actor::IndexSearch;
pub(crate) use actor::IndexSearchExt;

pub(crate) mod opensearch;
pub(crate) mod usearch;
