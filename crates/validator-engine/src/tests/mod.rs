/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod ann;
mod crud;
mod full_scan;
mod reconnect;
mod serde;

use vector_search_validator_tests::TestCase;

/// Returns a vector of all known test cases to be run. Each test case is registered with a name
pub(crate) async fn register() -> Vec<(String, TestCase)> {
    vec![
        ("ann", ann::new().await),
        ("crud", crud::new().await),
        ("full_scan", full_scan::new().await),
        ("reconnect", reconnect::new().await),
        ("serde", serde::new().await),
    ]
    .into_iter()
    .map(|(name, test_case)| (name.to_string(), test_case))
    .collect::<Vec<_>>()
}
