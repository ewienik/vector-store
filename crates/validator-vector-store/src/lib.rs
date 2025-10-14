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

pub async fn test_cases() -> impl Iterator<Item = (String, TestCase)> {
    vec![
        ("ann", ann::new().await),
        ("crud", crud::new().await),
        ("full_scan", full_scan::new().await),
        ("reconnect", reconnect::new().await),
        ("serde", serde::new().await),
    ]
    .into_iter()
    .map(|(name, test_case)| (name.to_string(), test_case))
}
