/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn pipeline_fullscan_add(c: &mut Criterion) {}

fn pipeline_search(c: &mut Criterion) {}

criterion_group!(benches, pipeline_fullscan_add, pipeline_search);
criterion_main!(benches);
