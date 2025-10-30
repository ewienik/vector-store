/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub mod common;
mod dns;
mod scylla_cluster;
mod vector_store_cluster;

pub use dns::Dns;
pub use dns::DnsExt;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::StreamExt;
pub use scylla_cluster::ScyllaCluster;
pub use scylla_cluster::ScyllaClusterExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::Instrument;
use tracing::Span;
use tracing::error;
use tracing::info;
use tracing::info_span;
pub use vector_store_cluster::VectorStoreCluster;
pub use vector_store_cluster::VectorStoreClusterExt;

/// Represents a subnet for services, derived from a base IP address.
pub struct ServicesSubnet([u8; 3]);

impl ServicesSubnet {
    pub fn new(ip: Ipv4Addr) -> Self {
        assert!(
            ip.is_loopback(),
            "Base IP for services must be a loopback address"
        );

        let octets = ip.octets();
        assert!(
            octets[3] == 1,
            "Base IP for services must have the last octet set to 1"
        );

        Self([octets[0], octets[1], octets[2]])
    }

    /// Returns an IP address in the subnet with the specified last octet.
    pub fn ip(&self, octet: u8) -> Ipv4Addr {
        [self.0[0], self.0[1], self.0[2], octet].into()
    }
}

#[derive(Clone)]
pub struct TestActors {
    pub services_subnet: Arc<ServicesSubnet>,
    pub dns: mpsc::Sender<Dns>,
    pub db: mpsc::Sender<ScyllaCluster>,
    pub vs: mpsc::Sender<VectorStoreCluster>,
}

type TestFuture = BoxFuture<'static, ()>;

type TestFn = Box<dyn Fn(TestActors) -> TestFuture>;

#[derive(Debug)]
/// Statistics for a test run, including total tests, launched, successful, and failed.
pub(crate) struct Statistics {
    total: usize,
    launched: usize,
    ok: usize,
    failed: usize,
}

impl Statistics {
    fn new(total: usize) -> Self {
        Self {
            total,
            launched: 0,
            ok: 0,
            failed: 0,
        }
    }

    fn append(&mut self, other: &Self) {
        self.total += other.total;
        self.launched += other.launched;
        self.ok += other.ok;
        self.failed += other.failed;
    }
}

/// Represents a single test case, which can include initialization, multiple tests, and cleanup.
pub struct TestCase {
    init: Option<(Duration, TestFn)>,
    tests: Vec<(String, Duration, TestFn)>,
    cleanup: Option<(Duration, TestFn)>,
}

impl TestCase {
    /// Creates a new empty test case.
    pub fn empty() -> Self {
        Self {
            init: None,
            tests: vec![],
            cleanup: None,
        }
    }

    /// Returns a reference to the tests in this test case.
    pub fn tests(&self) -> &Vec<(String, Duration, TestFn)> {
        &self.tests
    }

    /// Add an initialization function to the test case.
    pub fn with_init<F, R>(mut self, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.init = Some((timeout, wrap_test_fn(test_fn)));
        self
    }

    /// Add a test to the test case.
    pub fn with_test<F, R>(mut self, name: impl ToString, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.tests
            .push((name.to_string(), timeout, wrap_test_fn(test_fn)));
        self
    }

    /// Add a cleanup function to the test case.
    pub fn with_cleanup<F, R>(mut self, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.cleanup = Some((timeout, wrap_test_fn(test_fn)));
        self
    }

    /// Run initialization, all tests, and cleanup functions in the test case.
    async fn run(&self, actors: TestActors, test_cases: &HashSet<String>) -> Statistics {
        let mut stats = Statistics::new(
            self.tests.len() + self.init.is_some() as usize + self.cleanup.is_some() as usize,
        );

        if let Some((timeout, init)) = &self.init {
            stats.launched += 1;
            if !run_single(info_span!("init"), *timeout, init(actors.clone())).await {
                stats.failed += 1;
                return stats;
            }
            stats.ok += 1;
        }

        stream::iter(self.tests.iter())
            .filter(|(name, _, _)| {
                future::ready(test_cases.is_empty() || test_cases.contains(name))
            })
            .then(|(name, timeout, test)| {
                let actors = actors.clone();
                stats.launched += 1;
                async move { run_single(info_span!("test", name), *timeout, test(actors)).await }
            })
            .for_each(|ok| {
                if ok {
                    stats.ok += 1;
                } else {
                    stats.failed += 1;
                };
                future::ready(())
            })
            .await;

        if let Some((timeout, cleanup)) = &self.cleanup {
            stats.launched += 1;
            if !run_single(info_span!("cleanup"), *timeout, cleanup(actors.clone())).await {
                stats.failed += 1;
            } else {
                stats.ok += 1;
            }
        }

        stats
    }
}

/// Wraps a test function into a `TestFn` type, which is a boxed future that can be stored in a
/// container.
fn wrap_test_fn<F, R>(test_fn: F) -> TestFn
where
    F: Fn(TestActors) -> R + 'static,
    R: Future<Output = ()> + Send + 'static,
{
    Box::new(move |actors: TestActors| {
        let future = test_fn(actors);
        future.boxed()
    })
}

/// Runs a single test with a timeout, logging the result in the provided span.
async fn run_single(span: Span, timeout: Duration, future: TestFuture) -> bool {
    let task = tokio::spawn({
        async move {
            time::timeout(timeout, future)
                .await
                .expect("test timed out");
        }
        .instrument(span.clone())
    });
    if task.await.is_ok() {
        info!(parent: &span, "test ok");
        return true;
    }
    error!(parent: &span, "test failed");
    false
}

/// Runs all test cases, filtering them based on the provided filter map.
pub async fn run(
    actors: TestActors,
    test_cases: Vec<(String, TestCase)>,
    filter_map: Arc<HashMap<String, HashSet<String>>>,
) -> bool {
    let stats = stream::iter(test_cases.into_iter())
        .filter(|(file_name, _)| {
            let process = filter_map.is_empty() || filter_map.contains_key(file_name);
            async move { process }
        })
        .then(|(name, test_case)| {
            let actors = actors.clone();
            let filter = filter_map.clone();
            let file_name = name.clone();
            async move {
                let stats = test_case
                    .run(actors, filter.get(&file_name).unwrap_or(&HashSet::new()))
                    .instrument(info_span!("test-case", name))
                    .await;
                if stats.failed > 0 {
                    error!("test case failed: {stats:?}");
                } else {
                    info!("test case ok: {stats:?}");
                }
                stats
            }
        })
        .fold(Statistics::new(0), |mut acc, stats| async move {
            acc.append(&stats);
            acc
        })
        .await;
    if stats.failed > 0 {
        error!("test run failed: {stats:?}");
        return false;
    }
    info!("test run ok: {stats:?}");
    true
}
