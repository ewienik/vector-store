/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod data;
mod db;
mod vs;

use crate::data::Query;
use crate::db::Scylla;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use itertools::Itertools;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::Notify;
use tokio::time;
use tokio::time::Instant;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

const KEYSPACE: &str = "vsb_keyspace";
const TABLE: &str = "vsb_table";
const INDEX: &str = "vsb_idex";

#[derive(Parser)]
#[clap(version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum MetricType {
    Cosine,
    Euclidean,
    DotProduct,
}

#[derive(Subcommand)]
enum Command {
    BuildTable {
        #[clap(long)]
        data_dir: PathBuf,

        #[clap(long)]
        scylla: SocketAddr,

        #[clap(long, value_parser = clap::value_parser!(u32).range(1..=1_000_000))]
        concurrency: u32,
    },

    BuildIndex {
        #[clap(long)]
        data_dir: PathBuf,

        #[clap(long)]
        scylla: SocketAddr,

        #[clap(long, required = true)]
        vector_store: Vec<SocketAddr>,

        #[clap(long)]
        metric_type: MetricType,

        #[clap(long)]
        m: usize,

        #[clap(long)]
        ef_construction: usize,

        #[clap(long)]
        ef_search: usize,
    },

    DropTable {
        #[clap(long)]
        scylla: SocketAddr,
    },

    DropIndex {
        #[clap(long)]
        scylla: SocketAddr,
    },

    Search {
        #[clap(long)]
        data_dir: PathBuf,

        #[clap(long)]
        scylla: SocketAddr,

        #[clap(long, value_parser = clap::value_parser!(u32).range(1..=100))]
        limit: u32,

        #[clap(long)]
        duration: humantime::Duration,

        #[clap(long, value_parser = clap::value_parser!(u32).range(1..=1_000_000))]
        concurrency: u32,

        #[clap(long)]
        from: Option<humantime::Timestamp>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .with(fmt::layer().with_target(false))
        .init();

    match Args::parse().command {
        Command::BuildTable {
            data_dir,
            scylla,
            concurrency,
        } => {
            let dimension = data::dimension(&data_dir).await;
            let stream = data::vector_stream(&data_dir).await;
            let scylla = Scylla::new(scylla).await;
            let duration = measure_duration(async move {
                scylla.create_table(dimension).await;
                scylla.upload_vectors(stream, concurrency as usize).await;
            })
            .await;
            info!("Build table took {duration:.2?}");
        }

        Command::BuildIndex {
            data_dir,
            scylla,
            vector_store,
            metric_type,
            m,
            ef_construction,
            ef_search,
        } => {
            let count = data::count(&data_dir).await;
            let scylla = Scylla::new(scylla).await;
            let clients = vs::new_http_clients(vector_store);
            let duration = measure_duration(async move {
                scylla
                    .create_index(metric_type, m, ef_construction, ef_search)
                    .await;
                vs::wait_for_indexes_ready(&clients, count).await;
            })
            .await;
            info!("Build Index took {duration:.2?}");
        }

        Command::DropTable { scylla } => {
            let scylla = Scylla::new(scylla).await;
            let duration = measure_duration(async move {
                scylla.drop_table().await;
            })
            .await;
            info!("Drop Table took {duration:.2?}");
        }

        Command::DropIndex { scylla } => {
            let scylla = Scylla::new(scylla).await;
            let duration = measure_duration(async move {
                scylla.drop_index().await;
            })
            .await;
            info!("Drop Index took {duration:.2?}");
        }

        Command::Search {
            data_dir,
            scylla,
            limit,
            duration,
            concurrency,
            from,
        } => {
            let queries = Arc::new(data::queries(&data_dir, limit as usize).await);
            let start = from
                .map(SystemTime::from)
                .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(10));
            let stop = start + Duration::from(duration);
            let notify = Arc::new(Notify::new());
            let scylla = Scylla::new(scylla).await;

            let count = Arc::new(AtomicU64::new(0));
            let tasks = (0..concurrency)
                .map(|_| {
                    let scylla = scylla.clone();
                    let queries = Arc::clone(&queries);
                    let count = Arc::clone(&count);
                    let notify = Arc::clone(&notify);
                    tokio::spawn(async move {
                        let mut histogram = Histogram::new();
                        notify.notified().await;
                        while SystemTime::now() < stop {
                            count.fetch_add(1, Ordering::Relaxed);
                            let duration = measure_duration(scylla.search(random(&queries))).await;
                            histogram.record(duration);
                        }
                        histogram
                    })
                })
                .collect_vec();

            let wait_for_start = start.duration_since(SystemTime::now()).unwrap();
            info!("Synchronizing search tasks to start after {wait_for_start:.2?}");
            time::sleep_until(Instant::now() + wait_for_start).await;

            info!("Starting search tasks");
            let mut histogram = Histogram::new();
            let duration = measure_duration(async {
                notify.notify_waiters();
                for task in tasks.into_iter() {
                    histogram.append(&task.await.unwrap());
                }
            })
            .await;
            info!(
                "QPS: {:.2}",
                count.load(Ordering::Relaxed) as f64 / duration.as_secs_f64()
            );
            info!("P10: {:.1?}", histogram.percentile(10.0));
            info!("P25: {:.1?}", histogram.percentile(25.0));
            info!("P50: {:.1?}", histogram.percentile(50.0));
            info!("P75: {:.1?}", histogram.percentile(75.0));
            info!("P90: {:.1?}", histogram.percentile(90.0));
            info!("P99: {:.1?}", histogram.percentile(99.0));
        }
    };
}

async fn measure_duration(f: impl Future<Output = ()>) -> Duration {
    let start = Instant::now();
    f.await;
    start.elapsed()
}

fn random(data: &[Query]) -> &Query {
    &data[rand::random_range(0..data.len())]
}

const BUCKETS: usize = 10_000;
const MIN_DURATION: Duration = Duration::from_millis(1);
const MAX_DURATION: Duration = Duration::from_millis(1000);
const STEP_DURATION: Duration = MAX_DURATION
    .checked_sub(MIN_DURATION)
    .unwrap()
    .checked_div(BUCKETS as u32)
    .unwrap();

struct Histogram {
    buckets: [u64; BUCKETS + 2],
    count: u64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: [0; BUCKETS + 2],
            count: 0,
        }
    }

    fn record(&mut self, value: Duration) {
        let idx = if value < MIN_DURATION {
            0
        } else if value > MAX_DURATION {
            BUCKETS + 1
        } else {
            (value - MIN_DURATION)
                .div_duration_f64(STEP_DURATION)
                .round() as usize
                + 1
        };
        self.buckets[idx] += 1;
        self.count += 1;
    }

    fn percentile(&self, percentile: f64) -> Duration {
        let percentile = (self.count as f64 * percentile / 100.0) as u64;
        let mut sum = 0;
        let Some(idx) = self
            .buckets
            .iter()
            .enumerate()
            .map(|(idx, &count)| {
                sum += count;
                (idx, sum)
            })
            .find_map(|(idx, sum)| (sum >= percentile).then_some(idx))
        else {
            return Duration::MAX;
        };
        if idx == self.buckets.len() - 1 {
            return Duration::MAX;
        }
        MIN_DURATION + STEP_DURATION * idx as u32
    }

    fn append(&mut self, other: &Self) {
        for (a, b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a += b;
        }
        self.count += other.count;
    }
}
