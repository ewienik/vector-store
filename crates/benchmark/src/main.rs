/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod data;
mod db;
mod vs;

use crate::db::Scylla;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
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
}

#[tokio::main]
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
            let (duration, _) = measure_duration(async move {
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
            let (duration, _) = measure_duration(async move {
                scylla
                    .create_index(metric_type, m, ef_construction, ef_search)
                    .await;
                vs::wait_for_indexes_ready(&clients, count).await;
            })
            .await;
            info!("Build Index took {duration:.2?}");
        }
    };
}

async fn measure_duration<T>(f: impl Future<Output = T>) -> (Duration, T) {
    let start = Instant::now();
    let t = f.await;
    (start.elapsed(), t)
}
