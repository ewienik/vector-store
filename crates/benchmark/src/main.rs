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
#[derive(Parser)]
#[clap(version)]
struct Args {
    #[command(subcommand)]
    command: Command,
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
    };
}

async fn measure_duration<T>(f: impl Future<Output = T>) -> (Duration, T) {
    let start = Instant::now();
    let t = f.await;
    (start.elapsed(), t)
}
