/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod data;
mod db;
mod vs;

use clap::Parser;
use clap::Subcommand;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
#[clap(version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {}

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

    _ = Args::parse();
}
