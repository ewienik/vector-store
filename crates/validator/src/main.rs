mod db;
mod dns;
mod ip;
mod test_case;
mod tests;
mod vs;

use clap::Parser;
use db::DbExt;
use dns::DnsExt;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use test_case::TestActors;
use tokio::fs;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use vs::VsExt;

#[derive(Debug, Parser)]
#[clap(version)]
struct Args {
    #[arg(short, long, default_value = "127.0.1.1")]
    dns_ip: Ipv4Addr,

    #[arg(short, long, default_value = "127.0.2.1")]
    base_ip: Ipv4Addr,

    scylla: PathBuf,

    vector_store: PathBuf,
}

async fn executable_exists(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path).await else {
        return false;
    };
    metadata.is_file() && (metadata.permissions().mode() & 0o111 != 0)
}

fn validate_address(dns_ip: Ipv4Addr, base_ip: Ipv4Addr) {
    assert!(
        dns_ip.is_loopback(),
        "DNS server should listen on a localhost"
    );
    assert!(
        base_ip.is_loopback(),
        "DNS server should serve addresses from a localhost"
    );
    let dns_octets = dns_ip.octets();
    let base_octets = base_ip.octets();
    assert!(
        dns_octets[1] != base_octets[1] || dns_octets[2] != base_octets[2],
        "DNS server should serve addresses from a different subnet than its own"
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .expect("Failed to create EnvFilter"),
        )
        .with(fmt::layer().with_target(false))
        .init();

    let args = Args::parse();

    validate_address(args.dns_ip, args.base_ip);

    let dns = dns::new(args.dns_ip).await;
    let db = db::new(args.scylla).await;
    let vs = vs::new(args.vector_store).await;
    let ip = ip::new(args.base_ip).await;

    info!(
        "{} version: {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    info!("dns version: {}", dns.version().await);
    info!("scylla version: {}", db.version().await);
    info!("vector-store version: {}", vs.version().await);

    let test_cases = tests::register().await;

    // TODO: implement a filter using cmdline arguments
    assert!(
        tests::run(
            TestActors { dns, db, vs, ip },
            test_cases,
            Arc::new(HashMap::new())
        )
        .await
    );
}
