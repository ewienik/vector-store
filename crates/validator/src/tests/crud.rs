/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::dns::DnsExt;
use crate::ip::IpExt;
use crate::tests::*;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test("dummy", timeout, dummy)
}

const VS_NAME: &str = "vs";

const VS_OCTET: u8 = 1;

async fn init(actors: TestActors) {
    info!("started");
    let vs_ip = actors.ip.calculate(VS_OCTET).await;

    actors.dns.upsert(VS_NAME.to_string(), Some(vs_ip)).await;
    info!(
        "dns entry created for {}.{}: {}",
        VS_NAME,
        actors.dns.domain().await,
        vs_ip
    );

    info!("finished");
}

async fn cleanup(actors: TestActors) {
    info!("started");
    actors.dns.upsert(VS_NAME.to_string(), None).await;
    info!("finished");
}

async fn dummy(_actors: TestActors) {
    info!("started");
    info!("finished");
}
