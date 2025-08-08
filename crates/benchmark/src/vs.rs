/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::INDEX;
use crate::KEYSPACE;
use httpclient::HttpClient;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time;
use tracing::info;

pub(crate) fn new_http_clients(addrs: Vec<SocketAddr>) -> Vec<HttpClient> {
    addrs.into_iter().map(HttpClient::new).collect()
}

pub(crate) async fn wait_for_indexes_ready(clients: &[HttpClient], count: usize) {
    for client in clients {
        while client
            .count(&KEYSPACE.to_string().into(), &INDEX.to_string().into())
            .await
            .is_none()
        {
            time::sleep(Duration::from_secs(1)).await;
        }
        while let found = client
            .count(&KEYSPACE.to_string().into(), &INDEX.to_string().into())
            .await
            .unwrap()
            && found != count
        {
            info!("Waiting for index {INDEX} to be ready, found: {found}, expected: {count}");
            time::sleep(Duration::from_secs(1)).await;
        }
    }
}
