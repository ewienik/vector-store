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
        let url = client.url();
        loop {
            let found = client
                .count(&KEYSPACE.to_string().into(), &INDEX.to_string().into())
                .await;
            if let Some(found) = found {
                if found == count {
                    break;
                }
                info!(
                    "Waiting for index {INDEX} at {url} to be ready, found: {found}, expected: {count}"
                );
            } else {
                info!("Waiting for index {INDEX} at {url} to be ready, got no response");
            }
            time::sleep(Duration::from_secs(1)).await;
        }
        info!("Index {INDEX} at {url} is ready");
    }
}
