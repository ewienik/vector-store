/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub enum VectorStoreCluster {
    Version {
        tx: oneshot::Sender<String>,
    },
    Start {
        vs_addr: SocketAddr,
        db_addr: SocketAddr,
    },
    Stop {
        tx: oneshot::Sender<()>,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
}

pub trait VectorStoreClusterExt {
    /// Returns the version of the vector-store binary.
    fn version(&self) -> impl Future<Output = String>;

    /// Starts the vector-store server with the given addresses.
    fn start(&self, vs_addr: SocketAddr, db_addr: SocketAddr) -> impl Future<Output = ()>;

    /// Stops the vector-store server.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Waits for the vector-store server to be ready.
    fn wait_for_ready(&self) -> impl Future<Output = bool>;
}

impl VectorStoreClusterExt for mpsc::Sender<VectorStoreCluster> {
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::Version { tx })
            .await
            .expect("VectorStoreClusterExt::version: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::version: internal actor should send response")
    }

    async fn start(&self, vs_addr: SocketAddr, db_addr: SocketAddr) {
        self.send(VectorStoreCluster::Start { vs_addr, db_addr })
            .await
            .expect("VectorStoreClusterExt::start: internal actor should receive request");
    }

    async fn stop(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::Stop { tx })
            .await
            .expect("VectorStoreClusterExt::stop: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::stop: internal actor should send response");
    }

    async fn wait_for_ready(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::WaitForReady { tx })
            .await
            .expect("VectorStoreClusterExt::wait_for_ready: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::wait_for_ready: internal actor should send response")
    }
}
