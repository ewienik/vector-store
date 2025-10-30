/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use httpclient::HttpClient;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use vector_search_validator_tests::VectorStoreCluster;
use vector_store::httproutes::Status;

pub(crate) async fn new(
    path: PathBuf,
    verbose: bool,
    disable_colors: bool,
) -> mpsc::Sender<VectorStoreCluster> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "vector-store executable '{path:?}' does not exist"
    );

    let mut state = State::new(path, verbose, disable_colors).await;

    tokio::spawn(
        async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }

            debug!("finished");
        }
        .instrument(debug_span!("vs")),
    );

    tx
}

struct State {
    path: PathBuf,
    child: Option<Child>,
    client: Option<HttpClient>,
    version: String,
    verbose: bool,
    disable_colors: bool,
}

impl State {
    async fn new(path: PathBuf, verbose: bool, disable_colors: bool) -> Self {
        let version = String::from_utf8_lossy(
            &Command::new(&path)
                .arg("--version")
                .output()
                .await
                .expect("vs: State::new: failed to execute vector-store")
                .stdout,
        )
        .trim()
        .to_string();

        Self {
            path,
            version,
            child: None,
            client: None,
            verbose,
            disable_colors,
        }
    }
}

async fn process(msg: VectorStoreCluster, state: &mut State) {
    match msg {
        VectorStoreCluster::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process VectorStoreCluster::Version: failed to send a response");
        }

        VectorStoreCluster::Start { vs_addr, db_addr } => {
            start(vs_addr, db_addr, state).await;
        }

        VectorStoreCluster::Stop { tx } => {
            stop(state).await;
            tx.send(())
                .expect("process VectorStoreCluster::Stop: failed to send a response");
        }

        VectorStoreCluster::WaitForReady { tx } => {
            tx.send(wait_for_ready(state).await)
                .expect("process VectorStoreCluster::WaitForReady: failed to send a response");
        }
    }
}

async fn start(vs_addr: SocketAddr, db_addr: SocketAddr, state: &mut State) {
    let mut cmd = Command::new(&state.path);
    if !state.verbose {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    state.child = Some(
        cmd.env("VECTOR_STORE_URI", vs_addr.to_string())
            .env("VECTOR_STORE_SCYLLADB_URI", db_addr.to_string())
            .env("VECTOR_STORE_THREADS", "2")
            .env(
                "VECTOR_STORE_DISABLE_COLORS",
                state.disable_colors.to_string(),
            )
            .spawn()
            .expect("start: failed to spawn vector-store"),
    );
    state.client = Some(HttpClient::new(vs_addr));
}

async fn stop(state: &mut State) {
    let Some(mut child) = state.child.take() else {
        return;
    };
    child
        .start_kill()
        .expect("stop: failed to send SIGTERM to vector-store process");
    child
        .wait()
        .await
        .expect("stop: failed to wait for vector-store process to exit");
    state.child = None;
    state.client = None;
}

/// Waits for the vector-store server to be ready checking the status of the service.
async fn wait_for_ready(state: &State) -> bool {
    let Some(ref client) = state.client else {
        return false;
    };

    loop {
        if matches!(client.status().await, Ok(Status::Serving)) {
            return true;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}
