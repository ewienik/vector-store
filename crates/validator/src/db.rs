use std::net::SocketAddr;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub(crate) enum Db {
    Version {
        tx: oneshot::Sender<String>,
    },
    Start {
        vs_uri: String,
        // TODO: use a cluster
        db_addr: SocketAddr,
    },
    Stop,
}

pub(crate) trait DbExt {
    async fn version(&self) -> String;
    async fn start(&self, vs_uri: String, db_addr: SocketAddr);
    async fn stop(&self);
}

impl DbExt for mpsc::Sender<Db> {
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(Db::Version { tx })
            .await
            .expect("DbExt::version: internal actor should receive request");
        rx.await
            .expect("DbExt::version: internal actor should send response")
    }

    async fn start(&self, vs_uri: String, db_addr: SocketAddr) {
        self.send(Db::Start { vs_uri, db_addr })
            .await
            .expect("DbExt::start: internal actor should receive request");
    }

    async fn stop(&self) {
        self.send(Db::Stop)
            .await
            .expect("DbExt::stop: internal actor should receive request");
    }
}

pub(crate) async fn new(path: PathBuf) -> mpsc::Sender<Db> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "scylla executable '{path:?}' does not exist"
    );

    let mut state = State::new(path).await;

    tokio::spawn(
        async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }
            debug!("starting");
        }
        .instrument(debug_span!("db")),
    );

    tx
}

struct State {
    path: PathBuf,
    child: Option<Child>,
    workdir: Option<TempDir>,
    version: String,
}

impl State {
    async fn new(path: PathBuf) -> Self {
        let version = String::from_utf8_lossy(
            &Command::new(&path)
                .arg("--version")
                .output()
                .await
                .expect("db: State::new: failed to execute scylla")
                .stdout,
        )
        .trim()
        .to_string();

        Self {
            path,
            version,
            child: None,
            workdir: None,
        }
    }
}

async fn process(msg: Db, state: &mut State) {
    match msg {
        Db::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process Db::Version: failed to send a response");
        }

        Db::Start { vs_uri, db_addr } => {
            start(vs_uri, db_addr, state).await;
        }

        Db::Stop {} => {
            stop(state).await;
        }
    }
}

async fn start(vs_uri: String, db_addr: SocketAddr, state: &mut State) {
    let workdir = TempDir::new().expect("start: failed to create temporary directory for scylladb");
    state.child = Some(
        Command::new(&state.path)
            .arg("--overprovisioned")
            .arg("--workdir")
            .arg(workdir.path())
            .arg("--listen-address")
            .arg(db_addr.ip().to_string())
            .arg("--native-transport-port")
            .arg(db_addr.port().to_string())
            .arg("--vector-store-uri")
            .arg(vs_uri)
            .spawn()
            .expect("start: failed to spawn scylladb"),
    );
    state.workdir = Some(workdir);
}

async fn stop(state: &mut State) {
    let Some(mut child) = state.child.take() else {
        return;
    };
    child
        .wait()
        .await
        .expect("stop: failed to wait for scylladb process to exit");
    state.child = None;
    state.workdir = None;
}
