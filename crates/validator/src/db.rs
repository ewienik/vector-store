use std::path::PathBuf;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

type VersionR = String;

pub(crate) enum Db {
    Version { tx: oneshot::Sender<VersionR> },
}

pub(crate) trait DbExt {
    async fn version(&self) -> VersionR;
}

impl DbExt for mpsc::Sender<Db> {
    async fn version(&self) -> VersionR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::Version { tx })
            .await
            .expect("DbExt::version: internal actor should receive request");
        rx.await
            .expect("DbExt::version: internal actor should send response")
    }
}

pub(crate) async fn new(path: PathBuf) -> mpsc::Sender<Db> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "scylla executable '{path:?}' does not exist"
    );

    let state = State::new(path).await;

    tokio::spawn(
        async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &state).await;
            }
            debug!("starting");
        }
        .instrument(debug_span!("db")),
    );

    tx
}

struct State {
    #[allow(dead_code)]
    path: PathBuf,
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

        Self { path, version }
    }
}

async fn process(msg: Db, state: &State) {
    match msg {
        Db::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process Db::Version: failed to send a response");
        }
    }
}
