use std::path::PathBuf;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

type VersionR = String;

pub(crate) enum Vs {
    Version { tx: oneshot::Sender<VersionR> },
}

pub(crate) trait VsExt {
    async fn version(&self) -> VersionR;
}

impl VsExt for mpsc::Sender<Vs> {
    async fn version(&self) -> VersionR {
        let (tx, rx) = oneshot::channel();
        self.send(Vs::Version { tx })
            .await
            .expect("VsExt::version: internal actor should receive request");
        rx.await
            .expect("VsExt::version: internal actor should send response")
    }
}

pub(crate) async fn new(path: PathBuf) -> mpsc::Sender<Vs> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "vector-store executable '{path:?}' does not exist"
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
        .instrument(debug_span!("vs")),
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
                .expect("vs: State::new: failed to execute vector-store")
                .stdout,
        )
        .trim()
        .to_string();

        Self { path, version }
    }
}

async fn process(msg: Vs, state: &State) {
    match msg {
        Vs::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process Vs::Version: failed to send a response");
        }
    }
}
