use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub(crate) enum Vs {
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
}

pub(crate) trait VsExt {
    async fn version(&self) -> String;
    async fn start(&self, vs_addr: SocketAddr, db_addr: SocketAddr);
    async fn stop(&self);
}

impl VsExt for mpsc::Sender<Vs> {
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(Vs::Version { tx })
            .await
            .expect("VsExt::version: internal actor should receive request");
        rx.await
            .expect("VsExt::version: internal actor should send response")
    }

    async fn start(&self, vs_addr: SocketAddr, db_addr: SocketAddr) {
        self.send(Vs::Start { vs_addr, db_addr })
            .await
            .expect("VsExt::start: internal actor should receive request");
    }

    async fn stop(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(Vs::Stop { tx })
            .await
            .expect("VsExt::stop: internal actor should receive request");
        rx.await
            .expect("VsExt::stop: internal actor should send response");
    }
}

pub(crate) async fn new(path: PathBuf) -> mpsc::Sender<Vs> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "vector-store executable '{path:?}' does not exist"
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
        .instrument(debug_span!("vs")),
    );

    tx
}

struct State {
    path: PathBuf,
    child: Option<Child>,
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

        Self {
            path,
            version,
            child: None,
        }
    }
}

async fn process(msg: Vs, state: &mut State) {
    match msg {
        Vs::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process Vs::Version: failed to send a response");
        }

        Vs::Start { vs_addr, db_addr } => {
            start(vs_addr, db_addr, state).await;
        }

        Vs::Stop { tx } => {
            stop(state).await;
            tx.send(())
                .expect("process Vs::Stop: failed to send a response");
        }
    }
}

async fn start(vs_addr: SocketAddr, db_addr: SocketAddr, state: &mut State) {
    state.child = Some(
        Command::new(&state.path)
            .env("VECTOR_STORE_URI", dbg!(vs_addr.to_string()))
            .env("VECTOR_STORE_SCYLLADB_URI", dbg!(db_addr.to_string()))
            .spawn()
            .expect("start: failed to spawn vector-store"),
    );
}

async fn stop(state: &mut State) {
    let Some(mut child) = state.child.take() else {
        return;
    };
    child
        .wait()
        .await
        .expect("stop: failed to wait for vector-store process to exit");
    state.child = None;
}
