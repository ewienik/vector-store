use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time;
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
        db_ip: Ipv4Addr,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
    Stop,
}

pub(crate) trait DbExt {
    async fn version(&self) -> String;
    async fn start(&self, vs_uri: String, db_ip: Ipv4Addr);
    async fn stop(&self);
    async fn wait_for_ready(&self) -> bool;
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

    async fn start(&self, vs_uri: String, db_ip: Ipv4Addr) {
        self.send(Db::Start { vs_uri, db_ip })
            .await
            .expect("DbExt::start: internal actor should receive request");
    }

    async fn stop(&self) {
        self.send(Db::Stop)
            .await
            .expect("DbExt::stop: internal actor should receive request");
    }

    async fn wait_for_ready(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send(Db::WaitForReady { tx })
            .await
            .expect("DbExt::wait_for_ready: internal actor should receive request");
        rx.await
            .expect("DbExt::wait_for_ready: internal actor should send response")
    }
}

pub(crate) async fn new(path: PathBuf, conf: PathBuf, verbose: bool) -> mpsc::Sender<Db> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "scylla executable '{path:?}' does not exist"
    );
    assert!(
        crate::file_exists(&conf).await,
        "scylla config '{path:?}' does not exist"
    );

    let mut state = State::new(path, conf, verbose).await;

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
    conf: PathBuf,
    db_ip: Option<Ipv4Addr>,
    child: Option<Child>,
    workdir: Option<TempDir>,
    version: String,
    verbose: bool,
}

impl State {
    async fn new(path: PathBuf, conf: PathBuf, verbose: bool) -> Self {
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
            conf,
            version,
            db_ip: None,
            child: None,
            workdir: None,
            verbose,
        }
    }
}

async fn process(msg: Db, state: &mut State) {
    match msg {
        Db::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process Db::Version: failed to send a response");
        }

        Db::Start { vs_uri, db_ip } => {
            start(vs_uri, db_ip, state).await;
        }

        Db::Stop {} => {
            stop(state).await;
        }

        Db::WaitForReady { tx } => {
            tx.send(wait_for_ready(state).await)
                .expect("process Db::WaitForReady: failed to send a response");
        }
    }
}

async fn start(vs_uri: String, db_ip: Ipv4Addr, state: &mut State) {
    let workdir = TempDir::new().expect("start: failed to create temporary directory for scylladb");
    let mut cmd = Command::new(&state.path);
    if !state.verbose {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    state.child = Some(
        cmd.arg("--overprovisioned")
            .arg("--options-file")
            .arg(&state.conf)
            .arg("--workdir")
            .arg(workdir.path())
            .arg("--listen-address")
            .arg(db_ip.to_string())
            .arg("--rpc-address")
            .arg(db_ip.to_string())
            .arg("--api-address")
            .arg(db_ip.to_string())
            .arg("--seed-provider-parameters")
            .arg(format!("seeds={db_ip}"))
            .arg("--vector-store-uri")
            .arg(vs_uri)
            .arg("--developer-mode")
            .arg("true")
            .spawn()
            .expect("start: failed to spawn scylladb"),
    );
    state.workdir = Some(workdir);
    state.db_ip = Some(db_ip);
}

async fn stop(state: &mut State) {
    let Some(mut child) = state.child.take() else {
        return;
    };
    child
        .start_kill()
        .expect("stop: failed to send SIGTERM to scylladb process");
    child
        .wait()
        .await
        .expect("stop: failed to wait for scylladb process to exit");
    state.child = None;
    state.workdir = None;
    state.db_ip = None;
}

async fn wait_for_ready(state: &State) -> bool {
    let Some(db_ip) = state.db_ip else {
        return false;
    };
    let mut cmd = Command::new(&state.path);
    cmd.arg("nodetool")
        .arg("-h")
        .arg(db_ip.to_string())
        .arg("status");

    loop {
        if String::from_utf8_lossy(
            &cmd.output()
                .await
                .expect("start: failed to spawn scylladb")
                .stdout,
        )
        .lines()
        .filter(|line| line.starts_with(&format!("UN {db_ip}")))
        .next()
        .is_some()
        {
            return true;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}
