use std::net::Ipv4Addr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

type VersionR = String;

pub(crate) enum Dns {
    Version { tx: oneshot::Sender<VersionR> },
}

pub(crate) trait DnsExt {
    async fn version(&self) -> VersionR;
}

impl DnsExt for mpsc::Sender<Dns> {
    async fn version(&self) -> VersionR {
        let (tx, rx) = oneshot::channel();
        self.send(Dns::Version { tx })
            .await
            .expect("DnsExt::version: internal actor should receive request");
        rx.await
            .expect("DnsExt::version: internal actor should send response")
    }
}

pub(crate) async fn new(ip: Ipv4Addr, base: Ipv4Addr) -> mpsc::Sender<Dns> {
    let (tx, mut rx) = mpsc::channel(10);

    let state = State::new(ip, base).await;

    tokio::spawn(
        async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &state).await;
            }
            debug!("starting");
        }
        .instrument(debug_span!("dns")),
    );

    tx
}

struct State {
    #[allow(dead_code)]
    ip: Ipv4Addr,
    #[allow(dead_code)]
    base: Ipv4Addr,
    #[allow(dead_code)]
    base_octets: [u8; 4],
    version: String,
}

impl State {
    async fn new(ip: Ipv4Addr, base: Ipv4Addr) -> Self {
        assert!(ip.is_loopback(), "DNS server should run in a loopback mode");
        assert!(
            base.is_loopback(),
            "DNS server should serve addresses in a loopback mode"
        );
        let ip_octets = ip.octets();
        let base_octets = base.octets();
        assert_eq!(
            base_octets[3], 1,
            "DNS server should serve addresses from number 1"
        );
        assert!(
            ip_octets[1] != base_octets[1] || ip_octets[2] != base_octets[2],
            "DNS server should serve addresses from different subnet than dns server"
        );

        let version = env!("CARGO_PKG_VERSION").to_string();

        Self {
            ip,
            base,
            base_octets,
            version,
        }
    }
}

async fn process(msg: Dns, state: &State) {
    match msg {
        Dns::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process Dns::Version: failed to send a response");
        }
    }
}
