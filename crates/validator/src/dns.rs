use hickory_server::authority::Catalog;
use hickory_server::server::Request;
use hickory_server::server::RequestHandler;
use hickory_server::server::ServerFuture;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

type VersionR = String;

pub(crate) enum Dns {
    Version {
        tx: oneshot::Sender<VersionR>,
    },
    AddEntity {
        name: String,
        ip: Ipv4Addr,
        tx: oneshot::Sender<()>,
    },
}

pub(crate) trait DnsExt {
    async fn version(&self) -> VersionR;
    async fn add_entity(&self, name: String, ip: Ipv4Addr) -> ();
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

    async fn add_entity(&self, name: String, ip: Ipv4Addr) -> () {
        let (tx, rx) = oneshot::channel();
        self.send(Dns::AddEntity { name, ip, tx })
            .await
            .expect("DnsExt::add_entity: internal actor should receive request");
        rx.await
            .expect("DnsExt::add_entity: internal actor should send response")
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

#[derive(Clone)]
struct Store(Arc<RwLock<Catalog>>);

impl Store {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(Catalog::new())))
    }
}

impl RequestHandler for Store {
    fn handle_request<R>(&self, request: &Request, response_handle: R) -> Self::Response {
        let mut catalog = self.0.read().unwrap();
        catalog.handle_request(request, response_handle)
    }
}

impl State {
    async fn new(ip: Ipv4Addr, base: Ipv4Addr) -> Self {
        let version = format!("hicory-server-{}", hickory_server::version());

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

        let store = Store::new();
        let server = ServerFuture::new(store.clone());

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

        Dns::AddEntity { name, ip, tx } => {
            add_entity(name, ip, state).await;
            tx.send(())
                .expect("process Dns::AddEntity: failed to send a response");
        }
    }
}

async fn add_entity(name: String, ip: Ipv4Addr, state: &State) -> () {}
