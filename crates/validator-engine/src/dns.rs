/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use hickory_server::authority::Catalog;
use hickory_server::authority::ZoneType;
use hickory_server::proto::rr::DNSClass;
use hickory_server::proto::rr::LowerName;
use hickory_server::proto::rr::Name;
use hickory_server::proto::rr::RrKey;
use hickory_server::proto::rr::rdata::a::A;
use hickory_server::proto::rr::rdata::soa::SOA;
use hickory_server::proto::rr::record_data::RData;
use hickory_server::proto::rr::record_type::RecordType;
use hickory_server::proto::rr::resource::Record;
use hickory_server::server::ServerFuture;
use hickory_server::store::in_memory::InMemoryAuthority;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub(crate) enum Dns {
    Version { tx: oneshot::Sender<String> },
    Domain { tx: oneshot::Sender<String> },
    Remove { name: String },
    Upsert { name: String, ip: Ipv4Addr },
}

pub(crate) trait DnsExt {
    /// Returns the version of the DNS server.
    async fn version(&self) -> String;

    /// Returns the domain name of the DNS server.
    async fn domain(&self) -> String;

    /// Remove an A DNS record with the given name.
    async fn remove(&self, name: String);

    /// Upserts an A DNS record with the given name and IP address.
    async fn upsert(&self, name: String, ip: Ipv4Addr);
}

impl DnsExt for mpsc::Sender<Dns> {
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(Dns::Version { tx })
            .await
            .expect("DnsExt::version: internal actor should receive request");
        rx.await
            .expect("DnsExt::version: internal actor should send response")
    }

    async fn domain(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(Dns::Domain { tx })
            .await
            .expect("DnsExt::domain: internal actor should receive request");
        rx.await
            .expect("DnsExt::domain: internal actor should send response")
    }

    async fn remove(&self, name: String) {
        self.send(Dns::Remove { name })
            .await
            .expect("DnsExt::remove: internal actor should receive request");
    }

    async fn upsert(&self, name: String, ip: Ipv4Addr) {
        self.send(Dns::Upsert { name, ip })
            .await
            .expect("DnsExt::upsert: internal actor should receive request");
    }
}

/// Starts the DNS server on the given IP address.
pub(crate) async fn new(ip: Ipv4Addr) -> mpsc::Sender<Dns> {
    assert!(ip.is_loopback(), "DNS server should listen on a localhost");

    let (tx, mut rx) = mpsc::channel(10);

    let mut state = State::new().await;

    let socket = UdpSocket::bind((ip, 53))
        .await
        .expect("dns: failed to bind UDP socket");

    let mut catalog = Catalog::new();
    catalog.upsert(
        LowerName::from_str(ZONE).unwrap(),
        vec![state.authority.clone()],
    );
    let mut server = ServerFuture::new(catalog);
    server.register_socket(socket);

    tokio::spawn(
        async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }

            server
                .shutdown_gracefully()
                .await
                .expect("stop: failed to shutdown server gracefully");

            debug!("stopped");
        }
        .instrument(debug_span!("dns")),
    );

    tx
}

struct State {
    version: String,
    authority: Arc<InMemoryAuthority>,
    serial: u32,
}

const ZONE: &str = "validator.test.";
const TTL: u32 = 60;

impl State {
    async fn new() -> Self {
        let version = format!("hicory-server-{}", hickory_server::version());

        let authority = Arc::new(InMemoryAuthority::empty(
            Name::from_str(ZONE).unwrap(),
            ZoneType::Primary,
            false,
        ));
        let mut soa = Record::from_rdata(
            Name::from_str(ZONE).unwrap(),
            TTL,
            RData::SOA(SOA::new(
                Name::from_str(ZONE).unwrap(),
                Name::new(),
                0,
                0,
                0,
                0,
                0,
            )),
        );
        soa.set_dns_class(DNSClass::IN);
        authority.upsert(soa, 0).await;

        Self {
            version,
            authority,
            serial: 0,
        }
    }
}

async fn process(msg: Dns, state: &mut State) {
    match msg {
        Dns::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process Dns::Version: failed to send a response");
        }

        Dns::Domain { tx } => {
            tx.send(ZONE[..ZONE.len() - 1].to_string())
                .expect("process Dns::Domain: failed to send a response");
        }

        Dns::Remove { name } => {
            remove(name, state).await;
        }

        Dns::Upsert { name, ip } => {
            upsert(name, ip, state).await;
        }
    }
}

async fn remove(name: String, state: &mut State) {
    state.authority.records_mut().await.remove(&RrKey::new(
        LowerName::from_str(&format!("{name}.{ZONE}")).expect("remove: failed to parse name"),
        RecordType::A,
    ));
}

async fn upsert(name: String, ip: Ipv4Addr, state: &mut State) {
    let name = Name::from_str(&format!("{name}.{ZONE}")).expect("upsert: failed to parse name");

    let serial = state.serial;
    state.serial += 1;

    let octets = ip.octets();
    let mut record = Record::from_rdata(
        name,
        TTL,
        RData::A(A::new(octets[0], octets[1], octets[2], octets[3])),
    );
    record.set_dns_class(DNSClass::IN);

    state.authority.upsert(record, serial).await;
}
