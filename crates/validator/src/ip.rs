use std::net::Ipv4Addr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub(crate) enum Ip {
    Calculate {
        octet: u8,
        tx: oneshot::Sender<Ipv4Addr>,
    },
}

pub(crate) trait IpExt {
    async fn calculate(&self, octet: u8) -> Ipv4Addr;
}

impl IpExt for mpsc::Sender<Ip> {
    async fn calculate(&self, octet: u8) -> Ipv4Addr {
        let (tx, rx) = oneshot::channel();
        self.send(Ip::Calculate { octet, tx })
            .await
            .expect("IpExt::calculate: internal actor should receive request");
        rx.await
            .expect("IpExt::calculate: internal actor should send response")
    }
}

pub(crate) async fn new(ip: Ipv4Addr) -> mpsc::Sender<Ip> {
    let (tx, mut rx) = mpsc::channel(10);

    let octets = ip.octets();

    tokio::spawn(
        async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, octets).await;
            }

            debug!("stopped");
        }
        .instrument(debug_span!("ip")),
    );

    tx
}

async fn process(msg: Ip, mut octets: [u8; 4]) {
    match msg {
        Ip::Calculate { octet, tx } => {
            octets[3] = octet;
            tx.send(octets.into())
                .expect("process Ip::Calculate: failed to send a response");
        }
    }
}
