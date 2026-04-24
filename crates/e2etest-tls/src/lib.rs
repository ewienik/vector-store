/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::frame;
use async_backtrace::framed;
use rcgen::CertifiedKey;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use std::io::Write;
use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub enum Tls {
    /// Returns the filesystem path to the certificate file.
    CertPath { tx: oneshot::Sender<PathBuf> },
    /// Returns the filesystem path to the key file.
    KeyPath { tx: oneshot::Sender<PathBuf> },
    /// Returns the pre-built TLS client configuration.
    ClientTlsConfig {
        tx: oneshot::Sender<Arc<ClientConfig>>,
    },
}

pub trait TlsExt {
    /// Returns the filesystem path to the certificate file.
    fn cert_path(&self) -> impl Future<Output = PathBuf>;

    /// Returns the filesystem path to the key file.
    fn key_path(&self) -> impl Future<Output = PathBuf>;

    /// Returns the pre-built TLS client configuration.
    fn client_tls_config(&self) -> impl Future<Output = Arc<ClientConfig>>;
}

impl TlsExt for mpsc::Sender<Tls> {
    #[framed]
    async fn cert_path(&self) -> PathBuf {
        let (tx, rx) = oneshot::channel();
        self.send(Tls::CertPath { tx })
            .await
            .expect("TlsExt::cert_path: internal actor should receive request");
        rx.await
            .expect("TlsExt::cert_path: internal actor should send response")
    }

    #[framed]
    async fn key_path(&self) -> PathBuf {
        let (tx, rx) = oneshot::channel();
        self.send(Tls::KeyPath { tx })
            .await
            .expect("TlsExt::key_path: internal actor should receive request");
        rx.await
            .expect("TlsExt::key_path: internal actor should send response")
    }

    #[framed]
    async fn client_tls_config(&self) -> Arc<ClientConfig> {
        let (tx, rx) = oneshot::channel();
        self.send(Tls::ClientTlsConfig { tx })
            .await
            .expect("TlsExt::client_tls_config: internal actor should receive request");
        rx.await
            .expect("TlsExt::client_tls_config: internal actor should send response")
    }
}
/// Generates self-signed TLS certificates for the given IPs and starts
/// the TLS actor that serves certificate paths and TLS contexts.
pub async fn new(ips: &[Ipv4Addr]) -> mpsc::Sender<Tls> {
    let (tx, mut rx) = mpsc::channel(10);

    let subject_alt_names: Vec<String> = ips.iter().map(|ip| ip.to_string()).collect();
    let CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(subject_alt_names)
        .expect("failed to generate self-signed certificate");

    let mut cert_file = NamedTempFile::new().expect("failed to create temp cert file");
    cert_file
        .write_all(cert.pem().as_bytes())
        .expect("failed to write certificate");

    let mut key_file = NamedTempFile::new().expect("failed to create temp key file");
    key_file
        .write_all(signing_key.serialize_pem().as_bytes())
        .expect("failed to write key");

    // Make files readable by all users (ScyllaDB may run as a different user)
    std::fs::set_permissions(cert_file.path(), std::fs::Permissions::from_mode(0o644))
        .expect("failed to set cert file permissions");
    std::fs::set_permissions(key_file.path(), std::fs::Permissions::from_mode(0o644))
        .expect("failed to set key file permissions");

    let cert_pem = std::fs::read(cert_file.path()).expect("failed to read certificate file");
    let ca_der = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse certificate PEM");
    let mut root_store = RootCertStore::empty();
    root_store.add_parsable_certificates(ca_der);
    let client_tls_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    );

    tokio::spawn(
        frame!(async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                match msg {
                    Tls::CertPath { tx } => {
                        _ = tx.send(cert_file.path().to_path_buf());
                    }
                    Tls::KeyPath { tx } => {
                        _ = tx.send(key_file.path().to_path_buf());
                    }
                    Tls::ClientTlsConfig { tx } => {
                        _ = tx.send(Arc::clone(&client_tls_config));
                    }
                }
            }

            debug!("stopped");
        })
        .instrument(debug_span!("tls")),
    );

    tx
}
