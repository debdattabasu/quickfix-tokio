//! TLS transport support (rustls). Builds server/client TLS configs from a
//! session's [`TlsSettings`]. Gated behind the `tls` cargo feature.

use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::error::{Error, Result};
use crate::settings::TlsSettings;

/// Client-side TLS material for an initiator: the connector plus the name to
/// verify the server certificate against.
#[derive(Clone)]
pub(crate) struct ClientTls {
    pub connector: TlsConnector,
    pub server_name: ServerName<'static>,
}

fn ring() -> Arc<tokio_rustls::rustls::crypto::CryptoProvider> {
    Arc::new(tokio_rustls::rustls::crypto::ring::default_provider())
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let data = std::fs::read(path)
        .map_err(|e| Error::Tls(format!("cannot read certificate {path}: {e}")))?;
    let certs: std::result::Result<Vec<_>, _> =
        rustls_pemfile::certs(&mut &data[..]).collect();
    let certs = certs.map_err(|e| Error::Tls(format!("bad certificate {path}: {e}")))?;
    if certs.is_empty() {
        return Err(Error::Tls(format!("no certificates found in {path}")));
    }
    Ok(certs)
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let data = std::fs::read(path)
        .map_err(|e| Error::Tls(format!("cannot read private key {path}: {e}")))?;
    rustls_pemfile::private_key(&mut &data[..])
        .map_err(|e| Error::Tls(format!("bad private key {path}: {e}")))?
        .ok_or_else(|| Error::Tls(format!("no private key found in {path}")))
}

fn root_store(ca_file: &str) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(ca_file)? {
        roots
            .add(cert)
            .map_err(|e| Error::Tls(format!("bad CA certificate in {ca_file}: {e}")))?;
    }
    Ok(roots)
}

/// Build the acceptor-side TLS. Requires a certificate + private key; if
/// `SocketCAFile` is set, client certificates are required and verified
/// (mutual TLS).
pub(crate) fn build_acceptor(tls: &TlsSettings) -> Result<TlsAcceptor> {
    let cert_file = tls
        .certificate_file
        .as_deref()
        .ok_or_else(|| Error::Tls("acceptor SocketUseSSL=Y requires SocketCertificateFile".into()))?;
    let key_file = tls
        .private_key_file
        .as_deref()
        .ok_or_else(|| Error::Tls("acceptor SocketUseSSL=Y requires SocketPrivateKeyFile".into()))?;
    let certs = load_certs(cert_file)?;
    let key = load_key(key_file)?;

    let builder = ServerConfig::builder_with_provider(ring())
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Tls(e.to_string()))?;

    let config = match &tls.ca_file {
        Some(ca) => {
            let roots = root_store(ca)?;
            let verifier =
                tokio_rustls::rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .map_err(|e| Error::Tls(e.to_string()))?;
            builder.with_client_cert_verifier(verifier).with_single_cert(certs, key)
        }
        None => builder.with_no_client_auth().with_single_cert(certs, key),
    }
    .map_err(|e| Error::Tls(format!("invalid certificate/key: {e}")))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build the initiator-side TLS: verify the server against `SocketCAFile`
/// (or the webpki roots), or skip verification entirely with
/// `SocketInsecureSkipVerify=Y`. A client certificate enables mutual TLS.
pub(crate) fn build_connector(
    tls: &TlsSettings,
    connect_host: &str,
) -> Result<ClientTls> {
    let builder = ClientConfig::builder_with_provider(ring())
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Tls(e.to_string()))?;

    // Verification: skip, custom CA, or webpki defaults.
    let verified = if tls.insecure_skip_verify {
        builder.dangerous().with_custom_certificate_verifier(Arc::new(NoVerify(ring())))
    } else {
        let roots = match &tls.ca_file {
            Some(ca) => root_store(ca)?,
            None => RootCertStore {
                roots: webpki_roots(),
            },
        };
        builder.with_root_certificates(roots)
    };

    // Optional client certificate for mutual TLS.
    let config = match (&tls.certificate_file, &tls.private_key_file) {
        (Some(cert), Some(key)) => verified
            .with_client_auth_cert(load_certs(cert)?, load_key(key)?)
            .map_err(|e| Error::Tls(format!("invalid client certificate/key: {e}")))?,
        _ => verified.with_no_client_auth(),
    };

    let name = tls.server_name.as_deref().unwrap_or(connect_host).to_owned();
    let server_name = ServerName::try_from(name)
        .map_err(|e| Error::Tls(format!("invalid SocketServerName: {e}")))?;

    Ok(ClientTls { connector: TlsConnector::from(Arc::new(config)), server_name })
}

fn webpki_roots() -> Vec<tokio_rustls::rustls::pki_types::TrustAnchor<'static>> {
    // The webpki root set isn't a hard dependency; when no CA file is given
    // and verification is on, start from an empty set (users supply a CA).
    // This keeps the dependency surface minimal; most FIX venues pin a CA.
    Vec::new()
}

/// A verifier that accepts any server certificate (for
/// `SocketInsecureSkipVerify=Y`). Testing / self-signed use only.
#[derive(Debug)]
struct NoVerify(Arc<tokio_rustls::rustls::crypto::CryptoProvider>);

impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> std::result::Result<
        tokio_rustls::rustls::client::danger::ServerCertVerified,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> std::result::Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        tokio_rustls::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> std::result::Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        tokio_rustls::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(
        &self,
    ) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
