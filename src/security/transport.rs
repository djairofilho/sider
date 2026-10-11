use std::{io::Cursor, net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::{
    TlsAcceptor, TlsConnector,
    rustls::{
        self, RootCertStore,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    },
};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

use super::internal::{ExpiringStream, InternalPeer, Validity, certificate_validity};
use super::{Error, InternalPolicy};
use rustls::client::danger::ServerCertVerifier;

const HANDSHAKE: Duration = Duration::from_secs(5);

/// One encrypted or explicitly permitted legacy stream for networking code.
pub trait NetworkStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> NetworkStream for T {}

fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, Error> {
    let certs = rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Error::Configuration)?;
    if certs.is_empty() {
        return Err(Error::Configuration);
    }
    for cert in &certs {
        let (_, parsed) =
            parse_x509_certificate(cert.as_ref()).map_err(|_| Error::Configuration)?;
        if !parsed.validity().is_valid() {
            return Err(Error::Configuration);
        }
    }
    Ok(certs)
}
fn private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, Error> {
    rustls_pemfile::private_key(&mut Cursor::new(pem))
        .map_err(|_| Error::Configuration)?
        .ok_or(Error::Configuration)
}
fn no_resumption(config: &mut rustls::ServerConfig) {
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    config.max_early_data_size = 0;
}
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// TLS 1.2/1.3 client listener without client-certificate authentication.
#[derive(Clone)]
pub struct ClientTransport {
    acceptor: TlsAcceptor,
    pub(crate) digest: [u8; 32],
    valid_until: i64,
    valid_from: i64,
}
impl ClientTransport {
    /// Uses already validated generation bytes; does not reopen mutable paths.
    pub fn new(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, Error> {
        let certs = certificates(cert_pem)?;
        let valid_from = certs
            .iter()
            .map(|certificate| {
                parse_x509_certificate(certificate.as_ref())
                    .map(|(_, parsed)| parsed.validity().not_before.timestamp())
                    .map_err(|_| Error::Configuration)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .ok_or(Error::Configuration)?;
        let valid_until = certs
            .iter()
            .map(|certificate| {
                parse_x509_certificate(certificate.as_ref())
                    .map(|(_, parsed)| parsed.validity().not_after.timestamp())
                    .map_err(|_| Error::Configuration)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min()
            .ok_or(Error::Configuration)?;
        let mut config = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::Configuration)?
            .with_no_client_auth()
            .with_single_cert(certs, private_key(key_pem)?)
            .map_err(|_| Error::Configuration)?;
        no_resumption(&mut config);
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            digest: {
                use sha2::{Digest, Sha256};
                Sha256::digest(cert_pem).into()
            },
            valid_until,
            valid_from,
        })
    }
    /// Plaintext and invalid TLS peers fail without any RESP response.
    pub async fn accept(&self, socket: TcpStream) -> Result<Box<dyn NetworkStream>, Error> {
        if !self.current() {
            return Err(Error::Transport);
        }
        let stream = timeout(HANDSHAKE, self.acceptor.accept(socket))
            .await
            .map_err(|_| Error::Transport)?
            .map_err(|_| Error::Transport)?;
        Ok(Box::new(stream))
    }
    pub(crate) fn current(&self) -> bool {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        self.valid_from <= now && now < self.valid_until
    }
}

fn identity(cert: &CertificateDer<'_>) -> Result<String, Error> {
    let (_, parsed) = parse_x509_certificate(cert.as_ref()).map_err(|_| Error::Transport)?;
    let san = parsed
        .subject_alternative_name()
        .map_err(|_| Error::Transport)?
        .ok_or(Error::Transport)?;
    let mut names = san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        });
    let name = names.next().ok_or(Error::Transport)?;
    if names.next().is_some() {
        return Err(Error::Transport);
    }
    Ok(name.to_owned())
}

/// Internal mutual TLS validates trust, revocation, literal IP SAN, and exact URI identity.
#[derive(Clone)]
pub struct InternalTransport {
    acceptor: TlsAcceptor,
    connector: TlsConnector,
    policy: Arc<InternalPolicy>,
    validity: Validity,
    own_pair: [u8; 32],
    client_verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
    server_verifier: Arc<rustls::client::WebPkiServerVerifier>,
}
impl InternalTransport {
    pub fn new(
        cert_pem: &[u8],
        key_pem: &[u8],
        roots_pem: &[u8],
        crls_pem: &[u8],
        policy: Arc<InternalPolicy>,
    ) -> Result<Self, Error> {
        Self::build(cert_pem, key_pem, roots_pem, crls_pem, policy, true)
    }
    /// Internal tools require clientAuth; their executable paths never listen.
    pub fn tool(
        cert_pem: &[u8],
        key_pem: &[u8],
        roots_pem: &[u8],
        crls_pem: &[u8],
        policy: Arc<InternalPolicy>,
    ) -> Result<Self, Error> {
        Self::build(cert_pem, key_pem, roots_pem, crls_pem, policy, false)
    }
    fn build(
        cert_pem: &[u8],
        key_pem: &[u8],
        roots_pem: &[u8],
        crls_pem: &[u8],
        policy: Arc<InternalPolicy>,
        inbound: bool,
    ) -> Result<Self, Error> {
        let certs = certificates(cert_pem)?;
        if !policy.own_identity(&identity(&certs[0]).map_err(|_| Error::Configuration)?) {
            return Err(Error::Configuration);
        }
        let (_, own) =
            parse_x509_certificate(certs[0].as_ref()).map_err(|_| Error::Configuration)?;
        let eku = own
            .extended_key_usage()
            .map_err(|_| Error::Configuration)?
            .ok_or(Error::Configuration)?;
        if inbound && !eku.value.server_auth || !eku.value.client_auth {
            return Err(Error::Configuration);
        }
        let root_certs = certificates(roots_pem)?;
        let mut validity =
            certificate_validity(&certs)?.intersect(certificate_validity(&root_certs)?);
        let mut roots = RootCertStore::empty();
        for cert in root_certs {
            roots.add(cert).map_err(|_| Error::Configuration)?;
        }
        let roots = Arc::new(roots);
        let crls = rustls_pemfile::crls(&mut Cursor::new(crls_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Error::Configuration)?;
        if crls.is_empty() {
            return Err(Error::Configuration);
        }
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        for crl in &crls {
            let (_, parsed) =
                x509_parser::parse_x509_crl(crl.as_ref()).map_err(|_| Error::Configuration)?;
            if parsed.last_update().timestamp() > now
                || parsed
                    .next_update()
                    .is_none_or(|next| next.timestamp() <= now)
            {
                return Err(Error::Configuration);
            }
            validity = validity.intersect(Validity {
                from: parsed.last_update().timestamp(),
                until: parsed
                    .next_update()
                    .ok_or(Error::Configuration)?
                    .timestamp(),
            });
        }
        let client_verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(roots.clone(), provider())
                .with_crls(crls.clone())
                .enforce_revocation_expiration()
                .build()
                .map_err(|_| Error::Configuration)?;
        let mut server = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::Configuration)?
            .with_client_cert_verifier(client_verifier.clone())
            .with_single_cert(certs.clone(), private_key(key_pem)?)
            .map_err(|_| Error::Configuration)?;
        no_resumption(&mut server);
        let server_verifier =
            rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider())
                .with_crls(crls)
                .enforce_revocation_expiration()
                .build()
                .map_err(|_| Error::Configuration)?;
        let mut client = rustls::ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::Configuration)?
            .with_webpki_verifier(server_verifier.clone())
            .with_client_auth_cert(certs, private_key(key_pem)?)
            .map_err(|_| Error::Configuration)?;
        client.resumption = rustls::client::Resumption::disabled();
        client.enable_early_data = false;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            connector: TlsConnector::from(Arc::new(client)),
            policy,
            validity,
            own_pair: {
                use sha2::{Digest, Sha256};
                let mut hash = Sha256::new();
                hash.update((cert_pem.len() as u64).to_le_bytes());
                hash.update(cert_pem);
                hash.update(key_pem);
                hash.finalize().into()
            },
            client_verifier,
            server_verifier,
        })
    }
    pub async fn accept(
        &self,
        socket: TcpStream,
    ) -> Result<(Box<dyn NetworkStream>, String), Error> {
        let (stream, peer) = self.accept_with_peer(socket).await?;
        Ok((stream, peer.uri))
    }
    /// Retains the verified chain and authority needed by generation revocation.
    pub async fn accept_with_peer(
        &self,
        socket: TcpStream,
    ) -> Result<(Box<dyn NetworkStream>, InternalPeer), Error> {
        if !self.validity.current() {
            return Err(Error::Transport);
        }
        socket.set_nodelay(true).map_err(|_| Error::Transport)?;
        let stream = timeout(HANDSHAKE, self.acceptor.accept(socket))
            .await
            .map_err(|_| Error::Transport)?
            .map_err(|_| Error::Transport)?;
        let certs = stream
            .get_ref()
            .1
            .peer_certificates()
            .ok_or(Error::Transport)?
            .to_vec();
        let uri = identity(certs.first().ok_or(Error::Transport)?)?;
        // No role is inferred from a certificate or source address.
        let roles: Vec<_> = [
            super::Role::Replica,
            super::Role::Backup,
            super::Role::Monitor,
            super::Role::Promotion,
        ]
        .into_iter()
        .filter(|role| self.policy.authorize(&uri, *role).is_ok())
        .collect();
        if roles.is_empty() {
            return Err(Error::Permission);
        }
        let validity = self.validity.intersect(certificate_validity(&certs)?);
        if !validity.current() {
            return Err(Error::Transport);
        }
        let peer = InternalPeer {
            certificates: certs,
            uri,
            address: None,
            roles,
            own_pair: self.own_pair,
            validity,
        };
        Ok((ExpiringStream::boxed(Box::new(stream), validity), peer))
    }
    pub async fn connect(&self, address: SocketAddr) -> Result<Box<dyn NetworkStream>, Error> {
        Ok(self.connect_with_peer(address).await?.0)
    }
    /// Outgoing proof includes the exact literal address used for IP SAN validation.
    pub async fn connect_with_peer(
        &self,
        address: SocketAddr,
    ) -> Result<(Box<dyn NetworkStream>, InternalPeer), Error> {
        if !self.validity.current() {
            return Err(Error::Transport);
        }
        let stream = timeout(HANDSHAKE, async {
            let socket = TcpStream::connect(address)
                .await
                .map_err(|_| Error::Transport)?;
            socket.set_nodelay(true).map_err(|_| Error::Transport)?;
            let stream = self
                .connector
                .connect(ServerName::IpAddress(address.ip().into()), socket)
                .await
                .map_err(|_| Error::Transport)?;
            let certs = stream
                .get_ref()
                .1
                .peer_certificates()
                .ok_or(Error::Transport)?
                .to_vec();
            let uri = identity(certs.first().ok_or(Error::Transport)?)?;
            if !self.policy.upstream_identity(&uri) {
                return Err(Error::Permission);
            }
            let validity = self.validity.intersect(certificate_validity(&certs)?);
            if !validity.current() {
                return Err(Error::Transport);
            }
            let peer = InternalPeer {
                certificates: certs,
                uri,
                address: Some(address),
                roles: Vec::new(),
                own_pair: self.own_pair,
                validity,
            };
            Ok::<_, Error>((ExpiringStream::boxed(Box::new(stream), validity), peer))
        })
        .await
        .map_err(|_| Error::Transport)??;
        Ok(stream)
    }
    /// Validates an established peer against replacement material without trusting
    /// the old handshake. This is a component seam, not a live reload barrier.
    pub fn revalidate_peer(&self, peer: &InternalPeer) -> Result<(), Error> {
        if self.own_pair != peer.own_pair || !self.validity.current() || !peer.current() {
            return Err(Error::Transport);
        }
        let (leaf, chain) = peer.certificates.split_first().ok_or(Error::Transport)?;
        if let Some(address) = peer.address {
            if !self.policy.upstream_identity(peer.uri()) {
                return Err(Error::Permission);
            }
            self.server_verifier
                .verify_server_cert(
                    leaf,
                    chain,
                    &ServerName::IpAddress(address.ip().into()),
                    &[],
                    rustls::pki_types::UnixTime::now(),
                )
                .map_err(|_| Error::Transport)?;
        } else {
            if peer
                .roles
                .iter()
                .any(|role| self.policy.authorize(peer.uri(), *role).is_err())
            {
                return Err(Error::Permission);
            }
            self.client_verifier
                .verify_client_cert(leaf, chain, rustls::pki_types::UnixTime::now())
                .map_err(|_| Error::Transport)?;
        }
        Ok(())
    }
}
