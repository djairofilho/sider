//! Internal frame direction and established-certificate lifetime boundaries.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Duration, Instant, Sleep};
use tokio_rustls::rustls::pki_types::CertificateDer;

use super::{Error, InternalPolicy, NetworkStream, Role};

/// The next incoming frame allowed by an already admitted internal operation.
/// Reject is a bounded authenticated failure response, never additional authority.
#[derive(Clone, Copy)]
pub enum InternalPhase {
    Hello,
    ReplicaStart,
    BackupStart,
    SnapshotEntry,
    SnapshotEnd,
    ReplicaStream,
    ReplicaAck,
    Status,
    Promoted,
}

impl InternalPhase {
    /// Called after fixed-header validation and before any payload allocation.
    pub fn permits(self, kind: u8) -> bool {
        if kind == 9 && !matches!(self, Self::ReplicaAck) {
            return true;
        }
        match self {
            Self::Hello => kind == 1,
            Self::ReplicaStart => matches!(kind, 2 | 3),
            Self::BackupStart => kind == 3,
            Self::SnapshotEntry => kind == 4,
            Self::SnapshotEnd => kind == 5,
            Self::ReplicaStream => matches!(kind, 6 | 8),
            Self::ReplicaAck => kind == 7,
            Self::Status => kind == 14,
            Self::Promoted => kind == 13,
        }
    }
}

/// Authorizes only initial requests against exact certificate URI and explicit roles.
pub fn authorize_internal_request(policy: &InternalPolicy, uri: &str, kind: u8) -> bool {
    match kind {
        1 => policy.authorize(uri, Role::Replica).is_ok(),
        10 => policy.authorize(uri, Role::Backup).is_ok(),
        11 => {
            policy.authorize(uri, Role::Monitor).is_ok()
                || policy.authorize(uri, Role::Promotion).is_ok()
        }
        12 => policy.authorize(uri, Role::Promotion).is_ok(),
        _ => false,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Validity {
    pub(crate) from: i64,
    pub(crate) until: i64,
}
impl Validity {
    pub(crate) fn intersect(self, other: Self) -> Self {
        Self {
            from: self.from.max(other.from),
            until: self.until.min(other.until),
        }
    }
    pub(crate) fn current(self) -> bool {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        self.from <= now && now < self.until
    }
}

/// Opaque proof retained from a verified handshake; never formatted in diagnostics.
/// It enables a replacement transport to revalidate the actual established chain.
#[derive(Clone)]
pub struct InternalPeer {
    pub(crate) certificates: Vec<CertificateDer<'static>>,
    pub(crate) uri: String,
    pub(crate) address: Option<std::net::SocketAddr>,
    pub(crate) roles: Vec<Role>,
    pub(crate) own_pair: [u8; 32],
    pub(crate) validity: Validity,
    pub(crate) runtime_closed: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}
impl InternalPeer {
    /// Exact verified URI for authorization; callers must not log it.
    pub fn uri(&self) -> &str {
        &self.uri
    }
    pub fn current(&self) -> bool {
        self.validity.current()
            && self
                .runtime_closed
                .as_ref()
                .is_none_or(|closed| !closed.load(std::sync::atomic::Ordering::Acquire))
    }
}

/// Polls expiry even while TLS I/O is pending, without waiting for peer traffic.
pub(crate) struct ExpiringStream {
    stream: Box<dyn NetworkStream>,
    validity: Validity,
    read_wake: Pin<Box<Sleep>>,
    write_wake: Pin<Box<Sleep>>,
}
impl ExpiringStream {
    pub(crate) fn boxed(
        stream: Box<dyn NetworkStream>,
        validity: Validity,
    ) -> Box<dyn NetworkStream> {
        Box::new(Self {
            stream,
            validity,
            read_wake: Box::pin(tokio::time::sleep(Duration::from_secs(1))),
            write_wake: Box::pin(tokio::time::sleep(Duration::from_secs(1))),
        })
    }
    fn admitted(&mut self, cx: &mut Context<'_>, read: bool) -> io::Result<()> {
        if !self.validity.current() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "internal security expired",
            ));
        }
        // A split reader and writer may have different tasks/wakers.
        let wake = if read {
            &mut self.read_wake
        } else {
            &mut self.write_wake
        };
        if wake.as_mut().poll(cx).is_ready() {
            wake.as_mut().reset(Instant::now() + Duration::from_secs(1));
            // Register the new timer before an underlying idle stream returns Pending.
            let _ = wake.as_mut().poll(cx);
        }
        Ok(())
    }
}
impl AsyncRead for ExpiringStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.admitted(cx, true) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for ExpiringStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(error) = self.admitted(cx, false) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(error) = self.admitted(cx, false) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

pub(crate) fn certificate_validity(certs: &[CertificateDer<'_>]) -> Result<Validity, Error> {
    let mut result = Validity {
        from: i64::MIN,
        until: i64::MAX,
    };
    if certs.is_empty() {
        return Err(Error::Configuration);
    }
    for cert in certs {
        let (_, parsed) =
            x509_parser::parse_x509_certificate(cert.as_ref()).map_err(|_| Error::Configuration)?;
        result = result.intersect(Validity {
            from: parsed.validity().not_before.timestamp(),
            until: parsed.validity().not_after.timestamp(),
        });
    }
    Ok(result)
}
