//! Internal sockets owned by the generation registry, with short admission leases.

use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::OwnedRwLockReadGuard,
};

use super::{
    Error, Generation, GenerationRuntime, InternalPeer, InternalPolicy, InternalTransport,
    NetworkStream, authorize_internal_request,
};

/// Short read lease for one internal frame or complete accepted operation.
/// An owning worker must retain a clone when caller cancellation cannot cancel effects.
#[derive(Clone)]
pub struct InternalLease {
    generation: Arc<OwnedRwLockReadGuard<Generation>>,
    proof: Option<(InternalPeer, Option<u8>)>,
}
impl InternalLease {
    pub(super) fn new(generation: Arc<OwnedRwLockReadGuard<Generation>>) -> Self {
        Self {
            generation,
            proof: None,
        }
    }
    fn transport(&self) -> &InternalTransport {
        &self
            .generation
            .internal
            .as_ref()
            .expect("validated internal generation")
            .transport
    }
    /// Policy from the same generation as the read lease and transport.
    pub fn policy(&self) -> &InternalPolicy {
        &self
            .generation
            .internal
            .as_ref()
            .expect("validated internal generation")
            .policy
    }
    /// Revalidates the actual certificate chain and original roles at final admission.
    pub fn authorize_request(&self, peer: &InternalPeer, kind: u8) -> Result<(), Error> {
        self.transport().revalidate_peer(peer)?;
        if !authorize_internal_request(self.policy(), peer.uri(), kind) {
            return Err(Error::Permission);
        }
        Ok(())
    }
    /// Current trust, roles and expiry check for an already admitted frame direction.
    pub fn validate_peer(&self, peer: &InternalPeer) -> Result<(), Error> {
        self.transport().revalidate_peer(peer)
    }
    /// Binds final incoming operation admission to its initial request authority.
    pub fn for_request(mut self, peer: &InternalPeer, kind: u8) -> Result<Self, Error> {
        self.authorize_request(peer, kind)?;
        self.proof = Some((peer.clone(), Some(kind)));
        Ok(self)
    }
    /// Binds an outgoing replica operation to the exact verified upstream.
    pub fn for_upstream(mut self, peer: &InternalPeer) -> Result<Self, Error> {
        if peer.address.is_none() {
            return Err(Error::Permission);
        }
        self.validate_peer(peer)?;
        self.proof = Some((peer.clone(), None));
        Ok(self)
    }
    pub(crate) fn admitted_now(&self) -> bool {
        self.proof.as_ref().is_some_and(|(peer, kind)| {
            self.validate_peer(peer).is_ok()
                && kind
                    .is_none_or(|kind| authorize_internal_request(self.policy(), peer.uri(), kind))
        })
    }
}

/// Internal admission handle for a complete server generation.
/// Static `InternalTransport` APIs do not participate in this registry.
#[derive(Clone)]
pub struct InternalRuntime(GenerationRuntime);
impl InternalRuntime {
    pub(super) fn new(runtime: GenerationRuntime) -> Self {
        Self(runtime)
    }
    /// Acquire immediately before frame authorization or final operation admission;
    /// release while idle. This API does not attach leases to database requests.
    pub async fn lease(&self) -> InternalLease {
        self.0.internal_lease().await
    }
    pub async fn accept_with_peer(
        &self,
        socket: tokio::net::TcpStream,
    ) -> Result<(Box<dyn NetworkStream>, InternalPeer), Error> {
        let lease = self.lease().await;
        let (stream, peer) = lease.transport().accept_with_peer(socket).await?;
        Ok(self.register(stream, peer, &lease))
    }
    pub async fn connect_with_peer(
        &self,
        address: std::net::SocketAddr,
    ) -> Result<(Box<dyn NetworkStream>, InternalPeer), Error> {
        let lease = self.lease().await;
        let (stream, peer) = lease.transport().connect_with_peer(address).await?;
        Ok(self.register(stream, peer, &lease))
    }
    fn register(
        &self,
        stream: Box<dyn NetworkStream>,
        mut peer: InternalPeer,
        _lease: &InternalLease,
    ) -> (Box<dyn NetworkStream>, InternalPeer) {
        // Caller still owns the handshake read lease until registration returns.
        peer.runtime_closed = Some(Arc::new(std::sync::atomic::AtomicBool::new(false)));
        let endpoint = Arc::new(Endpoint {
            peer: peer.clone(),
            state: Mutex::new(SocketState {
                stream: Some(stream),
                read: None,
                write: None,
            }),
        });
        self.0.register_internal(&endpoint);
        let weak = Arc::downgrade(&endpoint);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let Some(endpoint) = weak.upgrade() else {
                    return;
                };
                if !endpoint.peer.current() {
                    endpoint.close();
                    return;
                }
                if endpoint
                    .state
                    .lock()
                    .expect("internal socket mutex")
                    .stream
                    .is_none()
                {
                    return;
                }
                // Drop this temporary strong reference before the next timer wait.
            }
        });
        (Box::new(RegisteredStream(endpoint)), peer)
    }
}

struct SocketState {
    stream: Option<Box<dyn NetworkStream>>,
    read: Option<Waker>,
    write: Option<Waker>,
}
pub(super) struct Endpoint {
    peer: InternalPeer,
    state: Mutex<SocketState>,
}
impl Endpoint {
    pub(super) fn revalidate(&self, transport: &InternalTransport) {
        if transport.revalidate_peer(&self.peer).is_err() {
            self.close();
        }
    }
    fn close(&self) {
        let (read, write) = {
            let mut state = self.state.lock().expect("internal socket mutex");
            // Concurrent expiry/drop/reload callers share this completion barrier.
            // A second close cannot acknowledge while the first still owns TCP.
            drop(state.stream.take());
            self.peer
                .runtime_closed
                .as_ref()
                .expect("registered close flag")
                .store(true, std::sync::atomic::Ordering::Release);
            (state.read.take(), state.write.take())
        };
        // Physical close precedes notification and generation activation success.
        // Never run an arbitrary waker while holding the socket mutex.
        if let Some(waker) = read {
            waker.wake();
        }
        if let Some(waker) = write {
            waker.wake();
        }
    }
    fn state(&self) -> std::sync::MutexGuard<'_, SocketState> {
        if !self.peer.current() {
            self.close();
        }
        self.state.lock().expect("internal socket mutex")
    }
}

struct RegisteredStream(Arc<Endpoint>);
impl Drop for RegisteredStream {
    fn drop(&mut self) {
        self.0.close();
    }
}
fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "internal security session closed",
    )
}
impl AsyncRead for RegisteredStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.0.state();
        state.read = Some(cx.waker().clone());
        match &mut state.stream {
            Some(stream) => Pin::new(stream).poll_read(cx, buf),
            None => Poll::Ready(Err(closed())),
        }
    }
}
impl AsyncWrite for RegisteredStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.0.state();
        state.write = Some(cx.waker().clone());
        match &mut state.stream {
            Some(stream) => Pin::new(stream).poll_write(cx, buf),
            None => Poll::Ready(Err(closed())),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.0.state();
        state.write = Some(cx.waker().clone());
        match &mut state.stream {
            Some(stream) => Pin::new(stream).poll_flush(cx),
            None => Poll::Ready(Err(closed())),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.0.state.lock().expect("internal socket mutex");
        match &mut state.stream {
            Some(stream) => Pin::new(stream).poll_shutdown(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}
