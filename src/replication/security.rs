//! Static compatibility and live generation admission for internal consumers.

use std::{sync::Arc, time::Duration};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::security::{
    InternalLease, InternalPeer, InternalPhase, InternalPolicy, InternalRuntime, InternalSecurity,
    NetworkStream,
};

use super::{
    Error,
    protocol::{self, Message},
};

#[derive(Clone)]
pub(super) enum Access {
    Static(InternalSecurity),
    Live(InternalRuntime),
}
pub(super) struct Authority {
    peer: InternalPeer,
    policy: Option<Arc<InternalPolicy>>,
    runtime: Option<InternalRuntime>,
    request: Option<u8>,
}
impl Access {
    pub(super) async fn accept(
        &self,
        socket: tokio::net::TcpStream,
    ) -> Result<(Box<dyn NetworkStream>, Authority), Error> {
        let (stream, peer, policy, runtime) = match self {
            Self::Static(security) => {
                let (stream, peer) = security.transport.accept_with_peer(socket).await?;
                (stream, peer, Some(security.policy.clone()), None)
            }
            Self::Live(runtime) => {
                let (stream, peer) = runtime.accept_with_peer(socket).await?;
                (stream, peer, None, Some(runtime.clone()))
            }
        };
        Ok((
            stream,
            Authority {
                peer,
                policy,
                runtime,
                request: None,
            },
        ))
    }
    pub(super) async fn connect(
        &self,
        address: std::net::SocketAddr,
    ) -> Result<(Box<dyn NetworkStream>, Authority), Error> {
        let (stream, peer, runtime) = match self {
            Self::Static(security) => {
                let (stream, peer) = security.transport.connect_with_peer(address).await?;
                (stream, peer, None)
            }
            Self::Live(runtime) => {
                let (stream, peer) = runtime.connect_with_peer(address).await?;
                (stream, peer, Some(runtime.clone()))
            }
        };
        Ok((
            stream,
            Authority {
                peer,
                policy: None,
                runtime,
                request: None,
            },
        ))
    }
}
impl Authority {
    pub(super) fn set_request(&mut self, message: &Message) {
        self.request = Some(match message {
            Message::Hello(_) => 1,
            Message::Export { .. } | Message::MemoryExport { .. } => 10,
            Message::StatusRequest => 11,
            Message::Promote => 12,
            _ => 0,
        });
    }
    async fn admit(&self, request: Option<u8>) -> Result<Option<InternalLease>, Error> {
        if let Some(runtime) = &self.runtime {
            let lease = runtime.lease().await;
            let lease = if let Some(kind) = request {
                lease.for_request(&self.peer, kind)?
            } else {
                lease.for_upstream(&self.peer)?
            };
            return Ok(Some(lease));
        }
        if !self.peer.current()
            || request.is_some_and(|kind| {
                self.policy.as_ref().is_none_or(|policy| {
                    !crate::security::authorize_internal_request(policy, self.peer.uri(), kind)
                })
            })
        {
            return Err(crate::security::Error::Permission.into());
        }
        Ok(None)
    }
    pub(super) async fn operation(
        &self,
        deadline: Duration,
    ) -> Result<Option<InternalLease>, Error> {
        tokio::time::timeout(deadline, self.admit(self.request))
            .await
            .map_err(|_| protocol::Error::Timeout)?
    }
}

pub(super) async fn read_phase(
    socket: &mut (impl AsyncRead + Unpin),
    limits: protocol::Limits,
    deadline: Duration,
    phase: InternalPhase,
    authority: Option<&Authority>,
) -> Result<(bytes::Bytes, Message), Error> {
    let (frame, message, _lease) = read(socket, limits, deadline, authority, Some(phase)).await?;
    Ok((frame, message))
}

/// Lease acquisition occurs only after header/direction validation. No read lease
/// is retained while waiting for a peer to send the next frame header.
pub(super) async fn read(
    socket: &mut (impl AsyncRead + Unpin),
    limits: protocol::Limits,
    deadline: Duration,
    authority: Option<&Authority>,
    phase: Option<InternalPhase>,
) -> Result<(bytes::Bytes, Message, Option<InternalLease>), Error> {
    Ok(
        protocol::read_admitted(socket, limits, deadline, |kind| async move {
            if phase.is_some_and(|phase| !phase.permits(kind)) {
                return Err(protocol::Error::Invalid("permission denied"));
            }
            let Some(authority) = authority else {
                return Ok(None);
            };
            let request = if phase.is_none() {
                Some(kind)
            } else {
                authority.request
            };
            authority
                .admit(request)
                .await
                .map_err(|_| protocol::Error::Invalid("permission denied"))
        })
        .await?,
    )
}

pub(super) async fn read_memory(
    socket: &mut (impl AsyncRead + Unpin),
    limits: protocol::Limits,
    deadline: Duration,
    authority: Option<&Authority>,
) -> Result<(bytes::Bytes, Message, Option<InternalLease>), Error> {
    Ok(
        protocol::read_memory_admitted(socket, limits, deadline, |kind| async move {
            if !matches!(kind, 1 | 10 | 11 | 12) {
                return Err(protocol::Error::Invalid("permission denied"));
            }
            let Some(authority) = authority else {
                return Ok(None);
            };
            authority
                .admit(Some(kind))
                .await
                .map_err(|_| protocol::Error::Invalid("permission denied"))
        })
        .await?,
    )
}

pub(super) async fn read_memory_phase(
    socket: &mut (impl AsyncRead + Unpin),
    limits: protocol::Limits,
    deadline: Duration,
    phase: InternalPhase,
    authority: Option<&Authority>,
) -> Result<(bytes::Bytes, Message), Error> {
    let (frame, message, _lease) =
        protocol::read_memory_admitted(socket, limits, deadline, |kind| async move {
            if !phase.permits(kind) {
                return Err(protocol::Error::Invalid("permission denied"));
            }
            let Some(authority) = authority else {
                return Ok(None);
            };
            authority
                .admit(authority.request)
                .await
                .map_err(|_| protocol::Error::Invalid("permission denied"))
        })
        .await?;
    Ok((frame, message))
}

pub(super) async fn write(
    socket: &mut (impl AsyncWrite + Unpin),
    message: &Message,
    limits: protocol::Limits,
    deadline: Duration,
    authority: Option<&Authority>,
) -> Result<(), Error> {
    tokio::time::timeout(deadline, async {
        let _lease = match authority {
            Some(authority) => authority.operation(deadline).await?,
            None => None,
        };
        protocol::write(socket, message, limits, deadline).await?;
        Ok(())
    })
    .await
    .map_err(|_| protocol::Error::Timeout)?
}

pub(super) async fn write_memory(
    socket: &mut (impl AsyncWrite + Unpin),
    message: &Message,
    limits: protocol::Limits,
    deadline: Duration,
    authority: Option<&Authority>,
) -> Result<(), Error> {
    tokio::time::timeout(deadline, async {
        let _lease = match authority {
            Some(authority) => authority.operation(deadline).await?,
            None => None,
        };
        protocol::write_memory(socket, message, limits, deadline).await?;
        Ok(())
    })
    .await
    .map_err(|_| protocol::Error::Timeout)?
}
pub(super) async fn write_frame(
    socket: &mut (impl AsyncWrite + Unpin),
    frame: &[u8],
    deadline: Duration,
    authority: Option<&Authority>,
) -> Result<(), Error> {
    use tokio::io::AsyncWriteExt;
    tokio::time::timeout(deadline, async {
        let _lease = match authority {
            Some(authority) => authority.operation(deadline).await?,
            None => None,
        };
        socket.write_all(frame).await?;
        socket.flush().await?;
        Ok(())
    })
    .await
    .map_err(|_| protocol::Error::Timeout)?
}
