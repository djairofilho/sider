//! Explicit transport-2 export of an already captured consistent snapshot.
//! The caller owns capture/sequence consistency; this API does not start a listener.

use super::Cursor;
use super::protocol::{self, Hello, Limits, Message, Reject};
use crate::persistence::{format, memory_format};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// Negotiates before sending any snapshot payload. Complete images only; no ACK/subscription.
pub async fn export_snapshot(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    source: Hello,
    cursor: Cursor,
    mutations: &[memory_format::Mutation],
    limits: Limits,
    deadline: Duration,
) -> Result<(), protocol::Error> {
    if source.record_version != memory_format::RECORD_VERSION || source.cursor != Some(cursor) {
        return Err(protocol::Error::Invalid("snapshot identity"));
    }
    let request = protocol::read_memory_authorized(stream, limits, deadline, |kind| kind == 10)
        .await?
        .1;
    let Message::MemoryExport {
        sider_version,
        record_version,
        max_record_bytes,
        max_snapshot_bytes,
    } = request
    else {
        return Err(protocol::Error::Invalid("expected export"));
    };
    let rejection =
        if sider_version != source.sider_version || record_version != source.record_version {
            Some(Reject::IncompatibleVersion)
        } else if max_record_bytes < source.max_record_bytes
            || max_snapshot_bytes < source.max_snapshot_bytes
        {
            Some(Reject::ResourceLimit)
        } else {
            None
        };
    if let Some(reason) = rejection {
        protocol::write_memory(stream, &Message::Reject(reason), limits, deadline).await?;
        stream.shutdown().await?;
        return Ok(());
    }
    // Preflight ordering and aggregate transfer size before advertising a valid cut.
    let mut previous = None;
    let mut bytes = 0u64;
    for mutation in mutations {
        if previous.is_some_and(|key| key >= mutation.key()) {
            return Err(protocol::Error::Invalid("snapshot key order"));
        }
        previous = Some(mutation.key());
        let frame =
            protocol::encode_memory(&Message::MemorySnapshotEntry(mutation.clone()), limits)?;
        bytes = bytes
            .checked_add(frame.len() as u64)
            .filter(|bytes| *bytes <= source.max_snapshot_bytes)
            .ok_or(protocol::Error::Limit)?;
    }
    protocol::write_memory(stream, &Message::Hello(source), limits, deadline).await?;
    protocol::write_memory(
        stream,
        &Message::FullStart {
            cursor,
            entries: mutations.len() as u64,
        },
        limits,
        deadline,
    )
    .await?;
    let mut digest = 0;
    for mutation in mutations {
        let frame =
            protocol::encode_memory(&Message::MemorySnapshotEntry(mutation.clone()), limits)?;
        digest = format::snapshot_digest(digest, &frame);
        tokio::time::timeout(deadline, stream.write_all(&frame))
            .await
            .map_err(|_| protocol::Error::Timeout)??;
    }
    protocol::write_memory(
        stream,
        &Message::FullEnd {
            cursor,
            entries: mutations.len() as u64,
            digest,
        },
        limits,
        deadline,
    )
    .await?;
    stream.shutdown().await?;
    Ok(())
}
