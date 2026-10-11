//! Explicit transport-3 export of an already captured record-1 snapshot.
//! The caller owns capture/sequence consistency; this API does not start a listener.

use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::Cursor;
use super::protocol::{self, ComposedHello, Limits, Message, Reject};
use crate::persistence::format;
use crate::storage::Mutation;

/// Negotiates the exact profile before sending any snapshot payload.
/// Complete record-1 images only; no ACK or incremental subscription.
pub async fn export_snapshot(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    source: ComposedHello,
    cursor: Cursor,
    mutations: &[Mutation],
    limits: Limits,
    deadline: Duration,
) -> Result<(), protocol::Error> {
    if source.base.cursor != Some(cursor) || source.accepts(&source).is_err() {
        return Err(protocol::Error::Invalid("snapshot identity"));
    }
    protocol::encode_composed(&Message::ComposedHello(source.clone()), limits)?;
    let snapshot_limits = Limits {
        record: format::Limits {
            max_record_bytes: limits
                .record
                .max_record_bytes
                .min(source.base.max_record_bytes as usize),
            max_mutations: limits
                .record
                .max_mutations
                .min(source.base.max_mutations as usize),
        },
        ..limits
    };
    let request = protocol::read_composed_authorized(stream, limits, deadline, |kind| kind == 10)
        .await?
        .1;
    let Message::ComposedExport {
        sider_version,
        record_version,
        aof_header_version,
        required_capabilities,
        max_record_bytes,
        max_snapshot_bytes,
    } = request
    else {
        return Err(protocol::Error::Invalid("expected composed export"));
    };
    let rejection = if sider_version != source.base.sider_version
        || record_version != source.base.record_version
        || aof_header_version != source.aof_header_version
        || required_capabilities != source.required_capabilities
    {
        Some(Reject::IncompatibleVersion)
    } else if max_record_bytes < source.base.max_record_bytes
        || max_snapshot_bytes < source.base.max_snapshot_bytes
    {
        Some(Reject::ResourceLimit)
    } else {
        None
    };
    if let Some(reason) = rejection {
        protocol::write_composed(stream, &Message::Reject(reason), limits, deadline).await?;
        stream.shutdown().await?;
        return Ok(());
    }

    // Preflight the full transfer before advertising a valid cut.
    let mut previous = None;
    let mut bytes = 0u64;
    for mutation in mutations {
        if previous.is_some_and(|key| key >= mutation.key()) {
            return Err(protocol::Error::Invalid("snapshot key order"));
        }
        previous = Some(mutation.key());
        let frame =
            protocol::encode_composed(&Message::SnapshotEntry(mutation.clone()), snapshot_limits)?;
        bytes = bytes
            .checked_add(frame.len() as u64)
            .filter(|bytes| *bytes <= source.base.max_snapshot_bytes)
            .ok_or(protocol::Error::Limit)?;
    }
    protocol::write_composed(stream, &Message::ComposedHello(source), limits, deadline).await?;
    protocol::write_composed(
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
            protocol::encode_composed(&Message::SnapshotEntry(mutation.clone()), snapshot_limits)?;
        digest = format::snapshot_digest(digest, &frame);
        tokio::time::timeout(deadline, stream.write_all(&frame))
            .await
            .map_err(|_| protocol::Error::Timeout)??;
    }
    protocol::write_composed(
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
