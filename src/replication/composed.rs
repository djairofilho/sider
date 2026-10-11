//! Explicit transport-3 export of a record-1 snapshot.
//! The caller owns capture/sequence consistency; this API does not start a listener.

use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::Cursor;
use super::protocol::{self, ComposedHello, Limits, Message, Reject};
use crate::persistence::format;
use crate::storage::Mutation;

/// A validated request bound to one source profile. Only negotiation can create it.
pub struct ExportAdmission {
    profile: ComposedHello,
    limits: Limits,
    deadline: Duration,
}

/// Validate a standalone export request before capturing a snapshot.
/// Pass a source profile with no cursor; `None` means a rejection was sent.
pub async fn negotiate_export(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    profile: ComposedHello,
    limits: Limits,
    deadline: Duration,
) -> Result<Option<ExportAdmission>, protocol::Error> {
    if profile.base.cursor.is_some() || profile.accepts(&profile).is_err() {
        return Err(protocol::Error::Invalid("export profile"));
    }
    protocol::encode_composed(&Message::ComposedHello(profile.clone()), limits)?;
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
    let rejection = if sider_version != profile.base.sider_version
        || record_version != profile.base.record_version
        || aof_header_version != profile.aof_header_version
        || required_capabilities != profile.required_capabilities
    {
        Some(Reject::IncompatibleVersion)
    } else if max_record_bytes < profile.base.max_record_bytes
        || max_snapshot_bytes < profile.base.max_snapshot_bytes
    {
        Some(Reject::ResourceLimit)
    } else {
        None
    };
    if let Some(reason) = rejection {
        protocol::write_composed(stream, &Message::Reject(reason), limits, deadline).await?;
        stream.shutdown().await?;
        return Ok(None);
    }
    Ok(Some(ExportAdmission {
        profile,
        limits,
        deadline,
    }))
}

impl ExportAdmission {
    /// Send a captured cut of the same profile, with no ACK or subscription.
    pub async fn send_snapshot(
        self,
        stream: &mut (impl AsyncWrite + Unpin),
        source: ComposedHello,
        cursor: Cursor,
        mutations: &[Mutation],
    ) -> Result<(), protocol::Error> {
        let mut captured_profile = source.clone();
        captured_profile.base.cursor = None;
        if source.base.cursor != Some(cursor) || captured_profile != self.profile {
            return Err(protocol::Error::Invalid("snapshot identity"));
        }
        let limits = self.limits;
        let deadline = self.deadline;
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
        // Preflight the full transfer before advertising a valid cut.
        let mut previous = None;
        let mut bytes = 0u64;
        for mutation in mutations {
            if previous.is_some_and(|key| key >= mutation.key()) {
                return Err(protocol::Error::Invalid("snapshot key order"));
            }
            previous = Some(mutation.key());
            let frame = protocol::encode_composed(
                &Message::SnapshotEntry(mutation.clone()),
                snapshot_limits,
            )?;
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
            let frame = protocol::encode_composed(
                &Message::SnapshotEntry(mutation.clone()),
                snapshot_limits,
            )?;
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
}

/// Convenience API for a snapshot already captured by the caller.
/// Production sources should call `negotiate_export` before capturing a cut.
pub async fn export_snapshot(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    source: ComposedHello,
    cursor: Cursor,
    mutations: &[Mutation],
    limits: Limits,
    deadline: Duration,
) -> Result<(), protocol::Error> {
    if source.base.cursor != Some(cursor) {
        return Err(protocol::Error::Invalid("snapshot identity"));
    }
    let mut profile = source.clone();
    profile.base.cursor = None;
    if let Some(admission) = negotiate_export(stream, profile, limits, deadline).await? {
        admission
            .send_snapshot(stream, source, cursor, mutations)
            .await?;
    }
    Ok(())
}
