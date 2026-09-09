//! Typed commands and parsing without storage access.

mod collections;
mod fpconv;
mod info;
mod parser;
mod reply;
mod score;
mod sorted_set;
pub use collections::{HashCommand, ListCommand, SetCommand};
pub use info::InfoSections;
pub use score::Score;
pub use sorted_set::SortedSetCommand;

use bytes::Bytes;
use std::time::Duration;

pub use parser::{RequestError, parse};
pub use reply::{ExecutionError, Reply};

/// Existence condition checked by the worker before replacing the value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SetCondition {
    #[default]
    Always,
    Missing,
    Present,
}

/// SET time policy, resolved at execution time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SetExpiry {
    #[default]
    Persistent,
    Keep,
    After(Duration),
}

/// Transport-independent options for a conditional replacement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SetOptions {
    pub condition: SetCondition,
    pub expiry: SetExpiry,
    pub return_previous: bool,
}

/// Unit of a time operation, retained for validation and error reporting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryUnit {
    Seconds,
    Milliseconds,
}

/// Canonical decimal compatible with the Redis integer parser.
pub(crate) fn parse_decimal(value: &[u8]) -> Option<i64> {
    if value == b"0" {
        return Some(0);
    }
    let digits = value.strip_prefix(b"-").unwrap_or(value);
    if !matches!(digits.first(), Some(b'1'..=b'9')) || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

/// Validated command, without channels or transport-protocol knowledge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Sider operational diagnostics, with a limited section selection.
    Info(InfoSections),
    SortedSet {
        key: Bytes,
        operation: SortedSetCommand,
    },
    SetCollection {
        key: Bytes,
        operation: SetCommand,
    },
    /// Operation on binary fields and values in a hash.
    Hash {
        key: Bytes,
        operation: HashCommand,
    },
    /// List operation, including individual pops and index ranges.
    List {
        key: Bytes,
        operation: ListCommand,
    },
    /// Returns PONG or the binary message.
    Ping(Option<Bytes>),
    /// Returns the payload exactly.
    Echo(Bytes),
    /// Reads a key's value.
    Get {
        key: Bytes,
    },
    /// Creates or replaces a persistent value.
    Set {
        key: Bytes,
        value: Bytes,
    },
    /// Applies conditions, expiry, and previous-value return.
    SetWithOptions {
        key: Bytes,
        value: Bytes,
        options: SetOptions,
    },
    /// Removes keys; duplicates are retained to count only actual effects.
    Del {
        keys: Vec<Bytes>,
    },
    /// Counts each occurrence of an existing key.
    Exists {
        keys: Vec<Bytes>,
    },
    /// Increments an i64 decimal integer, creating zero before the operation if absent.
    Incr {
        key: Bytes,
    },
    /// Decrements an i64 decimal integer.
    Decr {
        key: Bytes,
    },
    /// Reads values in key order, retaining duplicates.
    MGet {
        keys: Vec<Bytes>,
    },
    /// Applies an indivisible batch; the last pair for a key prevails.
    MSet {
        entries: Vec<(Bytes, Bytes)>,
    },
    /// Sets relative expiry in milliseconds; a non-positive expiry removes the key.
    Expire {
        key: Bytes,
        value: i64,
        unit: ExpiryUnit,
    },
    /// Returns the remaining expiry or sentinels -1 (persistent) and -2 (missing).
    Ttl {
        key: Bytes,
        milliseconds: bool,
    },
    /// Removes expiry from an existing key.
    Persist {
        key: Bytes,
    },
    /// Subscribes this connection to ephemeral channels, outside storage.
    Subscribe {
        channels: Vec<Bytes>,
    },
    /// Removes subscriptions; an empty list removes all subscriptions for the connection.
    Unsubscribe {
        channels: Vec<Bytes>,
    },
    /// Publishes a binary message without changing the dataset.
    Publish {
        channel: Bytes,
        message: Bytes,
    },
    /// Starts a transaction queue owned by the connection.
    Multi,
    /// Executes the entire queue in its single shard.
    Exec,
    /// Discards the queue and its observations.
    Discard,
    /// Watches keys until EXEC, DISCARD, UNWATCH, or shutdown.
    Watch {
        keys: Vec<Bytes>,
    },
    /// Releases this connection's observations.
    Unwatch,
}

impl Command {
    /// Classification without consulting the dataset; conditions that would fail remain writes.
    pub fn writes_dataset(&self) -> bool {
        match self {
            Self::Set { .. }
            | Self::SetWithOptions { .. }
            | Self::Del { .. }
            | Self::Incr { .. }
            | Self::Decr { .. }
            | Self::MSet { .. }
            | Self::Expire { .. }
            | Self::Persist { .. } => true,
            Self::Hash { operation, .. } => matches!(
                operation,
                HashCommand::Set { .. } | HashCommand::Delete { .. }
            ),
            Self::List { operation, .. } => matches!(
                operation,
                ListCommand::Push { .. } | ListCommand::Pop { .. }
            ),
            Self::SetCollection { operation, .. } => matches!(
                operation,
                SetCommand::Add { .. } | SetCommand::Remove { .. }
            ),
            Self::SortedSet { operation, .. } => matches!(
                operation,
                SortedSetCommand::Add { .. } | SortedSetCommand::Remove { .. }
            ),
            Self::Ping(_)
            | Self::Info(_)
            | Self::Echo(_)
            | Self::Get { .. }
            | Self::Exists { .. }
            | Self::MGet { .. }
            | Self::Ttl { .. }
            | Self::Subscribe { .. }
            | Self::Unsubscribe { .. }
            | Self::Publish { .. }
            | Self::Multi
            | Self::Exec
            | Self::Discard
            | Self::Watch { .. }
            | Self::Unwatch => false,
        }
    }

    /// Visits keys in original order without allocating or confusing values with keys.
    pub fn visit_keys(&self, mut visit: impl FnMut(&Bytes)) {
        match self {
            Self::Ping(_)
            | Self::Info(_)
            | Self::Echo(_)
            | Self::Subscribe { .. }
            | Self::Unsubscribe { .. }
            | Self::Publish { .. } => {}
            Self::Multi | Self::Exec | Self::Discard | Self::Unwatch => {}
            Self::Watch { keys } => keys.iter().for_each(visit),
            Self::Get { key }
            | Self::Hash { key, .. }
            | Self::List { key, .. }
            | Self::SetCollection { key, .. }
            | Self::SortedSet { key, .. }
            | Self::Set { key, .. }
            | Self::SetWithOptions { key, .. }
            | Self::Incr { key }
            | Self::Decr { key }
            | Self::Expire { key, .. }
            | Self::Ttl { key, .. }
            | Self::Persist { key } => visit(key),
            Self::Del { keys } | Self::Exists { keys } | Self::MGet { keys } => {
                keys.iter().for_each(visit)
            }
            Self::MSet { entries } => entries.iter().for_each(|(key, _)| visit(key)),
        }
    }
}
