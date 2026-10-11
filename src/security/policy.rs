use std::collections::BTreeSet;

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use serde::Deserialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::Error;
use crate::command::{Command, InfoSections};

pub(super) const COMMANDS: &[&str] = &[
    "GET",
    "MGET",
    "EXISTS",
    "TTL",
    "PTTL",
    "HGET",
    "HEXISTS",
    "HLEN",
    "HGETALL",
    "LLEN",
    "LRANGE",
    "SISMEMBER",
    "SCARD",
    "SMEMBERS",
    "ZCARD",
    "ZSCORE",
    "ZRANGE",
    "SET",
    "DEL",
    "MSET",
    "INCR",
    "DECR",
    "EXPIRE",
    "PEXPIRE",
    "PERSIST",
    "HSET",
    "HDEL",
    "LPUSH",
    "RPUSH",
    "LPOP",
    "RPOP",
    "SADD",
    "SREM",
    "ZADD",
    "ZREM",
    "PUBLISH",
    "SUBSCRIBE",
    "MULTI",
    "EXEC",
    "DISCARD",
    "UNWATCH",
    "WATCH",
    "INFO",
    "PING",
    "ECHO",
    "MEM.INFO",
    "MEM.GET",
    "MEM.PTTL",
    "MEM.SEARCH",
    "MEM.CREATE",
    "MEM.PUT",
    "MEM.DEL",
    "MEM.EXPIRE",
    "MEM.PERSIST",
];
const WRITES: &[&str] = &[
    "SET",
    "DEL",
    "MSET",
    "INCR",
    "DECR",
    "EXPIRE",
    "PEXPIRE",
    "PERSIST",
    "HSET",
    "HDEL",
    "LPUSH",
    "RPUSH",
    "LPOP",
    "RPOP",
    "SADD",
    "SREM",
    "ZADD",
    "ZREM",
    "MEM.CREATE",
    "MEM.PUT",
    "MEM.DEL",
    "MEM.EXPIRE",
    "MEM.PERSIST",
];
const SECTIONS: &[(&str, u8)] = &[
    ("server", InfoSections::SERVER),
    ("clients", InfoSections::CLIENTS),
    ("stats", InfoSections::STATS),
    ("memory", InfoSections::MEMORY),
    ("persistence", InfoSections::PERSISTENCE),
    ("config", InfoSections::CONFIG),
    ("replication", InfoSections::REPLICATION),
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientPolicy {
    version: u32,
    pub(super) users: Vec<User>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct User {
    pub(super) name: String,
    pub(super) credentials: Vec<Credential>,
    commands: BTreeSet<String>,
    read_keys: Scope,
    write_keys: Scope,
    publish_channels: Scope,
    subscribe_channels: Scope,
    info_sections: BTreeSet<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Credential {
    pub(super) verifier: String,
    not_before: String,
    expires_at: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scope {
    exact: Vec<String>,
    prefix: Vec<String>,
    #[serde(default)]
    all: bool,
}

impl Scope {
    fn validate(&self) -> Result<(), Error> {
        for value in self.exact.iter().chain(&self.prefix) {
            let bytes = STANDARD.decode(value).map_err(|_| Error::Policy)?;
            if STANDARD.encode(bytes) != *value {
                return Err(Error::Policy);
            }
        }
        if self.prefix.iter().any(String::is_empty) {
            return Err(Error::Policy);
        }
        Ok(())
    }
    fn permits(&self, value: &[u8]) -> bool {
        self.all
            || self
                .exact
                .iter()
                .any(|rule| STANDARD.decode(rule).is_ok_and(|v| v == value))
            || self
                .prefix
                .iter()
                .any(|rule| STANDARD.decode(rule).is_ok_and(|v| value.starts_with(&v)))
    }
}

fn timestamp(text: &str) -> Result<OffsetDateTime, Error> {
    let value = OffsetDateTime::parse(text, &Rfc3339).map_err(|_| Error::Policy)?;
    if value.offset() != time::UtcOffset::UTC {
        return Err(Error::Policy);
    }
    Ok(value)
}
impl Credential {
    fn interval(&self) -> Result<(OffsetDateTime, OffsetDateTime), Error> {
        let start = timestamp(&self.not_before)?;
        let end = timestamp(&self.expires_at)?;
        if start >= end {
            return Err(Error::Policy);
        }
        Ok((start, end))
    }
    pub(super) fn valid_at(&self, now: OffsetDateTime) -> bool {
        self.interval()
            .is_ok_and(|(start, end)| start <= now && now < end)
    }
    fn validate(&self) -> Result<(), Error> {
        let tail = self
            .verifier
            .strip_prefix("$argon2id$v=19$m=65536,t=3,p=1$")
            .ok_or(Error::Policy)?;
        let (salt, output) = tail.split_once('$').ok_or(Error::Policy)?;
        for (value, size) in [(salt, 16), (output, 32)] {
            let decoded = STANDARD_NO_PAD.decode(value).map_err(|_| Error::Policy)?;
            if decoded.len() != size || STANDARD_NO_PAD.encode(decoded) != value {
                return Err(Error::Policy);
            }
        }
        argon2::password_hash::PasswordHash::new(&self.verifier).map_err(|_| Error::Policy)?;
        self.interval()?;
        Ok(())
    }
}

fn component(value: &str, max: usize, uppercase: bool) -> bool {
    let allowed = |byte: u8| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || uppercase && byte.is_ascii_uppercase()
    };
    !value.is_empty()
        && value.len() <= max
        && value.as_bytes().first().is_some_and(|b| allowed(*b))
        && value.bytes().all(|b| allowed(b) || b"._-".contains(&b))
}

impl ClientPolicy {
    /// Loads a complete version-1 policy, rejecting unknown fields and ambiguous grants.
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(Error::Policy);
        }
        let policy: Self = serde_json::from_slice(bytes).map_err(|_| Error::Policy)?;
        if policy.version != 1 {
            return Err(Error::Policy);
        }
        let mut names = BTreeSet::new();
        for user in &policy.users {
            if !component(&user.name, 64, true)
                || !names.insert(&user.name)
                || !(1..=2).contains(&user.credentials.len())
                || user
                    .commands
                    .iter()
                    .any(|name| !COMMANDS.contains(&name.as_str()))
                || user
                    .info_sections
                    .iter()
                    .any(|name| !SECTIONS.iter().any(|(s, _)| s == name))
            {
                return Err(Error::Policy);
            }
            for credential in &user.credentials {
                credential.validate()?;
            }
            if user.credentials.len() == 2 {
                let (a, b) = user.credentials[0].interval()?;
                let (c, d) = user.credentials[1].interval()?;
                if b.min(d) - a.max(c) > time::Duration::hours(24)
                    || user.credentials[0].verifier == user.credentials[1].verifier
                {
                    return Err(Error::Policy);
                }
            }
            for scope in [
                &user.read_keys,
                &user.write_keys,
                &user.publish_channels,
                &user.subscribe_channels,
            ] {
                scope.validate()?;
            }
        }
        Ok(policy)
    }

    /// Authorizes a fully parsed command; values and fields are never treated as keys.
    pub fn authorize(&self, username: &str, name: &[u8], command: &Command) -> Result<(), Error> {
        if !name.eq_ignore_ascii_case(command_name(command).as_bytes()) {
            return Err(Error::Permission);
        }
        let user = self
            .users
            .iter()
            .find(|user| user.name == username)
            .ok_or(Error::Permission)?;
        if matches!(command, Command::Unsubscribe { .. }) {
            return Ok(());
        }
        let name = std::str::from_utf8(name)
            .map_err(|_| Error::Permission)?
            .to_ascii_uppercase();
        if !COMMANDS.contains(&name.as_str()) || !user.commands.contains(&name) {
            return Err(Error::Permission);
        }
        let scope = if WRITES.contains(&name.as_str()) {
            &user.write_keys
        } else {
            &user.read_keys
        };
        let mut keys_allowed = true;
        command.visit_keys(|key| keys_allowed &= scope.permits(key));
        if !keys_allowed {
            return Err(Error::Permission);
        }
        let allowed = match command {
            Command::Publish { channel, .. } => user.publish_channels.permits(channel),
            Command::Subscribe { channels } => channels
                .iter()
                .all(|channel| user.subscribe_channels.permits(channel)),
            Command::Info(sections) => SECTIONS
                .iter()
                .all(|(name, bit)| !sections.contains(*bit) || user.info_sections.contains(*name)),
            _ => true,
        };
        if allowed {
            Ok(())
        } else {
            Err(Error::Permission)
        }
    }
}

/// Derive grants from the typed operation, including rechecks of queued commands.
pub(crate) fn command_name(command: &Command) -> &'static str {
    use crate::command::{ExpiryUnit, HashCommand, ListCommand, SetCommand, SortedSetCommand};
    match command {
        Command::Memory { operation, .. } => operation.command_name(),
        Command::Info(_) => "INFO",
        Command::Ping(_) => "PING",
        Command::Echo(_) => "ECHO",
        Command::Get { .. } => "GET",
        Command::Set { .. } | Command::SetWithOptions { .. } => "SET",
        Command::Del { .. } => "DEL",
        Command::Exists { .. } => "EXISTS",
        Command::Incr { .. } => "INCR",
        Command::Decr { .. } => "DECR",
        Command::MGet { .. } => "MGET",
        Command::MSet { .. } => "MSET",
        Command::Expire {
            unit: ExpiryUnit::Seconds,
            ..
        } => "EXPIRE",
        Command::Expire {
            unit: ExpiryUnit::Milliseconds,
            ..
        } => "PEXPIRE",
        Command::Ttl {
            milliseconds: false,
            ..
        } => "TTL",
        Command::Ttl {
            milliseconds: true, ..
        } => "PTTL",
        Command::Persist { .. } => "PERSIST",
        Command::Subscribe { .. } => "SUBSCRIBE",
        Command::Unsubscribe { .. } => "UNSUBSCRIBE",
        Command::Publish { .. } => "PUBLISH",
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::Watch { .. } => "WATCH",
        Command::Unwatch => "UNWATCH",
        Command::Hash { operation, .. } => match operation {
            HashCommand::Set { .. } => "HSET",
            HashCommand::Get { .. } => "HGET",
            HashCommand::Delete { .. } => "HDEL",
            HashCommand::Exists { .. } => "HEXISTS",
            HashCommand::Len => "HLEN",
            HashCommand::GetAll => "HGETALL",
        },
        Command::List { operation, .. } => match operation {
            ListCommand::Push { left: true, .. } => "LPUSH",
            ListCommand::Push { left: false, .. } => "RPUSH",
            ListCommand::Pop { left: true } => "LPOP",
            ListCommand::Pop { left: false } => "RPOP",
            ListCommand::Len => "LLEN",
            ListCommand::Range { .. } => "LRANGE",
        },
        Command::SetCollection { operation, .. } => match operation {
            SetCommand::Add { .. } => "SADD",
            SetCommand::Remove { .. } => "SREM",
            SetCommand::IsMember { .. } => "SISMEMBER",
            SetCommand::Card => "SCARD",
            SetCommand::Members => "SMEMBERS",
        },
        Command::SortedSet { operation, .. } => match operation {
            SortedSetCommand::Add { .. } => "ZADD",
            SortedSetCommand::Remove { .. } => "ZREM",
            SortedSetCommand::Card => "ZCARD",
            SortedSetCommand::Score { .. } => "ZSCORE",
            SortedSetCommand::Range { .. } => "ZRANGE",
        },
    }
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Replica,
    Backup,
    Monitor,
    Promotion,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InternalPolicy {
    version: u32,
    instance_id: String,
    principals: Vec<Principal>,
    upstream_principal: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Principal {
    uri_san: String,
    roles: Vec<Role>,
    upstream_instance: Option<String>,
}

fn uri(value: &str) -> Option<(&str, &str)> {
    let (instance, principal) = value.strip_prefix("spiffe://sider/")?.split_once('/')?;
    (component(instance, 63, false) && component(principal, 63, false))
        .then_some((instance, principal))
}
impl InternalPolicy {
    pub fn parse(bytes: &[u8], outgoing: bool) -> Result<Self, Error> {
        let policy: Self = serde_json::from_slice(bytes).map_err(|_| Error::Policy)?;
        if policy.version != 1
            || !component(&policy.instance_id, 63, false)
            || outgoing != policy.upstream_principal.is_some()
            || policy
                .upstream_principal
                .as_ref()
                .is_some_and(|value| uri(value).is_none())
        {
            return Err(Error::Policy);
        }
        let mut names = BTreeSet::new();
        for principal in &policy.principals {
            if uri(&principal.uri_san).is_none()
                || !names.insert(&principal.uri_san)
                || principal.roles.is_empty()
                || principal
                    .roles
                    .iter()
                    .enumerate()
                    .any(|(index, role)| principal.roles[..index].contains(role))
                || principal.roles.contains(&Role::Replica) != principal.upstream_instance.is_some()
                || principal
                    .upstream_instance
                    .as_ref()
                    .is_some_and(|value| value != &policy.instance_id)
            {
                return Err(Error::Policy);
            }
        }
        Ok(policy)
    }
    pub fn authorize(&self, uri_san: &str, role: Role) -> Result<(), Error> {
        if self
            .principals
            .iter()
            .any(|p| p.uri_san == uri_san && p.roles.contains(&role))
        {
            Ok(())
        } else {
            Err(Error::Permission)
        }
    }
    pub fn own_identity(&self, uri_san: &str) -> bool {
        uri(uri_san).is_some_and(|(id, _)| id == self.instance_id)
    }
    pub(super) fn outgoing(&self) -> bool {
        self.upstream_principal.is_some()
    }
    pub fn upstream_identity(&self, uri_san: &str) -> bool {
        self.upstream_principal.as_deref() == Some(uri_san)
    }
}
