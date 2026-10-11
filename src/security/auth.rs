use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordVerifier},
};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::{
    sync::Semaphore,
    time::{Instant, timeout_at},
};

use super::{ClientPolicy, Error};

// Unknown users still perform one Argon2id pass with the contract parameters.
const DUMMY: &str = "$argon2id$v=19$m=65536,t=3,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

/// Proof remains bound to one exact verifier across credential overlap.
#[derive(Clone)]
pub struct CredentialProof {
    pub(super) username: String,
    verifier_digest: [u8; 32],
}
impl CredentialProof {
    #[cfg(test)]
    pub(crate) fn for_test(username: &str, verifier: &str) -> Self {
        Self {
            username: username.into(),
            verifier_digest: Sha256::digest(verifier.as_bytes()).into(),
        }
    }
    /// A removed verifier is explicit revocation, independently of its UTC interval.
    pub(crate) fn present(&self, policy: &ClientPolicy) -> bool {
        policy.users.iter().any(|user| {
            user.name == self.username
                && user.credentials.iter().any(|c| {
                    <[u8; 32]>::from(Sha256::digest(c.verifier.as_bytes())) == self.verifier_digest
                })
        })
    }
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn valid(&self, policy: &ClientPolicy, now: OffsetDateTime) -> bool {
        policy.users.iter().any(|user| {
            user.name == self.username
                && user.credentials.iter().any(|c| {
                    c.valid_at(now)
                        && <[u8; 32]>::from(Sha256::digest(c.verifier.as_bytes()))
                            == self.verifier_digest
                })
        })
    }
}

/// Two running Argon2id jobs and at most 64 waiting jobs, shared by all sessions.
#[derive(Clone)]
pub struct Authentication {
    running: Arc<Semaphore>,
    admitted: Arc<Semaphore>,
    addresses: Arc<Mutex<AddressBudget>>,
}
impl Default for Authentication {
    fn default() -> Self {
        Self {
            running: Arc::new(Semaphore::new(2)),
            admitted: Arc::new(Semaphore::new(66)),
            addresses: Arc::new(Mutex::new(AddressBudget::default())),
        }
    }
}
impl Authentication {
    /// Shared reconnect protection; callers close on an exhausted address budget.
    /// A failure reply is always the same controlled authentication error.
    pub async fn verify_peer(
        &self,
        peer: IpAddr,
        policy: Arc<ClientPolicy>,
        username: &[u8],
        password: &[u8],
        deadline: Instant,
    ) -> Result<CredentialProof, Error> {
        let peer = normalize(peer);
        if !self
            .addresses
            .lock()
            .map_err(|_| Error::Authentication)?
            .available(peer, Instant::now())
        {
            return Err(Error::Authentication);
        }
        let result = self.verify(policy, username, password, deadline).await;
        let mut addresses = self.addresses.lock().map_err(|_| Error::Authentication)?;
        if result.is_ok() {
            addresses.buckets.remove(&peer);
        } else {
            addresses.failure(peer, Instant::now());
        }
        result
    }

    pub fn peer_exhausted(&self, peer: IpAddr) -> bool {
        self.addresses.lock().map_or(true, |mut budget| {
            !budget.available(normalize(peer), Instant::now())
        })
    }

    /// Bounds admission and drops expired queued work before it starts.
    pub async fn verify(
        &self,
        policy: Arc<ClientPolicy>,
        username: &[u8],
        password: &[u8],
        deadline: Instant,
    ) -> Result<CredentialProof, Error> {
        if !(16..=1024).contains(&password.len()) {
            return Err(Error::Authentication);
        }
        let admitted = self
            .admitted
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Authentication)?;
        let running = timeout_at(deadline, self.running.clone().acquire_owned())
            .await
            .map_err(|_| Error::Authentication)?
            .map_err(|_| Error::Authentication)?;
        if Instant::now() >= deadline {
            return Err(Error::Authentication);
        }
        let username = username.to_vec();
        let password = password.to_vec();
        let completed_policy = policy.clone();
        let job = tokio::task::spawn_blocking(move || {
            let (_admitted, _running) = (admitted, running);
            let user = policy.users.iter().find(|u| u.name.as_bytes() == username);
            let now = OffsetDateTime::now_utc();
            let mut proof = None;
            if let Some(user) = user {
                for credential in &user.credentials {
                    let hash = PasswordHash::new(&credential.verifier)
                        .map_err(|_| Error::Authentication)?;
                    let matched = Argon2::default().verify_password(&password, &hash).is_ok();
                    if matched && credential.valid_at(now) {
                        proof = Some(CredentialProof {
                            username: user.name.clone(),
                            verifier_digest: Sha256::digest(credential.verifier.as_bytes()).into(),
                        });
                    }
                }
            } else {
                let hash = PasswordHash::new(DUMMY).map_err(|_| Error::Authentication)?;
                let _ = Argon2::default().verify_password(&password, &hash);
            }
            proof.ok_or(Error::Authentication)
        });
        let proof = timeout_at(deadline, job)
            .await
            .map_err(|_| Error::Authentication)?
            .map_err(|_| Error::Authentication)??;
        if Instant::now() >= deadline || !proof.valid(&completed_policy, OffsetDateTime::now_utc())
        {
            Err(Error::Authentication)
        } else {
            Ok(proof)
        }
    }
}

fn normalize(peer: IpAddr) -> IpAddr {
    match peer {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(peer, IpAddr::V4),
        _ => peer,
    }
}
#[derive(Default)]
struct AddressBudget {
    buckets: HashMap<IpAddr, Bucket>,
}
struct Bucket {
    tokens: u8,
    refill: Instant,
    touched: Instant,
}
impl Bucket {
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.refill).as_secs() / 12;
        if elapsed != 0 {
            self.tokens = (u64::from(self.tokens) + elapsed).min(5) as u8;
            self.refill += Duration::from_secs(elapsed * 12);
        }
    }
}
impl AddressBudget {
    fn available(&mut self, peer: IpAddr, now: Instant) -> bool {
        if let Some(bucket) = self.buckets.get_mut(&peer) {
            bucket.refill(now);
            return bucket.tokens > 0;
        }
        if self.buckets.len() < 65_536 {
            return true;
        }
        let oldest = self
            .buckets
            .iter()
            .filter(|(_, bucket)| {
                now.saturating_duration_since(bucket.touched) >= Duration::from_secs(12)
            })
            .min_by_key(|(_, bucket)| bucket.touched)
            .map(|(peer, _)| *peer);
        if let Some(oldest) = oldest {
            self.buckets.remove(&oldest);
            true
        } else {
            false
        }
    }
    fn failure(&mut self, peer: IpAddr, now: Instant) {
        if !self.available(peer, now) {
            return;
        }
        let bucket = self.buckets.entry(peer).or_insert(Bucket {
            tokens: 5,
            refill: now,
            touched: now,
        });
        bucket.tokens = bucket.tokens.saturating_sub(1);
        bucket.touched = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn security_address_budget_normalizes_and_refills_without_denial_extension() {
        let now = Instant::now();
        let peer: IpAddr = "127.0.0.1".parse().unwrap();
        assert_eq!(normalize("::ffff:127.0.0.1".parse().unwrap()), peer);
        let mut budget = AddressBudget::default();
        for _ in 0..5 {
            budget.failure(peer, now);
        }
        assert!(!budget.available(peer, now + Duration::from_secs(11)));
        assert!(!budget.available(peer, now + Duration::from_secs(11)));
        assert!(budget.available(peer, now + Duration::from_secs(12)));
        budget.failure(peer, now + Duration::from_secs(12));
        assert!(!budget.available(peer, now + Duration::from_secs(23)));
        assert!(budget.available(peer, now + Duration::from_secs(24)));
    }
}
