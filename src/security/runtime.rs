//! Serialized complete-generation activation and leases retained by accepted operations.

use std::sync::{Arc, Mutex, Weak};

use sha2::{Digest, Sha256};
use tokio::sync::{OwnedRwLockReadGuard, RwLock, mpsc, oneshot};

use super::{ClientPolicy, ClientSecurity, ClientTransport, Error, Generation};

/// Authorization snapshot. Clones retain the same admission through worker effects.
#[derive(Clone)]
pub struct ClientLease {
    pub(crate) policy: Arc<ClientPolicy>,
    pub(crate) transport: ClientTransport,
    _generation: Option<Arc<OwnedRwLockReadGuard<Generation>>>,
    proof: Option<super::CredentialProof>,
}
impl ClientLease {
    pub(crate) fn authenticated(mut self, proof: super::CredentialProof) -> Self {
        self.proof = Some(proof);
        self
    }
    pub(crate) fn admitted_now(&self) -> bool {
        self.transport.current()
            && self
                .proof
                .as_ref()
                .is_none_or(|proof| proof.valid(&self.policy, time::OffsetDateTime::now_utc()))
    }
}

struct State {
    active: Arc<RwLock<Generation>>,
    // Also serializes validation against the currently active generation.
    activation: tokio::sync::Mutex<()>,
    sessions: Mutex<Vec<Weak<Endpoint>>>,
    internal_sessions: Mutex<Vec<Weak<super::internal_runtime::Endpoint>>>,
}

/// Shared client/internal activation. Internal consumers must use `internal()`
/// and retain operation leases through accepted effects before enabling deployment.
#[derive(Clone)]
pub struct ClientRuntime(Arc<State>);

#[derive(Clone, Copy, PartialEq, Eq)]
enum ManifestIdentity {
    Unreadable,
    Oversized,
    Digest([u8; 32]),
}
impl ManifestIdentity {
    fn read(directory: &std::path::Path) -> Self {
        use std::io::Read;
        let path = directory.join("security-manifest.json");
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            return Self::Unreadable;
        };
        if !metadata.is_file() {
            return Self::Unreadable;
        }
        if metadata.len() > 4 * 1024 * 1024 {
            return Self::Oversized;
        }
        let Ok(file) = std::fs::File::open(path) else {
            return Self::Unreadable;
        };
        let mut bytes = Vec::new();
        if file
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .is_err()
        {
            return Self::Unreadable;
        }
        if bytes.len() > 4 * 1024 * 1024 {
            return Self::Oversized;
        }
        Self::Digest(Sha256::digest(bytes).into())
    }
}

pub(crate) struct Update {
    pub policy: Arc<ClientPolicy>,
    pub transport_digest: [u8; 32],
    pub acknowledged: oneshot::Sender<()>,
}
struct Endpoint {
    sender: mpsc::UnboundedSender<Update>,
    close_acknowledgements: Mutex<Vec<oneshot::Sender<()>>>,
}
pub(crate) struct Session {
    endpoint: Arc<Endpoint>,
    pub updates: mpsc::UnboundedReceiver<Update>,
}
impl Session {
    pub fn acknowledge_after_close(&self, acknowledgement: oneshot::Sender<()>) {
        self.endpoint
            .close_acknowledgements
            .lock()
            .expect("security close acknowledgement mutex")
            .push(acknowledgement);
    }
    pub fn acknowledge_cleanup(&self) {
        if let Some(acknowledgement) = self
            .endpoint
            .close_acknowledgements
            .lock()
            .expect("security close acknowledgement mutex")
            .pop()
        {
            let _ = acknowledgement.send(());
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        for acknowledgement in self
            .endpoint
            .close_acknowledgements
            .lock()
            .expect("security close acknowledgement mutex")
            .drain(..)
        {
            let _ = acknowledgement.send(());
        }
    }
}

impl ClientRuntime {
    pub(super) async fn validate_server(
        &self,
        internal: bool,
        outgoing: bool,
    ) -> Result<(), Error> {
        let active = self.0.active.read().await;
        let client = active.client.as_ref().ok_or(Error::Configuration)?;
        if !client.transport.current() || active.internal.is_some() != internal {
            return Err(Error::Configuration);
        }
        if let Some(security) = &active.internal
            && (!security.transport.current() || security.policy.outgoing() != outgoing)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }

    /// Active manifest number; no policy material is exposed by diagnostics.
    pub async fn number(&self) -> u64 {
        self.0.active.read().await.number
    }
    pub fn new(mut generation: Generation) -> Result<Self, Error> {
        if generation.client.is_none() {
            return Err(Error::Configuration);
        }
        generation
            .client
            .as_mut()
            .expect("validated client generation")
            .runtime = None;
        Ok(Self(Arc::new(State {
            active: Arc::new(RwLock::new(generation)),
            activation: tokio::sync::Mutex::new(()),
            sessions: Mutex::new(Vec::new()),
            internal_sessions: Mutex::new(Vec::new()),
        })))
    }
    /// Internal handle always resolves admission and reconnects from the active generation.
    pub async fn internal(&self) -> Option<super::InternalRuntime> {
        self.0.active.read().await.internal.as_ref()?;
        Some(super::InternalRuntime::new(self.clone()))
    }
    pub(super) async fn internal_lease(&self) -> super::InternalLease {
        let generation = Arc::new(self.0.active.clone().read_owned().await);
        super::InternalLease::new(generation)
    }
    pub(super) fn register_internal(&self, endpoint: &Arc<super::internal_runtime::Endpoint>) {
        let mut sessions = self
            .0
            .internal_sessions
            .lock()
            .expect("internal session registry mutex");
        sessions.retain(|entry| entry.strong_count() != 0);
        sessions.push(Arc::downgrade(endpoint));
    }
    pub async fn client(&self) -> ClientSecurity {
        let mut client = self
            .0
            .active
            .read()
            .await
            .client
            .as_ref()
            .expect("validated client generation")
            .clone();
        client.runtime = Some(self.clone());
        client
    }
    async fn lease(&self) -> ClientLease {
        let generation = Arc::new(self.0.active.clone().read_owned().await);
        let client = generation
            .client
            .as_ref()
            .expect("validated client generation");
        ClientLease {
            policy: client.policy.clone(),
            transport: client.transport.clone(),
            _generation: Some(generation),
            proof: None,
        }
    }
    pub(crate) fn register(&self, _lease: &ClientLease) -> Session {
        let (sender, updates) = mpsc::unbounded_channel();
        let endpoint = Arc::new(Endpoint {
            sender,
            close_acknowledgements: Mutex::new(Vec::new()),
        });
        let mut sessions = self
            .0
            .sessions
            .lock()
            .expect("security session registry mutex");
        sessions.retain(|entry| entry.strong_count() != 0);
        sessions.push(Arc::downgrade(&endpoint));
        Session { endpoint, updates }
    }
    /// Success means accepted effects, subscriber writes, cleanup and required
    /// connection closes completed. Invalid generations retain all active state.
    pub async fn activate(&self, mut replacement: Generation) -> Result<bool, Error> {
        let runtime = self.clone();
        // Cancellation of the caller cannot abandon cleanup halfway through activation.
        tokio::spawn(async move { runtime.activate_inner(&mut replacement).await })
            .await
            .map_err(|_| Error::Configuration)?
    }
    async fn activate_inner(&self, replacement: &mut Generation) -> Result<bool, Error> {
        let _serial = self.0.activation.lock().await;
        if replacement.client.is_none() {
            return Err(Error::Configuration);
        }
        let mut active = self.0.active.write().await;
        // Listener lanes are startup configuration; reload cannot silently add or
        // remove a listener's material while handles still exist.
        if replacement.internal.is_some() != active.internal.is_some() {
            return Err(Error::Configuration);
        }
        if !replacement.replacement_of(&active)? {
            return Ok(false);
        }
        // Validation may have completed before an accepted operation delayed the
        // write lock. Expired replacements must retain the old sessions and state.
        if !replacement
            .client
            .as_ref()
            .expect("validated client generation")
            .transport
            .current()
            || replacement
                .internal
                .as_ref()
                .is_some_and(|internal| !internal.transport.current())
        {
            return Err(Error::Transport);
        }
        let client = replacement
            .client
            .as_mut()
            .expect("validated client generation");
        // Retain the server-wide Argon2 pool and reconnect budgets across reloads.
        client.authentication = active
            .client
            .as_ref()
            .expect("validated client generation")
            .authentication
            .clone();
        client.runtime = None;
        let policy = client.policy.clone();
        let transport_digest = client.transport.digest;
        let endpoints: Vec<_> = self
            .0
            .sessions
            .lock()
            .expect("security session registry mutex")
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        let mut acknowledgements = Vec::new();
        if let Some(internal) = &replacement.internal {
            let endpoints: Vec<_> = self
                .0
                .internal_sessions
                .lock()
                .expect("internal session registry mutex")
                .iter()
                .filter_map(Weak::upgrade)
                .collect();
            for endpoint in endpoints {
                // Taking and dropping the owned TLS/TCP stream is the close ACK.
                // It does not depend on an idle consumer polling an update channel.
                endpoint.revalidate(&internal.transport);
            }
        }
        for endpoint in endpoints {
            let (acknowledged, acknowledgement) = oneshot::channel();
            if endpoint
                .sender
                .send(Update {
                    policy: policy.clone(),
                    transport_digest,
                    acknowledged,
                })
                .is_ok()
            {
                acknowledgements.push(acknowledgement);
            }
        }
        for acknowledgement in acknowledgements {
            // A canceled connection has already dropped its local state and stream.
            let _ = acknowledgement.await;
        }
        std::mem::swap(&mut *active, replacement);
        Ok(true)
    }

    /// Serialized manifest polling. An invalid or reused replacement retains the
    /// active generation and is retried on the next one-second tick.
    pub async fn poll(
        &self,
        directory: std::path::PathBuf,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        self.poll_server(directory, false, shutdown).await;
    }
    /// Polls the exact server file set used at startup, including both lanes.
    /// `outgoing` is the immutable replica configuration, not a policy inference.
    pub async fn poll_server(
        &self,
        directory: std::path::PathBuf,
        outgoing: bool,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let internal = self.0.active.read().await.internal.is_some();
        self.poll_with(
            move || {
                let path = directory.clone();
                async move {
                    tokio::task::spawn_blocking(move || {
                        let identity = ManifestIdentity::read(&path);
                        (Generation::server(&path, internal, outgoing), identity)
                    })
                    .await
                    .unwrap_or((Err(Error::Configuration), ManifestIdentity::Unreadable))
                }
            },
            shutdown,
        )
        .await;
    }
    async fn poll_with<F, Load>(
        &self,
        mut load: F,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) where
        F: FnMut() -> Load,
        Load: std::future::Future<Output = (Result<Generation, Error>, ManifestIdentity)>,
    {
        let mut ticks = tokio::time::interval(std::time::Duration::from_secs(1));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_rejected = None;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.wait_for(|stopping| *stopping) => return,
                _ = ticks.tick() => {}
            }
            let (loaded, identity) = load().await;
            let result = match loaded {
                Ok(generation) => self.activate(generation).await,
                Err(_) => Err(Error::Configuration),
            };
            match result {
                Ok(true) => {
                    last_rejected = None;
                    tracing::info!("security generation activated after revocation cleanup")
                }
                Ok(false) => last_rejected = None,
                Err(_) => {
                    if last_rejected != Some(identity) {
                        tracing::warn!(
                            "security replacement rejected; retaining active generation"
                        );
                        last_rejected = Some(identity);
                    }
                }
            }
        }
    }
}

impl ClientSecurity {
    pub(crate) async fn lease(&self) -> ClientLease {
        match &self.runtime {
            Some(runtime) => runtime.lease().await,
            None => ClientLease {
                policy: self.policy.clone(),
                transport: self.transport.clone(),
                _generation: None,
                proof: None,
            },
        }
    }
    pub(crate) async fn accept(
        &self,
        socket: tokio::net::TcpStream,
    ) -> Result<
        (
            Box<dyn super::NetworkStream>,
            ClientSecurity,
            Option<Session>,
        ),
        Error,
    > {
        let lease = self.lease().await;
        let stream = lease.transport.accept(socket).await?;
        let mut accepted = self.clone();
        accepted.policy = lease.policy.clone();
        accepted.transport = lease.transport.clone();
        // Registration shares the handshake's read lease: activation cannot miss
        // an established old-certificate socket between handshake and connection setup.
        let session = self
            .runtime
            .as_ref()
            .map(|runtime| runtime.register(&lease));
        Ok((stream, accepted, session))
    }
    pub(crate) async fn register(&self) -> Option<Session> {
        let lease = self.lease().await;
        self.runtime
            .as_ref()
            .map(|runtime| runtime.register(&lease))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        io::Write,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn security_manifest_identity_tracks_bytes_without_exposing_them() {
        let directory = std::env::temp_dir().join(format!(
            "sider-manifest-identity-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let manifest = directory.join("security-manifest.json");
        assert!(matches!(
            ManifestIdentity::read(&directory),
            ManifestIdentity::Unreadable
        ));
        std::fs::write(&manifest, b"first").unwrap();
        let first = ManifestIdentity::read(&directory);
        std::fs::write(&manifest, b"second").unwrap();
        assert!(first != ManifestIdentity::read(&directory));
        std::fs::write(&manifest, b"first").unwrap();
        assert!(first == ManifestIdentity::read(&directory));
        std::fs::remove_file(manifest).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

    async fn observed(
        runtime: &ClientRuntime,
        calls: &AtomicUsize,
        expected_calls: usize,
        expected_generation: u64,
    ) {
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == expected_calls
                && runtime.number().await == expected_generation
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("polling reached the expected generation and loader invocation count");
    }

    #[tokio::test(start_paused = true)]
    async fn security_poll_retries_invalid_reused_and_higher_generations_each_second() {
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let generation = |number| {
            Generation::client_files(
                number,
                br#"{"version":1,"users":[]}"#,
                certified.cert.pem().as_bytes(),
                certified.key_pair.serialize_pem().as_bytes(),
            )
            .unwrap()
        };
        let runtime = ClientRuntime::new(generation(1)).unwrap();
        let mut replacements = VecDeque::from([
            Err(Error::Configuration),
            Ok(generation(2)),
            Ok(generation(1)),
            Ok(generation(3)),
        ]);
        let calls = Arc::new(AtomicUsize::new(0));
        let invocations = calls.clone();
        let poller = runtime.clone();
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            poller
                .poll_with(
                    move || {
                        invocations.fetch_add(1, Ordering::SeqCst);
                        std::future::ready((
                            replacements
                                .pop_front()
                                .unwrap_or(Err(Error::Configuration)),
                            ManifestIdentity::Unreadable,
                        ))
                    },
                    shutdown,
                )
                .await
        });
        observed(&runtime, &calls, 1, 1).await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        observed(&runtime, &calls, 2, 2).await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        observed(&runtime, &calls, 3, 2).await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        observed(&runtime, &calls, 4, 3).await;
        stop.send_replace(true);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn security_poll_logs_each_invalid_manifest_once_until_change_or_success() {
        use tracing::instrument::WithSubscriber;

        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let generation = Generation::client_files(
            1,
            br#"{"version":1,"users":[]}"#,
            certified.cert.pem().as_bytes(),
            certified.key_pair.serialize_pem().as_bytes(),
        )
        .unwrap();
        let runtime = ClientRuntime::new(generation).unwrap();
        let output = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer({
                let output = output.clone();
                move || CaptureWriter(output.clone())
            })
            .finish();
        let first = ManifestIdentity::Digest([1; 32]);
        let second = ManifestIdentity::Digest([2; 32]);
        let mut replacements = VecDeque::from([
            (Err(Error::Configuration), first),
            (Err(Error::Configuration), first),
            (Err(Error::Configuration), second),
            (Err(Error::Configuration), second),
            (
                Ok(Generation::client_files(
                    2,
                    br#"{"version":1,"users":[]}"#,
                    certified.cert.pem().as_bytes(),
                    certified.key_pair.serialize_pem().as_bytes(),
                )
                .unwrap()),
                first,
            ),
            (Err(Error::Configuration), first),
        ]);
        let calls = Arc::new(AtomicUsize::new(0));
        let invocations = calls.clone();
        let poller = runtime.clone();
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        // A Tokio task may move to another worker thread between polls. Attach
        // the capture subscriber to every poll instead of this test thread only.
        let task = tokio::spawn(
            async move {
                poller
                    .poll_with(
                        move || {
                            invocations.fetch_add(1, Ordering::SeqCst);
                            std::future::ready(replacements.pop_front().unwrap())
                        },
                        shutdown,
                    )
                    .await
            }
            .with_subscriber(subscriber),
        );
        for index in 1..=6 {
            if index > 1 {
                tokio::time::advance(std::time::Duration::from_secs(1)).await;
            }
            observed(&runtime, &calls, index, if index < 5 { 1 } else { 2 }).await;
            let expected_warnings = match index {
                1 | 2 => 1,
                3 | 4 | 5 => 2,
                _ => 3,
            };
            let expected_activations = usize::from(index >= 5);
            let mut completed = false;
            for _ in 0..100 {
                let captured = String::from_utf8(output.lock().unwrap().clone()).unwrap();
                if captured.matches("security replacement rejected").count() == expected_warnings
                    && captured
                        .matches("security generation activated after revocation cleanup")
                        .count()
                        == expected_activations
                {
                    completed = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(completed, "poll {index} did not finish logging");
        }
        stop.send_replace(true);
        task.await.unwrap();
        let captured = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert_eq!(captured.matches("security replacement rejected").count(), 3);
        assert_eq!(
            captured
                .matches("security generation activated after revocation cleanup")
                .count(),
            1
        );
    }
}
