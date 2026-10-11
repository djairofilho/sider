//! Network identities, credential verification, and default-deny authorization.
//!
//! Policy types deliberately omit `Debug`: verifiers and binary grants must never
//! appear in diagnostics. Parsing errors expose only fixed categories.

mod auth;
mod generation;
mod internal;
mod internal_runtime;
mod policy;
mod profile;
mod runtime;
mod startup;
mod transport;

pub use auth::{Authentication, CredentialProof};
pub use generation::Generation;
pub use internal::{InternalPeer, InternalPhase, authorize_internal_request};
pub use internal_runtime::{InternalLease, InternalRuntime};
pub(crate) use policy::command_name;
pub use policy::{ClientPolicy, InternalPolicy, Role};
pub use profile::{Profile, StartupSecurity};
pub use runtime::{ClientLease, ClientRuntime};
pub use startup::ServerPreflight;
/// One atomic server generation for client and internal admission.
/// `ClientRuntime` remains the compatible name for existing client consumers.
pub type GenerationRuntime = ClientRuntime;
pub(crate) use runtime::{Session, Update};
pub use transport::{ClientTransport, InternalTransport, NetworkStream};

/// Validated client transport and identities shared by one listener.
#[derive(Clone)]
pub struct ClientSecurity {
    pub(crate) policy: std::sync::Arc<ClientPolicy>,
    pub(crate) transport: ClientTransport,
    pub(crate) authentication: Authentication,
    pub(crate) runtime: Option<ClientRuntime>,
}

/// Internal roles and mutual TLS identity validation shared by one instance.
#[derive(Clone)]
pub struct InternalSecurity {
    pub policy: std::sync::Arc<InternalPolicy>,
    pub transport: InternalTransport,
}
impl ClientSecurity {
    pub fn new(policy: ClientPolicy, transport: ClientTransport) -> Self {
        Self {
            policy: std::sync::Arc::new(policy),
            transport,
            authentication: Authentication::default(),
            runtime: None,
        }
    }
}

/// Redacted error suitable for logs and command line diagnostics.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid security policy")]
    Policy,
    #[error("security configuration unavailable")]
    Configuration,
    #[error("protected Windows profile unavailable pending filesystem permission validation")]
    PlatformValidation,
    #[error("protected internal profile unavailable pending generation revocation support")]
    InternalRuntimeUnavailable,
    #[error("authentication failed")]
    Authentication,
    #[error("permission denied")]
    Permission,
    #[error("secure transport unavailable")]
    Transport,
}
