//! Validated server identity and immutable listener configuration before recovery.

use super::{Error, Generation, GenerationRuntime};

/// Typed preflight for one complete live server runtime and exact configuration.
/// In-memory construction is an embedding API, not evidence of filesystem permissions.
/// Executable construction uses `StartupSecurity::preflight` and retains its gates.
pub struct ServerPreflight {
    runtime: GenerationRuntime,
    config: crate::ServerConfig,
}
impl ServerPreflight {
    /// Validates required identity lanes and outgoing policy before database I/O.
    pub fn new(config: &crate::ServerConfig, generation: Generation) -> Result<Self, Error> {
        config.validate().map_err(|_| Error::Configuration)?;
        let internal = config.replication.is_some();
        let outgoing = config
            .replication
            .as_ref()
            .is_some_and(|r| r.upstream.is_some());
        if generation.client.is_none() || generation.internal.is_some() != internal {
            return Err(Error::Configuration);
        }
        if let Some(security) = &generation.internal
            && (security.policy.outgoing() != outgoing || !security.transport.current())
        {
            return Err(Error::Configuration);
        }
        if !generation
            .client
            .as_ref()
            .is_some_and(|client| client.transport.current())
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            runtime: GenerationRuntime::new(generation)?,
            config: config.clone(),
        })
    }
    /// The same runtime supplies client admission, internal effects and reload.
    pub fn runtime(&self) -> GenerationRuntime {
        self.runtime.clone()
    }
    pub(crate) async fn validate(&self, config: &crate::ServerConfig) -> Result<(), Error> {
        if &self.config != config {
            return Err(Error::Configuration);
        }
        self.runtime
            .validate_server(
                config.replication.is_some(),
                config
                    .replication
                    .as_ref()
                    .is_some_and(|r| r.upstream.is_some()),
            )
            .await
    }
}
