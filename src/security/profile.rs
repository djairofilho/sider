use std::{ffi::OsString, net::SocketAddr, path::PathBuf};

use super::{Error, Generation};

/// Profile selection is explicit and never inferred from addresses or persisted state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Profile {
    #[default]
    Protected,
    LegacyLoopback,
}
/// Paths are configuration, while secret contents are accepted only from restricted files.
pub struct StartupSecurity {
    pub profile: Profile,
    directory: Option<PathBuf>,
}
impl StartupSecurity {
    pub fn directory(&self) -> Option<std::path::PathBuf> {
        self.directory.clone()
    }
    pub fn from_env() -> Result<Self, Error> {
        Self::from_lookup(|name| std::env::var_os(name))
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<OsString>) -> Result<Self, Error> {
        let selected = lookup("SIDER_SECURITY_PROFILE");
        let profile = match selected.as_deref().and_then(|v| v.to_str()) {
            None if selected.is_none() => Profile::Protected,
            Some("protected") => Profile::Protected,
            Some("legacy-loopback") => Profile::LegacyLoopback,
            _ => return Err(Error::Configuration),
        };
        let directory = lookup("SIDER_SECURITY_DIR").map(PathBuf::from);
        match profile {
            Profile::Protected if directory.as_ref().is_none_or(|path| !path.is_absolute()) => {
                return Err(Error::Configuration);
            }
            Profile::LegacyLoopback if directory.is_some() => return Err(Error::Configuration),
            _ => {}
        }
        Ok(Self { profile, directory })
    }
    /// Constructs the typed live-runtime preflight without bypassing deployment gates.
    pub fn preflight(
        &self,
        config: &crate::ServerConfig,
    ) -> Result<Option<super::ServerPreflight>, Error> {
        self.server(config)?
            .map(|generation| super::ServerPreflight::new(config, generation))
            .transpose()
    }
    pub fn server(&self, config: &crate::ServerConfig) -> Result<Option<Generation>, Error> {
        if self.profile == Profile::Protected && config.composed_export_addr.is_some() {
            // The transport-3 export listener has no internal mTLS role admission yet.
            return Err(Error::Configuration);
        }
        if self.profile == Profile::LegacyLoopback {
            self.target(config.bind_addr)?;
            if let Some(replication) = &config.replication {
                self.target(replication.listen)?;
                if let Some(upstream) = replication.upstream {
                    self.target(upstream)?;
                }
            }
            Ok(None)
        } else {
            Generation::server(
                self.directory.as_deref().ok_or(Error::Configuration)?,
                config.replication.is_some(),
                config
                    .replication
                    .as_ref()
                    .is_some_and(|config| config.upstream.is_some()),
            )
            .map(Some)
        }
    }
    pub fn tool(&self, target: SocketAddr) -> Result<Option<Generation>, Error> {
        self.target(target)?;
        if self.profile == Profile::LegacyLoopback {
            Ok(None)
        } else {
            Generation::tool(self.directory.as_deref().ok_or(Error::Configuration)?).map(Some)
        }
    }
    fn target(&self, address: SocketAddr) -> Result<(), Error> {
        if self.profile == Profile::LegacyLoopback && !address.ip().is_loopback() {
            Err(Error::Configuration)
        } else {
            Ok(())
        }
    }
    pub fn diagnostic(&self) -> &'static str {
        match self.profile {
            Profile::Protected => {
                "security_profile:protected\r\ntransport_mode:tls\r\nauthentication_required:1\r\nsecurity_validation:configuration_only\r\n"
            }
            Profile::LegacyLoopback => {
                "security_profile:legacy-loopback\r\ntransport_mode:plaintext\r\nauthentication_required:0\r\nsecurity_validation:configuration_only\r\n"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn security_profiles_default_to_protected_without_fallback() {
        assert!(StartupSecurity::from_lookup(|_| None).is_err());
        for profile in ["", "legacy", "Protected", "secret-canary"] {
            assert!(
                StartupSecurity::from_lookup(
                    |name| (name == "SIDER_SECURITY_PROFILE").then(|| profile.into())
                )
                .is_err()
            );
        }
        let legacy = StartupSecurity::from_lookup(|name| {
            (name == "SIDER_SECURITY_PROFILE").then(|| "legacy-loopback".into())
        })
        .unwrap();
        assert_eq!(legacy.profile, Profile::LegacyLoopback);
        assert!(legacy.tool("192.0.2.1:9123".parse().unwrap()).is_err());
        let config = crate::ServerConfig {
            bind_addr: "0.0.0.0:6379".parse().unwrap(),
            ..crate::ServerConfig::default()
        };
        assert!(legacy.server(&config).is_err());
        let protected = StartupSecurity::from_lookup(|name| match name {
            "SIDER_SECURITY_PROFILE" => Some("protected".into()),
            "SIDER_SECURITY_DIR" => Some(std::env::temp_dir().into_os_string()),
            _ => None,
        })
        .unwrap();
        let export = crate::ServerConfig {
            composed_export_addr: Some("127.0.0.1:0".parse().unwrap()),
            ..crate::ServerConfig::default()
        };
        assert!(protected.server(&export).is_err());
        assert!(
            StartupSecurity::from_lookup(|name| match name {
                "SIDER_SECURITY_PROFILE" => Some("legacy-loopback".into()),
                "SIDER_SECURITY_DIR" => Some("C:\\secret".into()),
                _ => None,
            })
            .is_err()
        );
    }
}
