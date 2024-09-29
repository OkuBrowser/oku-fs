use crate::fs::FS_PATH;
use log::error;
use miette::{miette, IntoDiagnostic};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Configuration of an Oku file system node.
pub struct OkuFsConfig {
    /// The configuration of an Oku file system node's connection to a relay node.
    relay_connection_config: Arc<Mutex<Option<OkuFsRelayConnectionConfig>>>,
}

impl OkuFsConfig {
    /// Loads the configuration of the file system from disk, or creates a new configuration if none exists.
    ///
    /// # Returns
    ///
    /// The configuration of the file system.
    pub fn load_or_create_config() -> miette::Result<Self> {
        let path = PathBuf::from(FS_PATH).join("config.toml");
        let config_file_contents = std::fs::read_to_string(path.clone());
        match config_file_contents {
            Ok(config_file_toml) => match toml::from_str(&config_file_toml) {
                Ok(config) => Ok(config),
                Err(e) => {
                    error!("{}", e);
                    let config = Self {
                        relay_connection_config: Arc::new(Mutex::new(None)),
                    };
                    Ok(config)
                }
            },
            Err(e) => {
                error!("{}", e);
                let config = Self {
                    relay_connection_config: Arc::new(Mutex::new(None)),
                };
                let config_toml = toml::to_string(&config).into_diagnostic()?;
                std::fs::write(path, config_toml).into_diagnostic()?;
                Ok(config)
            }
        }
    }

    /// The configuration of an Oku file system node's connection to a relay node.
    pub fn relay_connection_config(&self) -> miette::Result<Option<OkuFsRelayConnectionConfig>> {
        Ok(self
            .relay_connection_config
            .try_lock()
            .map_err(|e| miette!("{}", e))?
            .clone())
    }

    /// Sets the configuration of an Oku file system node's connection to a relay node.
    ///
    /// # Arguments
    ///
    /// * `relay_connection_config` - The configuration of an Oku file system node's connection to a relay node.
    pub fn set_relay_connection_config(
        &self,
        relay_connection_config: Option<OkuFsRelayConnectionConfig>,
    ) -> miette::Result<()> {
        *self
            .relay_connection_config
            .try_lock()
            .map_err(|e| miette!("{}", e))? = relay_connection_config;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Configuration of an Oku file system node's connection to a relay node.
pub struct OkuFsRelayConnectionConfig {
    /// An address to a relay server to perform hole punching.
    relay_address: Arc<Mutex<String>>,
    /// The number of times a node should re-attempt connecting to a relay before giving up.
    relay_connection_attempts: Arc<Mutex<i64>>,
}

impl OkuFsRelayConnectionConfig {
    /// Instantiate the configuration of an Oku file system node's connection to a relay node.
    ///
    /// # Arguments
    ///
    /// * `relay_address` - An address to a relay server to perform hole punching.
    ///
    /// * `relay_connection_attempts` - The number of times a node should re-attempt connecting to a relay before giving up.
    ///
    /// # Returns
    ///
    /// A configuration of an Oku file system node's connection to a relay node.
    pub fn new(relay_address: String, relay_connection_attempts: impl Into<i64>) -> Self {
        Self {
            relay_address: Arc::new(Mutex::new(relay_address)),
            relay_connection_attempts: Arc::new(Mutex::new(relay_connection_attempts.into())),
        }
    }

    /// An address to a relay server to perform hole punching.
    pub fn relay_address(&self) -> miette::Result<String> {
        Ok(self
            .relay_address
            .try_lock()
            .map_err(|e| miette!("{}", e))?
            .clone())
    }

    /// An address to a relay server to perform hole punching.
    ///
    /// # Arguments
    ///
    /// * `relay_address` - An address to a relay server to perform hole punching.
    pub fn set_relay_address(&self, relay_address: String) -> miette::Result<()> {
        *self
            .relay_address
            .try_lock()
            .map_err(|e| miette!("{}", e))? = relay_address;
        Ok(())
    }

    /// The number of times a node should re-attempt connecting to a relay before giving up.
    pub fn relay_connection_attempts(&self) -> miette::Result<i64> {
        Ok(self
            .relay_connection_attempts
            .try_lock()
            .map_err(|e| miette!("{}", e))?
            .clone())
    }

    /// Sets the number of times a node should re-attempt connecting to a relay before giving up.
    ///
    /// # Arguments
    ///
    /// * `relay_connection_attempts` - The number of times a node should re-attempt connecting to a relay before giving up.
    pub fn set_relay_connection_attempts(
        &self,
        relay_connection_attempts: impl Into<i64>,
    ) -> miette::Result<()> {
        *self
            .relay_connection_attempts
            .try_lock()
            .map_err(|e| miette!("{}", e))? = relay_connection_attempts.into();
        Ok(())
    }
}
