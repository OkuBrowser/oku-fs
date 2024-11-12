use crate::fs::FS_PATH;
use iroh::docs::NamespaceId;
use log::error;
use miette::{miette, IntoDiagnostic};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex},
};

pub(crate) static CONFIG_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| PathBuf::from(FS_PATH).join("config.toml"));

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Configuration of an Oku file system node.
pub struct OkuFsConfig {
    /// The home replica of the node.
    home_replica: Arc<Mutex<Option<NamespaceId>>>,
}

impl OkuFsConfig {
    /// Loads the configuration of the file system from disk, or creates a new configuration if none exists.
    ///
    /// # Returns
    ///
    /// The configuration of the file system.
    pub fn load_or_create_config() -> miette::Result<Self> {
        let config_file_contents = std::fs::read_to_string(&*CONFIG_PATH);
        match config_file_contents {
            Ok(config_file_toml) => match toml::from_str(&config_file_toml) {
                Ok(config) => Ok(config),
                Err(e) => {
                    error!("{}", e);
                    let config = Self {
                        home_replica: Arc::new(Mutex::new(None)),
                    };
                    Ok(config)
                }
            },
            Err(e) => {
                error!("{}", e);
                let config = Self {
                    home_replica: Arc::new(Mutex::new(None)),
                };
                let config_toml = toml::to_string_pretty(&config).into_diagnostic()?;
                std::fs::write(&*CONFIG_PATH, config_toml).into_diagnostic()?;
                Ok(config)
            }
        }
    }

    /// The home replica of the node.
    pub fn home_replica(&self) -> miette::Result<Option<NamespaceId>> {
        Ok(*self.home_replica.try_lock().map_err(|e| miette!("{}", e))?)
    }

    /// Sets the home replica of the node.
    ///
    /// # Arguments
    ///
    /// * `home_replica` - The home replica of the node.
    pub fn set_home_replica(&self, home_replica: Option<NamespaceId>) -> miette::Result<()> {
        *self.home_replica.try_lock().map_err(|e| miette!("{}", e))? = home_replica;
        Ok(())
    }

    /// Writes the configuration to disk.
    pub fn save(&self) -> miette::Result<()> {
        let config_toml = toml::to_string_pretty(&self).into_diagnostic()?;
        std::fs::write(&*CONFIG_PATH, config_toml).into_diagnostic()?;
        Ok(())
    }
}
