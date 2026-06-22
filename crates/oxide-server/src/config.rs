use oxide_core::{SchedulerPolicy, StreamConfig};
use oxide_wal::{FsyncMode, WalConfig};
use serde::{Deserialize, Serialize};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid listen address: {0}")]
    InvalidListenAddr(#[from] std::net::AddrParseError),
    #[error("invalid config: {0}")]
    Invalid(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct OxideConfig {
    pub listen_addr: String,
    pub data_dir: PathBuf,
    pub max_frame_bytes: usize,
    pub default_claim_wait_ms: u64,
    pub graceful_shutdown_ms: u64,
    pub auth_token: Option<String>,
    pub log_level: String,
    pub wal: WalSettings,
    pub stream_defaults: StreamDefaults,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WalSettings {
    pub segment_bytes: u64,
    pub fsync_mode: FsyncMode,
    pub batch_bytes: u64,
    pub batch_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StreamDefaults {
    pub partitions: u32,
    pub policy: SchedulerPolicy,
    pub max_payload_bytes: usize,
    pub default_ttr_ms: u64,
    pub dedupe_ttl_ms: u64,
}

impl Default for OxideConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:7379".to_owned(),
            data_dir: PathBuf::from("./oxide-data"),
            max_frame_bytes: 64 * 1024 * 1024,
            default_claim_wait_ms: 30_000,
            graceful_shutdown_ms: 5_000,
            auth_token: None,
            log_level: "info".to_owned(),
            wal: WalSettings::default(),
            stream_defaults: StreamDefaults::default(),
        }
    }
}

impl Default for WalSettings {
    fn default() -> Self {
        Self {
            segment_bytes: 128 * 1024 * 1024,
            fsync_mode: FsyncMode::Batch,
            batch_bytes: 4 * 1024 * 1024,
            batch_interval_ms: 10,
        }
    }
}

impl Default for StreamDefaults {
    fn default() -> Self {
        Self {
            partitions: 1,
            policy: SchedulerPolicy::Priority,
            max_payload_bytes: 1024 * 1024,
            default_ttr_ms: 30_000,
            dedupe_ttl_ms: 24 * 60 * 60 * 1000,
        }
    }
}

impl OxideConfig {
    pub fn from_path(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let mut config: Self = toml::from_str(&contents)?;
        config.apply_env();
        config.validate()?;
        Ok(config)
    }

    pub fn load(path: Option<impl AsRef<std::path::Path>>) -> Result<Self, ConfigError> {
        let mut config = if let Some(path) = path {
            Self::from_path(path)?
        } else {
            Self::default()
        };
        config.apply_env();
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let _: SocketAddr = self.listen_addr.parse()?;
        if self.max_frame_bytes == 0 {
            return Err(ConfigError::Invalid(
                "max_frame_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.stream_defaults.partitions == 0 {
            return Err(ConfigError::Invalid(
                "stream_defaults.partitions must be greater than zero".to_owned(),
            ));
        }
        if self.stream_defaults.max_payload_bytes == 0 {
            return Err(ConfigError::Invalid(
                "stream_defaults.max_payload_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.auth_token.as_deref().is_some_and(str::is_empty) {
            return Err(ConfigError::Invalid(
                "auth_token cannot be empty when configured".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn wal_config(&self) -> WalConfig {
        WalConfig {
            dir: self.wal_dir(),
            segment_bytes: self.wal.segment_bytes,
            fsync_mode: self.wal.fsync_mode,
            batch_bytes: self.wal.batch_bytes,
            batch_interval_ms: self.wal.batch_interval_ms,
        }
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    pub fn default_stream_config(&self, name: impl Into<String>) -> StreamConfig {
        StreamConfig {
            name: name.into(),
            partitions: self.stream_defaults.partitions,
            policy: self.stream_defaults.policy,
            max_payload_bytes: self.stream_defaults.max_payload_bytes,
            default_ttr_ms: self.stream_defaults.default_ttr_ms,
            dedupe_ttl_ms: self.stream_defaults.dedupe_ttl_ms,
            dead_letter_stream: None,
        }
    }

    pub fn graceful_shutdown_duration(&self) -> Duration {
        Duration::from_millis(self.graceful_shutdown_ms)
    }

    fn apply_env(&mut self) {
        if let Ok(value) = env::var("OXIDE_LISTEN_ADDR") {
            self.listen_addr = value;
        }
        if let Ok(value) = env::var("OXIDE_DATA_DIR") {
            self.data_dir = PathBuf::from(value);
        }
        if let Ok(value) = env::var("OXIDE_LOG_LEVEL") {
            self.log_level = value;
        }
        if let Ok(value) = env::var("OXIDE_AUTH_TOKEN") {
            self.auth_token = Some(value);
        }
        if let Ok(value) = env::var("OXIDE_WAL_FSYNC_MODE") {
            self.wal.fsync_mode = match value.as_str() {
                "always" => FsyncMode::Always,
                "async" => FsyncMode::Async,
                _ => FsyncMode::Batch,
            };
        }
    }
}
