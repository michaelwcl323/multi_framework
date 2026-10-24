// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{generate_production_keypair, PublicKey, SecretKey};
use log::info;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    ExportError { file: String, message: String },
}

pub trait Import: DeserializeOwned {
    fn import(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ImportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub trait Export: Serialize {
    fn export(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::ExportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub type Stake = u32;
pub type WorkerId = u32;

#[derive(Deserialize, Clone)]
pub struct Parameters {
    /// Number of entries assigned to each segment in an ISS epoch.
    #[serde(default = "default_segment_length")]
    pub segment_length: u64,
    /// Maximum segment stall before requesting the next view, in milliseconds.
    #[serde(
        default = "default_view_change_timeout",
        deserialize_with = "positive_timeout"
    )]
    pub view_change_timeout: u64,
    /// Number of recent decided sequence numbers to retain transport retries for.
    pub gc_depth: u64,
    /// Legacy compatibility field; unused by the PBFT normal path.
    pub sync_retry_delay: u64,
    /// Legacy compatibility field; unused by the PBFT normal path.
    pub sync_retry_nodes: usize,
    /// Preferred transaction-byte size of a PBFT batch proposal.
    /// Denominated in bytes.
    pub batch_size: usize,
    /// The delay after which the proposer seals a batch, even if `batch_size`
    /// is not reached. Denominated in ms.
    pub max_batch_delay: u64,
}

fn default_segment_length() -> u64 {
    25
}

fn default_view_change_timeout() -> u64 {
    60_000
}

fn positive_timeout<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let timeout = u64::deserialize(deserializer)?;
    if timeout == 0 {
        return Err(serde::de::Error::custom(
            "view_change_timeout must be positive",
        ));
    }
    Ok(timeout)
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            segment_length: default_segment_length(),
            view_change_timeout: default_view_change_timeout(),
            gc_depth: 50,
            sync_retry_delay: 5_000,
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: 100,
        }
    }
}

impl Import for Parameters {}

impl Parameters {
    pub fn epoch_length(&self, segments: usize) -> Option<u64> {
        if self.segment_length == 0 || segments == 0 {
            return None;
        }
        self.segment_length.checked_mul(segments as u64)
    }

    pub fn log(&self) {
        info!("Segment length set to {} entries", self.segment_length);
        info!("View change timeout set to {} ms", self.view_change_timeout);
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay);
    }
}

#[derive(Clone, Deserialize)]
pub struct PrimaryAddresses {
    /// Address to receive messages from other primaries (WAN).
    pub primary_to_primary: SocketAddr,
    /// Legacy field retained for committee compatibility; the port is unused.
    pub worker_to_primary: SocketAddr,
}

#[derive(Clone, Deserialize, Eq, Hash, PartialEq)]
pub struct WorkerAddresses {
    /// Address to receive client transactions (WAN).
    pub transactions: SocketAddr,
    /// Legacy unused data port; complete batches use primary_to_primary.
    pub worker_to_worker: SocketAddr,
    /// Legacy field retained for committee compatibility; the port is unused.
    pub primary_to_worker: SocketAddr,
}

#[derive(Clone, Deserialize)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    /// The network addresses of the primary.
    pub primary: PrimaryAddresses,
    /// Legacy address map: only entry 0's client-ingress address is used.
    pub workers: HashMap<WorkerId, WorkerAddresses>,
}

#[derive(Clone, Deserialize)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
}

impl Import for Committee {}

impl Committee {
    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake of all authorities except `myself`.
    pub fn others_stake(&self, myself: &PublicKey) -> Vec<(PublicKey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.stake))
            .collect()
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes + 2) / 3
    }

    /// Returns the primary addresses of the target primary.
    pub fn primary(&self, to: &PublicKey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.primary.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all primaries except `myself`.
    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &PublicKey, id: &WorkerId) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &to)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*to))?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &PublicKey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &myself)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))?
            .workers
            .values()
            .cloned()
            .map(Ok)
            .collect()
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers(
        &self,
        myself: &PublicKey,
        id: &WorkerId,
    ) -> Vec<(PublicKey, WorkerAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .filter_map(|(name, authority)| {
                authority
                    .workers
                    .iter()
                    .find(|(worker_id, _)| worker_id == &id)
                    .map(|(_, addresses)| (*name, addresses.clone()))
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
pub struct KeyPair {
    /// The node's public key (and identifier).
    pub name: PublicKey,
    /// The node's secret key.
    pub secret: SecretKey,
}

impl Import for KeyPair {}
impl Export for KeyPair {}

impl KeyPair {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

impl Default for KeyPair {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_length_defaults_to_25_and_epoch_length_scales_with_committee() {
        let mut json = serde_json::json!({
            "gc_depth": 50,
            "sync_retry_delay": 1000,
            "sync_retry_nodes": 3,
            "batch_size": 2048000,
            "max_batch_delay": 200
        });
        let legacy: Parameters = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(legacy.segment_length, 25);
        assert_eq!(legacy.view_change_timeout, 60_000);
        assert_eq!(legacy.epoch_length(4), Some(100));
        assert_eq!(legacy.epoch_length(10), Some(250));
        assert_eq!(legacy.epoch_length(0), None);
        json["epoch_length"] = 1000.into();
        let legacy_epoch: Parameters = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(legacy_epoch.epoch_length(10), Some(250));
        json["segment_length"] = 7.into();
        let explicit: Parameters = serde_json::from_value(json).unwrap();
        assert_eq!(explicit.segment_length, 7);
        assert_eq!(explicit.epoch_length(4), Some(28));
        assert_eq!(
            Parameters {
                segment_length: 0,
                ..Parameters::default()
            }
            .epoch_length(4),
            None
        );
        assert_eq!(
            Parameters {
                segment_length: u64::MAX,
                ..Parameters::default()
            }
            .epoch_length(10),
            None
        );
    }

    #[test]
    fn view_change_timeout_reads_milliseconds_and_rejects_invalid_values() {
        let mut json = serde_json::json!({
            "gc_depth": 50, "sync_retry_delay": 1000, "sync_retry_nodes": 3,
            "batch_size": 2048000, "max_batch_delay": 200,
            "view_change_timeout": 1234
        });
        let parameters: Parameters = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(parameters.view_change_timeout, 1234);
        for value in [
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(true),
            serde_json::json!(1.5),
        ] {
            json["view_change_timeout"] = value;
            assert!(serde_json::from_value::<Parameters>(json.clone()).is_err());
        }
    }
}
