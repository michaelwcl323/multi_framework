// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_waiter::WaiterMessage;
use crate::error::DagResult;
use crate::messages::{Batch, Certificate};
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use store::Store;
use tokio::sync::mpsc::Sender;

/// The `Synchronizer` checks for parent certificates referenced by a complete batch. It sends
/// a command to the `Waiter` to request the missing data.
pub struct Synchronizer {
    /// The persistent storage.
    store: Store,
    /// Send commands to the `BatchWaiter`.
    tx_batch_waiter: Sender<WaiterMessage>,
    /// Send commands to the `CertificateWaiter`.
    tx_certificate_waiter: Sender<Certificate>,
    /// The genesis and its digests.
    genesis: Vec<(Digest, Certificate)>,
}

impl Synchronizer {
    pub fn new(
        _name: PublicKey,
        committee: &Committee,
        store: Store,
        tx_batch_waiter: Sender<WaiterMessage>,
        tx_certificate_waiter: Sender<Certificate>,
    ) -> Self {
        Self {
            store,
            tx_batch_waiter,
            tx_certificate_waiter,
            genesis: Certificate::genesis(committee)
                .into_iter()
                .map(|x| (x.digest(), x))
                .collect(),
        }
    }

    /// Returns the parents of a batch if we have them all. If at least one parent is missing,
    /// we return an empty vector, synchronize with other nodes, and re-schedule processing
    /// of the batch for when we will have all the parents.
    pub async fn get_parents(&mut self, batch: &Batch) -> DagResult<Vec<Certificate>> {
        let mut missing = Vec::new();
        let mut parents = Vec::new();
        for digest in &batch.parents {
            if let Some(genesis) = self
                .genesis
                .iter()
                .find(|(x, _)| x == digest)
                .map(|(_, x)| x)
            {
                parents.push(genesis.clone());
                continue;
            }

            match self.store.read(digest.to_vec()).await? {
                Some(certificate) => parents.push(bincode::deserialize(&certificate)?),
                None => missing.push(digest.clone()),
            };
        }

        if missing.is_empty() {
            return Ok(parents);
        }

        self.tx_batch_waiter
            .send(WaiterMessage::SyncParents(missing, batch.clone()))
            .await
            .expect("Failed to send sync parents request");
        Ok(Vec::new())
    }

    /// Check whether we have all the ancestors of the certificate. If we don't, send the certificate to
    /// the `CertificateWaiter` which will trigger re-processing once we have all the missing data.
    pub async fn deliver_certificate(&mut self, certificate: &Certificate) -> DagResult<bool> {
        for digest in &certificate.batch.parents {
            if self.genesis.iter().any(|(x, _)| x == digest) {
                continue;
            }

            if self.store.read(digest.to_vec()).await?.is_none() {
                self.tx_certificate_waiter
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send sync certificate request");
                return Ok(false);
            };
        }
        Ok(true)
    }
}
