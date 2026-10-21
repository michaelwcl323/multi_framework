// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_waiter::BatchWaiter;
use crate::certificate_waiter::CertificateWaiter;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::helper::Helper;
use crate::messages::{Batch, Certificate, Transaction, Vote};
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters};
use crypto::{Digest, PublicKey, SignatureService};
use futures::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub const CHANNEL_CAPACITY: usize = 1_000;
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Batch(Batch),
    Vote(Vote),
    Certificate(Certificate),
    CertificatesRequest(Vec<Digest>, PublicKey),
}

pub struct Primary;

#[cfg(test)]
#[path = "tests/direct_node_tests.rs"]
mod direct_node_tests;

impl Primary {
    pub fn spawn(
        keypair: KeyPair,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        tx_consensus: Sender<Certificate>,
        rx_consensus: Receiver<Certificate>,
    ) {
        let (tx_transactions, rx_transactions) = channel(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_batches, rx_batches) = channel(CHANNEL_CAPACITY);
        let (tx_sync_batches, rx_sync_batches) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_batches_loopback, rx_batches_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);
        parameters.log();
        let name = keypair.name;
        let consensus_round = Arc::new(AtomicU64::new(0));

        let mut address = committee
            .primary(&name)
            .expect("Missing authority")
            .primary_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            PrimaryReceiverHandler {
                tx_primary_messages,
                tx_cert_requests,
            },
        );

        // Only the legacy client-ingress field is used; there is no worker or batch-data port.
        let mut transactions = committee
            .worker(&name, &0)
            .expect("Missing client endpoint")
            .transactions;
        transactions.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(transactions, TransactionReceiver { tx_transactions });

        let synchronizer = Synchronizer::new(
            name,
            &committee,
            store.clone(),
            tx_sync_batches,
            tx_sync_certificates,
        );
        let signature_service = SignatureService::new(keypair.secret);
        Core::spawn(
            name,
            committee.clone(),
            store.clone(),
            synchronizer,
            signature_service.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            rx_primary_messages,
            rx_batches_loopback,
            rx_certificates_loopback,
            rx_batches,
            tx_consensus,
            tx_parents,
        );
        GarbageCollector::spawn(consensus_round.clone(), rx_consensus);
        BatchWaiter::spawn(
            name,
            committee.clone(),
            store.clone(),
            consensus_round,
            parameters.gc_depth,
            parameters.sync_retry_delay,
            parameters.sync_retry_nodes,
            rx_sync_batches,
            tx_batches_loopback,
        );
        CertificateWaiter::spawn(
            store.clone(),
            rx_sync_certificates,
            tx_certificates_loopback,
        );
        Proposer::spawn(
            name,
            &committee,
            signature_service,
            parameters.batch_size,
            parameters.max_batch_delay,
            rx_parents,
            rx_transactions,
            tx_batches,
        );
        Helper::spawn(committee.clone(), store, rx_cert_requests);
        info!("Direct batch proposals enabled: transactions on {}, proposal/vote/certificate traffic on {}", transactions, address);
        info!(
            "Primary {} successfully booted on {}",
            name,
            committee.primary(&name).unwrap().primary_to_primary.ip()
        );
    }
}

#[derive(Clone)]
struct TransactionReceiver {
    tx_transactions: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TransactionReceiver {
    async fn dispatch(&self, _writer: &mut Writer, bytes: Bytes) -> Result<(), Box<dyn Error>> {
        self.tx_transactions.send(bytes.to_vec()).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
    tx_cert_requests: Sender<(Vec<Digest>, PublicKey)>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            PrimaryMessage::CertificatesRequest(missing, requestor) => {
                self.tx_cert_requests.send((missing, requestor)).await?;
            }
            request => self.tx_primary_messages.send(request).await?,
        }
        // Transport ACKs only drive retries; proposal creation never waits for an ACK quorum.
        writer.send(Bytes::from_static(b"Ack")).await?;
        Ok(())
    }
}
