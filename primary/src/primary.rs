// Copyright(C) Facebook, Inc. and its affiliates.
use crate::control_transport::{ControlAssembler, ControlChunk};
use crate::core::Core;
use crate::messages::{CommittedBatch, PbftMessage, Transaction};
use crate::proposer::Proposer;
use crate::transport::Network;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters};
use crypto::SignatureService;
use futures::SinkExt as _;
use log::{info, warn};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::{Arc, Mutex};
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::watch;

pub const CHANNEL_CAPACITY: usize = 1_000;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum PrimaryMessage {
    Pbft(PbftMessage),
    PbftChunk(ControlChunk),
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
        tx_consensus: Sender<CommittedBatch>,
        rx_epoch: watch::Receiver<u64>,
    ) {
        assert!(parameters.batch_size > 0, "Batch size must be positive");
        let epoch_length = parameters
            .epoch_length(committee.size())
            .expect("Segment length must be positive and epoch length must fit u64");
        let name = keypair.name;
        assert!(committee.stake(&name) > 0, "Node has no voting rights");
        let (tx_transactions, rx_transactions) = channel(CHANNEL_CAPACITY);
        let mut own_transactions = Some(rx_transactions);
        let signatures = SignatureService::new(keypair.secret);
        let network = Network::spawn_with_signatures(name, &committee, Some(signatures.clone()));
        let mut routes = Vec::with_capacity(committee.size());
        let mut local_instance = None;
        parameters.log();

        // Every replica runs every instance. Each replica leads exactly the
        // instance corresponding to its position in sorted committee key order.
        for (instance, owner) in committee.authorities.keys().enumerate() {
            let instance = instance as u64;
            let is_leader = name == *owner;
            let (tx_messages, rx_messages) = channel(CHANNEL_CAPACITY);
            let (tx_proposals, rx_proposals) = channel(CHANNEL_CAPACITY);
            let (tx_committed, rx_committed) = channel(CHANNEL_CAPACITY);
            routes.push(tx_messages);
            let rx_view = Core::spawn(
                name,
                committee.clone(),
                instance,
                epoch_length,
                rx_epoch.clone(),
                store.clone(),
                signatures.clone(),
                parameters.gc_depth,
                rx_messages,
                rx_proposals,
                tx_consensus.clone(),
                if is_leader { Some(tx_committed) } else { None },
                network.clone(),
            );
            if is_leader {
                local_instance = Some(instance);
                Proposer::spawn(
                    name,
                    instance,
                    epoch_length,
                    committee.size() as u64,
                    rx_epoch.clone(),
                    signatures.clone(),
                    parameters.batch_size,
                    parameters.max_batch_delay,
                    rx_committed,
                    own_transactions.take().expect("Duplicate local leader"),
                    tx_proposals,
                    Some(rx_view),
                );
            }
        }

        let mut address = committee
            .primary(&name)
            .expect("Missing authority")
            .primary_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            PrimaryReceiverHandler {
                routes: Arc::new(routes),
                fragments: Arc::new(Mutex::new(ControlAssembler::new(committee.clone()))),
            },
        );
        let mut transactions = committee
            .worker(&name, &0)
            .expect("Missing client endpoint")
            .transactions;
        transactions.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(transactions, TransactionReceiver { tx_transactions });
        info!(
            "MultiBFT enabled: {} instances; local instance {}; epoch 0, view 0",
            committee.size(),
            local_instance.expect("Missing local instance")
        );
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
    routes: Arc<Vec<Sender<PbftMessage>>>,
    fragments: Arc<Mutex<ControlAssembler>>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, bytes: Bytes) -> Result<(), Box<dyn Error>> {
        let message = match bincode::deserialize(&bytes)? {
            PrimaryMessage::Pbft(message) => Some(message),
            PrimaryMessage::PbftChunk(chunk) => self.fragments.lock().unwrap().insert(chunk)?,
        };
        if let Some(message) = message {
            let instance = message.position().instance;
            if instance < self.routes.len() as u64 {
                self.routes[instance as usize].send(message).await?;
            } else {
                warn!("Ignoring message for unknown PBFT instance {}", instance);
            }
        }
        // This ACK only acknowledges transport delivery, never a protocol vote.
        writer.send(Bytes::from_static(b"Ack")).await?;
        Ok(())
    }
}
