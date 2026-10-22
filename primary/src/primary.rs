// Copyright(C) Facebook, Inc. and its affiliates.
use crate::core::Core;
use crate::messages::{leader, CommittedBatch, PbftMessage, Transaction};
use crate::proposer::Proposer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters};
use crypto::SignatureService;
use futures::SinkExt as _;
use log::info;
use network::{CancelHandler, MessageHandler, Receiver as NetworkReceiver, ReliableSender, Writer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{interval, Duration};

pub const CHANNEL_CAPACITY: usize = 1_000;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum PrimaryMessage {
    Pbft(PbftMessage),
    Transaction(Transaction),
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
    ) {
        assert!(parameters.batch_size > 0, "Batch size must be positive");
        let name = keypair.name;
        assert!(committee.stake(&name) > 0, "Node has no voting rights");
        let is_leader = name == leader(&committee);
        let (tx_transactions, rx_transactions) = channel(CHANNEL_CAPACITY);
        let (tx_messages, rx_messages) = channel(CHANNEL_CAPACITY);
        let (tx_proposals, rx_proposals) = channel(CHANNEL_CAPACITY);
        let (tx_committed, rx_committed) = channel(CHANNEL_CAPACITY);
        parameters.log();

        let mut address = committee
            .primary(&name)
            .expect("Missing authority")
            .primary_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            PrimaryReceiverHandler {
                tx_messages,
                tx_transactions: if is_leader {
                    Some(tx_transactions.clone())
                } else {
                    None
                },
            },
        );
        let mut transactions = committee
            .worker(&name, &0)
            .expect("Missing client endpoint")
            .transactions;
        transactions.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(transactions, TransactionReceiver { tx_transactions });
        let signatures = SignatureService::new(keypair.secret);
        Core::spawn(
            name,
            committee.clone(),
            store,
            signatures.clone(),
            parameters.gc_depth,
            rx_messages,
            rx_proposals,
            tx_consensus,
            if is_leader { Some(tx_committed) } else { None },
        );
        if is_leader {
            Proposer::spawn(
                name,
                signatures,
                parameters.batch_size,
                parameters.max_batch_delay,
                rx_committed,
                rx_transactions,
                tx_proposals,
            );
        } else {
            // Preserve one client endpoint per node. Backups forward client input
            // once to the fixed leader; PBFT messages always use the common port.
            let address = committee
                .primary(&leader(&committee))
                .unwrap()
                .primary_to_primary;
            tokio::spawn(forward_transactions(rx_transactions, address));
        }
        info!(
            "Single-instance PBFT enabled: leader {}, epoch 0, instance 0, view 0",
            leader(&committee)
        );
        info!(
            "Primary {} successfully booted on {}",
            name,
            committee.primary(&name).unwrap().primary_to_primary.ip()
        );
    }
}

async fn forward_transactions(mut transactions: Receiver<Transaction>, address: SocketAddr) {
    let mut network = ReliableSender::new();
    let mut pending: HashMap<u64, CancelHandler> = HashMap::new();
    let mut counter = 0;
    let mut timer = interval(Duration::from_millis(50));
    loop {
        tokio::select! {
            Some(transaction) = transactions.recv(), if pending.len() < CHANNEL_CAPACITY => {
                let bytes = bincode::serialize(&PrimaryMessage::Transaction(transaction)).unwrap();
                pending.insert(counter, network.send(address, Bytes::from(bytes)).await);
                counter += 1;
            }
            _ = timer.tick() => {},
        }
        pending.retain(|_, handler| {
            matches!(
                handler.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            )
        });
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
    tx_messages: Sender<PbftMessage>,
    tx_transactions: Option<Sender<Transaction>>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, bytes: Bytes) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize(&bytes)? {
            PrimaryMessage::Pbft(message) => self.tx_messages.send(message).await?,
            PrimaryMessage::Transaction(transaction) => {
                if let Some(tx) = &self.tx_transactions {
                    tx.send(transaction).await?;
                }
            }
        }
        // This ACK only acknowledges transport delivery, never a protocol vote.
        writer.send(Bytes::from_static(b"Ack")).await?;
        Ok(())
    }
}
