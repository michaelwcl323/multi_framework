// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{PbftError, PbftResult};
use crate::messages::{Commit, CommittedBatch, PbftMessage, PrePrepare, Prepare, Sequence};
use crate::pbft::{Action, Pbft};
use crate::primary::PrimaryMessage;
use bytes::Bytes;
use config::Committee;
use crypto::{Hash as _, PublicKey, SignatureService};
use log::{debug, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, VecDeque};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval, Duration};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
mod tests;

pub(crate) struct Core {
    name: PublicKey,
    committee: Committee,
    store: Store,
    signatures: SignatureService,
    pbft: Pbft,
    gc_depth: Sequence,
    rx_messages: Receiver<PbftMessage>,
    rx_proposer: Receiver<PrePrepare>,
    tx_consensus: Sender<CommittedBatch>,
    tx_proposer: Option<Sender<Sequence>>,
    network: ReliableSender,
    cancel_handlers: HashMap<Sequence, Vec<CancelHandler>>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        signatures: SignatureService,
        gc_depth: Sequence,
        rx_messages: Receiver<PbftMessage>,
        rx_proposer: Receiver<PrePrepare>,
        tx_consensus: Sender<CommittedBatch>,
        tx_proposer: Option<Sender<Sequence>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                pbft: Pbft::new(committee.clone()),
                committee,
                store,
                signatures,
                gc_depth,
                rx_messages,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                network: ReliableSender::new(),
                cancel_handlers: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    async fn persist(&mut self, key: Vec<u8>, bytes: Vec<u8>) -> PbftResult<()> {
        self.store.write(key.clone(), bytes.clone()).await;
        ensure!(
            self.store.read(key).await?.as_deref() == Some(bytes.as_slice()),
            PbftError::StorageFailure
        );
        Ok(())
    }

    async fn broadcast(&mut self, message: PbftMessage) {
        let sequence = message.position().sequence;
        let bytes = bincode::serialize(&PrimaryMessage::Pbft(message))
            .expect("Failed to serialize PBFT message");
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(sequence)
            .or_default()
            .extend(handlers);
    }

    async fn process(&mut self, message: PbftMessage, own_proposal: bool) -> PbftResult<()> {
        let mut actions: VecDeque<_> = self.pbft.process(message.clone())?.into();
        if own_proposal {
            self.broadcast(message).await;
        }
        while let Some(action) = actions.pop_front() {
            let outgoing = match action {
                Action::Prepare(proposal) => {
                    // Keep the full proposal before issuing a Prepare for its digest.
                    self.persist(proposal.digest().to_vec(), bincode::serialize(&proposal)?)
                        .await?;
                    Some(PbftMessage::Prepare(
                        Prepare::new(&proposal, self.name, &mut self.signatures).await,
                    ))
                }
                Action::Commit(proposal) => Some(PbftMessage::Commit(
                    Commit::new(&proposal, self.name, &mut self.signatures).await,
                )),
                Action::Decide(decision) => {
                    debug!("PBFT decided {}", decision.proposal);
                    let sequence = decision.proposal.position.sequence;
                    let mut key = b"pbft/committed/".to_vec();
                    key.extend_from_slice(&sequence.to_le_bytes());
                    self.persist(key, bincode::serialize(&decision)?).await?;
                    self.tx_consensus
                        .send(decision)
                        .await
                        .expect("Consensus channel closed");
                    if let Some(tx) = &self.tx_proposer {
                        tx.send(sequence).await.expect("Proposer channel closed");
                    }
                    None
                }
            };
            if let Some(message) = outgoing {
                let mut key = b"pbft/local-vote/".to_vec();
                key.extend_from_slice(&bincode::serialize(&message.position())?);
                key.push(match &message {
                    PbftMessage::Prepare(_) => 1,
                    _ => 2,
                });
                self.persist(key, bincode::serialize(&message)?).await?;
                // Count our vote through the same verified path as every remote vote.
                actions.extend(self.pbft.process(message.clone())?);
                self.broadcast(message).await;
            }
        }
        Ok(())
    }

    fn cleanup(&mut self) {
        let low = self.pbft.low_watermark;
        let gc_depth = self.gc_depth;
        self.cancel_handlers.retain(|sequence, handlers| {
            // Successful ACKs can be forgotten immediately. Retain recent retries
            // after decision so a local commit does not cancel its own dissemination.
            let mut pending = Vec::new();
            for mut handler in handlers.drain(..) {
                if matches!(
                    handler.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                ) {
                    pending.push(handler);
                }
            }
            *handlers = pending;
            sequence.saturating_add(gc_depth) >= low && !handlers.is_empty()
        });
    }

    async fn run(&mut self) {
        let mut cleanup_timer = interval(Duration::from_millis(100));
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_messages.recv() => self.process(message, false).await,
                Some(proposal) = self.rx_proposer.recv() => self.process(PbftMessage::PrePrepare(proposal), true).await,
                _ = cleanup_timer.tick() => Ok(()),
            };
            match result {
                Ok(()) => (),
                Err(e @ PbftError::StoreError(_)) | Err(e @ PbftError::StorageFailure) => {
                    panic!("PBFT storage failure: {}", e);
                }
                Err(e) => warn!("Rejected PBFT message: {}", e),
            }
            self.cleanup();
        }
    }
}
