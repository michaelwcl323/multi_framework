// Copyright(C) Facebook, Inc. and its affiliates.
use crate::epoch::{Epoch, GlobalSn};
use crate::error::{PbftError, PbftResult};
use crate::messages::{
    Commit, CommittedBatch, Instance, PbftMessage, Position, PrePrepare, Prepare, Sequence,
};
use crate::pbft::{Action, Pbft};
use crate::transport::Network;
use config::Committee;
use crypto::{Hash as _, PublicKey, SignatureService};
use log::{debug, warn};
use network::CancelHandler;
use std::collections::{BTreeMap, HashMap, VecDeque};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;
use tokio::time::{interval, Duration};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
mod tests;

pub(crate) struct Core {
    name: PublicKey,
    store: Store,
    signatures: SignatureService,
    pbft: Pbft,
    committee: Committee,
    epoch: Epoch,
    rx_epoch: watch::Receiver<u64>,
    future_messages: BTreeMap<(Sequence, u8, PublicKey), (PbftMessage, bool)>,
    gc_depth: Sequence,
    rx_messages: Receiver<PbftMessage>,
    rx_proposer: Receiver<PrePrepare>,
    tx_consensus: Sender<CommittedBatch>,
    tx_proposer: Option<Sender<Position>>,
    network: Network,
    cancel_handlers: HashMap<GlobalSn, Vec<CancelHandler>>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        instance: Instance,
        epoch_length: u64,
        rx_epoch: watch::Receiver<u64>,
        store: Store,
        signatures: SignatureService,
        gc_depth: Sequence,
        rx_messages: Receiver<PbftMessage>,
        rx_proposer: Receiver<PrePrepare>,
        tx_consensus: Sender<CommittedBatch>,
        tx_proposer: Option<Sender<Position>>,
        network: Network,
    ) {
        tokio::spawn(async move {
            let epoch = Epoch::new(*rx_epoch.borrow(), epoch_length, committee.size() as u64)
                .expect("Invalid epoch layout");
            debug!(
                "Started segment epoch={} instance={} slots={}",
                epoch.number,
                instance,
                epoch.segment(instance).unwrap().slots
            );
            Self {
                name,
                pbft: Pbft::new(committee.clone(), epoch.segment(instance).unwrap()),
                committee,
                epoch,
                rx_epoch,
                future_messages: BTreeMap::new(),
                store,
                signatures,
                gc_depth,
                rx_messages,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                network,
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
        let (phase, proposal) = match &message {
            PbftMessage::PrePrepare(x) => ("PrePrepare", x.digest()),
            PbftMessage::Prepare(x) => ("Prepare", x.proposal.clone()),
            PbftMessage::Commit(x) => ("Commit", x.proposal.clone()),
        };
        debug!(
            "Broadcast {} node={} position={:?} proposal={:?}",
            phase,
            self.name,
            message.position(),
            proposal
        );
        let global_sn = self.epoch.global_sn(message.position()).unwrap();
        let handlers = self.network.broadcast(message).await;
        self.cancel_handlers
            .entry(global_sn)
            .or_default()
            .extend(handlers);
    }

    async fn receive(&mut self, message: PbftMessage, own_proposal: bool) -> PbftResult<()> {
        let position = message.position();
        if position.epoch < self.epoch.number {
            debug!("Ignoring old epoch message position={:?}", position);
            return Ok(());
        }
        if position.epoch > self.epoch.number {
            ensure!(
                Some(position.epoch) == self.epoch.number.checked_add(1),
                PbftError::WrongEpoch {
                    expected: self.epoch.number,
                    received: position.epoch
                }
            );
            ensure!(
                position.instance == self.pbft.segment.instance,
                PbftError::WrongInstance {
                    expected: self.pbft.segment.instance,
                    received: position.instance
                }
            );
            let next = self
                .epoch
                .next()
                .ok_or(PbftError::OutsideSegment(position))?;
            ensure!(
                next.global_sn(position).is_some(),
                PbftError::OutsideSegment(position)
            );
            ensure!(
                position.sequence <= crate::messages::SEQUENCE_WINDOW,
                PbftError::OutsideWindow(position.sequence)
            );
            message.verify(&self.committee)?;
            let (phase, author) = match &message {
                PbftMessage::PrePrepare(x) => (0, x.author),
                PbftMessage::Prepare(x) => (1, x.author),
                PbftMessage::Commit(x) => (2, x.author),
            };
            // Bound look-ahead to one epoch and one message per slot/phase/sender.
            self.future_messages
                .entry((position.sequence, phase, author))
                .or_insert((message, own_proposal));
            debug!("Buffered next epoch message position={:?}", position);
            return Ok(());
        }
        self.process(message, own_proposal).await
    }

    async fn process(&mut self, message: PbftMessage, own_proposal: bool) -> PbftResult<()> {
        let mut actions: VecDeque<_> = self.pbft.process(message.clone())?.into();
        if own_proposal {
            self.broadcast(message).await;
        }
        while let Some(action) = actions.pop_front() {
            let outgoing = match action {
                Action::Prepare(proposal) => Some(PbftMessage::Prepare(
                    Prepare::new(&proposal, self.name, &mut self.signatures).await,
                )),
                Action::Commit(proposal) => Some(PbftMessage::Commit(
                    Commit::new(&proposal, self.name, &mut self.signatures).await,
                )),
                Action::Decide(decision) => {
                    debug!(
                        "PBFT decided {} position={:?} global_sn={} proposal={:?} commit_votes={}",
                        decision.proposal,
                        decision.proposal.position,
                        self.epoch.global_sn(decision.proposal.position).unwrap(),
                        decision.proposal.digest(),
                        decision.commits.len()
                    );
                    let position = decision.proposal.position;
                    let mut key = b"pbft/committed/".to_vec();
                    key.extend_from_slice(&bincode::serialize(&decision.proposal.position)?);
                    // Store only the commit certificate, without proposal or batch payload.
                    self.persist(key, bincode::serialize(&decision.commits)?)
                        .await?;
                    self.tx_consensus
                        .send(decision)
                        .await
                        .expect("Consensus channel closed");
                    if let Some(tx) = &self.tx_proposer {
                        tx.send(position).await.expect("Proposer channel closed");
                    }
                    None
                }
            };
            if let Some(message) = outgoing {
                // Count our vote through the same verified path as every remote vote.
                actions.extend(self.pbft.process(message.clone())?);
                self.broadcast(message).await;
            }
        }
        Ok(())
    }

    fn cleanup(&mut self) {
        let low = self
            .pbft
            .segment
            .global_sn(self.pbft.low_watermark)
            .unwrap_or_else(|| self.epoch.last_sn().saturating_add(1));
        let gc_depth = self.gc_depth.saturating_mul(self.epoch.segments());
        self.cancel_handlers.retain(|global_sn, handlers| {
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
            global_sn.saturating_add(gc_depth) >= low && !handlers.is_empty()
        });
    }

    fn report(result: PbftResult<()>) {
        match result {
            Ok(()) => (),
            Err(e @ PbftError::StoreError(_)) | Err(e @ PbftError::StorageFailure) => {
                panic!("PBFT storage failure: {}", e);
            }
            Err(e) => warn!("Rejected PBFT message: {}", e),
        }
    }

    async fn run(&mut self) {
        let mut cleanup_timer = interval(Duration::from_millis(100));
        loop {
            let epoch_number = *self.rx_epoch.borrow_and_update();
            if epoch_number != self.epoch.number {
                assert!(
                    epoch_number > self.epoch.number,
                    "Epoch must not move backwards"
                );
                self.epoch = Epoch::new(epoch_number, self.epoch.length(), self.epoch.segments())
                    .expect("Global sequence space exhausted");
                let segment = self.epoch.segment(self.pbft.segment.instance).unwrap();
                self.pbft = Pbft::new(self.committee.clone(), segment);
                debug!(
                    "Started segment epoch={} instance={} slots={}",
                    segment.epoch, segment.instance, segment.slots
                );
                for (_, (message, own_proposal)) in std::mem::take(&mut self.future_messages) {
                    if message.position().epoch == epoch_number {
                        Self::report(self.process(message, own_proposal).await);
                    }
                }
            }
            let result = tokio::select! {
                Some(message) = self.rx_messages.recv() => self.receive(message, false).await,
                Some(proposal) = self.rx_proposer.recv() => self.receive(PbftMessage::PrePrepare(proposal), true).await,
                changed = self.rx_epoch.changed() => {
                    if changed.is_err() { return; }
                    Ok(())
                },
                _ = cleanup_timer.tick() => Ok(()),
            };
            Self::report(result);
            self.cleanup();
        }
    }
}
