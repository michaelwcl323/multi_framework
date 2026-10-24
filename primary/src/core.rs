// Copyright(C) Facebook, Inc. and its affiliates.
use crate::epoch::{Epoch, GlobalSn};
use crate::error::{PbftError, PbftResult};
use crate::messages::{
    Commit, CommittedBatch, Instance, PbftMessage, Position, PrePrepare, Prepare, Sequence,
};
use crate::pbft::{Action, Pbft};
use crate::transport::Network;
use crate::{NewView, ViewChange};
use config::Committee;
use crypto::{Hash as _, PublicKey, SignatureService};
use log::{debug, info, warn};
use network::CancelHandler;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::pin::Pin;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;
use tokio::time::{interval, sleep, Duration, Instant, Sleep};

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
    future_messages: BTreeMap<(u64, u64, Sequence, u8, PublicKey), (PbftMessage, bool)>,
    tx_view: watch::Sender<(u64, u64)>,
    gc_depth: Sequence,
    rx_messages: Receiver<PbftMessage>,
    rx_proposer: Receiver<PrePrepare>,
    tx_consensus: Sender<CommittedBatch>,
    tx_proposer: Option<Sender<Position>>,
    network: Network,
    cancel_handlers: HashMap<GlobalSn, Vec<CancelHandler>>,
    view_change_timeout: Duration,
    view_change_timer: Option<Pin<Box<Sleep>>>,
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
        view_change_timeout: u64,
        rx_messages: Receiver<PbftMessage>,
        rx_proposer: Receiver<PrePrepare>,
        tx_consensus: Sender<CommittedBatch>,
        tx_proposer: Option<Sender<Position>>,
        network: Network,
    ) -> watch::Receiver<(u64, u64)> {
        assert!(
            view_change_timeout > 0,
            "View change timeout must be positive"
        );
        let (tx_view, rx_view) = watch::channel((*rx_epoch.borrow(), 0));
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
                tx_view,
                store,
                signatures,
                gc_depth,
                rx_messages,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                network,
                cancel_handlers: HashMap::new(),
                view_change_timeout: Duration::from_millis(view_change_timeout),
                view_change_timer: if epoch.segment(instance).unwrap().slots > 0 {
                    Some(Box::pin(sleep(Duration::from_millis(view_change_timeout))))
                } else {
                    None
                },
            }
            .run()
            .await;
        });
        rx_view
    }

    async fn persist(&mut self, key: Vec<u8>, bytes: Vec<u8>) -> PbftResult<()> {
        self.store.write_checked(key, bytes).await?;
        Ok(())
    }

    async fn broadcast(&mut self, message: PbftMessage) {
        let (phase, proposal) = match &message {
            PbftMessage::PrePrepare(x) => ("PrePrepare", x.digest()),
            PbftMessage::Prepare(x) => ("Prepare", x.proposal.clone()),
            PbftMessage::Commit(x) => ("Commit", x.proposal.clone()),
            PbftMessage::ViewChange(x) => ("ViewChange", x.digest()),
            PbftMessage::NewView(x) => ("NewView", x.digest()),
        };
        debug!(
            "Broadcast {} node={} position={:?} proposal={:?}",
            phase,
            self.name,
            message.position(),
            proposal
        );
        let global_sn = if matches!(
            &message,
            PbftMessage::ViewChange(_) | PbftMessage::NewView(_)
        ) {
            self.epoch.last_sn()
        } else {
            self.epoch.global_sn(message.position()).unwrap()
        };
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
        let future_view = position.epoch == self.epoch.number
            && position.view > self.pbft.view
            && !matches!(
                &message,
                PbftMessage::ViewChange(_) | PbftMessage::NewView(_)
            );
        if position.epoch > self.epoch.number || future_view {
            ensure!(
                position.epoch == self.epoch.number
                    || Some(position.epoch) == self.epoch.number.checked_add(1),
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
            let next = Epoch::new(position.epoch, self.epoch.length(), self.epoch.segments())
                .ok_or(PbftError::OutsideSegment(position))?;
            ensure!(
                next.global_sn(position).is_some(),
                PbftError::OutsideSegment(position)
            );
            ensure!(
                future_view || position.sequence <= crate::messages::SEQUENCE_WINDOW,
                PbftError::OutsideWindow(position.sequence)
            );
            message.verify(&self.committee)?;
            let (phase, author) = message.phase_author();
            // Keep at most one (the latest) view per epoch/slot/phase/sender,
            // including authenticated votes that precede a multi-view jump.
            if self
                .future_messages
                .keys()
                .any(|(epoch, view, sequence, p, sender)| {
                    *epoch == position.epoch
                        && *sequence == position.sequence
                        && *p == phase
                        && *sender == author
                        && *view > position.view
                })
            {
                return Ok(());
            }
            self.future_messages
                .retain(|(epoch, view, sequence, p, sender), _| {
                    !(*epoch == position.epoch
                        && *sequence == position.sequence
                        && *p == phase
                        && *sender == author
                        && *view < position.view)
                });
            self.future_messages
                .entry((
                    position.epoch,
                    position.view,
                    position.sequence,
                    phase,
                    author,
                ))
                .or_insert((message, own_proposal));
            debug!("Buffered future epoch/view message position={:?}", position);
            return Ok(());
        }
        self.process(message, own_proposal).await
    }

    async fn process(&mut self, message: PbftMessage, own_proposal: bool) -> PbftResult<()> {
        if own_proposal && (self.pbft.view != 0 || self.pbft.target_view != 0) {
            return Ok(());
        }
        let changes_view = matches!(
            &message,
            PbftMessage::ViewChange(_) | PbftMessage::NewView(_)
        );
        let before = self.timer_progress();
        let actions = self.pbft.process(message.clone())?.into();
        self.record_recovery(&message);
        if changes_view {
            self.publish_view();
        }
        if own_proposal {
            self.broadcast(message).await;
        }
        let result = self.execute_actions(actions).await;
        if before != self.timer_progress() {
            self.reset_view_change_timer();
        }
        result
    }

    async fn execute_actions(&mut self, mut actions: VecDeque<Action>) -> PbftResult<()> {
        while let Some(action) = actions.pop_front() {
            let outgoing = match action {
                Action::ChangeView(view) => {
                    let prepared = self.pbft.begin_view_change(view)?;
                    self.publish_view();
                    Some(PbftMessage::ViewChange(
                        ViewChange::new(
                            self.epoch.number,
                            self.pbft.segment.instance,
                            view,
                            prepared,
                            self.name,
                            &mut self.signatures,
                        )
                        .await,
                    ))
                }
                Action::NewView(changes) => {
                    let view = changes[0].view;
                    if crate::primary(&self.committee, self.pbft.segment.instance, view)
                        == Some(self.name)
                    {
                        Some(PbftMessage::NewView(
                            NewView::new(
                                &self.committee,
                                self.pbft.segment.slots,
                                changes,
                                self.name,
                                &mut self.signatures,
                            )
                            .await?,
                        ))
                    } else {
                        None
                    }
                }
                Action::Prepare(proposal) => Some(PbftMessage::Prepare(
                    Prepare::new(&proposal, self.name, &mut self.signatures).await,
                )),
                Action::Commit(proposal) => Some(PbftMessage::Commit(
                    Commit::new(&proposal, self.name, &mut self.signatures).await,
                )),
                Action::Decide(decision) => {
                    #[cfg(feature = "benchmark")]
                    if !decision.proposal.batch.transactions.is_empty() {
                        info!(
                            "PBFT decided {} -> {:?} (instance {}) epoch={} global_sn={}",
                            decision.proposal,
                            decision.proposal.digest(),
                            decision.proposal.position.instance,
                            self.epoch.number,
                            self.epoch.global_sn(decision.proposal.position).unwrap()
                        );
                    }
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
                    key.extend_from_slice(&bincode::serialize(&position)?);
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
                let changes_view = matches!(
                    &message,
                    PbftMessage::ViewChange(_) | PbftMessage::NewView(_)
                );
                actions.extend(self.pbft.process(message.clone())?);
                self.record_recovery(&message);
                if changes_view {
                    self.publish_view();
                }
                self.broadcast(message).await;
            }
        }
        Ok(())
    }

    fn publish_view(&self) {
        let progress = (self.epoch.number, self.pbft.target_view);
        if *self.tx_view.borrow() != progress {
            if progress.1 > 0 {
                info!(
                    "PBFT view change epoch={} instance={} view={}",
                    progress.0, self.pbft.segment.instance, progress.1
                );
            }
            let _ = self.tx_view.send(progress);
        }
    }

    fn timer_progress(&self) -> (u64, u64, Sequence) {
        (
            self.pbft.view,
            self.pbft.target_view,
            self.pbft.low_watermark,
        )
    }

    fn reset_view_change_timer(&mut self) {
        if self.pbft.is_complete() {
            // Dropping Sleep deregisters the deadline, even while other
            // instances still hold up this epoch's global-delivery barrier.
            self.view_change_timer = None;
        } else if let Some(timer) = &mut self.view_change_timer {
            timer
                .as_mut()
                .reset(Instant::now() + self.view_change_timeout);
        } else {
            self.view_change_timer = Some(Box::pin(sleep(self.view_change_timeout)));
        }
    }

    async fn on_view_change_timeout(&mut self) -> PbftResult<()> {
        if self.pbft.is_complete() {
            self.view_change_timer = None;
            return Ok(());
        }
        let next_view = self
            .pbft
            .target_view
            .checked_add(1)
            .expect("View number exhausted");
        warn!(
            "PBFT timeout node={} epoch={} instance={} leader={} stalled_sequence={} target_view={}",
            self.name, self.epoch.number, self.pbft.segment.instance,
            crate::primary(&self.committee, self.pbft.segment.instance, self.pbft.target_view).unwrap(),
            self.pbft.low_watermark, next_view
        );
        let result = self
            .execute_actions(VecDeque::from(vec![Action::ChangeView(next_view)]))
            .await;
        self.reset_view_change_timer();
        result
    }

    async fn wait_for_timeout(timer: &mut Option<Pin<Box<Sleep>>>) {
        match timer {
            Some(timer) => timer.as_mut().await,
            None => std::future::pending::<()>().await,
        }
    }

    fn record_recovery(&self, message: &PbftMessage) {
        #[cfg(feature = "benchmark")]
        if let PbftMessage::NewView(view) = message {
            for proposal in &view.proposals {
                if proposal.bottom {
                    info!(
                        "Recovered bottom epoch={} instance={} view={} sequence={} global_sn={}",
                        proposal.position.epoch,
                        proposal.position.instance,
                        proposal.position.view,
                        proposal.position.sequence,
                        self.epoch.global_sn(proposal.position).unwrap()
                    );
                    continue;
                }
                info!(
                    "Recovered {} -> {:?} origin_proposal={:?} epoch={} global_sn={}",
                    proposal,
                    proposal.digest(),
                    proposal.original_digest(&self.committee).unwrap(),
                    proposal.position.epoch,
                    self.epoch.global_sn(proposal.position).unwrap()
                );
            }
        }
        #[cfg(not(feature = "benchmark"))]
        let _ = message;
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
        let mut replay_context = (0, 0);
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
                // Replacing PBFT drops the previous epoch's in-memory evidence.
                self.pbft = Pbft::new(self.committee.clone(), segment);
                self.reset_view_change_timer();
                self.publish_view();
                debug!(
                    "Started segment epoch={} instance={} slots={}",
                    segment.epoch, segment.instance, segment.slots
                );
            }
            if replay_context != (self.epoch.number, self.pbft.view) {
                replay_context = (self.epoch.number, self.pbft.view);
                // Replay only the installed view; votes can precede its NewView on
                // another peer connection and must never vote before installation.
                let buffered = std::mem::take(&mut self.future_messages);
                for (key, (message, own_proposal)) in buffered {
                    let position = message.position();
                    if position.epoch == self.epoch.number
                        && (matches!(
                            &message,
                            PbftMessage::ViewChange(_) | PbftMessage::NewView(_)
                        ) || (position.view == self.pbft.view
                            && self.pbft.view == self.pbft.target_view))
                    {
                        Self::report(self.process(message, own_proposal).await);
                    } else if position.epoch > self.epoch.number
                        || (position.epoch == self.epoch.number
                            && position.view >= self.pbft.target_view)
                    {
                        self.future_messages.insert(key, (message, own_proposal));
                    }
                }
            }
            let result = tokio::select! {
                changed = self.rx_epoch.changed() => {
                    if changed.is_err() { return; }
                    Ok(())
                },
                _ = Self::wait_for_timeout(&mut self.view_change_timer) => self.on_view_change_timeout().await,
                Some(message) = self.rx_messages.recv() => self.receive(message, false).await,
                Some(proposal) = self.rx_proposer.recv() => self.receive(PbftMessage::PrePrepare(proposal), true).await,
                _ = cleanup_timer.tick() => Ok(()),
            };
            Self::report(result);
            self.cleanup();
        }
    }
}
