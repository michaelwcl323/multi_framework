// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{commits, committee_with_base_port, keys, prepares, proposal};
use crate::primary::PrimaryMessage;
use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use std::error::Error;
use std::fs;
use tokio::sync::mpsc::channel;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
struct Observer(Sender<PrimaryMessage>);

fn evidence_key(prefix: &[u8], position: Position) -> PbftResult<Vec<u8>> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&bincode::serialize(&position)?);
    Ok(key)
}

#[async_trait]
impl MessageHandler for Observer {
    async fn dispatch(&self, writer: &mut Writer, bytes: Bytes) -> Result<(), Box<dyn Error>> {
        self.0.send(bincode::deserialize(&bytes)?).await?;
        writer.send(Bytes::from_static(b"Ack")).await?;
        Ok(())
    }
}

#[tokio::test]
async fn backup_broadcasts_prepare_and_commit_to_every_other_replica() {
    let committee = committee_with_base_port(17_000);
    let leader = crate::leader(&committee, 0).unwrap();
    let (name, secret) = keys()
        .into_iter()
        .find(|(name, _)| *name != leader)
        .unwrap();
    let mut observers = Vec::new();
    for (_, address) in committee.others_primaries(&name) {
        let (tx, rx) = channel(10);
        NetworkReceiver::spawn(address.primary_to_primary, Observer(tx));
        observers.push(rx);
    }
    let path = ".db_test_pbft_broadcast";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let (tx, rx) = channel(10);
    let (_tx_proposer, rx_proposer) = channel(10);
    let (tx_output, mut output) = channel(10);
    let (_epoch, rx_epoch) = tokio::sync::watch::channel(0);
    Core::spawn(
        name,
        committee.clone(),
        0,
        1000,
        rx_epoch,
        store.clone(),
        SignatureService::new(secret),
        50,
        rx,
        rx_proposer,
        tx_output,
        None,
        Network::spawn(name, &committee),
    );
    let pp = proposal(1, vec![vec![7; 10]]);
    tx.send(PbftMessage::PrePrepare(pp.clone())).await.unwrap();
    for observer in &mut observers {
        match timeout(Duration::from_secs(3), observer.recv())
            .await
            .unwrap()
            .unwrap()
        {
            PrimaryMessage::Pbft(PbftMessage::Prepare(vote)) => {
                vote.verify(&committee).unwrap();
                assert_eq!(vote.author, name);
                assert_eq!(vote.proposal, pp.digest());
            }
            message => panic!("Expected all-to-all Prepare, received {:?}", message),
        }
    }
    // Our local Prepare is already counted: only two additional voters are needed.
    for vote in prepares(&pp)
        .into_iter()
        .filter(|vote| vote.author != name)
        .take(2)
    {
        tx.send(PbftMessage::Prepare(vote)).await.unwrap();
    }
    for observer in &mut observers {
        match timeout(Duration::from_secs(3), observer.recv())
            .await
            .unwrap()
            .unwrap()
        {
            PrimaryMessage::Pbft(PbftMessage::Commit(vote)) => {
                vote.verify(&committee).unwrap();
                assert_eq!(vote.author, name);
                assert_eq!(vote.proposal, pp.digest());
            }
            message => panic!("Expected all-to-all Commit, received {:?}", message),
        }
    }
    assert!(output.try_recv().is_err());
    // Commit is broadcast without creating a Prepared record or a final proof.
    assert!(store
        .read(evidence_key(b"pbft/prepared/", pp.position).unwrap())
        .await
        .unwrap()
        .is_none());
    let committed_key = evidence_key(b"pbft/committed/", pp.position).unwrap();
    assert!(store.read(committed_key.clone()).await.unwrap().is_none());
    // Our local Commit is also already counted.
    for vote in commits(&pp)
        .into_iter()
        .filter(|vote| vote.author != name)
        .take(2)
    {
        tx.send(PbftMessage::Commit(vote)).await.unwrap();
    }
    let decision = timeout(Duration::from_secs(3), output.recv())
        .await
        .unwrap()
        .unwrap();
    decision.verify(&committee).unwrap();
    assert_eq!(decision.proposal.digest(), pp.digest());
    // Preserve Step 5's final Commit-proof write after a quorum is reached.
    let votes: Vec<Commit> =
        bincode::deserialize(&store.read(committed_key).await.unwrap().unwrap()).unwrap();
    CommittedBatch {
        proposal: pp,
        commits: votes,
    }
    .verify(&committee)
    .unwrap();
    // No fourth certificate-broadcast phase follows the Commit messages.
    for observer in &mut observers {
        assert!(observer.try_recv().is_err());
    }
}

#[tokio::test]
async fn future_epoch_messages_wait_for_barrier_then_replay_once() {
    let committee = committee_with_base_port(18_000);
    let leader = crate::leader(&committee, 0).unwrap();
    let (name, secret) = keys()
        .into_iter()
        .find(|(name, _)| *name != leader)
        .unwrap();
    let mut observers = Vec::new();
    for (_, address) in committee.others_primaries(&name) {
        let (tx, rx) = channel(10);
        NetworkReceiver::spawn(address.primary_to_primary, Observer(tx));
        observers.push(rx);
    }
    let path = ".db_test_epoch_buffer";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let (tx, rx) = channel(32);
    let (_tx_proposer, rx_proposer) = channel(10);
    let (tx_output, mut output) = channel(10);
    let (epoch, rx_epoch) = tokio::sync::watch::channel(0);
    let mut progress = Core::spawn(
        name,
        committee.clone(),
        0,
        8,
        rx_epoch,
        store.clone(),
        SignatureService::new(secret),
        50,
        rx,
        rx_proposer,
        tx_output,
        None,
        Network::spawn(name, &committee),
    );
    let proposal =
        crate::common::proposal_at(crate::Position::for_epoch(1, 0, 1), vec![vec![5; 10]]);
    let mut messages = vec![PbftMessage::PrePrepare(proposal.clone())];
    messages.extend(
        prepares(&proposal)
            .into_iter()
            .take(3)
            .map(PbftMessage::Prepare),
    );
    messages.extend(
        commits(&proposal)
            .into_iter()
            .take(3)
            .map(PbftMessage::Commit),
    );
    // Repeated, authenticated future messages remain bounded and do not vote early.
    for message in &messages {
        tx.send(message.clone()).await.unwrap();
        tx.send(message.clone()).await.unwrap();
    }
    assert!(timeout(Duration::from_millis(50), output.recv())
        .await
        .is_err());
    for observer in &mut observers {
        assert!(observer.try_recv().is_err());
    }
    epoch.send(1).unwrap();
    let decision = timeout(Duration::from_secs(3), output.recv())
        .await
        .unwrap()
        .unwrap();
    decision.verify(&committee).unwrap();
    assert_eq!(decision.proposal.digest(), proposal.digest());
    let prepared_key = evidence_key(b"pbft/prepared/", proposal.position).unwrap();
    assert!(store.read(prepared_key.clone()).await.unwrap().is_none());
    for message in messages {
        tx.send(message).await.unwrap();
    }
    tx.send(PbftMessage::PrePrepare(crate::common::proposal_at(
        crate::Position::for_epoch(0, 0, 1),
        vec![],
    )))
    .await
    .unwrap();
    assert!(timeout(Duration::from_millis(50), output.recv())
        .await
        .is_err());
    epoch.send(2).unwrap();
    timeout(Duration::from_secs(2), async {
        while *progress.borrow_and_update() != (2, 0) {
            progress.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(store
        .read(evidence_key(b"pbft/committed/", proposal.position).unwrap())
        .await
        .unwrap()
        .is_some());
    assert!(store.read(prepared_key).await.unwrap().is_none());
    // A fresh epoch must report no previous-epoch in-memory evidence.
    for (author, secret) in keys().into_iter().filter(|(key, _)| *key != name).take(2) {
        let mut report = ViewChange {
            epoch: 2,
            instance: 0,
            view: 1,
            prepared: vec![],
            author,
            signature: crypto::Signature::default(),
        };
        report.signature = crypto::Signature::new(&report.digest(), &secret);
        tx.send(PbftMessage::ViewChange(report)).await.unwrap();
    }
    let report = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(PrimaryMessage::Pbft(PbftMessage::ViewChange(change))) =
                observers[0].recv().await
            {
                break change;
            }
        }
    })
    .await
    .unwrap();
    report.verify(&committee).unwrap();
    assert_eq!(report.epoch, 2);
    assert!(report.prepared.is_empty());
}

#[tokio::test]
async fn segment_primary_builds_new_view_and_replays_votes_that_arrive_before_it() {
    let committee = committee_with_base_port(19_000);
    let target_view = 2;
    let name = crate::primary(&committee, 0, target_view).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let mut observers = Vec::new();
    for (_, address) in committee.others_primaries(&name) {
        let (tx, rx) = channel(50);
        NetworkReceiver::spawn(address.primary_to_primary, Observer(tx));
        observers.push(rx);
    }
    let path = ".db_test_new_view_buffer";
    let _ = fs::remove_dir_all(path);
    let (tx, rx) = channel(50);
    let (_tx_proposer, rx_proposer) = channel(10);
    let (tx_output, mut output) = channel(10);
    let (_epoch, rx_epoch) = watch::channel(0);
    let progress = Core::spawn(
        name,
        committee.clone(),
        0,
        100,
        rx_epoch,
        Store::new(path).unwrap(),
        SignatureService::new(secret),
        50,
        rx,
        rx_proposer,
        tx_output,
        None,
        Network::spawn(name, &committee),
    );
    let original = proposal(1, vec![vec![7]]);
    tx.send(PbftMessage::PrePrepare(original.clone()))
        .await
        .unwrap();
    for vote in prepares(&original)
        .into_iter()
        .filter(|vote| vote.author != name)
        .take(2)
    {
        tx.send(PbftMessage::Prepare(vote)).await.unwrap();
    }
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let recovered = PrePrepare::recover(
        &original,
        target_view,
        name,
        &mut SignatureService::new(secret),
    )
    .await;
    for vote in prepares(&recovered)
        .into_iter()
        .filter(|vote| vote.author != name)
        .take(2)
    {
        tx.send(PbftMessage::Prepare(vote)).await.unwrap();
    }
    for vote in commits(&recovered)
        .into_iter()
        .filter(|vote| vote.author != name)
        .take(2)
    {
        tx.send(PbftMessage::Commit(vote)).await.unwrap();
    }
    assert!(timeout(Duration::from_millis(50), output.recv())
        .await
        .is_err());
    // f+1 reports cause the replica to freeze view zero and report its own
    // prepared proof. The new primary then obtains a full three-voter quorum.
    for (author, secret) in keys().into_iter().filter(|(key, _)| *key != name).take(2) {
        let mut report = ViewChange {
            epoch: 0,
            instance: 0,
            view: target_view,
            prepared: vec![],
            author,
            signature: crypto::Signature::default(),
        };
        report.signature = crypto::Signature::new(&report.digest(), &secret);
        tx.send(PbftMessage::ViewChange(report)).await.unwrap();
    }
    let decision = timeout(Duration::from_secs(3), output.recv())
        .await
        .unwrap()
        .unwrap();
    decision.verify(&committee).unwrap();
    assert_eq!(decision.proposal.position.view, target_view);
    assert_eq!(decision.proposal.batch.id, original.batch.id);
    assert_eq!(*progress.borrow(), (0, target_view));
    for observer in &mut observers {
        let view = timeout(Duration::from_secs(3), async {
            loop {
                if let Some(PrimaryMessage::Pbft(PbftMessage::NewView(view))) =
                    observer.recv().await
                {
                    break view;
                }
            }
        })
        .await
        .unwrap();
        view.verify(&committee).unwrap();
        assert_eq!(view.proposals.len(), 1);
    }
    // Old-view commits cannot emit a second decision.
    for commit in commits(&original) {
        tx.send(PbftMessage::Commit(commit)).await.unwrap();
    }
    assert!(timeout(Duration::from_millis(30), output.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn lagging_core_retains_imported_commit_evidence_in_memory_for_the_next_view_change() {
    let committee = committee_with_base_port(21_000);
    let name = crate::leader(&committee, 0).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let mut observers = Vec::new();
    for (_, address) in committee.others_primaries(&name) {
        let (tx, rx) = channel(32);
        NetworkReceiver::spawn(address.primary_to_primary, Observer(tx));
        observers.push(rx);
    }
    let original = proposal(1, vec![vec![7; 4096]]);
    let proof = crate::PreparedCertificate {
        proposal: original.clone(),
        prepares: prepares(&original).into_iter().take(3).collect(),
        commits: commits(&original).into_iter().take(3).collect(),
    };
    let mut reports = Vec::new();
    for (index, (author, secret)) in keys().into_iter().take(3).enumerate() {
        let mut report = ViewChange {
            epoch: 0,
            instance: 0,
            view: 1,
            prepared: if index == 0 {
                vec![proof.clone()]
            } else {
                vec![]
            },
            author,
            signature: crypto::Signature::default(),
        };
        report.signature = crypto::Signature::new(&report.digest(), &secret);
        reports.push(report);
    }
    let primary = crate::primary(&committee, 0, 1).unwrap();
    let (_, signer) = keys().into_iter().find(|(key, _)| *key == primary).unwrap();
    let view = NewView::new(
        &committee,
        reports,
        primary,
        &mut SignatureService::new(signer),
    )
    .await
    .unwrap();
    let path = ".db_test_imported_prepared";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let (tx, rx) = channel(32);
    let (_tx_proposer, rx_proposer) = channel(10);
    let (tx_output, mut output) = channel(10);
    let (_epoch, rx_epoch) = tokio::sync::watch::channel(0);
    Core::spawn(
        name,
        committee.clone(),
        0,
        1000,
        rx_epoch,
        store.clone(),
        SignatureService::new(secret),
        50,
        rx,
        rx_proposer,
        tx_output,
        None,
        Network::spawn(name, &committee),
    );
    tx.send(PbftMessage::NewView(view)).await.unwrap();
    let decision = timeout(Duration::from_secs(3), output.recv())
        .await
        .unwrap()
        .unwrap();
    decision.verify(&committee).unwrap();
    assert_eq!(decision.proposal.position, original.position);
    assert!(store
        .read(evidence_key(b"pbft/prepared/", original.position).unwrap())
        .await
        .unwrap()
        .is_none());
    for (author, secret) in keys().into_iter().filter(|(key, _)| *key != name).take(2) {
        let mut report = ViewChange {
            epoch: 0,
            instance: 0,
            view: 2,
            prepared: vec![],
            author,
            signature: crypto::Signature::default(),
        };
        report.signature = crypto::Signature::new(&report.digest(), &secret);
        tx.send(PbftMessage::ViewChange(report)).await.unwrap();
    }
    let report = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(PrimaryMessage::Pbft(PbftMessage::ViewChange(change))) =
                observers[0].recv().await
            {
                break change;
            }
        }
    })
    .await
    .unwrap();
    report.verify(&committee).unwrap();
    assert_eq!(report.prepared.len(), 1);
    assert_eq!(
        report.prepared[0].proposal.batch.transactions,
        original.batch.transactions
    );
    assert_eq!(report.prepared[0].commits.len(), 3);
    assert!(output.try_recv().is_err());
}
