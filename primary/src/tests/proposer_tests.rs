// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee, keys};
use crate::messages::leader;
use tokio::sync::mpsc::channel;
use tokio::time::timeout;

#[tokio::test]
async fn proposer_waits_for_matching_commit_before_next_sequence() {
    let name = leader(&committee(), 0).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let (tx_committed, rx_committed) = channel(10);
    let (tx_transactions, rx_transactions) = channel(10);
    let (tx_proposals, mut proposals) = channel(10);
    let (_epoch, rx_epoch) = tokio::sync::watch::channel(0);
    Proposer::spawn(
        name,
        0,
        1000,
        4,
        rx_epoch,
        SignatureService::new(secret),
        32,
        20,
        rx_committed,
        rx_transactions,
        tx_proposals,
        None,
    );
    let first = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.position.sequence, 1);
    assert!(first.batch.transactions.is_empty());
    first.verify(&committee()).unwrap();
    tx_transactions.send(vec![7; 32]).await.unwrap();
    // A transport ACK, unrelated commit, or duplicate previous commit cannot release the slot.
    tx_committed
        .send(Position::for_instance(0, 2))
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(50), proposals.recv())
        .await
        .is_err());
    tx_committed
        .send(Position::for_instance(0, 1))
        .await
        .unwrap();
    let second = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.position.sequence, 2);
    assert_eq!(second.batch.transactions, vec![vec![7; 32]]);
    second.verify(&committee()).unwrap();
    tx_committed
        .send(Position::for_instance(0, 1))
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(50), proposals.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn full_batch_proposes_without_dag_parents_or_availability_ack() {
    let name = leader(&committee(), 0).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let (_tx_committed, rx_committed) = channel(1);
    let (tx_transactions, rx_transactions) = channel(1);
    let (tx_proposals, mut proposals) = channel(1);
    let (_epoch, rx_epoch) = tokio::sync::watch::channel(0);
    Proposer::spawn(
        name,
        0,
        1000,
        4,
        rx_epoch,
        SignatureService::new(secret),
        32,
        1_000_000,
        rx_committed,
        rx_transactions,
        tx_proposals,
        None,
    );
    tx_transactions.send(vec![7; 32]).await.unwrap();
    let proposal = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proposal.position.sequence, 1);
    assert_eq!(proposal.batch.transactions, vec![vec![7; 32]]);
}

#[tokio::test]
async fn finite_segment_waits_for_epoch_barrier_and_ignores_old_feedback() {
    let name = leader(&committee(), 0).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let (tx_epoch, rx_epoch) = tokio::sync::watch::channel(0);
    let (committed, rx_committed) = channel(10);
    let (transactions, rx_transactions) = channel(10);
    let (tx_core, mut proposals) = channel(10);
    Proposer::spawn(
        name,
        0,
        8,
        4,
        rx_epoch,
        SignatureService::new(secret),
        32,
        10,
        rx_committed,
        rx_transactions,
        tx_core,
        None,
    );
    for sequence in 1..=2 {
        let proposal = timeout(Duration::from_secs(1), proposals.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(proposal.position, Position::for_epoch(0, 0, sequence));
        assert!(proposal.batch.transactions.is_empty());
        committed.send(proposal.position).await.unwrap();
    }
    transactions.send(vec![9; 32]).await.unwrap();
    assert!(timeout(Duration::from_millis(50), proposals.recv())
        .await
        .is_err());
    tx_epoch.send(1).unwrap();
    let proposal = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proposal.position, Position::for_epoch(1, 0, 1));
    assert_eq!(proposal.batch.transactions, vec![vec![9; 32]]);
    committed.send(Position::for_epoch(0, 0, 1)).await.unwrap();
    assert!(timeout(Duration::from_millis(50), proposals.recv())
        .await
        .is_err());
    committed.send(proposal.position).await.unwrap();
    let second = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.position, Position::for_epoch(1, 0, 2));
}

#[tokio::test]
async fn segment_with_no_assigned_slots_never_proposes() {
    let name = leader(&committee(), 3).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let (tx_epoch, rx_epoch) = tokio::sync::watch::channel(0);
    let (_committed, rx_committed) = channel(10);
    let (_transactions, rx_transactions) = channel(10);
    let (tx_core, mut proposals) = channel(10);
    Proposer::spawn(
        name,
        3,
        2,
        4,
        rx_epoch,
        SignatureService::new(secret),
        32,
        1,
        rx_committed,
        rx_transactions,
        tx_core,
        None,
    );
    assert!(timeout(Duration::from_millis(30), proposals.recv())
        .await
        .is_err());
    tx_epoch.send(1).unwrap();
    assert!(timeout(Duration::from_millis(30), proposals.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn original_sender_stops_proposing_when_its_segment_leaves_view_zero() {
    let name = leader(&committee(), 0).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let (committed, rx_committed) = channel(10);
    let (_transactions, rx_transactions) = channel(10);
    let (tx_core, mut proposals) = channel(10);
    let (epoch, rx_epoch) = tokio::sync::watch::channel(0);
    let (view, rx_view) = tokio::sync::watch::channel((0, 0));
    Proposer::spawn(
        name,
        0,
        8,
        4,
        rx_epoch,
        SignatureService::new(secret),
        32,
        10,
        rx_committed,
        rx_transactions,
        tx_core,
        Some(rx_view),
    );
    let first = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    view.send((0, 1)).unwrap();
    committed.send(first.position).await.unwrap();
    assert!(timeout(Duration::from_millis(50), proposals.recv())
        .await
        .is_err());
    epoch.send(1).unwrap();
    let next = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.position, Position::for_epoch(1, 0, 1));
}
