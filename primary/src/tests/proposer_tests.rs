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
    Proposer::spawn(
        name,
        0,
        SignatureService::new(secret),
        32,
        20,
        rx_committed,
        rx_transactions,
        tx_proposals,
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
    tx_committed.send(2).await.unwrap();
    assert!(timeout(Duration::from_millis(50), proposals.recv())
        .await
        .is_err());
    tx_committed.send(1).await.unwrap();
    let second = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.position.sequence, 2);
    assert_eq!(second.batch.transactions, vec![vec![7; 32]]);
    second.verify(&committee()).unwrap();
    tx_committed.send(1).await.unwrap();
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
    Proposer::spawn(
        name,
        0,
        SignatureService::new(secret),
        32,
        1_000_000,
        rx_committed,
        rx_transactions,
        tx_proposals,
    );
    tx_transactions.send(vec![7; 32]).await.unwrap();
    let proposal = timeout(Duration::from_secs(1), proposals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proposal.position.sequence, 1);
    assert_eq!(proposal.batch.transactions, vec![vec![7; 32]]);
}
