// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee, keys};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn propose_empty() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_batches, mut rx_batches) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* batch_size */ 1_000,
        /* max_batch_delay */ 20,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_batches,
    );

    // Ensure the proposer makes a correct empty batch.
    let batch = rx_batches.recv().await.unwrap();
    assert_eq!(batch.round, 1);
    assert!(batch.transactions.is_empty());
    assert!(batch.verify(&committee()).is_ok());
}

#[tokio::test]
async fn propose_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_batches, mut rx_batches) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* batch_size */ 32,
        /* max_batch_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_batches,
    );

    // Client transactions are proposed directly, with no availability ACK input.
    let transaction = vec![7_u8; 32];
    tx_our_digests.send(transaction.clone()).await.unwrap();
    let batch = tokio::time::timeout(std::time::Duration::from_secs(1), rx_batches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.round, 1);
    assert_eq!(batch.transactions, vec![transaction]);
    assert!(batch.verify(&committee()).is_ok());
}
