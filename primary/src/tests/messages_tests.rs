use super::*;
use crate::common::{batch, certificate, committee};

#[test]
fn signed_batch_and_certificate_carry_complete_transactions() {
    let proposal = batch();
    assert!(!proposal.transactions.is_empty());
    assert!(proposal.verify(&committee()).is_ok());
    let cert = certificate(&proposal);
    assert!(cert.verify(&committee()).is_ok());
    let bytes = bincode::serialize(&cert).unwrap();
    let decoded: Certificate = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded.batch.transactions, proposal.transactions);
}

#[test]
fn changing_transaction_bytes_invalidates_the_proposal() {
    let mut proposal = batch();
    proposal.transactions[0][0] ^= 1;
    assert!(proposal.verify(&committee()).is_err());
    // Recomputing the ID cannot repair the original author's signature.
    proposal.id = proposal.digest();
    assert!(proposal.verify(&committee()).is_err());
}

#[test]
fn transaction_boundaries_are_committed_by_the_batch_hash() {
    let mut first = batch();
    let mut second = first.clone();
    first.transactions = vec![vec![1], vec![2, 3]];
    second.transactions = vec![vec![1, 2], vec![3]];
    assert_ne!(first.digest(), second.digest());
}
