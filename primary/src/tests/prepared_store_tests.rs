use super::*;
use crate::common::{commits, committee, prepares, proposal};
use std::fs;

async fn stored(path: &str) -> (Store, PreparedCertificate, PreparedRef) {
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let proposal = proposal(1, vec![vec![7; 4096]]);
    let reference = PreparedRef::new(&proposal);
    let certificate = PreparedCertificate {
        prepares: prepares(&proposal).into_iter().take(3).collect(),
        commits: vec![],
        proposal,
    };
    store
        .write_checked(
            evidence_key(b"pbft/prepared/", reference.position).unwrap(),
            bincode::serialize(&certificate).unwrap(),
        )
        .await
        .unwrap();
    (store, certificate, reference)
}

#[tokio::test]
async fn loads_prepared_uncommitted_payload_and_checks_its_index() {
    let (mut store, expected, reference) = stored(".db_test_prepared_read").await;
    assert!(std::mem::size_of::<PreparedRef>() < 256);
    let loaded = load_certificate(&mut store, &committee(), &reference)
        .await
        .unwrap();
    assert_eq!(
        loaded.proposal.batch.transactions,
        expected.proposal.batch.transactions
    );
    assert_eq!(loaded.prepares.len(), 3);
    assert!(loaded.commits.is_empty());
    let mut wrong = reference;
    wrong.batch = crypto::Digest::default();
    assert!(matches!(
        load_certificate(&mut store, &committee(), &wrong).await,
        Err(PbftError::StorageFailure)
    ));
}

#[tokio::test]
async fn committed_index_requires_the_matching_complete_commit_proof() {
    let (mut store, expected, mut reference) = stored(".db_test_prepared_commit_read").await;
    reference.committed = true;
    assert!(matches!(
        load_certificate(&mut store, &committee(), &reference).await,
        Err(PbftError::StorageFailure)
    ));
    let key = evidence_key(b"pbft/committed/", reference.position).unwrap();
    let votes: Vec<_> = commits(&expected.proposal).into_iter().take(3).collect();
    store
        .write_checked(key.clone(), bincode::serialize(&votes).unwrap())
        .await
        .unwrap();
    let loaded = load_certificate(&mut store, &committee(), &reference)
        .await
        .unwrap();
    loaded.verify(&committee()).unwrap();
    assert_eq!(loaded.commits.len(), 3);
    store
        .write_checked(key, bincode::serialize(&votes[..2]).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        load_certificate(&mut store, &committee(), &reference).await,
        Err(PbftError::StorageFailure)
    ));
}

#[tokio::test]
async fn absent_or_corrupt_prepared_evidence_fails_instead_of_omitting_the_slot() {
    let (mut store, _, reference) = stored(".db_test_prepared_corrupt").await;
    let mut missing = reference.clone();
    missing.position.view = 2;
    assert!(matches!(
        load_certificate(&mut store, &committee(), &missing).await,
        Err(PbftError::StorageFailure)
    ));
    store
        .write_checked(
            evidence_key(b"pbft/prepared/", reference.position).unwrap(),
            vec![0, 1, 2],
        )
        .await
        .unwrap();
    assert!(matches!(
        load_certificate(&mut store, &committee(), &reference).await,
        Err(PbftError::StorageFailure)
    ));
}
