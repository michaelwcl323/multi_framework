// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, PublicKey, SignatureService};
use primary::{leader, Batch, Commit, Position, PrePrepare, SEQUENCE_WINDOW};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::collections::HashMap;
use tokio::sync::mpsc::channel;
use tokio::time::{timeout, Duration};

fn new_order() -> GlobalOrder {
    GlobalOrder::new(Epoch::new(0, 12, 4).unwrap())
}

type Signers = Vec<(PublicKey, SignatureService)>;

fn setup() -> (Committee, Signers) {
    let mut rng = StdRng::from_seed([0; 32]);
    let keys: Vec<_> = (0..4).map(|_| generate_keypair(&mut rng)).collect();
    let committee = Committee {
        authorities: keys
            .iter()
            .map(|(name, _)| {
                (
                    *name,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "127.0.0.1:0".parse().unwrap(),
                            worker_to_primary: "127.0.0.1:0".parse().unwrap(),
                        },
                        workers: HashMap::new(),
                    },
                )
            })
            .collect(),
    };
    let signers = keys
        .into_iter()
        .map(|(name, secret)| (name, SignatureService::new(secret)))
        .collect();
    (committee, signers)
}

async fn decision(
    committee: &Committee,
    signers: &mut Signers,
    instance: u64,
    sequence: u64,
    transactions: Vec<Vec<u8>>,
) -> CommittedBatch {
    let owner = leader(committee, instance).unwrap();
    let signer = signers.iter_mut().find(|(name, _)| *name == owner).unwrap();
    let proposal = PrePrepare::new(
        Position::for_instance(instance, sequence),
        Batch::new(transactions),
        owner,
        &mut signer.1,
    )
    .await;
    let mut commits = Vec::new();
    for (name, signatures) in signers.iter_mut().take(3) {
        commits.push(Commit::new(&proposal, *name, signatures).await);
    }
    CommittedBatch { proposal, commits }
}

async fn fixtures() -> (Committee, Vec<CommittedBatch>) {
    let (committee, mut signers) = setup();
    let mut decisions = Vec::new();
    for sequence in 1..=3 {
        for instance in 0..committee.size() as u64 {
            decisions.push(
                decision(
                    &committee,
                    &mut signers,
                    instance,
                    sequence,
                    vec![vec![instance as u8, sequence as u8]],
                )
                .await,
            );
        }
    }
    (committee, decisions)
}

#[test]
fn fixed_interleaving_has_checked_unique_global_positions() {
    for sequence in 1..=3 {
        for instance in 0..4 {
            assert_eq!(
                Position::for_instance(instance, sequence)
                    .global_sequence(4, 12)
                    .unwrap(),
                (sequence - 1) * 4 + instance + 1
            );
        }
    }
    assert!(Position::for_instance(4, 1).global_sequence(4, 12).is_err());
    assert!(Position::new(1).global_sequence(0, 12).is_err());
    assert!(Position::new(0).global_sequence(4, 12).is_err());
    assert_eq!(
        Position::for_epoch(1, 0, 1).global_sequence(4, 12).unwrap(),
        13
    );
    assert!(matches!(
        Position::for_epoch(u64::MAX, 0, 1).global_sequence(4, 12),
        Err(PbftError::GlobalSequenceOverflow(_))
    ));
    assert_eq!(
        Position::new(u64::MAX)
            .global_sequence(1, u64::MAX)
            .unwrap(),
        u64::MAX
    );
    assert!(matches!(
        Position::new(u64::MAX).global_sequence(4, 12),
        Err(PbftError::OutsideSegment(_))
    ));
}

#[tokio::test]
async fn later_instances_and_rounds_wait_for_every_global_hole() {
    let (committee, decisions) = fixtures().await;
    let mut order = new_order();
    for index in [2, 4, 1] {
        order.insert(&committee, decisions[index].clone()).unwrap();
    }
    assert!(order.pop_next().is_none());
    order.insert(&committee, decisions[0].clone()).unwrap();
    for global in 1..=3 {
        assert_eq!(order.pop_next().unwrap().global_sequence, global);
    }
    assert!(order.pop_next().is_none()); // Missing instance 3, sequence 1: global 4.
    assert_eq!(order.delivered, 3);
    assert!(order.pending.contains_key(&5));
    order.insert(&committee, decisions[3].clone()).unwrap();
    for global in 4..=5 {
        assert_eq!(order.pop_next().unwrap().global_sequence, global);
    }
}

#[tokio::test]
async fn all_replicas_derive_the_same_log_despite_arrival_order_and_duplicates() {
    let (committee, decisions) = fixtures().await;
    let expected: Vec<_> = decisions
        .iter()
        .enumerate()
        .map(|(i, decision)| ((i + 1) as u64, decision.proposal.digest()))
        .collect();
    for seed in 0..16 {
        let mut incoming = decisions.clone();
        incoming.extend(decisions.clone());
        incoming.shuffle(&mut StdRng::seed_from_u64(seed));
        let mut order = new_order();
        let mut actual = Vec::new();
        for decision in incoming {
            order.insert(&committee, decision).unwrap();
            while let Some(ordered) = order.pop_next() {
                actual.push((ordered.global_sequence, ordered.committed.proposal.digest()));
            }
        }
        assert_eq!(actual, expected);
        assert!(order.pending.is_empty());
        assert_eq!(order.delivered, 12);
    }
}

#[tokio::test]
async fn invalid_quorum_cannot_unblock_a_global_slot() {
    let (committee, decisions) = fixtures().await;
    let mut order = new_order();
    order.insert(&committee, decisions[1].clone()).unwrap();
    let mut invalid = decisions[0].clone();
    invalid.commits = vec![invalid.commits[0].clone(); 3];
    assert!(order.insert(&committee, invalid).is_err());
    assert!(order.pop_next().is_none());
    assert_eq!(order.delivered, 0);
    order.insert(&committee, decisions[0].clone()).unwrap();
    assert_eq!(order.pop_next().unwrap().global_sequence, 1);
    assert_eq!(order.pop_next().unwrap().global_sequence, 2);
}

#[tokio::test]
async fn an_empty_batch_consumes_its_global_slot_and_releases_following_data() {
    let (committee, mut signers) = setup();
    let empty = decision(&committee, &mut signers, 0, 1, vec![]).await;
    let payload = decision(&committee, &mut signers, 1, 1, vec![vec![7; 10]]).await;
    let mut order = new_order();
    order.insert(&committee, payload).unwrap();
    assert!(order.pop_next().is_none());
    order.insert(&committee, empty).unwrap();
    let first = order.pop_next().unwrap();
    assert_eq!(first.global_sequence, 1);
    assert!(first.committed.proposal.batch.transactions.is_empty());
    let second = order.pop_next().unwrap();
    assert_eq!(second.global_sequence, 2);
    assert_eq!(
        second.committed.proposal.batch.transactions,
        vec![vec![7; 10]]
    );
}

#[tokio::test]
async fn conflicting_proofs_cannot_replace_a_buffered_global_value() {
    let (committee, mut signers) = setup();
    let first = decision(&committee, &mut signers, 0, 1, vec![]).await;
    let accepted = decision(&committee, &mut signers, 1, 1, vec![vec![7]]).await;
    let conflicting = decision(&committee, &mut signers, 1, 1, vec![vec![8]]).await;
    let digest = accepted.proposal.digest();
    let mut order = new_order();
    order.insert(&committee, accepted.clone()).unwrap();
    order.insert(&committee, accepted).unwrap(); // Duplicate before delivery.
    assert!(matches!(
        order.insert(&committee, conflicting),
        Err(PbftError::ConflictingGlobalDecision(2))
    ));
    order.insert(&committee, first.clone()).unwrap();
    order.pop_next().unwrap();
    assert_eq!(
        order.pop_next().unwrap().committed.proposal.digest(),
        digest
    );
    order.insert(&committee, first).unwrap(); // Duplicate after delivery.
    assert!(order.pop_next().is_none());
}

#[tokio::test]
async fn distant_verified_decisions_are_retained_and_overflow_is_rejected() {
    let (committee, mut signers) = setup();
    let epoch_length = 4 * (SEQUENCE_WINDOW + 2);
    let future = decision(&committee, &mut signers, 1, SEQUENCE_WINDOW + 2, vec![]).await;
    let global = future
        .proposal
        .position
        .global_sequence(4, epoch_length)
        .unwrap();
    let mut order = GlobalOrder::new(Epoch::new(0, epoch_length, 4).unwrap());
    order.insert(&committee, future).unwrap();
    assert!(order.pending.contains_key(&global));
    assert!(order.pop_next().is_none());
    let overflow = decision(&committee, &mut signers, 0, u64::MAX, vec![]).await;
    assert!(matches!(
        order.insert(&committee, overflow),
        Err(PbftError::OutsideSegment(_))
    ));
    assert_eq!(order.pending.len(), 1);
}

#[tokio::test]
async fn application_output_and_epoch_barrier_follow_only_the_global_prefix() {
    let (committee, decisions) = fixtures().await;
    let (tx, rx) = channel(10);
    let (output, mut delivered) = channel(10);
    let (tx_progress, progress) = watch::channel(0);
    Consensus::spawn(committee, 4, tx_progress, rx, output);
    for index in [3, 1, 2] {
        tx.send(decisions[index].clone()).await.unwrap();
    }
    assert!(timeout(Duration::from_millis(40), delivered.recv())
        .await
        .is_err());
    assert_eq!(*progress.borrow(), 0);
    tx.send(decisions[0].clone()).await.unwrap();
    tx.send(decisions[0].clone()).await.unwrap();
    drop(tx);
    for global in 1..=4 {
        let ordered = timeout(Duration::from_secs(1), delivered.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ordered.global_sequence, global);
        assert_eq!(ordered.committed.proposal.position.instance, global - 1);
    }
    assert!(timeout(Duration::from_secs(1), delivered.recv())
        .await
        .unwrap()
        .is_none());
    assert_eq!(*progress.borrow(), 1);
}

#[tokio::test]
async fn recovered_proof_keeps_global_position_and_old_view_replay_is_idempotent() {
    let (committee, mut signers) = setup();
    let original = decision(&committee, &mut signers, 0, 1, vec![vec![9; 64]]).await;
    let new_primary = primary::primary(&committee, 0, 1).unwrap();
    let signer = signers
        .iter_mut()
        .find(|(name, _)| *name == new_primary)
        .unwrap();
    let recovered = PrePrepare::recover(&original.proposal, 1, new_primary, &mut signer.1).await;
    let mut votes = Vec::new();
    for (name, signer) in signers.iter_mut().take(3) {
        votes.push(Commit::new(&recovered, *name, signer).await);
    }
    let proof = CommittedBatch {
        proposal: recovered,
        commits: votes,
    };
    proof.verify(&committee).unwrap();
    let mut order = new_order();
    order.insert(&committee, proof).unwrap();
    order.insert(&committee, original.clone()).unwrap();
    assert_eq!(order.pending.len(), 1);
    let output = order.pop_next().unwrap();
    assert_eq!(output.global_sequence, 1);
    assert_eq!(output.committed.proposal.position.view, 1);
    assert_eq!(
        output.committed.proposal.batch.id,
        original.proposal.batch.id
    );
    order.insert(&committee, original).unwrap();
    assert!(order.pop_next().is_none());
    let conflicting = decision(&committee, &mut signers, 0, 1, vec![vec![8; 64]]).await;
    assert!(matches!(
        order.insert(&committee, conflicting),
        Err(PbftError::ConflictingGlobalDecision(1))
    ));
}
