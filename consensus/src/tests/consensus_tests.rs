use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SignatureService};
use primary::{leader, Batch, Commit, Position, PrePrepare};
use rand::{rngs::StdRng, SeedableRng};
use std::collections::HashMap;
use tokio::sync::mpsc::channel;
use tokio::time::{sleep, timeout, Duration};

async fn fixtures(length: u64, epochs: u64) -> (Committee, Vec<CommittedBatch>) {
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
    let mut signers: Vec<_> = keys
        .into_iter()
        .map(|(name, secret)| (name, SignatureService::new(secret)))
        .collect();
    let mut decisions = Vec::new();
    for number in 0..epochs {
        let epoch = Epoch::new(number, length, 4).unwrap();
        for sn in epoch.first_sn()..=epoch.last_sn() {
            let position = epoch.position(sn).unwrap();
            let owner = leader(&committee, position.instance).unwrap();
            let signer = signers.iter_mut().find(|(name, _)| *name == owner).unwrap();
            let transactions = if sn % 2 == 0 {
                Vec::new()
            } else {
                vec![vec![sn as u8]]
            };
            let proposal =
                PrePrepare::new(position, Batch::new(transactions), owner, &mut signer.1).await;
            let mut commits = Vec::new();
            for (name, signatures) in signers.iter_mut().take(3) {
                commits.push(Commit::new(&proposal, *name, signatures).await);
            }
            decisions.push(CommittedBatch { proposal, commits });
        }
    }
    (committee, decisions)
}

#[tokio::test]
async fn delivers_global_round_robin_order_despite_reordering_and_retries() {
    let (committee, decisions) = fixtures(8, 1).await;
    let (tx, rx) = channel(32);
    let (output, mut delivered) = channel(32);
    let (epoch, mut progress) = watch::channel(0);
    Consensus::spawn(committee, 8, epoch, rx, output);
    tx.send(decisions[1].clone()).await.unwrap();
    tx.send(decisions[1].clone()).await.unwrap();
    assert!(timeout(Duration::from_millis(30), delivered.recv())
        .await
        .is_err());
    for index in [0, 3, 2, 7, 6, 5, 4] {
        tx.send(decisions[index].clone()).await.unwrap();
    }
    for sn in 1..=8 {
        let decision = timeout(Duration::from_secs(1), delivered.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            decision.proposal.position,
            Epoch::new(0, 8, 4).unwrap().position(sn).unwrap()
        );
    }
    timeout(Duration::from_secs(1), progress.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*progress.borrow(), 1);
    drop(tx);
    assert!(timeout(Duration::from_secs(1), delivered.recv())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn missing_slot_blocks_epoch_and_invalid_quorum_does_not_fill_it() {
    let (committee, decisions) = fixtures(7, 2).await;
    let (tx, rx) = channel(32);
    let (output, mut delivered) = channel(32);
    let (epoch, mut progress) = watch::channel(0);
    Consensus::spawn(committee, 7, epoch, rx, output);
    for decision in &decisions[1..7] {
        tx.send(decision.clone()).await.unwrap();
    }
    tx.send(decisions[6].clone()).await.unwrap();
    // An early next-epoch decision cannot bypass the missing current-epoch slot.
    tx.send(decisions[7].clone()).await.unwrap();
    let mut invalid = decisions[0].clone();
    invalid.commits = vec![invalid.commits[0].clone(); 3];
    tx.send(invalid).await.unwrap();
    assert!(timeout(Duration::from_millis(30), delivered.recv())
        .await
        .is_err());
    assert_eq!(*progress.borrow(), 0);
    tx.send(decisions[0].clone()).await.unwrap();
    for sn in 1..=7 {
        let decision = timeout(Duration::from_secs(1), delivered.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            decision.proposal.position,
            Epoch::new(0, 7, 4).unwrap().position(sn).unwrap()
        );
    }
    timeout(Duration::from_secs(1), progress.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*progress.borrow(), 1);
    tx.send(decisions[0].clone()).await.unwrap();
    tx.send(decisions[7].clone()).await.unwrap();
    let next = timeout(Duration::from_secs(1), delivered.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.proposal.position, Position::for_epoch(1, 0, 1));
    assert!(timeout(Duration::from_millis(30), delivered.recv())
        .await
        .is_err());
    assert_eq!(*progress.borrow(), 1);
}

#[tokio::test]
async fn completed_epochs_reset_local_sequences_without_duplicate_delivery() {
    let (committee, decisions) = fixtures(7, 3).await;
    let (tx, rx) = channel(64);
    let (output, mut delivered) = channel(64);
    let (epoch, mut progress) = watch::channel(0);
    Consensus::spawn(committee, 7, epoch, rx, output);
    for number in 0..3 {
        for decision in decisions[number * 7..(number + 1) * 7].iter().rev() {
            tx.send(decision.clone()).await.unwrap();
            tx.send(decision.clone()).await.unwrap();
        }
        for sn in number as u64 * 7 + 1..=(number as u64 + 1) * 7 {
            let decision = timeout(Duration::from_secs(1), delivered.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                Epoch::new(number as u64, 7, 4)
                    .unwrap()
                    .global_sn(decision.proposal.position),
                Some(sn)
            );
        }
        timeout(Duration::from_secs(1), progress.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*progress.borrow(), number as u64 + 1);
    }
    drop(tx);
    assert!(timeout(Duration::from_secs(1), delivered.recv())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn signed_decision_outside_segment_quota_is_rejected() {
    let (committee, decisions) = fixtures(12, 1).await;
    let (tx, rx) = channel(10);
    let (output, mut delivered) = channel(10);
    let (epoch, progress) = watch::channel(0);
    Consensus::spawn(committee, 7, epoch, rx, output);
    // Valid signatures, but segment 3 has only one slot in a seven-slot epoch.
    tx.send(decisions[7].clone()).await.unwrap();
    assert!(timeout(Duration::from_millis(30), delivered.recv())
        .await
        .is_err());
    assert_eq!(*progress.borrow(), 0);
    tx.send(decisions[0].clone()).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), delivered.recv())
            .await
            .unwrap()
            .unwrap()
            .proposal
            .position,
        Position::for_epoch(0, 0, 1)
    );
}

#[tokio::test]
async fn epoch_barrier_waits_for_last_slot_to_enter_output_channel() {
    let (committee, decisions) = fixtures(2, 1).await;
    let (tx, rx) = channel(10);
    let (output, mut delivered) = channel(1);
    let (epoch, mut progress) = watch::channel(0);
    Consensus::spawn(committee, 2, epoch, rx, output);
    for decision in decisions {
        tx.send(decision).await.unwrap();
    }
    sleep(Duration::from_millis(30)).await;
    assert_eq!(*progress.borrow(), 0);
    assert_eq!(
        delivered.recv().await.unwrap().proposal.position.instance,
        0
    );
    timeout(Duration::from_secs(1), progress.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*progress.borrow(), 1);
    assert_eq!(
        delivered.recv().await.unwrap().proposal.position.instance,
        1
    );
}
