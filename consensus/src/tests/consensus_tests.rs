// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SignatureService};
use primary::{leader, Batch, Commit, PrePrepare};
use rand::{rngs::StdRng, SeedableRng};
use std::collections::HashMap;
use tokio::sync::mpsc::channel;
use tokio::time::{timeout, Duration};

async fn fixtures() -> (Committee, Vec<CommittedBatch>) {
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
    let owner = leader(&committee);
    let mut decisions = Vec::new();
    for sequence in 1..=3 {
        let signer = signers.iter_mut().find(|(name, _)| *name == owner).unwrap();
        let proposal = PrePrepare::new(
            sequence,
            Batch::new(vec![vec![sequence as u8]]),
            owner,
            &mut signer.1,
        )
        .await;
        let mut commits = Vec::new();
        for (name, signatures) in signers.iter_mut().take(3) {
            commits.push(Commit::new(&proposal, *name, signatures).await);
        }
        decisions.push(CommittedBatch { proposal, commits });
    }
    (committee, decisions)
}

#[tokio::test]
async fn delivers_contiguous_prefix_despite_reordering_and_retries() {
    let (committee, decisions) = fixtures().await;
    let (tx, rx) = channel(10);
    let (output, mut delivered) = channel(10);
    Consensus::spawn(committee, rx, output);
    tx.send(decisions[1].clone()).await.unwrap();
    tx.send(decisions[1].clone()).await.unwrap();
    assert!(timeout(Duration::from_millis(50), delivered.recv())
        .await
        .is_err());
    tx.send(decisions[0].clone()).await.unwrap();
    tx.send(decisions[0].clone()).await.unwrap();
    tx.send(decisions[2].clone()).await.unwrap();
    drop(tx);
    for sequence in 1..=3 {
        let decision = timeout(Duration::from_secs(1), delivered.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decision.proposal.position.sequence, sequence);
    }
    assert!(timeout(Duration::from_secs(1), delivered.recv())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn invalid_quorum_never_reaches_application_output() {
    let (committee, mut decisions) = fixtures().await;
    let (tx, rx) = channel(10);
    let (output, mut delivered) = channel(10);
    Consensus::spawn(committee, rx, output);
    let mut invalid = decisions[0].clone();
    invalid.commits = vec![invalid.commits[0].clone(); 3];
    tx.send(invalid).await.unwrap();
    tx.send(decisions.remove(1)).await.unwrap();
    assert!(timeout(Duration::from_millis(50), delivered.recv())
        .await
        .is_err());
    tx.send(decisions.remove(0)).await.unwrap();
    let decision = timeout(Duration::from_secs(1), delivered.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.proposal.position.sequence, 1);
}
