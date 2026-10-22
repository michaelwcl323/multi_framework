// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{leader, Batch, Commit, CommittedBatch, Position, PrePrepare, Prepare};
use config::{Authority, Committee, PrimaryAddresses, WorkerAddresses};
use crypto::{generate_keypair, Hash as _, PublicKey, SecretKey, Signature};
use rand::{rngs::StdRng, SeedableRng as _};
use std::collections::HashMap;

pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

pub fn committee() -> Committee {
    Committee {
        authorities: keys()
            .iter()
            .enumerate()
            .map(|(i, (name, _))| {
                (
                    *name,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: format!("127.0.0.1:{}", 100 + i).parse().unwrap(),
                            worker_to_primary: format!("127.0.0.1:{}", 200 + i).parse().unwrap(),
                        },
                        workers: [(
                            0,
                            WorkerAddresses {
                                primary_to_worker: format!("127.0.0.1:{}", 300 + i)
                                    .parse()
                                    .unwrap(),
                                transactions: format!("127.0.0.1:{}", 400 + i).parse().unwrap(),
                                worker_to_worker: format!("127.0.0.1:{}", 500 + i).parse().unwrap(),
                            },
                        )]
                        .iter()
                        .cloned()
                        .collect::<HashMap<_, _>>(),
                    },
                )
            })
            .collect(),
    }
}

pub fn committee_with_base_port(base: u16) -> Committee {
    let mut committee = committee();
    for authority in committee.authorities.values_mut() {
        let primary = &mut authority.primary;
        primary
            .primary_to_primary
            .set_port(base + primary.primary_to_primary.port());
        primary
            .worker_to_primary
            .set_port(base + primary.worker_to_primary.port());
        for worker in authority.workers.values_mut() {
            worker
                .primary_to_worker
                .set_port(base + worker.primary_to_worker.port());
            worker
                .transactions
                .set_port(base + worker.transactions.port());
            worker
                .worker_to_worker
                .set_port(base + worker.worker_to_worker.port());
        }
    }
    committee
}

pub fn proposal(sequence: u64, transactions: Vec<Vec<u8>>) -> PrePrepare {
    proposal_for(0, sequence, transactions)
}

pub fn proposal_for(instance: u64, sequence: u64, transactions: Vec<Vec<u8>>) -> PrePrepare {
    let author = leader(&committee(), instance).unwrap();
    let (_, secret) = keys()
        .into_iter()
        .find(|(name, _)| *name == author)
        .unwrap();
    let mut proposal = PrePrepare {
        position: Position::for_instance(instance, sequence),
        batch: Batch::new(transactions),
        author,
        signature: Signature::default(),
    };
    proposal.signature = Signature::new(&proposal.digest(), &secret);
    proposal
}

pub fn prepares(proposal: &PrePrepare) -> Vec<Prepare> {
    keys()
        .into_iter()
        .map(|(author, secret)| {
            let mut vote = Prepare {
                position: proposal.position,
                proposal: proposal.digest(),
                author,
                signature: Signature::default(),
            };
            vote.signature = Signature::new(&vote.digest(), &secret);
            vote
        })
        .collect()
}

pub fn commits(proposal: &PrePrepare) -> Vec<Commit> {
    keys()
        .into_iter()
        .map(|(author, secret)| {
            let mut vote = Commit {
                position: proposal.position,
                proposal: proposal.digest(),
                author,
                signature: Signature::default(),
            };
            vote.signature = Signature::new(&vote.digest(), &secret);
            vote
        })
        .collect()
}

pub fn decision(sequence: u64) -> CommittedBatch {
    let proposal = proposal(sequence, vec![vec![sequence as u8; 10]]);
    CommittedBatch {
        commits: commits(&proposal).into_iter().take(3).collect(),
        proposal,
    }
}
