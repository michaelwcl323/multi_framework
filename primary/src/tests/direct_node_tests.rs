use super::*;
use crate::common::{committee_with_base_port, keys};
use crate::{Commit, Instance, Position};
use crypto::{Digest, Hash as _};
use futures::SinkExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

async fn submit(committee: &Committee, name: crypto::PublicKey, transactions: &[Vec<u8>]) {
    let address = committee.worker(&name, &0).unwrap().transactions;
    let stream = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(stream) = TcpStream::connect(address).await {
                break stream;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut client = Framed::new(stream, LengthDelimitedCodec::new());
    for tx in transactions {
        client.send(Bytes::from(tx.clone())).await.unwrap();
    }
}

#[tokio::test]
async fn parallel_instances_reach_quorum_independently_with_one_leader_offline() {
    let committee = committee_with_base_port(16_000);
    let mut live_keys = keys();
    live_keys.sort_by_key(|(name, _)| *name);
    live_keys.pop(); // Instance 3 has no live leader, but its replicas still run.
    let names: Vec<_> = live_keys.iter().map(|(name, _)| *name).collect();
    let third = live_keys.pop().unwrap();
    let mut stores = Vec::new();
    let mut outputs = Vec::new();
    let mut unused_ports = Vec::new();
    for authority in committee.authorities.values() {
        for address in [
            authority.primary.worker_to_primary,
            authority.workers[&0].primary_to_worker,
            authority.workers[&0].worker_to_worker,
        ] {
            unused_ports.push(TcpListener::bind(address).await.unwrap());
        }
    }
    let parameters = Parameters {
        batch_size: 200,
        max_batch_delay: 10_000,
        ..Parameters::default()
    };
    for (i, (name, secret)) in live_keys.into_iter().enumerate() {
        let path = format!(".db_test_multibft_node_{}", i);
        let _ = fs::remove_dir_all(&path);
        let store = Store::new(&path).unwrap();
        let (tx, rx) = channel(100);
        Primary::spawn(
            KeyPair { name, secret },
            committee.clone(),
            parameters.clone(),
            store.clone(),
            tx,
        );
        stores.push(store);
        outputs.push(rx);
    }
    let transactions: Vec<_> = (0..3)
        .map(|instance| {
            vec![
                vec![7 + instance as u8 * 2; 100],
                vec![8 + instance as u8 * 2; 100],
            ]
        })
        .collect();
    for i in 0..2 {
        submit(&committee, names[i], &transactions[i]).await;
    }
    for output in &mut outputs {
        assert!(
            timeout(Duration::from_millis(150), output.recv())
                .await
                .is_err(),
            "Two of four replicas must not commit any instance"
        );
    }
    let path = ".db_test_multibft_node_2";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let (tx, rx) = channel(100);
    Primary::spawn(
        KeyPair {
            name: third.0,
            secret: third.1,
        },
        committee.clone(),
        parameters,
        store.clone(),
        tx,
    );
    stores.push(store);
    outputs.push(rx);
    submit(&committee, names[2], &transactions[2]).await;

    let mut expected: Option<BTreeMap<Instance, Digest>> = None;
    for output in &mut outputs {
        let decisions = timeout(Duration::from_secs(5), async {
            let mut decisions = BTreeMap::new();
            while decisions.len() < 3 {
                let decision = output.recv().await.unwrap();
                decision.verify(&committee).unwrap();
                let position = decision.proposal.position;
                assert!(position.instance < 3);
                assert_eq!(position.sequence, 1);
                assert_eq!(
                    decision.proposal.batch.transactions,
                    transactions[position.instance as usize]
                );
                assert_eq!(decision.commits.len(), 3);
                assert!(decisions
                    .insert(position.instance, decision.proposal.digest())
                    .is_none());
            }
            decisions
        })
        .await
        .unwrap();
        if let Some(expected) = &expected {
            assert_eq!(&decisions, expected);
        }
        expected = Some(decisions);
    }
    // Persist only the final commit proof, independently for each instance.
    for store in &mut stores {
        for (instance, digest) in expected.as_ref().unwrap() {
            assert!(store.read(digest.to_vec()).await.unwrap().is_none());
            let position = Position::for_instance(*instance, 1);
            for phase in [1, 2] {
                let mut key = b"pbft/local-vote/".to_vec();
                key.extend_from_slice(&bincode::serialize(&position).unwrap());
                key.push(phase);
                assert!(store.read(key).await.unwrap().is_none());
            }
            let mut key = b"pbft/committed/".to_vec();
            key.extend_from_slice(&bincode::serialize(&position).unwrap());
            let bytes = store.read(key).await.unwrap().unwrap();
            let proof: Vec<Commit> = bincode::deserialize(&bytes).unwrap();
            assert_eq!(bytes, bincode::serialize(&proof).unwrap());
            assert_eq!(proof.len(), 3);
            let mut voters = BTreeSet::new();
            let mut weight = 0;
            for vote in proof {
                vote.verify(&committee).unwrap();
                assert_eq!(vote.position, position);
                assert_eq!(&vote.proposal, digest);
                assert!(voters.insert(vote.author));
                weight += committee.stake(&vote.author);
            }
            assert!(weight >= committee.quorum_threshold());
        }
    }
    // Continuing instance 1 must not wait for instance 0 or the absent leader 3.
    let next = vec![vec![21; 100], vec![22; 100]];
    submit(&committee, names[1], &next).await;
    for output in &mut outputs {
        let decision = timeout(Duration::from_secs(3), output.recv())
            .await
            .unwrap()
            .unwrap();
        decision.verify(&committee).unwrap();
        assert_eq!(decision.proposal.position, Position::for_instance(1, 2));
        assert_eq!(decision.proposal.batch.transactions, next);
    }
}
