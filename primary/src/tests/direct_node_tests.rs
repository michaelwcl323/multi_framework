use super::*;
use crate::common::{committee_with_base_port, keys};
use crypto::Hash as _;
use futures::SinkExt;
use std::fs;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn two_nodes_cannot_commit_then_third_node_completes_all_three_phases() {
    let committee = committee_with_base_port(16_000);
    let leader = leader(&committee);
    let offline = *committee.authorities.keys().last().unwrap();
    let mut live_keys: Vec<_> = keys()
        .into_iter()
        .filter(|(name, _)| *name != offline)
        .collect();
    live_keys.sort_by_key(|(name, _)| *name);
    assert_eq!(live_keys[0].0, leader);
    let third = live_keys.pop().unwrap();
    let ingress = live_keys[1].0; // Exercise a backup's client forwarding path.
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
        max_batch_delay: 500,
        ..Parameters::default()
    };
    for (i, (name, secret)) in live_keys.into_iter().enumerate() {
        let path = format!(".db_test_pbft_node_{}", i);
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
    let address = committee.worker(&ingress, &0).unwrap().transactions;
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
    let transactions = vec![vec![7_u8; 100], vec![8_u8; 100]];
    for tx in &transactions {
        client.send(Bytes::from(tx.clone())).await.unwrap();
    }
    for output in &mut outputs {
        assert!(
            timeout(Duration::from_millis(150), output.recv())
                .await
                .is_err(),
            "Two of four replicas must not commit"
        );
    }
    let path = ".db_test_pbft_node_2";
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
    let mut expected = None;
    for output in &mut outputs {
        let decision = timeout(Duration::from_secs(5), output.recv())
            .await
            .unwrap()
            .unwrap();
        decision.verify(&committee).unwrap();
        assert_eq!(decision.proposal.position.sequence, 1);
        assert_eq!(decision.proposal.batch.transactions, transactions);
        assert_eq!(decision.commits.len(), 3);
        let digest = decision.proposal.digest();
        if let Some(expected) = &expected {
            assert_eq!(&digest, expected);
        }
        expected = Some(digest);
    }
    for store in &mut stores {
        let bytes = store
            .read(expected.clone().unwrap().to_vec())
            .await
            .unwrap()
            .unwrap();
        let proposal: crate::PrePrepare = bincode::deserialize(&bytes).unwrap();
        assert_eq!(proposal.batch.transactions, transactions);
    }
}
