use super::*;
use crate::common::{committee_with_base_port, keys};
use futures::SinkExt;
use std::fs;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn three_of_four_nodes_certify_complete_batch_without_any_data_layer_ports() {
    let committee = committee_with_base_port(16_000);
    let owner = keys()[0].0;
    let mut stores = Vec::new();
    let mut outputs = Vec::new();
    let mut feedback = Vec::new();
    let mut unused_ports = Vec::new();
    // Reserve every former worker port; complete nodes must not use them.
    for authority in committee.authorities.values() {
        for address in [
            authority.primary.worker_to_primary,
            authority.workers[&0].primary_to_worker,
            authority.workers[&0].worker_to_worker,
        ] {
            unused_ports.push(TcpListener::bind(address).await.unwrap());
        }
    }
    for (i, (name, secret)) in keys().into_iter().take(3).enumerate() {
        let path = format!(".db_test_direct_node_{}", i);
        let _ = fs::remove_dir_all(&path);
        let store = Store::new(&path).unwrap();
        let (tx_output, rx_output) = channel(100);
        let (tx_feedback, rx_feedback) = channel(100);
        Primary::spawn(
            KeyPair { name, secret },
            committee.clone(),
            Parameters {
                batch_size: 200,
                max_batch_delay: 100,
                ..Parameters::default()
            },
            store.clone(),
            tx_output,
            rx_feedback,
        );
        stores.push(store);
        outputs.push(rx_output);
        feedback.push(tx_feedback);
    }
    let address = committee.worker(&owner, &0).unwrap().transactions;
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
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    let transactions = vec![vec![7_u8; 100], vec![8_u8; 100]];
    for tx in &transactions {
        transport.send(Bytes::from(tx.clone())).await.unwrap();
    }
    let certificate = timeout(Duration::from_secs(5), async {
        loop {
            let cert = outputs[0].recv().await.unwrap();
            if cert.batch.author == owner && cert.batch.transactions == transactions {
                break cert;
            }
        }
    })
    .await
    .unwrap();
    assert!(certificate.verify(&committee).is_ok());
    assert_eq!(certificate.votes.len(), 3);
    // Every live node must have the full proposal available when it casts its vote.
    for store in &mut stores {
        let bytes = store
            .read(certificate.batch.id.to_vec())
            .await
            .unwrap()
            .unwrap();
        let batch: Batch = bincode::deserialize(&bytes).unwrap();
        assert_eq!(batch.transactions, transactions);
    }
}
