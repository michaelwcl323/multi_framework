// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{commits, committee_with_base_port, keys, prepares, proposal};
use async_trait::async_trait;
use futures::SinkExt;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use std::error::Error;
use std::fs;
use tokio::sync::mpsc::channel;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
struct Observer(Sender<PrimaryMessage>);

#[async_trait]
impl MessageHandler for Observer {
    async fn dispatch(&self, writer: &mut Writer, bytes: Bytes) -> Result<(), Box<dyn Error>> {
        self.0.send(bincode::deserialize(&bytes)?).await?;
        writer.send(Bytes::from_static(b"Ack")).await?;
        Ok(())
    }
}

#[tokio::test]
async fn backup_broadcasts_prepare_and_commit_to_every_other_replica() {
    let committee = committee_with_base_port(17_000);
    let leader = crate::leader(&committee);
    let (name, secret) = keys()
        .into_iter()
        .find(|(name, _)| *name != leader)
        .unwrap();
    let mut observers = Vec::new();
    for (_, address) in committee.others_primaries(&name) {
        let (tx, rx) = channel(10);
        NetworkReceiver::spawn(address.primary_to_primary, Observer(tx));
        observers.push(rx);
    }
    let path = ".db_test_pbft_broadcast";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let (tx, rx) = channel(10);
    let (_tx_proposer, rx_proposer) = channel(10);
    let (tx_output, mut output) = channel(10);
    Core::spawn(
        name,
        committee.clone(),
        store,
        SignatureService::new(secret),
        50,
        rx,
        rx_proposer,
        tx_output,
        None,
    );
    let pp = proposal(1, vec![vec![7; 10]]);
    tx.send(PbftMessage::PrePrepare(pp.clone())).await.unwrap();
    for observer in &mut observers {
        match timeout(Duration::from_secs(3), observer.recv())
            .await
            .unwrap()
            .unwrap()
        {
            PrimaryMessage::Pbft(PbftMessage::Prepare(vote)) => {
                vote.verify(&committee).unwrap();
                assert_eq!(vote.author, name);
                assert_eq!(vote.proposal, pp.digest());
            }
            message => panic!("Expected all-to-all Prepare, received {:?}", message),
        }
    }
    // Our local Prepare is already counted: only two additional voters are needed.
    for vote in prepares(&pp)
        .into_iter()
        .filter(|vote| vote.author != name)
        .take(2)
    {
        tx.send(PbftMessage::Prepare(vote)).await.unwrap();
    }
    for observer in &mut observers {
        match timeout(Duration::from_secs(3), observer.recv())
            .await
            .unwrap()
            .unwrap()
        {
            PrimaryMessage::Pbft(PbftMessage::Commit(vote)) => {
                vote.verify(&committee).unwrap();
                assert_eq!(vote.author, name);
                assert_eq!(vote.proposal, pp.digest());
            }
            message => panic!("Expected all-to-all Commit, received {:?}", message),
        }
    }
    assert!(output.try_recv().is_err());
    // Our local Commit is also already counted.
    for vote in commits(&pp)
        .into_iter()
        .filter(|vote| vote.author != name)
        .take(2)
    {
        tx.send(PbftMessage::Commit(vote)).await.unwrap();
    }
    let decision = timeout(Duration::from_secs(3), output.recv())
        .await
        .unwrap()
        .unwrap();
    decision.verify(&committee).unwrap();
    assert_eq!(decision.proposal.digest(), pp.digest());
    // No fourth certificate-broadcast phase follows the Commit messages.
    for observer in &mut observers {
        assert!(observer.try_recv().is_err());
    }
}
