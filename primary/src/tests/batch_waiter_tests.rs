use super::*;
use crate::common::{batch, certificate, committee_with_base_port, keys};
use crypto::Hash as _;
use std::fs;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn parent_sync_resumes_the_complete_batch() {
    let name = keys().remove(0).0;
    let parent = certificate(&batch());
    let mut proposal = batch();
    proposal.round = 2;
    proposal.parents = [parent.digest()].iter().cloned().collect();
    let path = ".db_test_batch_parent_waiter";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let (tx_sync, rx_sync) = channel(1);
    let (tx_core, mut rx_core) = channel(1);
    BatchWaiter::spawn(
        name,
        committee_with_base_port(14_000),
        store.clone(),
        Arc::new(AtomicU64::new(0)),
        50,
        5_000,
        3,
        rx_sync,
        tx_core,
    );
    tx_sync
        .send(WaiterMessage::SyncParents(
            vec![parent.digest()],
            proposal.clone(),
        ))
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(50), rx_core.recv())
        .await
        .is_err());
    store
        .write(
            parent.digest().to_vec(),
            bincode::serialize(&parent).unwrap(),
        )
        .await;
    let received = timeout(Duration::from_secs(2), rx_core.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.transactions, proposal.transactions);
    assert_eq!(received.parents, proposal.parents);
}
