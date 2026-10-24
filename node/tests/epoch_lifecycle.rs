use bytes::Bytes;
use config::{Authority, Committee, KeyPair, Parameters, PrimaryAddresses, WorkerAddresses};
use consensus::Consensus;
use crypto::{generate_production_keypair, Hash as _};
use futures::SinkExt;
use primary::{Commit, Epoch, Primary};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::net::TcpListener as PortReservation;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::net::TcpStream;
use tokio::sync::{mpsc::channel, watch};
use tokio::time::{sleep, timeout, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_recovers_offline_leader_with_bottoms_and_advances_two_epochs() {
    let mut keys: Vec<_> = (0..4).map(|_| generate_production_keypair()).collect();
    keys.sort_by_key(|(name, _)| *name);
    let mut reservations = Vec::new();
    let mut authorities = BTreeMap::new();
    for (name, _) in &keys {
        let primary = PortReservation::bind("127.0.0.1:0").unwrap();
        let client = PortReservation::bind("127.0.0.1:0").unwrap();
        let primary_address = primary.local_addr().unwrap();
        let client_address = client.local_addr().unwrap();
        authorities.insert(
            *name,
            Authority {
                stake: 1,
                primary: PrimaryAddresses {
                    primary_to_primary: primary_address,
                    worker_to_primary: primary_address,
                },
                workers: [(
                    0,
                    WorkerAddresses {
                        primary_to_worker: client_address,
                        worker_to_worker: client_address,
                        transactions: client_address,
                    },
                )]
                .iter()
                .cloned()
                .collect(),
            },
        );
        reservations.extend([primary, client]);
    }
    let committee = Committee { authorities };
    keys.pop(); // Instance 3's original leader is absent throughout both epochs.
    let root = std::env::temp_dir().join(format!(
        "iss-timeout-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    drop(reservations);
    let mut outputs = Vec::new();
    let mut progress = Vec::new();
    for (i, (name, secret)) in keys.into_iter().enumerate() {
        let store = Store::new(root.join(i.to_string()).to_str().unwrap()).unwrap();
        let (tx_epoch, rx_epoch) = watch::channel(0);
        let (tx_decisions, decisions) = channel(128);
        let (tx_output, output) = channel(128);
        Consensus::spawn(committee.clone(), 8, tx_epoch, decisions, tx_output);
        Primary::spawn(
            KeyPair { name, secret },
            committee.clone(),
            Parameters {
                segment_length: 2,
                view_change_timeout: 2_000,
                max_batch_delay: 20,
                ..Parameters::default()
            },
            store,
            tx_decisions,
            rx_epoch.clone(),
        );
        outputs.push(output);
        progress.push(rx_epoch);
    }
    let mut reference = None;
    for (i, output) in outputs.iter_mut().enumerate() {
        let mut log = Vec::new();
        for sn in 1..=16 {
            let ordered = timeout(Duration::from_secs(10), output.recv())
                .await
                .unwrap()
                .unwrap();
            ordered.committed.verify(&committee).unwrap();
            let proposal = ordered.committed.proposal;
            assert_eq!(ordered.global_sequence, sn);
            assert_eq!(proposal.position.epoch, (sn - 1) / 8);
            assert_eq!(proposal.bottom, proposal.position.instance == 3);
            assert_eq!(
                proposal.position.view,
                if proposal.bottom { 1 } else { 0 },
                "node={} sn={} position={:?}",
                i,
                sn,
                proposal.position
            );
            assert!(proposal.batch.transactions.is_empty());
            log.push((sn, proposal.position, proposal.digest(), proposal.bottom));
        }
        if let Some(expected) = &reference {
            assert_eq!(&log, expected);
        } else {
            reference = Some(log);
        }
        timeout(Duration::from_secs(2), async {
            while *progress[i].borrow_and_update() < 2 {
                progress[i].changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_segment_holds_epoch_then_all_nodes_deliver_three_identical_epochs() {
    let mut keys: Vec<_> = (0..4).map(|_| generate_production_keypair()).collect();
    keys.sort_by_key(|(name, _)| *name);
    let mut ports = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..4 {
        let primary = PortReservation::bind("127.0.0.1:0").unwrap();
        let client = PortReservation::bind("127.0.0.1:0").unwrap();
        addresses.push((primary.local_addr().unwrap(), client.local_addr().unwrap()));
        ports.extend([primary, client]);
    }
    let committee = Committee {
        authorities: keys
            .iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let (primary, client) = addresses[i];
                (
                    *name,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: primary,
                            worker_to_primary: primary,
                        },
                        workers: [(
                            0,
                            WorkerAddresses {
                                primary_to_worker: client,
                                worker_to_worker: client,
                                transactions: client,
                            },
                        )]
                        .iter()
                        .cloned()
                        .collect::<HashMap<_, _>>(),
                    },
                )
            })
            .collect(),
    };
    let parameters = Parameters {
        segment_length: 2,
        batch_size: 64,
        max_batch_delay: 20,
        ..Parameters::default()
    };
    let root = std::env::temp_dir().join(format!(
        "iss-epoch-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    drop(ports);
    let mut fourth = keys.pop();
    let mut outputs = Vec::new();
    let mut progress = Vec::new();
    let mut stores = Vec::new();
    let mut logs = vec![Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for (i, (name, secret)) in keys.into_iter().enumerate() {
        let store = Store::new(root.join(i.to_string()).to_str().unwrap()).unwrap();
        let (epoch, rx_epoch) = watch::channel(0);
        progress.push(rx_epoch.clone());
        let (decisions, rx_decisions) = channel(32);
        let (output, rx_output) = channel(32);
        Primary::spawn(
            KeyPair { name, secret },
            committee.clone(),
            parameters.clone(),
            store.clone(),
            decisions,
            rx_epoch,
        );
        Consensus::spawn(committee.clone(), 8, epoch, rx_decisions, output);
        stores.push(store);
        outputs.push(rx_output);
    }
    // Carry a real client transaction through the finite segment/global log path.
    let stream = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(stream) = TcpStream::connect(addresses[0].1).await {
                break stream;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut client = Framed::new(stream, LengthDelimitedCodec::new());
    client.send(Bytes::from(vec![7; 64])).await.unwrap();
    for i in 0..3 {
        for sn in 1..=3 {
            let ordered = timeout(Duration::from_secs(5), outputs[i].recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ordered.global_sequence, logs[i].len() as u64 + 1);
            let decision = ordered.committed;
            assert_eq!(
                Epoch::new(0, 8, 4)
                    .unwrap()
                    .global_sn(decision.proposal.position),
                Some(sn)
            );
            logs[i].push(decision);
        }
        assert!(timeout(Duration::from_millis(50), outputs[i].recv())
            .await
            .is_err());
        assert_eq!(
            *progress[i].borrow(),
            0,
            "Missing segment must hold the epoch barrier"
        );
    }
    // Completing the missing segment releases the same local barrier on each node.
    let (name, secret) = fourth.take().unwrap();
    let store = Store::new(root.join("3").to_str().unwrap()).unwrap();
    let (epoch, rx_epoch) = watch::channel(0);
    progress.push(rx_epoch.clone());
    let (decisions, rx_decisions) = channel(32);
    let (output, rx_output) = channel(32);
    Primary::spawn(
        KeyPair { name, secret },
        committee.clone(),
        parameters,
        store.clone(),
        decisions,
        rx_epoch,
    );
    Consensus::spawn(committee.clone(), 8, epoch, rx_decisions, output);
    stores.push(store);
    outputs.push(rx_output);
    for i in 0..4 {
        while logs[i].len() < 24 {
            let ordered = timeout(Duration::from_secs(10), outputs[i].recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ordered.global_sequence, logs[i].len() as u64 + 1);
            let decision = ordered.committed;
            decision.verify(&committee).unwrap();
            let sn = logs[i].len() as u64 + 1;
            let epoch = Epoch::new((sn - 1) / 8, 8, 4).unwrap();
            assert_eq!(epoch.global_sn(decision.proposal.position), Some(sn));
            logs[i].push(decision);
        }
    }
    let reference: Vec<_> = logs[0].iter().map(|x| x.proposal.digest()).collect();
    assert!(logs[0]
        .iter()
        .any(|x| x.proposal.batch.transactions == vec![vec![7; 64]]));
    assert!(logs[0]
        .iter()
        .any(|x| x.proposal.batch.transactions.is_empty()));
    for i in 0..4 {
        assert_eq!(
            logs[i]
                .iter()
                .map(|x| x.proposal.digest())
                .collect::<Vec<_>>(),
            reference
        );
        assert!(*progress[i].borrow() >= 2);
        for decision in &logs[i] {
            let mut key = b"pbft/committed/".to_vec();
            key.extend_from_slice(&bincode::serialize(&decision.proposal.position).unwrap());
            let bytes = stores[i].read(key).await.unwrap().unwrap();
            let proof: Vec<Commit> = bincode::deserialize(&bytes).unwrap();
            assert_eq!(proof.len(), decision.commits.len());
            assert!(proof
                .iter()
                .all(|vote| vote.position == decision.proposal.position
                    && vote.proposal == decision.proposal.digest()));
            assert!(stores[i]
                .read(decision.proposal.digest().to_vec())
                .await
                .unwrap()
                .is_none());
        }
    }
    drop(client);
    fs::remove_dir_all(root).unwrap();
}
