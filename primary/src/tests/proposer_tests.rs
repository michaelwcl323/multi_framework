use super::*;
use crate::common::{committee, keys};
use crypto::Digest;
use tokio::sync::mpsc::channel;
use tokio::time::{advance as advance_time, timeout};

const HEADER_DELAY: Duration = Duration::from_millis(200);
const TX_SIZE: usize = 512;

struct Harness {
    committee: Committee,
    parents: Sender<(Vec<Certificate>, Round)>,
    transactions: Sender<Vec<Transaction>>,
    headers: Receiver<HeaderWithCertificate>,
    timeouts: Receiver<Timeout>,
    timeout_certificates: Sender<(TimeoutCert, Round)>,
    no_vote_certificates: Sender<(NoVoteCert, Round)>,
}

impl Harness {
    fn new() -> Self {
        // Be the next leader after round 1, exercising the former NVC stall.
        let authorities = committee();
        let name = authorities.leader(2);
        let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
        let committee = Committee::new(&name, authorities.authorities);
        let (parents, rx_parents) = channel(10);
        let (transactions, rx_transactions) = channel(10);
        let (tx_headers, headers) = channel(10);
        let (tx_timeouts, timeouts) = channel(10);
        let (timeout_certificates, rx_timeouts) = channel(10);
        let (no_vote_certificates, rx_no_votes) = channel(10);
        Proposer::spawn(
            name,
            committee.clone(),
            SignatureService::new(secret),
            2 * TX_SIZE,
            TX_SIZE,
            200,
            false,
            rx_parents,
            rx_transactions,
            tx_headers,
            tx_timeouts,
            rx_timeouts,
            rx_no_votes,
            1,
        );
        Self {
            committee,
            parents,
            transactions,
            headers,
            timeouts,
            timeout_certificates,
            no_vote_certificates,
        }
    }

    async fn header(&mut self) -> HeaderWithCertificate {
        timeout(Duration::from_millis(1), self.headers.recv())
            .await
            .expect("ready proposer did not emit a header")
            .expect("proposer stopped")
    }

    async fn full_first_header(&mut self) {
        self.transactions
            .send(vec![vec![1; TX_SIZE]; 2])
            .await
            .unwrap();
        let first = self.header().await;
        assert_eq!(first.header.round, 1);
        assert_eq!(first.header.payload.len(), 2);
    }

    async fn parents(&self, include_leader: bool) {
        let leader = self.committee.leader(1);
        let author = if include_leader {
            leader
        } else {
            *self
                .committee
                .authorities
                .keys()
                .find(|key| **key != leader)
                .unwrap()
        };
        let certificate = Certificate {
            header_id: Digest([1; 32]),
            sample_txn: 1,
            round: 1,
            origin: author,
            origin_id: self.committee.authorities[&author].id,
        };
        self.parents.send((vec![certificate], 1)).await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn deadline_wakes_idle_proposer_and_emits_empty_header() {
    let mut h = Harness::new();
    tokio::task::yield_now().await;
    advance_time(HEADER_DELAY - Duration::from_millis(1)).await;
    assert!(h.headers.try_recv().is_err());
    advance_time(Duration::from_millis(1)).await;
    let header = h.header().await;
    assert_eq!(header.header.round, 1);
    assert!(header.header.payload.is_empty());
    assert!(header.header.verify(&h.committee).is_ok());
}

#[tokio::test(start_paused = true)]
async fn deadline_flushes_partial_header_for_next_leader_without_tc_or_nvc() {
    let mut h = Harness::new();
    h.full_first_header().await;
    h.parents(true).await;
    let transaction = vec![1; TX_SIZE];
    h.transactions
        .send(vec![transaction.clone()])
        .await
        .unwrap();
    tokio::task::yield_now().await;
    advance_time(HEADER_DELAY - Duration::from_millis(1)).await;
    assert!(h.headers.try_recv().is_err());
    advance_time(Duration::from_millis(1)).await;
    let header = h.header().await;
    assert_eq!(header.header.round, 2);
    assert_eq!(header.header.author, h.committee.leader(2));
    assert_eq!(header.header.payload, vec![transaction]);
    assert!(header.header.verify(&h.committee).is_ok());
}

#[tokio::test(start_paused = true)]
async fn full_payload_proposes_before_deadline() {
    let mut h = Harness::new();
    h.full_first_header().await;
    h.parents(true).await;
    let started = Instant::now();
    h.transactions
        .send(vec![vec![1; TX_SIZE]; 2])
        .await
        .unwrap();
    let header = h.header().await;
    assert_eq!(header.header.round, 2);
    assert_eq!(header.header.payload.len(), 2);
    assert!(started.elapsed() < HEADER_DELAY);
    assert!(h.timeouts.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn expired_deadline_waits_for_parents_then_proposes_without_another_delay() {
    let mut h = Harness::new();
    h.full_first_header().await;
    h.transactions.send(vec![vec![1; TX_SIZE]]).await.unwrap();
    tokio::task::yield_now().await;
    advance_time(HEADER_DELAY).await;
    let timeout_msg = timeout(Duration::from_millis(1), h.timeouts.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(timeout_msg.round, 1);
    assert!(h.headers.try_recv().is_err());
    advance_time(HEADER_DELAY).await;
    assert!(h.headers.try_recv().is_err());
    assert!(h.timeouts.try_recv().is_err());
    let started = Instant::now();
    h.parents(true).await;
    let header = h.header().await;
    assert_eq!(header.header.round, 2);
    assert_eq!(header.header.payload.len(), 1);
    assert!(started.elapsed() < HEADER_DELAY);
}

#[tokio::test(start_paused = true)]
async fn expired_deadline_preserves_leader_readiness_and_certified_recovery() {
    let mut h = Harness::new();
    h.full_first_header().await;
    h.parents(false).await;
    h.transactions.send(vec![vec![1; TX_SIZE]]).await.unwrap();
    tokio::task::yield_now().await;
    advance_time(HEADER_DELAY).await;
    timeout(Duration::from_millis(1), h.timeouts.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(h.headers.try_recv().is_err());
    // TC alone cannot bypass the next leader's NVC requirement on recovery.
    h.timeout_certificates
        .send((TimeoutCert::new(1), 1))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(h.headers.try_recv().is_err());
    h.no_vote_certificates
        .send((NoVoteCert::new(1), 1))
        .await
        .unwrap();
    let header = h.header().await;
    assert_eq!(header.header.round, 2);
    assert_eq!(header.header.payload.len(), 1);
}
