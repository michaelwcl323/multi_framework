use super::*;
use crate::common::{commits, committee, keys, prepares, proposal, proposal_for};
use crate::pbft::{Action, Pbft};
use crate::{Epoch, PbftMessage, SlotKey};

fn certificate(proposal: PrePrepare, committed: bool) -> PreparedCertificate {
    PreparedCertificate {
        prepares: prepares(&proposal).into_iter().take(3).collect(),
        commits: if committed {
            commits(&proposal).into_iter().take(3).collect()
        } else {
            vec![]
        },
        proposal,
    }
}

fn report(
    index: usize,
    instance: u64,
    view: u64,
    prepared: Vec<PreparedCertificate>,
) -> ViewChange {
    let (author, secret) = keys().remove(index);
    let mut change = ViewChange {
        epoch: 0,
        instance,
        view,
        prepared,
        author,
        signature: Signature::default(),
    };
    change.signature = Signature::new(&change.digest(), &secret);
    change
}

async fn build(changes: Vec<ViewChange>) -> NewView {
    build_slots(25, changes).await
}

async fn build_slots(slots: u64, changes: Vec<ViewChange>) -> NewView {
    let primary = primary(&committee(), changes[0].instance, changes[0].view).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == primary).unwrap();
    NewView::new(
        &committee(),
        slots,
        changes,
        primary,
        &mut SignatureService::new(secret),
    )
    .await
    .unwrap()
}

fn state(instance: u64) -> Pbft {
    Pbft::new(
        committee(),
        Epoch::new(0, 100, 4).unwrap().segment(instance).unwrap(),
    )
}

fn prepare_at(state: &mut Pbft, proposal: &PrePrepare) {
    state
        .process(PbftMessage::PrePrepare(proposal.clone()))
        .unwrap();
    for vote in prepares(proposal).into_iter().take(3) {
        state.process(PbftMessage::Prepare(vote)).unwrap();
    }
}

// Exercise the actual retained evidence, including after live-slot cleanup.
fn export_evidence(
    state: &mut Pbft,
    view: u64,
    proof: PreparedCertificate,
) -> Vec<PreparedCertificate> {
    let evidence = state.begin_view_change(view).unwrap();
    assert_eq!(evidence.len(), 1);
    evidence[0].verify(&committee()).unwrap();
    assert_eq!(evidence[0].proposal.digest(), proof.proposal.digest());
    assert_eq!(
        evidence[0].proposal.batch.transactions,
        proof.proposal.batch.transactions
    );
    assert_eq!(evidence[0].prepares.len(), proof.prepares.len());
    assert_eq!(evidence[0].commits.len(), proof.commits.len());
    evidence
}

fn resign_new_view(view: &mut NewView) {
    let (_, secret) = keys()
        .into_iter()
        .find(|(name, _)| *name == view.author)
        .unwrap();
    view.signature = Signature::new(&view.digest(), &secret);
}

#[tokio::test]
async fn bottom_is_signed_distinct_from_empty_and_recoverable_without_sender_origin() {
    let owner = primary(&committee(), 0, 1).unwrap();
    let (_, secret) = keys().into_iter().find(|(name, _)| *name == owner).unwrap();
    let mut signer = SignatureService::new(secret);
    let normal = PrePrepare::recover(&proposal(1, vec![]), 1, owner, &mut signer).await;
    let bottom = PrePrepare::new_bottom(normal.position, owner, &mut signer).await;
    normal.verify(&committee()).unwrap();
    bottom.verify(&committee()).unwrap();
    assert_eq!(normal.batch.id, bottom.batch.id);
    assert_ne!(normal.digest(), bottom.digest());
    let mut tampered = bottom.clone();
    tampered.bottom = false;
    assert!(tampered.verify(&committee()).is_err());
    let mut payload = bottom.clone();
    payload.batch = crate::Batch::new(vec![vec![1]]);
    payload.signature = signer.request_signature(payload.digest()).await;
    assert!(matches!(
        payload.verify(&committee()),
        Err(PbftError::InvalidRecovery)
    ));
    let sender = crate::leader(&committee(), 0).unwrap();
    let (_, secret) = keys()
        .into_iter()
        .find(|(name, _)| *name == sender)
        .unwrap();
    let initial =
        PrePrepare::new_bottom(Position::new(1), sender, &mut SignatureService::new(secret)).await;
    assert!(matches!(
        initial.verify(&committee()),
        Err(PbftError::InvalidRecovery)
    ));
    let next_owner = primary(&committee(), 0, 2).unwrap();
    let (_, secret) = keys()
        .into_iter()
        .find(|(name, _)| *name == next_owner)
        .unwrap();
    let recovered =
        PrePrepare::recover(&bottom, 2, next_owner, &mut SignatureService::new(secret)).await;
    recovered.verify(&committee()).unwrap();
    assert!(recovered.bottom && recovered.origin.is_none());
    assert_eq!(recovered.position.slot(), bottom.position.slot());
}

#[tokio::test]
async fn new_view_fills_all_holes_but_cannot_omit_invent_or_replace_a_slot() {
    let prepared = proposal(2, vec![vec![7]]);
    let view = build_slots(
        3,
        vec![
            report(0, 0, 1, vec![certificate(prepared.clone(), false)]),
            report(1, 0, 1, vec![]),
            report(2, 0, 1, vec![]),
        ],
    )
    .await;
    view.verify(&committee()).unwrap();
    assert_eq!(
        view.proposals.iter().map(|p| p.bottom).collect::<Vec<_>>(),
        vec![true, false, true]
    );
    assert_eq!(view.proposals[1].batch.id, prepared.batch.id);
    let mut missing = view.clone();
    missing.proposals.pop();
    resign_new_view(&mut missing);
    assert!(missing.verify(&committee()).is_err());
    let mut duplicate = view.clone();
    duplicate.proposals[2] = duplicate.proposals[0].clone();
    resign_new_view(&mut duplicate);
    assert!(duplicate.verify(&committee()).is_err());
    let owner = view.author;
    let (_, secret) = keys().into_iter().find(|(name, _)| *name == owner).unwrap();
    let mut signer = SignatureService::new(secret);
    let mut replaced = view.clone();
    replaced.proposals[1] =
        PrePrepare::new_bottom(replaced.proposals[1].position, owner, &mut signer).await;
    resign_new_view(&mut replaced);
    assert!(replaced.verify(&committee()).is_err());
    let mut invented = view.clone();
    invented.proposals[0] = PrePrepare::recover(&proposal(1, vec![]), 1, owner, &mut signer).await;
    resign_new_view(&mut invented);
    assert!(invented.verify(&committee()).is_err());
    // A signed, internally consistent extent still has to match local config.
    let shorter = build_slots(2, (0..3).map(|i| report(i, 0, 1, vec![])).collect()).await;
    shorter.verify(&committee()).unwrap();
    let mut local = Pbft::new(
        committee(),
        Epoch::new(0, 12, 4).unwrap().segment(0).unwrap(),
    );
    assert!(matches!(
        local.process(PbftMessage::NewView(shorter)),
        Err(PbftError::InvalidRecovery)
    ));
    assert_eq!(local.view, 0);
    local.process(PbftMessage::NewView(view.clone())).unwrap();
    for proposal in &view.proposals {
        for vote in prepares(proposal).into_iter().take(3) {
            local.process(PbftMessage::Prepare(vote)).unwrap();
        }
        for vote in commits(proposal).into_iter().take(3) {
            local.process(PbftMessage::Commit(vote)).unwrap();
        }
    }
    assert!(local.is_complete());
    let evidence = local.begin_view_change(2).unwrap();
    assert_eq!(evidence.len(), 3);
    assert!(evidence
        .iter()
        .all(|proof| proof.verify(&committee()).is_ok() && proof.commits.len() == 3));
    let next = build_slots(
        3,
        vec![
            report(0, 0, 2, evidence),
            report(1, 0, 2, vec![]),
            report(2, 0, 2, vec![]),
        ],
    )
    .await;
    next.verify(&committee()).unwrap();
    assert_eq!(
        next.proposals.iter().map(|p| p.bottom).collect::<Vec<_>>(),
        vec![true, false, true]
    );
}

#[test]
fn slot_identity_and_global_sequence_do_not_depend_on_view() {
    let first = Position::for_epoch(2, 3, 5);
    let later = Position {
        view: u64::MAX,
        ..first
    };
    assert_eq!(first.slot(), later.slot());
    assert_eq!(
        later.slot(),
        SlotKey {
            epoch: 2,
            instance: 3,
            sequence: 5
        }
    );
    assert_eq!(first.global_sequence(10, 250).unwrap(), 544);
    assert_eq!(later.global_sequence(10, 250).unwrap(), 544);
    assert_eq!(primary(&committee(), 3, 1), crate::leader(&committee(), 0));
    assert_eq!(primary(&committee(), 3, 4), crate::leader(&committee(), 3));
    assert!(primary(&committee(), 4, 1).is_none());
}

#[test]
fn evidence_checks_quorums_signatures_phases_and_slot_identity() {
    let pp = proposal(5, vec![vec![7]]);
    let valid = certificate(pp, true);
    valid.verify(&committee()).unwrap();
    let mut invalid = valid.clone();
    invalid.prepares.pop();
    assert!(matches!(
        invalid.verify(&committee()),
        Err(PbftError::RequiresQuorum)
    ));
    invalid = valid.clone();
    invalid.prepares[1] = invalid.prepares[0].clone();
    assert!(matches!(
        invalid.verify(&committee()),
        Err(PbftError::AuthorityReuse(_))
    ));
    invalid = valid.clone();
    invalid.commits.pop();
    assert!(matches!(
        invalid.verify(&committee()),
        Err(PbftError::RequiresQuorum)
    ));
    invalid = valid.clone();
    invalid.prepares[0].signature = valid.commits[0].signature.clone();
    assert!(invalid.verify(&committee()).is_err());
    invalid = valid.clone();
    invalid.prepares[0] = prepares(&proposal(6, vec![]))[0].clone();
    assert!(invalid.verify(&committee()).is_err());
    let signed = report(0, 0, 1, vec![valid]);
    signed.verify(&committee()).unwrap();
    for position in [
        Position {
            epoch: 1,
            ..signed.position()
        },
        Position {
            instance: 1,
            ..signed.position()
        },
        Position {
            view: 2,
            ..signed.position()
        },
    ] {
        let mut invalid = signed.clone();
        invalid.epoch = position.epoch;
        invalid.instance = position.instance;
        invalid.view = position.view;
        assert!(invalid.verify(&committee()).is_err());
    }
    let mut invalid = signed.clone();
    invalid.prepared.clear();
    assert!(invalid.verify(&committee()).is_err());
    let mut invalid = signed;
    invalid.signature = Signature::new(&invalid.digest(), &keys().remove(1).1);
    assert!(invalid.verify(&committee()).is_err());
}

#[tokio::test]
async fn prepared_but_uncommitted_batch_is_recovered_and_commits_in_new_view() {
    let pp = proposal_for(3, 5, vec![vec![9; 50]]);
    let mut old = state(3);
    prepare_at(&mut old, &pp);
    let saved = export_evidence(&mut old, 1, certificate(pp.clone(), false));
    assert_eq!(saved.len(), 1);
    assert!(saved[0].commits.is_empty());
    let view = build(vec![
        report(0, 3, 1, saved),
        report(1, 3, 1, vec![]),
        report(2, 3, 1, vec![]),
    ])
    .await;
    view.verify(&committee()).unwrap();
    let recovered = &view.proposals[4];
    assert_eq!(recovered.position.slot(), pp.position.slot());
    assert_eq!(recovered.position.view, 1);
    assert_eq!(recovered.batch.transactions, pp.batch.transactions);
    assert_ne!(recovered.author, pp.author);
    assert_eq!(recovered.position.global_sequence(4, 100).unwrap(), 20);
    let mut replica = state(3);
    let actions = replica.process(PbftMessage::NewView(view.clone())).unwrap();
    assert_eq!(actions.len(), 25);
    assert!(actions.iter().all(|a| matches!(a, Action::Prepare(_))));
    for prepare in prepares(recovered).into_iter().take(3) {
        replica.process(PbftMessage::Prepare(prepare)).unwrap();
    }
    let mut decisions = Vec::new();
    for commit in commits(recovered).into_iter().take(3) {
        decisions.extend(replica.process(PbftMessage::Commit(commit)).unwrap());
    }
    let decision = decisions
        .into_iter()
        .find_map(|action| match action {
            Action::Decide(decision) => Some(decision),
            _ => None,
        })
        .unwrap();
    decision.verify(&committee()).unwrap();
    assert_eq!(decision.proposal.batch.id, pp.batch.id);
    assert!(replica
        .process(PbftMessage::NewView(view))
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn new_primary_cannot_replace_or_omit_prepared_batch_or_add_fresh_or_empty_batch() {
    let pp = proposal(5, vec![vec![1]]);
    let view = build(vec![
        report(0, 0, 1, vec![certificate(pp, false)]),
        report(1, 0, 1, vec![]),
        report(2, 0, 1, vec![]),
    ])
    .await;
    for batch in [proposal(5, vec![vec![99]]), proposal(5, vec![])] {
        let (_, secret) = keys()
            .into_iter()
            .find(|(name, _)| *name == view.author)
            .unwrap();
        let mut invalid = view.clone();
        invalid.proposals[4] =
            PrePrepare::recover(&batch, 1, view.author, &mut SignatureService::new(secret)).await;
        invalid.proposals[4].verify(&committee()).unwrap(); // Authentic origin still is not a safe selection.
        resign_new_view(&mut invalid);
        assert!(invalid.verify(&committee()).is_err());
    }
    let mut missing = view.clone();
    missing.proposals.clear();
    resign_new_view(&mut missing);
    assert!(missing.verify(&committee()).is_err());
    let mut duplicate = view.clone();
    duplicate.changes[1] = duplicate.changes[0].clone();
    resign_new_view(&mut duplicate);
    assert!(matches!(
        duplicate.verify(&committee()),
        Err(PbftError::AuthorityReuse(_))
    ));
    let mut insufficient = view.clone();
    insufficient.changes.pop();
    resign_new_view(&mut insufficient);
    assert!(matches!(
        insufficient.verify(&committee()),
        Err(PbftError::RequiresQuorum)
    ));
    let mut altered = view;
    altered.proposals[4].origin = None;
    resign_new_view(&mut altered);
    assert!(altered.verify(&committee()).is_err());
}

#[tokio::test]
async fn second_view_preserves_highest_prepared_evidence_and_original_sender_proof() {
    let old = certificate(proposal(1, vec![vec![1]]), false);
    let first = build(vec![
        report(0, 0, 1, vec![old.clone()]),
        report(1, 0, 1, vec![]),
        report(2, 0, 1, vec![]),
    ])
    .await;
    let newer = certificate(first.proposals[0].clone(), false);
    let second = build(vec![
        report(0, 0, 2, vec![old]),
        report(1, 0, 2, vec![newer]),
        report(2, 0, 2, vec![]),
    ])
    .await;
    second.verify(&committee()).unwrap();
    let selected = select(&committee(), 0, 0, 2, &second.changes).unwrap();
    assert_eq!(selected[&1].position.view, 1);
    assert_eq!(second.proposals[0].position.view, 2);
    assert_eq!(second.proposals[0].batch.id, first.proposals[0].batch.id);
    let mut replica = state(0);
    replica.process(PbftMessage::NewView(second)).unwrap();
    assert_eq!(replica.view, 2);
}

#[tokio::test]
async fn committed_history_survives_slot_cleanup_and_is_delivered_to_lagging_replicas_once() {
    let pp = proposal(1, vec![vec![7]]);
    let mut replica = state(0);
    prepare_at(&mut replica, &pp);
    for commit in commits(&pp).into_iter().take(3) {
        replica.process(PbftMessage::Commit(commit)).unwrap();
    }
    assert_eq!(replica.low_watermark, 2);
    let saved = export_evidence(&mut replica, 1, certificate(pp.clone(), true));
    assert_eq!(saved[0].commits.len(), 3);
    let view = build(vec![
        report(0, 0, 1, saved),
        report(1, 0, 1, vec![]),
        report(2, 0, 1, vec![]),
    ])
    .await;
    let actions = replica.process(PbftMessage::NewView(view.clone())).unwrap();
    assert_eq!(actions.len(), 24);
    assert!(actions
        .iter()
        .all(|a| matches!(a, Action::Prepare(p) if p.bottom)));
    let mut lagging = state(0);
    let actions = lagging.process(PbftMessage::NewView(view.clone())).unwrap();
    assert_eq!(
        actions
            .iter()
            .filter(|a| matches!(a, Action::Decide(_)))
            .count(),
        1
    );
    assert_eq!(actions.len(), 25);
    assert_eq!(lagging.low_watermark, 2);
    assert!(lagging
        .process(PbftMessage::NewView(view))
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn leaving_view_freezes_votes_and_late_messages_do_not_change_new_view() {
    let pp = proposal(1, vec![vec![7]]);
    let mut replica = state(0);
    prepare_at(&mut replica, &pp);
    let saved = export_evidence(&mut replica, 1, certificate(pp.clone(), false));
    for vote in commits(&pp) {
        assert!(replica
            .process(PbftMessage::Commit(vote))
            .unwrap()
            .is_empty());
    }
    let view = build(vec![
        report(0, 0, 1, saved),
        report(1, 0, 1, vec![]),
        report(2, 0, 1, vec![]),
    ])
    .await;
    replica.process(PbftMessage::NewView(view)).unwrap();
    assert!(replica
        .process(PbftMessage::PrePrepare(pp.clone()))
        .unwrap()
        .is_empty());
    for prepare in prepares(&pp) {
        assert!(replica
            .process(PbftMessage::Prepare(prepare))
            .unwrap()
            .is_empty());
    }
    assert_eq!(replica.view, 1);
    assert_eq!(replica.low_watermark, 1);
    let reports = replica.begin_view_change(2).unwrap();
    assert_eq!(reports.len(), 1); // The old evidence is still available in memory.
    reports[0].verify(&committee()).unwrap();
}

#[tokio::test]
async fn view_change_is_segment_local_and_empty_quorum_fills_explicit_bottoms() {
    let mut first = state(0);
    let mut other = state(1);
    assert!(first
        .process(PbftMessage::ViewChange(report(0, 0, 1, vec![])))
        .unwrap()
        .is_empty());
    assert_eq!(first.target_view, 0); // One faulty voter cannot trigger a view change.
    let actions = first
        .process(PbftMessage::ViewChange(report(1, 0, 1, vec![])))
        .unwrap();
    assert!(matches!(actions.as_slice(), [Action::ChangeView(1)]));
    let view = build((0..3).map(|i| report(i, 0, 1, vec![])).collect()).await;
    assert_eq!(view.proposals.len(), 25);
    assert!(view
        .proposals
        .iter()
        .all(|p| p.bottom && p.batch.transactions.is_empty()));
    first.process(PbftMessage::NewView(view.clone())).unwrap();
    let name = primary(&committee(), 0, 1).unwrap();
    let (_, secret) = keys().into_iter().find(|(key, _)| *key == name).unwrap();
    let unauthorized = PrePrepare::recover(
        &proposal(1, vec![]),
        1,
        name,
        &mut SignatureService::new(secret),
    )
    .await;
    assert!(matches!(
        first.process(PbftMessage::PrePrepare(unauthorized)),
        Err(PbftError::InvalidRecovery)
    ));
    assert_eq!(first.view, 1);
    assert_eq!(other.view, 0);
    assert!(other.process(PbftMessage::NewView(view)).is_err());
    assert_eq!(other.view, 0);
    let fresh = proposal_for(1, 1, vec![]);
    assert!(matches!(
        other
            .process(PbftMessage::PrePrepare(fresh))
            .unwrap()
            .as_slice(),
        [Action::Prepare(_)]
    ));
}

#[tokio::test]
async fn ten_equal_weight_authorities_require_seven_unique_view_changes() {
    use config::{Authority, PrimaryAddresses};
    use crypto::generate_keypair;
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::HashMap;
    let mut rng = StdRng::from_seed([44; 32]);
    let keys: Vec<_> = (0..10).map(|_| generate_keypair(&mut rng)).collect();
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
    assert_eq!(committee.quorum_threshold(), 7);
    let mut reports = Vec::new();
    for (name, secret) in &keys {
        let mut report = ViewChange {
            epoch: 0,
            instance: 3,
            view: 1,
            prepared: vec![],
            author: *name,
            signature: Signature::default(),
        };
        report.signature = Signature::new(&report.digest(), secret);
        reports.push(report);
    }
    let primary = primary(&committee, 3, 1).unwrap();
    let (_, secret) = keys.into_iter().find(|(name, _)| *name == primary).unwrap();
    let mut signer = SignatureService::new(secret);
    assert!(matches!(
        NewView::new(&committee, 25, reports[..6].to_vec(), primary, &mut signer).await,
        Err(PbftError::RequiresQuorum)
    ));
    let valid = NewView::new(&committee, 25, reports[..7].to_vec(), primary, &mut signer)
        .await
        .unwrap();
    valid.verify(&committee).unwrap();
}

#[tokio::test]
async fn out_of_segment_evidence_and_duplicate_reports_do_not_change_protocol_state() {
    let outside = proposal(26, vec![]);
    let change = report(0, 0, 1, vec![certificate(outside, false)]);
    change.verify(&committee()).unwrap();
    let mut replica = state(0);
    assert!(matches!(
        replica.process(PbftMessage::ViewChange(change)),
        Err(PbftError::OutsideSegment(_))
    ));
    assert_eq!(replica.view, 0);
    assert_eq!(replica.target_view, 0);
    let report = report(0, 0, 1, vec![]);
    replica
        .process(PbftMessage::ViewChange(report.clone()))
        .unwrap();
    assert!(replica
        .process(PbftMessage::ViewChange(report))
        .unwrap()
        .is_empty());
    assert_eq!(replica.target_view, 0);
}

#[tokio::test]
async fn prepared_evidence_and_new_view_use_weighted_unique_quorums() {
    let mut weighted = committee();
    let heavy = keys()[0].0;
    weighted.authorities.get_mut(&heavy).unwrap().stake = 5;
    assert_eq!(weighted.quorum_threshold(), 6);
    let proposal = proposal(1, vec![vec![4]]);
    let mut proof = PreparedCertificate {
        prepares: prepares(&proposal).into_iter().take(2).collect(),
        commits: commits(&proposal).into_iter().take(2).collect(),
        proposal,
    };
    proof.verify(&weighted).unwrap();
    proof.prepares.pop();
    assert!(matches!(
        proof.verify(&weighted),
        Err(PbftError::RequiresQuorum)
    ));
    let owner = primary(&weighted, 0, 1).unwrap();
    let (_, secret) = keys().into_iter().find(|(name, _)| *name == owner).unwrap();
    let mut signer = SignatureService::new(secret);
    assert!(matches!(
        NewView::new(
            &weighted,
            25,
            vec![report(0, 0, 1, vec![])],
            owner,
            &mut signer
        )
        .await,
        Err(PbftError::RequiresQuorum)
    ));
    let valid = NewView::new(
        &weighted,
        25,
        vec![report(0, 0, 1, vec![]), report(1, 0, 1, vec![])],
        owner,
        &mut signer,
    )
    .await
    .unwrap();
    valid.verify(&weighted).unwrap();
    assert!(matches!(
        NewView::new(
            &weighted,
            25,
            vec![report(0, 0, 1, vec![]), report(0, 0, 1, vec![])],
            owner,
            &mut signer
        )
        .await,
        Err(PbftError::AuthorityReuse(_))
    ));
}

#[tokio::test]
async fn committed_value_cannot_be_omitted_locally_or_displaced_by_newer_prepared_evidence() {
    let original = proposal(1, vec![vec![7]]);
    let mut replica = state(0);
    prepare_at(&mut replica, &original);
    for commit in commits(&original).into_iter().take(3) {
        replica.process(PbftMessage::Commit(commit)).unwrap();
    }
    let empty = build(vec![
        report(0, 0, 1, vec![]),
        report(1, 0, 1, vec![]),
        report(2, 0, 1, vec![]),
    ])
    .await;
    assert!(matches!(
        replica.process(PbftMessage::NewView(empty)),
        Err(PbftError::InvalidRecovery)
    ));
    assert_eq!(replica.view, 0);
    assert_eq!(replica.low_watermark, 2);
    let different = proposal(1, vec![vec![8]]);
    let new_primary = primary(&committee(), 0, 1).unwrap();
    let (_, secret) = keys()
        .into_iter()
        .find(|(name, _)| *name == new_primary)
        .unwrap();
    let newer = PrePrepare::recover(
        &different,
        1,
        new_primary,
        &mut SignatureService::new(secret),
    )
    .await;
    let reports = vec![
        report(0, 0, 2, vec![certificate(original, true)]),
        report(1, 0, 2, vec![certificate(newer, false)]),
        report(2, 0, 2, vec![]),
    ];
    let owner = primary(&committee(), 0, 2).unwrap();
    let (_, secret) = keys().into_iter().find(|(name, _)| *name == owner).unwrap();
    assert!(matches!(
        NewView::new(
            &committee(),
            25,
            reports,
            owner,
            &mut SignatureService::new(secret)
        )
        .await,
        Err(PbftError::InvalidRecovery)
    ));
}
