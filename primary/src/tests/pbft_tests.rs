use super::*;
use crate::common::{commits, committee, keys, prepares, proposal};
use crate::messages::Position;
use crypto::Signature;

fn counts(actions: &[Action]) -> (usize, usize, usize) {
    (
        actions
            .iter()
            .filter(|x| matches!(x, Action::Prepare(_)))
            .count(),
        actions
            .iter()
            .filter(|x| matches!(x, Action::Commit(_)))
            .count(),
        actions
            .iter()
            .filter(|x| matches!(x, Action::Decide(_)))
            .count(),
    )
}

fn accept(state: &mut Pbft, proposal: &PrePrepare) {
    assert_eq!(
        counts(
            &state
                .process(PbftMessage::PrePrepare(proposal.clone()))
                .unwrap()
        ),
        (1, 0, 0)
    );
}

#[test]
fn duplicate_messages_do_not_add_weight_or_repeat_actions() {
    let mut state = Pbft::new(committee());
    let pp = proposal(1, vec![vec![1]]);
    accept(&mut state, &pp);
    assert!(state
        .process(PbftMessage::PrePrepare(pp.clone()))
        .unwrap()
        .is_empty());
    let prepares = prepares(&pp);
    for _ in 0..8 {
        assert!(state
            .process(PbftMessage::Prepare(prepares[0].clone()))
            .unwrap()
            .is_empty());
    }
    assert!(state
        .process(PbftMessage::Prepare(prepares[1].clone()))
        .unwrap()
        .is_empty());
    assert_eq!(
        counts(
            &state
                .process(PbftMessage::Prepare(prepares[2].clone()))
                .unwrap()
        ),
        (0, 1, 0)
    );
    assert!(state
        .process(PbftMessage::Prepare(prepares[2].clone()))
        .unwrap()
        .is_empty());
    let commits = commits(&pp);
    for _ in 0..8 {
        assert!(state
            .process(PbftMessage::Commit(commits[0].clone()))
            .unwrap()
            .is_empty());
    }
    assert!(state
        .process(PbftMessage::Commit(commits[1].clone()))
        .unwrap()
        .is_empty());
    let actions = state
        .process(PbftMessage::Commit(commits[2].clone()))
        .unwrap();
    assert_eq!(counts(&actions), (0, 0, 1));
    match &actions[0] {
        Action::Decide(x) => x.verify(&committee()).unwrap(),
        _ => unreachable!(),
    }
    // Replay after the slot has been cleaned up must not recreate it.
    for msg in [
        PbftMessage::PrePrepare(pp),
        PbftMessage::Prepare(prepares[0].clone()),
        PbftMessage::Commit(commits[0].clone()),
    ] {
        assert!(state.process(msg).unwrap().is_empty());
    }
    assert_eq!(state.low_watermark, 2);
    assert!(state.slots.is_empty());
}

#[test]
fn early_prepare_and_commit_quorums_wait_for_the_proposal() {
    let mut state = Pbft::new(committee());
    let pp = proposal(1, vec![vec![1]]);
    for commit in commits(&pp).into_iter().take(3) {
        assert!(state
            .process(PbftMessage::Commit(commit))
            .unwrap()
            .is_empty());
    }
    for prepare in prepares(&pp).into_iter().take(3) {
        assert!(state
            .process(PbftMessage::Prepare(prepare))
            .unwrap()
            .is_empty());
    }
    let actions = state.process(PbftMessage::PrePrepare(pp)).unwrap();
    assert_eq!(counts(&actions), (1, 1, 1));
}

#[test]
fn commit_quorum_cannot_bypass_local_prepared_state() {
    let mut state = Pbft::new(committee());
    let pp = proposal(1, vec![]);
    accept(&mut state, &pp);
    for commit in commits(&pp).into_iter().take(3) {
        assert!(state
            .process(PbftMessage::Commit(commit))
            .unwrap()
            .is_empty());
    }
    for prepare in prepares(&pp).into_iter().take(2) {
        assert!(state
            .process(PbftMessage::Prepare(prepare))
            .unwrap()
            .is_empty());
    }
    assert_eq!(state.low_watermark, 1);
    let actions = state
        .process(PbftMessage::Prepare(prepares(&pp)[2].clone()))
        .unwrap();
    assert_eq!(counts(&actions), (0, 1, 1));
}

#[test]
fn conflicting_proposal_is_rejected_without_replacing_accepted_value() {
    let mut state = Pbft::new(committee());
    let original = proposal(1, vec![vec![1]]);
    let conflict = proposal(1, vec![vec![2]]);
    accept(&mut state, &original);
    assert!(matches!(
        state.process(PbftMessage::PrePrepare(conflict.clone())),
        Err(PbftError::ConflictingProposal(1))
    ));
    assert_eq!(
        state.slots[&1].proposal.as_ref().unwrap().digest(),
        original.digest()
    );
    // Even a quorum for another digest does not prepare or commit this proposal.
    for vote in prepares(&conflict) {
        assert!(state
            .process(PbftMessage::Prepare(vote))
            .unwrap()
            .is_empty());
    }
    for vote in commits(&conflict) {
        assert!(state.process(PbftMessage::Commit(vote)).unwrap().is_empty());
    }
    assert!(!state.slots[&1].commit_sent);
}

#[test]
fn equivocation_is_bounded_and_conflicting_digests_do_not_mix() {
    let mut state = Pbft::new(committee());
    let pp = proposal(1, vec![vec![1]]);
    let other = proposal(1, vec![vec![2]]);
    accept(&mut state, &pp);
    let correct = prepares(&pp);
    let wrong = prepares(&other);
    state
        .process(PbftMessage::Prepare(correct[0].clone()))
        .unwrap();
    assert!(matches!(
        state.process(PbftMessage::Prepare(wrong[0].clone())),
        Err(PbftError::ConflictingVote(_, 1))
    ));
    state
        .process(PbftMessage::Prepare(wrong[1].clone()))
        .unwrap();
    assert!(state
        .process(PbftMessage::Prepare(correct[2].clone()))
        .unwrap()
        .is_empty());
    assert_eq!(
        counts(
            &state
                .process(PbftMessage::Prepare(correct[3].clone()))
                .unwrap()
        ),
        (0, 1, 0)
    );
    let correct = commits(&pp);
    let wrong = commits(&other);
    state
        .process(PbftMessage::Commit(correct[0].clone()))
        .unwrap();
    assert!(matches!(
        state.process(PbftMessage::Commit(wrong[0].clone())),
        Err(PbftError::ConflictingVote(_, 1))
    ));
    state
        .process(PbftMessage::Commit(wrong[1].clone()))
        .unwrap();
    assert!(state
        .process(PbftMessage::Commit(correct[2].clone()))
        .unwrap()
        .is_empty());
    assert_eq!(
        counts(
            &state
                .process(PbftMessage::Commit(correct[3].clone()))
                .unwrap()
        ),
        (0, 0, 1)
    );
}

#[test]
fn quorum_uses_stake_and_not_number_of_messages() {
    let mut committee = committee();
    let voters = keys();
    committee.authorities.get_mut(&voters[0].0).unwrap().stake = 4;
    assert_eq!(committee.quorum_threshold(), 5);
    let mut state = Pbft::new(committee);
    let pp = proposal(1, vec![]);
    accept(&mut state, &pp);
    let prepares = prepares(&pp);
    assert!(state
        .process(PbftMessage::Prepare(prepares[0].clone()))
        .unwrap()
        .is_empty());
    assert_eq!(
        counts(
            &state
                .process(PbftMessage::Prepare(prepares[1].clone()))
                .unwrap()
        ),
        (0, 1, 0)
    );
    let commits = commits(&pp);
    assert!(state
        .process(PbftMessage::Commit(commits[0].clone()))
        .unwrap()
        .is_empty());
    assert_eq!(
        counts(
            &state
                .process(PbftMessage::Commit(commits[1].clone()))
                .unwrap()
        ),
        (0, 0, 1)
    );
}

#[test]
fn votes_from_other_slots_do_not_count_and_holes_hold_the_watermark() {
    let mut state = Pbft::new(committee());
    let first = proposal(1, vec![]);
    let second = proposal(2, vec![]);
    accept(&mut state, &first);
    accept(&mut state, &second);
    for prepare in prepares(&second).into_iter().take(3) {
        state.process(PbftMessage::Prepare(prepare)).unwrap();
    }
    for commit in commits(&second).into_iter().take(3) {
        state.process(PbftMessage::Commit(commit)).unwrap();
    }
    assert_eq!(state.low_watermark, 1);
    assert!(!state.slots[&1].commit_sent);
    assert!(state.slots[&2].decided);
    // Duplicates remain idempotent while a decided slot awaits prefix cleanup.
    assert!(state
        .process(PbftMessage::PrePrepare(second))
        .unwrap()
        .is_empty());
    for prepare in prepares(&first).into_iter().take(3) {
        state.process(PbftMessage::Prepare(prepare)).unwrap();
    }
    for commit in commits(&first).into_iter().take(3) {
        state.process(PbftMessage::Commit(commit)).unwrap();
    }
    assert_eq!(state.low_watermark, 3);
    assert!(state.slots.is_empty());
}

#[test]
fn invalid_senders_signatures_contexts_and_future_slots_do_not_allocate_state() {
    let mut state = Pbft::new(committee());
    let pp = proposal(1, vec![]);
    let mut forged = prepares(&pp)[0].clone();
    forged.signature = Signature::default();
    assert!(state.process(PbftMessage::Prepare(forged)).is_err());
    let mut wrong_leader = pp.clone();
    wrong_leader.author = keys()
        .into_iter()
        .find(|(name, _)| *name != pp.author)
        .unwrap()
        .0;
    assert!(matches!(
        state.process(PbftMessage::PrePrepare(wrong_leader)),
        Err(PbftError::UnexpectedLeader(_))
    ));
    for position in [
        Position {
            epoch: 1,
            ..pp.position
        },
        Position {
            instance: 1,
            ..pp.position
        },
        Position {
            view: 1,
            ..pp.position
        },
        Position::new(0),
    ] {
        let mut invalid = pp.clone();
        invalid.position = position;
        assert!(matches!(
            state.process(PbftMessage::PrePrepare(invalid)),
            Err(PbftError::UnsupportedPosition(_))
        ));
    }
    let future = proposal(SEQUENCE_WINDOW + 1, vec![]);
    assert!(matches!(
        state.process(PbftMessage::PrePrepare(future)),
        Err(PbftError::OutsideWindow(_))
    ));
    let (author, secret) = crypto::generate_production_keypair();
    let mut unknown = prepares(&pp)[0].clone();
    unknown.author = author;
    unknown.signature = Signature::new(&unknown.digest(), &secret);
    assert!(matches!(
        state.process(PbftMessage::Prepare(unknown)),
        Err(PbftError::UnknownAuthority(_))
    ));
    assert!(state.slots.is_empty());
}

#[test]
fn all_phase_interleavings_with_duplicates_decide_the_same_batch_once() {
    // For each seed, permute all three phases and inject deterministic retries.
    use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
    let pp = proposal(1, vec![vec![1, 2, 3]]);
    let mut messages = vec![PbftMessage::PrePrepare(pp.clone())];
    messages.extend(prepares(&pp).into_iter().map(PbftMessage::Prepare));
    messages.extend(commits(&pp).into_iter().map(PbftMessage::Commit));
    for seed in 0..32 {
        let mut messages = messages.clone();
        messages.extend(messages.clone());
        messages.shuffle(&mut StdRng::seed_from_u64(seed));
        let mut state = Pbft::new(committee());
        let mut total = (0, 0, 0);
        for message in messages {
            let actions = state.process(message).unwrap();
            let count = counts(&actions);
            total.0 += count.0;
            total.1 += count.1;
            total.2 += count.2;
            for action in actions {
                if let Action::Decide(decision) = action {
                    decision.verify(&committee()).unwrap();
                    assert_eq!(decision.proposal.digest(), pp.digest());
                }
            }
        }
        assert_eq!(total, (1, 1, 1));
    }
}
