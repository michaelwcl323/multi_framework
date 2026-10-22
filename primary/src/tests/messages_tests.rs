use super::*;
use crate::common::{commits, committee, decision, prepares, proposal};

#[test]
fn signed_proposal_and_commit_proof_carry_full_transactions() {
    let decision = decision(1);
    decision.verify(&committee()).unwrap();
    let bytes = bincode::serialize(&decision).unwrap();
    let decoded: CommittedBatch = bincode::deserialize(&bytes).unwrap();
    decoded.verify(&committee()).unwrap();
    assert_eq!(
        decoded.proposal.batch.transactions,
        decision.proposal.batch.transactions
    );
}

#[test]
fn changing_payload_or_transaction_boundaries_invalidates_signature() {
    let mut pp = proposal(1, vec![vec![1], vec![2, 3]]);
    let original = pp.batch.id.clone();
    pp.batch.transactions = vec![vec![1, 2], vec![3]];
    assert_ne!(pp.batch.digest(), original);
    assert!(pp.verify(&committee()).is_err());
    pp.batch.id = pp.batch.digest();
    assert!(pp.verify(&committee()).is_err());
}

#[test]
fn signatures_bind_phase_sender_and_every_position_field() {
    let pp = proposal(1, vec![]);
    let prepare = prepares(&pp)[0].clone();
    let mut commit = commits(&pp)[0].clone();
    commit.signature = prepare.signature.clone();
    assert!(commit.verify(&committee()).is_err());
    let mut replays = Vec::new();
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
        Position::new(2),
    ] {
        let mut replay = prepare.clone();
        replay.position = position;
        assert_ne!(replay.digest(), prepare.digest());
        replays.push(replay);
        let mut replay = pp.clone();
        replay.position = position;
        assert!(replay.verify(&committee()).is_err());
    }
    let mut wrong_sender = prepare.clone();
    wrong_sender.author = prepares(&pp)[1].author;
    replays.push(wrong_sender);
    for replay in replays {
        assert!(replay.verify(&committee()).is_err());
    }
}

#[test]
fn commit_proof_rejects_duplicates_missing_quorum_and_mismatched_slots_or_digests() {
    let valid = decision(1);
    let mut invalid = valid.clone();
    invalid.commits = vec![valid.commits[0].clone(); 3];
    assert!(matches!(
        invalid.verify(&committee()),
        Err(PbftError::AuthorityReuse(_))
    ));
    invalid.commits = valid.commits[..2].to_vec();
    assert!(matches!(
        invalid.verify(&committee()),
        Err(PbftError::RequiresQuorum)
    ));
    invalid.commits = commits(&proposal(2, vec![])).into_iter().take(3).collect();
    assert!(matches!(
        invalid.verify(&committee()),
        Err(PbftError::MismatchedCommit)
    ));
    invalid.commits = commits(&proposal(1, vec![vec![99]]))
        .into_iter()
        .take(3)
        .collect();
    assert!(matches!(
        invalid.verify(&committee()),
        Err(PbftError::MismatchedCommit)
    ));
}
