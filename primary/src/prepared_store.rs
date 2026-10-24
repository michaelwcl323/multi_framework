//! Recovery evidence lives in RocksDB; PBFT retains only these small references.
use crate::{Commit, PbftError, PbftResult, Position, PrePrepare, PreparedCertificate};
use config::Committee;
use crypto::{Digest, Hash as _};
use store::Store;

#[cfg(test)]
#[path = "tests/prepared_store_tests.rs"]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct PreparedRef {
    pub position: Position,
    pub proposal: Digest,
    pub batch: Digest,
    pub committed: bool,
}

impl PreparedRef {
    pub fn new(proposal: &PrePrepare) -> Self {
        Self {
            position: proposal.position,
            proposal: proposal.digest(),
            batch: proposal.batch.id.clone(),
            committed: false,
        }
    }
}

pub(crate) fn evidence_key(prefix: &[u8], position: Position) -> PbftResult<Vec<u8>> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&bincode::serialize(&position)?);
    Ok(key)
}

pub(crate) async fn load_certificate(
    store: &mut Store,
    committee: &Committee,
    reference: &PreparedRef,
) -> PbftResult<PreparedCertificate> {
    let bytes = store
        .read(evidence_key(b"pbft/prepared/", reference.position)?)
        .await?
        .ok_or(PbftError::StorageFailure)?;
    let mut certificate: PreparedCertificate =
        bincode::deserialize(&bytes).map_err(|_| PbftError::StorageFailure)?;
    ensure!(
        certificate.proposal.position == reference.position
            && certificate.proposal.digest() == reference.proposal
            && certificate.proposal.batch.id == reference.batch,
        PbftError::StorageFailure
    );
    certificate.commits = if reference.committed {
        let bytes = store
            .read(evidence_key(b"pbft/committed/", reference.position)?)
            .await?
            .ok_or(PbftError::StorageFailure)?;
        let commits: Vec<Commit> =
            bincode::deserialize(&bytes).map_err(|_| PbftError::StorageFailure)?;
        ensure!(!commits.is_empty(), PbftError::StorageFailure);
        commits
    } else {
        Vec::new()
    };
    certificate
        .verify(committee)
        .map_err(|_| PbftError::StorageFailure)?;
    Ok(certificate)
}
