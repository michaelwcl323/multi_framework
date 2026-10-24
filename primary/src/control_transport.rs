//! Authenticated fragments for recovery messages larger than a transport frame.
use crate::primary::PrimaryMessage;
use crate::{PbftError, PbftMessage, PbftResult};
use config::Committee;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::{Digest as _, Sha512};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;

pub(crate) const CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MAX_CONTROL_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ControlChunk {
    author: PublicKey,
    epoch: u64,
    instance: u64,
    view: u64,
    phase: u8,
    id: Digest,
    total_len: usize,
    index: usize,
    payload: Vec<u8>,
    signature: Signature,
}

impl Hash for ControlChunk {
    fn digest(&self) -> Digest {
        digest(
            &bincode::serialize(&(
                b"iss/control-chunk/v1",
                self.author,
                self.epoch,
                self.instance,
                self.view,
                self.phase,
                &self.id,
                self.total_len,
                self.index,
                &self.payload,
            ))
            .expect("serializable fragment"),
        )
    }
}

impl ControlChunk {
    pub async fn split(
        message: &PbftMessage,
        bytes: &[u8],
        signatures: &mut SignatureService,
    ) -> PbftResult<Vec<Self>> {
        ensure!(
            bytes.len() <= MAX_CONTROL_BYTES,
            PbftError::ControlMessageTooLarge
        );
        let position = message.position();
        let (phase, author) = message.phase_author();
        ensure!(phase == 3 || phase == 4, PbftError::InvalidRecovery);
        let id = digest(bytes);
        let mut chunks = Vec::new();
        for (index, payload) in bytes.chunks(CHUNK_BYTES).enumerate() {
            let mut chunk = Self {
                author,
                epoch: position.epoch,
                instance: position.instance,
                view: position.view,
                phase,
                id: id.clone(),
                total_len: bytes.len(),
                index,
                payload: payload.to_vec(),
                signature: Signature::default(),
            };
            chunk.signature = signatures.request_signature(chunk.digest()).await;
            chunks.push(chunk);
        }
        Ok(chunks)
    }
}

struct Assembly {
    epoch: u64,
    view: u64,
    id: Digest,
    total_len: usize,
    parts: BTreeMap<usize, Vec<u8>>,
    received: usize,
    complete: bool,
}

pub(crate) struct ControlAssembler {
    committee: Committee,
    // One latest message per authenticated author/segment/control phase.
    messages: HashMap<(PublicKey, u64, u8), Assembly>,
}

impl ControlAssembler {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            messages: HashMap::new(),
        }
    }

    pub fn insert(&mut self, chunk: ControlChunk) -> PbftResult<Option<PbftMessage>> {
        ensure!(
            self.committee.stake(&chunk.author) > 0,
            PbftError::UnknownAuthority(chunk.author)
        );
        ensure!(
            chunk.instance < self.committee.size() as u64
                && chunk.view > 0
                && (chunk.phase == 3 || chunk.phase == 4),
            PbftError::InvalidRecovery
        );
        ensure!(
            chunk.total_len > 0 && chunk.total_len <= MAX_CONTROL_BYTES,
            PbftError::ControlMessageTooLarge
        );
        let offset = chunk
            .index
            .checked_mul(CHUNK_BYTES)
            .ok_or(PbftError::InvalidRecovery)?;
        ensure!(
            offset < chunk.total_len
                && chunk.payload.len() == CHUNK_BYTES.min(chunk.total_len - offset),
            PbftError::InvalidRecovery
        );
        // Authenticate before allocating or replacing assembly state.
        chunk.signature.verify(&chunk.digest(), &chunk.author)?;
        let key = (chunk.author, chunk.instance, chunk.phase);
        let version = (chunk.epoch, chunk.view);
        if let Some(previous) = self.messages.get(&key) {
            if version < (previous.epoch, previous.view) {
                return Ok(None);
            }
            if version == (previous.epoch, previous.view) {
                ensure!(
                    chunk.id == previous.id && chunk.total_len == previous.total_len,
                    PbftError::InvalidRecovery
                );
                if previous.complete {
                    return Ok(None);
                }
            }
        }
        let entry = self.messages.entry(key).or_insert_with(|| Assembly {
            epoch: chunk.epoch,
            view: chunk.view,
            id: chunk.id.clone(),
            total_len: chunk.total_len,
            parts: BTreeMap::new(),
            received: 0,
            complete: false,
        });
        if version > (entry.epoch, entry.view) {
            *entry = Assembly {
                epoch: chunk.epoch,
                view: chunk.view,
                id: chunk.id.clone(),
                total_len: chunk.total_len,
                parts: BTreeMap::new(),
                received: 0,
                complete: false,
            };
        }
        if let Some(previous) = entry.parts.get(&chunk.index) {
            ensure!(previous == &chunk.payload, PbftError::InvalidRecovery);
        } else {
            entry.received += chunk.payload.len();
            entry.parts.insert(chunk.index, chunk.payload);
        }
        if entry.received != entry.total_len {
            return Ok(None);
        }
        let mut bytes = Vec::with_capacity(entry.total_len);
        for part in entry.parts.values() {
            bytes.extend_from_slice(part);
        }
        ensure!(digest(&bytes) == entry.id, PbftError::InvalidRecovery);
        let message = match bincode::deserialize(&bytes)? {
            PrimaryMessage::Pbft(message) => message,
            _ => return Err(PbftError::InvalidRecovery),
        };
        let position = message.position();
        ensure!(
            message.phase_author() == (chunk.phase, chunk.author)
                && position.epoch == chunk.epoch
                && position.instance == chunk.instance
                && position.view == chunk.view,
            PbftError::InvalidRecovery
        );
        message.verify(&self.committee)?;
        entry.parts.clear();
        entry.complete = true;
        Ok(Some(message))
    }
}

fn digest(bytes: &[u8]) -> Digest {
    let mut hash = Sha512::new();
    hash.update(b"iss/control-transport/v1");
    hash.update(bytes);
    Digest(hash.finalize()[..32].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{committee, keys};
    use crate::ViewChange;

    #[tokio::test]
    async fn authenticated_fragments_reject_tampering_and_deliver_exactly_once() {
        let (name, secret) = keys().remove(0);
        let mut signer = SignatureService::new(secret);
        let message =
            PbftMessage::ViewChange(ViewChange::new(0, 0, 1, vec![], name, &mut signer).await);
        let bytes = bincode::serialize(&PrimaryMessage::Pbft(message.clone())).unwrap();
        let chunks = ControlChunk::split(&message, &bytes, &mut signer)
            .await
            .unwrap();
        let mut assembler = ControlAssembler::new(committee());
        let mut invalid = chunks[0].clone();
        invalid.payload[0] ^= 1;
        assert!(assembler.insert(invalid).is_err());
        assert!(assembler.messages.is_empty());
        let result = assembler.insert(chunks[0].clone()).unwrap().unwrap();
        assert_eq!(result.phase_author(), message.phase_author());
        assert!(assembler.insert(chunks[0].clone()).unwrap().is_none());
        assert!(assembler
            .messages
            .values()
            .all(|entry| entry.parts.is_empty()));
    }
}
