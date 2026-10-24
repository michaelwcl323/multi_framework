use crate::control_transport::{ControlChunk, CHUNK_BYTES};
use crate::messages::PbftMessage;
use crate::primary::{PrimaryMessage, CHANNEL_CAPACITY};
use bytes::Bytes;
use config::Committee;
use crypto::{PublicKey, SignatureService};
use network::{CancelHandler, ReliableSender};
use tokio::sync::{mpsc, oneshot};

struct Broadcast {
    message: PbftMessage,
    reply: oneshot::Sender<Vec<CancelHandler>>,
}

/// Every local instance shares one sender and one outgoing connection per peer.
/// Each Core retains its own retry handles and cleans them using its own progress.
#[derive(Clone)]
pub(crate) struct Network {
    tx: mpsc::Sender<Broadcast>,
}

impl Network {
    #[cfg(test)]
    pub fn spawn(name: PublicKey, committee: &Committee) -> Self {
        Self::spawn_with_signatures(name, committee, None)
    }

    pub fn spawn_with_signatures(
        name: PublicKey,
        committee: &Committee,
        mut signatures: Option<SignatureService>,
    ) -> Self {
        let addresses: Vec<_> = committee
            .others_primaries(&name)
            .iter()
            .map(|(_, address)| address.primary_to_primary)
            .collect();
        let (tx, mut rx) = mpsc::channel::<Broadcast>(CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let mut sender = ReliableSender::new();
            while let Some(Broadcast { message, reply }) = rx.recv().await {
                let control = matches!(
                    &message,
                    PbftMessage::ViewChange(_) | PbftMessage::NewView(_)
                );
                let envelope = PrimaryMessage::Pbft(message);
                let bytes =
                    bincode::serialize(&envelope).expect("Failed to serialize PBFT message");
                let handlers = if control && bytes.len() > CHUNK_BYTES {
                    let message = match &envelope {
                        PrimaryMessage::Pbft(message) => message,
                        _ => unreachable!(),
                    };
                    let signatures = signatures
                        .as_mut()
                        .expect("Recovery fragment signing service is missing");
                    match ControlChunk::split(message, &bytes, signatures).await {
                        Ok(chunks) => {
                            let mut handlers = Vec::new();
                            for chunk in chunks {
                                let bytes =
                                    bincode::serialize(&PrimaryMessage::PbftChunk(chunk)).unwrap();
                                handlers.extend(
                                    sender
                                        .broadcast(addresses.clone(), Bytes::from(bytes))
                                        .await,
                                );
                            }
                            handlers
                        }
                        Err(error) => {
                            log::warn!("Rejected oversized recovery message: {}", error);
                            Vec::new()
                        }
                    }
                } else {
                    sender
                        .broadcast(addresses.clone(), Bytes::from(bytes))
                        .await
                };
                let _ = reply.send(handlers);
            }
        });
        Self { tx }
    }

    pub async fn broadcast(&self, message: PbftMessage) -> Vec<CancelHandler> {
        let (reply, result) = oneshot::channel();
        self.tx
            .send(Broadcast { message, reply })
            .await
            .expect("Network task closed");
        result.await.expect("Network task closed")
    }
}
