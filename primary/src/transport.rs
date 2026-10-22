use crate::messages::PbftMessage;
use crate::primary::{PrimaryMessage, CHANNEL_CAPACITY};
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
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
    pub fn spawn(name: PublicKey, committee: &Committee) -> Self {
        let addresses: Vec<_> = committee
            .others_primaries(&name)
            .iter()
            .map(|(_, address)| address.primary_to_primary)
            .collect();
        let (tx, mut rx) = mpsc::channel::<Broadcast>(CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let mut sender = ReliableSender::new();
            while let Some(Broadcast { message, reply }) = rx.recv().await {
                let bytes = bincode::serialize(&PrimaryMessage::Pbft(message))
                    .expect("Failed to serialize PBFT message");
                let handlers = sender
                    .broadcast(addresses.clone(), Bytes::from(bytes))
                    .await;
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
