use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration, Instant};

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    tx_txns: Sender<Vec<Transaction>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;
    use tokio::time::timeout;

    #[tokio::test]
    async fn full_batches_reset_size_before_accepting_more_transactions() {
        let (sender, receiver) = channel(10);
        let (batches, mut output) = channel(10);
        BatchMaker::spawn(1000, 1000, receiver, batches);
        for _ in 0..2 {
            sender.send(vec![0; 500]).await.unwrap();
        }
        assert_eq!(output.recv().await.unwrap().len(), 2);
        sender.send(vec![0; 500]).await.unwrap();
        assert!(timeout(Duration::from_millis(20), output.recv()).await.is_err());
        sender.send(vec![0; 500]).await.unwrap();
        assert_eq!(output.recv().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn partial_batches_seal_on_timeout_and_start_a_fresh_size_budget() {
        let (sender, receiver) = channel(10);
        let (batches, mut output) = channel(10);
        BatchMaker::spawn(1000, 100, receiver, batches);
        sender.send(vec![0; 500]).await.unwrap();
        assert_eq!(timeout(Duration::from_secs(1), output.recv()).await.unwrap().unwrap().len(), 1);
        sender.send(vec![0; 500]).await.unwrap();
        assert!(timeout(Duration::from_millis(20), output.recv()).await.is_err());
        assert_eq!(timeout(Duration::from_secs(1), output.recv()).await.unwrap().unwrap().len(), 1);
    }
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_txns: Sender<Vec<Transaction>>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                current_batch: Batch::new(),
                current_batch_size: 0,
                tx_txns,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        let batch: Vec<Transaction> = self.current_batch.drain(..).collect();
        self.current_batch_size = 0;

        self.tx_txns
            .send(batch)
            .await
            .expect("Failed to send digest");
    }
}
