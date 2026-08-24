use crate::infrai_queue::{InfraiQueue, QueueClientError};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{sync::Semaphore, task::JoinSet, time::interval};

const CHECKOUT_QUEUE: &str = "paid-checkouts";
const ORDER_UPDATE_QUEUE: &str = "order-updates";

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CommerceJob {
    CheckoutPaid {
        order_id: String,
        customer_id: String,
        email: String,
        product: ProductKind,
        amount_cents: u64,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProductKind {
    Physical,
    Digital,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrderUpdate {
    pub order_id: String,
    pub customer_id: String,
    pub status: OrderStatus,
    pub fulfillment: FulfillmentAction,
    pub receipt: Receipt,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    ReadyForFulfillment,
    Fulfilled,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum FulfillmentAction {
    PickAndPack,
    GrantDownload,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Receipt {
    pub recipient: String,
    pub amount_cents: u64,
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Queue(#[from] QueueClientError),
    #[error("worker task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

pub fn decide(job: CommerceJob) -> OrderUpdate {
    match job {
        CommerceJob::CheckoutPaid {
            order_id,
            customer_id,
            email,
            product,
            amount_cents,
        } => {
            let (status, fulfillment) = match product {
                ProductKind::Physical => (
                    OrderStatus::ReadyForFulfillment,
                    FulfillmentAction::PickAndPack,
                ),
                ProductKind::Digital => {
                    (OrderStatus::Fulfilled, FulfillmentAction::GrantDownload)
                }
            };
            OrderUpdate {
                order_id,
                customer_id,
                status,
                fulfillment,
                receipt: Receipt {
                    recipient: email,
                    amount_cents,
                },
            }
        }
    }
}

pub async fn run(queue: InfraiQueue, concurrency: usize, jobs_per_second: u32) -> Result<(), WorkerError> {
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut pace = interval(Duration::from_secs_f64(1.0 / jobs_per_second as f64));
    let mut tasks = JoinSet::new();

    loop {
        let messages = queue
            .consume::<CommerceJob>(CHECKOUT_QUEUE, concurrency as u16, 60)
            .await?;
        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        for message in messages {
            pace.tick().await;
            let permit = permits.clone().acquire_owned().await.expect("semaphore is open");
            let queue = queue.clone();
            tasks.spawn(async move {
                let _permit = permit;
                let update = decide(message.payload);
                let publish_key = format!("order-update:{}", message.message_id);
                queue
                    .publish(ORDER_UPDATE_QUEUE, &update, &publish_key)
                    .await?;
                queue.ack(CHECKOUT_QUEUE, &message.message_id).await?;
                println!(
                    "order_id={} status={:?} message_id={}",
                    update.order_id, update.status, message.message_id
                );
                Ok::<(), QueueClientError>(())
            });
        }

        while let Some(result) = tasks.try_join_next() {
            result??;
        }
    }
}
