use rate_limited_commerce_worker::{infrai_queue::InfraiQueue, queue_worker};

#[tokio::main]
async fn main() -> Result<(), queue_worker::WorkerError> {
    let queue = InfraiQueue::from_env()?;
    queue_worker::run(queue, 4, 8).await
}

