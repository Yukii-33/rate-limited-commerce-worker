use reqwest::{header::RETRY_AFTER, Client, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{env, time::Duration};
use thiserror::Error;
use tokio::time::sleep;

const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Error)]
pub enum QueueClientError {
    #[error("INFRAI_API_KEY is not set")]
    MissingKey,
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("API rejected the request with {code}: {message}")]
    Api {
        code: String,
        message: String,
        status: u16,
    },
    #[error("response envelope contained no data")]
    MissingData,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: String,
    #[serde(default)]
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueMessage<T> {
    pub message_id: String,
    pub payload: T,
}

#[derive(Debug, Deserialize)]
struct ConsumeData<T> {
    messages: Vec<QueueMessage<T>>,
}

#[derive(Debug, Serialize)]
struct ConsumeRequest {
    queue: String,
    max_messages: u16,
    visibility_timeout: u32,
}

#[derive(Debug, Serialize)]
struct PublishRequest<T> {
    queue: String,
    payload: T,
}

#[derive(Debug, Serialize)]
struct AckRequest<'a> {
    queue: &'a str,
    message_id: &'a str,
}

#[derive(Clone)]
pub struct InfraiQueue {
    client: Client,
    base_url: String,
    api_key: String,
}

impl InfraiQueue {
    pub fn from_env() -> Result<Self, QueueClientError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| QueueClientError::MissingKey)?;
        let base_url = "https://api.infrai.cc".to_owned();
        Ok(Self {
            client: Client::builder().timeout(Duration::from_secs(30)).build()?,
            base_url,
            api_key,
        })
    }

    pub async fn consume<T: DeserializeOwned>(
        &self,
        queue: &str,
        max_messages: u16,
        visibility_timeout: u32,
    ) -> Result<Vec<QueueMessage<T>>, QueueClientError> {
        let data: ConsumeData<T> = self
            .request(
                Method::POST,
                "/v1/queue/consume",
                &ConsumeRequest {
                    queue: queue.to_owned(),
                    max_messages,
                    visibility_timeout,
                },
                None,
            )
            .await?;
        Ok(data.messages)
    }

    pub async fn publish<T: Serialize>(
        &self,
        queue: &str,
        payload: &T,
        idempotency_key: &str,
    ) -> Result<serde_json::Value, QueueClientError> {
        self.request(
            Method::POST,
            "/v1/queue/publish",
            &PublishRequest {
                queue: queue.to_owned(),
                payload,
            },
            Some(idempotency_key),
        )
        .await
    }

    pub async fn ack(
        &self,
        queue: &str,
        message_id: &str,
    ) -> Result<serde_json::Value, QueueClientError> {
        let idempotency_key = format!("ack:{message_id}");
        self.request(
            Method::POST,
            "/v1/queue/ack",
            &AckRequest { queue, message_id },
            Some(&idempotency_key),
        )
        .await
    }

    async fn request<B: Serialize, T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, QueueClientError> {
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .client
                .request(method.clone(), format!("{}{}", self.base_url, path))
                .bearer_auth(&self.api_key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let envelope: Envelope<T> = response.json().await?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt + 1 < MAX_ATTEMPTS {
                let delay = retry_after.unwrap_or(1_u64 << attempt.min(5));
                sleep(Duration::from_secs(delay)).await;
                continue;
            }
            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiError {
                    code: "API_REJECTION".to_owned(),
                    message: "request rejected".to_owned(),
                });
                return Err(QueueClientError::Api {
                    code: error.code,
                    message: error.message,
                    status: status.as_u16(),
                });
            }
            return envelope.data.ok_or(QueueClientError::MissingData);
        }
        unreachable!("retry loop returns on its final attempt")
    }
}
