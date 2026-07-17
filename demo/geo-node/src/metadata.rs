use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Consensus-backed authority for durable prefix-range ownership.
///
/// A production deployment points this at a three-or-more member etcd v3
/// cluster. `Txn` compares and writes are committed through etcd's Raft log,
/// so a Redis or network partition cannot independently grant the same range
/// to two nodes. The key is intentionally retained after bootstrap; only an
/// explicit merge releases it.
#[derive(Clone)]
pub struct EtcdRangeAuthority {
    endpoints: Vec<String>,
    client: Client,
    key_prefix: String,
}

pub enum ClaimResult {
    Granted,
    HeldBy(String),
}

impl EtcdRangeAuthority {
    pub fn new(endpoints: Vec<String>, client: Client, namespace: &str) -> Self {
        Self {
            endpoints: endpoints
                .into_iter()
                .map(|endpoint| endpoint.trim_end_matches('/').to_string())
                .collect(),
            client,
            key_prefix: format!("/geo-redis/{namespace}/range-claims/"),
        }
    }

    pub fn enabled(&self) -> bool {
        !self.endpoints.is_empty()
    }

    pub async fn claim(&self, prefix_start: &str, node_id: &str) -> Result<ClaimResult> {
        let key = self.key(prefix_start);
        let request = TxnRequest {
            compare: vec![Compare {
                key: encode(&key),
                target: "VERSION",
                result: "EQUAL",
                version: Some("0"),
                value: None,
            }],
            success: vec![TxnOperation {
                request_put: Some(PutRequest {
                    key: encode(&key),
                    value: encode(node_id),
                }),
                request_range: None,
                request_delete_range: None,
            }],
            failure: vec![TxnOperation {
                request_put: None,
                request_range: Some(RangeRequest { key: encode(&key) }),
                request_delete_range: None,
            }],
        };
        let response: TxnResponse = self.post("/v3/kv/txn", &request).await?;
        if response.succeeded {
            return Ok(ClaimResult::Granted);
        }
        let holder = response
            .responses
            .into_iter()
            .flat_map(|response| response.response_range.into_iter())
            .flat_map(|range| range.kvs.into_iter())
            .next()
            .map(|kv| decode(&kv.value))
            .transpose()?
            .unwrap_or_else(|| "unknown".to_string());
        Ok(ClaimResult::HeldBy(holder))
    }

    pub async fn release(&self, prefix_start: &str, node_id: &str) -> Result<()> {
        let key = self.key(prefix_start);
        let request = TxnRequest {
            compare: vec![Compare {
                key: encode(&key),
                target: "VALUE",
                result: "EQUAL",
                version: None,
                value: Some(encode(node_id)),
            }],
            success: vec![TxnOperation {
                request_put: None,
                request_range: None,
                request_delete_range: Some(DeleteRequest { key: encode(&key) }),
            }],
            failure: vec![],
        };
        let response: TxnResponse = self.post("/v3/kv/txn", &request).await?;
        if !response.succeeded {
            bail!("range claim for {prefix_start:?} is not owned by {node_id}");
        }
        Ok(())
    }

    fn key(&self, prefix_start: &str) -> String {
        format!(
            "{}{}",
            self.key_prefix,
            if prefix_start.is_empty() {
                "root"
            } else {
                prefix_start
            }
        )
    }

    async fn post<T: Serialize, R: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R> {
        let mut errors = Vec::new();
        for endpoint in &self.endpoints {
            match self
                .client
                .post(format!("{endpoint}{path}"))
                .json(body)
                .send()
                .await
            {
                Ok(response) => match response.error_for_status() {
                    Ok(response) => {
                        return response.json().await.context("decode etcd v3 response")
                    }
                    Err(error) => errors.push(error.to_string()),
                },
                Err(error) => errors.push(error.to_string()),
            }
        }
        bail!("all etcd metadata endpoints failed: {}", errors.join("; "))
    }
}

fn encode(value: &str) -> String {
    STANDARD.encode(value)
}

fn decode(value: &str) -> Result<String> {
    String::from_utf8(STANDARD.decode(value)?).context("decode etcd base64 value")
}

#[derive(Serialize)]
struct TxnRequest {
    compare: Vec<Compare>,
    success: Vec<TxnOperation>,
    failure: Vec<TxnOperation>,
}

#[derive(Serialize)]
struct Compare {
    key: String,
    target: &'static str,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

#[derive(Serialize)]
struct TxnOperation {
    #[serde(skip_serializing_if = "Option::is_none")]
    request_put: Option<PutRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_range: Option<RangeRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_delete_range: Option<DeleteRequest>,
}

#[derive(Serialize)]
struct PutRequest {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct RangeRequest {
    key: String,
}

#[derive(Serialize)]
struct DeleteRequest {
    key: String,
}

#[derive(Deserialize)]
struct TxnResponse {
    succeeded: bool,
    #[serde(default)]
    responses: Vec<TxnOperationResponse>,
}

#[derive(Deserialize)]
struct TxnOperationResponse {
    response_range: Option<RangeResponse>,
}

#[derive(Deserialize)]
struct RangeResponse {
    #[serde(default)]
    kvs: Vec<KeyValue>,
}

#[derive(Deserialize)]
struct KeyValue {
    value: String,
}
