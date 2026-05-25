use crate::types::{parse_quantity, BlockInfo, Hash, PayloadEnvelope, PeerStats, SyncStatus};
use reqwest::Url;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use thiserror::Error;

const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum RpcClientError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json-rpc error {code}: {message}")]
    JsonRpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("json-rpc response is missing result and error")]
    MissingResult,
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("type error: {0}")]
    Type(#[from] crate::types::TypeError),
}

impl RpcClientError {
    pub fn is_invalid_params(&self) -> bool {
        matches!(self, Self::JsonRpc { code: -32602, .. })
    }

    pub fn is_method_not_found(&self) -> bool {
        matches!(self, Self::JsonRpc { code: -32601, .. })
    }
}

#[derive(Debug)]
pub struct JsonRpcHttpClient {
    url: Url,
    client: reqwest::Client,
    next_id: AtomicU64,
}

impl Clone for JsonRpcHttpClient {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            client: self.client.clone(),
            next_id: AtomicU64::new(self.next_id.load(Ordering::Relaxed)),
        }
    }
}

impl JsonRpcHttpClient {
    pub fn new(url: Url) -> Self {
        Self::with_timeout(url, RPC_CALL_TIMEOUT)
    }

    pub fn with_timeout(url: Url, timeout: Duration) -> Self {
        Self {
            url,
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("valid reqwest client config"),
            next_id: AtomicU64::new(1),
        }
    }

    pub async fn call<T>(&self, method: &str, params: Value) -> Result<T, RpcClientError>
    where
        T: DeserializeOwned,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let response = self
            .client
            .post(self.url.clone())
            .json(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }))
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;

        if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(RpcClientError::InvalidResponse(
                "json-rpc response is missing jsonrpc 2.0".to_string(),
            ));
        }
        let expected_id = json!(id);
        match response.get("id") {
            Some(actual_id) if actual_id == &expected_id => {}
            Some(actual_id) => {
                return Err(RpcClientError::InvalidResponse(format!(
                    "json-rpc response id mismatch: expected {expected_id}, got {actual_id}"
                )));
            }
            None => {
                return Err(RpcClientError::InvalidResponse(
                    "json-rpc response is missing id".to_string(),
                ));
            }
        }

        if let Some(err) = response.get("error") {
            let code = err.get("code").and_then(Value::as_i64).unwrap_or(0);
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown json-rpc error")
                .to_string();
            return Err(RpcClientError::JsonRpc {
                code,
                message,
                data: err.get("data").cloned(),
            });
        }

        let result = response
            .get("result")
            .ok_or(RpcClientError::MissingResult)?;
        serde_json::from_value(result.clone())
            .map_err(|err| RpcClientError::InvalidResponse(err.to_string()))
    }
}

#[derive(Clone, Debug)]
pub struct RollupNodeClient {
    rpc: JsonRpcHttpClient,
}

impl RollupNodeClient {
    pub fn new(url: Url) -> Self {
        Self {
            rpc: JsonRpcHttpClient::new(url),
        }
    }

    pub async fn start_sequencer_with_hash(&self, hash: Hash) -> Result<(), RpcClientError> {
        self.rpc.call("admin_startSequencer", json!([hash])).await
    }

    pub async fn start_sequencer_parameterless(&self) -> Result<(), RpcClientError> {
        self.rpc.call("admin_startSequencer", json!([])).await
    }

    pub async fn stop_sequencer(&self) -> Result<Hash, RpcClientError> {
        self.rpc.call("admin_stopSequencer", json!([])).await
    }

    pub async fn sequencer_active(&self) -> Result<bool, RpcClientError> {
        self.rpc.call("admin_sequencerActive", json!([])).await
    }

    pub async fn conductor_enabled(&self) -> Result<bool, RpcClientError> {
        self.rpc.call("admin_conductorEnabled", json!([])).await
    }

    pub async fn sync_status(&self) -> Result<SyncStatus, RpcClientError> {
        self.rpc.call("optimism_syncStatus", json!([])).await
    }

    pub async fn peer_stats(&self) -> Result<PeerStats, RpcClientError> {
        self.rpc.call("opp2p_peerStats", json!([])).await
    }

    pub async fn post_unsafe_payload(
        &self,
        payload: &PayloadEnvelope,
    ) -> Result<(), RpcClientError> {
        self.rpc
            .call("admin_postUnsafePayload", json!([payload.raw()]))
            .await
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionClient {
    rpc: JsonRpcHttpClient,
}

impl ExecutionClient {
    pub fn new(url: Url) -> Self {
        Self {
            rpc: JsonRpcHttpClient::new(url),
        }
    }

    pub async fn latest_unsafe_block(&self) -> Result<BlockInfo, RpcClientError> {
        let raw: Value = self
            .rpc
            .call("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let hash = raw
            .get("hash")
            .and_then(Value::as_str)
            .ok_or(crate::types::TypeError::MissingField("hash"))?
            .parse()?;
        let number = raw
            .get("number")
            .ok_or(crate::types::TypeError::MissingField("number"))
            .and_then(parse_quantity)?;
        Ok(BlockInfo { hash, number })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::post, Json, Router};
    use std::sync::{Arc, Mutex};
    use tokio::{net::TcpListener, task::JoinHandle};

    async fn rpc_server(response: Value) -> (Url, JoinHandle<()>) {
        async fn handler(State(response): State<Arc<Value>>) -> Json<Value> {
            Json(response.as_ref().clone())
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let app = Router::new()
            .route("/", post(handler))
            .with_state(Arc::new(response));
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, handle)
    }

    async fn hanging_rpc_server() -> (Url, JoinHandle<()>) {
        async fn handler() -> Json<Value> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Json(json!({"jsonrpc":"2.0","id":1,"result":true}))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let app = Router::new().route("/", post(handler));
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, handle)
    }

    async fn recording_rpc_server() -> (Url, Arc<Mutex<Option<Value>>>, JoinHandle<()>) {
        async fn handler(
            State(last_request): State<Arc<Mutex<Option<Value>>>>,
            Json(request): Json<Value>,
        ) -> Json<Value> {
            *last_request.lock().unwrap() = Some(request.clone());
            let id = request.get("id").cloned().unwrap_or(Value::Null);
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                    "number": "0x2a"
                }
            }))
        }

        let last_request = Arc::new(Mutex::new(None));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let app = Router::new()
            .route("/", post(handler))
            .with_state(last_request.clone());
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, last_request, handle)
    }

    #[tokio::test]
    async fn rejects_mismatched_response_id() {
        let (url, handle) = rpc_server(json!({"jsonrpc":"2.0","id":99,"result":true})).await;
        let err = JsonRpcHttpClient::new(url)
            .call::<bool>("test_method", json!([]))
            .await
            .unwrap_err();
        handle.abort();

        match err {
            RpcClientError::InvalidResponse(message) => {
                assert!(message.contains("response id mismatch"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn rejects_missing_jsonrpc_version() {
        let (url, handle) = rpc_server(json!({"id":1,"result":true})).await;
        let err = JsonRpcHttpClient::new(url)
            .call::<bool>("test_method", json!([]))
            .await
            .unwrap_err();
        handle.abort();

        match err {
            RpcClientError::InvalidResponse(message) => {
                assert!(message.contains("missing jsonrpc 2.0"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn times_out_hung_rpc_call() {
        let (url, handle) = hanging_rpc_server().await;
        let err = JsonRpcHttpClient::with_timeout(url, Duration::from_millis(25))
            .call::<bool>("test_method", json!([]))
            .await
            .unwrap_err();
        handle.abort();

        match err {
            RpcClientError::Http(err) => assert!(err.is_timeout()),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn latest_unsafe_block_uses_upstream_latest_label() {
        let (url, last_request, handle) = recording_rpc_server().await;

        let block = ExecutionClient::new(url)
            .latest_unsafe_block()
            .await
            .unwrap();
        handle.abort();

        assert_eq!(block.number, 42);
        let request = last_request.lock().unwrap().clone().unwrap();
        assert_eq!(request["method"], "eth_getBlockByNumber");
        assert_eq!(request["params"], json!(["latest", false]));
    }
}
