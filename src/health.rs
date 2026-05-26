use crate::{
    rpc::{JsonRpcHttpClient, RpcClientError},
    types::parse_quantity,
};
use reqwest::{Response, StatusCode, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
use thiserror::Error;

const HEALTHZ_ENDPOINT: &str = "/healthz";
const MAX_HEALTH_RESPONSE_BYTES: usize = 1 << 20;

#[derive(Clone, Debug)]
pub enum RollupBoostHealthConfig {
    StatusCode { base_url: Url, timeout: Duration },
    Json { url: String, timeout: Duration },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollupBoostHealthStatus {
    Healthy,
    Partial,
    Unhealthy,
}

#[derive(Clone, Debug)]
pub struct RollupBoostHealthClient {
    config: RollupBoostHealthConfig,
    http: reqwest::Client,
}

#[derive(Clone, Debug)]
pub struct SupervisorHealthConfig {
    pub rpc: Url,
}

#[derive(Clone, Debug)]
pub struct SupervisorHealthClient {
    rpc: JsonRpcHttpClient,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionP2pCheckApi {
    Net,
    Admin,
}

#[derive(Clone, Debug)]
pub struct ExecutionP2pHealthConfig {
    pub rpc: Url,
    pub check_api: ExecutionP2pCheckApi,
    pub min_peer_count: u64,
}

#[derive(Clone, Debug)]
pub struct ExecutionP2pHealthClient {
    rpc: JsonRpcHttpClient,
    check_api: ExecutionP2pCheckApi,
}

#[derive(Debug, Error)]
pub enum RollupBoostHealthError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("unexpected status code {0}")]
    UnexpectedStatus(StatusCode),
    #[error("unexpected rollup_boost_health {0:?}")]
    UnexpectedJsonHealth(String),
    #[error("invalid health response: {0}")]
    InvalidResponse(String),
}

#[derive(Debug, Deserialize)]
struct RollupBoostNextHealthResponse {
    rollup_boost_health: String,
}

impl RollupBoostHealthClient {
    pub fn new(config: RollupBoostHealthConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    pub async fn check(&self) -> Result<RollupBoostHealthStatus, RollupBoostHealthError> {
        match &self.config {
            RollupBoostHealthConfig::StatusCode { base_url, timeout } => {
                self.check_status_code(base_url, *timeout).await
            }
            RollupBoostHealthConfig::Json { url, timeout } => self.check_json(url, *timeout).await,
        }
    }

    async fn check_status_code(
        &self,
        base_url: &Url,
        timeout: Duration,
    ) -> Result<RollupBoostHealthStatus, RollupBoostHealthError> {
        let response = self
            .http
            .get(healthz_url(base_url))
            .timeout(timeout)
            .send()
            .await?;
        let status = response.status();
        let _ = drain_body(response).await;

        match status {
            StatusCode::OK => Ok(RollupBoostHealthStatus::Healthy),
            StatusCode::PARTIAL_CONTENT => Ok(RollupBoostHealthStatus::Partial),
            StatusCode::SERVICE_UNAVAILABLE => Ok(RollupBoostHealthStatus::Unhealthy),
            other => Err(RollupBoostHealthError::UnexpectedStatus(other)),
        }
    }

    async fn check_json(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<RollupBoostHealthStatus, RollupBoostHealthError> {
        let response = self.http.get(url).timeout(timeout).send().await?;
        let status = response.status();
        if status != StatusCode::OK {
            return Err(RollupBoostHealthError::UnexpectedStatus(status));
        }

        let body = read_limited_body(response).await?;
        let mut decoder = serde_json::Deserializer::from_slice(&body);
        let payload = RollupBoostNextHealthResponse::deserialize(&mut decoder)
            .map_err(|err| RollupBoostHealthError::InvalidResponse(err.to_string()))?;

        match payload.rollup_boost_health.as_str() {
            "Healthy" => Ok(RollupBoostHealthStatus::Healthy),
            "PartialContent" => Ok(RollupBoostHealthStatus::Partial),
            "ServiceUnavailable" => Ok(RollupBoostHealthStatus::Unhealthy),
            other => Err(RollupBoostHealthError::UnexpectedJsonHealth(
                other.to_string(),
            )),
        }
    }
}

async fn drain_body(mut response: Response) -> Result<(), RollupBoostHealthError> {
    while response.chunk().await?.is_some() {}
    Ok(())
}

async fn read_limited_body(mut response: Response) -> Result<Vec<u8>, RollupBoostHealthError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        let remaining = MAX_HEALTH_RESPONSE_BYTES.saturating_sub(body.len());
        if remaining == 0 {
            break;
        }
        if chunk.len() <= remaining {
            body.extend_from_slice(&chunk);
        } else {
            body.extend_from_slice(&chunk[..remaining]);
            break;
        }
    }
    Ok(body)
}

impl SupervisorHealthClient {
    pub fn new(config: SupervisorHealthConfig) -> Self {
        Self {
            rpc: JsonRpcHttpClient::new(config.rpc),
        }
    }

    pub async fn check(&self) -> Result<(), RpcClientError> {
        let _: Value = self.rpc.call("supervisor_syncStatus", json!([])).await?;
        Ok(())
    }
}

impl ExecutionP2pHealthClient {
    pub fn new(config: ExecutionP2pHealthConfig) -> Self {
        Self {
            rpc: JsonRpcHttpClient::new(config.rpc),
            check_api: config.check_api,
        }
    }

    pub async fn peer_count(&self) -> Result<u64, RpcClientError> {
        match self.check_api {
            ExecutionP2pCheckApi::Net => {
                let raw: Value = self.rpc.call("net_peerCount", json!([])).await?;
                parse_quantity(&raw).map_err(RpcClientError::Type)
            }
            ExecutionP2pCheckApi::Admin => {
                let peers: Vec<Value> = self.rpc.call("admin_peers", json!([])).await?;
                Ok(peers.len() as u64)
            }
        }
    }
}

fn healthz_url(base_url: &Url) -> Url {
    let mut url = base_url.clone();
    let path = format!("{}{}", url.path().trim_end_matches('/'), HEALTHZ_ENDPOINT);
    url.set_path(&path);
    url
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::State,
        http::{header, StatusCode},
        routing::get,
        routing::post,
        Json, Router,
    };
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tokio::{io::AsyncWriteExt, net::TcpListener};

    async fn server(
        status: StatusCode,
        body: Value,
        route: &'static str,
    ) -> (Url, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let state = Arc::new((status, body));
        async fn handler(
            State(state): State<Arc<(StatusCode, Value)>>,
        ) -> (StatusCode, Json<Value>) {
            (state.0, Json(state.1.clone()))
        }
        let app = Router::new().route(route, get(handler)).with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, handle)
    }

    async fn raw_server(
        status: StatusCode,
        body: String,
        route: &'static str,
    ) -> (Url, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let state = Arc::new((status, body));
        async fn handler(
            State(state): State<Arc<(StatusCode, String)>>,
        ) -> (StatusCode, [(header::HeaderName, &'static str); 1], String) {
            (
                state.0,
                [(header::CONTENT_TYPE, "application/json")],
                state.1.clone(),
            )
        }
        let app = Router::new().route(route, get(handler)).with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, handle)
    }

    async fn broken_chunked_server(status: StatusCode) -> (Url, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let reason = status.canonical_reason().unwrap_or("status");
            let response = format!(
                "HTTP/1.1 {} {}\r\ncontent-type: text/plain\r\ntransfer-encoding: chunked\r\n\r\nzz\r\nbroken",
                status.as_u16(),
                reason
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
        (url, handle)
    }

    async fn rpc_server(result: Value) -> (Url, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let state = Arc::new(result);
        async fn handler(State(result): State<Arc<Value>>) -> Json<Value> {
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": result.as_ref()}))
        }
        let app = Router::new().route("/", post(handler)).with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, handle)
    }

    #[tokio::test]
    async fn status_code_health_maps_partial_content() {
        let (url, _handle) = server(StatusCode::PARTIAL_CONTENT, json!(null), "/healthz").await;
        let client = RollupBoostHealthClient::new(RollupBoostHealthConfig::StatusCode {
            base_url: url,
            timeout: Duration::from_secs(1),
        });

        let status = client.check().await.unwrap();

        assert_eq!(status, RollupBoostHealthStatus::Partial);
    }

    #[tokio::test]
    async fn status_code_health_uses_status_when_body_drain_fails_like_upstream() {
        let (url, _handle) = broken_chunked_server(StatusCode::PARTIAL_CONTENT).await;
        let client = RollupBoostHealthClient::new(RollupBoostHealthConfig::StatusCode {
            base_url: url,
            timeout: Duration::from_secs(1),
        });

        let status = client.check().await.unwrap();

        assert_eq!(status, RollupBoostHealthStatus::Partial);
    }

    #[tokio::test]
    async fn json_health_maps_service_unavailable_payload() {
        let (url, _handle) = server(
            StatusCode::OK,
            json!({"version": "v1", "rollup_boost_health": "ServiceUnavailable"}),
            "/healthz",
        )
        .await;
        let client = RollupBoostHealthClient::new(RollupBoostHealthConfig::Json {
            url: url.join("/healthz").unwrap().to_string(),
            timeout: Duration::from_secs(1),
        });

        let status = client.check().await.unwrap();

        assert_eq!(status, RollupBoostHealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn json_health_truncates_oversized_invalid_body_like_upstream() {
        let (url, _handle) = server(
            StatusCode::OK,
            json!({
                "version": "v1",
                "rollup_boost_health": "Healthy",
                "padding": "x".repeat(MAX_HEALTH_RESPONSE_BYTES),
            }),
            "/healthz",
        )
        .await;
        let client = RollupBoostHealthClient::new(RollupBoostHealthConfig::Json {
            url: url.join("/healthz").unwrap().to_string(),
            timeout: Duration::from_secs(1),
        });

        let err = client.check().await.unwrap_err();

        assert!(matches!(err, RollupBoostHealthError::InvalidResponse(_)));
    }

    #[tokio::test]
    async fn json_health_accepts_oversized_body_when_limited_json_is_valid() {
        let mut body = r#"{"version":"v1","rollup_boost_health":"Healthy"}"#.to_string();
        body.push_str(&" ".repeat(MAX_HEALTH_RESPONSE_BYTES + 16));
        let (url, _handle) = raw_server(StatusCode::OK, body, "/healthz").await;
        let client = RollupBoostHealthClient::new(RollupBoostHealthConfig::Json {
            url: url.join("/healthz").unwrap().to_string(),
            timeout: Duration::from_secs(1),
        });

        let status = client.check().await.unwrap();

        assert_eq!(status, RollupBoostHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn json_health_decodes_first_json_value_like_upstream() {
        let body = r#"{"version":"v1","rollup_boost_health":"Healthy"}{"ignored":true}"#;
        let (url, _handle) = raw_server(StatusCode::OK, body.to_string(), "/healthz").await;
        let client = RollupBoostHealthClient::new(RollupBoostHealthConfig::Json {
            url: url.join("/healthz").unwrap().to_string(),
            timeout: Duration::from_secs(1),
        });

        let status = client.check().await.unwrap();

        assert_eq!(status, RollupBoostHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn json_health_reports_invalid_url_as_http_error_like_upstream() {
        let client = RollupBoostHealthClient::new(RollupBoostHealthConfig::Json {
            url: "not a url that reqwest would accept".to_string(),
            timeout: Duration::from_secs(1),
        });

        let err = client.check().await.unwrap_err();

        assert!(matches!(err, RollupBoostHealthError::Http(_)));
    }

    #[tokio::test]
    async fn supervisor_health_calls_sync_status() {
        let (url, _handle) = rpc_server(json!({"minSyncedL1": {}, "chains": {}})).await;
        let client = SupervisorHealthClient::new(SupervisorHealthConfig { rpc: url });

        client.check().await.unwrap();
    }

    #[tokio::test]
    async fn execution_p2p_net_peer_count_parses_quantity() {
        let (url, _handle) = rpc_server(json!("0x2")).await;
        let client = ExecutionP2pHealthClient::new(ExecutionP2pHealthConfig {
            rpc: url,
            check_api: ExecutionP2pCheckApi::Net,
            min_peer_count: 2,
        });

        let count = client.peer_count().await.unwrap();

        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn execution_p2p_admin_peer_count_uses_array_length() {
        let (url, _handle) = rpc_server(json!([{"id": "a"}, {"id": "b"}])).await;
        let client = ExecutionP2pHealthClient::new(ExecutionP2pHealthConfig {
            rpc: url,
            check_api: ExecutionP2pCheckApi::Admin,
            min_peer_count: 2,
        });

        let count = client.peer_count().await.unwrap();

        assert_eq!(count, 2);
    }
}
