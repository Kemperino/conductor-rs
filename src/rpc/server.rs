use crate::{
    conductor::Conductor,
    consensus::Consensus,
    rpc::{JsonRpcHttpClient, RpcClientError},
    sequencer::SequencerControl,
    types::{parse_quantity, PayloadEnvelope},
};
use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response as AxumResponse},
    routing::get,
    Json, Router,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::{net::SocketAddr, sync::Arc, time::Instant};
use thiserror::Error;
use tokio::net::TcpListener;
use url::Url;

#[derive(Debug, Error)]
pub enum JsonRpcServerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("server error: {0}")]
    Server(#[from] axum::Error),
}

#[derive(Debug, Deserialize)]
struct Request {
    jsonrpc: Option<String>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct Response {
    jsonrpc: &'static str,
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Debug, Serialize)]
struct RpcError {
    code: i64,
    message: String,
}

type SharedConductor<C, S> = Arc<Conductor<C, S>>;
type SharedState<C, S> = Arc<RpcState<C, S>>;

#[derive(Clone, Debug)]
pub struct ProxyConfig {
    pub execution_rpc: Url,
    pub node_rpc: Url,
}

#[derive(Debug)]
struct RpcState<C, S> {
    conductor: SharedConductor<C, S>,
    proxy: Option<ProxyClients>,
}

#[derive(Clone, Debug)]
struct ProxyClients {
    execution: JsonRpcHttpClient,
    node: JsonRpcHttpClient,
}

pub async fn serve<C, S>(
    conductor: SharedConductor<C, S>,
    addr: SocketAddr,
) -> Result<(), JsonRpcServerError>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    serve_with_proxy(conductor, addr, None).await
}

pub async fn serve_with_proxy<C, S>(
    conductor: SharedConductor<C, S>,
    addr: SocketAddr,
    proxy: Option<ProxyConfig>,
) -> Result<(), JsonRpcServerError>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    let state = Arc::new(RpcState {
        conductor: conductor.clone(),
        proxy: proxy.map(|proxy| ProxyClients {
            execution: JsonRpcHttpClient::new(proxy.execution_rpc),
            node: JsonRpcHttpClient::new(proxy.node_rpc),
        }),
    });
    let app = Router::new()
        .route("/", get(ws_handler::<C, S>).post(handle::<C, S>))
        .route("/ws", get(ws_handler::<C, S>))
        .route("/ws/", get(ws_handler::<C, S>))
        .route("/healthz", get(healthz))
        .route("/healthz/", get(healthz))
        .with_state(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown({
            let conductor = conductor.clone();
            async move {
                conductor.wait_stopped().await;
            }
        })
        .await?;
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "application/json")],
        format!("{{\"version\":\"{}\"}}\n", env!("CARGO_PKG_VERSION")),
    )
}

async fn handle<C, S>(State(state): State<SharedState<C, S>>, body: Bytes) -> AxumResponse
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    match handle_bytes(state, &body).await {
        Some(response) => Json(response).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn ws_handler<C, S>(
    State(state): State<SharedState<C, S>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    ws.on_upgrade(move |socket| handle_ws(state, socket))
}

async fn handle_ws<C, S>(state: SharedState<C, S>, mut socket: WebSocket)
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    while let Some(message) = socket.next().await {
        let Ok(message) = message else {
            break;
        };
        let response = match message {
            Message::Text(text) => handle_bytes(state.clone(), text.as_bytes()).await,
            Message::Binary(bytes) => handle_bytes(state.clone(), bytes.as_ref()).await,
            Message::Ping(bytes) => {
                if socket.send(Message::Pong(bytes)).await.is_err() {
                    break;
                }
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(frame) => {
                let _ = socket.send(Message::Close(frame)).await;
                break;
            }
        };
        let Some(response) = response else {
            continue;
        };
        let Ok(text) = serde_json::to_string(&response) else {
            break;
        };
        if socket.send(Message::Text(text)).await.is_err() {
            break;
        }
    }
}

async fn handle_bytes<C, S>(state: SharedState<C, S>, body: &[u8]) -> Option<Value>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    let payload = match serde_json::from_slice::<Value>(body) {
        Ok(payload) => payload,
        Err(err) => {
            return Some(response_value(
                None,
                Err(parse_error(format!("parse error: {err}"))),
            ));
        }
    };
    handle_payload(state, payload).await
}

async fn handle_payload<C, S>(state: SharedState<C, S>, payload: Value) -> Option<Value>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    match payload {
        Value::Array(requests) if requests.is_empty() => Some(response_value(
            None,
            Err(invalid_request("batch request must not be empty")),
        )),
        Value::Array(requests) => {
            let mut responses = Vec::with_capacity(requests.len());
            for request in requests {
                if let Some(response) = handle_request_value(state.clone(), request).await {
                    responses.push(response);
                }
            }
            if responses.is_empty() {
                None
            } else {
                Some(Value::Array(responses))
            }
        }
        request => handle_request_value(state, request).await,
    }
}

async fn handle_request_value<C, S>(state: SharedState<C, S>, value: Value) -> Option<Value>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    let is_notification = value
        .as_object()
        .is_some_and(|request| !request.contains_key("id"));
    let id = value.get("id").cloned();
    let request = match serde_json::from_value::<Request>(value) {
        Ok(request) => request,
        Err(err) => {
            return Some(response_value(
                id,
                Err(invalid_request(format!("invalid request: {err}"))),
            ));
        }
    };
    let method = request.method.clone();
    let params_size = serde_json::to_vec(&request.params)
        .map(|params| params.len())
        .unwrap_or(0);
    let started = Instant::now();
    let response = dispatch(state.clone(), request).await;
    if is_notification {
        state
            .conductor
            .metrics()
            .record_rpc_server_notification(&method);
        None
    } else {
        let (error_code, result_size) = match &response {
            Ok(result) => (
                None,
                serde_json::to_vec(result).ok().map(|result| result.len()),
            ),
            Err(error) => (Some(error.code), None),
        };
        state.conductor.metrics().record_rpc_server_request(
            &method,
            params_size,
            started.elapsed(),
            error_code,
            result_size,
        );
        Some(response_value(id, response))
    }
}

fn response_value(id: Option<Value>, response: Result<Value, RpcError>) -> Value {
    let response = match response {
        Ok(result) => Response {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        },
        Err(error) => Response {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        },
    };
    serde_json::to_value(response).expect("serializing JSON-RPC response cannot fail")
}

async fn dispatch<C, S>(state: SharedState<C, S>, request: Request) -> Result<Value, RpcError>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    if request.jsonrpc.as_deref() != Some("2.0") {
        return Err(invalid_request("jsonrpc must be 2.0"));
    }
    if state.proxy.is_none() && is_proxy_method(&request.method) {
        return Err(method_not_found());
    }

    let conductor = state.conductor.clone();
    match request.method.as_str() {
        "rpc_modules" => {
            no_params(&request.params)?;
            Ok(rpc_modules(state.proxy.is_some()))
        }
        "health_status" => {
            no_params(&request.params)?;
            Ok(json!(env!("CARGO_PKG_VERSION")))
        }
        "conductor_leader" => {
            no_params(&request.params)?;
            Ok(json!(conductor.leader()))
        }
        "conductor_leaderWithID" => {
            no_params(&request.params)?;
            Ok(json!(conductor.leader_with_id()))
        }
        "conductor_leaderOverridden" => {
            no_params(&request.params)?;
            Ok(json!(conductor.leader_overridden()))
        }
        "conductor_overrideLeader" => {
            let override_leader = bool_param(&request.params)?;
            conductor.set_leader_override(override_leader);
            Ok(Value::Null)
        }
        "conductor_paused" => {
            no_params(&request.params)?;
            Ok(json!(conductor.paused()))
        }
        "conductor_pause" => {
            no_params(&request.params)?;
            conductor.pause().await.map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_resume" => {
            no_params(&request.params)?;
            conductor.resume().await.map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_stopped" => {
            no_params(&request.params)?;
            Ok(json!(conductor.stopped()))
        }
        "conductor_stop" => {
            no_params(&request.params)?;
            conductor.stop().await.map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_active" => {
            no_params(&request.params)?;
            Ok(json!(conductor.active()))
        }
        "conductor_sequencerHealthy" => {
            no_params(&request.params)?;
            Ok(json!(conductor.sequencer_healthy().await))
        }
        "conductor_clusterMembership" => {
            no_params(&request.params)?;
            Ok(json!(conductor
                .cluster_membership()
                .await
                .map_err(internal)?))
        }
        "conductor_transferLeader" => {
            no_params(&request.params)?;
            conductor.transfer_leader().await.map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_transferLeaderToServer" => {
            let (id, addr) = string_string_params(&request.params)?;
            conductor
                .transfer_leader_to_server(id, addr)
                .await
                .map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_addServerAsVoter" => {
            let (id, addr, version) = membership_change_params(&request.params)?;
            conductor
                .add_server_as_voter(id, addr, version)
                .await
                .map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_addServerAsNonvoter" => {
            let (id, addr, version) = membership_change_params(&request.params)?;
            conductor
                .add_server_as_nonvoter(id, addr, version)
                .await
                .map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_removeServer" => {
            let (id, version) = id_version_params(&request.params)?;
            conductor
                .remove_server(id, version)
                .await
                .map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_demoteVoter" => {
            let (id, version) = id_version_params(&request.params)?;
            conductor
                .demote_voter(id, version)
                .await
                .map_err(internal)?;
            Ok(Value::Null)
        }
        "conductor_commitUnsafePayload" => {
            let payload = unsafe_payload_param(&request.params)?;
            conductor
                .commit_unsafe_payload(payload)
                .await
                .map_err(internal)?;
            Ok(Value::Null)
        }
        "eth_getBlockByNumber" => {
            let params = eth_get_block_by_number_proxy_params(&request.params)?;
            proxy_to_leader(
                &state,
                ProxyTarget::Execution,
                "eth_getBlockByNumber",
                params,
            )
            .await
        }
        "miner_setMaxDASize" => {
            let params = miner_set_max_da_size_proxy_params(&request.params)?;
            proxy_to_backend(&state, ProxyTarget::Execution, "miner_setMaxDASize", params).await
        }
        "optimism_syncStatus" => {
            no_params(&request.params)?;
            proxy_to_leader(&state, ProxyTarget::Node, "optimism_syncStatus", json!([])).await
        }
        "optimism_outputAtBlock" => {
            let params = output_at_block_proxy_params(&request.params)?;
            proxy_to_leader(&state, ProxyTarget::Node, "optimism_outputAtBlock", params).await
        }
        "optimism_rollupConfig" => {
            no_params(&request.params)?;
            proxy_to_leader(
                &state,
                ProxyTarget::Node,
                "optimism_rollupConfig",
                json!([]),
            )
            .await
        }
        "admin_sequencerActive" => {
            no_params(&request.params)?;
            proxy_to_leader(
                &state,
                ProxyTarget::Node,
                "admin_sequencerActive",
                json!([]),
            )
            .await
        }
        _ => Err(method_not_found()),
    }
}

fn rpc_modules(proxy_enabled: bool) -> Value {
    let mut modules = Map::new();
    modules.insert("rpc".to_string(), json!("1.0"));
    modules.insert("health".to_string(), json!("1.0"));
    modules.insert("conductor".to_string(), json!("1.0"));
    if proxy_enabled {
        modules.insert("eth".to_string(), json!("1.0"));
        modules.insert("miner".to_string(), json!("1.0"));
        modules.insert("optimism".to_string(), json!("1.0"));
        modules.insert("admin".to_string(), json!("1.0"));
    }
    Value::Object(modules)
}

fn is_proxy_method(method: &str) -> bool {
    matches!(
        method,
        "eth_getBlockByNumber"
            | "miner_setMaxDASize"
            | "optimism_syncStatus"
            | "optimism_outputAtBlock"
            | "optimism_rollupConfig"
            | "admin_sequencerActive"
    )
}

#[derive(Clone, Copy)]
enum ProxyTarget {
    Execution,
    Node,
}

async fn proxy_to_leader<C, S>(
    state: &SharedState<C, S>,
    target: ProxyTarget,
    method: &'static str,
    params: Value,
) -> Result<Value, RpcError>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    let response = proxy_to_backend(state, target, method, params).await?;
    if !state.conductor.leader() {
        return Err(RpcError {
            code: -32000,
            message: "refusing to proxy request to non-leader sequencer".to_string(),
        });
    }
    Ok(response)
}

async fn proxy_to_backend<C, S>(
    state: &SharedState<C, S>,
    target: ProxyTarget,
    method: &'static str,
    params: Value,
) -> Result<Value, RpcError>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    let proxy = state.proxy.as_ref().ok_or_else(method_not_found)?;
    let client = match target {
        ProxyTarget::Execution => &proxy.execution,
        ProxyTarget::Node => &proxy.node,
    };
    client
        .call(method, normalize_proxy_params(params))
        .await
        .map_err(proxy_error)
}

fn normalize_proxy_params(params: Value) -> Value {
    match params {
        Value::Null => json!([]),
        other => other,
    }
}

fn eth_get_block_by_number_proxy_params(params: &Value) -> Result<Value, RpcError> {
    let params = array_params(params)?;
    if params.len() != 2 {
        return Err(invalid_params(
            "expected block number and full transaction flag",
        ));
    }
    let block = match params[0].as_str() {
        Some(tag @ ("earliest" | "latest" | "pending" | "safe" | "finalized")) => {
            Value::String(tag.to_string())
        }
        Some(raw) => {
            let block = decode_hex_quantity(raw)?;
            if block > i64::MAX as u64 {
                return Err(invalid_params(format!("block number out of range {raw}")));
            }
            Value::String(format!("0x{block:x}"))
        }
        None => return Err(invalid_params("expected block number string")),
    };
    let full_tx = params[1]
        .as_bool()
        .ok_or_else(|| invalid_params("expected full transaction flag"))?;
    Ok(json!([block, full_tx]))
}

fn output_at_block_proxy_params(params: &Value) -> Result<Value, RpcError> {
    let params = array_params(params)?;
    if params.len() != 1 {
        return Err(invalid_params("expected block number"));
    }
    let raw = params[0]
        .as_str()
        .ok_or_else(|| invalid_params("expected hex block number string"))?;
    let block = decode_hex_quantity(raw)?;
    Ok(json!([format!("0x{block:x}")]))
}

fn miner_set_max_da_size_proxy_params(params: &Value) -> Result<Value, RpcError> {
    let params = array_params(params)?;
    if params.len() != 2 {
        return Err(invalid_params("expected max tx size and max block size"));
    }
    let max_tx_size = params[0]
        .as_str()
        .ok_or_else(|| invalid_params("expected hex max tx size"))?;
    let max_block_size = params[1]
        .as_str()
        .ok_or_else(|| invalid_params("expected hex max block size"))?;
    Ok(json!([
        canonical_hex_quantity(max_tx_size, "max tx size")?,
        canonical_hex_quantity(max_block_size, "max block size")?
    ]))
}

fn decode_hex_quantity(raw: &str) -> Result<u64, RpcError> {
    let canonical = canonical_hex_quantity(raw, "block number")?;
    u64::from_str_radix(&canonical[2..], 16)
        .map_err(|_| invalid_params(format!("invalid block number {raw}")))
}

fn canonical_hex_quantity(raw: &str, name: &'static str) -> Result<String, RpcError> {
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .ok_or_else(|| invalid_params(format!("expected 0x-prefixed {name}")))?;
    if hex.is_empty() {
        return Err(invalid_params(format!("invalid {name} {raw}")));
    }
    if hex.len() > 1 && hex.starts_with('0') {
        return Err(invalid_params("hex number with leading zero digits"));
    }
    if !hex.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_params(format!("invalid {name} {raw}")));
    }
    Ok(format!("0x{}", hex.to_ascii_lowercase()))
}

fn unsafe_payload_param(params: &Value) -> Result<PayloadEnvelope, RpcError> {
    let params = array_params(params)?;
    let payload = match params {
        [] => return Err(invalid_params("missing payload")),
        [payload] => PayloadEnvelope::new(payload.clone()),
        _ => return Err(invalid_params("expected single payload")),
    };
    payload
        .block_number()
        .map_err(|err| invalid_params(err.to_string()))?;
    payload
        .block_hash()
        .map_err(|err| invalid_params(err.to_string()))?;
    Ok(payload)
}

fn no_params(params: &Value) -> Result<(), RpcError> {
    let params = array_params(params)?;
    if params.is_empty() {
        Ok(())
    } else {
        Err(invalid_params("expected no params"))
    }
}

fn bool_param(params: &Value) -> Result<bool, RpcError> {
    let params = array_params(params)?;
    if params.len() != 1 {
        return Err(invalid_params("expected boolean override flag"));
    }
    match params[0] {
        Value::Bool(value) => Ok(value),
        _ => Err(invalid_params("expected boolean override flag")),
    }
}

fn array_params(params: &Value) -> Result<&[Value], RpcError> {
    match params {
        Value::Array(items) => Ok(items.as_slice()),
        Value::Null => Ok(&[]),
        _ => Err(invalid_params("expected params array")),
    }
}

fn membership_change_params(params: &Value) -> Result<(String, String, u64), RpcError> {
    let params = array_params(params)?;
    if params.len() != 3 {
        return Err(invalid_params("expected id, addr, version"));
    }
    Ok((
        string_param(&params[0], "id")?,
        string_param(&params[1], "addr")?,
        version_param(&params[2])?,
    ))
}

fn string_string_params(params: &Value) -> Result<(String, String), RpcError> {
    let params = array_params(params)?;
    if params.len() != 2 {
        return Err(invalid_params("expected id, addr"));
    }
    Ok((
        string_param(&params[0], "id")?,
        string_param(&params[1], "addr")?,
    ))
}

fn id_version_params(params: &Value) -> Result<(String, u64), RpcError> {
    let params = array_params(params)?;
    if params.len() != 2 {
        return Err(invalid_params("expected id, version"));
    }
    Ok((string_param(&params[0], "id")?, version_param(&params[1])?))
}

fn string_param(value: &Value, name: &'static str) -> Result<String, RpcError> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_params(format!("expected string {name}")))
}

fn version_param(value: &Value) -> Result<u64, RpcError> {
    parse_quantity(value).map_err(|err| invalid_params(err.to_string()))
}

fn invalid_request(message: impl Into<String>) -> RpcError {
    RpcError {
        code: -32600,
        message: message.into(),
    }
}

fn method_not_found() -> RpcError {
    RpcError {
        code: -32601,
        message: "method not found".to_string(),
    }
}

fn parse_error(message: impl Into<String>) -> RpcError {
    RpcError {
        code: -32700,
        message: message.into(),
    }
}

fn invalid_params(message: impl Into<String>) -> RpcError {
    RpcError {
        code: -32602,
        message: message.into(),
    }
}

fn internal(error: impl std::fmt::Display) -> RpcError {
    RpcError {
        code: -32000,
        message: error.to_string(),
    }
}

fn proxy_error(error: RpcClientError) -> RpcError {
    match error {
        RpcClientError::JsonRpc { code, message, .. } => RpcError { code, message },
        other => internal(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        consensus::LocalConsensus,
        sequencer::{SequencerControl, SequencerError},
        store::FilePayloadStore,
        types::{BlockInfo, Hash, L2BlockRef, PeerStats, SyncStatus},
        ConductorConfig,
    };
    use axum::{
        body::{to_bytes, Body},
        http::{Request as HttpRequest, StatusCode},
        routing::post,
    };
    use futures_util::{SinkExt, StreamExt};
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    };
    use tower::ServiceExt;

    #[derive(Debug, Default)]
    struct FakeSequencer {
        active: AtomicBool,
    }

    #[derive(Debug, Default)]
    struct ProxyBackendState {
        calls: AtomicUsize,
        last_request: Mutex<Option<Value>>,
    }

    #[async_trait::async_trait]
    impl SequencerControl for FakeSequencer {
        async fn latest_unsafe_block(&self) -> Result<BlockInfo, SequencerError> {
            Ok(BlockInfo {
                hash: Hash::ZERO,
                number: 0,
            })
        }

        async fn start_sequencer(&self, _expected_hash: Hash) -> Result<(), SequencerError> {
            self.active.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn stop_sequencer(&self) -> Result<Hash, SequencerError> {
            self.active.store(false, Ordering::SeqCst);
            Ok(Hash::ZERO)
        }

        async fn sequencer_active(&self) -> Result<bool, SequencerError> {
            Ok(self.active.load(Ordering::SeqCst))
        }

        async fn sync_status(&self) -> Result<SyncStatus, SequencerError> {
            Ok(SyncStatus {
                unsafe_l2: L2BlockRef {
                    hash: Some(Hash::ZERO),
                    number: 0,
                    time: u64::MAX,
                },
                safe_l2: L2BlockRef {
                    hash: Some(Hash::ZERO),
                    number: 0,
                    time: u64::MAX,
                },
            })
        }

        async fn peer_stats(&self) -> Result<PeerStats, SequencerError> {
            Ok(PeerStats { connected: 1 })
        }

        async fn post_unsafe_payload(
            &self,
            _payload: &PayloadEnvelope,
        ) -> Result<(), SequencerError> {
            Ok(())
        }

        async fn conductor_enabled(&self) -> Result<bool, SequencerError> {
            Ok(true)
        }
    }

    type TestConsensus = LocalConsensus<FilePayloadStore>;
    type TestConductor = SharedConductor<TestConsensus, FakeSequencer>;
    type TestState = SharedState<TestConsensus, FakeSequencer>;

    async fn conductor() -> TestConductor {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeSequencer::default());
        Conductor::new(consensus, sequencer, ConductorConfig::default())
    }

    async fn state() -> TestState {
        Arc::new(RpcState {
            conductor: conductor().await,
            proxy: None,
        })
    }

    fn unsafe_payload(number: u64, byte: u8) -> Value {
        json!({
            "executionPayload": {
                "blockHash": format!("0x{}", hex::encode([byte; 32])),
                "blockNumber": format!("0x{number:x}")
            }
        })
    }

    async fn router() -> Router {
        Router::new()
            .route(
                "/",
                get(ws_handler::<LocalConsensus<FilePayloadStore>, FakeSequencer>)
                    .post(handle::<LocalConsensus<FilePayloadStore>, FakeSequencer>),
            )
            .route(
                "/ws",
                get(ws_handler::<LocalConsensus<FilePayloadStore>, FakeSequencer>),
            )
            .route(
                "/ws/",
                get(ws_handler::<LocalConsensus<FilePayloadStore>, FakeSequencer>),
            )
            .route("/healthz", get(healthz))
            .route("/healthz/", get(healthz))
            .with_state(state().await)
    }

    async fn proxy_backend() -> (Url, Arc<ProxyBackendState>, tokio::task::JoinHandle<()>) {
        async fn handler(
            State(state): State<Arc<ProxyBackendState>>,
            Json(request): Json<Value>,
        ) -> Json<Value> {
            state.calls.fetch_add(1, Ordering::SeqCst);
            *state.last_request.lock().unwrap() = Some(request.clone());
            let id = request.get("id").cloned().unwrap_or(Value::Null);
            let result = match request.get("method").and_then(Value::as_str).unwrap() {
                "eth_getBlockByNumber" => json!({"number": "0x1", "hash": Hash::ZERO}),
                "miner_setMaxDASize" => json!(true),
                "optimism_syncStatus" => json!({"unsafe_l2": {"number": "0x1"}}),
                "admin_sequencerActive" => json!(true),
                _ => Value::Null,
            };
            Json(json!({"jsonrpc": "2.0", "id": id, "result": result}))
        }

        let state = Arc::new(ProxyBackendState::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let app = Router::new()
            .route("/", post(handler))
            .with_state(state.clone());
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, state, handle)
    }

    async fn proxy_state(leader: bool, execution_rpc: Url, node_rpc: Url) -> TestState {
        let conductor = conductor().await;
        conductor
            .commit_unsafe_payload(PayloadEnvelope::new(json!({
                "executionPayload": {
                    "blockHash": Hash::ZERO,
                    "blockNumber": "0x0"
                }
            })))
            .await
            .unwrap();
        conductor.update_leader(leader).await.unwrap();
        Arc::new(RpcState {
            conductor,
            proxy: Some(ProxyClients {
                execution: JsonRpcHttpClient::new(execution_rpc),
                node: JsonRpcHttpClient::new(node_rpc),
            }),
        })
    }

    #[tokio::test]
    async fn rpc_modules_reports_upstream_namespaces_without_proxy() {
        let modules = dispatch(
            state().await,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "rpc_modules".to_string(),
                params: json!([]),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            modules,
            json!({
                "rpc": "1.0",
                "health": "1.0",
                "conductor": "1.0"
            })
        );
    }

    #[tokio::test]
    async fn proxy_methods_are_unregistered_when_proxy_disabled_like_upstream() {
        let state = state().await;

        for (method, params) in [
            ("eth_getBlockByNumber", json!([true])),
            ("miner_setMaxDASize", json!([])),
            ("optimism_syncStatus", json!([true])),
            ("optimism_outputAtBlock", json!([])),
            ("optimism_rollupConfig", json!([true])),
            ("admin_sequencerActive", json!([true])),
        ] {
            let err = dispatch(
                state.clone(),
                Request {
                    jsonrpc: Some("2.0".to_string()),
                    method: method.to_string(),
                    params,
                },
            )
            .await
            .unwrap_err();

            assert_eq!(err.code, -32601, "{method}");
            assert_eq!(err.message, "method not found", "{method}");
        }
    }

    #[tokio::test]
    async fn rpc_modules_reports_proxy_namespaces_when_enabled() {
        let (url, _backend, handle) = proxy_backend().await;
        let modules = dispatch(
            proxy_state(true, url.clone(), url).await,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "rpc_modules".to_string(),
                params: json!([]),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            modules,
            json!({
                "rpc": "1.0",
                "health": "1.0",
                "conductor": "1.0",
                "eth": "1.0",
                "miner": "1.0",
                "optimism": "1.0",
                "admin": "1.0"
            })
        );
        handle.abort();
    }

    #[tokio::test]
    async fn rpc_server_records_upstream_rpc_metrics() {
        let state = state().await;
        let _ = handle_request_value(
            state.clone(),
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "conductor_leader",
                "params": []
            }),
        )
        .await
        .unwrap();
        let _ = handle_request_value(
            state.clone(),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "conductor_missing",
                "params": []
            }),
        )
        .await
        .unwrap();
        assert!(handle_request_value(
            state.clone(),
            json!({
                "jsonrpc": "2.0",
                "method": "conductor_active",
                "params": []
            }),
        )
        .await
        .is_none());

        let rendered = state.conductor.metrics().render_prometheus();

        assert!(rendered.contains(
            "op_conductor_rpc_server_requests_total{rpc=\"main\",method=\"conductor_leader\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_server_responses_total{rpc=\"main\",method=\"conductor_leader\",error=\"<nil>\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_server_responses_total{rpc=\"main\",method=\"conductor_missing\",error=\"rpc_-32601\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_server_request_duration_seconds_count{rpc=\"main\",method=\"conductor_leader\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_client_notifications_received_total{rpc=\"main\",method=\"conductor_active\"} 1"
        ));
    }

    #[tokio::test]
    async fn websocket_rpc_supports_upstream_main_rpc_endpoint() {
        let app = router().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        for path in ["/", "/ws", "/ws/"] {
            let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}{path}"))
                .await
                .unwrap();
            ws.send(tokio_tungstenite::tungstenite::Message::Text(
                r#"{"jsonrpc":"2.0","id":1,"method":"conductor_leader","params":[]}"#.into(),
            ))
            .await
            .unwrap();

            let response = ws.next().await.unwrap().unwrap();
            let tokio_tungstenite::tungstenite::Message::Text(text) = response else {
                panic!("expected websocket text response for {path}, got {response:?}");
            };
            let payload: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(
                payload,
                json!({"jsonrpc":"2.0","id":1,"result":true}),
                "{path}"
            );
        }

        server.abort();
    }

    #[tokio::test]
    async fn override_leader_rejects_missing_bool_like_upstream() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"conductor_overrideLeader","params":[]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn leader_with_id_reports_upstream_override_sentinel() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"[
                            {"jsonrpc":"2.0","id":1,"method":"conductor_overrideLeader","params":[true]},
                            {"jsonrpc":"2.0","id":2,"method":"conductor_leaderWithID","params":[]}
                        ]"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload,
            json!([
                {"jsonrpc":"2.0","id":1,"result":null},
                {"jsonrpc":"2.0","id":2,"result":{"id":"N/A (Leader overridden)","addr":"N/A","suffrage":0}}
            ])
        );
    }

    #[tokio::test]
    async fn json_rpc_batch_returns_ordered_responses() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"[
                            {"jsonrpc":"2.0","id":1,"method":"conductor_leader","params":[]},
                            {"jsonrpc":"2.0","id":2,"method":"conductor_active","params":[]},
                            {"jsonrpc":"2.0","id":3,"method":"conductor_missing","params":[]}
                        ]"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload,
            json!([
                {"jsonrpc":"2.0","id":1,"result":true},
                {"jsonrpc":"2.0","id":2,"result":true},
                {"jsonrpc":"2.0","id":3,"error":{"code":-32601,"message":"method not found"}}
            ])
        );
    }

    #[tokio::test]
    async fn json_rpc_batch_omits_notification_responses() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"[
                            {"jsonrpc":"2.0","method":"conductor_leader","params":[]},
                            {"jsonrpc":"2.0","id":2,"method":"conductor_active","params":[]}
                        ]"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload, json!([{"jsonrpc":"2.0","id":2,"result":true}]));
    }

    #[tokio::test]
    async fn json_rpc_single_notification_returns_no_content() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","method":"conductor_leader","params":[]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn healthz_matches_upstream_default_route() {
        let app = router().await;
        for path in ["/healthz", "/healthz/"] {
            let response = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("GET")
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(
                body.as_ref(),
                format!("{{\"version\":\"{}\"}}\n", env!("CARGO_PKG_VERSION")).as_bytes()
            );
            let payload: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(payload, json!({"version": env!("CARGO_PKG_VERSION")}));
        }
    }

    #[tokio::test]
    async fn health_status_matches_upstream_rpc_namespace() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"health_status","params":[]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload,
            json!({"jsonrpc":"2.0","id":1,"result":env!("CARGO_PKG_VERSION")})
        );
    }

    #[tokio::test]
    async fn json_rpc_null_id_is_not_a_notification() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":null,"method":"conductor_leader","params":[]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload, json!({"jsonrpc":"2.0","id":null,"result":true}));
    }

    #[tokio::test]
    async fn malformed_json_returns_json_rpc_parse_error() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"conductor_leader""#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["jsonrpc"], "2.0");
        assert_eq!(payload["id"], Value::Null);
        assert_eq!(payload["error"]["code"], -32700);
        assert!(payload["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("parse error:"));
    }

    #[tokio::test]
    async fn invalid_request_preserves_request_id() {
        let app = router().await;
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":7,"params":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload,
            json!({
                "jsonrpc": "2.0",
                "id": 7,
                "error": {
                    "code": -32600,
                    "message": "invalid request: missing field `method`"
                }
            })
        );
    }

    #[tokio::test]
    async fn membership_rpc_uses_upstream_param_and_response_shape() {
        let state = state().await;
        dispatch(
            state.clone(),
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_addServerAsNonvoter".to_string(),
                params: json!(["seq-b", "127.0.0.1:8548", 1]),
            },
        )
        .await
        .unwrap();
        dispatch(
            state.clone(),
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_addServerAsVoter".to_string(),
                params: json!(["seq-c", "127.0.0.1:8549", "0x2"]),
            },
        )
        .await
        .unwrap();
        dispatch(
            state.clone(),
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_demoteVoter".to_string(),
                params: json!(["seq-c", 3]),
            },
        )
        .await
        .unwrap();

        let membership = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_clusterMembership".to_string(),
                params: Value::Null,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            membership,
            json!({
                "servers": [
                    {"id": "seq-a", "addr": "127.0.0.1:0", "suffrage": 0},
                    {"id": "seq-b", "addr": "127.0.0.1:8548", "suffrage": 1},
                    {"id": "seq-c", "addr": "127.0.0.1:8549", "suffrage": 1}
                ],
                "version": 4
            })
        );
    }

    #[tokio::test]
    async fn commit_unsafe_payload_requires_single_positional_payload_like_upstream() {
        let state = state().await;
        let baseline = unsafe_payload(10, 0x10);
        dispatch(
            state.clone(),
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_commitUnsafePayload".to_string(),
                params: json!([baseline.clone()]),
            },
        )
        .await
        .unwrap();

        let replacement = unsafe_payload(11, 0x11);
        let extra_param_error = dispatch(
            state.clone(),
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_commitUnsafePayload".to_string(),
                params: json!([replacement.clone(), {"ignored": true}]),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(extra_param_error.code, -32602);
        assert_eq!(extra_param_error.message, "expected single payload");

        let object_param_error = dispatch(
            state.clone(),
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_commitUnsafePayload".to_string(),
                params: replacement,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(object_param_error.code, -32602);
        assert_eq!(object_param_error.message, "expected params array");

        assert_eq!(
            state.conductor.latest_unsafe_payload().await.unwrap(),
            Some(PayloadEnvelope::new(baseline))
        );
    }

    #[tokio::test]
    async fn commit_unsafe_payload_rejects_invalid_hash_before_consensus_write_like_upstream() {
        let state = state().await;
        let invalid = json!({
            "executionPayload": {
                "blockHash": "0x1234",
                "blockNumber": "0x1"
            }
        });

        let err = dispatch(
            state.clone(),
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_commitUnsafePayload".to_string(),
                params: json!([invalid]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32602);
        assert!(err.message.contains("expected 0x-prefixed 32-byte hash"));
        assert_eq!(state.conductor.latest_unsafe_payload().await.unwrap(), None);
    }

    #[tokio::test]
    async fn no_arg_conductor_rpc_rejects_extra_params_before_side_effects_like_upstream() {
        let state = state().await;

        let err = dispatch(
            state.clone(),
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "conductor_stop".to_string(),
                params: json!([true]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32602);
        assert_eq!(err.message, "expected no params");
        assert!(!state.conductor.stopped());
    }

    #[tokio::test]
    async fn no_arg_proxy_rpc_rejects_extra_params_before_backend_like_upstream() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(true, url.clone(), url).await;

        for method in [
            "optimism_syncStatus",
            "optimism_rollupConfig",
            "admin_sequencerActive",
        ] {
            let err = dispatch(
                state.clone(),
                Request {
                    jsonrpc: Some("2.0".to_string()),
                    method: method.to_string(),
                    params: json!([true]),
                },
            )
            .await
            .unwrap_err();
            assert_eq!(err.code, -32602, "{method}");
            assert_eq!(err.message, "expected no params", "{method}");
        }

        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_eth_calls_backend_then_refuses_result_on_follower() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(false, url.clone(), url).await;

        let err = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "eth_getBlockByNumber".to_string(),
                params: json!(["latest", false]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32000);
        assert_eq!(
            err.message,
            "refusing to proxy request to non-leader sequencer"
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let request = backend.last_request.lock().unwrap().clone().unwrap();
        assert_eq!(request["params"], json!(["latest", false]));
        handle.abort();
    }

    #[tokio::test]
    async fn leader_override_allows_and_clearing_refuses_leader_gated_proxies_like_upstream() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(false, url.clone(), url).await;
        state.conductor.set_leader_override(true);

        for (method, params) in [
            ("admin_sequencerActive", json!([])),
            ("optimism_syncStatus", json!([])),
            ("optimism_outputAtBlock", json!(["0x1"])),
            ("optimism_rollupConfig", json!([])),
            ("eth_getBlockByNumber", json!(["latest", false])),
        ] {
            dispatch(
                state.clone(),
                Request {
                    jsonrpc: Some("2.0".to_string()),
                    method: method.to_string(),
                    params,
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 5);

        state.conductor.set_leader_override(false);
        let err = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "admin_sequencerActive".to_string(),
                params: json!([]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32000);
        assert_eq!(
            err.message,
            "refusing to proxy request to non-leader sequencer"
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 6);
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_eth_get_block_by_number_normalizes_hex_quantity() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(true, url.clone(), url).await;

        let result = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "eth_getBlockByNumber".to_string(),
                params: json!(["0X2", true]),
            },
        )
        .await
        .unwrap();

        assert_eq!(result, json!({"number": "0x1", "hash": Hash::ZERO}));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let request = backend.last_request.lock().unwrap().clone().unwrap();
        assert_eq!(request["method"], "eth_getBlockByNumber");
        assert_eq!(request["params"], json!(["0x2", true]));
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_eth_get_block_by_number_rejects_bad_params_before_backend() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(true, url.clone(), url).await;

        let err = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "eth_getBlockByNumber".to_string(),
                params: json!(["0x02", false]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32602);
        assert_eq!(err.message, "hex number with leading zero digits");
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_eth_get_block_by_number_requires_full_tx_bool() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(true, url.clone(), url).await;

        let err = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "eth_getBlockByNumber".to_string(),
                params: json!(["latest", "false"]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32602);
        assert_eq!(err.message, "expected full transaction flag");
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_miner_set_max_da_size_is_not_leader_gated() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(false, url.clone(), url).await;

        let result = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "miner_setMaxDASize".to_string(),
                params: json!(["0X1", "0x2"]),
            },
        )
        .await
        .unwrap();

        assert_eq!(result, json!(true));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let request = backend.last_request.lock().unwrap().clone().unwrap();
        assert_eq!(request["method"], "miner_setMaxDASize");
        assert_eq!(request["params"], json!(["0x1", "0x2"]));
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_miner_set_max_da_size_rejects_non_quantity_before_backend() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(false, url.clone(), url).await;

        let err = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "miner_setMaxDASize".to_string(),
                params: json!(["1", "0x2"]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32602);
        assert_eq!(err.message, "expected 0x-prefixed max tx size");
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_output_at_block_normalizes_hex_quantity() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(true, url.clone(), url).await;

        let result = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "optimism_outputAtBlock".to_string(),
                params: json!(["0x2"]),
            },
        )
        .await
        .unwrap();

        assert_eq!(result, Value::Null);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let request = backend.last_request.lock().unwrap().clone().unwrap();
        assert_eq!(request["method"], "optimism_outputAtBlock");
        assert_eq!(request["params"], json!(["0x2"]));
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_output_at_block_rejects_decimal_string_before_backend() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(true, url.clone(), url).await;

        let err = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "optimism_outputAtBlock".to_string(),
                params: json!(["2"]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32602);
        assert_eq!(err.message, "expected 0x-prefixed block number");
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_output_at_block_rejects_leading_zero_quantity_before_backend() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(true, url.clone(), url).await;

        let err = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "optimism_outputAtBlock".to_string(),
                params: json!(["0x02"]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32602);
        assert_eq!(err.message, "hex number with leading zero digits");
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn proxy_output_at_block_rejects_numeric_param_before_backend() {
        let (url, backend, handle) = proxy_backend().await;
        let state = proxy_state(true, url.clone(), url).await;

        let err = dispatch(
            state,
            Request {
                jsonrpc: Some("2.0".to_string()),
                method: "optimism_outputAtBlock".to_string(),
                params: json!([2]),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, -32602);
        assert_eq!(err.message, "expected hex block number string");
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        handle.abort();
    }
}
