use axum::{
    extract::{
        ws::{Message, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use conductor_rs::{Hash, PayloadEnvelope};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    net::TcpListener as StdTcpListener,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::Notify};
use url::Url;

mod common;
use common::{kona_payload, kona_payload_hash, validate_kona_payload_shape};

static BINARY_PROCESS_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn lock_binary_process_test() -> tokio::sync::MutexGuard<'static, ()> {
    BINARY_PROCESS_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

#[test]
fn op_conductor_binary_target_is_available_for_drop_in_manifests() {
    let output = Command::new(env!("CARGO_BIN_EXE_op-conductor"))
        .arg("--help")
        .output()
        .expect("op-conductor binary alias should run");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Rust OP Stack sequencer conductor"));
    assert!(stdout.contains("--raft.server.id"));
}

#[derive(Debug)]
struct FakeKonaState {
    latest: Mutex<(Hash, u64)>,
    active: Mutex<bool>,
    hash_param_starts: Mutex<Vec<Hash>>,
    parameterless_starts: Mutex<u64>,
    posted_payloads: Mutex<Vec<PayloadEnvelope>>,
}

impl FakeKonaState {
    fn new(hash: Hash, number: u64) -> Self {
        Self {
            latest: Mutex::new((hash, number)),
            active: Mutex::new(false),
            hash_param_starts: Mutex::new(Vec::new()),
            parameterless_starts: Mutex::new(0),
            posted_payloads: Mutex::new(Vec::new()),
        }
    }

    fn hash_param_starts(&self) -> Vec<Hash> {
        self.hash_param_starts.lock().unwrap().clone()
    }

    fn parameterless_starts(&self) -> u64 {
        *self.parameterless_starts.lock().unwrap()
    }

    fn active(&self) -> bool {
        *self.active.lock().unwrap()
    }

    fn set_active(&self, active: bool) {
        *self.active.lock().unwrap() = active;
    }
}

async fn fake_kona_handler(
    State(state): State<Arc<FakeKonaState>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let method = request.get("method").and_then(Value::as_str).unwrap();
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let params = request
        .get("params")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let response = match method {
        "admin_conductorEnabled" => json!({"jsonrpc": "2.0", "id": id, "result": true}),
        "admin_sequencerActive" => {
            json!({"jsonrpc": "2.0", "id": id, "result": state.active()})
        }
        "admin_startSequencer" if params.len() == 1 => {
            let expected = params[0].as_str().unwrap().parse::<Hash>().unwrap();
            let (actual, _) = *state.latest.lock().unwrap();
            if expected != actual {
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": -32000,
                        "message": "unsafe head mismatch"
                    }
                })
            } else {
                state.hash_param_starts.lock().unwrap().push(expected);
                *state.active.lock().unwrap() = true;
                json!({"jsonrpc": "2.0", "id": id, "result": null})
            }
        }
        "admin_startSequencer" => {
            *state.parameterless_starts.lock().unwrap() += 1;
            *state.active.lock().unwrap() = true;
            json!({"jsonrpc": "2.0", "id": id, "result": null})
        }
        "admin_stopSequencer" => {
            *state.active.lock().unwrap() = false;
            let (hash, _) = *state.latest.lock().unwrap();
            json!({"jsonrpc": "2.0", "id": id, "result": hash})
        }
        "admin_postUnsafePayload" => {
            let Some(raw_payload) = params.first() else {
                return Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32602, "message": "missing unsafe payload"}
                }));
            };
            if let Err(message) = validate_kona_payload_shape(raw_payload) {
                return Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32602, "message": message}
                }));
            }
            let payload = PayloadEnvelope::new(raw_payload.clone());
            *state.latest.lock().unwrap() = (
                payload.block_hash().unwrap(),
                payload.block_number().unwrap(),
            );
            state.posted_payloads.lock().unwrap().push(payload);
            json!({"jsonrpc": "2.0", "id": id, "result": null})
        }
        "optimism_syncStatus" => {
            let (hash, number) = *state.latest.lock().unwrap();
            let now = unix_now();
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "unsafe_l2": {"hash": hash, "number": number, "timestamp": now},
                    "safe_l2": {"hash": hash, "number": number.saturating_sub(1), "timestamp": now}
                }
            })
        }
        "optimism_outputAtBlock" => {
            if params != [json!("0xa")] {
                return Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32602, "message": "expected hex output block lookup"}
                }));
            }
            let (hash, number) = *state.latest.lock().unwrap();
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "version": "0x0",
                    "outputRoot": hash,
                    "blockRef": {"hash": hash, "number": number}
                }
            })
        }
        "optimism_rollupConfig" => {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"l2_chain_id": 10, "block_time": 2}
            })
        }
        "opp2p_peerStats" => {
            json!({"jsonrpc": "2.0", "id": id, "result": {"connected": "0x1"}})
        }
        "eth_getBlockByNumber" => {
            if params != [json!("latest"), json!(false)] {
                return Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32602, "message": "expected latest unsafe block lookup"}
                }));
            }
            let (hash, number) = *state.latest.lock().unwrap();
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"hash": hash, "number": format!("0x{number:x}")}
            })
        }
        "miner_setMaxDASize" => {
            if params != [json!("0x10"), json!("0x20")] {
                return Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32602, "message": "expected max DA size params"}
                }));
            }
            json!({"jsonrpc": "2.0", "id": id, "result": true})
        }
        _ => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": "method not found"}
        }),
    };
    Json(response)
}

async fn start_fake_kona(state: Arc<FakeKonaState>) -> Url {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let app = Router::new()
        .route("/", post(fake_kona_handler))
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    url
}

fn payload(number: u64, block_hash: Hash) -> Value {
    kona_payload(number, block_hash)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn free_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_conductor(
    kona_url: &Url,
    storage_dir: &TempDir,
    rpc_port: u16,
    pprof_port: u16,
) -> ChildGuard {
    spawn_conductor_with_extra_args(kona_url, storage_dir, rpc_port, pprof_port, &[])
}

fn spawn_conductor_with_extra_args(
    kona_url: &Url,
    storage_dir: &TempDir,
    rpc_port: u16,
    pprof_port: u16,
    extra_args: &[String],
) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_op-conductor"));
    command.args([
        "--node.rpc",
        kona_url.as_str(),
        "--execution.rpc",
        kona_url.as_str(),
        "--network",
        "op-mainnet",
        "--raft.server.id",
        "seq-a",
        "--raft.storage.dir",
        storage_dir.path().to_str().unwrap(),
        "--raft.bootstrap",
        "--consensus.addr",
        "127.0.0.1",
        "--consensus.port",
        "0",
        "--rpc.addr",
        "127.0.0.1",
        "--rpc.port",
        &rpc_port.to_string(),
        "--pprof.enabled",
        "--pprof.addr",
        "127.0.0.1",
        "--pprof.port",
        &pprof_port.to_string(),
        "--healthcheck.interval",
        "100ms",
        "--healthcheck.unsafe-interval",
        "60s",
        "--healthcheck.min-peer-count",
        "1",
        "--log.level",
        "error",
    ]);
    command.args(extra_args);

    let child = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    ChildGuard { child }
}

struct FakeRollupBoost {
    url: Url,
    connected: Arc<Notify>,
    send: Arc<Notify>,
    _task: tokio::task::JoinHandle<()>,
}

impl FakeRollupBoost {
    async fn wait_connected(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.connected.notified())
            .await
            .unwrap();
    }

    fn release_messages(&self) {
        self.send.notify_one();
    }
}

struct FakeRollupBoostState {
    connected: Arc<Notify>,
    send: Arc<Notify>,
    message: &'static str,
}

async fn start_fake_rollup_boost(message: &'static str) -> FakeRollupBoost {
    let connected = Arc::new(Notify::new());
    let send = Arc::new(Notify::new());
    let state = Arc::new(FakeRollupBoostState {
        connected: connected.clone(),
        send: send.clone(),
        message,
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("ws://{}/ws", listener.local_addr().unwrap())).unwrap();
    let app = Router::new()
        .route("/ws", get(fake_rollup_boost_ws))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    FakeRollupBoost {
        url,
        connected,
        send,
        _task: task,
    }
}

async fn fake_rollup_boost_ws(
    ws: WebSocketUpgrade,
    State(state): State<Arc<FakeRollupBoostState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |mut socket| async move {
        state.connected.notify_one();
        state.send.notified().await;
        for _ in 0..20 {
            if socket
                .send(Message::Text(state.message.to_string()))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        while socket.next().await.is_some() {}
    })
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn conductor_call(
    rpc_url: &Url,
    method: &str,
    params: Value,
) -> Result<Value, reqwest::Error> {
    reqwest::Client::new()
        .post(rpc_url.clone())
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await
}

async fn wait_for_pprof(child: &mut ChildGuard, pprof_port: u16) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    let url = format!("http://127.0.0.1:{pprof_port}/debug/pprof/");
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("conductor-rs exited before pprof became reachable: {status}");
        }
        if let Ok(response) = reqwest::get(&url).await {
            if response.status().is_success() {
                return response.text().await.unwrap();
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for conductor pprof"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_metrics(child: &mut ChildGuard, metrics_port: u16) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    let url = format!("http://127.0.0.1:{metrics_port}/metrics");
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("conductor-rs exited before metrics became reachable: {status}");
        }
        if let Ok(response) = reqwest::get(&url).await {
            if response.status().is_success() {
                return response.text().await.unwrap();
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for conductor metrics"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_binary_proxy_surface(rpc_url: &Url, expected_hash: Hash) {
    let latest = conductor_call(rpc_url, "eth_getBlockByNumber", json!(["latest", false]))
        .await
        .unwrap();
    assert_eq!(latest["result"]["hash"], json!(expected_hash));
    assert_eq!(latest["result"]["number"], json!("0xa"));

    let sync_status = conductor_call(rpc_url, "optimism_syncStatus", json!([]))
        .await
        .unwrap();
    assert_eq!(
        sync_status["result"]["unsafe_l2"]["hash"],
        json!(expected_hash)
    );

    let output = conductor_call(rpc_url, "optimism_outputAtBlock", json!(["0xa"]))
        .await
        .unwrap();
    assert_eq!(output["result"]["outputRoot"], json!(expected_hash));

    let rollup_config = conductor_call(rpc_url, "optimism_rollupConfig", json!([]))
        .await
        .unwrap();
    assert_eq!(rollup_config["result"]["l2_chain_id"], json!(10));

    let active = conductor_call(rpc_url, "admin_sequencerActive", json!([]))
        .await
        .unwrap();
    assert_eq!(active.get("result").and_then(Value::as_bool), Some(false));

    let max_da = conductor_call(rpc_url, "miner_setMaxDASize", json!(["0x10", "0x20"]))
        .await
        .unwrap();
    assert_eq!(max_da.get("result").and_then(Value::as_bool), Some(true));
}

async fn wait_for_leader(child: &mut ChildGuard, rpc_url: &Url) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("conductor-rs exited before becoming leader: {status}");
        }
        if let Ok(response) = conductor_call(rpc_url, "conductor_leader", json!([])).await {
            if response.get("result").and_then(Value::as_bool) == Some(true) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for conductor leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_hash_start(child: &mut ChildGuard, state: &FakeKonaState, expected_hash: Hash) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("conductor-rs exited before starting sequencer: {status}");
        }
        if state.hash_param_starts() == vec![expected_hash] {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for hash-gated sequencer start"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_flashblocks_text(
    child: &mut ChildGuard,
    websocket_port: u16,
    rollup_boost: &FakeRollupBoost,
    expected: &str,
) {
    rollup_boost.wait_connected().await;
    let downstream_url = format!("ws://127.0.0.1:{websocket_port}/ws");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("conductor-rs exited before serving flashblocks websocket: {status}");
        }
        if let Ok((mut downstream, _)) = tokio_tungstenite::connect_async(&downstream_url).await {
            rollup_boost.release_messages();
            loop {
                match tokio::time::timeout(Duration::from_secs(2), downstream.next()).await {
                    Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))))
                        if text.as_str() == expected =>
                    {
                        return;
                    }
                    Ok(Some(Ok(_))) => continue,
                    other => panic!("expected flashblocks websocket text frame, got {other:?}"),
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for flashblocks websocket server"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_exit(child: &mut ChildGuard) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait().unwrap().is_some() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for conductor-rs exit"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn conductor_binary_starts_hash_gated_kona_endpoint_after_commit() {
    let _guard = lock_binary_process_test().await;
    let expected_hash = kona_payload_hash();
    let fake_kona = Arc::new(FakeKonaState::new(expected_hash, 10));
    let kona_url = start_fake_kona(fake_kona.clone()).await;
    let storage_dir = tempfile::tempdir().unwrap();
    let rpc_port = free_port();
    let pprof_port = free_port();
    let metrics_port = free_port();
    let rpc_url = Url::parse(&format!("http://127.0.0.1:{rpc_port}")).unwrap();
    let extra_args = vec![
        "--metrics.enabled".to_string(),
        "--metrics.addr".to_string(),
        "127.0.0.1".to_string(),
        "--metrics.port".to_string(),
        metrics_port.to_string(),
    ];
    let mut child =
        spawn_conductor_with_extra_args(&kona_url, &storage_dir, rpc_port, pprof_port, &extra_args);

    wait_for_leader(&mut child, &rpc_url).await;
    let pprof_index = wait_for_pprof(&mut child, pprof_port).await;
    assert!(pprof_index.contains("/debug/pprof/profile"));
    assert!(pprof_index.contains("/debug/pprof/heap"));
    let metrics = wait_for_metrics(&mut child, metrics_port).await;
    assert!(metrics.contains("op_conductor_up 1"));
    assert!(metrics.contains("op_conductor_rpc_server_requests_total"));
    assert_binary_proxy_surface(&rpc_url, expected_hash).await;

    let paused = conductor_call(&rpc_url, "conductor_paused", json!([]))
        .await
        .unwrap();
    assert_eq!(paused.get("result").and_then(Value::as_bool), Some(true));
    let membership = conductor_call(&rpc_url, "conductor_clusterMembership", json!([]))
        .await
        .unwrap();
    let advertised = membership["result"]["servers"][0]["addr"].as_str().unwrap();
    assert_ne!(advertised, "127.0.0.1:0");
    let advertised_port = advertised
        .rsplit_once(':')
        .unwrap()
        .1
        .parse::<u16>()
        .unwrap();
    assert_ne!(advertised_port, 0);

    let commit = conductor_call(
        &rpc_url,
        "conductor_commitUnsafePayload",
        json!([payload(10, expected_hash)]),
    )
    .await
    .unwrap();
    assert!(commit.get("error").is_none(), "{commit}");

    let resume = conductor_call(&rpc_url, "conductor_resume", json!([]))
        .await
        .unwrap();
    assert!(resume.get("error").is_none(), "{resume}");

    wait_for_hash_start(&mut child, &fake_kona, expected_hash).await;

    assert!(fake_kona.active());
    assert_eq!(fake_kona.parameterless_starts(), 0);

    let stop = conductor_call(&rpc_url, "conductor_stop", json!([]))
        .await
        .unwrap();
    assert!(stop.get("error").is_none(), "{stop}");
    wait_for_exit(&mut child).await;
}

#[tokio::test]
async fn conductor_binary_recovers_committed_unsafe_head_after_restart() {
    let _guard = lock_binary_process_test().await;
    let expected_hash = kona_payload_hash();
    let fake_kona = Arc::new(FakeKonaState::new(expected_hash, 10));
    let kona_url = start_fake_kona(fake_kona.clone()).await;
    let storage_dir = tempfile::tempdir().unwrap();

    let first_rpc_port = free_port();
    let first_pprof_port = free_port();
    let first_rpc_url = Url::parse(&format!("http://127.0.0.1:{first_rpc_port}")).unwrap();
    let mut first_child =
        spawn_conductor(&kona_url, &storage_dir, first_rpc_port, first_pprof_port);

    wait_for_leader(&mut first_child, &first_rpc_url).await;
    let paused = conductor_call(&first_rpc_url, "conductor_paused", json!([]))
        .await
        .unwrap();
    assert_eq!(paused.get("result").and_then(Value::as_bool), Some(true));

    let commit = conductor_call(
        &first_rpc_url,
        "conductor_commitUnsafePayload",
        json!([payload(10, expected_hash)]),
    )
    .await
    .unwrap();
    assert!(commit.get("error").is_none(), "{commit}");

    let stop = conductor_call(&first_rpc_url, "conductor_stop", json!([]))
        .await
        .unwrap();
    assert!(stop.get("error").is_none(), "{stop}");
    wait_for_exit(&mut first_child).await;

    fake_kona.set_active(false);

    let restarted_rpc_port = free_port();
    let restarted_pprof_port = free_port();
    let restarted_rpc_url = Url::parse(&format!("http://127.0.0.1:{restarted_rpc_port}")).unwrap();
    let mut restarted_child = spawn_conductor(
        &kona_url,
        &storage_dir,
        restarted_rpc_port,
        restarted_pprof_port,
    );

    wait_for_leader(&mut restarted_child, &restarted_rpc_url).await;
    let paused = conductor_call(&restarted_rpc_url, "conductor_paused", json!([]))
        .await
        .unwrap();
    assert_eq!(
        paused.get("result").and_then(Value::as_bool),
        Some(false),
        "reusing an initialized raft store with --raft.bootstrap should not pause again"
    );

    wait_for_hash_start(&mut restarted_child, &fake_kona, expected_hash).await;
    assert!(fake_kona.active());
    assert_eq!(fake_kona.parameterless_starts(), 0);

    let stop = conductor_call(&restarted_rpc_url, "conductor_stop", json!([]))
        .await
        .unwrap();
    assert!(stop.get("error").is_none(), "{stop}");
    wait_for_exit(&mut restarted_child).await;
}

#[tokio::test]
async fn conductor_binary_serves_flashblocks_websocket_when_leader() {
    let _guard = lock_binary_process_test().await;
    let expected_hash = kona_payload_hash();
    let fake_kona = Arc::new(FakeKonaState::new(expected_hash, 10));
    let kona_url = start_fake_kona(fake_kona.clone()).await;
    let rollup_boost = start_fake_rollup_boost("binary-flashblock").await;
    let storage_dir = tempfile::tempdir().unwrap();
    let rpc_port = free_port();
    let pprof_port = free_port();
    let websocket_port = free_port();
    let rpc_url = Url::parse(&format!("http://127.0.0.1:{rpc_port}")).unwrap();
    let extra_args = vec![
        "--rollupboost.ws-url".to_string(),
        rollup_boost.url.to_string(),
        "--websocket.server-port".to_string(),
        websocket_port.to_string(),
    ];
    let mut child =
        spawn_conductor_with_extra_args(&kona_url, &storage_dir, rpc_port, pprof_port, &extra_args);

    wait_for_leader(&mut child, &rpc_url).await;

    let commit = conductor_call(
        &rpc_url,
        "conductor_commitUnsafePayload",
        json!([payload(10, expected_hash)]),
    )
    .await
    .unwrap();
    assert!(commit.get("error").is_none(), "{commit}");

    let resume = conductor_call(&rpc_url, "conductor_resume", json!([]))
        .await
        .unwrap();
    assert!(resume.get("error").is_none(), "{resume}");

    wait_for_hash_start(&mut child, &fake_kona, expected_hash).await;
    wait_for_flashblocks_text(
        &mut child,
        websocket_port,
        &rollup_boost,
        "binary-flashblock",
    )
    .await;

    let stop = conductor_call(&rpc_url, "conductor_stop", json!([]))
        .await
        .unwrap();
    assert!(stop.get("error").is_none(), "{stop}");
    wait_for_exit(&mut child).await;
}
