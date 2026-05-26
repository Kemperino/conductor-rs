use axum::{extract::State, routing::post, Json, Router};
use conductor_rs::{Hash, PayloadEnvelope};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    net::TcpListener as StdTcpListener,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use url::Url;

mod common;
use common::{hash, kona_payload, kona_payload_hash, validate_kona_payload_shape};

#[derive(Debug)]
struct FakeKonaState {
    latest: Mutex<(Hash, u64)>,
    active: Mutex<bool>,
    peer_count: Mutex<u64>,
    hash_param_starts: Mutex<Vec<Hash>>,
    parameterless_starts: Mutex<u64>,
    stops: Mutex<u64>,
    posted_payloads: Mutex<Vec<PayloadEnvelope>>,
}

impl FakeKonaState {
    fn new(hash: Hash, number: u64) -> Self {
        Self {
            latest: Mutex::new((hash, number)),
            active: Mutex::new(false),
            peer_count: Mutex::new(1),
            hash_param_starts: Mutex::new(Vec::new()),
            parameterless_starts: Mutex::new(0),
            stops: Mutex::new(0),
            posted_payloads: Mutex::new(Vec::new()),
        }
    }

    fn set_latest(&self, hash: Hash, number: u64) {
        *self.latest.lock().unwrap() = (hash, number);
    }

    fn set_peer_count(&self, peer_count: u64) {
        *self.peer_count.lock().unwrap() = peer_count;
    }

    fn active(&self) -> bool {
        *self.active.lock().unwrap()
    }

    fn hash_param_starts(&self) -> Vec<Hash> {
        self.hash_param_starts.lock().unwrap().clone()
    }

    fn parameterless_starts(&self) -> u64 {
        *self.parameterless_starts.lock().unwrap()
    }

    fn stops(&self) -> u64 {
        *self.stops.lock().unwrap()
    }

    fn posted_payloads(&self) -> Vec<PayloadEnvelope> {
        self.posted_payloads.lock().unwrap().clone()
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
                    "error": {"code": -32000, "message": "unsafe head mismatch"}
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
            *state.stops.lock().unwrap() += 1;
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
            state.posted_payloads.lock().unwrap().push(payload.clone());
            state.set_latest(
                payload.block_hash().unwrap(),
                payload.block_number().unwrap(),
            );
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
        "opp2p_peerStats" => {
            let connected = *state.peer_count.lock().unwrap();
            json!({"jsonrpc": "2.0", "id": id, "result": {"connected": connected}})
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

#[derive(Debug)]
struct NodeProcess {
    id: String,
    rpc_url: Url,
    consensus_addr: String,
    kona: Arc<FakeKonaState>,
    child: ChildGuard,
}

impl NodeProcess {
    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }
}

#[derive(Debug)]
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

async fn spawn_node(
    id: &str,
    bootstrap: bool,
    storage_dir: &TempDir,
    kona: Arc<FakeKonaState>,
) -> NodeProcess {
    let kona_url = start_fake_kona(kona.clone()).await;
    let rpc_port = free_port();
    let consensus_port = free_port();
    let consensus_addr = format!("127.0.0.1:{consensus_port}");
    let mut args = vec![
        "--node.rpc".to_string(),
        kona_url.to_string(),
        "--execution.rpc".to_string(),
        kona_url.to_string(),
        "--network".to_string(),
        "op-mainnet".to_string(),
        "--raft.server.id".to_string(),
        id.to_string(),
        "--raft.storage.dir".to_string(),
        storage_dir.path().to_str().unwrap().to_string(),
        "--consensus.addr".to_string(),
        "127.0.0.1".to_string(),
        "--consensus.port".to_string(),
        consensus_port.to_string(),
        "--consensus.advertised".to_string(),
        consensus_addr.clone(),
        "--rpc.addr".to_string(),
        "127.0.0.1".to_string(),
        "--rpc.port".to_string(),
        rpc_port.to_string(),
        "--healthcheck.interval".to_string(),
        "100ms".to_string(),
        "--healthcheck.unsafe-interval".to_string(),
        "60s".to_string(),
        "--healthcheck.min-peer-count".to_string(),
        "1".to_string(),
        "--raft.heartbeat-timeout".to_string(),
        "150ms".to_string(),
        "--raft.lease-timeout".to_string(),
        "100ms".to_string(),
        "--raft.round-robin-leader-transfer".to_string(),
        "--log.level".to_string(),
        "error".to_string(),
    ];
    if bootstrap {
        args.push("--raft.bootstrap".to_string());
    }
    let child = Command::new(env!("CARGO_BIN_EXE_op-conductor"))
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    NodeProcess {
        id: id.to_string(),
        rpc_url: Url::parse(&format!("http://127.0.0.1:{rpc_port}")).unwrap(),
        consensus_addr,
        kona,
        child: ChildGuard { child },
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

async fn assert_public_ha_contract(nodes: &mut [NodeProcess], expected_members: usize) -> String {
    let client = reqwest::Client::new();
    let mut leader_ids = Vec::new();

    for node in nodes.iter_mut() {
        assert_child_running(node);

        let healthz: Value = client
            .get(node.rpc_url.join("/healthz").unwrap())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            healthz.get("version").and_then(Value::as_str).is_some(),
            "{} /healthz should expose an upstream-style version payload",
            node.id
        );

        let health_status = conductor_call(&node.rpc_url, "health_status", json!([]))
            .await
            .unwrap();
        assert!(
            health_status
                .get("result")
                .and_then(Value::as_str)
                .is_some(),
            "{} health_status should return the app version string: {health_status}",
            node.id
        );

        let modules = conductor_call(&node.rpc_url, "rpc_modules", json!([]))
            .await
            .unwrap();
        for namespace in [
            "rpc",
            "health",
            "conductor",
            "eth",
            "miner",
            "optimism",
            "admin",
        ] {
            assert_eq!(
                modules
                    .get("result")
                    .and_then(|result| result.get(namespace))
                    .and_then(Value::as_str),
                Some("1.0"),
                "{} rpc_modules should expose {namespace}=1.0: {modules}",
                node.id
            );
        }

        let overridden = conductor_call(&node.rpc_url, "conductor_leaderOverridden", json!([]))
            .await
            .unwrap();
        assert_eq!(
            overridden.get("result").and_then(Value::as_bool),
            Some(false),
            "{} should not require a manual leader override during HA validation",
            node.id
        );

        let active = conductor_call(&node.rpc_url, "conductor_active", json!([]))
            .await
            .unwrap();
        assert_eq!(
            active.get("result").and_then(Value::as_bool),
            Some(true),
            "{} should be active once the bootstrapped cluster has resumed",
            node.id
        );

        let leader = conductor_call(&node.rpc_url, "conductor_leader", json!([]))
            .await
            .unwrap();
        if leader.get("result").and_then(Value::as_bool) == Some(true) {
            leader_ids.push(node.id.clone());
        }
    }

    assert_eq!(
        leader_ids.len(),
        1,
        "exactly one conductor should report leader"
    );
    let leader_id = leader_ids.remove(0);
    let leader_rpc = nodes
        .iter()
        .find(|node| node.id == leader_id)
        .map(|node| node.rpc_url.clone())
        .unwrap();
    let membership = conductor_call(&leader_rpc, "conductor_clusterMembership", json!([]))
        .await
        .unwrap();
    assert!(
        membership
            .get("result")
            .and_then(|result| result.get("servers"))
            .and_then(Value::as_array)
            .is_some_and(|servers| servers.len() >= expected_members),
        "leader membership should include every supplied conductor endpoint: {membership}"
    );

    leader_id
}

async fn wait_rpc_ready(nodes: &mut [NodeProcess]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert_children_running(nodes);
        let mut ready = 0;
        for node in nodes.iter() {
            if let Ok(response) = conductor_call(&node.rpc_url, "conductor_active", json!([])).await
            {
                if response.get("error").is_none() {
                    ready += 1;
                }
            }
        }
        if ready == nodes.len() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for conductor RPCs"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_leader_id(nodes: &mut [NodeProcess], excluded: Option<&str>) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert_children_running(nodes);
        for node in nodes.iter() {
            if excluded == Some(node.id.as_str()) {
                continue;
            }
            if let Ok(response) = conductor_call(&node.rpc_url, "conductor_leader", json!([])).await
            {
                if response.get("result").and_then(Value::as_bool) == Some(true) {
                    return node.id.clone();
                }
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_membership(node: &mut NodeProcess, expected_len: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert_child_running(node);
        if let Ok(response) =
            conductor_call(&node.rpc_url, "conductor_clusterMembership", json!([])).await
        {
            let len = response
                .get("result")
                .and_then(|result| result.get("servers"))
                .and_then(Value::as_array)
                .map(Vec::len);
            if len == Some(expected_len) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for membership size {expected_len}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_member_suffrage(node: &mut NodeProcess, id: &str, expected_suffrage: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert_child_running(node);
        if let Ok(response) =
            conductor_call(&node.rpc_url, "conductor_clusterMembership", json!([])).await
        {
            let actual_suffrage = response
                .get("result")
                .and_then(|result| result.get("servers"))
                .and_then(Value::as_array)
                .and_then(|servers| {
                    servers
                        .iter()
                        .find(|server| server.get("id").and_then(Value::as_str) == Some(id))
                })
                .and_then(|server| server.get("suffrage"))
                .and_then(Value::as_u64);
            if actual_suffrage == Some(expected_suffrage) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {id} suffrage {expected_suffrage}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_start(nodes: &mut [NodeProcess], node_id: &str, expected_hash: Hash) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert_children_running(nodes);
        let node = nodes.iter().find(|node| node.id == node_id).unwrap();
        if node.kona.hash_param_starts().contains(&expected_hash) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {node_id} to start with {expected_hash}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn assert_child_running(node: &mut NodeProcess) {
    if let Some(status) = node.try_wait().unwrap() {
        panic!("{} conductor exited unexpectedly: {status}", node.id);
    }
}

fn assert_children_running(nodes: &mut [NodeProcess]) {
    for node in nodes {
        assert_child_running(node);
    }
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

#[tokio::test]
async fn three_conductor_binaries_fail_over_and_repair_stale_kona_node() {
    let storage_dir = tempfile::tempdir().unwrap();
    let committed_hash = kona_payload_hash();
    let mut nodes = Vec::new();
    for (id, bootstrap) in [("seq-a", true), ("seq-b", false), ("seq-c", false)] {
        nodes.push(
            spawn_node(
                id,
                bootstrap,
                &storage_dir,
                Arc::new(FakeKonaState::new(committed_hash, 10)),
            )
            .await,
        );
    }
    wait_rpc_ready(&mut nodes).await;

    let by_id = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.id.clone(), index))
        .collect::<BTreeMap<_, _>>();
    let first_leader_id = wait_leader_id(&mut nodes, None).await;
    let leader_index = by_id[&first_leader_id];
    for id in ["seq-b", "seq-c"] {
        let node_id = nodes[by_id[id]].id.clone();
        let node_consensus_addr = nodes[by_id[id]].consensus_addr.clone();
        let response = conductor_call(
            &nodes[leader_index].rpc_url,
            "conductor_addServerAsNonvoter",
            json!([node_id, node_consensus_addr, 0]),
        )
        .await
        .unwrap();
        assert!(response.get("error").is_none(), "{response}");
        wait_for_member_suffrage(&mut nodes[leader_index], id, 1).await;

        let node_id = nodes[by_id[id]].id.clone();
        let node_consensus_addr = nodes[by_id[id]].consensus_addr.clone();
        let response = conductor_call(
            &nodes[leader_index].rpc_url,
            "conductor_addServerAsVoter",
            json!([node_id, node_consensus_addr, 0]),
        )
        .await
        .unwrap();
        assert!(response.get("error").is_none(), "{response}");
        wait_for_member_suffrage(&mut nodes[leader_index], id, 0).await;
    }
    wait_for_membership(&mut nodes[leader_index], 3).await;

    let committed_payload = payload(10, committed_hash);
    let response = conductor_call(
        &nodes[leader_index].rpc_url,
        "conductor_commitUnsafePayload",
        json!([committed_payload.clone()]),
    )
    .await
    .unwrap();
    assert!(response.get("error").is_none(), "{response}");

    let response = conductor_call(&nodes[leader_index].rpc_url, "conductor_resume", json!([]))
        .await
        .unwrap();
    assert!(response.get("error").is_none(), "{response}");

    wait_for_start(&mut nodes, &first_leader_id, committed_hash).await;
    let public_leader_id = assert_public_ha_contract(&mut nodes, 3).await;
    assert_eq!(public_leader_id, first_leader_id);

    let transfer_target_id = ["seq-a", "seq-b", "seq-c"]
        .into_iter()
        .find(|id| *id != first_leader_id)
        .unwrap()
        .to_string();
    let transfer_target_addr = nodes[by_id[&transfer_target_id]].consensus_addr.clone();
    let response = conductor_call(
        &nodes[leader_index].rpc_url,
        "conductor_transferLeaderToServer",
        json!([transfer_target_id.clone(), transfer_target_addr]),
    )
    .await
    .unwrap();
    assert!(response.get("error").is_none(), "{response}");

    let transferred_leader_id = wait_leader_id(&mut nodes, Some(&first_leader_id)).await;
    assert_eq!(transferred_leader_id, transfer_target_id);
    wait_for_start(&mut nodes, &transferred_leader_id, committed_hash).await;
    let public_leader_id = assert_public_ha_contract(&mut nodes, 3).await;
    assert_eq!(public_leader_id, transferred_leader_id);

    for node in &nodes {
        if node.id != transferred_leader_id {
            node.kona.set_latest(hash(0x09), 9);
        }
    }
    let transferred_leader_index = by_id[&transferred_leader_id];
    let transferred_leader = &nodes[transferred_leader_index];
    transferred_leader.kona.set_peer_count(0);

    let next_leader_id = wait_leader_id(&mut nodes, Some(&transferred_leader_id)).await;
    let next_index = by_id[&next_leader_id];

    wait_for_start(&mut nodes, &next_leader_id, committed_hash).await;
    let public_leader_id = assert_public_ha_contract(&mut nodes, 3).await;
    assert_eq!(public_leader_id, next_leader_id);

    let next = &nodes[next_index];
    assert_eq!(
        next.kona.posted_payloads(),
        vec![PayloadEnvelope::new(committed_payload)]
    );
    assert!(next.kona.active());
    assert_eq!(next.kona.parameterless_starts(), 0);
    assert!(next.kona.hash_param_starts().contains(&committed_hash));

    let old_leader = &nodes[transferred_leader_index];
    assert!(!old_leader.kona.active());
    assert!(old_leader.kona.stops() >= 1);
    assert!(old_leader
        .kona
        .hash_param_starts()
        .contains(&committed_hash));
    assert_eq!(old_leader.kona.parameterless_starts(), 0);

    for node in &nodes {
        assert_eq!(node.kona.parameterless_starts(), 0);
        if node.id != first_leader_id
            && node.id != transferred_leader_id
            && node.id != next_leader_id
        {
            assert!(!node.kona.active(), "{} should not be sequencing", node.id);
            assert!(node.kona.hash_param_starts().is_empty());
            assert!(node.kona.posted_payloads().is_empty());
        }
    }
}

#[tokio::test]
async fn conductor_binary_demotes_current_leader_and_removes_raft_member() {
    let storage_dir = tempfile::tempdir().unwrap();
    let committed_hash = kona_payload_hash();
    let mut nodes = Vec::new();
    for (id, bootstrap) in [("seq-a", true), ("seq-b", false)] {
        nodes.push(
            spawn_node(
                id,
                bootstrap,
                &storage_dir,
                Arc::new(FakeKonaState::new(committed_hash, 10)),
            )
            .await,
        );
    }
    wait_rpc_ready(&mut nodes).await;

    let by_id = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.id.clone(), index))
        .collect::<BTreeMap<_, _>>();
    let old_leader_id = wait_leader_id(&mut nodes, None).await;
    let old_leader_index = by_id[&old_leader_id];

    let node_id = nodes[by_id["seq-b"]].id.clone();
    let node_consensus_addr = nodes[by_id["seq-b"]].consensus_addr.clone();
    let response = conductor_call(
        &nodes[old_leader_index].rpc_url,
        "conductor_addServerAsNonvoter",
        json!([node_id.clone(), node_consensus_addr.clone(), 0]),
    )
    .await
    .unwrap();
    assert!(response.get("error").is_none(), "{response}");
    wait_for_member_suffrage(&mut nodes[old_leader_index], &node_id, 1).await;

    let response = conductor_call(
        &nodes[old_leader_index].rpc_url,
        "conductor_addServerAsVoter",
        json!([node_id.clone(), node_consensus_addr, 0]),
    )
    .await
    .unwrap();
    assert!(response.get("error").is_none(), "{response}");
    wait_for_member_suffrage(&mut nodes[old_leader_index], &node_id, 0).await;

    let response = conductor_call(
        &nodes[old_leader_index].rpc_url,
        "conductor_demoteVoter",
        json!([old_leader_id.clone(), 0]),
    )
    .await
    .unwrap();
    assert!(response.get("error").is_none(), "{response}");

    let new_leader_id = wait_leader_id(&mut nodes, Some(&old_leader_id)).await;
    assert_eq!(new_leader_id, node_id);
    let new_leader_index = by_id[&new_leader_id];
    wait_for_member_suffrage(&mut nodes[new_leader_index], &old_leader_id, 1).await;

    let response = conductor_call(
        &nodes[new_leader_index].rpc_url,
        "conductor_removeServer",
        json!([old_leader_id.clone(), 0]),
    )
    .await
    .unwrap();
    assert!(response.get("error").is_none(), "{response}");
    wait_for_membership(&mut nodes[new_leader_index], 1).await;
    assert_child_running(&mut nodes[old_leader_index]);

    let membership = conductor_call(
        &nodes[new_leader_index].rpc_url,
        "conductor_clusterMembership",
        json!([]),
    )
    .await
    .unwrap();
    let servers = membership
        .get("result")
        .and_then(|result| result.get("servers"))
        .and_then(Value::as_array)
        .unwrap();
    assert!(
        servers
            .iter()
            .all(|server| server.get("id").and_then(Value::as_str) != Some(old_leader_id.as_str())),
        "{membership}"
    );
}
