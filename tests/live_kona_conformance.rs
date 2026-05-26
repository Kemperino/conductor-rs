use conductor_rs::{types::PeerStats, types::SyncStatus, Hash, PayloadEnvelope};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Url};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::{env, time::Duration};
use tokio::time::{sleep, Instant};

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

#[tokio::test]
#[ignore = "requires CONDUCTOR_RS_LIVE_KONA_NODE_RPC and CONDUCTOR_RS_LIVE_KONA_EXECUTION_RPC"]
async fn live_kona_admin_rpc_supports_current_interop() {
    let node_rpc = required_url("CONDUCTOR_RS_LIVE_KONA_NODE_RPC");
    let execution_rpc = required_url("CONDUCTOR_RS_LIVE_KONA_EXECUTION_RPC");
    let client = client();

    let conductor_enabled: bool = rpc_call(&client, &node_rpc, "admin_conductorEnabled", json!([]))
        .await
        .unwrap();
    assert!(conductor_enabled, "kona node must be in conductor mode");

    let _: bool = rpc_call(&client, &node_rpc, "admin_sequencerActive", json!([]))
        .await
        .unwrap();
    let sync_status: SyncStatus = rpc_call(&client, &node_rpc, "optimism_syncStatus", json!([]))
        .await
        .unwrap();
    let _: PeerStats = rpc_call(&client, &node_rpc, "opp2p_peerStats", json!([]))
        .await
        .unwrap();
    let latest = latest_execution_block(&client, &execution_rpc).await;

    if let Some(sync_hash) = sync_status.unsafe_l2.hash {
        assert_eq!(
            sync_hash, latest.hash,
            "kona unsafe_l2 and execution latest must agree before a start check"
        );
    }

    if let Some(payload) = optional_payload("CONDUCTOR_RS_LIVE_KONA_UNSAFE_PAYLOAD_FILE").await {
        let expected_hash = payload.block_hash().unwrap();
        let expected_number = payload.block_number().unwrap();
        rpc_call::<Value>(
            &client,
            &node_rpc,
            "admin_postUnsafePayload",
            json!([payload.raw()]),
        )
        .await
        .unwrap();

        wait_for_execution_block(&client, &execution_rpc, expected_hash, expected_number).await;
    }

    if env_flag("CONDUCTOR_RS_LIVE_KONA_OVERRIDE_CHECK") {
        let conductor_rpc = required_url("CONDUCTOR_RS_LIVE_KONA_CONDUCTOR_RPC");
        let overridden_before: bool = rpc_call(
            &client,
            &conductor_rpc,
            "conductor_leaderOverridden",
            json!([]),
        )
        .await
        .unwrap();
        assert!(
            !overridden_before,
            "override check requires a conductor without an existing leader override"
        );

        rpc_call::<Value>(&client, &node_rpc, "admin_overrideLeader", json!([]))
            .await
            .unwrap();
        let overridden_after: bool = rpc_call(
            &client,
            &conductor_rpc,
            "conductor_leaderOverridden",
            json!([]),
        )
        .await
        .unwrap();
        let clear_result = rpc_call::<Value>(
            &client,
            &conductor_rpc,
            "conductor_overrideLeader",
            json!([false]),
        )
        .await;
        assert!(
            overridden_after,
            "admin_overrideLeader should set conductor leader override"
        );
        clear_result.unwrap();
    }

    if env_flag("CONDUCTOR_RS_LIVE_KONA_START_CHECK") {
        rpc_call::<Value>(
            &client,
            &node_rpc,
            "admin_startSequencer",
            json!([latest.hash]),
        )
        .await
        .unwrap();

        let active_after: bool = rpc_call(&client, &node_rpc, "admin_sequencerActive", json!([]))
            .await
            .unwrap();
        assert!(active_after, "hash-gated start must activate sequencing");

        if env_flag("CONDUCTOR_RS_LIVE_KONA_STOP_AFTER_START") {
            let _: Hash = rpc_call(&client, &node_rpc, "admin_stopSequencer", json!([]))
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "requires CONDUCTOR_RS_LIVE_CONDUCTOR_RPCS"]
async fn live_conductor_cluster_exposes_upstream_ha_contract() {
    let conductors = required_url_list("CONDUCTOR_RS_LIVE_CONDUCTOR_RPCS");
    let client = client();

    let mut leader_indexes = Vec::new();
    for (index, conductor) in conductors.iter().enumerate() {
        let healthz: Value = client
            .get(conductor.join("/healthz").unwrap())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            healthz.get("version").is_some(),
            "{conductor} /healthz should expose version"
        );
        assert_live_main_rpc_websocket(conductor).await;

        let _: String = rpc_call(&client, conductor, "health_status", json!([]))
            .await
            .unwrap();
        let overridden: bool =
            rpc_call(&client, conductor, "conductor_leaderOverridden", json!([]))
                .await
                .unwrap();
        assert!(
            !overridden,
            "{conductor} has leader override enabled; live HA validation would be ambiguous"
        );

        let active: bool = rpc_call(&client, conductor, "conductor_active", json!([]))
            .await
            .unwrap();
        assert!(
            active,
            "{conductor} should be active during live validation"
        );

        let leader: bool = rpc_call(&client, conductor, "conductor_leader", json!([]))
            .await
            .unwrap();
        if leader {
            leader_indexes.push(index);
        }
    }

    assert_eq!(
        leader_indexes.len(),
        1,
        "exactly one conductor should report leader"
    );

    let leader_url = &conductors[leader_indexes[0]];
    let leader_before = leader_id(&client, leader_url).await;
    let membership: Value = rpc_call(
        &client,
        leader_url,
        "conductor_clusterMembership",
        json!([]),
    )
    .await
    .unwrap();
    assert!(
        membership
            .get("servers")
            .and_then(Value::as_array)
            .is_some_and(|servers| servers.len() >= conductors.len()),
        "cluster membership should include every supplied conductor endpoint"
    );

    if env_flag("CONDUCTOR_RS_LIVE_TRANSFER_CHECK") {
        rpc_call::<Value>(&client, leader_url, "conductor_transferLeader", json!([]))
            .await
            .unwrap();
        let leader_after = wait_for_new_leader(&client, &conductors, &leader_before).await;
        assert_ne!(leader_before, leader_after);
    }

    if env_flag("CONDUCTOR_RS_LIVE_PROXY_CHECK") {
        assert_live_proxy_surface(&client, leader_url).await;
        for (index, conductor) in conductors.iter().enumerate() {
            if index == leader_indexes[0] {
                continue;
            }
            for (method, params) in [
                ("eth_getBlockByNumber", json!(["latest", false])),
                ("optimism_syncStatus", json!([])),
            ] {
                let err = rpc_call::<Value>(&client, conductor, method, params)
                    .await
                    .expect_err(
                        "{method} follower proxy calls must be rejected after backend lookup",
                    );
                assert_eq!(err.code, -32000, "{conductor} {method} returned {err:?}");
                assert!(
                    err.message.contains("non-leader"),
                    "{conductor} {method} returned unexpected follower proxy error: {err:?}"
                );
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ExecutionBlock {
    hash: Hash,
    number: u64,
}

async fn latest_execution_block(client: &Client, execution_rpc: &Url) -> ExecutionBlock {
    let value: Value = rpc_call(
        client,
        execution_rpc,
        "eth_getBlockByNumber",
        json!(["latest", false]),
    )
    .await
    .unwrap();
    let hash = value
        .get("hash")
        .and_then(Value::as_str)
        .expect("latest execution block must include hash")
        .parse()
        .unwrap();
    let number = value
        .get("number")
        .and_then(Value::as_str)
        .and_then(|raw| raw.strip_prefix("0x"))
        .and_then(|hex| u64::from_str_radix(hex, 16).ok())
        .expect("latest execution block must include hex number");
    ExecutionBlock { hash, number }
}

async fn wait_for_execution_block(
    client: &Client,
    execution_rpc: &Url,
    expected_hash: Hash,
    expected_number: u64,
) -> ExecutionBlock {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let latest = latest_execution_block(client, execution_rpc).await;
        if latest.hash == expected_hash && latest.number == expected_number {
            return latest;
        }
        assert!(
            Instant::now() < deadline,
            "admin_postUnsafePayload did not repair execution latest before timeout; expected {expected_hash:?}/{expected_number}, latest was {:?}/{}",
            latest.hash,
            latest.number
        );
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_new_leader(client: &Client, conductors: &[Url], old: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for conductor in conductors {
            if let Ok(true) =
                rpc_call::<bool>(client, conductor, "conductor_leader", json!([])).await
            {
                let id = leader_id(client, conductor).await;
                if id != old {
                    return id;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for new leader"
        );
        sleep(Duration::from_millis(250)).await;
    }
}

async fn assert_live_proxy_surface(client: &Client, leader_url: &Url) {
    let latest: Value = rpc_call(
        client,
        leader_url,
        "eth_getBlockByNumber",
        json!(["latest", false]),
    )
    .await
    .unwrap();
    assert!(
        latest.get("hash").and_then(Value::as_str).is_some(),
        "leader eth_getBlockByNumber should return a block hash: {latest}"
    );
    assert!(
        latest.get("number").and_then(Value::as_str).is_some(),
        "leader eth_getBlockByNumber should return a block number: {latest}"
    );
    let latest_number = latest
        .get("number")
        .and_then(Value::as_str)
        .and_then(|raw| raw.strip_prefix("0x"))
        .and_then(|hex| u64::from_str_radix(hex, 16).ok())
        .expect("leader eth_getBlockByNumber should return a parseable block number");

    let _: SyncStatus = rpc_call(client, leader_url, "optimism_syncStatus", json!([]))
        .await
        .unwrap();
    let output: Value = rpc_call(
        client,
        leader_url,
        "optimism_outputAtBlock",
        json!([format!("0x{latest_number:x}")]),
    )
    .await
    .unwrap();
    assert!(
        output.get("outputRoot").is_some() || output.get("blockRef").is_some(),
        "leader optimism_outputAtBlock response should look like an output response: {output}"
    );

    let rollup_config: Value = rpc_call(client, leader_url, "optimism_rollupConfig", json!([]))
        .await
        .unwrap();
    assert!(
        rollup_config.get("genesis").is_some()
            || rollup_config.get("l2_chain_id").is_some()
            || rollup_config.get("l2ChainID").is_some(),
        "leader optimism_rollupConfig response should look like a rollup config: {rollup_config}"
    );

    let _: bool = rpc_call(client, leader_url, "admin_sequencerActive", json!([]))
        .await
        .unwrap();
}

async fn assert_live_main_rpc_websocket(conductor: &Url) {
    for path in ["/", "/ws", "/ws/"] {
        let (mut ws, _) =
            tokio_tungstenite::connect_async(websocket_url(conductor, path).to_string())
                .await
                .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"jsonrpc":"2.0","id":1,"method":"conductor_leader","params":[]}"#.into(),
        ))
        .await
        .unwrap();

        let response = ws.next().await.unwrap().unwrap();
        let tokio_tungstenite::tungstenite::Message::Text(text) = response else {
            panic!("{conductor}{path} expected websocket text response, got {response:?}");
        };
        let payload: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            payload.get("jsonrpc").and_then(Value::as_str),
            Some("2.0"),
            "{conductor}{path} websocket JSON-RPC version"
        );
        assert_eq!(
            payload.get("id").and_then(Value::as_i64),
            Some(1),
            "{conductor}{path} websocket JSON-RPC id"
        );
        assert!(
            payload.get("result").and_then(Value::as_bool).is_some(),
            "{conductor}{path} websocket conductor_leader response should contain bool result: {payload}"
        );
    }
}

fn websocket_url(base: &Url, path: &str) -> Url {
    let mut url = base.clone();
    let scheme = match base.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" => "ws",
        "wss" => "wss",
        other => panic!("cannot derive websocket URL from {other} scheme in {base}"),
    };
    url.set_scheme(scheme).unwrap();
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    url
}

async fn leader_id(client: &Client, conductor: &Url) -> String {
    let value: Value = rpc_call(client, conductor, "conductor_leaderWithID", json!([]))
        .await
        .unwrap();
    value
        .get("id")
        .and_then(Value::as_str)
        .expect("leaderWithID response must include id")
        .to_string()
}

async fn rpc_call<T>(client: &Client, url: &Url, method: &str, params: Value) -> Result<T, RpcError>
where
    T: DeserializeOwned,
{
    let response = client
        .post(url.clone())
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

    if let Some(error) = response.get("error") {
        return Err(RpcError {
            code: error
                .get("code")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown json-rpc error")
                .to_string(),
        });
    }

    let result = response
        .get("result")
        .unwrap_or_else(|| panic!("{method} response missing result: {response}"));
    serde_json::from_value(result.clone()).map_err(|err| RpcError {
        code: -1,
        message: err.to_string(),
    })
}

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

fn required_url(name: &str) -> Url {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set"))
        .parse()
        .unwrap_or_else(|err| panic!("{name} must be a URL: {err}"))
}

fn required_url_list(name: &str) -> Vec<Url> {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set"))
        .split(',')
        .map(|raw| raw.trim().parse().unwrap())
        .collect()
}

fn env_flag(name: &str) -> bool {
    matches!(
        env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

async fn optional_payload(name: &str) -> Option<PayloadEnvelope> {
    let path = env::var(name).ok()?;
    let raw = tokio::fs::read_to_string(&path)
        .await
        .unwrap_or_else(|err| panic!("failed to read {name}={path}: {err}"));
    let value = serde_json::from_str::<Value>(&raw)
        .unwrap_or_else(|err| panic!("failed to parse {name}={path} as JSON: {err}"));
    Some(PayloadEnvelope::new(value))
}
