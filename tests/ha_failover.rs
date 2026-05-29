use async_trait::async_trait;
use conductor_rs::{
    sequencer::{SequencerControl, SequencerError},
    types::{BlockInfo, L2BlockRef, PeerStats, SyncStatus},
    Conductor, ConductorConfig, Consensus, Hash, InMemoryRaftNetwork, PayloadEnvelope,
    RaftConsensus,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
struct FakeSequencer {
    latest: Mutex<BlockInfo>,
    blocks: Mutex<BTreeMap<u64, BlockInfo>>,
    starts: Mutex<Vec<Hash>>,
    stops: Mutex<u64>,
    posts: Mutex<Vec<PayloadEnvelope>>,
    active: AtomicBool,
    peer_count: Mutex<u64>,
}

impl FakeSequencer {
    fn new(latest: BlockInfo) -> Self {
        let mut blocks = BTreeMap::new();
        blocks.insert(latest.number, latest.clone());
        Self {
            latest: Mutex::new(latest),
            blocks: Mutex::new(blocks),
            starts: Mutex::new(Vec::new()),
            stops: Mutex::new(0),
            posts: Mutex::new(Vec::new()),
            active: AtomicBool::new(false),
            peer_count: Mutex::new(1),
        }
    }

    fn set_latest(&self, latest: BlockInfo) {
        self.blocks
            .lock()
            .unwrap()
            .insert(latest.number, latest.clone());
        *self.latest.lock().unwrap() = latest;
    }

    fn set_peer_count(&self, peer_count: u64) {
        *self.peer_count.lock().unwrap() = peer_count;
    }

    fn starts(&self) -> Vec<Hash> {
        self.starts.lock().unwrap().clone()
    }

    fn stops(&self) -> u64 {
        *self.stops.lock().unwrap()
    }

    fn posts(&self) -> Vec<PayloadEnvelope> {
        self.posts.lock().unwrap().clone()
    }

    fn active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SequencerControl for FakeSequencer {
    async fn latest_unsafe_block(&self) -> Result<BlockInfo, SequencerError> {
        Ok(self.latest.lock().unwrap().clone())
    }

    async fn block_by_number(&self, number: u64) -> Result<BlockInfo, SequencerError> {
        if let Some(block) = self.blocks.lock().unwrap().get(&number).cloned() {
            Ok(block)
        } else {
            Err(SequencerError::Rpc(
                conductor_rs::rpc::RpcClientError::InvalidResponse("block not found".to_string()),
            ))
        }
    }

    async fn start_sequencer(&self, expected_hash: Hash) -> Result<(), SequencerError> {
        self.starts.lock().unwrap().push(expected_hash);
        self.active.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn stop_sequencer(&self) -> Result<Hash, SequencerError> {
        *self.stops.lock().unwrap() += 1;
        self.active.store(false, Ordering::SeqCst);
        Ok(self.latest.lock().unwrap().hash)
    }

    async fn sequencer_active(&self) -> Result<bool, SequencerError> {
        Ok(self.active())
    }

    async fn sync_status(&self) -> Result<SyncStatus, SequencerError> {
        let latest = self.latest.lock().unwrap().clone();
        let now = unix_now();
        Ok(SyncStatus {
            unsafe_l2: L2BlockRef {
                hash: Some(latest.hash),
                number: latest.number,
                time: now,
            },
            safe_l2: L2BlockRef {
                hash: Some(latest.hash),
                number: latest.number.saturating_sub(1),
                time: now,
            },
        })
    }

    async fn peer_stats(&self) -> Result<PeerStats, SequencerError> {
        Ok(PeerStats {
            connected: *self.peer_count.lock().unwrap(),
        })
    }

    async fn post_unsafe_payload(&self, payload: &PayloadEnvelope) -> Result<(), SequencerError> {
        self.posts.lock().unwrap().push(payload.clone());
        self.set_latest(BlockInfo {
            hash: payload.block_hash().unwrap(),
            number: payload.block_number().unwrap(),
        });
        Ok(())
    }

    async fn conductor_enabled(&self) -> Result<bool, SequencerError> {
        Ok(true)
    }
}

fn hash(byte: u8) -> Hash {
    format!("0x{}", hex::encode([byte; 32])).parse().unwrap()
}

fn payload(number: u64, byte: u8) -> PayloadEnvelope {
    PayloadEnvelope::new(serde_json::json!({
        "executionPayload": {
            "blockHash": hash(byte).to_string(),
            "blockNumber": format!("0x{number:x}")
        }
    }))
}

fn block(number: u64, byte: u8) -> BlockInfo {
    BlockInfo {
        hash: hash(byte),
        number,
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn test_cluster() -> Vec<Arc<RaftConsensus>> {
    let network = InMemoryRaftNetwork::new();
    let a = RaftConsensus::new_in_memory("seq-a", "127.0.0.1:1001", network.clone())
        .await
        .unwrap();
    let b = RaftConsensus::new_in_memory("seq-b", "127.0.0.1:1002", network.clone())
        .await
        .unwrap();
    let c = RaftConsensus::new_in_memory("seq-c", "127.0.0.1:1003", network)
        .await
        .unwrap();
    a.initialize([
        ("seq-a".to_string(), "127.0.0.1:1001".to_string()),
        ("seq-b".to_string(), "127.0.0.1:1002".to_string()),
        ("seq-c".to_string(), "127.0.0.1:1003".to_string()),
    ])
    .await
    .unwrap();
    a.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    vec![a, b, c]
}

async fn wait_for_leader_matching(
    nodes: &[Arc<RaftConsensus>],
    matches: impl Fn(&str) -> bool,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(node) = nodes
            .iter()
            .find(|node| node.leader() && matches(node.server_id()))
        {
            return node.server_id().to_string();
        }
        assert!(Instant::now() < deadline, "timed out waiting for leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_payload(node: &RaftConsensus, expected: PayloadEnvelope) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if node.latest_unsafe_payload().await.unwrap() == Some(expected.clone()) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for payload");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn real_raft_failover_repairs_stale_candidate_before_starting() {
    let nodes = test_cluster().await;
    let first_leader_id = wait_for_leader_matching(&nodes, |_| true).await;
    let committed = payload(10, 0x10);
    let first_leader = nodes
        .iter()
        .find(|node| node.server_id() == first_leader_id)
        .unwrap();

    first_leader
        .commit_unsafe_payload(committed.clone())
        .await
        .unwrap();
    for node in &nodes {
        wait_payload(node, committed.clone()).await;
    }

    let conductors = nodes
        .iter()
        .map(|node| {
            let sequencer = Arc::new(FakeSequencer::new(block(10, 0x10)));
            let conductor = Conductor::new(
                node.clone(),
                sequencer.clone(),
                ConductorConfig {
                    round_robin_leader_transfer: true,
                    unsafe_repair_depth: 1,
                    ..ConductorConfig::default()
                },
            );
            (node.clone(), sequencer, conductor)
        })
        .collect::<Vec<_>>();

    let first = conductors
        .iter()
        .find(|(node, _, _)| node.server_id() == first_leader_id)
        .unwrap();

    first.2.tick().await.unwrap();
    assert_eq!(first.1.starts(), vec![hash(0x10)]);

    first.1.set_peer_count(0);
    first.2.tick().await.unwrap();
    assert_eq!(first.1.stops(), 1);
    assert!(!first.1.active());

    let next_leader_id = wait_for_leader_matching(&nodes, |id| id != first_leader_id).await;
    let next = conductors
        .iter()
        .find(|(node, _, _)| node.server_id() == next_leader_id)
        .unwrap();
    next.1.set_latest(block(9, 0x09));

    next.2.tick().await.unwrap();

    assert_eq!(next.1.posts(), vec![committed]);
    assert_eq!(next.1.starts(), vec![hash(0x10)]);
    assert!(next.1.active());

    for node in &nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn real_raft_failover_starts_ahead_candidate_on_same_chain() {
    let nodes = test_cluster().await;
    let first_leader_id = wait_for_leader_matching(&nodes, |_| true).await;
    let committed = payload(10, 0x10);
    let first_leader = nodes
        .iter()
        .find(|node| node.server_id() == first_leader_id)
        .unwrap();

    first_leader.commit_unsafe_payload(committed).await.unwrap();
    for node in &nodes {
        wait_payload(node, payload(10, 0x10)).await;
    }

    let conductors = nodes
        .iter()
        .map(|node| {
            let sequencer = Arc::new(FakeSequencer::new(block(10, 0x10)));
            let conductor = Conductor::new(
                node.clone(),
                sequencer.clone(),
                ConductorConfig {
                    round_robin_leader_transfer: true,
                    unsafe_repair_depth: 1,
                    ..ConductorConfig::default()
                },
            );
            (node.clone(), sequencer, conductor)
        })
        .collect::<Vec<_>>();

    let first = conductors
        .iter()
        .find(|(node, _, _)| node.server_id() == first_leader_id)
        .unwrap();

    first.2.tick().await.unwrap();
    assert_eq!(first.1.starts(), vec![hash(0x10)]);

    first.1.set_peer_count(0);
    first.2.tick().await.unwrap();

    let next_leader_id = wait_for_leader_matching(&nodes, |id| id != first_leader_id).await;
    let next = conductors
        .iter()
        .find(|(node, _, _)| node.server_id() == next_leader_id)
        .unwrap();
    next.1.set_latest(block(12, 0x12));

    next.2.tick().await.unwrap();

    assert!(next.1.posts().is_empty());
    assert_eq!(next.1.starts(), vec![hash(0x12)]);
    assert!(next.1.active());

    for node in &nodes {
        node.shutdown().await.unwrap();
    }
}
