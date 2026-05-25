use crate::{
    consensus::{ClusterMembership, Consensus, ConsensusError, ServerInfo, ServerSuffrage},
    types::PayloadEnvelope,
};
use axum::{extract::State, routing::post, Json, Router};
use futures_util::{Stream, TryStreamExt};
use openraft::{
    alias::{
        EntryOf, LogIdOf, SnapshotDataOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
    },
    entry::RaftEntry,
    error::{ClientWriteError, NetworkError, RaftError, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
        VoteRequest, VoteResponse,
    },
    rt::WatchReceiver,
    storage::{EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine},
    ChangeMembers, Config, EntryPayload, OptionalSend, Raft, RaftLogReader, RaftSnapshotBuilder,
    ReadPolicy, ServerState, SnapshotPolicy,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::{self, Cursor, Read},
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    fs,
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
};

const MEMBER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftNode {
    pub addr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RaftCommand {
    CommitUnsafePayload(PayloadEnvelope),
}

impl fmt::Display for RaftCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitUnsafePayload(payload) => {
                match (payload.block_number(), payload.block_hash()) {
                    (Ok(number), Ok(hash)) => write!(f, "CommitUnsafePayload({number}, {hash})"),
                    _ => write!(f, "CommitUnsafePayload(<invalid>)"),
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RaftCommandResponse {
    pub previous: Option<PayloadEnvelope>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftCommand,
        R = RaftCommandResponse,
        NodeId = String,
        Node = RaftNode,
);

type OpenRaft = Raft<TypeConfig, Arc<MemoryRaftStore>>;

#[derive(Clone, Debug)]
pub struct RaftConsensusConfig {
    pub server_id: String,
    pub advertised_addr: String,
    pub storage_dir: PathBuf,
    pub bootstrap: bool,
    pub snapshot_interval: Duration,
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub snapshot_threshold: u64,
    pub trailing_logs: u64,
}

impl RaftConsensusConfig {
    fn openraft_config(&self) -> Result<Arc<Config>, ConsensusError> {
        let config = Config {
            cluster_name: "op-conductor".to_string(),
            heartbeat_interval: millis(self.heartbeat_interval),
            election_timeout_min: millis(self.election_timeout_min),
            election_timeout_max: millis(self.election_timeout_max),
            snapshot_policy: if self.snapshot_threshold == 0 {
                SnapshotPolicy::Never
            } else {
                SnapshotPolicy::LogsSinceLast(self.snapshot_threshold)
            },
            max_in_snapshot_log_to_keep: self.trailing_logs,
            ..Default::default()
        };
        Ok(Arc::new(config.validate().map_err(raft_err)?))
    }
}

impl Default for RaftConsensusConfig {
    fn default() -> Self {
        Self {
            server_id: String::new(),
            advertised_addr: String::new(),
            storage_dir: PathBuf::from("."),
            bootstrap: false,
            snapshot_interval: Duration::from_secs(120),
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            snapshot_threshold: 8192,
            trailing_logs: 10240,
        }
    }
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn default_transfer_target(
    membership: &ClusterMembership,
    local_id: &str,
    matched_indexes: &BTreeMap<String, Option<u64>>,
) -> Result<ServerInfo, ConsensusError> {
    let mut pick = None;
    let mut current = 0;
    for server in &membership.servers {
        if server.id == local_id || server.suffrage != ServerSuffrage::Voter {
            continue;
        }
        let Some(matched_index) = matched_indexes.get(&server.id) else {
            continue;
        };
        let next_index = matched_index
            .map(|index| index.saturating_add(1))
            .unwrap_or(0);
        if next_index > current {
            current = next_index;
            pick = Some(server.clone());
        }
    }
    pick.ok_or_else(|| ConsensusError::Raft("cannot find peer".to_string()))
}

#[derive(Clone)]
pub struct InMemoryRaftNetwork {
    peers: Arc<RwLock<BTreeMap<String, OpenRaft>>>,
}

impl InMemoryRaftNetwork {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    async fn register(&self, id: String, raft: OpenRaft) {
        self.peers.write().await.insert(id, raft);
    }

    fn factory(&self) -> InMemoryRaftNetworkFactory {
        InMemoryRaftNetworkFactory {
            peers: self.peers.clone(),
        }
    }
}

impl Default for InMemoryRaftNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InMemoryRaftNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryRaftNetwork")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct InMemoryRaftNetworkFactory {
    peers: Arc<RwLock<BTreeMap<String, OpenRaft>>>,
}

impl fmt::Debug for InMemoryRaftNetworkFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryRaftNetworkFactory")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct InMemoryRaftClient {
    target: String,
    peers: Arc<RwLock<BTreeMap<String, OpenRaft>>>,
}

impl fmt::Debug for InMemoryRaftClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryRaftClient")
            .field("target", &self.target)
            .finish()
    }
}

impl RaftNetworkFactory<TypeConfig> for InMemoryRaftNetworkFactory {
    type Network = InMemoryRaftClient;

    async fn new_client(&mut self, target: String, _node: &RaftNode) -> Self::Network {
        InMemoryRaftClient {
            target,
            peers: self.peers.clone(),
        }
    }
}

impl RaftNetworkV2<TypeConfig> for InMemoryRaftClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, openraft::error::RPCError<TypeConfig>> {
        self.target_raft()
            .await?
            .append_entries(rpc)
            .await
            .map_err(network_error)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, openraft::error::RPCError<TypeConfig>> {
        self.target_raft()
            .await?
            .vote(rpc)
            .await
            .map_err(network_error)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        _cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
            + OptionalSend
            + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        self.target_raft()
            .await
            .map_err(StreamingError::from)?
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|err| StreamingError::Network(NetworkError::from_string(err.to_string())))
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<(), openraft::error::RPCError<TypeConfig>> {
        self.target_raft()
            .await?
            .handle_transfer_leader(req)
            .await
            .map_err(network_error)
    }
}

impl InMemoryRaftClient {
    async fn target_raft(&self) -> Result<OpenRaft, openraft::error::RPCError<TypeConfig>> {
        self.peers
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                openraft::error::RPCError::Unreachable(Unreachable::from_string(format!(
                    "missing raft peer {}",
                    self.target
                )))
            })
    }
}

#[derive(Clone, Debug, Default)]
pub struct HttpRaftNetwork {
    client: reqwest::Client,
}

impl HttpRaftNetwork {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpRaftNetwork {
    type Network = HttpRaftClient;

    async fn new_client(&mut self, target: String, node: &RaftNode) -> Self::Network {
        HttpRaftClient {
            target,
            addr: node.addr.clone(),
            client: self.client.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpRaftClient {
    target: String,
    addr: String,
    client: reqwest::Client,
}

impl RaftNetworkV2<TypeConfig> for HttpRaftClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, openraft::error::RPCError<TypeConfig>> {
        self.post("append_entries", &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, openraft::error::RPCError<TypeConfig>> {
        self.post("vote", &rpc).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        mut snapshot: SnapshotOf<TypeConfig>,
        _cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
            + OptionalSend
            + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let mut data = Vec::new();
        snapshot
            .snapshot
            .read_to_end(&mut data)
            .map_err(|err| StreamingError::Network(NetworkError::new(&err)))?;
        let request = FullSnapshotHttpRequest {
            vote,
            meta: snapshot.meta,
            data,
        };
        self.post("full_snapshot", &request)
            .await
            .map_err(StreamingError::from)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<(), openraft::error::RPCError<TypeConfig>> {
        self.post("transfer_leader", &req).await
    }
}

impl HttpRaftClient {
    async fn post<T, R>(
        &self,
        method: &'static str,
        request: &T,
    ) -> Result<R, openraft::error::RPCError<TypeConfig>>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = raft_url(&self.addr, method);
        let response = self
            .client
            .post(url)
            .json(request)
            .send()
            .await
            .map_err(network_error)?;
        if !response.status().is_success() {
            return Err(openraft::error::RPCError::Network(
                NetworkError::from_string(format!(
                    "raft peer {} returned HTTP {}",
                    self.target,
                    response.status()
                )),
            ));
        }
        let response = response
            .json::<RaftHttpResponse<R>>()
            .await
            .map_err(network_error)?;
        response.into_result().map_err(|message| {
            openraft::error::RPCError::Network(NetworkError::from_string(format!(
                "raft peer {}: {message}",
                self.target
            )))
        })
    }
}

fn raft_url(addr: &str, method: &str) -> String {
    let base = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", addr.trim_end_matches('/'))
    };
    format!("{base}/raft/{method}")
}

#[derive(Debug, Error)]
pub enum RaftTransportError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("server error: {0}")]
    Server(#[from] axum::Error),
}

#[derive(Debug, Serialize, Deserialize)]
struct RaftHttpResponse<T> {
    result: Option<T>,
    error: Option<String>,
}

impl<T> RaftHttpResponse<T> {
    fn ok(result: T) -> Self {
        Self {
            result: Some(result),
            error: None,
        }
    }

    fn err(error: impl fmt::Display) -> Self {
        Self {
            result: None,
            error: Some(error.to_string()),
        }
    }

    fn into_result(self) -> Result<T, String> {
        match (self.result, self.error) {
            (Some(result), None) => Ok(result),
            (_, Some(error)) => Err(error),
            _ => Err("raft peer returned empty response".to_string()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FullSnapshotHttpRequest {
    vote: VoteOf<TypeConfig>,
    meta: SnapshotMetaOf<TypeConfig>,
    data: Vec<u8>,
}

#[derive(Clone)]
struct RaftTransportState {
    raft: OpenRaft,
}

pub async fn serve_raft_transport(
    consensus: Arc<RaftConsensus>,
    addr: std::net::SocketAddr,
) -> Result<(), RaftTransportError> {
    let listener = TcpListener::bind(addr).await?;
    serve_raft_transport_on_listener(consensus, listener).await
}

pub async fn serve_raft_transport_on_listener(
    consensus: Arc<RaftConsensus>,
    listener: TcpListener,
) -> Result<(), RaftTransportError> {
    let state = RaftTransportState {
        raft: consensus.raft.clone(),
    };
    let app = Router::new()
        .route("/raft/append_entries", post(http_append_entries))
        .route("/raft/vote", post(http_vote))
        .route("/raft/full_snapshot", post(http_full_snapshot))
        .route("/raft/transfer_leader", post(http_transfer_leader))
        .with_state(state);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn http_append_entries(
    State(state): State<RaftTransportState>,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<RaftHttpResponse<AppendEntriesResponse<TypeConfig>>> {
    Json(match state.raft.append_entries(request).await {
        Ok(response) => RaftHttpResponse::ok(response),
        Err(err) => RaftHttpResponse::err(err),
    })
}

async fn http_vote(
    State(state): State<RaftTransportState>,
    Json(request): Json<VoteRequest<TypeConfig>>,
) -> Json<RaftHttpResponse<VoteResponse<TypeConfig>>> {
    Json(match state.raft.vote(request).await {
        Ok(response) => RaftHttpResponse::ok(response),
        Err(err) => RaftHttpResponse::err(err),
    })
}

async fn http_full_snapshot(
    State(state): State<RaftTransportState>,
    Json(request): Json<FullSnapshotHttpRequest>,
) -> Json<RaftHttpResponse<SnapshotResponse<TypeConfig>>> {
    let snapshot = SnapshotOf::<TypeConfig> {
        meta: request.meta,
        snapshot: Cursor::new(request.data),
    };
    Json(
        match state
            .raft
            .install_full_snapshot(request.vote, snapshot)
            .await
        {
            Ok(response) => RaftHttpResponse::ok(response),
            Err(err) => RaftHttpResponse::err(err),
        },
    )
}

async fn http_transfer_leader(
    State(state): State<RaftTransportState>,
    Json(request): Json<TransferLeaderRequest<TypeConfig>>,
) -> Json<RaftHttpResponse<()>> {
    Json(match state.raft.handle_transfer_leader(request).await {
        Ok(response) => RaftHttpResponse::ok(response),
        Err(err) => RaftHttpResponse::err(err),
    })
}

fn network_error(err: impl std::error::Error) -> openraft::error::RPCError<TypeConfig> {
    openraft::error::RPCError::Network(NetworkError::from_string(err.to_string()))
}

#[derive(Debug)]
pub struct RaftConsensus {
    id: String,
    addr: String,
    raft: OpenRaft,
    store: Arc<MemoryRaftStore>,
    bootstrapped: AtomicBool,
}

impl RaftConsensus {
    pub async fn new_in_memory(
        id: impl Into<String>,
        addr: impl Into<String>,
        network: InMemoryRaftNetwork,
    ) -> Result<Arc<Self>, ConsensusError> {
        let id = id.into();
        let addr = addr.into();
        let store = Arc::new(MemoryRaftStore::new());
        let config = Arc::new(Config::default().validate().map_err(raft_err)?);
        let consensus =
            Self::new_with_network(id.clone(), addr, store, config, network.factory()).await?;
        network.register(id, consensus.raft.clone()).await;
        Ok(consensus)
    }

    pub async fn new_http(config: RaftConsensusConfig) -> Result<Arc<Self>, ConsensusError> {
        if config.server_id.is_empty() {
            return Err(ConsensusError::Raft("missing raft server id".to_string()));
        }
        if config.advertised_addr.is_empty() {
            return Err(ConsensusError::Raft(
                "missing raft advertised address".to_string(),
            ));
        }
        let store_path = config
            .storage_dir
            .join(&config.server_id)
            .join("raft-store.json");
        let store = Arc::new(
            MemoryRaftStore::open(store_path)
                .await
                .map_err(|err| ConsensusError::Raft(err.to_string()))?,
        );
        let consensus = Self::new_with_network(
            config.server_id.clone(),
            config.advertised_addr.clone(),
            store,
            config.openraft_config()?,
            HttpRaftNetwork::new(),
        )
        .await?;
        if config.bootstrap && !consensus.raft.is_initialized().await.map_err(raft_err)? {
            consensus
                .initialize([(config.server_id, config.advertised_addr)])
                .await?;
            consensus.bootstrapped.store(true, Ordering::SeqCst);
        }
        Ok(consensus)
    }

    async fn new_with_network<N>(
        id: String,
        addr: String,
        store: Arc<MemoryRaftStore>,
        config: Arc<Config>,
        network: N,
    ) -> Result<Arc<Self>, ConsensusError>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let raft = Raft::new(id.clone(), config, network, store.clone(), store.clone())
            .await
            .map_err(raft_err)?;

        Ok(Arc::new(Self {
            id,
            addr,
            raft,
            store,
            bootstrapped: AtomicBool::new(false),
        }))
    }

    pub fn bootstrapped(&self) -> bool {
        self.bootstrapped.load(Ordering::SeqCst)
    }

    pub async fn initialize(
        &self,
        members: impl IntoIterator<Item = (String, String)>,
    ) -> Result<(), ConsensusError> {
        let members = members
            .into_iter()
            .map(|(id, addr)| (id, RaftNode { addr }))
            .collect::<BTreeMap<_, _>>();
        self.raft.initialize(members).await.map_err(raft_err)
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<ServerInfo, ConsensusError> {
        let deadline = Instant::now() + timeout;
        loop {
            let info = self.leader_with_id();
            if !info.id.is_empty() {
                return Ok(info);
            }
            if Instant::now() >= deadline {
                return Err(ConsensusError::Raft(
                    "timed out waiting for leader".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn wait_for_leader_status_change(
        &self,
        previous: bool,
    ) -> Result<bool, ConsensusError> {
        let mut metrics = self.raft.metrics();
        loop {
            metrics
                .changed()
                .await
                .map_err(|err| ConsensusError::Raft(format!("raft metrics watch closed: {err}")))?;
            let leader = metrics.borrow_watched().state == ServerState::Leader;
            if leader != previous {
                return Ok(leader);
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), ConsensusError> {
        self.raft.shutdown().await.map_err(raft_err)
    }

    fn membership_snapshot(&self) -> ClusterMembership {
        let metrics = self.raft.metrics().borrow_watched().clone();
        let membership = metrics.membership_config.membership();
        let voters = membership.voter_ids().collect::<BTreeSet<_>>();
        let mut servers = membership
            .nodes()
            .map(|(id, node)| ServerInfo {
                id: id.clone(),
                addr: node.addr.clone(),
                suffrage: if voters.contains(id) {
                    ServerSuffrage::Voter
                } else {
                    ServerSuffrage::Nonvoter
                },
            })
            .collect::<Vec<_>>();
        servers.sort_by(|left, right| left.id.cmp(&right.id));
        ClusterMembership {
            servers,
            version: metrics
                .membership_config
                .log_id()
                .as_ref()
                .map(|log_id| log_id.index())
                .unwrap_or(0),
        }
    }

    async fn check_version(&self, expected: u64) -> Result<(), ConsensusError> {
        if expected == 0 {
            return Ok(());
        }
        let actual = self.membership_snapshot().version;
        if actual != expected {
            return Err(ConsensusError::VersionMismatch { expected, actual });
        }
        Ok(())
    }

    fn check_leader(&self) -> Result<(), ConsensusError> {
        if self.leader() {
            Ok(())
        } else {
            Err(ConsensusError::NotLeader)
        }
    }

    async fn check_member_addr_open(addr: &str) -> Result<(), ConsensusError> {
        tokio::time::timeout(MEMBER_CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| ConsensusError::Raft(format!("connection test to {addr} timed out")))?
            .map(|_| ())
            .map_err(|err| ConsensusError::Raft(format!("connection test to {addr} failed: {err}")))
    }

    async fn change_membership(
        &self,
        change: ChangeMembers<String, RaftNode>,
        retain: bool,
    ) -> Result<(), ConsensusError> {
        self.raft
            .change_membership(change, retain)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }
}

#[async_trait::async_trait]
impl Consensus for RaftConsensus {
    fn addr(&self) -> String {
        self.addr.clone()
    }

    fn server_id(&self) -> &str {
        &self.id
    }

    fn leader(&self) -> bool {
        self.raft.metrics().borrow_watched().state == ServerState::Leader
    }

    fn set_leader_for_tests(&self, _leader: bool) {}

    fn leader_with_id(&self) -> ServerInfo {
        let metrics = self.raft.metrics().borrow_watched().clone();
        let Some(id) = metrics.current_leader else {
            return ServerInfo {
                id: String::new(),
                addr: String::new(),
                suffrage: ServerSuffrage::Voter,
            };
        };
        let addr = metrics
            .membership_config
            .membership()
            .get_node(&id)
            .map(|node| node.addr.clone())
            .unwrap_or_default();
        ServerInfo {
            id,
            addr,
            suffrage: ServerSuffrage::Voter,
        }
    }

    async fn add_voter(
        &self,
        id: String,
        addr: String,
        version: u64,
    ) -> Result<(), ConsensusError> {
        self.check_leader()?;
        Self::check_member_addr_open(&addr).await?;
        self.check_version(version).await?;
        let mut nodes = BTreeMap::new();
        nodes.insert(id, RaftNode { addr });
        self.change_membership(ChangeMembers::AddVoters(nodes), true)
            .await
    }

    async fn add_non_voter(
        &self,
        id: String,
        addr: String,
        version: u64,
    ) -> Result<(), ConsensusError> {
        self.check_leader()?;
        Self::check_member_addr_open(&addr).await?;
        self.check_version(version).await?;
        let mut nodes = BTreeMap::new();
        nodes.insert(id, RaftNode { addr });
        self.change_membership(ChangeMembers::AddNodes(nodes), true)
            .await
    }

    async fn demote_voter(&self, id: String, version: u64) -> Result<(), ConsensusError> {
        self.check_leader()?;
        self.check_version(version).await?;
        self.change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([id])), true)
            .await
    }

    async fn remove_server(&self, id: String, version: u64) -> Result<(), ConsensusError> {
        self.check_leader()?;
        self.check_version(version).await?;
        let suffrage = self
            .membership_snapshot()
            .servers
            .iter()
            .find(|server| server.id == id)
            .map(|server| server.suffrage);
        let id = BTreeSet::from([id]);
        let change = match suffrage {
            Some(ServerSuffrage::Nonvoter) => ChangeMembers::RemoveNodes(id),
            _ => ChangeMembers::Batch(vec![
                ChangeMembers::RemoveVoters(id.clone()),
                ChangeMembers::RemoveNodes(id),
            ]),
        };
        self.change_membership(change, false).await
    }

    async fn membership(&self) -> Result<ClusterMembership, ConsensusError> {
        Ok(self.membership_snapshot())
    }

    async fn transfer_leader(&self) -> Result<(), ConsensusError> {
        if !self.leader() {
            return Ok(());
        }
        let metrics = self.raft.metrics().borrow_watched().clone();
        let matched_indexes = metrics
            .replication
            .unwrap_or_default()
            .into_iter()
            .map(|(id, log_id)| (id, log_id.map(|log_id| log_id.index())))
            .collect::<BTreeMap<_, _>>();
        let membership = self.membership_snapshot();
        let target = default_transfer_target(&membership, &self.id, &matched_indexes)?;
        self.transfer_leader_to(target.id.clone(), target.addr.clone())
            .await
    }

    async fn transfer_leader_to(&self, id: String, addr: String) -> Result<(), ConsensusError> {
        let membership = self.membership_snapshot();
        let target = membership
            .servers
            .iter()
            .find(|server| server.id == id)
            .ok_or_else(|| ConsensusError::ServerNotFound(id.clone()))?;
        if target.addr != addr {
            return Err(ConsensusError::ServerAddrMismatch {
                id,
                expected: addr,
                actual: target.addr.clone(),
            });
        }
        self.raft
            .trigger()
            .transfer_leader(id)
            .await
            .map_err(raft_err)
    }

    async fn commit_unsafe_payload(&self, payload: PayloadEnvelope) -> Result<(), ConsensusError> {
        payload
            .block_number()
            .map_err(|err| ConsensusError::InvalidPayload(err.to_string()))?;
        payload
            .block_hash()
            .map_err(|err| ConsensusError::InvalidPayload(err.to_string()))?;
        self.raft
            .client_write(RaftCommand::CommitUnsafePayload(payload))
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    async fn latest_unsafe_payload(&self) -> Result<Option<PayloadEnvelope>, ConsensusError> {
        if self.leader() {
            self.raft
                .ensure_linearizable(ReadPolicy::ReadIndex)
                .await
                .map_err(raft_err)?;
        }
        Ok(self.store.latest_payload().await)
    }

    async fn shutdown(&self) -> Result<(), ConsensusError> {
        RaftConsensus::shutdown(self).await
    }
}

#[derive(Debug)]
pub struct MemoryRaftStore {
    path: Option<PathBuf>,
    persist_lock: Mutex<()>,
    last_purged_log_id: RwLock<Option<LogIdOf<TypeConfig>>>,
    committed: RwLock<Option<LogIdOf<TypeConfig>>>,
    log: RwLock<BTreeMap<u64, String>>,
    vote: RwLock<Option<VoteOf<TypeConfig>>>,
    sm: RwLock<UnsafeHeadStateMachine>,
    current_snapshot: RwLock<Option<UnsafeHeadSnapshot>>,
    snapshot_idx: AtomicU64,
}

impl MemoryRaftStore {
    fn new() -> Self {
        Self {
            path: None,
            persist_lock: Mutex::new(()),
            last_purged_log_id: RwLock::new(None),
            committed: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            vote: RwLock::new(None),
            sm: RwLock::new(UnsafeHeadStateMachine::default()),
            current_snapshot: RwLock::new(None),
            snapshot_idx: AtomicU64::new(0),
        }
    }

    async fn open(path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let path = path.as_ref().to_path_buf();
        let persisted = match fs::read(&path).await {
            Ok(data) if !data.is_empty() => {
                serde_json::from_slice::<PersistedRaftStore>(&data).map_err(io_invalid)?
            }
            Ok(_) => PersistedRaftStore::default(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => PersistedRaftStore::default(),
            Err(err) => return Err(err),
        };
        let store = Self {
            path: Some(path),
            persist_lock: Mutex::new(()),
            last_purged_log_id: RwLock::new(persisted.last_purged_log_id),
            committed: RwLock::new(persisted.committed),
            log: RwLock::new(persisted.log),
            vote: RwLock::new(persisted.vote),
            sm: RwLock::new(persisted.sm),
            current_snapshot: RwLock::new(persisted.current_snapshot),
            snapshot_idx: AtomicU64::new(persisted.snapshot_idx),
        };
        store.persist_to_disk().await?;
        Ok(store)
    }

    async fn latest_payload(&self) -> Option<PayloadEnvelope> {
        self.sm.read().await.latest_unsafe_payload.clone()
    }

    async fn persist_to_disk(&self) -> Result<(), io::Error> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let _guard = self.persist_lock.lock().await;
        let persisted = PersistedRaftStore {
            last_purged_log_id: self.last_purged_log_id.read().await.clone(),
            committed: self.committed.read().await.clone(),
            log: self.log.read().await.clone(),
            vote: self.vote.read().await.clone(),
            sm: self.sm.read().await.clone(),
            current_snapshot: self.current_snapshot.read().await.clone(),
            snapshot_idx: self.snapshot_idx.load(Ordering::SeqCst),
        };
        let data = serde_json::to_vec_pretty(&persisted).map_err(io_invalid)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let tmp = path.with_extension("json.tmp");
        {
            let file = fs::File::create(&tmp).await?;
            file.set_len(0).await?;
            fs::write(&tmp, data).await?;
            let file = fs::OpenOptions::new().read(true).open(&tmp).await?;
            file.sync_all().await?;
        }
        fs::rename(&tmp, path).await?;
        if let Some(parent) = path.parent() {
            if let Ok(dir) = fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PersistedRaftStore {
    last_purged_log_id: Option<LogIdOf<TypeConfig>>,
    committed: Option<LogIdOf<TypeConfig>>,
    log: BTreeMap<u64, String>,
    vote: Option<VoteOf<TypeConfig>>,
    sm: UnsafeHeadStateMachine,
    current_snapshot: Option<UnsafeHeadSnapshot>,
    snapshot_idx: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct UnsafeHeadStateMachine {
    last_applied_log: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
    latest_unsafe_payload: Option<PayloadEnvelope>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UnsafeHeadSnapshot {
    meta: SnapshotMetaOf<TypeConfig>,
    data: Vec<u8>,
}

impl RaftLogReader<TypeConfig> for Arc<MemoryRaftStore> {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + fmt::Debug + OptionalSend,
    {
        let log = self.log.read().await;
        log.range(range)
            .map(|(_, serialized)| decode_json(serialized))
            .collect()
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        Ok(self.vote.read().await.clone())
    }
}

impl RaftLogStorage<TypeConfig> for Arc<MemoryRaftStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let log = self.log.read().await;
        let last = log
            .iter()
            .next_back()
            .map(|(_, serialized)| decode_json::<EntryOf<TypeConfig>>(serialized))
            .transpose()?
            .map(|entry| entry.log_id());
        let last_purged = self.last_purged_log_id.read().await.clone();
        Ok(LogState {
            last_purged_log_id: last_purged.clone(),
            last_log_id: last.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        *self.vote.write().await = Some(vote.clone());
        self.persist_to_disk().await?;
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        *self.committed.write().await = committed;
        self.persist_to_disk().await?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        Ok(self.committed.read().await.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut log = self.log.write().await;
            for entry in entries {
                log.insert(entry.index(), encode_json(&entry)?);
            }
        }
        if let Err(err) = self.persist_to_disk().await {
            callback.io_completed(Err(io::Error::new(err.kind(), err.to_string())));
            return Err(err);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        let start = last_log_id.map(|log_id| log_id.index() + 1).unwrap_or(0);
        {
            let mut log = self.log.write().await;
            let keys = log.range(start..).map(|(key, _)| *key).collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }
        self.persist_to_disk().await?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        *self.last_purged_log_id.write().await = Some(log_id.clone());
        {
            let mut log = self.log.write().await;
            let keys = log
                .range(..=log_id.index())
                .map(|(key, _)| *key)
                .collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }
        self.persist_to_disk().await?;
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<MemoryRaftStore> {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig>, io::Error> {
        let sm = self.sm.read().await.clone();
        let data = serde_json::to_vec(&sm).map_err(io_invalid)?;
        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::SeqCst) + 1;
        let last_applied_log = sm.last_applied_log.clone();
        let snapshot_id = sm
            .last_applied_log
            .as_ref()
            .map(|log_id| {
                format!(
                    "{}-{}-{snapshot_idx}",
                    log_id.committed_leader_id(),
                    log_id.index()
                )
            })
            .unwrap_or_else(|| format!("empty-{snapshot_idx}"));
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: last_applied_log,
            last_membership: sm.last_membership,
            snapshot_id,
        };
        *self.current_snapshot.write().await = Some(UnsafeHeadSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        self.persist_to_disk().await?;
        Ok(SnapshotOf::<TypeConfig> {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<MemoryRaftStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log.clone(), sm.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut responses = Vec::new();
        {
            let mut sm = self.sm.write().await;
            while let Some((entry, responder)) = entries.try_next().await? {
                sm.last_applied_log = Some(entry.log_id.clone());
                let response = match entry.payload {
                    EntryPayload::Blank => RaftCommandResponse::default(),
                    EntryPayload::Normal(RaftCommand::CommitUnsafePayload(payload)) => {
                        payload.block_number().map_err(io_invalid)?;
                        payload.block_hash().map_err(io_invalid)?;
                        let previous = sm.latest_unsafe_payload.clone();
                        let should_update = previous
                            .as_ref()
                            .and_then(|payload| payload.block_number().ok())
                            .is_none_or(|previous_number| {
                                payload
                                    .block_number()
                                    .map(|number| number > previous_number)
                                    .unwrap_or(false)
                            });
                        if should_update {
                            sm.latest_unsafe_payload = Some(payload);
                        }
                        RaftCommandResponse { previous }
                    }
                    EntryPayload::Membership(membership) => {
                        sm.last_membership =
                            StoredMembershipOf::<TypeConfig>::new(Some(entry.log_id), membership);
                        RaftCommandResponse::default()
                    }
                };
                responses.push((responder, response));
            }
        }
        self.persist_to_disk().await?;
        for (responder, response) in responses {
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotDataOf<TypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: SnapshotDataOf<TypeConfig>,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let sm = serde_json::from_slice::<UnsafeHeadStateMachine>(&data).map_err(io_invalid)?;
        *self.sm.write().await = sm;
        *self.current_snapshot.write().await = Some(UnsafeHeadSnapshot {
            meta: meta.clone(),
            data,
        });
        self.persist_to_disk().await?;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TypeConfig>>, io::Error> {
        Ok(self
            .current_snapshot
            .read()
            .await
            .as_ref()
            .map(|snapshot| SnapshotOf::<TypeConfig> {
                meta: snapshot.meta.clone(),
                snapshot: Cursor::new(snapshot.data.clone()),
            }))
    }
}

fn encode_json(value: &impl Serialize) -> Result<String, io::Error> {
    serde_json::to_string(value).map_err(io_invalid)
}

fn decode_json<T: for<'de> Deserialize<'de>>(value: &str) -> Result<T, io::Error> {
    serde_json::from_str(value).map_err(io_invalid)
}

fn io_invalid(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

fn raft_err(err: impl std::fmt::Display) -> ConsensusError {
    ConsensusError::Raft(err.to_string())
}

fn map_client_write_error(
    err: RaftError<TypeConfig, ClientWriteError<TypeConfig>>,
) -> ConsensusError {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => ConsensusError::NotLeader,
        other => ConsensusError::Raft(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(number: u64, byte: u8) -> PayloadEnvelope {
        PayloadEnvelope::new(serde_json::json!({
            "executionPayload": {
                "blockHash": format!("0x{}", hex::encode([byte; 32])),
                "blockNumber": format!("0x{number:x}")
            }
        }))
    }

    fn payload_with_hash(number: u64, hash: &str) -> PayloadEnvelope {
        PayloadEnvelope::new(serde_json::json!({
            "executionPayload": {
                "blockHash": hash,
                "blockNumber": format!("0x{number:x}")
            }
        }))
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

    #[test]
    fn default_transfer_target_uses_highest_replication_progress_like_hashicorp_raft() {
        let membership = ClusterMembership {
            version: 7,
            servers: vec![
                ServerInfo {
                    id: "seq-a".to_string(),
                    addr: "127.0.0.1:1001".to_string(),
                    suffrage: ServerSuffrage::Voter,
                },
                ServerInfo {
                    id: "seq-b".to_string(),
                    addr: "127.0.0.1:1002".to_string(),
                    suffrage: ServerSuffrage::Voter,
                },
                ServerInfo {
                    id: "seq-c".to_string(),
                    addr: "127.0.0.1:1003".to_string(),
                    suffrage: ServerSuffrage::Voter,
                },
                ServerInfo {
                    id: "seq-d".to_string(),
                    addr: "127.0.0.1:1004".to_string(),
                    suffrage: ServerSuffrage::Nonvoter,
                },
            ],
        };
        let matched_indexes = BTreeMap::from([
            ("seq-b".to_string(), Some(8)),
            ("seq-c".to_string(), Some(12)),
            ("seq-d".to_string(), Some(99)),
        ]);

        let target = default_transfer_target(&membership, "seq-a", &matched_indexes).unwrap();

        assert_eq!(target.id, "seq-c");
        assert_eq!(target.addr, "127.0.0.1:1003");
    }

    #[test]
    fn default_transfer_target_errors_when_no_replicated_voter_exists_like_hashicorp_raft() {
        let membership = ClusterMembership {
            version: 1,
            servers: vec![ServerInfo {
                id: "seq-a".to_string(),
                addr: "127.0.0.1:1001".to_string(),
                suffrage: ServerSuffrage::Voter,
            }],
        };

        let err = default_transfer_target(&membership, "seq-a", &BTreeMap::new()).unwrap_err();

        assert_eq!(err.to_string(), "raft error: cannot find peer");
    }

    #[tokio::test]
    async fn leader_with_id_reports_empty_fields_before_raft_knows_a_leader() {
        let node =
            RaftConsensus::new_in_memory("seq-a", "127.0.0.1:1001", InMemoryRaftNetwork::new())
                .await
                .unwrap();

        assert_eq!(
            node.leader_with_id(),
            ServerInfo {
                id: String::new(),
                addr: String::new(),
                suffrage: ServerSuffrage::Voter,
            }
        );
    }

    #[tokio::test]
    async fn commit_unsafe_payload_rejects_invalid_hash_before_raft_apply_like_upstream() {
        let nodes = test_cluster().await;
        let leader = nodes
            .iter()
            .find(|node| node.leader())
            .expect("cluster should elect a leader");

        let err = leader
            .commit_unsafe_payload(payload_with_hash(1, "0x1234"))
            .await
            .unwrap_err();

        assert!(matches!(err, ConsensusError::InvalidPayload(_)));
        assert!(leader.latest_unsafe_payload().await.unwrap().is_none());
    }

    async fn http_test_cluster() -> (
        Vec<Arc<RaftConsensus>>,
        Vec<tokio::task::JoinHandle<()>>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut listeners = Vec::new();
        let mut addrs = Vec::new();
        for _ in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(listener.local_addr().unwrap().to_string());
            listeners.push(listener);
        }

        let mut nodes = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let id = format!("seq-{}", index + 1);
            let node = RaftConsensus::new_http(RaftConsensusConfig {
                server_id: id,
                advertised_addr: addrs[index].clone(),
                storage_dir: dir.path().to_path_buf(),
                snapshot_threshold: 3,
                ..Default::default()
            })
            .await
            .unwrap();
            nodes.push((node, listener));
        }

        let mut handles = Vec::new();
        let nodes = nodes
            .into_iter()
            .map(|(node, listener)| {
                let served_node = node.clone();
                handles.push(tokio::spawn(async move {
                    let _ = serve_raft_transport_on_listener(served_node, listener).await;
                }));
                node
            })
            .collect::<Vec<_>>();

        nodes[0]
            .initialize([
                ("seq-1".to_string(), addrs[0].clone()),
                ("seq-2".to_string(), addrs[1].clone()),
                ("seq-3".to_string(), addrs[2].clone()),
            ])
            .await
            .unwrap();
        nodes[0]
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();
        (nodes, handles, dir)
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
    async fn raft_consensus_replicates_unsafe_payload_to_all_nodes() {
        let nodes = test_cluster().await;
        let leader = nodes
            .iter()
            .find(|node| node.leader())
            .expect("cluster should elect a leader");
        let expected = payload(10, 0x10);

        leader
            .commit_unsafe_payload(expected.clone())
            .await
            .unwrap();

        for node in &nodes {
            wait_payload(node, expected.clone()).await;
        }
    }

    #[tokio::test]
    async fn http_raft_transport_replicates_unsafe_payload_to_all_nodes() {
        let (nodes, handles, _dir) = http_test_cluster().await;
        let leader = nodes
            .iter()
            .find(|node| node.leader())
            .expect("cluster should elect a leader");
        let expected = payload(10, 0x10);

        leader
            .commit_unsafe_payload(expected.clone())
            .await
            .unwrap();

        for node in &nodes {
            wait_payload(node, expected.clone()).await;
        }

        for node in &nodes {
            node.shutdown().await.unwrap();
        }
        for handle in handles {
            handle.abort();
        }
    }

    #[tokio::test]
    async fn follower_rejects_unsafe_payload_commit() {
        let nodes = test_cluster().await;
        let follower = nodes
            .iter()
            .find(|node| !node.leader())
            .expect("cluster should have a follower");

        let err = follower
            .commit_unsafe_payload(payload(10, 0x10))
            .await
            .unwrap_err();

        assert!(matches!(err, ConsensusError::NotLeader), "{err:?}");
    }

    #[tokio::test]
    async fn follower_rejects_membership_change_like_hashicorp_raft() {
        let nodes = test_cluster().await;
        let leader = nodes
            .iter()
            .find(|node| node.leader())
            .expect("cluster should elect a leader");
        let follower = nodes
            .iter()
            .find(|node| !node.leader())
            .expect("cluster should have a follower");

        let err = follower
            .remove_server(leader.server_id().to_string(), 0)
            .await
            .unwrap_err();

        assert!(matches!(err, ConsensusError::NotLeader), "{err:?}");
    }

    #[tokio::test]
    async fn raft_consensus_transfers_leader_to_target_voter() {
        let nodes = test_cluster().await;
        let leader = nodes
            .iter()
            .find(|node| node.leader())
            .expect("cluster should elect a leader");

        leader
            .transfer_leader_to("seq-b".to_string(), "127.0.0.1:1002".to_string())
            .await
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if nodes[1].leader() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for seq-b leadership"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn raft_consensus_reports_leader_status_changes_like_upstream_leader_ch() {
        let nodes = test_cluster().await;
        let old_leader = nodes
            .iter()
            .find(|node| node.leader())
            .expect("cluster should elect a leader")
            .clone();
        let target = nodes
            .iter()
            .find(|node| !node.leader())
            .expect("cluster should have a follower")
            .clone();

        let old_leader_watch = tokio::spawn({
            let old_leader = old_leader.clone();
            async move { old_leader.wait_for_leader_status_change(true).await }
        });
        let target_watch = tokio::spawn({
            let target = target.clone();
            async move { target.wait_for_leader_status_change(false).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        old_leader
            .transfer_leader_to(target.server_id().to_string(), target.addr())
            .await
            .unwrap();

        assert!(!old_leader_watch.await.unwrap().unwrap());
        assert!(target_watch.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn raft_consensus_transfer_to_target_rejects_stale_addr_like_upstream() {
        let nodes = test_cluster().await;
        let leader = nodes
            .iter()
            .find(|node| node.leader())
            .expect("cluster should elect a leader");

        let err = leader
            .transfer_leader_to("seq-b".to_string(), "127.0.0.1:9999".to_string())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ConsensusError::ServerAddrMismatch {
                id,
                expected,
                actual
            } if id == "seq-b"
                && expected == "127.0.0.1:9999"
                && actual == "127.0.0.1:1002"
        ));
        assert!(leader.leader());
    }

    #[tokio::test]
    async fn follower_transfer_leader_is_idempotent_noop() {
        let nodes = test_cluster().await;
        let follower = nodes
            .iter()
            .find(|node| !node.leader())
            .expect("cluster should have a follower");

        follower.transfer_leader().await.unwrap();
    }

    #[tokio::test]
    async fn remove_server_removes_nonvoter_like_hashicorp_raft() {
        let nodes = test_cluster().await;
        let leader = nodes
            .iter()
            .find(|node| node.leader())
            .expect("cluster should elect a leader");
        let non_leader = nodes
            .iter()
            .find(|node| !node.leader())
            .expect("cluster should have a follower");
        let target_id = non_leader.server_id().to_string();

        leader.demote_voter(target_id.clone(), 0).await.unwrap();
        let membership = leader.membership().await.unwrap();
        assert_eq!(
            membership
                .servers
                .iter()
                .find(|server| server.id == target_id)
                .map(|server| server.suffrage),
            Some(ServerSuffrage::Nonvoter)
        );

        leader.remove_server(target_id.clone(), 0).await.unwrap();
        let membership = leader.membership().await.unwrap();

        assert!(membership
            .servers
            .iter()
            .all(|server| server.id != target_id));
    }

    #[tokio::test]
    async fn add_voter_rejects_unreachable_member_before_membership_change() {
        let dir = tempfile::tempdir().unwrap();
        let closed_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_addr = closed_listener.local_addr().unwrap();
        drop(closed_listener);
        let node = RaftConsensus::new_http(RaftConsensusConfig {
            server_id: "seq-a".to_string(),
            advertised_addr: "127.0.0.1:1001".to_string(),
            storage_dir: dir.path().to_path_buf(),
            bootstrap: true,
            ..Default::default()
        })
        .await
        .unwrap();
        node.wait_for_leader(Duration::from_secs(5)).await.unwrap();

        let err = node
            .add_voter("seq-b".to_string(), closed_addr.to_string(), 0)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("connection test"));
        assert_eq!(node.membership().await.unwrap().servers.len(), 1);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persistent_raft_store_recovers_latest_payload_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = RaftConsensusConfig {
            server_id: "seq-a".to_string(),
            advertised_addr: "127.0.0.1:1001".to_string(),
            storage_dir: dir.path().to_path_buf(),
            bootstrap: true,
            ..Default::default()
        };
        let expected = payload(42, 0x42);

        let node = RaftConsensus::new_http(config.clone()).await.unwrap();
        assert!(node.bootstrapped());
        node.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        node.commit_unsafe_payload(expected.clone()).await.unwrap();
        wait_payload(&node, expected.clone()).await;
        node.shutdown().await.unwrap();

        let restarted = RaftConsensus::new_http(RaftConsensusConfig {
            bootstrap: true,
            ..config
        })
        .await
        .unwrap();

        assert!(!restarted.bootstrapped());
        assert_eq!(
            restarted.latest_unsafe_payload().await.unwrap(),
            Some(expected)
        );
        restarted.shutdown().await.unwrap();
    }
}
