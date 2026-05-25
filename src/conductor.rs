use crate::{
    consensus::{ClusterMembership, Consensus, ConsensusError, ServerInfo, ServerSuffrage},
    health::{
        ExecutionP2pHealthClient, ExecutionP2pHealthConfig, RollupBoostHealthClient,
        RollupBoostHealthConfig, RollupBoostHealthStatus, SupervisorHealthClient,
        SupervisorHealthConfig,
    },
    metrics::ConductorMetrics,
    sequencer::{SequencerControl, SequencerError},
    types::{Hash, PayloadEnvelope, PeerStats, SyncStatus},
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::{watch, Mutex};

const ERR_SEQUENCER_NOT_HEALTHY: &str = "sequencer is not healthy";
const ERR_SEQUENCER_CONNECTION_DOWN: &str = "cannot connect to sequencer rpc endpoints";
const ERR_SUPERVISOR_CONNECTION_DOWN: &str = "cannot connect to supervisor rpc endpoint";
const ERR_ROLLUP_BOOST_CONNECTION_DOWN: &str = "cannot connect to rollup boost rpc endpoints";
const ERR_ROLLUP_BOOST_PARTIALLY_HEALTHY: &str =
    "rollup boost is partially healthy, meaning that rbuilder is not healthy but the execution client is healthy";
const ERR_ROLLUP_BOOST_NOT_HEALTHY: &str = "rollup boost is not healthy";

#[derive(Clone, Debug)]
pub struct ConductorConfig {
    pub start_paused: bool,
    pub unsafe_repair_depth: u64,
    pub round_robin_leader_transfer: bool,
    pub healthcheck_unsafe_interval: Duration,
    pub healthcheck_safe_enabled: bool,
    pub healthcheck_safe_interval: Duration,
    pub healthcheck_min_peer_count: u64,
    pub execution_p2p_health: Option<ExecutionP2pHealthConfig>,
    pub supervisor_health: Option<SupervisorHealthConfig>,
    pub rollup_boost_health: Option<RollupBoostHealthConfig>,
    pub rollup_boost_partial_health_tolerance: Option<RollupBoostPartialHealthTolerance>,
}

impl Default for ConductorConfig {
    fn default() -> Self {
        Self {
            start_paused: false,
            unsafe_repair_depth: 1,
            round_robin_leader_transfer: false,
            healthcheck_unsafe_interval: Duration::from_secs(60),
            healthcheck_safe_enabled: false,
            healthcheck_safe_interval: Duration::from_secs(1200),
            healthcheck_min_peer_count: 1,
            execution_p2p_health: None,
            supervisor_health: None,
            rollup_boost_health: None,
            rollup_boost_partial_health_tolerance: None,
        }
    }
}

/// Tolerates a bounded number of rollup-boost partial-health responses per interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RollupBoostPartialHealthTolerance {
    pub limit: u64,
    pub interval: Duration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct State {
    pub leader: bool,
    pub healthy: bool,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Health {
    Healthy,
    Unhealthy(String),
    SequencerConnectionDown(String),
    SupervisorConnectionDown(String),
    ExecutionP2pConnectionDown(String),
    RollupBoostConnectionDown(String),
    RollupBoostPartiallyHealthy(String),
}

impl Health {
    fn healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    fn prevents_new_leader_start(&self) -> bool {
        matches!(self, Self::SequencerConnectionDown(_))
    }

    fn prevents_health_recovery_wait(&self) -> bool {
        matches!(
            self,
            Self::SequencerConnectionDown(_) | Self::RollupBoostPartiallyHealthy(_)
        )
    }

    fn metric_error(&self) -> String {
        match self {
            Self::Healthy => String::new(),
            Self::Unhealthy(err) if err == ERR_ROLLUP_BOOST_NOT_HEALTHY => err.clone(),
            Self::Unhealthy(_) => ERR_SEQUENCER_NOT_HEALTHY.to_string(),
            Self::SequencerConnectionDown(_) => ERR_SEQUENCER_CONNECTION_DOWN.to_string(),
            Self::SupervisorConnectionDown(_) => ERR_SUPERVISOR_CONNECTION_DOWN.to_string(),
            Self::ExecutionP2pConnectionDown(err) => err.clone(),
            Self::RollupBoostConnectionDown(_) => ERR_ROLLUP_BOOST_CONNECTION_DOWN.to_string(),
            Self::RollupBoostPartiallyHealthy(_) => ERR_ROLLUP_BOOST_PARTIALLY_HEALTHY.to_string(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConductorError {
    #[error("conductor is stopped")]
    Stopped,
    #[error("no unsafe head")]
    NoUnsafeHead,
    #[error("unsafe head mismatch")]
    UnsafeHeadMismatch,
    #[error("consensus error: {0}")]
    Consensus(#[from] ConsensusError),
    #[error("sequencer error: {0}")]
    Sequencer(#[from] SequencerError),
    #[error("waiting for sequencing to become healthy by itself")]
    WaitingForHealthRecovery,
    #[error("failed to stop sequencer and transfer leadership: stop={stop}; transfer={transfer}")]
    StopAndTransfer { stop: String, transfer: String },
}

#[derive(Debug)]
pub struct Conductor<C, S> {
    consensus: Arc<C>,
    sequencer: Arc<S>,
    cfg: ConductorConfig,
    metrics: Arc<ConductorMetrics>,
    supervisor_health: Option<SupervisorHealthClient>,
    execution_p2p_health: Option<ExecutionP2pHealthClient>,
    rollup_boost_health: Option<RollupBoostHealthClient>,
    leader_override: AtomicBool,
    paused: AtomicBool,
    stopping: AtomicBool,
    stopped: AtomicBool,
    stopped_tx: watch::Sender<bool>,
    action_seq: AtomicU64,
    action_tx: watch::Sender<u64>,
    action_lock: Mutex<()>,
    stop_lock: Mutex<()>,
    healthy: Mutex<Health>,
    seq_active: AtomicBool,
    previous: Mutex<State>,
    rollup_boost_partial_health_counter: Mutex<TimeBoundedCounter>,
}

impl<C, S> Conductor<C, S>
where
    C: Consensus + 'static,
    S: SequencerControl + 'static,
{
    pub fn new(consensus: Arc<C>, sequencer: Arc<S>, cfg: ConductorConfig) -> Arc<Self> {
        let supervisor_health = cfg
            .supervisor_health
            .clone()
            .map(SupervisorHealthClient::new);
        let execution_p2p_health = cfg
            .execution_p2p_health
            .clone()
            .map(ExecutionP2pHealthClient::new);
        let rollup_boost_health = cfg
            .rollup_boost_health
            .clone()
            .map(RollupBoostHealthClient::new);
        let previous = State {
            leader: consensus.leader(),
            healthy: true,
            active: false,
        };
        let (stopped_tx, _) = watch::channel(false);
        let (action_tx, _) = watch::channel(0);
        Arc::new(Self {
            consensus,
            sequencer,
            metrics: Arc::new(ConductorMetrics::new(env!("CARGO_PKG_VERSION"))),
            supervisor_health,
            execution_p2p_health,
            rollup_boost_health,
            paused: AtomicBool::new(cfg.start_paused),
            cfg,
            leader_override: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            stopped_tx,
            action_seq: AtomicU64::new(0),
            action_tx,
            action_lock: Mutex::new(()),
            stop_lock: Mutex::new(()),
            healthy: Mutex::new(Health::Healthy),
            seq_active: AtomicBool::new(false),
            previous: Mutex::new(previous),
            rollup_boost_partial_health_counter: Mutex::new(TimeBoundedCounter::default()),
        })
    }

    pub fn leader(&self) -> bool {
        self.leader_override.load(Ordering::SeqCst) || self.consensus.leader()
    }

    pub fn leader_overridden(&self) -> bool {
        self.leader_override.load(Ordering::SeqCst)
    }

    pub fn set_leader_override(&self, override_leader: bool) {
        self.leader_override
            .store(override_leader, Ordering::SeqCst);
    }

    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    fn stopping_or_stopped(&self) -> bool {
        self.stopping.load(Ordering::SeqCst) || self.stopped()
    }

    pub fn active(&self) -> bool {
        !self.paused() && !self.stopped()
    }

    pub fn metrics(&self) -> Arc<ConductorMetrics> {
        self.metrics.clone()
    }

    pub async fn sequencer_healthy(&self) -> bool {
        self.healthy.lock().await.healthy()
    }

    pub fn leader_with_id(&self) -> ServerInfo {
        if self.leader_overridden() {
            return ServerInfo {
                id: "N/A (Leader overridden)".to_string(),
                addr: "N/A".to_string(),
                suffrage: ServerSuffrage::Voter,
            };
        }
        self.consensus.leader_with_id()
    }

    pub fn consensus_endpoint(&self) -> String {
        self.consensus.addr()
    }

    pub async fn add_server_as_voter(
        &self,
        id: String,
        addr: String,
        version: u64,
    ) -> Result<(), ConductorError> {
        Ok(self.consensus.add_voter(id, addr, version).await?)
    }

    pub async fn add_server_as_nonvoter(
        &self,
        id: String,
        addr: String,
        version: u64,
    ) -> Result<(), ConductorError> {
        Ok(self.consensus.add_non_voter(id, addr, version).await?)
    }

    pub async fn remove_server(&self, id: String, version: u64) -> Result<(), ConductorError> {
        Ok(self.consensus.remove_server(id, version).await?)
    }

    pub async fn demote_voter(&self, id: String, version: u64) -> Result<(), ConductorError> {
        Ok(self.consensus.demote_voter(id, version).await?)
    }

    pub async fn transfer_leader(&self) -> Result<(), ConductorError> {
        Ok(self.consensus.transfer_leader().await?)
    }

    pub async fn transfer_leader_to_server(
        &self,
        id: String,
        addr: String,
    ) -> Result<(), ConductorError> {
        Ok(self.consensus.transfer_leader_to(id, addr).await?)
    }

    pub async fn cluster_membership(&self) -> Result<ClusterMembership, ConductorError> {
        Ok(self.consensus.membership().await?)
    }

    pub async fn pause(&self) -> Result<(), ConductorError> {
        if self.stopped() {
            return Err(ConductorError::Stopped);
        }
        let _guard = self.action_lock.lock().await;
        self.paused.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub async fn resume(&self) -> Result<(), ConductorError> {
        if self.stopped() {
            return Err(ConductorError::Stopped);
        }
        let active = self.sequencer.sequencer_active().await?;
        self.seq_active.store(active, Ordering::SeqCst);
        self.paused.store(false, Ordering::SeqCst);
        self.queue_action();
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), ConductorError> {
        let _guard = self.stop_lock.lock().await;
        if self.stopped() {
            return Ok(());
        }
        self.stopping.store(true, Ordering::SeqCst);
        let _action_guard = self.action_lock.lock().await;
        self.consensus.shutdown().await?;
        self.stopped.store(true, Ordering::SeqCst);
        let _ = self.stopped_tx.send(true);
        Ok(())
    }

    pub async fn wait_stopped(&self) {
        let mut stopped_rx = self.stopped_tx.subscribe();
        loop {
            if *stopped_rx.borrow() {
                return;
            }
            if stopped_rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn queue_action(&self) {
        let next = self.action_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.action_tx.send(next);
    }

    pub fn subscribe_actions(&self) -> watch::Receiver<u64> {
        self.action_tx.subscribe()
    }

    pub async fn update_leader(&self, leader: bool) -> Result<(), ConductorError> {
        self.consensus.set_leader_for_tests(leader);
        self.action().await
    }

    pub async fn update_health(&self, health: Health) -> Result<(), ConductorError> {
        *self.healthy.lock().await = health;
        self.action().await
    }

    pub async fn refresh_sequencer_active(&self) -> Result<(), ConductorError> {
        let active = self.sequencer.sequencer_active().await?;
        self.seq_active.store(active, Ordering::SeqCst);
        Ok(())
    }

    /// Initializes startup state from the attached sequencer before the first control action.
    pub async fn initialize_startup_state(&self) -> Result<(), ConductorError> {
        self.refresh_sequencer_active().await?;
        self.seed_previous_state_from_current().await;
        Ok(())
    }

    async fn seed_previous_state_from_current(&self) {
        let health = self.healthy.lock().await.clone();
        *self.previous.lock().await = State {
            leader: self.consensus.leader(),
            healthy: health.healthy(),
            active: self.seq_active.load(Ordering::SeqCst),
        };
    }

    pub async fn tick(&self) -> Result<(), ConductorError> {
        let started = Instant::now();
        if self.stopping_or_stopped() {
            self.metrics.record_loop_execution_time(started.elapsed());
            return Ok(());
        }
        let result = match self.refresh_sequencer_active().await {
            Ok(()) => {
                let health = self.check_health_at(current_unix_time()).await;
                self.metrics
                    .record_health_check(health.healthy(), health.metric_error());
                self.update_health(health).await
            }
            Err(err) => {
                self.metrics
                    .record_health_check(false, ERR_SEQUENCER_CONNECTION_DOWN);
                self.update_health(Health::SequencerConnectionDown(err.to_string()))
                    .await
            }
        };
        self.metrics.record_loop_execution_time(started.elapsed());
        result
    }

    pub async fn check_health_at(&self, now: u64) -> Health {
        let status = match self.sequencer.sync_status().await {
            Ok(status) => status,
            Err(err) => return Health::SequencerConnectionDown(format!("sync status: {err}")),
        };

        if let Some(health) = self.check_supervisor().await {
            return health;
        }

        if let Some(health) = self.check_sync_status(now, &status) {
            return health;
        }

        let stats = match self.sequencer.peer_stats().await {
            Ok(stats) => stats,
            Err(err) => return Health::SequencerConnectionDown(format!("peer stats: {err}")),
        };

        if let Some(health) = self.check_peer_stats(&stats) {
            return health;
        }

        if let Some(health) = self.check_execution_p2p().await {
            return health;
        }

        self.check_rollup_boost(now)
            .await
            .unwrap_or(Health::Healthy)
    }

    pub async fn commit_unsafe_payload(
        &self,
        payload: PayloadEnvelope,
    ) -> Result<(), ConductorError> {
        self.consensus.commit_unsafe_payload(payload).await?;
        Ok(())
    }

    pub async fn latest_unsafe_payload(&self) -> Result<Option<PayloadEnvelope>, ConductorError> {
        Ok(self.consensus.latest_unsafe_payload().await?)
    }

    pub async fn action(&self) -> Result<(), ConductorError> {
        let _guard = self.action_lock.lock().await;
        self.action_locked().await
    }

    async fn action_locked(&self) -> Result<(), ConductorError> {
        if self.paused() || self.stopping_or_stopped() {
            return Ok(());
        }

        let health = self.healthy.lock().await.clone();
        let status = State {
            leader: self.consensus.leader(),
            healthy: health.healthy(),
            active: self.seq_active.load(Ordering::SeqCst),
        };

        let result = match status {
            State {
                leader: false,
                healthy: _,
                active: true,
            } => self.stop_sequencer().await,
            State {
                leader: true,
                healthy: true,
                active: false,
            } => self.start_sequencer().await,
            State {
                leader: true,
                healthy: false,
                active: false,
            } => {
                let prev = *self.previous.lock().await;
                if !prev.leader && !prev.active && !health.prevents_new_leader_start() {
                    match self.start_sequencer().await {
                        Ok(()) => Ok(()),
                        Err(_) => self.transfer_leader_internal().await,
                    }
                } else {
                    self.transfer_leader_internal().await
                }
            }
            State {
                leader: true,
                healthy: false,
                active: true,
            } => {
                if self.should_wait_for_health_recovery(&health).await {
                    return Err(ConductorError::WaitingForHealthRecovery);
                }
                self.stop_and_transfer_leader().await
            }
            _ => Ok(()),
        };

        if result.is_ok() {
            let mut previous = self.previous.lock().await;
            if *previous != status {
                self.metrics
                    .record_state_change(status.leader, status.healthy, status.active);
                *previous = status;
            }
        }
        result
    }

    async fn should_wait_for_health_recovery(&self, health: &Health) -> bool {
        let prev = *self.previous.lock().await;
        prev.leader && !prev.healthy && !prev.active && !health.prevents_health_recovery_wait()
    }

    async fn stop_and_transfer_leader(&self) -> Result<(), ConductorError> {
        let stop_result = self.stop_sequencer().await;
        let transfer_result = self.transfer_leader_internal().await;

        match (stop_result, transfer_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
            (Err(stop), Err(transfer)) => Err(ConductorError::StopAndTransfer {
                stop: stop.to_string(),
                transfer: transfer.to_string(),
            }),
        }
    }

    async fn stop_sequencer(&self) -> Result<(), ConductorError> {
        let mut already_stopped = false;
        let result = match self.sequencer.stop_sequencer().await {
            Ok(_) => {
                self.seq_active.store(false, Ordering::SeqCst);
                Ok(())
            }
            Err(err) if err.is_sequencer_already_stopped() => {
                already_stopped = true;
                self.seq_active.store(false, Ordering::SeqCst);
                Ok(())
            }
            Err(err) => Err(err.into()),
        };
        self.metrics
            .record_stop_sequencer(result.is_ok() && !already_stopped);
        result
    }

    async fn start_sequencer(&self) -> Result<(), ConductorError> {
        let mut already_started = false;
        let result = async {
            let unsafe_payload = self
                .consensus
                .latest_unsafe_payload()
                .await?
                .ok_or(ConductorError::NoUnsafeHead)?;
            let consensus_hash = unsafe_payload
                .block_hash()
                .map_err(|_| ConductorError::UnsafeHeadMismatch)?;
            let consensus_number = unsafe_payload
                .block_number()
                .map_err(|_| ConductorError::UnsafeHeadMismatch)?;
            let node_head = self.sequencer.latest_unsafe_block().await?;

            if node_head.hash != consensus_hash {
                if consensus_number > node_head.number
                    && consensus_number - node_head.number <= self.cfg.unsafe_repair_depth
                {
                    self.sequencer.post_unsafe_payload(&unsafe_payload).await?;
                } else {
                    return Err(ConductorError::UnsafeHeadMismatch);
                }
            }

            match self.sequencer.start_sequencer(consensus_hash).await {
                Ok(()) => {}
                Err(err) if err.is_sequencer_already_started() => {
                    already_started = true;
                }
                Err(err) => return Err(err.into()),
            }
            self.seq_active.store(true, Ordering::SeqCst);
            Ok(())
        }
        .await;
        self.metrics
            .record_start_sequencer(result.is_ok() && !already_started);
        result
    }

    async fn transfer_leader_internal(&self) -> Result<(), ConductorError> {
        let was_consensus_leader = self.consensus.leader();
        let result = async {
            if !was_consensus_leader {
                return Ok(());
            }
            if self.cfg.round_robin_leader_transfer {
                self.transfer_leader_round_robin().await?;
            } else {
                self.consensus.transfer_leader().await?;
            }
            self.consensus.set_leader_for_tests(false);
            Ok(())
        }
        .await;
        self.metrics
            .record_leader_transfer(result.is_ok() && was_consensus_leader);
        result
    }

    async fn transfer_leader_round_robin(&self) -> Result<(), ConductorError> {
        let membership = self.consensus.membership().await?;
        let mut voters = membership
            .servers
            .into_iter()
            .filter(|server| server.suffrage == crate::consensus::ServerSuffrage::Voter)
            .collect::<Vec<_>>();
        if voters.len() <= 1 {
            return Ok(());
        }
        voters.sort_by(|left, right| left.id.cmp(&right.id));
        let current = voters
            .iter()
            .position(|server| server.id == self.consensus.server_id())
            .ok_or_else(|| {
                ConsensusError::ServerNotFound(self.consensus.server_id().to_string())
            })?;

        let mut last_err = None;
        for offset in 1..voters.len() {
            let target = &voters[(current + offset) % voters.len()];
            match self
                .consensus
                .transfer_leader_to(target.id.clone(), target.addr.clone())
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) if err.is_leadership_transfer_in_progress() => return Ok(()),
                Err(err) => last_err = Some(err),
            }
        }

        if last_err.is_some() {
            self.consensus.transfer_leader().await?;
        }
        Ok(())
    }

    fn check_sync_status(&self, now: u64, status: &SyncStatus) -> Option<Health> {
        let unsafe_lag = saturating_time_diff(now, status.unsafe_l2.time);
        if unsafe_lag > self.cfg.healthcheck_unsafe_interval.as_secs() {
            return Some(Health::Unhealthy(format!(
                "unsafe head is {unsafe_lag}s behind, above {}s interval",
                self.cfg.healthcheck_unsafe_interval.as_secs()
            )));
        }

        if self.cfg.healthcheck_safe_enabled {
            let safe_lag = saturating_time_diff(now, status.safe_l2.time);
            if safe_lag > self.cfg.healthcheck_safe_interval.as_secs() {
                return Some(Health::Unhealthy(format!(
                    "safe head is {safe_lag}s behind, above {}s interval",
                    self.cfg.healthcheck_safe_interval.as_secs()
                )));
            }
        }

        None
    }

    fn check_peer_stats(&self, stats: &PeerStats) -> Option<Health> {
        if stats.connected < self.cfg.healthcheck_min_peer_count {
            return Some(Health::Unhealthy(format!(
                "peer count {} below minimum {}",
                stats.connected, self.cfg.healthcheck_min_peer_count
            )));
        }
        None
    }

    async fn check_supervisor(&self) -> Option<Health> {
        let Some(client) = &self.supervisor_health else {
            return None;
        };
        client
            .check()
            .await
            .err()
            .map(|err| Health::SupervisorConnectionDown(format!("supervisor sync status: {err}")))
    }

    async fn check_execution_p2p(&self) -> Option<Health> {
        let Some(client) = &self.execution_p2p_health else {
            return None;
        };
        let min_peer_count = self
            .cfg
            .execution_p2p_health
            .as_ref()
            .map(|config| config.min_peer_count)
            .unwrap_or_default();
        match client.peer_count().await {
            Ok(peer_count) if peer_count < min_peer_count => Some(Health::Unhealthy(format!(
                "execution p2p peer count {peer_count} below minimum {min_peer_count}"
            ))),
            Ok(_) => None,
            Err(err) => Some(Health::ExecutionP2pConnectionDown(format!(
                "execution p2p peer count: {err}"
            ))),
        }
    }

    async fn check_rollup_boost(&self, now: u64) -> Option<Health> {
        let Some(client) = &self.rollup_boost_health else {
            return None;
        };
        match client.check().await {
            Ok(RollupBoostHealthStatus::Healthy) => None,
            Ok(RollupBoostHealthStatus::Partial) => {
                if self.tolerate_rollup_boost_partial_health(now).await {
                    None
                } else {
                    Some(Health::RollupBoostPartiallyHealthy(
                        "rollup boost is partially healthy".to_string(),
                    ))
                }
            }
            Ok(RollupBoostHealthStatus::Unhealthy) => {
                Some(Health::Unhealthy(ERR_ROLLUP_BOOST_NOT_HEALTHY.to_string()))
            }
            Err(err) => Some(Health::RollupBoostConnectionDown(format!(
                "rollup boost: {err}"
            ))),
        }
    }

    async fn tolerate_rollup_boost_partial_health(&self, now: u64) -> bool {
        let Some(tolerance) = self.cfg.rollup_boost_partial_health_tolerance else {
            return false;
        };
        let interval_secs = tolerance.interval.as_secs();
        if tolerance.limit == 0 || interval_secs == 0 {
            return false;
        }

        let mut counter = self.rollup_boost_partial_health_counter.lock().await;
        if counter.current_value(now, interval_secs) >= tolerance.limit {
            return false;
        }
        counter.increment(now, interval_secs);
        true
    }

    pub fn set_seq_active_for_tests(&self, active: bool) {
        self.seq_active.store(active, Ordering::SeqCst);
    }

    pub async fn set_previous_for_tests(&self, previous: State) {
        *self.previous.lock().await = previous;
    }

    pub async fn expected_start_hash(&self) -> Result<Hash, ConductorError> {
        let payload = self
            .latest_unsafe_payload()
            .await?
            .ok_or(ConductorError::NoUnsafeHead)?;
        payload
            .block_hash()
            .map_err(|_| ConductorError::UnsafeHeadMismatch)
    }
}

fn current_unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn saturating_time_diff(now: u64, then: u64) -> u64 {
    now.saturating_sub(then)
}

#[derive(Debug, Default)]
struct TimeBoundedCounter {
    buckets: BTreeMap<u64, u64>,
}

impl TimeBoundedCounter {
    fn current_value(&self, now: u64, interval_secs: u64) -> u64 {
        self.buckets
            .get(&Self::bucket(now, interval_secs))
            .copied()
            .unwrap_or_default()
    }

    fn increment(&mut self, now: u64, interval_secs: u64) -> u64 {
        let bucket = Self::bucket(now, interval_secs);
        let value = self.buckets.entry(bucket).or_default();
        *value += 1;
        let latest = *value;
        if self.buckets.len() > 1000 {
            self.buckets.clear();
            self.buckets.insert(bucket, latest);
        }
        latest
    }

    fn bucket(now: u64, interval_secs: u64) -> u64 {
        now / interval_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        consensus::LocalConsensus,
        health::{
            ExecutionP2pCheckApi, ExecutionP2pHealthConfig, RollupBoostHealthConfig,
            SupervisorHealthConfig,
        },
        rpc::RpcClientError,
        store::{FilePayloadStore, PayloadStore},
        types::{BlockInfo, L2BlockRef},
    };
    use axum::{
        extract::State as AxumState,
        http::StatusCode,
        routing::{get, post},
        Json, Router,
    };
    use serde_json::Value;
    use std::{
        collections::VecDeque,
        sync::{atomic::AtomicU64, Mutex as StdMutex},
    };
    use tokio::{net::TcpListener, sync::Notify};
    use url::Url;

    #[derive(Debug)]
    struct StartGate {
        entered: Notify,
        release: Notify,
    }

    #[derive(Debug, Default)]
    struct FakeSequencer {
        latest: StdMutex<Option<BlockInfo>>,
        starts: StdMutex<Vec<Hash>>,
        stops: StdMutex<u64>,
        posts: StdMutex<u64>,
        active_checks: AtomicU64,
        sync_status_checks: AtomicU64,
        sync_status: StdMutex<Option<SyncStatus>>,
        sync_status_down: AtomicBool,
        peer_stats: StdMutex<Option<PeerStats>>,
        peer_stats_down: AtomicBool,
        active: AtomicBool,
        start_error: StdMutex<Option<&'static str>>,
        stop_error: StdMutex<Option<&'static str>>,
        post_error: StdMutex<Option<&'static str>>,
        post_leaves_latest_stale: AtomicBool,
        start_delay: StdMutex<Option<Duration>>,
        start_gate: StdMutex<Option<Arc<StartGate>>>,
    }

    #[async_trait::async_trait]
    impl SequencerControl for FakeSequencer {
        async fn latest_unsafe_block(&self) -> Result<BlockInfo, SequencerError> {
            Ok(self.latest.lock().unwrap().clone().unwrap())
        }

        async fn start_sequencer(&self, expected_hash: Hash) -> Result<(), SequencerError> {
            if let Some(message) = *self.start_error.lock().unwrap() {
                return Err(json_rpc_error(message));
            }
            let start_delay = *self.start_delay.lock().unwrap();
            if let Some(delay) = start_delay {
                tokio::time::sleep(delay).await;
            }
            let start_gate = self.start_gate.lock().unwrap().clone();
            if let Some(gate) = start_gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            let latest = self.latest.lock().unwrap().clone().unwrap();
            if latest.hash != expected_hash {
                return Err(SequencerError::UnsafeHeadMismatch {
                    expected: expected_hash,
                    actual: latest.hash,
                    number: latest.number,
                });
            }
            self.starts.lock().unwrap().push(expected_hash);
            self.active.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn stop_sequencer(&self) -> Result<Hash, SequencerError> {
            if let Some(message) = *self.stop_error.lock().unwrap() {
                return Err(json_rpc_error(message));
            }
            *self.stops.lock().unwrap() += 1;
            self.active.store(false, Ordering::SeqCst);
            Ok(Hash::ZERO)
        }

        async fn sequencer_active(&self) -> Result<bool, SequencerError> {
            self.active_checks.fetch_add(1, Ordering::SeqCst);
            Ok(self.active.load(Ordering::SeqCst))
        }

        async fn sync_status(&self) -> Result<SyncStatus, SequencerError> {
            self.sync_status_checks.fetch_add(1, Ordering::SeqCst);
            if self.sync_status_down.load(Ordering::SeqCst) {
                return Err(rpc_error("sync status down"));
            }
            Ok(self
                .sync_status
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| healthy_sync_status(current_unix_time())))
        }

        async fn peer_stats(&self) -> Result<PeerStats, SequencerError> {
            if self.peer_stats_down.load(Ordering::SeqCst) {
                return Err(rpc_error("peer stats down"));
            }
            Ok(self
                .peer_stats
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(PeerStats { connected: 1 }))
        }

        async fn post_unsafe_payload(
            &self,
            payload: &PayloadEnvelope,
        ) -> Result<(), SequencerError> {
            if let Some(message) = *self.post_error.lock().unwrap() {
                return Err(rpc_error(message));
            }
            *self.posts.lock().unwrap() += 1;
            if !self.post_leaves_latest_stale.load(Ordering::SeqCst) {
                *self.latest.lock().unwrap() = Some(BlockInfo {
                    hash: payload.block_hash().unwrap(),
                    number: payload.block_number().unwrap(),
                });
            }
            Ok(())
        }

        async fn conductor_enabled(&self) -> Result<bool, SequencerError> {
            Ok(true)
        }
    }

    #[derive(Debug)]
    struct RecordingConsensus {
        server_id: String,
        addr: String,
        leader: AtomicBool,
        membership: Vec<ServerInfo>,
        transfers: StdMutex<Vec<String>>,
        transfer_errors: StdMutex<VecDeque<&'static str>>,
        latest_payload: StdMutex<Option<PayloadEnvelope>>,
        shutdowns: AtomicU64,
    }

    impl RecordingConsensus {
        fn new(server_id: &str, membership: Vec<ServerInfo>) -> Self {
            Self {
                server_id: server_id.to_string(),
                addr: "127.0.0.1:0".to_string(),
                leader: AtomicBool::new(true),
                membership,
                transfers: StdMutex::new(Vec::new()),
                transfer_errors: StdMutex::new(VecDeque::new()),
                latest_payload: StdMutex::new(None),
                shutdowns: AtomicU64::new(0),
            }
        }

        fn with_transfer_errors(self, errors: Vec<&'static str>) -> Self {
            *self.transfer_errors.lock().unwrap() = errors.into();
            self
        }

        fn with_latest_payload(self, payload: PayloadEnvelope) -> Self {
            *self.latest_payload.lock().unwrap() = Some(payload);
            self
        }
    }

    #[async_trait::async_trait]
    impl Consensus for RecordingConsensus {
        fn addr(&self) -> String {
            self.addr.clone()
        }

        fn server_id(&self) -> &str {
            &self.server_id
        }

        fn leader(&self) -> bool {
            self.leader.load(Ordering::SeqCst)
        }

        fn set_leader_for_tests(&self, leader: bool) {
            self.leader.store(leader, Ordering::SeqCst);
        }

        fn leader_with_id(&self) -> ServerInfo {
            self.membership
                .iter()
                .find(|server| server.id == self.server_id)
                .cloned()
                .unwrap_or_else(|| ServerInfo {
                    id: self.server_id.clone(),
                    addr: self.addr.clone(),
                    suffrage: ServerSuffrage::Voter,
                })
        }

        async fn add_voter(
            &self,
            _id: String,
            _addr: String,
            _version: u64,
        ) -> Result<(), ConsensusError> {
            Ok(())
        }

        async fn add_non_voter(
            &self,
            _id: String,
            _addr: String,
            _version: u64,
        ) -> Result<(), ConsensusError> {
            Ok(())
        }

        async fn demote_voter(&self, _id: String, _version: u64) -> Result<(), ConsensusError> {
            Ok(())
        }

        async fn remove_server(&self, _id: String, _version: u64) -> Result<(), ConsensusError> {
            Ok(())
        }

        async fn membership(&self) -> Result<ClusterMembership, ConsensusError> {
            Ok(ClusterMembership {
                servers: self.membership.clone(),
                version: 1,
            })
        }

        async fn transfer_leader(&self) -> Result<(), ConsensusError> {
            self.transfers.lock().unwrap().push("fallback".to_string());
            if let Some(message) = self.transfer_errors.lock().unwrap().pop_front() {
                return Err(ConsensusError::Raft(message.to_string()));
            }
            Ok(())
        }

        async fn transfer_leader_to(
            &self,
            id: String,
            _addr: String,
        ) -> Result<(), ConsensusError> {
            self.transfers.lock().unwrap().push(id);
            if let Some(message) = self.transfer_errors.lock().unwrap().pop_front() {
                return Err(ConsensusError::Raft(message.to_string()));
            }
            Ok(())
        }

        async fn commit_unsafe_payload(
            &self,
            _payload: PayloadEnvelope,
        ) -> Result<(), ConsensusError> {
            Ok(())
        }

        async fn latest_unsafe_payload(&self) -> Result<Option<PayloadEnvelope>, ConsensusError> {
            Ok(self.latest_payload.lock().unwrap().clone())
        }

        async fn shutdown(&self) -> Result<(), ConsensusError> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
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

    fn rpc_error(message: &str) -> SequencerError {
        SequencerError::Rpc(RpcClientError::InvalidResponse(message.to_string()))
    }

    fn json_rpc_error(message: &str) -> SequencerError {
        SequencerError::Rpc(RpcClientError::JsonRpc {
            code: -32000,
            message: message.to_string(),
            data: None,
        })
    }

    fn healthy_sync_status(now: u64) -> SyncStatus {
        SyncStatus {
            unsafe_l2: L2BlockRef {
                hash: Some(hash(0x10)),
                number: 10,
                time: now,
            },
            safe_l2: L2BlockRef {
                hash: Some(hash(0x0f)),
                number: 9,
                time: now,
            },
        }
    }

    async fn test_conductor() -> (
        Arc<Conductor<LocalConsensus<FilePayloadStore>, FakeSequencer>>,
        Arc<FakeSequencer>,
    ) {
        test_conductor_with_config(ConductorConfig::default()).await
    }

    async fn test_conductor_with_config(
        cfg: ConductorConfig,
    ) -> (
        Arc<Conductor<LocalConsensus<FilePayloadStore>, FakeSequencer>>,
        Arc<FakeSequencer>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        store.commit(payload(10, 0x10)).await.unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeSequencer::default());
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x10),
            number: 10,
        });
        let conductor = Conductor::new(consensus, sequencer.clone(), cfg);
        (conductor, sequencer)
    }

    async fn rollup_boost_server(status: StatusCode) -> Url {
        async fn handler(AxumState(status): AxumState<StatusCode>) -> StatusCode {
            status
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let app = Router::new()
            .route("/healthz", get(handler))
            .with_state(status);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        url
    }

    async fn supervisor_server(response: Value) -> Url {
        async fn handler(AxumState(response): AxumState<Value>) -> Json<Value> {
            Json(response)
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse::<Url>()
            .unwrap();
        let app = Router::new().route("/", post(handler)).with_state(response);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        url
    }

    #[tokio::test]
    async fn healthy_leader_starts_with_consensus_hash() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.update_leader(true).await.unwrap();

        let starts = sequencer.starts.lock().unwrap();
        assert_eq!(starts.as_slice(), &[hash(0x10)]);
    }

    #[tokio::test]
    async fn concurrent_actions_start_sequencer_once_like_upstream_loop() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.consensus.set_leader_for_tests(true);
        *sequencer.start_delay.lock().unwrap() = Some(Duration::from_millis(50));

        let left = conductor.clone();
        let right = conductor.clone();
        let (left_result, right_result) = tokio::join!(left.action(), right.action());

        left_result.unwrap();
        right_result.unwrap();
        assert_eq!(sequencer.starts.lock().unwrap().as_slice(), &[hash(0x10)]);
    }

    #[tokio::test]
    async fn leader_with_id_matches_upstream_override_sentinel() {
        let (conductor, _) = test_conductor().await;
        conductor.set_leader_override(true);

        assert_eq!(
            conductor.leader_with_id(),
            ServerInfo {
                id: "N/A (Leader overridden)".to_string(),
                addr: "N/A".to_string(),
                suffrage: ServerSuffrage::Voter,
            }
        );
    }

    #[tokio::test]
    async fn leader_override_only_changes_reported_leader_like_upstream() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.update_leader(false).await.unwrap();
        conductor.set_leader_override(true);

        conductor.action().await.unwrap();

        assert!(conductor.leader());
        assert!(conductor.leader_overridden());
        assert!(sequencer.starts.lock().unwrap().is_empty());
        assert!(!conductor.seq_active.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn follower_stops_active_sequencer() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.set_seq_active_for_tests(true);
        conductor.update_leader(false).await.unwrap();

        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn startup_initialization_seeds_previous_state_like_upstream_start() {
        let consensus = Arc::new(RecordingConsensus::new("seq-a", vec![]));
        consensus.set_leader_for_tests(false);
        let sequencer = Arc::new(FakeSequencer::default());
        sequencer.active.store(true, Ordering::SeqCst);
        let conductor = Conductor::new(consensus, sequencer.clone(), ConductorConfig::default());

        conductor.initialize_startup_state().await.unwrap();
        conductor.action().await.unwrap();

        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
        assert_eq!(
            *conductor.previous.lock().await,
            State {
                leader: false,
                healthy: true,
                active: true,
            }
        );
        assert!(!conductor
            .metrics()
            .render_prometheus()
            .contains("op_conductor_state_changes_count{leader="));
    }

    #[tokio::test]
    async fn stop_shuts_down_consensus_once_like_upstream() {
        let consensus = Arc::new(RecordingConsensus::new("seq-a", vec![]));
        let conductor = Conductor::new(
            consensus.clone(),
            Arc::new(FakeSequencer::default()),
            ConductorConfig::default(),
        );

        conductor.stop().await.unwrap();
        conductor.stop().await.unwrap();

        assert!(conductor.stopped());
        assert_eq!(consensus.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stop_waits_for_inflight_action_before_consensus_shutdown() {
        let consensus = Arc::new(
            RecordingConsensus::new("seq-a", vec![]).with_latest_payload(payload(10, 0x10)),
        );
        let sequencer = Arc::new(FakeSequencer::default());
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x10),
            number: 10,
        });
        let conductor = Conductor::new(
            consensus.clone(),
            sequencer.clone(),
            ConductorConfig::default(),
        );
        let gate = Arc::new(StartGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *sequencer.start_gate.lock().unwrap() = Some(gate.clone());

        let action = tokio::spawn({
            let conductor = conductor.clone();
            async move { conductor.action().await }
        });
        gate.entered.notified().await;

        let stop = tokio::spawn({
            let conductor = conductor.clone();
            async move { conductor.stop().await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(consensus.shutdowns.load(Ordering::SeqCst), 0);
        assert!(conductor.active());

        gate.release.notify_one();
        action.await.unwrap().unwrap();
        stop.await.unwrap().unwrap();

        assert_eq!(consensus.shutdowns.load(Ordering::SeqCst), 1);
        assert!(!conductor.active());
    }

    #[tokio::test]
    async fn tick_is_noop_while_stop_waits_for_inflight_action_like_upstream() {
        let consensus = Arc::new(
            RecordingConsensus::new("seq-a", vec![]).with_latest_payload(payload(10, 0x10)),
        );
        let sequencer = Arc::new(FakeSequencer::default());
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x10),
            number: 10,
        });
        let conductor = Conductor::new(consensus, sequencer.clone(), ConductorConfig::default());
        let gate = Arc::new(StartGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *sequencer.start_gate.lock().unwrap() = Some(gate.clone());

        let action = tokio::spawn({
            let conductor = conductor.clone();
            async move { conductor.action().await }
        });
        gate.entered.notified().await;

        let stop = tokio::spawn({
            let conductor = conductor.clone();
            async move { conductor.stop().await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        conductor.tick().await.unwrap();

        assert_eq!(sequencer.active_checks.load(Ordering::SeqCst), 0);
        assert_eq!(sequencer.sync_status_checks.load(Ordering::SeqCst), 0);

        gate.release.notify_one();
        action.await.unwrap().unwrap();
        stop.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn pause_waits_for_inflight_action_like_upstream() {
        let consensus = Arc::new(
            RecordingConsensus::new("seq-a", vec![]).with_latest_payload(payload(10, 0x10)),
        );
        let sequencer = Arc::new(FakeSequencer::default());
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x10),
            number: 10,
        });
        let conductor = Conductor::new(consensus, sequencer.clone(), ConductorConfig::default());
        let gate = Arc::new(StartGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *sequencer.start_gate.lock().unwrap() = Some(gate.clone());

        let action = tokio::spawn({
            let conductor = conductor.clone();
            async move { conductor.action().await }
        });
        gate.entered.notified().await;

        let pause = tokio::spawn({
            let conductor = conductor.clone();
            async move { conductor.pause().await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(!conductor.paused());
        assert!(!pause.is_finished());

        gate.release.notify_one();
        action.await.unwrap().unwrap();
        pause.await.unwrap().unwrap();

        assert!(conductor.paused());
        assert_eq!(sequencer.starts.lock().unwrap().as_slice(), &[hash(0x10)]);
    }

    #[tokio::test]
    async fn already_started_error_is_treated_as_reconciled_state() {
        let (conductor, sequencer) = test_conductor().await;
        *sequencer.start_error.lock().unwrap() = Some("sequencer already running");

        conductor.update_leader(true).await.unwrap();

        assert!(conductor.seq_active.load(Ordering::SeqCst));
        assert!(sequencer.starts.lock().unwrap().is_empty());
        assert!(conductor
            .metrics()
            .render_prometheus()
            .contains("op_conductor_sequencer_starts_count{success=\"false\"} 1"));
    }

    #[tokio::test]
    async fn already_stopped_error_is_treated_as_reconciled_state() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.set_seq_active_for_tests(true);
        *sequencer.stop_error.lock().unwrap() = Some("sequencer not running");

        conductor.update_leader(false).await.unwrap();

        assert!(!conductor.seq_active.load(Ordering::SeqCst));
        assert_eq!(*sequencer.stops.lock().unwrap(), 0);
        assert!(conductor
            .metrics()
            .render_prometheus()
            .contains("op_conductor_sequencer_stops_count{success=\"false\"} 1"));
    }

    #[tokio::test]
    async fn posts_one_block_behind_payload_before_starting() {
        let (conductor, sequencer) = test_conductor().await;
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x09),
            number: 9,
        });

        conductor.update_leader(true).await.unwrap();

        assert_eq!(*sequencer.posts.lock().unwrap(), 1);
        assert_eq!(sequencer.starts.lock().unwrap().as_slice(), &[hash(0x10)]);
    }

    #[tokio::test]
    async fn failed_unsafe_payload_repair_retries_without_starting_like_upstream() {
        let (conductor, sequencer) = test_conductor().await;
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x09),
            number: 9,
        });
        *sequencer.post_error.lock().unwrap() = Some("temporary post failure");

        conductor.update_leader(true).await.unwrap_err();

        assert_eq!(*sequencer.posts.lock().unwrap(), 0);
        assert!(sequencer.starts.lock().unwrap().is_empty());
        assert!(!conductor.seq_active.load(Ordering::SeqCst));
        assert_eq!(
            *conductor.previous.lock().await,
            State {
                leader: true,
                healthy: true,
                active: false,
            }
        );

        *sequencer.post_error.lock().unwrap() = None;
        conductor.action().await.unwrap();

        assert_eq!(*sequencer.posts.lock().unwrap(), 1);
        assert_eq!(sequencer.starts.lock().unwrap().as_slice(), &[hash(0x10)]);
        assert!(conductor.seq_active.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn delayed_unsafe_payload_repair_retries_until_kona_head_matches() {
        let (conductor, sequencer) = test_conductor().await;
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x09),
            number: 9,
        });
        sequencer
            .post_leaves_latest_stale
            .store(true, Ordering::SeqCst);

        conductor.update_leader(true).await.unwrap_err();

        assert_eq!(*sequencer.posts.lock().unwrap(), 1);
        assert!(sequencer.starts.lock().unwrap().is_empty());
        assert!(!conductor.seq_active.load(Ordering::SeqCst));

        sequencer
            .post_leaves_latest_stale
            .store(false, Ordering::SeqCst);
        conductor.action().await.unwrap();

        assert_eq!(*sequencer.posts.lock().unwrap(), 2);
        assert_eq!(sequencer.starts.lock().unwrap().as_slice(), &[hash(0x10)]);
        assert!(conductor.seq_active.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unhealthy_takeover_without_consensus_unsafe_head_transfers_like_upstream() {
        let consensus = Arc::new(RecordingConsensus::new("seq-a", vec![]));
        let sequencer = Arc::new(FakeSequencer::default());
        let conductor = Conductor::new(
            consensus.clone(),
            sequencer.clone(),
            ConductorConfig::default(),
        );
        conductor
            .set_previous_for_tests(State {
                leader: false,
                healthy: false,
                active: false,
            })
            .await;

        conductor
            .update_health(Health::Unhealthy("sequencer unhealthy".to_string()))
            .await
            .unwrap();

        assert!(!conductor.leader());
        assert!(sequencer.starts.lock().unwrap().is_empty());
        assert_eq!(
            consensus.transfers.lock().unwrap().as_slice(),
            &["fallback"]
        );
        assert_eq!(
            *conductor.previous.lock().await,
            State {
                leader: true,
                healthy: false,
                active: false,
            }
        );
    }

    #[tokio::test]
    async fn refuses_to_start_when_candidate_is_too_stale() {
        let (conductor, sequencer) = test_conductor().await;
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x05),
            number: 5,
        });

        let err = conductor.update_leader(true).await.unwrap_err();
        assert!(matches!(err, ConductorError::UnsafeHeadMismatch));
        assert!(sequencer.starts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_action_does_not_record_state_change() {
        let (conductor, sequencer) = test_conductor().await;
        *sequencer.latest.lock().unwrap() = Some(BlockInfo {
            hash: hash(0x05),
            number: 5,
        });

        conductor.update_leader(true).await.unwrap_err();

        assert!(!conductor.metrics().render_prometheus().contains(
            "op_conductor_state_changes_count{leader=\"true\",healthy=\"true\",active=\"false\"}"
        ));
    }

    #[test]
    fn health_metric_error_uses_upstream_sentinel_labels() {
        assert_eq!(
            Health::Unhealthy("unsafe head is stale".to_string()).metric_error(),
            ERR_SEQUENCER_NOT_HEALTHY
        );
        assert_eq!(
            Health::SequencerConnectionDown("sync status rpc failed".to_string()).metric_error(),
            ERR_SEQUENCER_CONNECTION_DOWN
        );
        assert_eq!(
            Health::SupervisorConnectionDown("supervisor down".to_string()).metric_error(),
            ERR_SUPERVISOR_CONNECTION_DOWN
        );
        assert_eq!(
            Health::RollupBoostConnectionDown("dial refused".to_string()).metric_error(),
            ERR_ROLLUP_BOOST_CONNECTION_DOWN
        );
        assert_eq!(
            Health::RollupBoostPartiallyHealthy("partial".to_string()).metric_error(),
            ERR_ROLLUP_BOOST_PARTIALLY_HEALTHY
        );
        assert_eq!(
            Health::Unhealthy(ERR_ROLLUP_BOOST_NOT_HEALTHY.to_string()).metric_error(),
            ERR_ROLLUP_BOOST_NOT_HEALTHY
        );
    }

    #[tokio::test]
    async fn paused_conductor_does_not_drive_sequencer() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.pause().await.unwrap();
        conductor.update_leader(true).await.unwrap();

        assert!(sequencer.starts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn resume_does_not_surface_queued_action_failure_like_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeSequencer::default());
        let conductor = Conductor::new(
            consensus,
            sequencer.clone(),
            ConductorConfig {
                start_paused: true,
                ..ConductorConfig::default()
            },
        );
        let mut action_rx = conductor.subscribe_actions();

        conductor.resume().await.unwrap();
        action_rx.changed().await.unwrap();

        assert!(!conductor.paused());
        assert!(sequencer.starts.lock().unwrap().is_empty());
        let metrics = conductor.metrics().render_prometheus();
        assert!(!metrics.contains("op_conductor_sequencer_starts_count{success=\"true\"}"));
        assert!(!metrics.contains("op_conductor_sequencer_starts_count{success=\"false\"}"));
    }

    #[tokio::test]
    async fn resume_refreshes_sequencer_active_before_action_like_upstream() {
        let (conductor, sequencer) = test_conductor_with_config(ConductorConfig {
            start_paused: true,
            ..ConductorConfig::default()
        })
        .await;
        conductor.update_leader(false).await.unwrap();
        sequencer.active.store(true, Ordering::SeqCst);

        conductor.resume().await.unwrap();

        assert!(!conductor.paused());
        assert_eq!(*sequencer.stops.lock().unwrap(), 0);
        assert!(conductor.seq_active.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn resume_returns_before_queued_action_runs_like_upstream() {
        let (conductor, sequencer) = test_conductor_with_config(ConductorConfig {
            start_paused: true,
            ..ConductorConfig::default()
        })
        .await;
        let gate = Arc::new(StartGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *sequencer.start_gate.lock().unwrap() = Some(gate);
        let mut action_rx = conductor.subscribe_actions();

        tokio::time::timeout(Duration::from_millis(50), conductor.resume())
            .await
            .expect("resume should acknowledge before queued start runs")
            .unwrap();
        action_rx.changed().await.unwrap();

        assert!(!conductor.paused());
        assert!(sequencer.starts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unhealthy_leader_waits_after_starting_for_recovery() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.set_seq_active_for_tests(true);
        conductor
            .set_previous_for_tests(State {
                leader: true,
                healthy: false,
                active: false,
            })
            .await;

        let err = conductor
            .update_health(Health::Unhealthy("temporary".to_string()))
            .await
            .unwrap_err();

        assert!(matches!(err, ConductorError::WaitingForHealthRecovery));
        assert_eq!(*sequencer.stops.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn unhealthy_leader_does_not_wait_on_connection_down() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.set_seq_active_for_tests(true);
        conductor
            .set_previous_for_tests(State {
                leader: true,
                healthy: false,
                active: false,
            })
            .await;

        conductor
            .update_health(Health::SequencerConnectionDown("rpc down".to_string()))
            .await
            .unwrap();

        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn newly_elected_unhealthy_leader_starts_unless_sequencer_connection_is_down() {
        let (conductor, sequencer) = test_conductor().await;
        conductor
            .set_previous_for_tests(State {
                leader: false,
                healthy: false,
                active: false,
            })
            .await;

        conductor
            .update_health(Health::SupervisorConnectionDown(
                "supervisor down".to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(sequencer.starts.lock().unwrap().as_slice(), &[hash(0x10)]);
        assert!(conductor.leader());
    }

    #[tokio::test]
    async fn newly_elected_leader_starts_on_rollup_boost_partial_like_upstream() {
        let (conductor, sequencer) = test_conductor().await;
        conductor
            .set_previous_for_tests(State {
                leader: false,
                healthy: false,
                active: false,
            })
            .await;

        conductor
            .update_health(Health::RollupBoostPartiallyHealthy(
                "rollup boost partial".to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(sequencer.starts.lock().unwrap().as_slice(), &[hash(0x10)]);
        assert!(conductor.leader());
    }

    #[tokio::test]
    async fn unhealthy_active_leader_transfers_even_when_stop_fails() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.set_seq_active_for_tests(true);
        *sequencer.stop_error.lock().unwrap() = Some("stop unavailable");

        let err = conductor
            .update_health(Health::SequencerConnectionDown("rpc down".to_string()))
            .await
            .unwrap_err();

        assert!(matches!(err, ConductorError::Sequencer(_)));
        assert!(conductor.seq_active.load(Ordering::SeqCst));
        assert!(!conductor.leader());
        assert_eq!(*sequencer.stops.lock().unwrap(), 0);
        assert!(conductor
            .metrics()
            .render_prometheus()
            .contains("op_conductor_leader_transfers_count{success=\"true\"} 1"));
    }

    #[tokio::test]
    async fn failed_stop_and_transfer_retries_until_both_succeed_like_upstream() {
        let consensus = Arc::new(
            RecordingConsensus::new("seq-a", vec![])
                .with_transfer_errors(vec!["transfer down", "transfer still down"]),
        );
        let sequencer = Arc::new(FakeSequencer::default());
        sequencer.active.store(true, Ordering::SeqCst);
        *sequencer.stop_error.lock().unwrap() = Some("stop down");
        let conductor = Conductor::new(
            consensus.clone(),
            sequencer.clone(),
            ConductorConfig::default(),
        );
        conductor.set_seq_active_for_tests(true);
        conductor
            .set_previous_for_tests(State {
                leader: true,
                healthy: true,
                active: true,
            })
            .await;

        conductor
            .update_health(Health::Unhealthy("sequencer unhealthy".to_string()))
            .await
            .unwrap_err();

        assert!(conductor.leader());
        assert!(conductor.seq_active.load(Ordering::SeqCst));
        assert_eq!(
            *conductor.previous.lock().await,
            State {
                leader: true,
                healthy: true,
                active: true,
            }
        );
        assert_eq!(*sequencer.stops.lock().unwrap(), 0);
        assert_eq!(consensus.transfers.lock().unwrap().len(), 1);

        *sequencer.stop_error.lock().unwrap() = None;
        conductor.action().await.unwrap_err();

        assert!(conductor.leader());
        assert!(!conductor.seq_active.load(Ordering::SeqCst));
        assert_eq!(
            *conductor.previous.lock().await,
            State {
                leader: true,
                healthy: true,
                active: true,
            }
        );
        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
        assert_eq!(consensus.transfers.lock().unwrap().len(), 2);

        conductor.action().await.unwrap();

        assert!(!conductor.leader());
        assert!(!conductor.seq_active.load(Ordering::SeqCst));
        assert_eq!(
            *conductor.previous.lock().await,
            State {
                leader: true,
                healthy: false,
                active: false,
            }
        );
        assert_eq!(consensus.transfers.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn failed_stop_after_successful_transfer_retries_as_follower_like_upstream() {
        let (conductor, sequencer) = test_conductor().await;
        conductor.set_seq_active_for_tests(true);
        sequencer.active.store(true, Ordering::SeqCst);
        *sequencer.stop_error.lock().unwrap() = Some("stop down");
        conductor
            .set_previous_for_tests(State {
                leader: true,
                healthy: true,
                active: true,
            })
            .await;

        conductor
            .update_health(Health::Unhealthy("sequencer unhealthy".to_string()))
            .await
            .unwrap_err();

        assert!(!conductor.leader());
        assert!(conductor.seq_active.load(Ordering::SeqCst));
        assert_eq!(
            *conductor.previous.lock().await,
            State {
                leader: true,
                healthy: true,
                active: true,
            }
        );
        assert_eq!(*sequencer.stops.lock().unwrap(), 0);

        *sequencer.stop_error.lock().unwrap() = None;
        conductor.action().await.unwrap();

        assert!(!conductor.leader());
        assert!(!conductor.seq_active.load(Ordering::SeqCst));
        assert_eq!(
            *conductor.previous.lock().await,
            State {
                leader: false,
                healthy: false,
                active: true,
            }
        );
        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn control_transfer_from_consensus_follower_records_failed_metric_like_upstream() {
        let consensus = Arc::new(RecordingConsensus::new("seq-a", vec![]));
        consensus.set_leader_for_tests(false);
        let conductor = Conductor::new(
            consensus.clone(),
            Arc::new(FakeSequencer::default()),
            ConductorConfig::default(),
        );

        conductor.transfer_leader_internal().await.unwrap();

        assert!(consensus.transfers.lock().unwrap().is_empty());
        assert!(conductor
            .metrics()
            .render_prometheus()
            .contains("op_conductor_leader_transfers_count{success=\"false\"} 1"));
    }

    #[tokio::test]
    async fn rpc_transfer_leader_delegates_without_control_loop_side_effects_like_upstream() {
        let consensus = Arc::new(RecordingConsensus::new("seq-a", vec![]));
        let conductor = Conductor::new(
            consensus.clone(),
            Arc::new(FakeSequencer::default()),
            ConductorConfig::default(),
        );

        conductor.transfer_leader().await.unwrap();
        conductor
            .transfer_leader_to_server("seq-b".to_string(), "127.0.0.1:2".to_string())
            .await
            .unwrap();

        assert!(consensus.leader());
        assert_eq!(
            consensus.transfers.lock().unwrap().as_slice(),
            &["fallback".to_string(), "seq-b".to_string()]
        );
        assert!(!conductor
            .metrics()
            .render_prometheus()
            .contains("op_conductor_leader_transfers_count{success="));
    }

    #[tokio::test]
    async fn round_robin_control_transfer_sorts_voters_like_upstream() {
        let consensus = Arc::new(RecordingConsensus::new(
            "seq-b",
            vec![
                ServerInfo {
                    id: "seq-c".to_string(),
                    addr: "127.0.0.1:3".to_string(),
                    suffrage: ServerSuffrage::Voter,
                },
                ServerInfo {
                    id: "seq-b".to_string(),
                    addr: "127.0.0.1:2".to_string(),
                    suffrage: ServerSuffrage::Voter,
                },
                ServerInfo {
                    id: "seq-a".to_string(),
                    addr: "127.0.0.1:1".to_string(),
                    suffrage: ServerSuffrage::Voter,
                },
                ServerInfo {
                    id: "seq-d".to_string(),
                    addr: "127.0.0.1:4".to_string(),
                    suffrage: ServerSuffrage::Nonvoter,
                },
            ],
        ));
        let conductor = Conductor::new(
            consensus.clone(),
            Arc::new(FakeSequencer::default()),
            ConductorConfig {
                round_robin_leader_transfer: true,
                ..ConductorConfig::default()
            },
        );

        conductor.transfer_leader_internal().await.unwrap();

        assert_eq!(
            consensus.transfers.lock().unwrap().as_slice(),
            &["seq-c".to_string()]
        );
    }

    #[tokio::test]
    async fn round_robin_control_transfer_treats_in_progress_as_success_like_upstream() {
        let consensus = Arc::new(
            RecordingConsensus::new(
                "seq-b",
                vec![
                    ServerInfo {
                        id: "seq-c".to_string(),
                        addr: "127.0.0.1:3".to_string(),
                        suffrage: ServerSuffrage::Voter,
                    },
                    ServerInfo {
                        id: "seq-b".to_string(),
                        addr: "127.0.0.1:2".to_string(),
                        suffrage: ServerSuffrage::Voter,
                    },
                    ServerInfo {
                        id: "seq-a".to_string(),
                        addr: "127.0.0.1:1".to_string(),
                        suffrage: ServerSuffrage::Voter,
                    },
                ],
            )
            .with_transfer_errors(vec!["leadership transfer already in progress"]),
        );
        let conductor = Conductor::new(
            consensus.clone(),
            Arc::new(FakeSequencer::default()),
            ConductorConfig {
                round_robin_leader_transfer: true,
                ..ConductorConfig::default()
            },
        );

        conductor.transfer_leader_internal().await.unwrap();

        assert!(!conductor.leader());
        assert_eq!(
            consensus.transfers.lock().unwrap().as_slice(),
            &["seq-c".to_string()]
        );
        assert!(conductor
            .metrics()
            .render_prometheus()
            .contains("op_conductor_leader_transfers_count{success=\"true\"} 1"));
    }

    #[tokio::test]
    async fn round_robin_control_transfer_returns_fallback_error_like_upstream() {
        let consensus = Arc::new(
            RecordingConsensus::new(
                "seq-b",
                vec![
                    ServerInfo {
                        id: "seq-a".to_string(),
                        addr: "127.0.0.1:1".to_string(),
                        suffrage: ServerSuffrage::Voter,
                    },
                    ServerInfo {
                        id: "seq-b".to_string(),
                        addr: "127.0.0.1:2".to_string(),
                        suffrage: ServerSuffrage::Voter,
                    },
                    ServerInfo {
                        id: "seq-c".to_string(),
                        addr: "127.0.0.1:3".to_string(),
                        suffrage: ServerSuffrage::Voter,
                    },
                ],
            )
            .with_transfer_errors(vec![
                "target c down",
                "target a down",
                "fallback down",
            ]),
        );
        let conductor = Conductor::new(
            consensus.clone(),
            Arc::new(FakeSequencer::default()),
            ConductorConfig {
                round_robin_leader_transfer: true,
                ..ConductorConfig::default()
            },
        );

        let err = conductor.transfer_leader_internal().await.unwrap_err();

        let message = err.to_string();
        assert!(message.contains("fallback down"));
        assert!(!message.contains("target a down"));
        assert!(!message.contains("target c down"));
        assert_eq!(
            consensus.transfers.lock().unwrap().as_slice(),
            &[
                "seq-c".to_string(),
                "seq-a".to_string(),
                "fallback".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn tick_starts_healthy_inactive_leader() {
        let (conductor, sequencer) = test_conductor().await;

        conductor.tick().await.unwrap();

        assert_eq!(sequencer.starts.lock().unwrap().as_slice(), &[hash(0x10)]);
    }

    #[tokio::test]
    async fn tick_stops_and_transfers_when_unsafe_head_is_stale() {
        let (conductor, sequencer) = test_conductor().await;
        sequencer.active.store(true, Ordering::SeqCst);
        *sequencer.sync_status.lock().unwrap() = Some(healthy_sync_status(100));

        conductor.tick().await.unwrap();

        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
        assert!(!conductor.leader());
        let metrics = conductor.metrics().render_prometheus();
        assert!(metrics.contains(
            "op_conductor_healthchecks_count{success=\"false\",error=\"sequencer is not healthy\"} 1"
        ));
        assert!(!metrics.contains("unsafe head is"));
    }

    #[tokio::test]
    async fn tick_stops_and_transfers_when_peer_count_is_too_low() {
        let (conductor, sequencer) = test_conductor().await;
        sequencer.active.store(true, Ordering::SeqCst);
        *sequencer.peer_stats.lock().unwrap() = Some(PeerStats { connected: 0 });

        conductor.tick().await.unwrap();

        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
        assert!(!conductor.leader());
    }

    #[tokio::test]
    async fn tick_treats_health_rpc_failure_as_connection_down() {
        let (conductor, sequencer) = test_conductor().await;
        sequencer.active.store(true, Ordering::SeqCst);
        sequencer.peer_stats_down.store(true, Ordering::SeqCst);
        conductor
            .set_previous_for_tests(State {
                leader: true,
                healthy: false,
                active: false,
            })
            .await;

        conductor.tick().await.unwrap();

        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn safe_head_check_is_optional() {
        let (conductor, sequencer) = test_conductor().await;
        let mut status = healthy_sync_status(1_000);
        status.safe_l2.time = 1;
        *sequencer.sync_status.lock().unwrap() = Some(status);

        let health = conductor.check_health_at(1_000).await;

        assert_eq!(health, Health::Healthy);
    }

    #[tokio::test]
    async fn safe_head_check_marks_stale_safe_head_unhealthy_when_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        store.commit(payload(10, 0x10)).await.unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeSequencer::default());
        let mut status = healthy_sync_status(1_000);
        status.safe_l2.time = 1;
        *sequencer.sync_status.lock().unwrap() = Some(status);
        let conductor = Conductor::new(
            consensus,
            sequencer,
            ConductorConfig {
                healthcheck_safe_enabled: true,
                healthcheck_safe_interval: Duration::from_secs(10),
                ..ConductorConfig::default()
            },
        );

        let health = conductor.check_health_at(1_000).await;

        assert!(matches!(health, Health::Unhealthy(_)));
    }

    #[tokio::test]
    async fn rollup_boost_partial_health_does_not_wait_for_recovery() {
        let url = rollup_boost_server(StatusCode::PARTIAL_CONTENT).await;
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        store.commit(payload(10, 0x10)).await.unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeSequencer::default());
        sequencer.active.store(true, Ordering::SeqCst);
        let conductor = Conductor::new(
            consensus,
            sequencer.clone(),
            ConductorConfig {
                rollup_boost_health: Some(RollupBoostHealthConfig::StatusCode {
                    base_url: url,
                    timeout: Duration::from_secs(1),
                }),
                ..ConductorConfig::default()
            },
        );
        conductor
            .set_previous_for_tests(State {
                leader: true,
                healthy: false,
                active: false,
            })
            .await;

        conductor.tick().await.unwrap();

        assert_eq!(*sequencer.stops.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn rollup_boost_partial_health_is_tolerated_until_limit() {
        let url = rollup_boost_server(StatusCode::PARTIAL_CONTENT).await;
        let (conductor, _) = test_conductor_with_config(ConductorConfig {
            rollup_boost_health: Some(RollupBoostHealthConfig::StatusCode {
                base_url: url,
                timeout: Duration::from_secs(1),
            }),
            rollup_boost_partial_health_tolerance: Some(RollupBoostPartialHealthTolerance {
                limit: 2,
                interval: Duration::from_secs(10),
            }),
            ..ConductorConfig::default()
        })
        .await;

        assert_eq!(conductor.check_health_at(100).await, Health::Healthy);
        assert_eq!(conductor.check_health_at(101).await, Health::Healthy);
        assert!(matches!(
            conductor.check_health_at(102).await,
            Health::RollupBoostPartiallyHealthy(_)
        ));
    }

    #[tokio::test]
    async fn rollup_boost_partial_health_tolerance_resets_by_interval() {
        let url = rollup_boost_server(StatusCode::PARTIAL_CONTENT).await;
        let (conductor, _) = test_conductor_with_config(ConductorConfig {
            rollup_boost_health: Some(RollupBoostHealthConfig::StatusCode {
                base_url: url,
                timeout: Duration::from_secs(1),
            }),
            rollup_boost_partial_health_tolerance: Some(RollupBoostPartialHealthTolerance {
                limit: 1,
                interval: Duration::from_secs(10),
            }),
            ..ConductorConfig::default()
        })
        .await;

        assert_eq!(conductor.check_health_at(100).await, Health::Healthy);
        assert!(matches!(
            conductor.check_health_at(101).await,
            Health::RollupBoostPartiallyHealthy(_)
        ));
        assert_eq!(conductor.check_health_at(110).await, Health::Healthy);
    }

    #[tokio::test]
    async fn supervisor_health_failure_waits_for_recovery_after_start_like_upstream() {
        let url = supervisor_server(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32000, "message": "supervisor down"}
        }))
        .await;
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        store.commit(payload(10, 0x10)).await.unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeSequencer::default());
        sequencer.active.store(true, Ordering::SeqCst);
        let conductor = Conductor::new(
            consensus,
            sequencer.clone(),
            ConductorConfig {
                supervisor_health: Some(SupervisorHealthConfig { rpc: url }),
                ..ConductorConfig::default()
            },
        );
        conductor
            .set_previous_for_tests(State {
                leader: true,
                healthy: false,
                active: false,
            })
            .await;

        let err = conductor.tick().await.unwrap_err();

        assert!(matches!(err, ConductorError::WaitingForHealthRecovery));
        assert_eq!(*sequencer.stops.lock().unwrap(), 0);
        assert!(matches!(
            conductor.healthy.lock().await.clone(),
            Health::SupervisorConnectionDown(_)
        ));
    }

    #[tokio::test]
    async fn execution_p2p_low_peer_count_is_unhealthy() {
        let url = supervisor_server(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0x1"
        }))
        .await;
        let (conductor, _) = test_conductor_with_config(ConductorConfig {
            execution_p2p_health: Some(ExecutionP2pHealthConfig {
                rpc: url,
                check_api: ExecutionP2pCheckApi::Net,
                min_peer_count: 2,
            }),
            ..ConductorConfig::default()
        })
        .await;

        let health = conductor.check_health_at(current_unix_time()).await;

        assert!(matches!(health, Health::Unhealthy(_)));
    }
}
