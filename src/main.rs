use anyhow::Context;
use clap::{ArgAction, CommandFactory, Parser, ValueEnum};
use conductor_rs::pprof::{start_cpu_file_profiler, CpuFileProfiler};
use conductor_rs::sequencer::{SequencerControl, SequencerError};
use conductor_rs::{
    rpc::{ExecutionClient, ProxyConfig, RollupNodeClient},
    serve_flashblocks, serve_metrics_with_shutdown, serve_pprof_with_shutdown,
    serve_raft_transport_on_listener, Conductor, ConductorConfig, Consensus, ExecutionP2pCheckApi,
    ExecutionP2pHealthConfig, FlashblocksConfig, RaftConsensus, RaftConsensusConfig,
    RollupBoostHealthConfig, RollupBoostPartialHealthTolerance, SequencerController,
    SequencerStartMode, SupervisorHealthConfig,
};
use std::{
    collections::BTreeSet,
    ffi::OsStr,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use url::Url;

#[cfg(not(test))]
const MAX_ACTION_RETRY_BACKOFF_MS: u64 = 2_000;
const ENV_VAR_PREFIX: &str = "OP_CONDUCTOR";

#[derive(Debug, Parser)]
#[command(author, version, about = "Rust OP Stack sequencer conductor")]
struct Args {
    #[arg(long = "node.rpc", env = "OP_CONDUCTOR_NODE_RPC")]
    node_rpc: Url,
    #[arg(long = "execution.rpc", env = "OP_CONDUCTOR_EXECUTION_RPC")]
    execution_rpc: Url,
    #[arg(
        long = "log.level",
        env = "OP_CONDUCTOR_LOG_LEVEL",
        default_value = "info",
        value_parser = parse_log_level
    )]
    log_level: LogLevel,
    #[arg(
        long = "log.format",
        env = "OP_CONDUCTOR_LOG_FORMAT",
        default_value = "text"
    )]
    log_format: LogFormat,
    #[arg(
        long = "log.color",
        env = "OP_CONDUCTOR_LOG_COLOR",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    log_color: bool,
    #[arg(
        long = "log.pid",
        env = "OP_CONDUCTOR_LOG_PID",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    log_pid: bool,
    #[arg(long = "network", env = "OP_CONDUCTOR_NETWORK")]
    network: Option<String>,
    #[arg(long = "rollup.config", env = "OP_CONDUCTOR_ROLLUP_CONFIG")]
    rollup_config: Option<PathBuf>,
    #[arg(long = "override.canyon", env = "OP_CONDUCTOR_OVERRIDE_CANYON")]
    override_canyon: Option<u64>,
    #[arg(long = "override.delta", env = "OP_CONDUCTOR_OVERRIDE_DELTA")]
    override_delta: Option<u64>,
    #[arg(long = "override.ecotone", env = "OP_CONDUCTOR_OVERRIDE_ECOTONE")]
    override_ecotone: Option<u64>,
    #[arg(long = "override.fjord", env = "OP_CONDUCTOR_OVERRIDE_FJORD")]
    override_fjord: Option<u64>,
    #[arg(long = "override.granite", env = "OP_CONDUCTOR_OVERRIDE_GRANITE")]
    override_granite: Option<u64>,
    #[arg(long = "override.holocene", env = "OP_CONDUCTOR_OVERRIDE_HOLOCENE")]
    override_holocene: Option<u64>,
    #[arg(long = "override.isthmus", env = "OP_CONDUCTOR_OVERRIDE_ISTHMUS")]
    override_isthmus: Option<u64>,
    #[arg(long = "override.jovian", env = "OP_CONDUCTOR_OVERRIDE_JOVIAN")]
    override_jovian: Option<u64>,
    #[arg(long = "override.karst", env = "OP_CONDUCTOR_OVERRIDE_KARST")]
    override_karst: Option<u64>,
    #[arg(long = "override.interop", env = "OP_CONDUCTOR_OVERRIDE_INTEROP")]
    override_interop: Option<u64>,
    #[arg(
        long = "override.pectrablobschedule",
        env = "OP_CONDUCTOR_OVERRIDE_PECTRABLOBSCHEDULE"
    )]
    override_pectrablobschedule: Option<u64>,
    #[arg(long = "supervisor.rpc", env = "OP_CONDUCTOR_SUPERVISOR_RPC")]
    supervisor_rpc: Option<String>,
    #[arg(long = "raft.server.id", env = "OP_CONDUCTOR_RAFT_SERVER_ID")]
    server_id: String,
    #[arg(long = "raft.storage.dir", env = "OP_CONDUCTOR_RAFT_STORAGE_DIR")]
    storage_dir: String,
    #[arg(
        long = "raft.bootstrap",
        env = "OP_CONDUCTOR_RAFT_BOOTSTRAP",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    raft_bootstrap: bool,
    #[arg(
        long = "consensus.addr",
        env = "OP_CONDUCTOR_CONSENSUS_ADDR",
        default_value = "127.0.0.1"
    )]
    consensus_addr: String,
    #[arg(
        long = "consensus.port",
        env = "OP_CONDUCTOR_CONSENSUS_PORT",
        default_value_t = 50050
    )]
    consensus_port: u16,
    #[arg(
        long = "consensus.advertised",
        env = "OP_CONDUCTOR_CONSENSUS_ADVERTISED"
    )]
    consensus_advertised: Option<String>,
    #[arg(
        long = "raft.snapshot-interval",
        env = "OP_CONDUCTOR_RAFT_SNAPSHOT_INTERVAL",
        default_value = "120s",
        value_parser = parse_duration_arg
    )]
    raft_snapshot_interval: Duration,
    #[arg(
        long = "raft.snapshot-threshold",
        env = "OP_CONDUCTOR_RAFT_SNAPSHOT_THRESHOLD",
        default_value_t = 8192
    )]
    raft_snapshot_threshold: u64,
    #[arg(
        long = "raft.trailing-logs",
        env = "OP_CONDUCTOR_RAFT_TRAILING_LOGS",
        default_value_t = 10240
    )]
    raft_trailing_logs: u64,
    #[arg(
        long = "raft.heartbeat-timeout",
        env = "OP_CONDUCTOR_RAFT_HEARTBEAT_TIMEOUT",
        default_value = "1000ms",
        value_parser = humantime::parse_duration
    )]
    raft_heartbeat_timeout: Duration,
    #[arg(
        long = "raft.lease-timeout",
        env = "OP_CONDUCTOR_RAFT_LEASE_TIMEOUT",
        default_value = "500ms",
        value_parser = humantime::parse_duration
    )]
    raft_lease_timeout: Duration,
    #[arg(
        long = "rpc.addr",
        env = "OP_CONDUCTOR_RPC_ADDR",
        default_value = "0.0.0.0"
    )]
    rpc_addr: String,
    #[arg(
        long = "rpc.port",
        env = "OP_CONDUCTOR_RPC_PORT",
        default_value_t = 8545
    )]
    rpc_port: u16,
    #[arg(
        long = "rpc.enable-admin",
        env = "OP_CONDUCTOR_RPC_ENABLE_ADMIN",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    rpc_enable_admin: bool,
    #[arg(
        long = "rpc.enable-proxy",
        env = "OP_CONDUCTOR_RPC_ENABLE_PROXY",
        default_value_t = true,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    rpc_enable_proxy: bool,
    #[arg(
        long = "metrics.enabled",
        env = "OP_CONDUCTOR_METRICS_ENABLED",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    metrics_enabled: bool,
    #[arg(
        long = "metrics.addr",
        env = "OP_CONDUCTOR_METRICS_ADDR",
        default_value = "0.0.0.0"
    )]
    metrics_addr: String,
    #[arg(
        long = "metrics.port",
        env = "OP_CONDUCTOR_METRICS_PORT",
        default_value_t = 7300
    )]
    metrics_port: u16,
    #[arg(
        long = "pprof.enabled",
        env = "OP_CONDUCTOR_PPROF_ENABLED",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    pprof_enabled: bool,
    #[arg(
        long = "pprof.addr",
        env = "OP_CONDUCTOR_PPROF_ADDR",
        default_value = "0.0.0.0"
    )]
    pprof_addr: String,
    #[arg(
        long = "pprof.port",
        env = "OP_CONDUCTOR_PPROF_PORT",
        default_value_t = 6060
    )]
    pprof_port: u16,
    #[arg(long = "pprof.path", env = "OP_CONDUCTOR_PPROF_PATH")]
    pprof_path: Option<PathBuf>,
    #[arg(long = "pprof.type", env = "OP_CONDUCTOR_PPROF_TYPE")]
    pprof_type: Option<PprofProfile>,
    #[arg(long = "rollupboost.ws-url", env = "OP_CONDUCTOR_ROLLUPBOOST_WS_URL")]
    rollupboost_ws_url: Option<String>,
    #[arg(
        long = "websocket.server-port",
        env = "OP_CONDUCTOR_WEBSOCKET_SERVER_PORT",
        default_value_t = 8546
    )]
    websocket_server_port: u16,
    #[arg(
        long = "paused",
        env = "OP_CONDUCTOR_PAUSED",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    paused: bool,
    #[arg(
        long = "sequencer.start-mode",
        env = "OP_CONDUCTOR_SEQUENCER_START_MODE",
        default_value = "hash-param"
    )]
    start_mode: SequencerStartMode,
    #[arg(
        long = "unsafe-repair-depth",
        env = "OP_CONDUCTOR_UNSAFE_REPAIR_DEPTH",
        default_value_t = 1
    )]
    unsafe_repair_depth: u64,
    #[arg(
        long = "raft.round-robin-leader-transfer",
        env = "OP_CONDUCTOR_RAFT_ROUND_ROBIN_LEADER_TRANSFER",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    raft_round_robin_leader_transfer: bool,
    #[arg(
        long = "healthcheck.interval",
        alias = "control.interval",
        env = "OP_CONDUCTOR_HEALTHCHECK_INTERVAL",
        value_parser = parse_duration_arg
    )]
    healthcheck_interval: Option<Duration>,
    #[arg(
        long = "healthcheck.unsafe-interval",
        env = "OP_CONDUCTOR_HEALTHCHECK_UNSAFE_INTERVAL",
        value_parser = parse_duration_arg
    )]
    healthcheck_unsafe_interval: Option<Duration>,
    #[arg(
        long = "healthcheck.safe-enabled",
        env = "OP_CONDUCTOR_HEALTHCHECK_SAFE_ENABLED",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    healthcheck_safe_enabled: bool,
    #[arg(
        long = "healthcheck.safe-interval",
        env = "OP_CONDUCTOR_HEALTHCHECK_SAFE_INTERVAL",
        default_value = "1200s",
        value_parser = parse_duration_arg
    )]
    healthcheck_safe_interval: Duration,
    #[arg(
        long = "healthcheck.min-peer-count",
        env = "OP_CONDUCTOR_HEALTHCHECK_MIN_PEER_COUNT"
    )]
    healthcheck_min_peer_count: Option<u64>,
    #[arg(
        long = "healthcheck.execution-p2p-enabled",
        env = "OP_CONDUCTOR_HEALTHCHECK_EXECUTION_P2P_ENABLED",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    healthcheck_execution_p2p_enabled: bool,
    #[arg(
        long = "healthcheck.execution-p2p-min-peer-count",
        env = "OP_CONDUCTOR_HEALTHCHECK_EXECUTION_P2P_MIN_PEER_COUNT",
        default_value_t = 0
    )]
    healthcheck_execution_p2p_min_peer_count: u64,
    #[arg(
        long = "healthcheck.execution-p2p-rpc-url",
        env = "OP_CONDUCTOR_HEALTHCHECK_EXECUTION_P2P_RPC_URL"
    )]
    healthcheck_execution_p2p_rpc_url: Option<String>,
    #[arg(
        long = "healthcheck.execution-p2p-check-api",
        env = "OP_CONDUCTOR_HEALTHCHECK_EXECUTION_P2P_CHECK_API",
        default_value = "net"
    )]
    healthcheck_execution_p2p_check_api: String,
    #[arg(
        long = "rollup-boost.enabled",
        env = "OP_CONDUCTOR_ROLLUP_BOOST_ENABLED",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    rollup_boost_enabled: bool,
    #[arg(
        long = "rollup-boost.healthcheck-timeout",
        env = "OP_CONDUCTOR_ROLLUP_BOOST_HEALTHCHECK_TIMEOUT",
        default_value = "5s",
        value_parser = parse_duration_arg
    )]
    rollup_boost_healthcheck_timeout: Duration,
    #[arg(
        long = "rollup-boost.next-enabled",
        env = "OP_CONDUCTOR_ROLLUP_BOOST_NEXT_ENABLED",
        default_value_t = false,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    rollup_boost_next_enabled: bool,
    #[arg(
        long = "rollup-boost.next-healthcheck-url",
        env = "OP_CONDUCTOR_ROLLUP_BOOST_NEXT_HEALTHCHECK_URL"
    )]
    rollup_boost_next_healthcheck_url: Option<String>,
    #[arg(
        long = "healthcheck.rollup-boost-partial-healthiness-tolerance-limit",
        env = "OP_CONDUCTOR_HEALTHCHECK_ROLLUP_BOOST_PARTIAL_HEALTHINESS_TOLERANCE_LIMIT",
        default_value_t = 0
    )]
    rollup_boost_partial_healthiness_tolerance_limit: u64,
    #[arg(
        long = "healthcheck.rollup-boost-partial-healthiness-tolerance-interval-seconds",
        env = "OP_CONDUCTOR_HEALTHCHECK_ROLLUP_BOOST_PARTIAL_HEALTHINESS_TOLERANCE_INTERVAL_SECONDS",
        default_value_t = 0
    )]
    rollup_boost_partial_healthiness_tolerance_interval_seconds: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Crit,
}

impl LogLevel {
    fn as_filter(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error | Self::Crit => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum LogFormat {
    Text,
    Terminal,
    Logfmt,
    Logfmtms,
    Json,
    Jsonms,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum PprofProfile {
    Cpu,
    Heap,
    Goroutine,
    Threadcreate,
    Block,
    Mutex,
    Allocs,
}

const KNOWN_ROLLUP_NETWORKS: &[&str] = &[
    "arena-z-mainnet",
    "arena-z-sepolia",
    "automata-mainnet",
    "base-devnet-0-sepolia-dev-0",
    "base-mainnet",
    "base-sepolia",
    "bob-mainnet",
    "camp-sepolia",
    "creator-chain-testnet-sepolia",
    "cyber-mainnet",
    "cyber-sepolia",
    "ethernity-mainnet",
    "ethernity-sepolia",
    "fraxtal-mainnet",
    "funki-mainnet",
    "funki-sepolia",
    "hashkeychain-mainnet",
    "ink-mainnet",
    "ink-sepolia",
    "lisk-mainnet",
    "lisk-sepolia",
    "lyra-mainnet",
    "metal-mainnet",
    "metal-sepolia",
    "mint-mainnet",
    "mode-mainnet",
    "mode-sepolia",
    "op-mainnet",
    "op-sepolia",
    "oplabs-devnet-0-sepolia-dev-0",
    "orderly-mainnet",
    "ozean-sepolia",
    "pivotal-sepolia",
    "polynomial-mainnet",
    "race-mainnet",
    "race-sepolia",
    "radius_testnet-sepolia",
    "redstone-mainnet",
    "rehearsal-0-bn-0-rehearsal-0-bn",
    "rehearsal-0-bn-1-rehearsal-0-bn",
    "settlus-mainnet-mainnet",
    "settlus-sepolia-sepolia",
    "shape-mainnet",
    "shape-sepolia",
    "silent-data-mainnet-mainnet",
    "snax-mainnet",
    "soneium-mainnet",
    "soneium-minato-sepolia",
    "sseed-mainnet",
    "swan-mainnet",
    "swell-mainnet",
    "tbn-mainnet",
    "tbn-sepolia",
    "unichain-mainnet",
    "unichain-sepolia",
    "worldchain-mainnet",
    "worldchain-sepolia",
    "xterio-eth-mainnet",
    "zora-mainnet",
    "zora-sepolia",
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing(&args);
    warn_unknown_env_vars();
    log_parsed_compatibility_flags(&args);
    validate_args(&args)?;
    let healthcheck_interval = args
        .healthcheck_interval
        .context("flag healthcheck.interval is required")?;
    let healthcheck_unsafe_interval = args
        .healthcheck_unsafe_interval
        .context("flag healthcheck.unsafe-interval is required")?;
    let healthcheck_min_peer_count = args
        .healthcheck_min_peer_count
        .context("flag healthcheck.min-peer-count is required")?;
    let rollup_boost_partial_health_tolerance = match (
        args.rollup_boost_partial_healthiness_tolerance_limit,
        args.rollup_boost_partial_healthiness_tolerance_interval_seconds,
    ) {
        (0, 0) => None,
        (limit, interval) if limit != 0 && interval != 0 => {
            Some(RollupBoostPartialHealthTolerance {
                limit,
                interval: Duration::from_secs(interval),
            })
        }
        _ => anyhow::bail!(
            "rollup-boost partial-health tolerance limit and interval must be set together"
        ),
    };
    let execution_p2p_health = execution_p2p_health_config(&args)?;
    let supervisor_rpc = parse_optional_url(args.supervisor_rpc.as_deref(), "supervisor.rpc")?;
    let rollupboost_ws_url =
        parse_optional_url(args.rollupboost_ws_url.as_deref(), "rollupboost.ws-url")?;
    let rollup_boost_health = rollup_boost_health_config(&args)?;
    let consensus_listen_addr = listen_addr(&args.consensus_addr, args.consensus_port)
        .context("parsing consensus listen address")?;
    let consensus_listener = TcpListener::bind(consensus_listen_addr)
        .await
        .context("binding consensus listen address")?;
    let consensus_bound_addr = consensus_listener
        .local_addr()
        .context("reading consensus bound address")?;
    let consensus_advertised = optional_string(args.consensus_advertised.as_deref())
        .unwrap_or_else(|| consensus_bound_addr.to_string());
    let rpc_addr =
        listen_addr(&args.rpc_addr, args.rpc_port).context("parsing rpc listen address")?;
    let raft_heartbeat_interval =
        std::cmp::max(args.raft_heartbeat_timeout / 3, Duration::from_millis(1));
    let consensus = RaftConsensus::new_http(RaftConsensusConfig {
        server_id: args.server_id,
        advertised_addr: consensus_advertised,
        storage_dir: PathBuf::from(args.storage_dir),
        bootstrap: args.raft_bootstrap,
        snapshot_interval: args.raft_snapshot_interval,
        heartbeat_interval: raft_heartbeat_interval,
        election_timeout_min: args.raft_heartbeat_timeout,
        election_timeout_max: args.raft_heartbeat_timeout + args.raft_lease_timeout,
        snapshot_threshold: args.raft_snapshot_threshold,
        trailing_logs: args.raft_trailing_logs,
    })
    .await
    .context("opening raft consensus")?;
    let bootstrapped = consensus.bootstrapped();
    let sequencer = SequencerController::new(
        RollupNodeClient::new(args.node_rpc.clone()),
        ExecutionClient::new(args.execution_rpc.clone()),
        args.start_mode,
    );
    wait_for_conductor_enabled(sequencer.as_ref(), 60, Duration::from_secs(5)).await?;
    let conductor = Conductor::new(
        consensus.clone(),
        sequencer,
        ConductorConfig {
            start_paused: args.paused || bootstrapped,
            unsafe_repair_depth: args.unsafe_repair_depth,
            round_robin_leader_transfer: args.raft_round_robin_leader_transfer,
            healthcheck_unsafe_interval,
            healthcheck_safe_enabled: args.healthcheck_safe_enabled,
            healthcheck_safe_interval: args.healthcheck_safe_interval,
            healthcheck_min_peer_count,
            execution_p2p_health,
            supervisor_health: supervisor_rpc.map(|rpc| SupervisorHealthConfig { rpc }),
            rollup_boost_health,
            rollup_boost_partial_health_tolerance,
        },
    );
    conductor
        .initialize_startup_state()
        .await
        .context("failed to initialize sequencer active status")?;
    let metrics = conductor.metrics();
    let metrics_addr = listen_addr(&args.metrics_addr, args.metrics_port)
        .context("parsing metrics listen address")?;
    let pprof_addr = if args.pprof_enabled {
        Some(
            listen_addr(&args.pprof_addr, args.pprof_port)
                .context("parsing pprof listen address")?,
        )
    } else {
        None
    };
    let pprof_file_profiler =
        start_pprof_file_profiler(args.pprof_type, args.pprof_path.as_deref())?;
    if args.rpc_enable_admin {
        tracing::debug!("rpc.enable-admin is accepted for upstream CLI compatibility");
    }

    tracing::info!(addr = %rpc_addr, "starting conductor-rs rpc server");
    tracing::info!(addr = %consensus_bound_addr, "starting conductor-rs raft transport");
    let proxy_config = args.rpc_enable_proxy.then_some(ProxyConfig {
        execution_rpc: args.execution_rpc,
        node_rpc: args.node_rpc,
    });
    let consensus_for_transport = consensus.clone();
    let consensus_for_leader_watch = consensus;
    let conductor_for_metrics = conductor.clone();
    let conductor_for_pprof = conductor.clone();
    metrics.record_up();
    let runtime_result: anyhow::Result<()> = tokio::select! {
        result = conductor_rs::rpc::serve_with_proxy(conductor.clone(), rpc_addr, proxy_config) => result.context("rpc server exited").map(|_| ()),
        result = serve_raft_transport_on_listener(consensus_for_transport, consensus_listener) => result.context("raft transport exited"),
        result = run_metrics_server(metrics, metrics_addr, args.metrics_enabled, conductor_for_metrics) => result.context("metrics server exited"),
        result = run_pprof_server(pprof_addr, conductor_for_pprof) => result.context("pprof server exited"),
        result = run_flashblocks_server(rollupboost_ws_url, args.websocket_server_port, conductor.clone()) => result.context("flashblocks websocket server exited"),
        result = run_leader_watch(consensus_for_leader_watch, conductor.clone()) => result.context("leader watch exited"),
        result = run_control_loop(conductor.clone(), healthcheck_interval) => result.context("control loop exited"),
        signal = wait_for_shutdown_signal() => {
            signal.map(|signal| {
                tracing::info!(signal, "shutdown signal received");
            })
        },
    };
    finalize_runtime_exit(conductor, runtime_result, pprof_file_profiler).await
}

fn init_tracing(args: &Args) {
    let filter = EnvFilter::new(args.log_level.as_filter());
    if args.log_pid {
        let pid = std::process::id();
        match args.log_format {
            LogFormat::Json | LogFormat::Jsonms => {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(
                        tracing_subscriber::fmt::layer()
                            .json()
                            .with_ansi(args.log_color),
                    )
                    .init();
            }
            _ => {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(tracing_subscriber::fmt::layer().with_ansi(args.log_color))
                    .init();
            }
        }
        tracing::debug!(pid, "process id");
        return;
    }

    match args.log_format {
        LogFormat::Json | LogFormat::Jsonms => {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_ansi(args.log_color),
                )
                .init();
        }
        _ => {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().with_ansi(args.log_color))
                .init();
        }
    }
}

fn log_parsed_compatibility_flags(args: &Args) {
    let rollup_flags_set = args.network.is_some()
        || args.rollup_config.is_some()
        || args.override_canyon.is_some()
        || args.override_delta.is_some()
        || args.override_ecotone.is_some()
        || args.override_fjord.is_some()
        || args.override_granite.is_some()
        || args.override_holocene.is_some()
        || args.override_isthmus.is_some()
        || args.override_jovian.is_some()
        || args.override_karst.is_some()
        || args.override_interop.is_some()
        || args.override_pectrablobschedule.is_some();
    if rollup_flags_set {
        tracing::debug!("rollup config flags parsed for upstream CLI compatibility");
    }

    if args.pprof_enabled || args.pprof_path.is_some() || args.pprof_type.is_some() {
        tracing::debug!(
            addr = %args.pprof_addr,
            port = args.pprof_port,
            "pprof flags parsed for upstream CLI compatibility"
        );
    }
}

fn warn_unknown_env_vars() {
    let defined_env_vars = defined_env_vars();
    for env_var in unknown_env_vars(ENV_VAR_PREFIX, std::env::vars_os(), &defined_env_vars) {
        tracing::warn!(prefix = ENV_VAR_PREFIX, env_var = %env_var, "Unknown env var");
    }
}

fn defined_env_vars() -> BTreeSet<String> {
    Args::command()
        .get_arguments()
        .filter_map(|arg| {
            arg.get_env()
                .map(|env_var| env_var.to_string_lossy().into_owned())
        })
        .collect()
}

fn unknown_env_vars<I, K, V>(
    prefix: &str,
    provided_env_vars: I,
    defined_env_vars: &BTreeSet<String>,
) -> Vec<String>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    provided_env_vars
        .into_iter()
        .filter_map(|(key, value)| {
            let key = key.as_ref().to_string_lossy();
            if !key.starts_with(prefix) || defined_env_vars.contains(key.as_ref()) {
                return None;
            }
            let value = value.as_ref().to_string_lossy();
            Some(format!("{key}={value}"))
        })
        .collect()
}

fn validate_args(args: &Args) -> anyhow::Result<()> {
    if args.rollup_boost_enabled && args.rollup_boost_next_enabled {
        anyhow::bail!("only one of rollup-boost or rollup-boost next healthchecks can be enabled");
    }
    if args.rollup_boost_next_enabled
        && optional_string(args.rollup_boost_next_healthcheck_url.as_deref()).is_none()
    {
        anyhow::bail!("missing rollup-boost next healthcheck URL");
    }
    validate_rollup_config_source(args)?;
    if args.consensus_addr.is_empty() {
        anyhow::bail!("missing consensus address");
    }
    if args.server_id.is_empty() {
        anyhow::bail!("missing raft server ID");
    }
    if args.storage_dir.is_empty() {
        anyhow::bail!("missing raft storage directory");
    }
    let healthcheck_interval = args
        .healthcheck_interval
        .context("flag healthcheck.interval is required")?;
    let _healthcheck_unsafe_interval = args
        .healthcheck_unsafe_interval
        .context("flag healthcheck.unsafe-interval is required")?;
    let healthcheck_min_peer_count = args
        .healthcheck_min_peer_count
        .context("flag healthcheck.min-peer-count is required")?;
    if healthcheck_interval.is_zero() {
        anyhow::bail!("missing health check interval");
    }
    if args.healthcheck_safe_interval.is_zero() {
        anyhow::bail!("missing safe interval");
    }
    if healthcheck_min_peer_count == 0 {
        anyhow::bail!("missing minimum peer count");
    }
    if args.healthcheck_execution_p2p_enabled && args.healthcheck_execution_p2p_min_peer_count == 0
    {
        anyhow::bail!("missing minimum el p2p peers");
    }
    if args.healthcheck_execution_p2p_enabled
        && args.healthcheck_execution_p2p_check_api.trim().is_empty()
    {
        anyhow::bail!("missing el p2p check api");
    }
    if args.healthcheck_execution_p2p_enabled
        && !matches!(
            args.healthcheck_execution_p2p_check_api.as_str(),
            "net" | "admin"
        )
    {
        anyhow::bail!("invalid el p2p check api");
    }
    if (args.rollup_boost_partial_healthiness_tolerance_limit != 0
        && args.rollup_boost_partial_healthiness_tolerance_interval_seconds == 0)
        || (args.rollup_boost_partial_healthiness_tolerance_limit == 0
            && args.rollup_boost_partial_healthiness_tolerance_interval_seconds != 0)
    {
        anyhow::bail!(
            "only one of RollupBoostPartialHealthinessToleranceLimit or RollupBoostPartialHealthinessToleranceIntervalSeconds found to be defined. Either define both of them or none."
        );
    }
    Ok(())
}

fn execution_p2p_health_config(args: &Args) -> anyhow::Result<Option<ExecutionP2pHealthConfig>> {
    if !args.healthcheck_execution_p2p_enabled {
        return Ok(None);
    }
    let rpc = parse_optional_url(
        args.healthcheck_execution_p2p_rpc_url.as_deref(),
        "healthcheck.execution-p2p-rpc-url",
    )?
    .unwrap_or_else(|| args.execution_rpc.clone());

    Ok(Some(ExecutionP2pHealthConfig {
        rpc,
        check_api: parse_execution_p2p_check_api(&args.healthcheck_execution_p2p_check_api)?,
        min_peer_count: args.healthcheck_execution_p2p_min_peer_count,
    }))
}

fn rollup_boost_health_config(args: &Args) -> anyhow::Result<Option<RollupBoostHealthConfig>> {
    if args.rollup_boost_enabled {
        return Ok(Some(RollupBoostHealthConfig::StatusCode {
            base_url: args.execution_rpc.clone(),
            timeout: args.rollup_boost_healthcheck_timeout,
        }));
    }
    if args.rollup_boost_next_enabled {
        let url = optional_string(args.rollup_boost_next_healthcheck_url.as_deref())
            .context("missing rollup-boost next healthcheck URL")?;
        return Ok(Some(RollupBoostHealthConfig::Json {
            url,
            timeout: args.rollup_boost_healthcheck_timeout,
        }));
    }
    Ok(None)
}

fn validate_rollup_config_source(args: &Args) -> anyhow::Result<()> {
    if let Some(network) = args
        .network
        .as_deref()
        .filter(|network| !network.trim().is_empty())
    {
        validate_network_name(network)?;
        if args.rollup_config.is_some() {
            tracing::warn!(
                "network and rollup.config are both set; matching upstream by using network"
            );
        }
        return Ok(());
    }

    let path = args
        .rollup_config
        .as_ref()
        .context("failed to load rollup config: missing network or rollup.config")?;
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read rollup config {}", path.display()))?;
    let config = serde_json::from_str::<serde_json::Value>(&raw)
        .with_context(|| format!("failed to decode rollup config {}", path.display()))?;
    validate_rollup_config_json(&config)
        .with_context(|| format!("invalid rollup config {}", path.display()))
}

fn validate_network_name(raw: &str) -> anyhow::Result<()> {
    let normalized = match raw.trim().to_ascii_lowercase().as_str() {
        "mainnet" => "op-mainnet".to_string(),
        "sepolia" => "op-sepolia".to_string(),
        other => other.to_string(),
    };
    if KNOWN_ROLLUP_NETWORKS
        .iter()
        .any(|network| network.eq_ignore_ascii_case(&normalized))
    {
        Ok(())
    } else {
        anyhow::bail!("invalid network: {raw:?}")
    }
}

fn validate_rollup_config_json(config: &serde_json::Value) -> anyhow::Result<()> {
    non_empty_hash(config.pointer("/genesis/l1/hash"), "genesis.l1.hash")?;
    non_empty_hash(config.pointer("/genesis/l2/hash"), "genesis.l2.hash")?;
    if config.pointer("/genesis/l1/hash") == config.pointer("/genesis/l2/hash") {
        anyhow::bail!("genesis L1 and L2 hashes must differ");
    }
    non_zero_u64(config.pointer("/genesis/l2_time"), "genesis.l2_time")?;
    non_zero_address(
        config.pointer("/genesis/system_config/batcherAddr"),
        "genesis.system_config.batcherAddr",
    )?;
    non_empty_hash(
        config.pointer("/genesis/system_config/scalar"),
        "genesis.system_config.scalar",
    )?;
    non_zero_u64(
        config.pointer("/genesis/system_config/gasLimit"),
        "genesis.system_config.gasLimit",
    )?;
    non_zero_u64(config.get("block_time"), "block_time")?;
    non_zero_u64(config.get("channel_timeout"), "channel_timeout")?;
    non_zero_u64(config.get("max_sequencer_drift"), "max_sequencer_drift")?;
    if numeric_value(config.get("seq_window_size"), "seq_window_size")? < 2 {
        anyhow::bail!("seq_window_size must be at least 2");
    }
    let l1_chain_id = numeric_value(config.get("l1_chain_id"), "l1_chain_id")?;
    let l2_chain_id = numeric_value(config.get("l2_chain_id"), "l2_chain_id")?;
    if l1_chain_id == 0 || l2_chain_id == 0 {
        anyhow::bail!("chain IDs must be positive");
    }
    if l1_chain_id == l2_chain_id {
        anyhow::bail!("l1_chain_id and l2_chain_id must differ");
    }
    non_zero_address(config.get("batch_inbox_address"), "batch_inbox_address")?;
    non_zero_address(
        config.get("deposit_contract_address"),
        "deposit_contract_address",
    )?;
    Ok(())
}

fn non_zero_u64(value: Option<&serde_json::Value>, name: &'static str) -> anyhow::Result<u64> {
    let value = numeric_value(value, name)?;
    if value == 0 {
        anyhow::bail!("{name} must be non-zero");
    }
    Ok(value)
}

fn numeric_value(value: Option<&serde_json::Value>, name: &'static str) -> anyhow::Result<u64> {
    match value {
        Some(serde_json::Value::Number(number)) => number
            .as_u64()
            .with_context(|| format!("{name} must be a positive integer")),
        Some(serde_json::Value::String(raw)) => raw
            .parse::<u64>()
            .with_context(|| format!("{name} must be a positive integer")),
        _ => anyhow::bail!("missing {name}"),
    }
}

fn non_empty_hash(value: Option<&serde_json::Value>, name: &'static str) -> anyhow::Result<()> {
    let raw = value
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("missing {name}"))?;
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .with_context(|| format!("{name} must be 0x-prefixed"))?;
    if hex.len() != 64 || !hex.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("{name} must be a 32-byte hex value");
    }
    if hex.as_bytes().iter().all(|byte| *byte == b'0') {
        anyhow::bail!("{name} must be non-zero");
    }
    Ok(())
}

fn non_zero_address(value: Option<&serde_json::Value>, name: &'static str) -> anyhow::Result<()> {
    let raw = value
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("missing {name}"))?;
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .with_context(|| format!("{name} must be 0x-prefixed"))?;
    if hex.len() != 40 || !hex.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("{name} must be a 20-byte hex address");
    }
    if hex.as_bytes().iter().all(|byte| *byte == b'0') {
        anyhow::bail!("{name} must be non-zero");
    }
    Ok(())
}

fn listen_addr(host_or_addr: &str, port: u16) -> anyhow::Result<SocketAddr> {
    if let Ok(addr) = host_or_addr.parse::<SocketAddr>() {
        return Ok(addr);
    }

    (host_or_addr, port)
        .to_socket_addrs()
        .context("resolving listen address")?
        .next()
        .context("listen address resolved to no socket addresses")
}

fn optional_string(raw: Option<&str>) -> Option<String> {
    raw.filter(|value| !value.is_empty()).map(ToOwned::to_owned)
}

fn parse_optional_url(raw: Option<&str>, flag: &'static str) -> anyhow::Result<Option<Url>> {
    let Some(raw) = optional_string(raw) else {
        return Ok(None);
    };
    raw.parse::<Url>()
        .with_context(|| format!("invalid {flag}: {raw:?}"))
        .map(Some)
}

fn parse_duration_arg(raw: &str) -> Result<Duration, String> {
    let trimmed = raw.trim();
    if !trimmed.is_empty() && trimmed.chars().all(|c| c.is_ascii_digit()) {
        let seconds = trimmed.parse::<u64>().map_err(|err| err.to_string())?;
        return Ok(Duration::from_secs(seconds));
    }
    humantime::parse_duration(trimmed).map_err(|err| err.to_string())
}

fn parse_log_level(raw: &str) -> Result<LogLevel, String> {
    match raw.to_ascii_lowercase().as_str() {
        "trace" | "trce" => Ok(LogLevel::Trace),
        "debug" | "dbug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" => Ok(LogLevel::Warn),
        "error" | "eror" => Ok(LogLevel::Error),
        "crit" => Ok(LogLevel::Crit),
        _ => Err(format!("unknown level: {raw}")),
    }
}

fn parse_execution_p2p_check_api(raw: &str) -> anyhow::Result<ExecutionP2pCheckApi> {
    match raw {
        "net" => Ok(ExecutionP2pCheckApi::Net),
        "admin" => Ok(ExecutionP2pCheckApi::Admin),
        _ => anyhow::bail!("invalid execution p2p check api: {raw}"),
    }
}

async fn run_control_loop<C, S>(
    conductor: std::sync::Arc<Conductor<C, S>>,
    interval: Duration,
) -> anyhow::Result<()>
where
    C: conductor_rs::Consensus + 'static,
    S: conductor_rs::sequencer::SequencerControl + 'static,
{
    let mut ticker = tokio::time::interval(std::cmp::max(interval, Duration::from_millis(1)));
    ticker.tick().await;
    let mut action_rx = conductor.subscribe_actions();
    let mut action_retry = Box::pin(tokio::time::sleep(Duration::ZERO));
    let mut action_pending = true;
    loop {
        tokio::select! {
            changed = action_rx.changed() => {
                if changed.is_err() || conductor.stopped() {
                    return Ok(());
                }
                if !action_pending {
                    action_retry.as_mut().reset(tokio::time::Instant::now());
                    action_pending = true;
                }
            }
            _ = &mut action_retry, if action_pending => {
                if conductor.stopped() {
                    return Ok(());
                }
                action_pending = match run_control_action_once(&conductor).await {
                    Ok(()) => false,
                    Err(err) => {
                        tracing::warn!(%err, "conductor control action failed; retrying");
                        action_retry.as_mut().reset(tokio::time::Instant::now() + action_retry_backoff());
                        true
                    }
                };
            }
            _ = ticker.tick() => {
                if conductor.stopped() {
                    return Ok(());
                }
                if let Err(err) = conductor.tick().await {
                    tracing::warn!(%err, "conductor control tick failed; retrying action");
                    action_retry.as_mut().reset(tokio::time::Instant::now() + action_retry_backoff());
                    action_pending = true;
                }
            }
        }
    }
}

async fn finalize_runtime_exit<C, S>(
    conductor: std::sync::Arc<Conductor<C, S>>,
    runtime_result: anyhow::Result<()>,
    pprof_file_profiler: Option<CpuFileProfiler>,
) -> anyhow::Result<()>
where
    C: conductor_rs::Consensus + 'static,
    S: conductor_rs::sequencer::SequencerControl + 'static,
{
    if let Err(runtime_err) = runtime_result {
        if let Err(stop_err) = conductor.stop().await {
            tracing::error!(%stop_err, "failed to stop conductor after runtime exit");
        }
        if let Err(profile_err) = finish_pprof_file_profile(pprof_file_profiler) {
            tracing::error!(%profile_err, "failed to finish pprof file profile after runtime exit");
        }
        return Err(runtime_err);
    }

    conductor.stop().await.context("stopping conductor")?;
    finish_pprof_file_profile(pprof_file_profiler)
}

fn start_pprof_file_profiler(
    profile_type: Option<PprofProfile>,
    path: Option<&Path>,
) -> anyhow::Result<Option<CpuFileProfiler>> {
    match profile_type {
        Some(PprofProfile::Cpu) => {
            let profiler = start_cpu_file_profiler(path)?;
            tracing::info!(
                path = %profiler.target().display(),
                "started CPU pprof file profiler"
            );
            Ok(Some(profiler))
        }
        Some(profile_type) => {
            tracing::warn!(
                ?profile_type,
                "pprof file profile type is Go-runtime-specific and not supported by conductor-rs"
            );
            Ok(None)
        }
        None => {
            if let Some(path) = path {
                tracing::debug!(
                    path = %path.display(),
                    "pprof.path is set without pprof.type; no file profile started"
                );
            }
            Ok(None)
        }
    }
}

fn finish_pprof_file_profile(pprof_file_profiler: Option<CpuFileProfiler>) -> anyhow::Result<()> {
    if let Some(profiler) = pprof_file_profiler {
        let target = profiler.finish()?;
        tracing::info!(path = %target.display(), "wrote CPU pprof file profile");
    }
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("installing SIGTERM handler")?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal.context("installing ctrl-c handler")?;
            Ok("interrupt")
        }
        _ = terminate.recv() => Ok("SIGTERM"),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    tokio::signal::ctrl_c()
        .await
        .context("installing ctrl-c handler")?;
    Ok("interrupt")
}

async fn run_control_action_once<C, S>(
    conductor: &std::sync::Arc<Conductor<C, S>>,
) -> Result<(), conductor_rs::conductor::ConductorError>
where
    C: conductor_rs::Consensus + 'static,
    S: conductor_rs::sequencer::SequencerControl + 'static,
{
    let started = std::time::Instant::now();
    let result = conductor.action().await;
    conductor
        .metrics()
        .record_loop_execution_time(started.elapsed());
    result
}

#[cfg(not(test))]
fn action_retry_backoff() -> Duration {
    use rand::Rng;

    Duration::from_millis(rand::thread_rng().gen_range(0..MAX_ACTION_RETRY_BACKOFF_MS))
}

#[cfg(test)]
fn action_retry_backoff() -> Duration {
    Duration::ZERO
}

async fn wait_for_conductor_enabled<S>(
    sequencer: &S,
    attempts: usize,
    delay: Duration,
) -> anyhow::Result<()>
where
    S: SequencerControl + ?Sized,
{
    let attempts = attempts.max(1);
    for attempt in 1..=attempts {
        match sequencer.conductor_enabled().await {
            Ok(true) => return Ok(()),
            Ok(false) => anyhow::bail!("conductor is not enabled on sequencer"),
            Err(SequencerError::Rpc(err)) if err.is_method_not_found() => {
                tracing::warn!("admin_conductorEnabled is not exposed by the node; continuing");
                return Ok(());
            }
            Err(err) if attempt < attempts => {
                tracing::warn!(%err, attempt, attempts, "checking sequencer conductor mode failed; retrying");
                tokio::time::sleep(delay).await;
            }
            Err(err) => return Err(err).context("checking sequencer conductor mode"),
        }
    }
    unreachable!("attempts is clamped to at least one");
}

async fn run_metrics_server<C, S>(
    metrics: std::sync::Arc<conductor_rs::ConductorMetrics>,
    addr: SocketAddr,
    enabled: bool,
    conductor: std::sync::Arc<Conductor<C, S>>,
) -> anyhow::Result<()>
where
    C: conductor_rs::Consensus + 'static,
    S: conductor_rs::sequencer::SequencerControl + 'static,
{
    if enabled {
        tracing::info!(addr = %addr, "starting conductor-rs metrics server");
        serve_metrics_with_shutdown(metrics, addr, async move {
            conductor.wait_stopped().await;
        })
        .await?;
    } else {
        std::future::pending::<()>().await;
    }
    Ok(())
}

async fn run_pprof_server<C, S>(
    addr: Option<SocketAddr>,
    conductor: std::sync::Arc<Conductor<C, S>>,
) -> anyhow::Result<()>
where
    C: conductor_rs::Consensus + 'static,
    S: conductor_rs::sequencer::SequencerControl + 'static,
{
    if let Some(addr) = addr {
        tracing::info!(addr = %addr, "starting conductor-rs pprof server");
        serve_pprof_with_shutdown(addr, async move {
            conductor.wait_stopped().await;
        })
        .await?;
    } else {
        std::future::pending::<()>().await;
    }
    Ok(())
}

async fn run_flashblocks_server<C, S>(
    rollupboost_ws_url: Option<Url>,
    websocket_server_port: u16,
    conductor: std::sync::Arc<Conductor<C, S>>,
) -> anyhow::Result<()>
where
    C: conductor_rs::Consensus + 'static,
    S: conductor_rs::sequencer::SequencerControl + 'static,
{
    let Some(rollup_boost_ws_url) = rollupboost_ws_url else {
        std::future::pending::<()>().await;
        return Ok(());
    };
    let listen_addr = SocketAddr::from(([0, 0, 0, 0], websocket_server_port));
    let metrics = conductor.metrics();
    let leader_conductor = conductor.clone();
    tracing::info!(addr = %listen_addr, upstream = %rollup_boost_ws_url, "starting flashblocks websocket server");
    serve_flashblocks(
        FlashblocksConfig {
            listen_addr,
            rollup_boost_ws_url,
        },
        metrics,
        std::sync::Arc::new(move || leader_conductor.leader()),
    )
    .await?;
    Ok(())
}

async fn run_leader_watch<S>(
    consensus: std::sync::Arc<RaftConsensus>,
    conductor: std::sync::Arc<Conductor<RaftConsensus, S>>,
) -> anyhow::Result<()>
where
    S: conductor_rs::sequencer::SequencerControl + 'static,
{
    let mut leader = consensus.leader();
    loop {
        leader = match consensus.wait_for_leader_status_change(leader).await {
            Ok(leader) => leader,
            Err(_) if conductor.stopped() => return Ok(()),
            Err(err) => return Err(err).context("watching raft leader status"),
        };
        if conductor.stopped() {
            return Ok(());
        }
        tracing::debug!(leader, "queuing conductor action for raft leader change");
        conductor.queue_action();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{error::ErrorKind, Parser};
    use conductor_rs::{
        consensus::LocalConsensus,
        rpc::RpcClientError,
        store::{FilePayloadStore, PayloadStore},
        types::{BlockInfo, Hash, L2BlockRef, PayloadEnvelope, PeerStats, SyncStatus},
        Health, State,
    };
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex,
        },
    };

    fn base_args() -> Vec<&'static str> {
        vec![
            "conductor-rs",
            "--node.rpc",
            "http://127.0.0.1:9545",
            "--execution.rpc",
            "http://127.0.0.1:8545",
            "--raft.server.id",
            "seq-a",
            "--raft.storage.dir",
            "/tmp/conductor-rs-test",
        ]
    }

    fn add_required_healthcheck_args(args: &mut Vec<&'static str>) {
        args.extend([
            "--healthcheck.interval",
            "1s",
            "--healthcheck.unsafe-interval",
            "60s",
            "--healthcheck.min-peer-count",
            "1",
        ]);
    }

    fn args_without_rollup_source() -> Vec<&'static str> {
        let mut args = base_args();
        add_required_healthcheck_args(&mut args);
        args
    }

    fn required_args() -> Vec<&'static str> {
        let mut args = args_without_rollup_source();
        args.extend(["--network", "op-mainnet"]);
        args
    }

    fn valid_rollup_config() -> &'static str {
        r#"{
          "genesis": {
            "l1": {
              "hash": "0xf39446e09aeca67452545d06a6e6a6a11184575ecf421f9306cf3602febf93ba",
              "number": 1
            },
            "l2": {
              "hash": "0x2a92ff72dad302d39fa80ef81522f0ccb27dc903255b618dfc4feddb22a8f80d",
              "number": 0
            },
            "l2_time": 1728358574,
            "system_config": {
              "batcherAddr": "0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc",
              "overhead": "0x0000000000000000000000000000000000000000000000000000000000000834",
              "scalar": "0x00000000000000000000000000000000000000000000000000000000000f4240",
              "gasLimit": 30000000
            }
          },
          "block_time": 2,
          "max_sequencer_drift": 300,
          "seq_window_size": 200,
          "channel_timeout": 120,
          "l1_chain_id": 900,
          "l2_chain_id": 901,
          "batch_inbox_address": "0xff00000000000000000000000000000000000901",
          "deposit_contract_address": "0x55bdfb0bfef1070c457124920546359426153833",
          "l1_system_config_address": "0x3649f526889a918af0a5498706db29e81bc91e0c"
        }"#
    }

    fn test_hash(byte: u8) -> Hash {
        format!("0x{}", hex::encode([byte; 32])).parse().unwrap()
    }

    fn test_payload(number: u64, byte: u8) -> PayloadEnvelope {
        PayloadEnvelope::new(serde_json::json!({
            "executionPayload": {
                "blockHash": test_hash(byte).to_string(),
                "blockNumber": format!("0x{number:x}")
            }
        }))
    }

    #[test]
    fn unknown_env_var_validation_matches_upstream_prefix_scan() {
        let defined_env_vars = BTreeSet::from([
            "OP_CONDUCTOR_NODE_RPC".to_string(),
            "OP_CONDUCTOR_EXECUTION_RPC".to_string(),
        ]);
        let provided_env_vars = [
            ("OP_CONDUCTOR_NODE_RPC", "http://127.0.0.1:9545"),
            ("OP_CONDUCTOR_EXECUTION_RPC", "http://127.0.0.1:8545"),
            ("OP_CONDUCTOR_FAKE", "false"),
            ("OP_CONDUCTORISH", "true"),
            ("LD_PRELOAD", "/lib/fake.so"),
        ];

        let invalids = unknown_env_vars(ENV_VAR_PREFIX, provided_env_vars, &defined_env_vars);

        assert_eq!(
            invalids,
            ["OP_CONDUCTOR_FAKE=false", "OP_CONDUCTORISH=true"]
        );
    }

    #[test]
    fn declared_env_vars_include_upstream_required_sources() {
        let defined_env_vars = defined_env_vars();

        for env_var in [
            "OP_CONDUCTOR_NODE_RPC",
            "OP_CONDUCTOR_EXECUTION_RPC",
            "OP_CONDUCTOR_RAFT_SERVER_ID",
            "OP_CONDUCTOR_RAFT_STORAGE_DIR",
            "OP_CONDUCTOR_HEALTHCHECK_INTERVAL",
        ] {
            assert!(defined_env_vars.contains(env_var), "{env_var}");
        }
    }

    #[test]
    fn upstream_compatibility_flags_parse() {
        let mut args = args_without_rollup_source();
        args.extend([
            "--rpc.addr",
            "127.0.0.1",
            "--rpc.port",
            "9555",
            "--rpc.enable-admin",
            "--log.level",
            "debug",
            "--log.format",
            "json",
            "--network",
            "op-mainnet",
            "--rollup.config",
            "/tmp/rollup.json",
            "--override.canyon",
            "1",
            "--override.pectrablobschedule",
            "2",
            "--pprof.enabled",
            "--pprof.addr",
            "127.0.0.1",
            "--pprof.port",
            "6061",
            "--pprof.type",
            "heap",
            "--raft.snapshot-interval",
            "120s",
            "--raft.round-robin-leader-transfer",
            "--rollupboost.ws-url",
            "ws://127.0.0.1:8080",
            "--websocket.server-port",
            "8546",
        ]);

        let parsed = Args::try_parse_from(args).unwrap();

        assert_eq!(parsed.raft_snapshot_interval, Duration::from_secs(120));
        assert!(parsed.raft_round_robin_leader_transfer);
        assert_eq!(parsed.websocket_server_port, 8546);
        assert_eq!(parsed.rpc_addr, "127.0.0.1");
        assert_eq!(parsed.rpc_port, 9555);
        assert!(parsed.rpc_enable_admin);
        assert_eq!(parsed.log_level, LogLevel::Debug);
        assert_eq!(parsed.log_format, LogFormat::Json);
        assert_eq!(parsed.network.as_deref(), Some("op-mainnet"));
        assert_eq!(
            parsed.rollup_config.as_deref(),
            Some(std::path::Path::new("/tmp/rollup.json"))
        );
        assert_eq!(parsed.override_canyon, Some(1));
        assert_eq!(parsed.override_pectrablobschedule, Some(2));
        assert!(parsed.pprof_enabled);
        assert_eq!(parsed.pprof_addr, "127.0.0.1");
        assert_eq!(parsed.pprof_port, 6061);
        assert_eq!(parsed.pprof_type, Some(PprofProfile::Heap));
        assert_eq!(
            listen_addr(&parsed.rpc_addr, parsed.rpc_port).unwrap(),
            "127.0.0.1:9555".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn sequencer_start_mode_defaults_to_hash_gated_upstream_shape() {
        let parsed = Args::try_parse_from(required_args()).unwrap();

        assert_eq!(parsed.start_mode, SequencerStartMode::HashParam);
    }

    #[test]
    fn log_level_accepts_upstream_aliases_and_case() {
        for (raw, expected) in [
            ("trace", LogLevel::Trace),
            ("TRCE", LogLevel::Trace),
            ("dbug", LogLevel::Debug),
            ("WARN", LogLevel::Warn),
            ("eror", LogLevel::Error),
            ("crit", LogLevel::Crit),
        ] {
            let mut args = required_args();
            args.extend(["--log.level", raw]);
            let parsed = Args::try_parse_from(args).unwrap();

            assert_eq!(parsed.log_level, expected);
        }
    }

    #[test]
    fn log_format_matches_upstream_supported_values() {
        for (raw, expected) in [
            ("text", LogFormat::Text),
            ("terminal", LogFormat::Terminal),
            ("logfmt", LogFormat::Logfmt),
            ("logfmtms", LogFormat::Logfmtms),
            ("json", LogFormat::Json),
            ("jsonms", LogFormat::Jsonms),
        ] {
            let mut args = required_args();
            args.extend(["--log.format", raw]);
            let parsed = Args::try_parse_from(args).unwrap();

            assert_eq!(parsed.log_format, expected);
        }

        let mut args = required_args();
        args.extend(["--log.format", "json-pretty"]);
        let err = Args::try_parse_from(args).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidValue);
    }

    #[test]
    fn proxy_default_matches_upstream_enabled_default() {
        let parsed = Args::try_parse_from(required_args()).unwrap();

        assert!(parsed.rpc_enable_proxy);
        assert_eq!(parsed.consensus_addr, "127.0.0.1");
        assert_eq!(parsed.rpc_addr, "0.0.0.0");
        assert_eq!(parsed.rpc_port, 8545);
    }

    #[test]
    fn optional_string_url_flags_accept_empty_values_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--consensus.advertised",
            "",
            "--supervisor.rpc",
            "",
            "--rollupboost.ws-url",
            "",
            "--rollup-boost.next-healthcheck-url",
            "",
            "--healthcheck.execution-p2p-rpc-url",
            "",
        ]);

        let parsed = Args::try_parse_from(args).unwrap();

        assert_eq!(
            optional_string(parsed.consensus_advertised.as_deref())
                .unwrap_or_else(|| "127.0.0.1:50050".to_string()),
            "127.0.0.1:50050"
        );
        assert!(
            parse_optional_url(parsed.supervisor_rpc.as_deref(), "supervisor.rpc")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_optional_url(parsed.rollupboost_ws_url.as_deref(), "rollupboost.ws-url")
                .unwrap()
                .is_none()
        );
        assert!(parse_optional_url(
            parsed.rollup_boost_next_healthcheck_url.as_deref(),
            "rollup-boost.next-healthcheck-url"
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn disabled_rollup_boost_next_ignores_unused_healthcheck_url_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--rollup-boost.next-healthcheck-url",
            "not a url that reqwest would accept",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        validate_args(&parsed).unwrap();

        assert!(rollup_boost_health_config(&parsed).unwrap().is_none());
    }

    #[test]
    fn rollup_boost_next_accepts_malformed_non_empty_url_until_healthcheck_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--rollup-boost.next-enabled",
            "--rollup-boost.next-healthcheck-url",
            "not a url that reqwest would accept",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        validate_args(&parsed).unwrap();

        let config = rollup_boost_health_config(&parsed).unwrap().unwrap();
        match config {
            RollupBoostHealthConfig::Json { url, .. } => {
                assert_eq!(url, "not a url that reqwest would accept");
            }
            other => panic!("expected rollup-boost next config, got {other:?}"),
        }
    }

    #[test]
    fn validation_rejects_rollup_boost_modes_mutually_exclusive_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--rollup-boost.enabled",
            "--rollup-boost.next-enabled",
            "--rollup-boost.next-healthcheck-url",
            "http://rollupboost.example",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(
            err.to_string(),
            "only one of rollup-boost or rollup-boost next healthchecks can be enabled"
        );
    }

    #[test]
    fn validation_rejects_missing_rollup_boost_next_url_like_upstream() {
        let mut args = required_args();
        args.push("--rollup-boost.next-enabled");
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "missing rollup-boost next healthcheck URL");
    }

    #[test]
    fn validation_rejects_rollup_boost_partial_tolerance_limit_without_interval_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--healthcheck.rollup-boost-partial-healthiness-tolerance-limit",
            "2",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(
            err.to_string(),
            "only one of RollupBoostPartialHealthinessToleranceLimit or RollupBoostPartialHealthinessToleranceIntervalSeconds found to be defined. Either define both of them or none."
        );
    }

    #[test]
    fn validation_rejects_rollup_boost_partial_tolerance_interval_without_limit_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--healthcheck.rollup-boost-partial-healthiness-tolerance-interval-seconds",
            "30",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(
            err.to_string(),
            "only one of RollupBoostPartialHealthinessToleranceLimit or RollupBoostPartialHealthinessToleranceIntervalSeconds found to be defined. Either define both of them or none."
        );
    }

    #[test]
    fn validation_accepts_rollup_boost_partial_tolerance_pair_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--healthcheck.rollup-boost-partial-healthiness-tolerance-limit",
            "2",
            "--healthcheck.rollup-boost-partial-healthiness-tolerance-interval-seconds",
            "30",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        validate_args(&parsed).unwrap();
    }

    #[test]
    fn bool_flags_accept_upstream_explicit_values() {
        let mut args = required_args();
        args.extend([
            "--log.color=true",
            "--log.pid=true",
            "--raft.bootstrap=false",
            "--rpc.enable-admin=true",
            "--rpc.enable-proxy=false",
            "--metrics.enabled=true",
            "--pprof.enabled=false",
            "--paused=false",
            "--raft.round-robin-leader-transfer=true",
            "--healthcheck.safe-enabled=true",
            "--healthcheck.execution-p2p-enabled=false",
            "--rollup-boost.enabled=false",
            "--rollup-boost.next-enabled=false",
        ]);

        let parsed = Args::try_parse_from(args).unwrap();

        assert!(parsed.log_color);
        assert!(parsed.log_pid);
        assert!(!parsed.raft_bootstrap);
        assert!(parsed.rpc_enable_admin);
        assert!(!parsed.rpc_enable_proxy);
        assert!(parsed.metrics_enabled);
        assert!(!parsed.pprof_enabled);
        assert!(!parsed.paused);
        assert!(parsed.raft_round_robin_leader_transfer);
        assert!(parsed.healthcheck_safe_enabled);
        assert!(!parsed.healthcheck_execution_p2p_enabled);
        assert!(!parsed.rollup_boost_enabled);
        assert!(!parsed.rollup_boost_next_enabled);
    }

    #[test]
    fn bool_flags_still_accept_bare_true_form() {
        let mut args = required_args();
        args.extend([
            "--rpc.enable-admin",
            "--metrics.enabled",
            "--raft.round-robin-leader-transfer",
        ]);

        let parsed = Args::try_parse_from(args).unwrap();

        assert!(parsed.rpc_enable_admin);
        assert!(parsed.metrics_enabled);
        assert!(parsed.raft_round_robin_leader_transfer);
    }

    #[test]
    fn rpc_addr_accepts_legacy_host_port_value() {
        let mut args = required_args();
        args.extend(["--rpc.addr", "127.0.0.1:9547", "--rpc.port", "9548"]);

        let parsed = Args::try_parse_from(args).unwrap();

        assert_eq!(
            listen_addr(&parsed.rpc_addr, parsed.rpc_port).unwrap(),
            "127.0.0.1:9547".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn validation_rejects_missing_rollup_config_source_like_upstream() {
        let parsed = Args::try_parse_from(args_without_rollup_source()).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(
            err.to_string(),
            "failed to load rollup config: missing network or rollup.config"
        );
    }

    #[test]
    fn validation_rejects_unknown_network_like_upstream() {
        let mut args = args_without_rollup_source();
        args.extend(["--network", "totally-made-up-mainnet"]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(
            err.to_string(),
            "invalid network: \"totally-made-up-mainnet\""
        );
    }

    #[test]
    fn validation_accepts_legacy_network_aliases_like_upstream() {
        for network in ["mainnet", "sepolia", "OP-MAINNET"] {
            let mut args = args_without_rollup_source();
            args.extend(["--network", network]);
            let parsed = Args::try_parse_from(args).unwrap();

            validate_args(&parsed).unwrap();
        }
    }

    #[test]
    fn known_rollup_networks_are_sorted_and_unique_like_upstream() {
        assert!(KNOWN_ROLLUP_NETWORKS
            .windows(2)
            .all(|pair| pair[0] < pair[1]));
        assert_eq!(
            KNOWN_ROLLUP_NETWORKS
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            KNOWN_ROLLUP_NETWORKS.len()
        );
    }

    #[test]
    fn validation_matches_current_op_geth_superchain_networks() {
        for network in [
            "base-devnet-0-sepolia-dev-0",
            "base-mainnet",
            "base-sepolia",
            "snax-mainnet",
            "worldchain-mainnet",
            "op-sepolia",
        ] {
            let mut args = args_without_rollup_source();
            args.extend(["--network", network]);
            let parsed = Args::try_parse_from(args).unwrap();

            validate_args(&parsed).unwrap();
        }

        for network in ["celo-sep-sepolia", "totally-made-up-mainnet"] {
            let mut args = args_without_rollup_source();
            args.extend(["--network", network]);
            let parsed = Args::try_parse_from(args).unwrap();

            assert_eq!(
                validate_args(&parsed).unwrap_err().to_string(),
                format!("invalid network: {network:?}")
            );
        }
    }

    #[test]
    fn validation_rejects_missing_rollup_config_file_like_upstream() {
        let mut args = args_without_rollup_source();
        args.extend(["--rollup.config", "/tmp/conductor-rs-missing-rollup.json"]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert!(err.to_string().starts_with("failed to read rollup config"));
    }

    #[test]
    fn validation_accepts_file_rollup_config_source_like_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollup.json");
        std::fs::write(&path, valid_rollup_config()).unwrap();
        let mut args = args_without_rollup_source();
        args.extend(["--rollup.config", path.to_str().unwrap()]);
        let parsed = Args::try_parse_from(args).unwrap();

        validate_args(&parsed).unwrap();
    }

    #[test]
    fn validation_accepts_current_world_chain_sequencer_manifest_flags() {
        let dir = tempfile::tempdir().unwrap();
        let rollup_config = dir.path().join("rollup.json");
        let raft_storage = dir.path().join("raft");
        std::fs::write(&rollup_config, valid_rollup_config()).unwrap();

        let args = vec![
            "conductor-rs".to_string(),
            "--raft.server.id=reth-0".to_string(),
            format!("--raft.storage.dir={}", raft_storage.display()),
            "--raft.bootstrap=true".to_string(),
            "--paused=true".to_string(),
            "--consensus.addr=0.0.0.0".to_string(),
            "--consensus.port=50050".to_string(),
            "--consensus.advertised=reth-0.world-chain-sequencer.svc.cluster.local:50050"
                .to_string(),
            "--rpc.addr=0.0.0.0".to_string(),
            "--rpc.port=8547".to_string(),
            "--rpc.enable-proxy".to_string(),
            "--node.rpc=http://127.0.0.1:9545".to_string(),
            "--execution.rpc=http://127.0.0.1:8545".to_string(),
            format!("--rollup.config={}", rollup_config.display()),
            "--healthcheck.interval=5".to_string(),
            "--healthcheck.unsafe-interval=60".to_string(),
            "--healthcheck.min-peer-count=1".to_string(),
            "--healthcheck.execution-p2p-enabled".to_string(),
            "--healthcheck.execution-p2p-min-peer-count=1".to_string(),
            "--metrics.enabled".to_string(),
            "--metrics.addr=0.0.0.0".to_string(),
            "--metrics.port=7301".to_string(),
            "--log.format=json".to_string(),
            "--log.level=info".to_string(),
        ];

        let parsed = Args::try_parse_from(args).unwrap();

        validate_args(&parsed).unwrap();
        assert!(parsed.raft_bootstrap);
        assert!(parsed.paused);
        assert!(parsed.rpc_enable_proxy);
        assert!(parsed.metrics_enabled);
        assert!(parsed.healthcheck_execution_p2p_enabled);
        assert_eq!(parsed.healthcheck_interval, Some(Duration::from_secs(5)));
        assert_eq!(
            parsed.healthcheck_unsafe_interval,
            Some(Duration::from_secs(60))
        );
        assert_eq!(parsed.healthcheck_execution_p2p_check_api, "net");
        assert_eq!(parsed.start_mode, SequencerStartMode::HashParam);
    }

    #[test]
    fn validation_rejects_invalid_rollup_config_file_like_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollup.json");
        std::fs::write(&path, "{}").unwrap();
        let mut args = args_without_rollup_source();
        args.extend(["--rollup.config", path.to_str().unwrap()]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert!(err.to_string().starts_with("invalid rollup config"));
    }

    #[test]
    fn validation_rejects_empty_consensus_addr_like_upstream() {
        let mut args = required_args();
        args.extend(["--consensus.addr", ""]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "missing consensus address");
    }

    #[test]
    fn validation_rejects_empty_raft_server_id_like_upstream() {
        let mut args = required_args();
        let server_id = args
            .iter()
            .position(|arg| *arg == "seq-a")
            .expect("test args include raft server id");
        args[server_id] = "";
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "missing raft server ID");
    }

    #[test]
    fn validation_rejects_empty_raft_storage_dir_like_upstream() {
        let mut args = required_args();
        let storage_dir = args
            .iter()
            .position(|arg| *arg == "/tmp/conductor-rs-test")
            .expect("test args include raft storage dir");
        args[storage_dir] = "";
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "missing raft storage directory");
    }

    #[derive(Debug)]
    struct FakeStartupSequencer {
        responses: Mutex<VecDeque<Result<bool, SequencerError>>>,
    }

    impl FakeStartupSequencer {
        fn new(responses: Vec<Result<bool, SequencerError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
            }
        }
    }

    #[async_trait::async_trait]
    impl SequencerControl for FakeStartupSequencer {
        async fn latest_unsafe_block(&self) -> Result<BlockInfo, SequencerError> {
            unimplemented!()
        }

        async fn start_sequencer(&self, _expected_hash: Hash) -> Result<(), SequencerError> {
            unimplemented!()
        }

        async fn stop_sequencer(&self) -> Result<Hash, SequencerError> {
            unimplemented!()
        }

        async fn sequencer_active(&self) -> Result<bool, SequencerError> {
            unimplemented!()
        }

        async fn sync_status(&self) -> Result<SyncStatus, SequencerError> {
            unimplemented!()
        }

        async fn peer_stats(&self) -> Result<PeerStats, SequencerError> {
            unimplemented!()
        }

        async fn post_unsafe_payload(
            &self,
            _payload: &PayloadEnvelope,
        ) -> Result<(), SequencerError> {
            unimplemented!()
        }

        async fn conductor_enabled(&self) -> Result<bool, SequencerError> {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("test response available")
        }
    }

    #[tokio::test]
    async fn conductor_enabled_startup_check_retries_transient_errors() {
        let sequencer = FakeStartupSequencer::new(vec![
            Err(SequencerError::Rpc(RpcClientError::InvalidResponse(
                "not ready".to_string(),
            ))),
            Ok(true),
        ]);

        wait_for_conductor_enabled(&sequencer, 2, Duration::from_millis(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn conductor_enabled_startup_check_accepts_missing_method() {
        let sequencer =
            FakeStartupSequencer::new(vec![Err(SequencerError::Rpc(RpcClientError::JsonRpc {
                code: -32601,
                message: "method not found".to_string(),
                data: None,
            }))]);

        wait_for_conductor_enabled(&sequencer, 2, Duration::from_millis(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn conductor_enabled_startup_check_rejects_disabled_sequencer() {
        let sequencer = FakeStartupSequencer::new(vec![Ok(false)]);

        let err = wait_for_conductor_enabled(&sequencer, 2, Duration::from_millis(1))
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "conductor is not enabled on sequencer");
    }

    #[derive(Debug)]
    struct FakeLoopSequencer {
        active: AtomicBool,
        starts: AtomicU64,
        sync_status_calls: AtomicU64,
        stop_failures: AtomicU64,
        stops: AtomicU64,
    }

    #[async_trait::async_trait]
    impl SequencerControl for FakeLoopSequencer {
        async fn latest_unsafe_block(&self) -> Result<BlockInfo, SequencerError> {
            Ok(BlockInfo {
                hash: test_hash(0x10),
                number: 10,
            })
        }

        async fn start_sequencer(&self, _expected_hash: Hash) -> Result<(), SequencerError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.active.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn stop_sequencer(&self) -> Result<Hash, SequencerError> {
            if self.stop_failures.load(Ordering::SeqCst) > 0 {
                self.stop_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(SequencerError::Rpc(RpcClientError::InvalidResponse(
                    "temporary stop failure".to_string(),
                )));
            }
            self.stops.fetch_add(1, Ordering::SeqCst);
            self.active.store(false, Ordering::SeqCst);
            Ok(Hash::ZERO)
        }

        async fn sequencer_active(&self) -> Result<bool, SequencerError> {
            Ok(self.active.load(Ordering::SeqCst))
        }

        async fn sync_status(&self) -> Result<SyncStatus, SequencerError> {
            self.sync_status_calls.fetch_add(1, Ordering::SeqCst);
            Ok(SyncStatus {
                unsafe_l2: L2BlockRef {
                    hash: Some(test_hash(0x10)),
                    number: 10,
                    time: 0,
                },
                safe_l2: L2BlockRef {
                    hash: Some(test_hash(0x0f)),
                    number: 9,
                    time: 0,
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

    fn loop_execution_count(metrics: &str) -> u64 {
        metrics
            .lines()
            .find_map(|line| {
                line.strip_prefix("op_conductor_loop_execution_time_count ")
                    .and_then(|value| value.parse().ok())
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn control_loop_runs_initial_action_before_first_health_check_like_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        store.commit(test_payload(10, 0x10)).await.unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeLoopSequencer {
            active: AtomicBool::new(false),
            starts: AtomicU64::new(0),
            sync_status_calls: AtomicU64::new(0),
            stop_failures: AtomicU64::new(0),
            stops: AtomicU64::new(0),
        });
        let conductor = Conductor::new(consensus, sequencer.clone(), ConductorConfig::default());
        conductor.initialize_startup_state().await.unwrap();

        let handle = tokio::spawn(run_control_loop(
            conductor.clone(),
            Duration::from_secs(60 * 60),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while sequencer.starts.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        conductor.stop().await.unwrap();
        handle.abort();

        assert!(sequencer.active.load(Ordering::SeqCst));
        assert_eq!(sequencer.starts.load(Ordering::SeqCst), 1);
        assert_eq!(sequencer.stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn control_loop_runs_resume_queued_action_before_next_health_check() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        store.commit(test_payload(10, 0x10)).await.unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeLoopSequencer {
            active: AtomicBool::new(false),
            starts: AtomicU64::new(0),
            sync_status_calls: AtomicU64::new(0),
            stop_failures: AtomicU64::new(0),
            stops: AtomicU64::new(0),
        });
        let conductor = Conductor::new(
            consensus,
            sequencer.clone(),
            ConductorConfig {
                start_paused: true,
                ..ConductorConfig::default()
            },
        );
        conductor.initialize_startup_state().await.unwrap();

        let handle = tokio::spawn(run_control_loop(
            conductor.clone(),
            Duration::from_secs(60 * 60),
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(sequencer.starts.load(Ordering::SeqCst), 0);

        conductor.resume().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while sequencer.starts.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        conductor.stop().await.unwrap();
        handle.abort();

        assert!(sequencer.active.load(Ordering::SeqCst));
        assert_eq!(sequencer.sync_status_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn control_loop_keeps_health_ticks_when_initial_action_keeps_failing() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeLoopSequencer {
            active: AtomicBool::new(false),
            starts: AtomicU64::new(0),
            sync_status_calls: AtomicU64::new(0),
            stop_failures: AtomicU64::new(0),
            stops: AtomicU64::new(0),
        });
        let conductor = Conductor::new(consensus, sequencer.clone(), ConductorConfig::default());
        conductor.initialize_startup_state().await.unwrap();

        let handle = tokio::spawn(run_control_loop(
            conductor.clone(),
            Duration::from_millis(10),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while sequencer.sync_status_calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        conductor.stop().await.unwrap();
        handle.abort();

        assert_eq!(sequencer.starts.load(Ordering::SeqCst), 0);
        assert!(sequencer.sync_status_calls.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn control_loop_retries_failed_action_before_next_health_interval() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        store.commit(test_payload(10, 0x10)).await.unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", false, store));
        let sequencer = Arc::new(FakeLoopSequencer {
            active: AtomicBool::new(true),
            starts: AtomicU64::new(0),
            sync_status_calls: AtomicU64::new(0),
            stop_failures: AtomicU64::new(1),
            stops: AtomicU64::new(0),
        });
        let conductor = Conductor::new(consensus, sequencer.clone(), ConductorConfig::default());
        conductor.initialize_startup_state().await.unwrap();

        let handle = tokio::spawn(run_control_loop(
            conductor.clone(),
            Duration::from_secs(60 * 60),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while sequencer.stops.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        conductor.stop().await.unwrap();
        handle.abort();

        assert!(!conductor.leader());
        assert!(!sequencer.active.load(Ordering::SeqCst));
        assert_eq!(sequencer.stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn control_loop_retries_health_recovery_wait_before_next_health_interval_like_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        store.commit(test_payload(10, 0x10)).await.unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeLoopSequencer {
            active: AtomicBool::new(true),
            starts: AtomicU64::new(0),
            sync_status_calls: AtomicU64::new(0),
            stop_failures: AtomicU64::new(0),
            stops: AtomicU64::new(0),
        });
        let conductor = Conductor::new(consensus, sequencer.clone(), ConductorConfig::default());
        conductor.initialize_startup_state().await.unwrap();
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
        assert_eq!(
            err.to_string(),
            "waiting for sequencing to become healthy by itself"
        );

        let handle = tokio::spawn(run_control_loop(
            conductor.clone(),
            Duration::from_secs(60 * 60),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while loop_execution_count(&conductor.metrics().render_prometheus()) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        conductor.stop().await.unwrap();
        handle.abort();

        assert_eq!(sequencer.stops.load(Ordering::SeqCst), 0);
        assert_eq!(sequencer.sync_status_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn leader_watch_exits_cleanly_after_conductor_stop() {
        let consensus = RaftConsensus::new_in_memory(
            "seq-a",
            "127.0.0.1:1001",
            conductor_rs::InMemoryRaftNetwork::new(),
        )
        .await
        .unwrap();
        consensus
            .initialize([("seq-a".to_string(), "127.0.0.1:1001".to_string())])
            .await
            .unwrap();
        consensus
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();
        let sequencer = Arc::new(FakeLoopSequencer {
            active: AtomicBool::new(false),
            starts: AtomicU64::new(0),
            sync_status_calls: AtomicU64::new(0),
            stop_failures: AtomicU64::new(0),
            stops: AtomicU64::new(0),
        });
        let conductor = Conductor::new(consensus.clone(), sequencer, ConductorConfig::default());

        conductor.stop().await.unwrap();

        run_leader_watch(consensus, conductor).await.unwrap();
    }

    #[tokio::test]
    async fn runtime_exit_stops_conductor_like_upstream_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        let consensus = Arc::new(LocalConsensus::new("seq-a", "127.0.0.1:0", true, store));
        let sequencer = Arc::new(FakeLoopSequencer {
            active: AtomicBool::new(false),
            starts: AtomicU64::new(0),
            sync_status_calls: AtomicU64::new(0),
            stop_failures: AtomicU64::new(0),
            stops: AtomicU64::new(0),
        });
        let conductor = Conductor::new(consensus, sequencer, ConductorConfig::default());

        finalize_runtime_exit(conductor.clone(), Ok(()), None)
            .await
            .unwrap();

        assert!(conductor.stopped());
    }

    #[test]
    fn validation_rejects_zero_healthcheck_interval_like_upstream() {
        let mut args = base_args();
        args.extend([
            "--network",
            "op-mainnet",
            "--healthcheck.interval",
            "0",
            "--healthcheck.unsafe-interval",
            "60s",
            "--healthcheck.min-peer-count",
            "1",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "missing health check interval");
    }

    #[test]
    fn validation_rejects_missing_healthcheck_interval_like_upstream() {
        let mut args = base_args();
        args.extend([
            "--network",
            "op-mainnet",
            "--healthcheck.unsafe-interval",
            "60s",
            "--healthcheck.min-peer-count",
            "1",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "flag healthcheck.interval is required");
    }

    #[test]
    fn validation_rejects_missing_unsafe_interval_like_upstream() {
        let mut args = base_args();
        args.extend([
            "--network",
            "op-mainnet",
            "--healthcheck.interval",
            "1s",
            "--healthcheck.min-peer-count",
            "1",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(
            err.to_string(),
            "flag healthcheck.unsafe-interval is required"
        );
    }

    #[test]
    fn validation_rejects_zero_min_peer_count_like_upstream() {
        let mut args = base_args();
        args.extend([
            "--network",
            "op-mainnet",
            "--healthcheck.interval",
            "1s",
            "--healthcheck.unsafe-interval",
            "60s",
            "--healthcheck.min-peer-count",
            "0",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "missing minimum peer count");
    }

    #[test]
    fn validation_rejects_missing_min_peer_count_like_upstream() {
        let mut args = base_args();
        args.extend([
            "--network",
            "op-mainnet",
            "--healthcheck.interval",
            "1s",
            "--healthcheck.unsafe-interval",
            "60s",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(
            err.to_string(),
            "flag healthcheck.min-peer-count is required"
        );
    }

    #[test]
    fn validation_rejects_enabled_el_p2p_without_min_peers_like_upstream() {
        let mut args = required_args();
        args.push("--healthcheck.execution-p2p-enabled");
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "missing minimum el p2p peers");
    }

    #[test]
    fn validation_accepts_enabled_el_p2p_without_rpc_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--healthcheck.execution-p2p-enabled",
            "--healthcheck.execution-p2p-min-peer-count",
            "1",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        validate_args(&parsed).unwrap();
        let cfg = execution_p2p_health_config(&parsed).unwrap().unwrap();

        assert_eq!(cfg.rpc, parsed.execution_rpc);
    }

    #[test]
    fn validation_accepts_empty_enabled_el_p2p_rpc_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--healthcheck.execution-p2p-enabled",
            "--healthcheck.execution-p2p-min-peer-count",
            "1",
            "--healthcheck.execution-p2p-rpc-url",
            "",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        validate_args(&parsed).unwrap();
        let cfg = execution_p2p_health_config(&parsed).unwrap().unwrap();

        assert_eq!(cfg.rpc, parsed.execution_rpc);
    }

    #[test]
    fn validation_rejects_invalid_el_p2p_check_api_like_upstream() {
        let mut args = required_args();
        args.extend([
            "--healthcheck.execution-p2p-enabled",
            "--healthcheck.execution-p2p-min-peer-count",
            "1",
            "--healthcheck.execution-p2p-rpc-url",
            "http://127.0.0.1:8545",
            "--healthcheck.execution-p2p-check-api",
            "trace",
        ]);
        let parsed = Args::try_parse_from(args).unwrap();

        let err = validate_args(&parsed).unwrap_err();

        assert_eq!(err.to_string(), "invalid el p2p check api");
    }
}
