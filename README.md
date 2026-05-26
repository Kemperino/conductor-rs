# conductor-rs

Rust implementation of the OP Stack sequencer conductor compatibility surface.
The package builds a production `op-conductor` binary target so existing
manifests can exec it directly.

This crate is intentionally centered on the parts that matter for Kona-node
interop:

- `conductor_*` JSON-RPC methods used by op-node and Kona-node.
- `admin_startSequencer` support for the upstream hash-gated shape by default,
  with an explicit legacy parameterless Kona mode.
- OpenRaft-backed multi-node consensus with HTTP transport and persistent
  on-disk log/state.
- Persistent unsafe-payload tracking, with monotonic block-number updates.
- The op-conductor start/stop decision loop for leader, health, and active
  sequencer state.
- Upstream-style health checks using `optimism_syncStatus` unsafe/safe head
  freshness and `opp2p_peerStats` peer counts.
- Optional execution-layer P2P health checks using either `net_peerCount` or
  `admin_peers`.
- Optional supervisor liveness check through `supervisor_syncStatus`.
- Optional rollup-boost health checks using either the HTTP status-code
  `/healthz` contract or the newer JSON health endpoint.
- Optional rollup-boost partial-health tolerance matching upstream's
  count-per-interval behavior.
- Optional Prometheus metrics endpoint with upstream `op_conductor_*` metric
  names for health checks, state changes, leader transfers, and sequencer
  start/stop attempts.
- Optional flashblocks websocket relay: the leader reads from
  `rollupboost.ws-url` and serves downstream clients on `/ws`.
- Optional upstream-style RPC proxying for `eth_getBlockByNumber`,
  `miner_setMaxDASize`, `optimism_syncStatus`, `optimism_outputAtBlock`,
  `optimism_rollupConfig`, and `admin_sequencerActive`.
- Upstream op-service health surface: `GET /healthz` and `health_status`.

## Compatibility notes

`op-conductor` starts a sequencer with `admin_startSequencer(hash)`.
`conductor-rs` defaults to that same hash-parameter shape, and current Kona
accepts it even though it does not enforce the hash today. In explicit
`--sequencer.start-mode=auto`, `conductor-rs` can still fall back for older Kona
builds: if the node rejects the hash argument with `invalid params`, it verifies
the local unsafe head against the committed conductor unsafe head before making
the parameterless call.

That fallback keeps stale candidates from starting under normal operation, but
the check and start are not atomic unless the node itself validates the expected
unsafe-head hash.

## Run

```sh
cargo run --bin op-conductor -- \
  --node.rpc http://127.0.0.1:9545 \
  --execution.rpc http://127.0.0.1:8545 \
  --supervisor.rpc http://127.0.0.1:8555 \
  --network op-mainnet \
  --raft.server.id sequencer-a \
  --raft.storage.dir ./data \
  --raft.bootstrap \
  --consensus.addr 0.0.0.0 \
  --consensus.port 50050 \
  --consensus.advertised 10.0.0.10:50050 \
  --metrics.enabled \
  --metrics.addr 0.0.0.0 \
  --metrics.port 7300 \
  --rollupboost.ws-url ws://127.0.0.1:8080 \
  --websocket.server-port 8546 \
  --healthcheck.interval 1s \
  --healthcheck.unsafe-interval 60s \
  --healthcheck.min-peer-count 1 \
  --rpc.enable-proxy \
  --rpc.addr 0.0.0.0 \
  --rpc.port 8545
```

## Container Image

Build a local production-style image with:

```sh
docker build -t conductor-rs:local .
docker run --rm conductor-rs:local op-conductor --help
```

The image installs the small shell-tool surface used by current `op-conductor`
Kubernetes command wrappers before they exec the binary, including `sh`, `awk`,
and `nc`.

## Production Readiness Check

Run the local compatibility suite, lint gate, and container build smoke test
with:

```sh
scripts/verify-production-readiness.sh
```

The default readiness check is self-contained in this repository. It does not
require an Optimism checkout, submodule, or workspace-specific path.

To include live endpoint conformance, set
`CONDUCTOR_RS_RUN_LIVE=1` with the relevant `CONDUCTOR_RS_LIVE_*` variables
from the live validation section below.

Start exactly one node with `--raft.bootstrap` to create the initial cluster.
Bring up additional nodes without bootstrap, then call
`conductor_addServerAsNonvoter` followed by `conductor_addServerAsVoter` through
the conductor JSON-RPC API, using each node's advertised consensus address. A
freshly bootstrapped node starts paused like upstream op-conductor; call
`conductor_resume` after the initial membership and unsafe-head state are ready
for sequencing. Reusing an initialized data directory with `--raft.bootstrap`
does not pause the node again.

At startup the binary checks `admin_conductorEnabled` on the configured node,
matching upstream op-conductor behavior. If the method is absent it logs a
warning and continues for older nodes. The control loop then refreshes sequencer
state on `--healthcheck.interval` and applies the leader/follower start-stop
policy. Bare numeric healthcheck durations are interpreted as seconds, matching
upstream op-conductor flags; duration strings such as `750ms` or `2m` are also
accepted.

The startup CLI accepts the upstream op-service flags used by existing
op-conductor manifests, including `log.*`, `rpc.addr` plus `rpc.port`,
`network`, `rollup.config`, fork `override.*`, and `pprof.*` flags. The legacy
single-value `--rpc.addr host:port` form is still accepted as a transition aid.
Boolean flags accept both the bare upstream form, such as `--metrics.enabled`,
and explicit values, such as `--rpc.enable-proxy=false`.
As in upstream op-conductor, startup requires either `--network` or
`--rollup.config`; named networks are validated against the current op-node
network list, with `mainnet` and `sepolia` accepted as legacy aliases. If both
are set, the named network wins. The upstream-required healthcheck flags
`--healthcheck.interval`, `--healthcheck.unsafe-interval`, and
`--healthcheck.min-peer-count` must also be set.
When `--pprof.enabled` is set, conductor-rs serves the upstream
`/debug/pprof/` route surface on `--pprof.addr` and `--pprof.port`.
`/debug/pprof/profile` returns a protobuf CPU profile, and
`--pprof.type=cpu --pprof.path <path>` writes a CPU profile on shutdown. Go
runtime-only profile bodies are reported as unsupported because they do not
exist in the Rust runtime.

`--rollup-boost.enabled` appends `/healthz` to `--execution.rpc` and maps HTTP
200/206/503 to healthy/partial/unhealthy. `--rollup-boost.next-enabled` instead
uses `--rollup-boost.next-healthcheck-url` and reads the JSON
`rollup_boost_health` value.

`--healthcheck.execution-p2p-enabled` adds an execution-layer P2P check. It
uses `--healthcheck.execution-p2p-rpc-url` when set, otherwise falling back to
`--execution.rpc`. It compares the peer count against
`--healthcheck.execution-p2p-min-peer-count`, and supports
`--healthcheck.execution-p2p-check-api net|admin`.

`--metrics.enabled` serves Prometheus text on `/metrics` using
`--metrics.addr` and `--metrics.port`.

When `--rollupboost.ws-url` is set, conductor-rs serves a websocket endpoint at
`/ws` on `--websocket.server-port`. It keeps the upstream rollup-boost
websocket connected, reads only while this conductor is the leader, and drops
slow downstream clients instead of allowing one blocked client to stall
broadcasts.

Partial rollup-boost responses can be tolerated with
`--healthcheck.rollup-boost-partial-healthiness-tolerance-limit` and
`--healthcheck.rollup-boost-partial-healthiness-tolerance-interval-seconds`.
Set both flags together; within each interval bucket, the first `limit` partial
responses are treated as healthy and the next partial response triggers the
same immediate transfer behavior as upstream op-conductor.

When `--rpc.enable-proxy` is set, the conductor RPC server also exposes the
proxy namespaces used by upstream op-conductor. Execution and node read methods
return useful results only from the current conductor leader; `miner_setMaxDASize`
is forwarded directly, matching the upstream miner proxy behavior.

The conductor RPC server also exposes `GET /healthz` and JSON-RPC
`health_status`, matching the default op-service health surface.

## Live Kona validation

The default test suite uses deterministic fake Kona endpoints so it can run
locally without a full OP Stack devnet. Real HA readiness still needs a live
Kona/conductor topology check. The ignored integration tests in
`tests/live_kona_conformance.rs` provide optional live gates:

```sh
CONDUCTOR_RS_LIVE_KONA_NODE_RPC=http://127.0.0.1:9545 \
CONDUCTOR_RS_LIVE_KONA_EXECUTION_RPC=http://127.0.0.1:8545 \
cargo test --test live_kona_conformance \
  live_kona_admin_rpc_supports_current_interop -- --ignored
```

That read-only check verifies `admin_conductorEnabled`,
`admin_sequencerActive`, `optimism_syncStatus`, `opp2p_peerStats`, and the
execution latest-head lookup used by the conductor. To prove unsafe-head repair
against a real Kona node, set
`CONDUCTOR_RS_LIVE_KONA_UNSAFE_PAYLOAD_FILE=/path/to/payload.json`; the file
must contain the `ExecutionPayloadEnvelope` JSON passed to
`admin_postUnsafePayload`, and the test verifies the execution latest head after
the call. To prove Kona's `admin_overrideLeader` path against the configured
conductor, set `CONDUCTOR_RS_LIVE_KONA_OVERRIDE_CHECK=1` and
`CONDUCTOR_RS_LIVE_KONA_CONDUCTOR_RPC=http://...`; the test requires no existing
override, calls the Kona admin method, verifies `conductor_leaderOverridden`,
and clears the override afterward. To prove start activation, set
`CONDUCTOR_RS_LIVE_KONA_START_CHECK=1` to call `admin_startSequencer(hash)` with
the current execution unsafe head. If the start check should clean up after
itself, also set `CONDUCTOR_RS_LIVE_KONA_STOP_AFTER_START=1`.

For a running conductor cluster:

```sh
CONDUCTOR_RS_LIVE_CONDUCTOR_RPCS=http://127.0.0.1:8545,http://127.0.0.1:8547,http://127.0.0.1:8549 \
cargo test --test live_kona_conformance \
  live_conductor_cluster_exposes_upstream_ha_contract -- --ignored
```

That check verifies the upstream health surface, leader override state,
exactly-one-leader reporting, active state, and membership. On an isolated
cluster, add `CONDUCTOR_RS_LIVE_TRANSFER_CHECK=1` to call
`conductor_transferLeader` and wait for a different leader.
