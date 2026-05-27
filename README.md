# conductor-rs

Rust implementation of the OP Stack sequencer conductor compatibility surface.
The package name is `conductor-rs`, but the production binary is named
`op-conductor` on purpose: existing OP Stack manifests, scripts, and container
entrypoints expect to exec `op-conductor` directly.

## Scope

`conductor-rs` implements the operational surface needed to replace upstream
`op-conductor`:

- `conductor_*` JSON-RPC methods used by op-node and Kona-node.
- `admin_startSequencer(hash)` by default, with an explicit legacy
  parameterless Kona mode.
- Raft-backed leader election, membership changes, leader transfer, and
  replicated unsafe-payload state.
- Leader/follower start-stop control loop driven by sequencer health and active
  state.
- Health checks for unsafe/safe head freshness, consensus peers, optional
  execution P2P peers, optional supervisor status, and optional rollup-boost
  health.
- Prometheus metrics, op-service health endpoints, pprof endpoints, optional
  RPC proxying, and optional flashblocks websocket relay.

## Kona Compatibility

Upstream `op-conductor` starts a sequencer with `admin_startSequencer(hash)`.
`conductor-rs` uses that same call shape by default. Current Kona accepts the
hash parameter but does not enforce it today.

For explicit `--sequencer.start-mode=auto`, `conductor-rs` can fall back for
older Kona builds that reject the hash argument with `invalid params`: it first
checks the node unsafe head against the committed conductor unsafe head, then
makes the parameterless start call. That fallback avoids normal stale-candidate
starts, but the check and start are not atomic unless the node validates the
expected unsafe-head hash itself.

## Architecture

Startup lives in `src/main.rs`. It parses the upstream-compatible CLI, validates
the rollup source and required health flags, opens the Raft store, wires the
sequencer clients, and runs the RPC server, Raft transport, metrics/pprof,
flashblocks, leader watcher, and control loop in one Tokio runtime.

The conductor state machine lives in `src/conductor.rs`. It watches Raft
leadership, sequencer health, and sequencer active state. Leaders repair the
candidate node to the committed unsafe payload before starting it; followers
stop active sequencers. Unsafe payload commits are persisted through the
consensus layer so a new leader starts from the same unsafe-head decision.

Raft is implemented in `src/raft_consensus.rs` with
[`openraft`](https://crates.io/crates/openraft). OpenRaft gives the project a
Rust-native Raft implementation with async networking hooks, explicit
membership APIs, snapshots, and a typed state machine, so this repo only owns
the OP Stack-specific state and transport. The transport is HTTP/JSON over
Axum, exposing Raft append, vote, snapshot, and transfer endpoints between
conductor nodes. State is stored under `--raft.storage.dir` in a persistent JSON
store; snapshot triggering is driven explicitly to match the operator-facing
snapshot interval/threshold flags.

The sequencer and execution RPC clients live in `src/rpc/client.rs` and
`src/sequencer.rs`. The public JSON-RPC server lives in `src/rpc/server.rs`;
health, metrics, pprof, flashblocks, and payload persistence are split into
their own modules.

## Run

```sh
cargo run --bin op-conductor -- \
  --node.rpc http://127.0.0.1:9545 \
  --execution.rpc http://127.0.0.1:8545 \
  --network op-mainnet \
  --raft.server.id sequencer-a \
  --raft.storage.dir ./data \
  --raft.bootstrap \
  --consensus.addr 0.0.0.0 \
  --consensus.port 50050 \
  --consensus.advertised 10.0.0.10:50050 \
  --healthcheck.interval 1s \
  --healthcheck.unsafe-interval 60s \
  --healthcheck.min-peer-count 1
```

Start exactly one node with `--raft.bootstrap` to create the initial cluster.
Bring up additional nodes without bootstrap, then call
`conductor_addServerAsNonvoter` followed by `conductor_addServerAsVoter` using
each node's advertised consensus address. A freshly bootstrapped node starts
paused; call `conductor_resume` after membership and unsafe-head state are ready
for sequencing.

## Container Image

```sh
docker build -t conductor-rs:local .
docker run --rm conductor-rs:local op-conductor --help
```

The image includes the small shell-tool surface used by existing `op-conductor`
Kubernetes command wrappers before they exec the binary, including `sh`, `awk`,
and `nc`.

## Verification

```sh
scripts/verify-production-readiness.sh
```

The readiness script is self-contained in this repository. It runs format,
tests, clippy, a Docker image build, `op-conductor --help`, and a container
smoke check for the wrapper tools. It does not require an Optimism checkout,
submodule, or workspace-specific path.

Optional live checks are available through ignored integration tests:

```sh
CONDUCTOR_RS_RUN_LIVE=1 \
CONDUCTOR_RS_LIVE_KONA_NODE_RPC=http://127.0.0.1:9545 \
CONDUCTOR_RS_LIVE_KONA_EXECUTION_RPC=http://127.0.0.1:8545 \
scripts/verify-production-readiness.sh
```

For a live conductor cluster, set `CONDUCTOR_RS_LIVE_CONDUCTOR_RPCS` to a
comma-separated list of conductor RPC URLs. Add the narrower
`CONDUCTOR_RS_LIVE_*` flags from `tests/live_kona_conformance.rs` only when an
isolated devnet can safely mutate sequencer state.
