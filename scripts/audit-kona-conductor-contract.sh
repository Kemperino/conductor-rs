#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPTIMISM_ROOT="${OPTIMISM_ROOT:-$HOME/rust/optimism}"

require_file() {
  if [ ! -f "$1" ]; then
    echo "missing required Kona source: $1" >&2
    exit 1
  fi
}

require_source() {
  local file="$1"
  local desc="$2"
  local pattern="$3"
  if ! perl -0777 -e 'my ($pattern) = @ARGV; my $src = do { local $/; <STDIN> }; exit($src =~ /$pattern/s ? 0 : 1)' "$pattern" < "$file"; then
    echo "Kona conductor contract check failed: $desc" >&2
    echo "  file: $file" >&2
    exit 1
  fi
}

require_order() {
  local file="$1"
  local desc="$2"
  local first="$3"
  local second="$4"
  if ! perl -0777 -e 'my ($first, $second) = @ARGV; my $src = do { local $/; <STDIN> }; my $a = index($src, $first); my $b = index($src, $second); exit($a >= 0 && $b >= 0 && $a < $b ? 0 : 1)' "$first" "$second" < "$file"; then
    echo "Kona conductor contract order check failed: $desc" >&2
    echo "  file: $file" >&2
    exit 1
  fi
}

rpc_admin="$OPTIMISM_ROOT/rust/kona/crates/node/rpc/src/admin.rs"
rpc_client="$OPTIMISM_ROOT/rust/kona/crates/node/rpc/src/client.rs"
rpc_jsonrpsee="$OPTIMISM_ROOT/rust/kona/crates/node/rpc/src/jsonrpsee.rs"
sequencer_rpc_client="$OPTIMISM_ROOT/rust/kona/crates/node/service/src/actors/rpc/sequencer_rpc_client.rs"
sequencer_config="$OPTIMISM_ROOT/rust/kona/crates/node/service/src/actors/sequencer/config.rs"
sequencer_actor="$OPTIMISM_ROOT/rust/kona/crates/node/service/src/actors/sequencer/actor.rs"
sequencer_admin="$OPTIMISM_ROOT/rust/kona/crates/node/service/src/actors/sequencer/admin_api_impl.rs"
sequencer_conductor="$OPTIMISM_ROOT/rust/kona/crates/node/service/src/actors/sequencer/conductor.rs"
sequencer_flags="$OPTIMISM_ROOT/rust/kona/bin/node/src/flags/sequencer.rs"
service_node="$OPTIMISM_ROOT/rust/kona/crates/node/service/src/service/node.rs"
actor_tests="$OPTIMISM_ROOT/rust/kona/crates/node/service/src/actors/sequencer/tests/actor_test.rs"
admin_tests="$OPTIMISM_ROOT/rust/kona/crates/node/service/src/actors/sequencer/tests/admin_api_impl_test.rs"

for file in \
  "$rpc_admin" \
  "$rpc_client" \
  "$rpc_jsonrpsee" \
  "$sequencer_rpc_client" \
  "$sequencer_config" \
  "$sequencer_actor" \
  "$sequencer_admin" \
  "$sequencer_conductor" \
  "$sequencer_flags" \
  "$service_node" \
  "$actor_tests" \
  "$admin_tests"; do
  require_file "$file"
done

require_source "$rpc_jsonrpsee" "admin_startSequencer must accept the upstream conductor block hash" \
  'async\s+fn\s+admin_start_sequencer\s*\(\s*&self\s*,\s*expected_hash:\s*Option<B256>,?\s*\)\s*->\s*RpcResult<\(\)>;'
require_source "$rpc_client" "sequencer admin client must carry the optional start hash" \
  'async\s+fn\s+start_sequencer\s*\(\s*&self\s*,\s*expected_hash:\s*Option<B256>,?\s*\)\s*->\s*Result<\(\),\s*SequencerAdminAPIError>'
require_source "$sequencer_admin" "sequencer actor query must preserve the optional start hash" \
  'StartSequencer\(Option<B256>,\s*oneshot::Sender<Result<\(\),\s*SequencerAdminAPIError>>\)'
require_source "$sequencer_rpc_client" "queued admin client must forward the start hash" \
  'SequencerAdminQuery::StartSequencer\(expected_hash,\s*tx\)'
require_source "$sequencer_admin" "startSequencer must reject stale conductor start hashes" \
  'if\s+let\s+Some\(expected_hash\)\s*=\s*expected_hash\s*\{.*get_unsafe_head\(\)\.await.*if\s+actual\s*!=\s*expected_hash.*UnsafeHeadMismatch\s*\{\s*expected_hash,\s*actual\s*\}'

require_source "$rpc_admin" "admin_postUnsafePayload must reconstruct the payload block hash" \
  'try_into_block_with_sidecar::<OpTxEnvelope>.*hash_slow\(\)'
require_source "$rpc_admin" "admin_postUnsafePayload must reject payload hash mismatches" \
  'if\s+actual\s*!=\s*expected\s*\{.*payload has bad block hash'
require_order "$rpc_admin" "payload hash validation must happen before forwarding to the network actor" \
  'validate_unsafe_payload_block_hash(&payload)?;' \
  '.send(NetworkAdminQuery::PostUnsafePayload { payload })'

require_source "$sequencer_conductor" "Kona must commit unsafe payloads through the conductor RPC method" \
  'request\("conductor_commitUnsafePayload",\s*\[payload\]\)'
require_source "$sequencer_conductor" "Kona must propagate the upstream overrideLeader boolean parameter" \
  'request\("conductor_overrideLeader",\s*\[true\]\)'
require_source "$sequencer_conductor" "overrideLeader parameter compatibility must be unit tested" \
  'async\s+fn\s+override_leader_sends_upstream_bool_param\(\)'
require_source "$sequencer_flags" "sequencer CLI config must preserve conductor RPC timeout" \
  'conductor_rpc_timeout:\s*self\.conductor_rpc_timeout'
require_source "$sequencer_config" "sequencer config must carry conductor RPC timeout" \
  'pub\s+conductor_rpc_timeout:\s*Duration'
require_source "$service_node" "service startup must pass configured conductor RPC timeout" \
  'ConductorClient::new_http\(url,\s*self\.sequencer_config\.conductor_rpc_timeout\)'
require_source "$sequencer_conductor" "conductor RPC client must apply configured HTTP timeout" \
  'reqwest::Client::builder\(\)\.timeout\(timeout\)\.build\(\)\?.*ReqwestClient::new_http_with_client\(client,\s*url\)'
require_source "$sequencer_conductor" "conductor RPC timeout behavior must be unit tested" \
  'async\s+fn\s+conductor_rpc_calls_use_configured_timeout\(\)'

require_source "$sequencer_actor" "sequencing must honor local conductor leader override" \
  'if\s+!self\.conductor_leader_overridden\s*\{.*commit_unsafe_payload\(&payload\)\.await'
require_order "$sequencer_actor" "failed conductor commits must stop before unsafe payload gossip" \
  'return Err(err.into());' \
  'schedule_execution_payload_gossip(payload)'
require_source "$sequencer_admin" "admin overrideLeader must set the local disaster-recovery override" \
  'self\.conductor_leader_overridden\s*=\s*true;.*conductor\.override_leader\(\)\.await'

require_source "$actor_tests" "fail-closed conductor commit behavior must stay covered" \
  'test_seal_and_commit_does_not_gossip_if_conductor_commit_fails'
require_source "$actor_tests" "local override bypass behavior must stay covered" \
  'test_seal_and_commit_skips_conductor_when_leader_is_overridden'
require_source "$admin_tests" "hash-gated startSequencer behavior must stay covered" \
  'test_start_sequencer_checks_expected_unsafe_head'
require_source "$admin_tests" "overrideLeader behavior must stay covered" \
  'test_override_leader'
require_source "$ROOT/tests/binary_cluster_failover.rs" "binary raft membership demote/remove behavior must stay covered" \
  'conductor_binary_demotes_current_leader_and_removes_raft_member'
require_source "$rpc_admin" "admin_postUnsafePayload must stay covered for V2/V3/V4 conductor repair payloads" \
  'admin_post_unsafe_payload_accepts_current_fork_payload_versions'
require_source "$rpc_admin" "admin_postUnsafePayload must reject current-fork payloads without parent beacon roots" \
  'unsafe_payload_hash_validation_rejects_cancun_payload_without_parent_beacon_root'

echo "Kona conductor contract audit passed"
