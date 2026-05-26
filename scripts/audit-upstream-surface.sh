#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -z "${OPTIMISM_ROOT:-}" ]; then
  echo "OPTIMISM_ROOT must point to an Optimism checkout for upstream surface auditing." >&2
  exit 1
fi

require_file() {
  if [ ! -f "$1" ]; then
    echo "missing required upstream source: $1" >&2
    exit 1
  fi
}

comm_missing() {
  local expected="$1"
  local actual="$2"
  comm -23 <(sort -u "$expected") <(sort -u "$actual")
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cd "$ROOT"

api_go="$OPTIMISM_ROOT/op-conductor/rpc/api.go"
flags_go="$OPTIMISM_ROOT/op-conductor/flags/flags.go"
config_go="$OPTIMISM_ROOT/op-conductor/conductor/config.go"
service_go="$OPTIMISM_ROOT/op-conductor/conductor/service.go"
forks_go="$OPTIMISM_ROOT/op-core/forks/forks.go"
op_node_config_docs="$OPTIMISM_ROOT/docs/public-docs/node-operators/reference/op-node-config.mdx"
op_service_rpc_cli="$OPTIMISM_ROOT/op-service/rpc/cli.go"
op_service_rpc_test="$OPTIMISM_ROOT/op-service/rpc/server_test.go"
op_service_metrics_cli="$OPTIMISM_ROOT/op-service/metrics/cli.go"
op_service_pprof_cli="$OPTIMISM_ROOT/op-service/oppprof/cli.go"
op_conductor_flashblocks_handler="$OPTIMISM_ROOT/op-conductor/rpc/ws/flashblocks_handler.go"
op_conductor_rollupboost="$OPTIMISM_ROOT/op-conductor/client/rollupboost.go"
op_conductor_rollupboost_next="$OPTIMISM_ROOT/op-conductor/client/rollupboost_next.go"

require_file "$api_go"
require_file "$flags_go"
require_file "$config_go"
require_file "$service_go"
require_file "$forks_go"
require_file "$op_node_config_docs"
require_file "$op_service_rpc_cli"
require_file "$op_service_rpc_test"
require_file "$op_service_metrics_cli"
require_file "$op_service_pprof_cli"
require_file "$op_conductor_flashblocks_handler"
require_file "$op_conductor_rollupboost"
require_file "$op_conductor_rollupboost_next"

perl -0ne '
sub rpc_name {
  my ($namespace, $method) = @_;
  $method = lc(substr($method, 0, 1)) . substr($method, 1);
  return $namespace . "_" . $method;
}
my %interfaces = (
  API => "conductor",
  ExecutionProxyAPI => "eth",
  ExecutionMinerProxyAPI => "miner",
  NodeProxyAPI => "optimism",
  NodeAdminProxyAPI => "admin",
);
for my $interface (sort keys %interfaces) {
  next unless /type\s+\Q$interface\E\s+interface\s*\{(.*?)\n\}/s;
  my $body = $1;
  while ($body =~ /^\s*([A-Z][A-Za-z0-9]*)\s*\(/mg) {
    print rpc_name($interfaces{$interface}, $1), "\n";
  }
}
' "$api_go" | sort -u > "$tmpdir/upstream_rpc_methods"

rg -o '"(conductor_[A-Za-z0-9]+|eth_[A-Za-z0-9]+|miner_[A-Za-z0-9]+|optimism_[A-Za-z0-9]+|admin_[A-Za-z0-9]+)"' \
  src/rpc/server.rs |
  sed 's/.*"\([^"]*\)".*/\1/' |
  sort -u > "$tmpdir/local_rpc_methods"

missing_methods="$(comm_missing "$tmpdir/upstream_rpc_methods" "$tmpdir/local_rpc_methods")"
if [ -n "$missing_methods" ]; then
  echo "Rust RPC server is missing upstream op-conductor methods:" >&2
  echo "$missing_methods" >&2
  exit 1
fi

{
  perl -ne 'print "$1\n" if /Name:\s*"([^"]+)"/' "$flags_go"
  perl -ne 'print "$1\n" if /^\s*[A-Za-z0-9_]+FlagName\s*=\s*"([^"]+)"/' \
    "$OPTIMISM_ROOT/op-service/rpc/cli.go" \
    "$OPTIMISM_ROOT/op-service/log/cli.go" \
    "$OPTIMISM_ROOT/op-service/metrics/cli.go" \
    "$OPTIMISM_ROOT/op-service/oppprof/cli.go" \
    "$OPTIMISM_ROOT/op-service/flags/flags.go"
  perl -0ne '
    my %name;
    while (/^\s*(\w+)\s+(?:Name\s+)?=\s+"([^"]+)"/mg) {
      $name{$1} = $2;
    }
    if (/var\s+All\s*=\s*\[\]Name\s*\{(.*?)\}/s) {
      my $seen_canyon = 0;
      while ($1 =~ /^\s*(\w+),/mg) {
        my $id = $1;
        $seen_canyon = 1 if $id eq "Canyon";
        print "override.$name{$id}\n" if $seen_canyon && exists $name{$id};
      }
    }
    if (/var\s+AllOpt\s*=\s*\[\]Name\s*\{(.*?)\}/s) {
      while ($1 =~ /^\s*(\w+),/mg) {
        my $id = $1;
        print "override.$name{$id}\n" if exists $name{$id};
      }
    }
  ' "$forks_go"
} | sort -u > "$tmpdir/upstream_flags"

{
  perl -ne 'print "OP_CONDUCTOR_$1\n" if /PrefixEnvVar\(\s*EnvVarPrefix\s*,\s*"([^"]+)"\s*\)/' "$flags_go"
  perl -ne 'print "OP_CONDUCTOR_$1\n" if /PrefixEnvVar\(\s*envPrefix\s*,\s*"([^"]+)"\s*\)/' \
    "$OPTIMISM_ROOT/op-service/rpc/cli.go" \
    "$OPTIMISM_ROOT/op-service/log/cli.go" \
    "$OPTIMISM_ROOT/op-service/metrics/cli.go" \
    "$OPTIMISM_ROOT/op-service/oppprof/cli.go" \
    "$OPTIMISM_ROOT/op-service/flags/flags.go"
  perl -0ne '
    my %name;
    while (/^\s*(\w+)\s+(?:Name\s+)?=\s+"([^"]+)"/mg) {
      $name{$1} = uc($2);
    }
    if (/var\s+All\s*=\s*\[\]Name\s*\{(.*?)\}/s) {
      my $seen_canyon = 0;
      while ($1 =~ /^\s*(\w+),/mg) {
        my $id = $1;
        $seen_canyon = 1 if $id eq "Canyon";
        print "OP_CONDUCTOR_OVERRIDE_$name{$id}\n" if $seen_canyon && exists $name{$id};
      }
    }
    if (/var\s+AllOpt\s*=\s*\[\]Name\s*\{(.*?)\}/s) {
      while ($1 =~ /^\s*(\w+),/mg) {
        my $id = $1;
        print "OP_CONDUCTOR_OVERRIDE_$name{$id}\n" if exists $name{$id};
      }
    }
  ' "$forks_go"
} | sort -u > "$tmpdir/upstream_env_vars"

cargo run --quiet --bin op-conductor -- --help > "$tmpdir/local_help"
perl -ne 'print "$1\n" if /^\s*--([^\s\[=<]+)/' "$tmpdir/local_help" | sort -u > "$tmpdir/local_flags"
perl -ne 'print "$1\n" while /\[env:\s*([A-Z0-9_]+)=\]/g' "$tmpdir/local_help" | sort -u > "$tmpdir/local_env_vars"

missing_flags="$(comm_missing "$tmpdir/upstream_flags" "$tmpdir/local_flags")"
if [ -n "$missing_flags" ]; then
  echo "Rust op-conductor CLI is missing upstream flags:" >&2
  echo "$missing_flags" >&2
  exit 1
fi

missing_env_vars="$(comm_missing "$tmpdir/upstream_env_vars" "$tmpdir/local_env_vars")"
if [ -n "$missing_env_vars" ]; then
  echo "Rust op-conductor CLI is missing upstream env vars:" >&2
  echo "$missing_env_vars" >&2
  exit 1
fi

perl -0ne '
if (/Available networks:\s*(.*?)\.\n\n<Tabs>/s) {
  my $body = $1;
  $body =~ s/\s+/ /g;
  for my $network (split /\s*,\s*/, $body) {
    $network =~ s/^\s+|\s+$//g;
    print "$network\n" if $network ne "";
  }
} else {
  die "failed to parse upstream op-node network list\n";
}
' "$op_node_config_docs" | sort -u > "$tmpdir/upstream_networks"

perl -0777 -ne '
if (/const KNOWN_ROLLUP_NETWORKS: &\[&str\] = &\[(.*?)\];/s) {
  my $body = $1;
  while ($body =~ /"([^"]+)"/g) {
    print "$1\n";
  }
} else {
  die "failed to parse local KNOWN_ROLLUP_NETWORKS\n";
}
' src/main.rs | sort -u > "$tmpdir/local_networks"

missing_networks="$(comm_missing "$tmpdir/upstream_networks" "$tmpdir/local_networks")"
if [ -n "$missing_networks" ]; then
  echo "Rust op-conductor network validation is missing upstream op-node networks:" >&2
  echo "$missing_networks" >&2
  exit 1
fi

extra_networks="$(comm_missing "$tmpdir/local_networks" "$tmpdir/upstream_networks")"
if [ -n "$extra_networks" ]; then
  echo "Rust op-conductor network validation accepts networks not listed by upstream op-node:" >&2
  echo "$extra_networks" >&2
  exit 1
fi

if rg -q 'supports websocket' "$op_service_rpc_test"; then
  if ! rg -q 'ws_handler' src/rpc/server.rs \
    || ! rg -q '\.route\("/ws"' src/rpc/server.rs \
    || ! rg -q '"/ws/"' src/rpc/server.rs \
    || ! rg -q 'websocket_rpc_supports_upstream_main_rpc_endpoint' src/rpc/server.rs; then
    echo "Upstream op-service RPC supports WebSocket JSON-RPC on /, /ws, and /ws/; Rust op-conductor must keep main-port WebSocket support and coverage." >&2
    exit 1
  fi
fi

if rg -q 'If not leader, avoid pulling messages' "$op_conductor_flashblocks_handler"; then
  if ! rg -q 'reconnect_waits_until_leader_like_upstream' src/flashblocks.rs; then
    echo "Upstream op-conductor waits until leadership before rollup-boost WebSocket reconnects; Rust op-conductor must keep coverage for that follower behavior." >&2
    exit 1
  fi
fi

if rg -q 'io.Copy\(io.Discard, resp.Body\)' "$op_conductor_rollupboost"; then
  if ! rg -q 'status_code_health_uses_status_when_body_drain_fails_like_upstream' src/health.rs; then
    echo "Upstream op-conductor ignores rollup-boost status-code health body drain errors; Rust op-conductor must keep coverage for that behavior." >&2
    exit 1
  fi
fi

if rg -q 'json.NewDecoder\(io.LimitReader\(resp.Body, 1<<20\)\).Decode' "$op_conductor_rollupboost_next"; then
  if ! rg -q 'json_health_decodes_first_json_value_like_upstream' src/health.rs; then
    echo "Upstream op-conductor rollup-boost-next health decodes the first limited JSON value; Rust op-conductor must keep coverage for that behavior." >&2
    exit 1
  fi
fi

if rg -q 'executionP2pRpcUrl = ctx.String\(flags.ExecutionRPC.Name\)' "$config_go"; then
  if ! rg -q 'validation_accepts_enabled_el_p2p_without_rpc_like_upstream' src/main.rs \
    || ! rg -q 'validation_accepts_empty_enabled_el_p2p_rpc_like_upstream' src/main.rs; then
    echo "Upstream op-conductor falls back to execution.rpc when the EL P2P healthcheck RPC URL is empty; Rust op-conductor must keep coverage for that behavior." >&2
    exit 1
  fi
fi

if rg -q 'strings.Contains\(errText, "method not found"\)' "$service_go"; then
  if ! rg -q 'method_not_found_detection_matches_upstream_code_or_message' src/rpc/client.rs; then
    echo "Upstream op-conductor treats admin_conductorEnabled text containing 'method not found' as a missing method; Rust op-conductor must keep coverage for that startup behavior." >&2
    exit 1
  fi
fi

if rg -q 'if !m.Enabled' "$op_service_metrics_cli" \
  && rg -q 'if !m.ListenEnabled' "$op_service_pprof_cli"; then
  if ! rg -q 'disabled_observability_servers_ignore_unused_invalid_ports_like_upstream' src/main.rs; then
    echo "Upstream metrics and pprof only validate ports when enabled; Rust op-conductor must keep coverage for disabled observability compatibility." >&2
    exit 1
  fi
fi

if rg -q 'ErrInvalidPort = errors.New\("invalid RPC port"\)' "$op_service_rpc_cli" \
  && rg -q 'ConsensusPort = &cli.IntFlag' "$flags_go"; then
  if ! rg -q 'validation_rejects_invalid_rpc_ports_like_upstream' src/main.rs; then
    echo "Upstream RPC and consensus ports use signed IntFlag values with invalid RPC port validation; Rust op-conductor must keep coverage for that CLI behavior." >&2
    exit 1
  fi
fi

if rg -q 'WebSocket server port invalid' "$op_conductor_flashblocks_handler"; then
  if ! rg -q 'disabled_flashblocks_ignores_unused_invalid_websocket_port_like_upstream' src/main.rs \
    || ! rg -q 'validation_rejects_enabled_flashblocks_invalid_websocket_port_like_upstream' src/main.rs; then
    echo "Upstream only constructs the flashblocks WebSocket server when rollupboost.ws-url is set; Rust op-conductor must keep coverage for enabled and disabled websocket port behavior." >&2
    exit 1
  fi
fi

echo "upstream surface audit passed: $(wc -l < "$tmpdir/upstream_rpc_methods" | tr -d " ") RPC methods, $(wc -l < "$tmpdir/upstream_flags" | tr -d " ") flags, $(wc -l < "$tmpdir/upstream_env_vars" | tr -d " ") env vars, $(wc -l < "$tmpdir/upstream_networks" | tr -d " ") networks"
