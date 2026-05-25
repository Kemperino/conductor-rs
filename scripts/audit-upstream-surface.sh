#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPTIMISM_ROOT="${OPTIMISM_ROOT:-$HOME/rust/optimism}"

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
forks_go="$OPTIMISM_ROOT/op-core/forks/forks.go"

require_file "$api_go"
require_file "$flags_go"
require_file "$forks_go"

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

echo "upstream surface audit passed: $(wc -l < "$tmpdir/upstream_rpc_methods" | tr -d " ") RPC methods, $(wc -l < "$tmpdir/upstream_flags" | tr -d " ") flags, $(wc -l < "$tmpdir/upstream_env_vars" | tr -d " ") env vars"
