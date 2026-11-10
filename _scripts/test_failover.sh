#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PODMAN=${PODMAN:-podman}
VALKEY_IMAGE=${VALKEY_IMAGE:-docker.io/valkey/valkey:8-alpine}

if ! command -v "$PODMAN" >/dev/null 2>&1; then
  echo "container runtime not found: $PODMAN" >&2
  exit 1
fi

work=$(mktemp -d /tmp/opencode/valkey-sentinel-proxy-failover.XXXXXX)
suffix=$$
primary=vsp-it-primary-$suffix
replica=vsp-it-replica-$suffix
sentinel1=vsp-it-sentinel1-$suffix
sentinel2=vsp-it-sentinel2-$suffix
sentinel3=vsp-it-sentinel3-$suffix
proxy_pid=
idle_client_pid=

cleanup() {
  status=$?
  if [[ $status -ne 0 ]]; then
    echo "--- proxy log ---" >&2
    cat "$work/proxy.log" >&2 2>/dev/null || true
    for container in "$primary" "$replica" "$sentinel1" "$sentinel2" "$sentinel3"; do
      echo "--- $container log ---" >&2
      "$PODMAN" logs "$container" >&2 2>/dev/null || true
    done
  fi
  if [[ -n "${idle_client_pid:-}" ]]; then
    kill "$idle_client_pid" 2>/dev/null || true
    wait "$idle_client_pid" 2>/dev/null || true
  fi
  if [[ -n "${proxy_pid:-}" ]]; then
    kill "$proxy_pid" 2>/dev/null || true
    wait "$proxy_pid" 2>/dev/null || true
  fi
  "$PODMAN" rm -f "$primary" "$replica" "$sentinel1" "$sentinel2" "$sentinel3" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT INT TERM

read -r primary_port replica_port sentinel1_port sentinel2_port sentinel3_port proxy_port < <(python3 - <<'PY'
import socket
sockets = []
ports = []
for _ in range(6):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    sockets.append(s)
    ports.append(s.getsockname()[1])
print(" ".join(map(str, ports)))
for s in sockets:
    s.close()
PY
)

cli() {
  local container=$1 port=$2
  shift 2
  "$PODMAN" exec "$container" valkey-cli --raw -h 127.0.0.1 -p "$port" "$@"
}

wait_for() {
  local description=$1
  shift
  for _ in $(seq 1 150); do
    if "$@"; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for $description" >&2
  return 1
}

start_sentinel() {
  local name=$1
  local port=$2
  local dir="$work/$name"
  local conf="$dir/sentinel.conf"
  mkdir -p "$dir"
  cat >"$conf" <<EOF
bind 127.0.0.1
protected-mode no
port $port
dir /sentinel
sentinel monitor mymaster 127.0.0.1 $primary_port 2
sentinel down-after-milliseconds mymaster 1000
sentinel failover-timeout mymaster 10000
sentinel parallel-syncs mymaster 1
EOF
  chmod 777 "$dir"
  chmod 666 "$conf"
  "$PODMAN" run -d --name "$name" --network host \
    -v "$dir:/sentinel:Z" "$VALKEY_IMAGE" \
    valkey-server /sentinel/sentinel.conf --sentinel >/dev/null
}

replication_connected() {
  [[ $(cli "$replica" "$replica_port" ROLE 2>/dev/null | sed -n '4p') == connected ]]
}

quorum_ready() {
  [[ $(cli "$sentinel2" "$sentinel2_port" SENTINEL CKQUORUM mymaster 2>/dev/null) == OK\ 3* ]]
}

proxy_role_is_master() {
  [[ $(cli "$primary" "$proxy_port" ROLE 2>/dev/null | sed -n '1p') == master ]]
}

cat >"$work/idle_client.py" <<'PY'
import socket
import sys

port = int(sys.argv[1])
log_path = sys.argv[2]
command = b"*1\r\n$4\r\nROLE\r\n"
with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
    conn.settimeout(5)
    conn.sendall(command)
    response = conn.recv(4096)
    role = "master" if b"master" in response else "slave" if b"slave" in response else "unknown"
    with open(log_path, "a", buffering=1) as log:
        log.write("FIRST_ROLE=" + role + "\n")
        conn.settimeout(15)
        try:
            extra = conn.recv(1)
            log.write("CLOSED=1\n" if extra == b"" else "CLOSED=0\n")
        except (TimeoutError, socket.timeout):
            log.write("CLOSED=0\n")
PY

start_idle_client() {
  local log=$1
  : >"$log"
  python3 "$work/idle_client.py" "$proxy_port" "$log" &
  idle_client_pid=$!
  wait_for "idle client initial ROLE" grep -q '^FIRST_ROLE=master$' "$log"
}

assert_idle_client_closed() {
  local log=$1
  wait "$idle_client_pid"
  idle_client_pid=
  if ! grep -q '^CLOSED=1$' "$log"; then
    cat "$log" >&2
    echo "idle proxy connection was not closed after failover" >&2
    return 1
  fi
}

printf 'starting real Valkey failover environment (A=%s B=%s S1=%s S2=%s S3=%s proxy=%s)\n' "$primary_port" "$replica_port" "$sentinel1_port" "$sentinel2_port" "$sentinel3_port" "$proxy_port"
"$PODMAN" run -d --name "$primary" --network host "$VALKEY_IMAGE" \
  valkey-server --bind 127.0.0.1 --protected-mode no --port "$primary_port" \
  --save '' --appendonly no >/dev/null
"$PODMAN" run -d --name "$replica" --network host "$VALKEY_IMAGE" \
  valkey-server --bind 127.0.0.1 --protected-mode no --port "$replica_port" \
  --save '' --appendonly no --replicaof 127.0.0.1 "$primary_port" >/dev/null
start_sentinel "$sentinel1" "$sentinel1_port"
start_sentinel "$sentinel2" "$sentinel2_port"
start_sentinel "$sentinel3" "$sentinel3_port"
wait_for "replica synchronization" replication_connected
wait_for "three-Sentinel quorum" quorum_ready

go build -o "$work/valkey-sentinel-proxy" "$ROOT/cmd"
"$work/valkey-sentinel-proxy" \
  -listen "127.0.0.1:$proxy_port" \
  -master mymaster \
  -sentinel-addr "127.0.0.1:$sentinel1_port,127.0.0.1:$sentinel2_port,127.0.0.1:$sentinel3_port" \
  -topology-refresh-interval 200ms >"$work/proxy.log" 2>&1 &
proxy_pid=$!
wait_for "initial primary through proxy" proxy_role_is_master
printf 'PASS: initial connection resolves to master\n'

start_idle_client "$work/idle-round-1.log"
failover_reply=$(cli "$sentinel2" "$sentinel2_port" SENTINEL FAILOVER mymaster)
[[ $failover_reply == OK ]]
for _ in $(seq 1 150); do
  a_role=$(cli "$primary" "$primary_port" ROLE | sed -n '1p')
  b_role=$(cli "$replica" "$replica_port" ROLE | sed -n '1p')
  if [[ $a_role == slave && $b_role == master ]]; then
    break
  fi
  sleep 0.1
done
[[ $a_role == slave && $b_role == master ]]
assert_idle_client_closed "$work/idle-round-1.log"
wait_for "new primary after first failover" proxy_role_is_master
printf 'PASS: first failover closed the old session and reconnects to the new master\n'

# Isolate S1 and explicitly give it a stale configuration pointing to A while
# preserving the real post-failover roles A=slave and B=master.
"$PODMAN" stop "$sentinel2" "$sentinel3" >/dev/null
cli "$sentinel1" "$sentinel1_port" SENTINEL REMOVE mymaster >/dev/null
cli "$sentinel1" "$sentinel1_port" SENTINEL MONITOR mymaster 127.0.0.1 "$primary_port" 2 >/dev/null
stale_addr=$(cli "$sentinel1" "$sentinel1_port" SENTINEL GET-MASTER-ADDR-BY-NAME mymaster | paste -sd: -)
[[ $stale_addr == "127.0.0.1:$primary_port" ]]
set +e
stale_output=$(cli "$primary" "$proxy_port" ROLE 2>&1)
stale_rc=$?
set -e
if [[ $stale_rc -eq 0 || "$stale_output" == *slave* ]]; then
  printf 'stale Sentinel unexpectedly produced a client connection: rc=%s output=%s\n' "$stale_rc" "$stale_output" >&2
  exit 1
fi
printf 'PASS: stale Sentinel answer cannot create a replica session\n'

"$PODMAN" start "$sentinel2" "$sentinel3" >/dev/null
wait_for "Sentinel quorum after recovery" quorum_ready
wait_for "primary resolution after Sentinel recovery" proxy_role_is_master

start_idle_client "$work/idle-round-2.log"
failover_reply=$(cli "$sentinel2" "$sentinel2_port" SENTINEL FAILOVER mymaster)
[[ $failover_reply == OK ]]
for _ in $(seq 1 150); do
  a_role=$(cli "$primary" "$primary_port" ROLE | sed -n '1p')
  b_role=$(cli "$replica" "$replica_port" ROLE | sed -n '1p')
  if [[ $a_role == master && $b_role == slave ]]; then
    break
  fi
  sleep 0.1
done
[[ $a_role == master && $b_role == slave ]]
assert_idle_client_closed "$work/idle-round-2.log"
wait_for "new primary after second failover" proxy_role_is_master
printf 'PASS: repeated failover also closes stale sessions and resolves the master\n'
printf 'all real failover checks passed\n'
