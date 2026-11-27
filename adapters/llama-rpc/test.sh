#!/usr/bin/env sh
set -eu
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 /path/to/prepared-llama.cpp /path/to/ggml-build" >&2
    exit 2
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
source=$1
build=$2
target=${CARGO_TARGET_DIR:-"$root/target"}
profile=${U4RD_PROFILE:-debug}
ctl="$target/$profile/usb4-rdma-ctl"
work=$(mktemp -d)
pids=
cleanup() {
    result=$?
    for pid in $pids; do kill -TERM "$pid" 2>/dev/null || true; done
    for pid in $pids; do wait "$pid" 2>/dev/null || true; done
    if [ "$result" -ne 0 ]; then
        cat "$work"/*.log 2>/dev/null || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT
${CXX:-c++} -std=c++17 -O2 -Wall -Wextra -Werror -pthread -I"$source/ggml/include" \
    "$root/adapters/llama-rpc/rpc-fixture.cpp" -L"$build/bin" -lggml-rpc -lggml-cpu -lggml -lggml-base -o "$work/rpc"
mkdir "$work/links"
for node in 0 1; do
    "$ctl" serve --config "$root/examples/two-node.toml" --node "$node" --socket "$work/node$node" \
        --backend emulated --emulated-dir "$work/links" > "$work/node$node.log" 2>&1 &
    pids="$pids $!"
done
for attempt in $(seq 1 100); do
    if "$ctl" monitor --socket "$work/node0" >/dev/null 2>&1 && "$ctl" monitor --socket "$work/node1" >/dev/null 2>&1; then
        break
    fi
    test "$attempt" -lt 100
    sleep 0.05
done
port=$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    print(s.getsockname()[1])
PY
)
endpoint="127.0.0.1:$port"
export GGML_RPC_USB4=1 LD_LIBRARY_PATH="$build/bin:$target/$profile${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
USB4_RDMA_SOCKET="$work/node1" "$work/rpc" --server "$endpoint" > "$work/server.log" 2>&1 &
pids="$pids $!"
for attempt in $(seq 1 100); do
    if ss -Hltn "sport = :$port" | grep -q .; then break; fi
    test "$attempt" -lt 100
    sleep 0.05
done
USB4_RDMA_SOCKET="$work/node0" timeout 90 "$work/rpc" "$endpoint"
"$ctl" monitor --socket "$work/node0" --links > "$work/mon0"
"$ctl" monitor --socket "$work/node1" --links > "$work/mon1"
python3 - "$work/mon0" "$work/mon1" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    client = json.load(f)
with open(sys.argv[2]) as f:
    server = json.load(f)
assert client['submitted_payload_bytes'] >= 4 * 1024 * 1024
assert client['received_payload_bytes'] >= 4 * 1024 * 1024
assert client['submitted_payload_bytes'] == server['received_payload_bytes']
assert client['received_payload_bytes'] == server['submitted_payload_bytes']
print('Verified peer-matched ggml-rpc bytes on emulated USB4STREAM')
PY
