#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
PROFILE="${U4RD_PROFILE:-debug}"
cd "$ROOT"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}/$PROFILE"
DAEMON="$TARGET_DIR/usb4-rdma-daemon"

if [ ! -x "$DAEMON" ]; then
    echo "error: build workspace first ($DAEMON not found)" >&2
    exit 1
fi

TMPDIR_TEST="$(mktemp -d "${TMPDIR:-/tmp}/u4rd-c-test.XXXXXX")"
P0=""
P1=""
cleanup() {
    [ -n "$P0" ] && kill "$P0" 2>/dev/null || true
    [ -n "$P1" ] && kill "$P1" 2>/dev/null || true
    wait 2>/dev/null || true
    rm -rf "$TMPDIR_TEST"
}
trap cleanup EXIT INT TERM

cc -std=c11 -Wall -Wextra -Werror \
    -I"$ROOT/include" \
    "$ROOT/tests/c_api_test.c" \
    -L"$TARGET_DIR" -lusb4_rdma "-Wl,-rpath,$TARGET_DIR" -lpthread \
    -o "$TMPDIR_TEST/c_api_test"

cat > "$TMPDIR_TEST/cluster.toml" <<'EOF'
epoch = 77
queue_depth = 16
timeout_ms = 5000

[[nodes]]
id = 0
name = "node0"

[[nodes]]
id = 1
name = "node1"

[[links]]
id = "l0"
a = 0
b = 1
endpoint_a = "e0"
endpoint_b = "e1"
controller_a = "c0"
controller_b = "c0"
speed_mbps = 40000
EOF

mkdir -p "$TMPDIR_TEST/emu"
"$DAEMON" serve --config "$TMPDIR_TEST/cluster.toml" --node 0 \
    --socket "$TMPDIR_TEST/n0.sock" --backend emulated --emulated-dir "$TMPDIR_TEST/emu" >/dev/null 2>&1 &
P0=$!
"$DAEMON" serve --config "$TMPDIR_TEST/cluster.toml" --node 1 \
    --socket "$TMPDIR_TEST/n1.sock" --backend emulated --emulated-dir "$TMPDIR_TEST/emu" >/dev/null 2>&1 &
P1=$!

i=0
while [ "$i" -lt 100 ]; do
    if "$DAEMON" monitor --socket "$TMPDIR_TEST/n0.sock" >/dev/null 2>&1 && \
       "$DAEMON" monitor --socket "$TMPDIR_TEST/n1.sock" >/dev/null 2>&1; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done

"$TMPDIR_TEST/c_api_test" "$TMPDIR_TEST/n0.sock" "$TMPDIR_TEST/n1.sock"
