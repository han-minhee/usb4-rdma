#!/usr/bin/env sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
target=${CARGO_TARGET_DIR:-"$root/target"}
profile=${U4RD_PROFILE:-debug}
ctl="$target/$profile/usb4-rdma-ctl"
work=$(mktemp -d)
pids=
cleanup() {
    for pid in $pids; do kill -TERM "$pid" 2>/dev/null || true; done
    for pid in $pids; do wait "$pid" 2>/dev/null || true; done
    rm -rf "$work"
}
trap cleanup EXIT
mkdir "$work/providers" "$work/links"
ln -s "$target/$profile/libusb4_fi.so" "$work/providers/libusb4-fi.so"
# pkg-config supplies individual compiler arguments.
${CC:-cc} -std=c11 -O2 -Wall -Wextra -Werror $(pkg-config --cflags libfabric) \
    "$root/adapters/ofi/rma-test.c" $(pkg-config --libs libfabric) -o "$work/rma"
for node in 0 1; do
    "$ctl" serve --config "$root/examples/two-node.toml" --node "$node" --socket "$work/node$node" \
        --backend emulated --emulated-dir "$work/links" > "$work/node$node.log" 2>&1 &
    pids="$pids $!"
done
for attempt in $(seq 1 100); do
    if "$ctl" monitor --socket "$work/node0" >/dev/null 2>&1 && "$ctl" monitor --socket "$work/node1" >/dev/null 2>&1; then
        break
    fi
    test "$attempt" -lt 100 || { cat "$work"/node*.log; exit 1; }
    sleep 0.05
done
export FI_PROVIDER=usb4 FI_PROVIDER_PATH="$work/providers"
"$work/rma" "$work/node0" "$work/node1"
"$work/rma" "$work/node0" "$work/node0"
