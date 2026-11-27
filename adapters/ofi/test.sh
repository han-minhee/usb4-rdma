#!/bin/sh
# Exercise standard MPI across two emulated daemon nodes with two ranks each.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
target=${CARGO_TARGET_DIR:-"$repo/target"}
daemon="$target/release/usb4-rdma-ctl"
test -x "$daemon"
test -f "$target/release/libusb4-fi.so"
work=$(mktemp -d /tmp/u4rd-ofi.XXXXXX)
pids=
keep=${U4RD_KEEP_TEST_DIR:-0}
cleanup() {
    for pid in $pids; do kill -TERM "$pid" 2>/dev/null || true; done
    for pid in $pids; do wait "$pid" 2>/dev/null || true; done
    if [ "$keep" = 1 ]; then
        printf 'OFI test artifacts: %s\n' "$work" >&2
    else
        rm -rf "$work"
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mpicc -O2 -std=c11 -Wall -Wextra -Werror "$repo/benchmarks/mpi-qualify.c" -o "$work/qualify"
mkdir "$work/links"
cat > "$work/cluster.toml" <<'CONFIG'
epoch = 172156
timeout_ms = 10000
[[nodes]]
id = 0
name = "test0"
[[nodes]]
id = 1
name = "test1"
[[links]]
id = "test-link"
a = 0
b = 1
endpoint_a = "unused"
endpoint_b = "unused"
controller_a = "test"
controller_b = "test"
CONFIG
for node in 0 1; do
    "$daemon" serve --config "$work/cluster.toml" --node "$node" \
        --socket "$work/$node.sock" --backend emulated --emulated-dir "$work/links" >"$work/node-$node.log" 2>&1 &
    pids="$pids $!"
done
for node in 0 1; do
    attempts=0
    until "$daemon" monitor --socket "$work/$node.sock" 2>/dev/null | grep -q '"fabric_ready":true'; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 100 ]; then cat "$work"/node-*.log >&2; exit 1; fi
        sleep .1
    done
done
cat > "$work/rank" <<'RANK'
#!/bin/sh
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
node=$((OMPI_COMM_WORLD_RANK / 2))
export USB4_RDMA_SOCKET="$here/$node.sock"
exec "$here/qualify" "$@"
RANK
chmod +x "$work/rank"
export FI_PROVIDER_PATH="$target/release" FI_PROVIDER=usb4
export USB4_RDMA_SOCKET="$work/0.sock"
fi_info -p usb4 -t FI_EP_RDM
if [ "$#" -eq 0 ]; then set -- --idle-check; fi
timeout 120 mpirun --oversubscribe -np 4 --mca mpi_add_procs_cutoff 9 --mca pml cm --mca mtl ofi \
    --mca mtl_ofi_provider_include usb4 --mca mtl_ofi_tag_mode ofi_tag_1 \
    --mca mtl_ofi_disable_hmem 1 --mca mtl_ofi_av table \
    -x FI_PROVIDER -x FI_PROVIDER_PATH "$work/rank" "$@"
for node in 0 1; do
    "$daemon" monitor --socket "$work/$node.sock" --links
done
