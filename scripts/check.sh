#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

if [ "$(uname -s)" != "Linux" ]; then
    echo "error: usb4-rdma workspace checks require Linux" >&2
    exit 1
fi

cargo fmt --all -- --check
cargo check --locked --workspace
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
cargo build --locked --workspace --all-features
sh scripts/test-plugins.sh
sh adapters/ofi/test-rma.sh
python3 adapters/net/test-host.py
cargo run --locked -p usb4-rdma-core --example rdma_write_read
cargo run --locked -p usb4-rdma-core --example ring_allreduce
