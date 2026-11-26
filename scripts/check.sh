#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

if [ "$(uname -s)" != "Linux" ]; then
    echo "error: usb4-rdma checks require Linux" >&2
    exit 1
fi

cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo build --locked --workspace --all-features
cargo test --locked --workspace --all-features

sh scripts/test-c-api.sh
cargo run -p usb4-rdma --example rdma_write_read
cargo run -p usb4-rdma --example ring_allreduce
