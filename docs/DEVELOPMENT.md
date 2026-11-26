# Development Contract

## Toolchain & Checks

- Rust edition `2024`, MSRV `1.99` (`Cargo.toml`).
- Build and test on Linux with a C11 compiler.
- Workspace lints deny `unsafe_op_in_unsafe_fn` and `clippy::undocumented_unsafe_blocks`.
- Run the full local check script:

```sh
sh scripts/check.sh
```

Or run individual checks:

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
cargo build --locked --release --workspace --all-features
U4RD_PROFILE=release sh scripts/test-c-api.sh
```

Install the pinned runtime from the [MPI guide](MPI.md). The OFI test runs
four Open MPI ranks across two emulated nodes.

~~~sh
cargo build --locked --release -p usb4-rdma-daemon
make -C adapters/ofi
sh adapters/ofi/test.sh
~~~

## Linux `USB4STREAM` Prerequisites

- Requires Linux kernel support for `CONFIG_USB4_STREAM`, `CONFIG_USB4_CONFIGFS`, and `CONFIG_CONFIGFS_FS`.
- Check sysfs and ConfigFS with `usb4-rdma-daemon doctor`.
- Readiness polling (`--backend usb4`) defaults to per-link worker threads, a 256-entry kernel ring, 8,192 ns interrupt moderation, a fabric queue depth of 64, 64-frame TX batching, a 200 µs post-progress spin budget, and a 100 µs active probe interval. The spin budget trades active CPU time for lower latency.
- Kernel ring polling (`--backend usb4-busy-poll`) is opt-in. Provision matched endpoints with `init --busy-poll --apply`, then use `serve --backend usb4-busy-poll` with their ConfigFS paths. This backend requires the kernel to report `busy_poll=1` and defaults to per-link worker threads, a 500 µs post-progress spin budget, and a 50 µs active probe interval.
- `io_uring` (`--backend uring`, compiled with `--features uring`) requires `throttling_ns = 0` on ConfigFS endpoints; `SQPOLL` defaults off.
- Never modify ConfigFS attributes while a `/dev/tbstream*` endpoint is open.

## Verification Separation

Report these checks separately.
1. **Unit, property & `loom` tests** (`cargo test --workspace`)
2. **Emulated multiprocess RDMA & C11 ABI tests** (`rdma_integration.rs`, `scripts/test-c-api.sh`)
3. **Linux `io_uring` & `memfd` seal tests** (Linux host with `--all-features` and a native backend run)
4. **Physical Linux `USB4STREAM` cable qualification** (two or more hosts connected by physical USB4 cables)
