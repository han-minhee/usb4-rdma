# Development

Prerequisites on Linux: Rust 1.100, C11 compiler, Python 3, libfabric 2.7.0 headers, and `cbindgen` 0.29.2. Workspace tests run without USB4 hardware or GPU SDKs:

```sh
cargo check --locked --workspace
cargo test --locked --workspace
sh scripts/check.sh
```

For the 4-rank OFI emulation check, run `make -C adapters/ofi && sh adapters/ofi/test.sh`. See [adapters/llama-rpc/README.md](../adapters/llama-rpc/README.md) for the `ggml-rpc` fixture.

## Daemon transport backends

| Profile | Mechanism | Notes |
| --- | --- | --- |
| `usb4` | Readiness polling (`busy_poll=0`) | Keeps endpoint moderation; `200 µs` spin, `100 µs` active probe |
| `uring` | Linux `io_uring` (`busy_poll=0`, `throttling=0`) | SQPOLL opt-in |
| `usb4-busy-poll` | Kernel busy polling (`busy_poll=1`) | Per-link workers; `500 µs` spin, `50 µs` active probe |
| `emulated` / `emulated-uring` | Unix socket reactor | Software testing without physical USB4 hardware |

Requires `CONFIG_USB4_STREAM`, `CONFIG_USB4_CONFIGFS`, and `CONFIG_CONFIGFS_FS` (`usb4-rdma-ctl doctor`).

## Fault reporting and coordinated stops

- On link error, heartbeat timeout, or closed stream, `usb4-rdma-ctl serve` logs local node/epoch/port/link ID and exits nonzero cleanly.
- Paired test harnesses can pass `--shutdown-fd <fd>` (`--shutdown-timeout-ms`, default `5,000 ms`) to coordinate clean `PREPARE`/`PREPARED`/`RELEASE` teardown across both daemons before signaling `SIGTERM`/`SIGINT`.
