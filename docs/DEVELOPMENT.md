# Development

Use Linux with Rust 1.100, a C11 compiler, Python 3, libfabric 2.7.0 development headers, and cbindgen 0.29.2 for the complete check. The default Rust workspace tests require neither vendor GPU SDKs nor attached USB4 hardware. The OFI `native` feature builds the actual libfabric provider.

```sh
cargo check --locked --workspace
cargo test --locked --workspace
sh scripts/check.sh
```

Configure `PKG_CONFIG_PATH` and `LD_LIBRARY_PATH` for a user-installed libfabric as shown in the [MPI guide](MPI.md). The check script exercises host transfers through the loaded OFI and net v12 plugins. For the four-rank test, build the release daemon and run `make -C adapters/ofi` followed by `sh adapters/ofi/test.sh`. See the [llama.cpp adapter](../adapters/llama-rpc/README.md) for the upstream ggml-rpc CPU fixture.

## Native profiles

- `uring` uses ConfigFS endpoints with `busy_poll=0` or the attribute absent, and `throttling=0`. SQPOLL is opt-in.
- `usb4-busy-poll` uses ConfigFS `busy_poll=1` and per-link workers. Defaults are a 500 µs post-progress spin budget and 50 µs recent-transfer polling interval.
- Both profiles require `CONFIG_USB4_STREAM`, `CONFIG_USB4_CONFIGFS`, and `CONFIG_CONFIGFS_FS`. Use `usb4-rdma-ctl doctor` for capability readback. Close endpoints before changing ConfigFS attributes.
- `emulated` runs the same reactor on Unix sockets. `emulated-uring` tests registered I/O and CQEs on Linux without physical USB4 devices.

Report simulator, multiprocess emulation, Linux `io_uring`, physical USB4STREAM, and accelerator runtime evidence individually.

## Link failure diagnostics

When fabric polling fails on a specific link, `usb4-rdma-ctl serve` reports the
local node and epoch, the zero-based local port, its configured link ID, and
the peer node. The local port order matches `monitor --links`. The cause
records an unready link at the startup deadline, a missed heartbeat deadline,
a closed stream, or an error while polling that link. The command still
reports the original error. The local stop flag describes the state when the
diagnostic is logged and cannot establish which peer began teardown.
