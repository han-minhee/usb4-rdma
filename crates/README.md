# USB4 RDMA for Rust

`usb4-rdma-core` supports tagged messaging, registered host memory, and
one-sided reads and writes over direct USB4. Build the SDK and
`usb4-rdma-ctl` on Linux with Rust 1.100 or later.

```sh
cargo add usb4-rdma-core@0.1.0
cargo install usb4-rdma-ctl --version 0.1.0 --locked
```

Provision USB4STREAM and run one daemon per host. [Two-host setup](https://github.com/han-minhee/usb4-rdma/blob/main/docs/QUICKSTART.md)

Point `USB4_RDMA_SOCKET` at the daemon's Unix socket before calling
`usb4_rdma_core::Context::open_default()`. Use compatible SDK, daemon, and wire
protocol versions. The [SDK examples](https://github.com/han-minhee/usb4-rdma/tree/main/crates/usb4-rdma-core/examples)
post operations and wait for completions on emulated links.

| Crate | Role |
| --- | --- |
| `usb4-rdma-core` | Rust SDK and fabric runtime |
| `usb4-rdma-ctl` | Daemon, endpoint management, and benchmarks |
| `usb4-proto` | Portable wire codec and stream framing |
| `usb4-stream-hal` | USB4STREAM, bonded links, and optional Linux `uring` |
| `usb4-sim` | In-memory and Unix-socket transports for tests |
| `usb4-gpu-staging` | Host staging for accelerator transfers |
| `usb4-ofi-provider` | OFI plugin (`native`, libfabric 2.7.0 or later) |
| `usb4-nccl-net` | NCCL/RCCL net v12 plugin (CUDA and ROCm features) |
| `usb4-llama-rpc` | ggml-rpc bridge and `include/usb4_rpc.h` |

[Build the native plugins and Linux bundle](https://github.com/han-minhee/usb4-rdma/blob/main/docs/RELEASE.md)
