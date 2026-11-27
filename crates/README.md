# USB4 RDMA for Rust

`usb4-rdma-core` is the Rust SDK for tagged messaging, registered host
memory, and one-sided reads and writes over direct USB4 cables. A client
connects to a local `usb4-rdma-ctl` daemon over a Unix socket. The SDK and
daemon require Linux and Rust 1.100 or later to build.

```sh
cargo add usb4-rdma-core@0.1.0
cargo install usb4-rdma-ctl --version 0.1.0 --locked
```

Follow the [two-host setup guide](https://github.com/han-minhee/usb4-rdma/blob/main/docs/QUICKSTART.md)
to provision Linux USB4STREAM endpoints and start one daemon per host.
Set `USB4_RDMA_SOCKET` to the local daemon socket before calling
`usb4_rdma_core::Context::open_default()`. The
[SDK examples](https://github.com/han-minhee/usb4-rdma/tree/main/crates/usb4-rdma-core/examples)
show how to post operations and wait for completions on emulated links.
Use compatible SDK, daemon, and wire protocol versions across the fabric.

| Crate | Role |
| --- | --- |
| `usb4-rdma-core` | Rust SDK and fabric runtime |
| `usb4-rdma-ctl` | Daemon, endpoint management, and benchmarks |
| `usb4-proto` | Portable wire codec and stream framing |
| `usb4-stream-hal` | USB4STREAM and bonded transport backends, with optional Linux `uring` support |
| `usb4-sim` | In-memory and Unix-socket transports for tests |
| `usb4-gpu-staging` | Host staging for accelerator transfers |
| `usb4-ofi-provider` | Libfabric plugin, built with `native` and libfabric 2.7.0 or later |
| `usb4-nccl-net` | NCCL/RCCL net v12 plugin with CUDA and ROCm build features |
| `usb4-llama-rpc` | ggml-rpc bridge with the C header in `include/usb4_rpc.h` |

See the [Linux release guide](https://github.com/han-minhee/usb4-rdma/blob/main/docs/RELEASE.md)
for native plugin builds and the prebuilt bundle layout.
