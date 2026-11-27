# Architecture

One `usb4-rdma-ctl serve` process per host owns the USB4STREAM endpoints. Applications use the Rust API or load an independent adapter. The reactor is shared across the transport paths and assigns queue pairs, memory keys, credits, and rails.

| Crate | Responsibility |
| --- | --- |
| `usb4-proto` | Portable little-endian wire frames, borrowed header validation, CRC32, reassembly, and pooled packet storage |
| `usb4-stream-hal` | `StreamTransport`, ConfigFS and sysfs, device claims, kernel busy-poll workers, and fixed-buffer `io_uring` |
| `usb4-rdma-core` | Fabric routing, per-hop flow control, QP/CQ progress, memory protection, and same-user Unix IPC |
| `usb4-sim` | Deterministic in-memory stream faults and Unix-socket emulation |
| `usb4-gpu-staging` | Pinned CUDA, ROCm, and Level Zero copy buffers and a verified UMA mapping interface |
| `usb4-nccl-net` | Separate vendor builds of the NCCL and RCCL net v12 plugin |
| `usb4-ofi-provider` | Libfabric RDM tagged messaging and manual-progress host RMA |
| `usb4-llama-rpc` | Transactional byte stream and C ABI for ggml-rpc |
| `usb4-rdma-ctl` | Provisioning, diagnostics, daemon entry point, and benchmarks |

The HAL and simulator share the same framer. The core links to the HAL's transport interface. The plugins link to core without exposing vendor types to the reactor. Cargo builds each plugin as its own shared library.

The fabric reserves separate bounded control, direct, and transit queues. Control traffic can advance when a peer's bulk class is backpressured. Multi-rail frame reassembly is offset-based. An operation waits for remote commit before publishing success. A cable failure terminates the epoch and retires outstanding work.

For one-sided SDK memory, core maps a sealed `memfd` owned by the client. OFI can also register ordinary host memory. That path progresses through its OFI endpoint and core host-memory registry, which validates the key and copies before producing an acknowledgement. The core tagged endpoint owns mask matching, eager and rendezvous flow, and the host software RMA envelope over QPs. The OFI library maps libfabric calls and CQ entries to that endpoint.

See [wire protocol](PROTOCOL.md), [memory ownership](MEMORY.md), and [qualification](QUALIFICATION.md).
