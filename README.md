# usb4-rdma

`usb4-rdma` turns direct Linux USB4/Thunderbolt cables (`USB4STREAM`) into a userspace RDMA fabric with one daemon per host (`usb4-rdma-ctl`), bonded multi-cable striping, and drop-in adapters for Rust, Open MPI, NCCL/RCCL, and `llama.cpp`.

[Connect two hosts](docs/QUICKSTART.md) · [Benchmarks](benchmarks/README.md) · [Benchmark commands](docs/BENCHMARK.md) · [Release](docs/RELEASE.md)

## What it provides

| Interface | Capabilities |
| --- | --- |
| **Daemon & CLI (`usb4-rdma-ctl`)** | Peer discovery (`doctor`), ConfigFS endpoint provisioning (`init`), bonded multi-link daemon (`serve`), and live link telemetry (`monitor --links`). |
| **Rust SDK (`usb4-rdma`)** | Protection domains, shared `memfd` & local memory registration, two-sided tagged `SEND`/`RECV`, and one-sided `RDMA_WRITE` / `RDMA_READ`. |
| **Libfabric & Open MPI** | `FI_EP_RDM` provider (`usb4`) for host messages, tags, and manual-progress RMA ([MPI guide](docs/MPI.md)). |
| **NCCL / RCCL plugin** | Net v12 shared library (`librccl-net-usb4.so` / `libnccl-net-usb4.so`) for multi-node PyTorch & SGLang/vLLM collectives ([ROCm guide](docs/ROCM.md)). |
| **ggml-rpc adapter** | Transactional C byte stream (`libllama_usb4_rpc.so`) for distributed `llama.cpp` ([adapter guide](adapters/llama-rpc/README.md)). |

[Rust crate guide](crates/README.md) · [Roadmap](docs/ROADMAP.md)

## Baseline performance (v0.1)

Measured between two Ryzen AI Max+ 395 / Radeon 8060S hosts connected by two 40 Gbit/s USB4 cables vs. 10 GbE TCP:

| Workload | 10 GbE TCP | Dual USB4 (`--backend usb4-busy-poll`) |
| --- | ---: | ---: |
| 16 MiB full-duplex RDMA write (4 streams) | 4.69 Gbit/s | **73.81 Gbit/s** (74.17 paired) |
| 16 MiB unidirectional RDMA write / read (4 streams) | 9.39 Gbit/s | **41.92 / 42.20 Gbit/s** |
| 64 B host RDMA write / read p50 | 33.8 µs | **13.6 / 13.1 µs** |
| 1 MiB host `f32` SUM AllReduce p50 (algbw) | 3,609 µs (0.29 GB/s) | **293.9 µs (3.55 GB/s)** |
| 64 MiB Open MPI one-way goodput | 9.40 Gbit/s | **12.04 Gbit/s** |
| 16 MiB RCCL BF16 AllReduce p50 | 16.11 ms | **10.78 ms** |
| Qwen Flash Next TP=2 (`1,024` tokens, `c=4`) TTFT / throughput | 5.90 s / 15.43 tok/s | **2.67 s / 23.94 tok/s** |

![16 MiB full-duplex goodput across 10 GbE and USB4](benchmarks/results/roofline-duplex.png)

[Latency & write plots](benchmarks/README.md#host-rdma-and-layered-roofline) ·
[MPI results](benchmarks/README.md#native-mpi) ·
[RCCL results](benchmarks/README.md#rocm-rccl) ·
[Inference results](benchmarks/README.md#two-host-rocm-inference)

## Docs

[Architecture](docs/ARCHITECTURE.md) · [Wire protocol](docs/PROTOCOL.md) · [Memory ownership](docs/MEMORY.md) · [Qualification](docs/QUALIFICATION.md) · [Development](docs/DEVELOPMENT.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [MIT](LICENSE)
