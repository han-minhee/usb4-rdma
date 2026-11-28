# Benchmarks

Measured on two FEVM FAEX1 hosts (Ryzen AI Max+ 395 / Radeon 8060S) over two 40 Gbit/s USB4 Gen3x2 cables (`76.38 Gbit/s` raw line rate after framing) vs. 10 GbE TCP. See [docs/BENCHMARK.md](../docs/BENCHMARK.md) for reproduction commands.

## Host RDMA and layered roofline

[One-way writes](results/roofline-write.png) · [Full duplex](results/roofline-duplex.png) · [Latency](results/roofline-latency.png) · [Goodput scaling](results/roofline-goodput-scaling.png) · [Latency scaling](results/roofline-latency-scaling.png) · [Host AllReduce](results/roofline-allreduce.png)

- **Raw `USB4STREAM` ceiling** ([stream-roofline.c](stream-roofline.c), 4 open stream pairs): **5.36 GB/s TX / 5.47 GB/s RX** (`42.9–43.8 Gbit/s`) at 256 KiB, **5.20 GB/s** (`41.6 Gbit/s`) at 32 KiB, and **7.17 µs** 64 B half-RTT (`--busy-poll`).
- **Four-stream RDMA (`--backend usb4-busy-poll`, 32 KiB striping, CRC32-verified)** ([roofline-20261125.json](results/roofline-20261125.json)):

| Metric | Dual USB4 (4 streams) | 10 GbE TCP |
| --- | ---: | ---: |
| `rdma_write` / `rdma_read` / `send_recv` 64 B p50 | **13.6 / 13.1 / 17.3 µs** | 33.8 µs (ping-pong) |
| `rdma_write` 64 KiB p50 | **53.3 µs** | 204.8 µs |
| `rdma_write_uni` / `rdma_read_uni` 64 KiB | **1.516 / 1.551 GB/s** | 1.164 GB/s / — |
| `rdma_duplex` 1 MiB (`qd=128`) | **8.436 GB/s** (67.49 Gbit/s) | 0.587 GB/s (4.70 Gbit/s) |
| `rdma_write_uni` / `rdma_read_uni` 16 MiB | **5.239 / 5.276 GB/s** | 1.174 GB/s / — |
| `rdma_duplex` 16 MiB (`qd=128`) | **9.227 GB/s** (73.81 Gbit/s) | 0.587 GB/s (4.69 Gbit/s) |
| Host `f32` AllReduce 64 KiB p50 / algbw | **44.7 µs** / **1.46 GB/s** | 249.2 µs / 0.25 GB/s |
| Host `f32` AllReduce 1 MiB p50 / algbw | **293.9 µs** / **3.55 GB/s** | 3,609.3 µs / 0.29 GB/s |
| Host `f32` AllReduce 4 MiB p50 / algbw | **1,303.6 µs** / **3.24 GB/s** | 14,276.0 µs / 0.29 GB/s |

## Native MPI

[MPI goodput](results/native-mpi-goodput.png) · [MPI ping-pong](results/native-mpi-latency.png) · [Session receipts](results/native-mpi.json) · [Soak receipts](results/native-mpi-stability.json)

| Blocking one-way goodput | 10 GbE | Single USB4 | Bonded USB4 |
| --- | ---: | ---: | ---: |
| 16 MiB | 9.36 Gbit/s | 6.95–6.98 Gbit/s | **11.56 Gbit/s** (`1.64×`) |
| 64 MiB | 9.40 Gbit/s | 7.23–7.24 Gbit/s | **12.04 Gbit/s** (`1.67×`) |

Bonded 8 KiB half-RTT: `403 µs` p50 / `962 µs` p99 (`47 / 50 µs` on 10 GbE). A 4-rank 30-minute soak completed `2,197,656` verified rounds and exited nonzero in `1.91 s` on controlled peer stop.

## ROCm RCCL

[8 KiB latency](results/rccl-link-8kib.png) · [16 MiB latency](results/rccl-link-16mib.png) · [Link receipts](results/link-profile-20261125.json) · [Suite receipts](results/rccl-v01.json)

Two-rank BF16 AllReduce (Ring, Simple, 1 channel, slower rank reported):

| Condition | 8 KiB p50 | 8 KiB p99 | 16 MiB p50 |
| --- | ---: | ---: | ---: |
| 10 GbE | 0.179 ms | **0.564 ms** | 16.11 ms |
| 1 cable, 2 streams | 0.284 ms | 0.835 ms | 11.93 ms |
| 2 cables, readiness | 0.291 ms | 1.205 ms | 11.90 ms |
| 2 cables, busy polling | 0.193 ms | 1.628 ms | **10.78 ms** |

## Two-host ROCm inference

[Qwen TTFT](results/llm-v01-qwen-ttft.png) · [Qwen throughput](results/llm-v01-qwen-throughput.png) · [Gemma TTFT](results/llm-v01-gemma-ttft.png) · [Gemma throughput](results/llm-v01-gemma-throughput.png) · [Receipts](results/llm-v01.json)

SGLang `TP=2` (`--disable-custom-all-reduce`, 64 output tokens, 4 USB4 streams):

| Model & prompt tokens | Concurrency | 10 GbE TTFT | USB4 TTFT | 10 GbE | USB4 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Qwen Flash Next (dummy), 32 | 1 | 0.26 s | **0.25 s** | **10.02 tok/s** | 9.94 tok/s |
| Qwen Flash Next (dummy), 32 | 4 | 0.54 s | **0.46 s** | 30.68 tok/s | **34.29 tok/s** |
| Qwen Flash Next (dummy), 1,024 | 1 | 1.97 s | **1.03 s** | 7.82 tok/s | **8.85 tok/s** |
| Qwen Flash Next (dummy), 1,024 | 4 | 5.90 s | **2.67 s** | 15.43 tok/s | **23.94 tok/s** |
| Gemma 4 31B BF16, 24 | 1 | **0.32 s** | 0.34 s | **5.64 tok/s** | 5.07 tok/s |
| Gemma 4 31B BF16, 1,088 | 1 | 2.74 s | **2.23 s** | 4.10 tok/s | **4.36 tok/s** |
| Gemma 4 31B BF16, 24 | 4 | **0.65 s** | 0.69 s | **21.71 tok/s** | 14.46 tok/s |

To regenerate charts: `.venv/bin/pip install matplotlib==3.11.2 && for p in benchmarks/plot*.py; do .venv/bin/python "$p"; done`.
