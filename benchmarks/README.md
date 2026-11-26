# Benchmarks

Both hosts are FEVM FAEX1 mini PCs with AMD Ryzen AI Max+ 395 and Radeon 8060S,
connected by two 40 Gbit/s USB4 Gen3x2 cables (`76.38 Gbit/s` after `128b/132b`
and `256/260` framing) and a 10 GbE link. See [test devices](../docs/QUALIFICATION.md#test-devices)
and the [two-host runner](../docs/BENCHMARK.md) for reproduction commands.

## Host RDMA and layered roofline

[One-way writes](results/roofline-write.png) · [Full duplex](results/roofline-duplex.png) · [Latency](results/roofline-latency.png)

[Goodput by size](results/roofline-goodput-scaling.png) · [Latency by size](results/roofline-latency-scaling.png) · [Host AllReduce](results/roofline-allreduce.png)

Probing Linux `USB4STREAM` via [stream-roofline](stream-roofline.c) shows a
hardware controller limit of two concurrent open stream pairs per host router
(`4` streams total across two cables). At four streams with `--busy-poll`, raw
character-device streaming reaches **5.36 GB/s TX / 5.47 GB/s RX (`42.9–43.8 Gbit/s`)**
at 256 KiB blocks, **5.20 GB/s (`41.6 Gbit/s`)** at 32 KiB blocks, and **7.17 µs**
64 B half-RTT.

Using `--backend usb4-busy-poll` (`500 µs` spin / `50 µs` active probe,
per-link worker threads, 32 KiB striping above `CHUNK`, 64-frame TX / 32-frame
RX batches, 8 `PIPELINE_WINDOWS`):

| Metric | Dual USB4 (4 streams, `--backend usb4-busy-poll`) | 10 GbE TCP |
| --- | ---: | ---: |
| `rdma_write` 64 B p50 | **13.6 µs** | 33.8 µs |
| `rdma_read` 64 B p50 | **13.1 µs** | — |
| `rdma_send_recv` 64 B p50 | **17.3 µs** | — |
| `rdma_write` 64 KiB p50 | **53.3 µs** | 204.8 µs |
| `rdma_write_uni` 64 KiB | **1.516 GB/s** (12.13 Gbit/s) | 1.164 GB/s (9.31 Gbit/s) |
| `rdma_read_uni` 64 KiB | **1.551 GB/s** (12.41 Gbit/s) | — |
| `rdma_duplex` 1 MiB (`qd=128`) | **8.436 GB/s** (67.49 Gbit/s) | 0.587 GB/s (4.70 Gbit/s) |
| `rdma_write_uni` 16 MiB | **5.239 GB/s** (41.92 Gbit/s) | 1.174 GB/s (9.39 Gbit/s) |
| `rdma_read_uni` 16 MiB | **5.276 GB/s** (42.20 Gbit/s) | — |
| `rdma_duplex` 16 MiB (`qd=128`) | **9.227 GB/s** (73.81 Gbit/s; 74.17 paired) | 0.587 GB/s (4.69 Gbit/s) |
| `AllReduce` 64 KiB p50 / algbw | **44.7 µs** / **1.46 GB/s** | 249.2 µs / 0.25 GB/s |
| `AllReduce` 1 MiB p50 / algbw | **293.9 µs** / **3.55 GB/s** | 3,609.3 µs / 0.29 GB/s |
| `AllReduce` 4 MiB p50 / algbw | **1,303.6 µs** / **3.24 GB/s** | 14,276.0 µs / 0.29 GB/s |

All transfers verified end-to-end CRC32 integrity and matched daemon payload
counters (`--streams-per-cable 2`, 200 latency iterations, 256–512 MiB bandwidth
transfers). Ping-pong uses half-RTT; one-sided RDMA reads, writes, and
AllReduce use full completion latency. [Roofline receipts](results/roofline-20261125.json)

## Native MPI

[MPI goodput](results/native-mpi-goodput.png) · [MPI ping-pong](results/native-mpi-latency.png)

Five paired sessions used Open MPI 5.0.11, libfabric 2.4.0, one rank per host,
fixed CPU placement, and matched per-cable payload counters. [MPI commands](../docs/MPI.md)

| Blocking one-way goodput | 10 GbE | USB4 cable 1 | USB4 cable 2 | Bonded USB4 |
| --- | ---: | ---: | ---: | ---: |
| 16 MiB | 9.36 Gbit/s | 6.98 Gbit/s | 6.95 Gbit/s | **11.56 Gbit/s** |
| 64 MiB | 9.40 Gbit/s | 7.24 Gbit/s | 7.23 Gbit/s | **12.04 Gbit/s** |

The bond reached **1.64×** (16 MiB) and **1.67×** (64 MiB) the faster single
cable (median of paired ratios). At 8 KiB, bonded half-RTT was `403 µs` p50 and
`962 µs` p99 (`47 µs` / `50 µs` over 10 GbE). A four-rank 30-minute soak completed
`2,197,656` verified rounds with matching per-cable byte counts, and a controlled
peer-daemon stop exited nonzero in `1.91 s`.
[Per-session MPI receipts](results/native-mpi.json) · [Stability soak receipts](results/native-mpi-stability.json)

## ROCm RCCL

[8 KiB latency](results/rccl-link-8kib.png) · [16 MiB latency](results/rccl-link-16mib.png)

Two-rank BF16 AllReduce (Ring, Simple, one channel, 10 warm-ups, 30 iterations
per size; slower rank percentile reported):

| Condition | 8 KiB p50 | 8 KiB p99 | 16 MiB p50 |
| --- | ---: | ---: | ---: |
| 10 GbE | 0.179 ms | **0.564 ms** | 16.11 ms |
| 1 cable, 1 stream | 0.253 ms | 1.014 ms | 15.50 ms |
| 1 cable, 2 streams | 0.284 ms | 0.835 ms | 11.93 ms |
| 2 cables, readiness | 0.291 ms | 1.205 ms | 11.90 ms |
| 2 cables, busy polling | 0.193 ms | 1.628 ms | **10.78 ms** |

[RCCL link receipts](results/link-profile-20261125.json) · [Four-operation collective verification](results/rccl-v01.json)

## Two-host ROCm inference

[Qwen TTFT](results/llm-v01-qwen-ttft.png) · [Qwen throughput](results/llm-v01-qwen-throughput.png) · [Gemma TTFT](results/llm-v01-gemma-ttft.png) · [Gemma throughput](results/llm-v01-gemma-throughput.png)

SGLang 0.5.23 served `TP=2` across both hosts (`--disable-custom-all-reduce`,
64 output tokens per request, 3 trials per shape). Qwen Flash Next ran the
bounded BF16 dummy fixture over 4 streams (`--backend usb4-busy-poll`), matching
all `30/30` output SHA-256 hashes and exchanging `13,896,538,128` symmetric
payload bytes across all four links in both directions. Gemma 4 31B ran BF16
weights (`16/18` paired hashes matched).

| Model and prompt | Concurrency | 10 GbE TTFT | USB4 TTFT | 10 GbE | USB4 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Qwen Flash Next (dummy), 32 tokens | 1 | 0.26 s | **0.25 s** | **10.02 tok/s** | 9.94 tok/s |
| Qwen Flash Next (dummy), 32 tokens | 4 | 0.54 s | **0.46 s** | 30.68 tok/s | **34.29 tok/s** |
| Qwen Flash Next (dummy), 1,024 tokens | 1 | 1.97 s | **1.03 s** | 7.82 tok/s | **8.85 tok/s** |
| Qwen Flash Next (dummy), 1,024 tokens | 4 | 5.90 s | **2.67 s** | 15.43 tok/s | **23.94 tok/s** |
| Gemma 4 31B BF16, 24 tokens | 1 | **0.32 s** | 0.34 s | **5.64 tok/s** | 5.07 tok/s |
| Gemma 4 31B BF16, 1,088 tokens | 1 | 2.74 s | **2.23 s** | 4.10 tok/s | **4.36 tok/s** |
| Gemma 4 31B BF16, 24 tokens | 4 | **0.65 s** | 0.69 s | **21.71 tok/s** | 14.46 tok/s |

[Inference receipts and per-request counters](results/llm-v01.json)

## Reproduce

### Open MPI and RCCL smoke

Use a private hostfile with two machines (one slot each) and matching paths on
both hosts:

~~~sh
mpicc -O2 -Wall -Werror benchmarks/mpi-smoke.c -o /tmp/u4rd-mpi-smoke
mpirun -np 2 --hostfile hosts --map-by ppr:1:node /tmp/u4rd-mpi-smoke
~~~

For ROCm collectives over `usb4-rdma`:

~~~sh
export NCCL_NET=Usb4Rdma NCCL_NET_PLUGIN=usb4 NCCL_DEBUG=INFO NCCL_NET_GDR_LEVEL=LOC
export USB4_RDMA_SOCKET=/run/usb4-rdma/node.sock
mpirun -np 2 --hostfile hosts --map-by ppr:1:node \
  -x MASTER_ADDR -x MASTER_PORT -x NCCL_NET -x NCCL_NET_PLUGIN \
  -x NCCL_DEBUG -x NCCL_NET_GDR_LEVEL -x USB4_RDMA_SOCKET -x LD_LIBRARY_PATH \
  python3 scripts/bench-rocm-rccl.py
~~~

Use `NCCL_NET=Socket`, `NCCL_NET_PLUGIN=none`, and `NCCL_SOCKET_IFNAME=<iface>`
for Ethernet controls.

### TP=2 inference

Start one SGLang worker per host from the same model revision and runtime, then
run the matching benchmark client:

~~~sh
# Per host (NODE_RANK=0 or 1):
sglang serve --model-path "$MODEL_PATH" --served-model-name "$MODEL" \
  --tp-size 2 --nnodes 2 --node-rank "$NODE_RANK" --dist-init-addr "$RENDEZVOUS" \
  --disable-custom-all-reduce --host 127.0.0.1 --port 30000

# Gemma 4 31B client:
python3 benchmarks/gemma-inference.py --url http://127.0.0.1:30000 \
  --model "$MODEL" --revision "$REVISION" --transport Usb4Rdma --output gemma-usb4.json

# Bounded Qwen Flash Next dummy client:
python3 benchmarks/dummy-inference.py --url http://127.0.0.1:30000 \
  --model "$MODEL" --revision "$REVISION" --transport Usb4Rdma \
  --fixture "$FIXTURE" --output dummy-usb4.json
~~~

### Regenerate the figures

From the repository root, install Matplotlib 3.11.2 and regenerate the charts
using the bundled [Matplotlib style sheet](usb4.mplstyle) and
[Source Sans 3 v3.052](https://github.com/adobe-fonts/source-sans/releases/tag/3.052R)
([SIL Open Font License](fonts/LICENSE.md)):

~~~sh
python3 -m venv .venv
.venv/bin/pip install matplotlib==3.11.2
for plot in benchmarks/plot*.py; do
  .venv/bin/python "$plot"
done
~~~
