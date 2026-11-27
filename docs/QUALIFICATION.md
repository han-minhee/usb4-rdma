# Published 0.1 qualification and current acceptance gates

Build the same source on both Linux hosts. Test one and two MPI ranks per
host and ROCm tensor-parallel inference. Publish results with transport,
correctness, and measurement evidence.

## Test devices

Qualification used two FEVM FAEX1 mini PCs with an AMD Ryzen AI Max+ 395
processor and integrated Radeon 8060S graphics.

| Device | Operating system | Linux kernel |
| --- | --- | --- |
| FEVM FAEX1 · host A | Fedora Linux 44 Server Edition | `7.3.4-200.fc44.x86_64` |
| FEVM FAEX1 · host B | Ubuntu 26.04.1 LTS | `7.3.0-1004-oem` |

The pair used two 40 Gbit/s USB4 cables and 10 GbE as the TCP control. Release
archives use an x86-64 Linux build environment as described in the
[release guide](RELEASE.md).

## Validation

1. Run the Linux workspace, C11 ABI, and OFI emulation checks.
2. Use task-owned endpoints for native MPI point-to-point, collectives, and
   mixed same-host and cross-host traffic. Run the 30-minute mixed-message
   soak and verify that a controlled peer loss exits nonzero within 30 seconds.
3. Compare 10 GbE, single-link, and bonded runs with paired sessions, CPU
   affinity, runtime and daemon configuration, cable negotiation, and per-link
   byte counters.
4. Run all four RCCL operations, then both TP=2 inference fixtures with
   identical request shapes on the controls.

| Path | Release gate |
| --- | --- |
| Host RDMA | CRC32-verified transfers with matching daemon payload counters across single-cable and bonded configurations. |
| Native MPI | Bonded one-way 16 and 64 MiB goodput meets 10 GbE and reaches at least 1.5 times the faster single cable. |
| ROCm RCCL | All four operations pass. Bonded 16 MiB all-reduce p50 improves over 10 GbE with verified rank symmetry. |
| Inference | Complete and verify the bounded Qwen Flash Next dummy fixture and the real Gemma 4 31B BF16 workload, with matching peer counters. Publish every measured TTFT and throughput shape. |

Use application payload bytes for goodput and wall time for completion
latency. Report p50, p95, and p99, including the slowest rank where applicable.
A raw `USB4STREAM` rate includes the Linux character-device path; compare its
result separately from MPI, RCCL, and physical signaling.

## Physical transport evidence

- **Host RDMA:** The [4-stream roofline measurements](../benchmarks/results/roofline-20261125.json) reached `41.92 Gbit/s` unidirectional RDMA write, `42.20 Gbit/s` RDMA read, and `73.81 Gbit/s` full-duplex (`74.17 Gbit/s` paired against `4.69 Gbit/s` 10 GbE).
- **Native MPI:** The five [paired physical MPI runs](../benchmarks/results/native-mpi.json) delivered `11.56 Gbit/s` (16 MiB, `1.64×` single cable) and `12.04 Gbit/s` (64 MiB, `1.67×`). The [stability trials](../benchmarks/results/native-mpi-stability.json) completed `2,197,656` verified rounds in the 30-minute four-rank soak and exited nonzero in `1.91` seconds on controlled peer loss.
- **ROCm RCCL:** [Two-cable busy-poll confirmation](../benchmarks/results/link-profile-20261125.json) measured `10.78 ms` 16 MiB BF16 AllReduce p50 versus `16.11 ms` on 10 GbE; the [multi-operation suite](../benchmarks/results/rccl-v01.json) verified AllReduce, AllGather, ReduceScatter, and peer exchange.
- **Inference:** [Paired TP=2 trials](../benchmarks/results/llm-v01.json) completed the 4-stream Qwen Flash Next (`30/30` matching hashes; `13.9 GB` symmetric payload) and Gemma 4 31B BF16 (`16/18` matching hashes) suites.

## Publication checklist

Run the [manual Linux release checks and bundle build](RELEASE.md) at the exact
release revision. Audit reachable Git history, model and benchmark inputs, and
license attribution before publishing. Open MPI, libfabric, and model weights
have separate upstream distribution terms. Keep private host identifiers,
runtime archives, model weights, and raw operational logs outside the public
repository.
