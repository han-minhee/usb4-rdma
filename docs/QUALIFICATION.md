# Qualification

## Test devices

Tested on two FEVM FAEX1 mini PCs (AMD Ryzen AI Max+ 395 / Radeon 8060S), two 40 Gbit/s USB4 cables, and 10 GbE TCP:

| Device | Operating system | Linux kernel |
| --- | --- | --- |
| Host A | Fedora Linux 44 Server | `7.3.4-200.fc44.x86_64` |
| Host B | Ubuntu 26.04.1 LTS | `7.3.0-1004-oem` |

## Acceptance gates and results

| Path | Release gate | Verified baseline |
| --- | --- | --- |
| **Host RDMA** | CRC32-verified transfers + matching daemon byte counters (1- and 2-cable) | `41.92 / 42.20 Gbit/s` write/read; `73.81 Gbit/s` duplex (`roofline-20261125.json`) |
| **Native MPI** | Bonded 16/64 MiB goodput >= 10 GbE and >= 1.5x single cable; 30-min soak + fault exit | `11.56 / 12.04 Gbit/s` (`1.64x / 1.67x`); `2.19M` soak rounds; `1.91 s` fault exit |
| **ROCm RCCL** | All 4 collectives pass; bonded 16 MiB AllReduce p50 faster than 10 GbE | `10.78 ms` p50 vs. `16.11 ms` 10 GbE (`link-profile-20261125.json`, `rccl-v01.json`) |
| **TP=2 Inference** | Verified outputs + matching link counters on Qwen Flash Next & Gemma 4 31B | `30/30` Qwen (`13.9 GB` symmetric); `16/18` Gemma (`llm-v01.json`) |

Before public releases, audit Git history and build clean release bundles ([RELEASE.md](RELEASE.md)). Keep private host identifiers and raw logs outside the repository.
