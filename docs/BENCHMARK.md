# USB4 RDMA vs Ethernet TCP Benchmarking

`usb4-rdma-ctl bench` compares `USB4STREAM` RDMA against TCP (`TCP_NODELAY`)
on the same pair. It matches payloads, warm-ups, and iterations and verifies
integrity with CRC32.

## What Is Measured

1. **Latency (`--suite latency`)**:
   - `ethernet / tcp_pingpong`: Half-RTT across `64 B .. 1 MiB` (`TCP_NODELAY`).
   - `usb4-rdma / rdma_write`: One-sided `RDMA_WRITE` completion RTT (zero responder app involvement).
   - `usb4-rdma / rdma_read`: One-sided `RDMA_READ` completion RTT (zero responder app involvement).
   - `usb4-rdma / rdma_send_recv`: Two-sided tagged `SEND`/`RECV` ping-pong half-RTT.
2. **Bandwidth / Goodput (`--suite bandwidth`)**:
   - `ethernet / tcp_uni` & `tcp_duplex`
   - `usb4-rdma / rdma_write_uni`, `rdma_read_uni`, and simultaneous full-duplex `rdma_duplex`
   - Verifies exact `usb4-rdma-ctl` payload byte counter deltas (`tx_payload_bytes`, `rx_payload_bytes`) so RDMA payload is proven to traverse the USB4 fabric rather than TCP.
3. **2-Node Host `f32` SUM AllReduce (`--suite allreduce`)**:
   - Compares `RDMA_WRITE_WITH_IMM` exchange + vector reduction against TCP exchange + vector reduction with exact element-wise verification.

With current clients and responders, RDMA duplex bandwidth times the two
write loops through the control acknowledgement that both peers finished.
The initiator requests the remote CRC after stopping the clock. A result
still requires both CRC checks and exact payload counters. The JSON field
`duplex_timing` records `transfer_completion`. With an older responder,
`transfer_and_remote_crc` identifies results that include the peer's CRC
verification time.

## Two-Host Linux Execution

1. **Preflight (read-only):**

```sh
python3 scripts/bench-usb4-vs-eth.py preflight --host-a <node0> --host-b <node1>
```

2. **Run single-cable or dual-cable bonded comparison:**

```sh
# Single cable
python3 scripts/bench-usb4-vs-eth.py run \
  --host-a <node0> --host-b <node1> \
  --eth-b <node1-ethernet-ip> \
  --services-a <svcA0> --services-b <svcB0> \
  --artifacts-dir ./linux-release-bin \
  --backend usb4 \
  --output results-single.json

# Dual-cable bonded
python3 scripts/bench-usb4-vs-eth.py run \
  --host-a <node0> --host-b <node1> \
  --eth-b <node1-ethernet-ip> \
  --services-a <svcA0>,<svcA1> --services-b <svcB0>,<svcB1> \
  --artifacts-dir ./linux-release-bin \
  --backend usb4 \
  --output results-dual.json
```

Add `--fresh-endpoints` to use new ConfigFS endpoint names for each run.
The runner removes those endpoints after verifying that both daemons stopped.
Busy polling and `--streams-per-cable 2` do this automatically.

### Run two streams per cable

Set `--streams-per-cable 2` for two DMA rings per cable. List each service
once per host. The runner shares a controller label, divides `--speed-mbps`
across the rings, and cleans up its ConfigFS endpoints after both daemons exit.

Compare one and two streams per cable at the same queue depth, spin budget
and interrupt moderation. Check `monitor --links` on both peers to verify
that the receiver accounts for each stream's transmitted bytes.

To test concurrent bulk writes, post one registered write per queue pair
to disjoint remote memory ranges before polling their completions. Keep
the aggregate byte count fixed across comparisons, verify every
completion, and validate the remote contents after the writes finish.

### Character-device streaming control

Build [the standalone stream benchmark](../benchmarks/stream-roofline.c)
on each Linux host. Use dedicated matched USB4STREAM endpoints that no
daemon has open.

```sh
cc -O3 -std=c11 -Wall -Wextra -Werror benchmarks/stream-roofline.c -o stream-roofline

# Use fresh READY and GO paths in a private directory on each host.
./stream-roofline "$DEVICE" recv "$READY" "$GO" 2048 262144 # receiver
./stream-roofline "$DEVICE" send "$READY" "$GO" 2048 262144 # sender
```

After both READY files appear, create GO first on the receiver and then on
the sender. Compare sender-verified goodput with the RDMA write runs.
For a two-cable raw test, start one process per cable on each host, wait for
all four READY files, and start both cables together. Use the sum of sent
bytes divided by the longest sender time.

For a raw latency control use `ping` and `pong` in place of `send` and
`recv`. Pass the measured iteration count in place of MiB. For example,
run both peers with `1024 8192` for 1,024 measured 8 KiB exchanges after
40 warmups. The ping process reports p50 and p99 half-RTT.

For kernel ring polling, use dedicated endpoints with `busy_poll=1` and
pass `--busy-poll` to both raw benchmark processes. To benchmark RDMA in
this mode, pass `--backend usb4-busy-poll` to the two-host runner. Pin the
daemon and benchmark to separate physical cores and record CPU cost with
latency and goodput. The runner provisions fresh busy-poll endpoints and
checks the same payload integrity as the readiness path.

## ROCm / RCCL GPU Collectives and LLM Serving

`usb4-nccl-net` builds the RCCL v12 network plugin as `Usb4Rdma`.

1. **Install plugin symlink on `LD_LIBRARY_PATH`:**
   RCCL searches for `librccl-net-<name>.so` when `NCCL_NET_PLUGIN=<name>` is set:

```sh
cp target/release/libusb4_net.so /opt/usb4-rdma/lib/librccl-net-usb4.so
```

2. **Run PyTorch ROCm GPU collectives (`scripts/bench-rocm-rccl.py`):**

```sh
# Dual-cable USB4 RDMA
LD_LIBRARY_PATH=/opt/usb4-rdma/lib:/opt/rocm/lib:/opt/rocm/lib64 \
NCCL_NET=Usb4Rdma NCCL_NET_PLUGIN=usb4 NCCL_NET_GDR_LEVEL=LOC \
USB4_RDMA_SOCKET=/run/usb4-rdma/node.sock USB4_RDMA_SPEED_MBPS=80000 \
torchrun --nnodes=2 --nproc_per_node=1 --node_rank=<0|1> \
  --master_addr=<node0-ip> --master_port=29611 scripts/bench-rocm-rccl.py

# Ethernet Socket baseline
NCCL_NET=Socket NCCL_NET_PLUGIN=none NCCL_SOCKET_IFNAME=<iface> \
torchrun --nnodes=2 --nproc_per_node=1 --node_rank=<0|1> \
  --master_addr=<node0-ip> --master_port=29612 scripts/bench-rocm-rccl.py
```

3. **2-Node Tensor-Parallel (`TP=2`) LLM Serving (SGLang / vLLM):**
   Pass the same `NCCL_NET=Usb4Rdma NCCL_NET_PLUGIN=usb4 NCCL_NET_GDR_LEVEL=LOC` environment along with `--disable-custom-all-reduce` so inter-node TP AllReduce traffic uses RCCL over `usb4-rdma`. Confirm `NCCL INFO Using network Usb4Rdma` in startup logs and non-zero payload deltas in `usb4-rdma-ctl monitor --links`.

## Qualifying a transport profile before inference

1. Run the [Open MPI smoke test](../benchmarks/README.md#open-mpi-and-rccl)
   to verify rank placement and the remote launcher.
2. Measure RCCL over the Ethernet interface, each USB4 cable independently,
   and the bond. Verify AllReduce, AllGather, ReduceScatter and point-to-point
   exchange. Use the same rank placement, device, CPU affinity and collective
   settings. Record the slower rank's p50 and p99 across repeated runs.
3. Verify the selected RCCL network and match each USB4 cable's transmitted
   bytes with the peer's received bytes. Compare small AllReduce latency and
   large AllReduce bandwidth before starting a model.

For probe A/B tests, use `--active-probe-us 1000` and `--active-probe-us 100`
with the same ring size, moderation, `--queue-depth` and `--spin-us`. The
probe interval applies for one second after non-health traffic. Idle heartbeat
cadence and failure deadlines follow the cluster timeout.

Use `--throttling-ns 0` to compare zero moderation under readiness polling.
Then use `--backend uring` with zero moderation and SQPOLL off. Run each
profile on dedicated endpoint names and stop both daemons before changing
endpoint attributes. Retain both peers' per-cable submitted and received
bytes. Use CPU samples and ROCm traces in separate sessions from the reported
latency and throughput runs.

## Notes on Fair Comparison and Hardware Hygiene

- Both transports carry host-memory payloads with CRC32 end-to-end verification; upstream Linux `USB4STREAM` still copies between userspace and kernel DMA pages.
- Avoid running Ethernet benchmarks while unrelated heavy LAN/WAN transfers saturate the same NIC or switch link.
- Keep emulated software smoke tests distinct from physical USB4/Ethernet hardware measurements.
- Always stop `usb4-rdma-ctl` gracefully (`SIGINT`/`SIGTERM`, e.g., `docker stop -t 5`) rather than abrupt `SIGKILL` (`docker rm -f`) while `/dev/tbstream*` devices are open. Abrupt termination can trigger upstream Linux `thunderbolt` `hop deactivation failed` errors requiring physical cable reconnection or an orderly host reboot.
- Match the container UID (`--user $(id -u):$(id -g)`) to the daemon UID because core IPC enforces `SO_PEERCRED` same-UID client authentication on Linux.
