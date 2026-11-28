# Reproduce benchmarks

## 1. Host RDMA vs. 10 GbE TCP

`scripts/bench-usb4-vs-eth.py` runs `usb4-rdma-ctl bench` across latency (`rdma_write`, `rdma_read`, `rdma_send_recv`, `tcp_pingpong`), bandwidth (`rdma_write_uni`, `rdma_read_uni`, `rdma_duplex`, `tcp_uni`, `tcp_duplex`), and 2-node `f32` SUM AllReduce with CRC32 verification and daemon link-counter checks.

```sh
# Preflight
python3 scripts/bench-usb4-vs-eth.py preflight --host-a <node0> --host-b <node1>

# Dual-cable bonded 4-stream busy-poll comparison
python3 scripts/bench-usb4-vs-eth.py run \
  --host-a <node0> --host-b <node1> --eth-b <node1-eth-ip> \
  --services-a <svcA0>,<svcA1> --services-b <svcB0>,<svcB1> \
  --artifacts-dir ./linux-release-bin \
  --backend usb4-busy-poll --streams-per-cable 2 \
  --output results-dual.json
```

Use `--backend usb4` (readiness polling), `--backend uring` (`throttling=0`), or `--fresh-endpoints` as needed.

### Raw character-device ceiling (`stream-roofline.c`)

Run directly against dedicated `/dev/tbstream*` endpoints (use `--busy-poll` with `busy_poll=1` endpoints; replace `recv`/`send` with `pong`/`ping` for half-RTT):

```sh
cc -O3 -std=c11 -Wall -Wextra -Werror benchmarks/stream-roofline.c -o stream-roofline
./stream-roofline "$DEVICE" recv "$READY" "$GO" 2048 262144  # start GO on receiver first
./stream-roofline "$DEVICE" send "$READY" "$GO" 2048 262144
```

## 2. Open MPI and RCCL collectives

```sh
# Open MPI smoke (2 nodes, 1 rank/node)
mpicc -O2 -Wall -Werror benchmarks/mpi-smoke.c -o /tmp/u4rd-mpi-smoke
mpirun -np 2 --hostfile hosts --map-by ppr:1:node /tmp/u4rd-mpi-smoke

# PyTorch ROCm RCCL collectives
export LD_LIBRARY_PATH=/opt/usb4-rdma/lib:/opt/rocm/lib:/opt/rocm/lib64
export NCCL_NET=Usb4Rdma NCCL_NET_PLUGIN=usb4 NCCL_NET_GDR_LEVEL=LOC
export USB4_RDMA_SOCKET=/run/usb4-rdma/node.sock USB4_RDMA_SPEED_MBPS=80000
torchrun --nnodes=2 --nproc_per_node=1 --node_rank=<0|1> \
  --master_addr=<node0-ip> --master_port=29611 scripts/bench-rocm-rccl.py
```

For Ethernet TCP controls, set `NCCL_NET=Socket NCCL_NET_PLUGIN=none NCCL_SOCKET_IFNAME=<iface>`.

## 3. Two-host TP=2 inference (SGLang)

Start SGLang (`NODE_RANK=0` and `1`) with `--disable-custom-all-reduce` and the same `NCCL_NET=Usb4Rdma` environment:

```sh
sglang serve --model-path "$MODEL_PATH" --served-model-name "$MODEL" \
  --tp-size 2 --nnodes 2 --node-rank "$NODE_RANK" --dist-init-addr "$RENDEZVOUS" \
  --disable-custom-all-reduce --host 127.0.0.1 --port 30000

python3 benchmarks/gemma-inference.py --url http://127.0.0.1:30000 \
  --model "$MODEL" --revision "$REVISION" --transport Usb4Rdma --output gemma-usb4.json
```

## Operational hygiene

- Stop `usb4-rdma-ctl` cleanly (`SIGTERM`/`SIGINT`) rather than `SIGKILL` while `/dev/tbstream*` handles are open so the kernel USB4 driver deactivates hops cleanly.
- Run containers under the daemon's UID (`--user $(id -u):$(id -g)`) because IPC enforces `SO_PEERCRED` same-UID checks.
