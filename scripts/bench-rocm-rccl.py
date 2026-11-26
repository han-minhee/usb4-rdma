#!/usr/bin/env python3
"""Generic 2-node ROCm/RCCL GPU collective benchmark for usb4-rdma vs Ethernet.

Run under `torchrun` on two ROCm nodes (one GPU per node):

  # USB4 RDMA mode (librccl-net-usb4.so on LD_LIBRARY_PATH, daemon serving):
  NCCL_NET=Usb4Rdma NCCL_NET_PLUGIN=usb4 NCCL_NET_GDR_LEVEL=LOC \
  USB4_RDMA_SOCKET=/run/usb4-rdma/node.sock USB4_RDMA_SPEED_MBPS=80000 \
  torchrun --nnodes=2 --nproc_per_node=1 --node_rank=0 \
    --master_addr=<node0-ip> --master_port=29611 scripts/bench-rocm-rccl.py

  # Ethernet Socket baseline:
  NCCL_NET=Socket NCCL_NET_PLUGIN=none NCCL_SOCKET_IFNAME=<iface> \
  torchrun --nnodes=2 --nproc_per_node=1 --node_rank=0 \
    --master_addr=<node0-ip> --master_port=29612 scripts/bench-rocm-rccl.py
"""
from __future__ import annotations

import argparse
from functools import partial
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import time

import torch
import torch.distributed as dist


def read_u4rd_counters(daemon_bin: str, sock: str) -> dict | None:
    if not sock or not os.path.exists(sock):
        return None
    out = subprocess.check_output(
        [daemon_bin, "monitor", "--socket", sock, "--links"], text=True
    )
    return json.loads(out)


OPERATIONS = ("all_reduce", "all_gather", "reduce_scatter", "p2p")


def measure(operation, nbytes, warmup, iterations, rank, world, device):
    count = nbytes // 2
    value = float(rank + 1)
    summed = float(world * (world + 1) // 2)
    allocate = partial(torch.empty, device=device, dtype=torch.bfloat16)
    source = allocate(count * world if operation == "reduce_scatter" else count)
    target = source if operation == "all_reduce" else allocate(
        count * world if operation == "all_gather" else count)
    peer = (rank - 1) % world
    if operation == "all_reduce":
        expected = torch.full_like(target, summed)
        perform = partial(dist.all_reduce, source)
    elif operation == "all_gather":
        expected = torch.arange(1, world + 1, device=device, dtype=target.dtype)
        expected = expected.repeat_interleave(count)
        perform = partial(dist.all_gather_into_tensor, target, source)
    elif operation == "reduce_scatter":
        expected = torch.full_like(target, summed)
        perform = partial(dist.reduce_scatter_tensor, target, source)
    else:
        expected = torch.full_like(target, peer + 1)
        exchange = [dist.P2POp(dist.isend, source, (rank + 1) % world),
                    dist.P2POp(dist.irecv, target, peer)]

        def perform():
            for request in dist.batch_isend_irecv(exchange):
                request.wait()

    def sample():
        source.fill_(value)
        torch.cuda.synchronize()
        start = time.perf_counter_ns()
        perform()
        torch.cuda.synchronize()
        return (time.perf_counter_ns() - start) / 1_000

    for _ in range(warmup):
        sample()
    dist.barrier()
    samples = sorted(sample() for _ in range(iterations))
    assert torch.equal(target, expected), f"{operation} verification failed for {nbytes} bytes"
    mean = statistics.mean(samples)
    # Sizes are per-rank partition bytes for all-gather and reduce-scatter.
    algorithm_bytes = nbytes * world if operation in ("all_gather", "reduce_scatter") else nbytes
    traffic_factor = 2 * (world - 1) / world if operation == "all_reduce" else 1
    if operation in ("all_gather", "reduce_scatter"):
        traffic_factor = (world - 1) / world
    return {
        "operation": operation, "bytes": nbytes, "dtype": "bfloat16", "iters": iterations,
        "p50_us": round(statistics.median(samples), 2),
        "p95_us": round(samples[math.ceil(.95 * iterations) - 1], 2),
        "p99_us": round(samples[math.ceil(.99 * iterations) - 1], 2),
        "mean_us": round(mean, 2),
        "alg_gb_s": round(algorithm_bytes / mean / 1_000, 4),
        "bus_gb_s": round(algorithm_bytes * traffic_factor / mean / 1_000, 4),
        "verified": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--operations", nargs="+", choices=OPERATIONS, default=["all_reduce"])
    parser.add_argument("--sizes", nargs="+", type=int, metavar="BYTES")
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--warmup", type=int)
    parser.add_argument("--output-dir", type=Path, help="Write one JSON report per rank")
    args = parser.parse_args()
    if args.sizes and any(size < 2 or size % 2 for size in args.sizes):
        parser.error("--sizes requires positive, even BF16 byte counts")
    if args.iterations is not None and args.iterations < 1:
        parser.error("--iterations must be positive")
    if args.warmup is not None and args.warmup < 0:
        parser.error("--warmup must be nonnegative")

    if not torch.version.hip:
        raise SystemExit("ROCm PyTorch build required (torch.version.hip is empty)")

    # Open MPI sets placement on each host. torchrun provides these directly.
    for target, source in (("RANK", "OMPI_COMM_WORLD_RANK"),
                           ("LOCAL_RANK", "OMPI_COMM_WORLD_LOCAL_RANK"),
                           ("WORLD_SIZE", "OMPI_COMM_WORLD_SIZE")):
        if source in os.environ:
            os.environ.setdefault(target, os.environ[source])

    torch.cuda.set_device(int(os.environ.get("LOCAL_RANK", "0")))
    dev = torch.device("cuda")
    props = torch.cuda.get_device_properties(torch.cuda.current_device())

    mode = os.environ.get("BENCH_MODE", os.environ.get("NCCL_NET", "default"))
    sock = os.environ.get("USB4_RDMA_SOCKET", "")
    daemon_bin = os.environ.get("U4RD_DAEMON_BIN", "usb4-rdma-daemon")

    physical = os.environ.get("NCCL_NET") == "Usb4Rdma"
    if physical and not sock:
        raise SystemExit("USB4_RDMA_SOCKET is required for physical measurements")
    before = read_u4rd_counters(daemon_bin, sock)

    t_init0 = time.perf_counter()
    dist.init_process_group(backend="nccl")
    rank = dist.get_rank()
    world = dist.get_world_size()
    if world < 2:
        raise SystemExit(f"Expected world_size >= 2, got {world}")

    # Verify core LLM collective primitives (AllReduce, AllGather, ReduceScatter, P2P).
    base = torch.arange(4096, device=dev, dtype=torch.float32)
    val = base + rank
    dist.all_reduce(val)
    assert torch.equal(val, base * world + world * (world - 1) // 2), "all_reduce mismatch"

    parts = [torch.empty(1024, device=dev, dtype=torch.float32) for _ in range(world)]
    dist.all_gather(parts, torch.full((1024,), float(rank), device=dev, dtype=torch.float32))
    for peer, part in enumerate(parts):
        assert torch.equal(part, torch.full_like(part, float(peer))), "all_gather mismatch"

    src = torch.arange(world * 1024, device=dev, dtype=torch.float32) + rank
    out_rs = torch.empty(1024, device=dev, dtype=torch.float32)
    dist.reduce_scatter_tensor(out_rs, src)
    exp_rs = (
        torch.arange(rank * 1024, (rank + 1) * 1024, device=dev, dtype=torch.float32) * world
        + world * (world - 1) // 2
    )
    assert torch.equal(out_rs, exp_rs), "reduce_scatter mismatch"

    sent = torch.full((1024,), float(rank + 1), device=dev, dtype=torch.float32)
    recvd = torch.empty_like(sent)
    ops = [
        dist.P2POp(dist.isend, sent, (rank + 1) % world),
        dist.P2POp(dist.irecv, recvd, (rank - 1) % world),
    ]
    for req in dist.batch_isend_irecv(ops):
        req.wait()
    torch.cuda.synchronize()
    assert torch.equal(
        recvd, torch.full_like(recvd, float(((rank - 1) % world) + 1))
    ), "p2p mismatch"

    dist.barrier()
    init_ms = (time.perf_counter() - t_init0) * 1000.0

    # Bandwidth is measured from per-rank BF16 partitions.
    cases = [
        (8192, 20, 120), (16384, 20, 120), (65536, 20, 100),
        (262144, 15, 80), (1048576, 10, 60), (4194304, 10, 40),
        (16777216, 5, 25), (67108864, 3, 12),
    ]
    if args.sizes:
        cases = [(size, 20, 120) for size in args.sizes]
    results = []
    for operation in args.operations:
        for size, warmup, iterations in cases:
            result = measure(operation, size, warmup if args.warmup is None else args.warmup,
                             args.iterations or iterations, rank, world, dev)
            if size % (1024 * 1024) == 0:
                result["label"] = f"{size // (1024 * 1024)} MiB"
            elif size % 1024 == 0:
                result["label"] = f"{size // 1024} KiB"
            else:
                result["label"] = f"{size} B"
            results.append(result)

    dist.barrier()
    after = read_u4rd_counters(daemon_bin, sock)

    if physical:
        if not before["fabric_ready"] or not after["fabric_ready"] or before["epoch"] != after["epoch"]:
            raise RuntimeError("fabric lost readiness or changed epoch")
        for field in ("submitted_payload_bytes", "received_payload_bytes"):
            if after[field] <= before[field]:
                raise RuntimeError(f"USB4 {field} did not advance on rank {rank}")

    payload = {
        "mode": mode,
        "rank": rank,
        "world": world,
        "torch": torch.__version__,
        "hip": torch.version.hip,
        "gpu": props.name,
        "arch": props.gcnArchName,
        "nccl_net": os.environ.get("NCCL_NET"),
        "nccl_net_plugin": os.environ.get("NCCL_NET_PLUGIN"),
        "init_and_verify_ms": round(init_ms, 2),
        "collectives_verified": [
            "all_reduce",
            "all_gather",
            "reduce_scatter_tensor",
            "batch_isend_irecv",
        ],
        "all_reduce_bf16": [r for r in results if r["operation"] == "all_reduce"],
        "measurements": results,
        "u4rd_before": before,
        "u4rd_after": after,
    }
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / f"rank-{rank}.json").write_text(json.dumps(payload, indent=2) + "\n")
    else:
        print("BENCH_JSON:" + json.dumps(payload), flush=True)
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
