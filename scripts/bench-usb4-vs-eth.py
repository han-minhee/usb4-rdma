#!/usr/bin/env python3
"""Generic two-host USB4 RDMA vs Ethernet TCP benchmark orchestrator.

Zero hardcoded hosts or private addresses. Supports single-cable and dual-cable
bonded Linux USB4STREAM deployments alongside Ethernet TCP comparison.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def ssh(host: str, cmd: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, cmd],
        text=True,
        capture_output=True,
        check=check,
        timeout=timeout,
    )


def scp_to(local: Path, host: str, remote: str) -> None:
    subprocess.run(
        ["scp", "-o", "BatchMode=yes", "-q", str(local), f"{host}:{remote}"],
        check=True,
        timeout=120,
    )


def inspect_host(host: str) -> dict:
    script = r"""
python3 - <<'PY'
import glob, json, os, shutil, subprocess

def read(p):
    try:
        with open(p) as f:
            return f.read().strip()
    except OSError:
        return None

du = shutil.disk_usage("/")
links = []
services = []
tb_root = "/sys/bus/thunderbolt/devices"
if os.path.isdir(tb_root):
    for entry in sorted(os.listdir(tb_root)):
        p = os.path.join(tb_root, entry)
        if os.path.isfile(os.path.join(p, "rx_speed")):
            links.append({
                "name": entry,
                "rx_speed": read(os.path.join(p, "rx_speed")),
                "rx_lanes": read(os.path.join(p, "rx_lanes")),
                "tx_speed": read(os.path.join(p, "tx_speed")),
                "tx_lanes": read(os.path.join(p, "tx_lanes")),
                "unique_id": read(os.path.join(p, "unique_id")),
            })
        if read(os.path.join(p, "key")) == "stream":
            parent = os.path.dirname(os.path.realpath(p))
            services.append({
                "name": entry,
                "prtcid": read(os.path.join(p, "prtcid")),
                "prtcvers": read(os.path.join(p, "prtcvers")),
                "peer_uuid": read(os.path.join(parent, "unique_id")),
            })

domains = {}
for dom in sorted(glob.glob(os.path.join(tb_root, "*-0"))):
    uid = read(os.path.join(dom, "unique_id"))
    if uid:
        domains[os.path.basename(dom)] = uid

nets = []
for net in sorted(glob.glob("/sys/class/net/*")):
    n = os.path.basename(net)
    if n == "lo" or n.startswith(("veth", "br-", "docker", "vnet")):
        continue
    nets.append({
        "name": n,
        "operstate": read(os.path.join(net, "operstate")),
        "speed_mbps": read(os.path.join(net, "speed")),
    })

print(json.dumps({
    "hostname": os.uname().nodename,
    "kernel": os.uname().release,
    "root_free_gib": round(du.free / (1024 ** 3), 2),
    "configfs_stream": os.path.isdir("/sys/kernel/config/thunderbolt/stream"),
    "tbstream_devs": sorted(glob.glob("/dev/tbstream*")),
    "domains": domains,
    "links": links,
    "services": services,
    "nets": nets,
}))
PY
"""
    res = ssh(host, script)
    return json.loads(res.stdout)


def cmd_preflight(args: argparse.Namespace) -> int:
    info_a = inspect_host(args.host_a)
    info_b = inspect_host(args.host_b)
    report = {"host_a": info_a, "host_b": info_b, "warnings": [], "ready": True}

    for label, info in [("host_a", info_a), ("host_b", info_b)]:
        if info["root_free_gib"] < 20.0:
            report["warnings"].append(f"{label} ({info['hostname']}) has < 20 GiB free on /")
            report["ready"] = False
        if not info["configfs_stream"]:
            report["warnings"].append(f"{label} ({info['hostname']}) missing /sys/kernel/config/thunderbolt/stream")
            report["ready"] = False
        if not info["services"]:
            report["warnings"].append(f"{label} ({info['hostname']}) has no discovered USB4 stream services")
            report["ready"] = False

    print(json.dumps(report, indent=2))
    return 0 if report["ready"] else 1


def provision_pair(
    host_a: str,
    host_b: str,
    daemon_remote: str,
    svc_a: str,
    svc_b: str,
    stream_name: str,
    ring_size: int,
    throttling_ns: int,
    owned_endpoints: list[tuple[str, str]] | None = None,
    busy_poll: bool = False,
) -> tuple[str, str]:
    """Provision matched ConfigFS USB4STREAM endpoints with crossed HopIDs."""
    ep_a = f"/sys/kernel/config/thunderbolt/stream/{svc_a}/{stream_name}"
    ep_b = f"/sys/kernel/config/thunderbolt/stream/{svc_b}/{stream_name}"
    if owned_endpoints is not None:
        for host, endpoint in ((host_a, ep_a), (host_b, ep_b)):
            result = ssh(host, f"sudo -n mkdir {shlex.quote(endpoint)}", check=False)
            if result.returncode != 0:
                raise RuntimeError(f"could not create {host}:{endpoint}: {result.stderr.strip()}")
            owned_endpoints.append((host, endpoint))

    busy_option = " --busy-poll" if busy_poll else ""
    # Step 1: Provision host A with auto HopIDs (-1, -1), read back assigned in/out HopIDs.
    cmd_a = (
        f"sudo {shlex.quote(daemon_remote)} init "
        f"--service {shlex.quote(svc_a)} --name {shlex.quote(stream_name)} "
        f"--in-hopid=-1 --out-hopid=-1 --ring-size {ring_size} "
        f"--throttling-ns {throttling_ns}{busy_option} --apply"
    )
    res_a = ssh(host_a, cmd_a, check=False)
    if res_a.returncode != 0:
        raise RuntimeError(f"host_a init failed ({res_a.returncode}): {res_a.stdout}\n{res_a.stderr}")

    hop_out = ssh(
        host_a,
        f"printf '%s %s %s' $(cat {shlex.quote(ep_a)}/in_hopid) $(cat {shlex.quote(ep_a)}/out_hopid) $(cat {shlex.quote(ep_a)}/index)",
    ).stdout.strip().split()
    a_in, a_out, a_idx = int(hop_out[0]), int(hop_out[1]), int(hop_out[2])

    # Step 2: Provision host B with crossed HopIDs (B.in = A.out, B.out = A.in).
    cmd_b = (
        f"sudo {shlex.quote(daemon_remote)} init "
        f"--service {shlex.quote(svc_b)} --name {shlex.quote(stream_name)} "
        f"--in-hopid={a_out} --out-hopid={a_in} --ring-size {ring_size} "
        f"--throttling-ns {throttling_ns}{busy_option} --apply"
    )
    res_b = ssh(host_b, cmd_b, check=False)
    if res_b.returncode != 0:
        raise RuntimeError(f"host_b init failed ({res_b.returncode}): {res_b.stdout}\n{res_b.stderr}")

    b_idx = int(ssh(host_b, f"cat {shlex.quote(ep_b)}/index").stdout.strip())

    # Grant current SSH user read/write access to just the provisioned tbstream device nodes.
    ssh(host_a, f"sudo chown $(id -u):$(id -g) /dev/tbstream{a_idx} && sudo chmod 0600 /dev/tbstream{a_idx}")
    ssh(host_b, f"sudo chown $(id -u):$(id -g) /dev/tbstream{b_idx} && sudo chmod 0600 /dev/tbstream{b_idx}")

    return ep_a, ep_b


def build_cluster_toml(
    epoch: int,
    endpoints: list[tuple[str, str, str, str]],
    speed_mbps: int,
    queue_depth: int | None,
) -> str:
    lines = [
        f"epoch = {epoch}",
        "timeout_ms = 10000",
        "",
        "[[nodes]]",
        "id = 0",
        'name = "node0"',
        "",
        "[[nodes]]",
        "id = 1",
        'name = "node1"',
        "",
    ]
    if queue_depth is not None:
        lines.insert(1, f"queue_depth = {queue_depth}")
    for idx, (ep_a, ep_b, ctrl_a, ctrl_b) in enumerate(endpoints):
        lines.extend(
            [
                "[[links]]",
                f'id = "cable{idx}"',
                "a = 0",
                "b = 1",
                f'endpoint_a = "{ep_a}"',
                f'endpoint_b = "{ep_b}"',
                f'controller_a = "{ctrl_a}"',
                f'controller_b = "{ctrl_b}"',
                f"speed_mbps = {speed_mbps}",
                "",
            ]
        )
    return "\n".join(lines)


def stop_run(host: str, remote_dir: str) -> bool:
    # A late cleanup can encounter a recycled PID. Match the run's executable.
    script = r"""
cd "$1" || exit 1
running() {
    [ -e "/proc/$pid/exe" ] && [ "$(readlink "/proc/$pid/exe")" = "$PWD/usb4-rdma-ctl" ]
}
for file in bench-server.pid daemon.pid; do
    [ -f "$file" ] || continue
    pid=$(cat "$file")
    case "$pid" in ""|*[!0-9]*) exit 1 ;; esac
    if running; then
        kill -TERM "$pid" || exit 1
        for attempt in $(seq 1 100); do running || break; sleep 0.1; done
        if running; then exit 1; fi
    fi
done
"""
    command = shlex.join(["sh", "-c", script, "cleanup", remote_dir])
    return ssh(host, command, check=False).returncode == 0


def cmd_run(args: argparse.Namespace) -> int:
    svcs_a = [s.strip() for s in args.services_a.split(",") if s.strip()]
    svcs_b = [s.strip() for s in args.services_b.split(",") if s.strip()]
    if len(svcs_a) != len(svcs_b) or not svcs_a:
        sys.exit("error: --services-a and --services-b must have equal non-empty counts")
    if args.speed_mbps < args.streams_per_cable:
        sys.exit("error: --speed-mbps must cover every stream on a cable")
    busy_poll = args.backend == "usb4-busy-poll"
    create_fresh_endpoints = args.fresh_endpoints or args.streams_per_cable > 1 or busy_poll
    if args.streams_per_cable > 1:
        if len(set(svcs_a)) != len(svcs_a) or len(set(svcs_b)) != len(svcs_b):
            sys.exit("error: list each cable's service once with --streams-per-cable")
    if create_fresh_endpoints:
        if (not args.stream_prefix.isascii()
                or any(Path(value).name != value or value in ("", ".", "..")
                       for value in [*svcs_a, *svcs_b, args.stream_prefix])):
            sys.exit("error: services and stream prefix must be single path components")
        if len(svcs_a) * args.streams_per_cable > 256:
            sys.exit("error: at most 256 fresh endpoints are supported per host")

    binary = Path(args.artifacts_dir) / "usb4-rdma-ctl"
    if not binary.is_file():
        sys.exit(f"error: missing binary {binary}")

    # Preflight disk headroom & ConfigFS before touching either host.
    for label, host in [("host_a", args.host_a), ("host_b", args.host_b)]:
        info = inspect_host(host)
        if info["root_free_gib"] < 20.0:
            sys.exit(f"error: {label} ({host}) has < 20 GiB free ({info['root_free_gib']} GiB)")
        if not info["configfs_stream"]:
            sys.exit(f"error: {label} ({host}) missing /sys/kernel/config/thunderbolt/stream")

    run_tag = f"u4rd-bench-{int(time.time())}-{secrets.token_hex(3)}"
    remote_dir = f"/tmp/{run_tag}"
    epoch = secrets.randbelow((1 << 53) - 1) + 1
    throttling_ns = args.throttling_ns
    if throttling_ns is None:
        throttling_ns = 0
    if args.backend == "uring" and throttling_ns != 0:
        sys.exit("error: io_uring requires --throttling-ns 0")
    serve_overrides = "".join(
        f"--{option} {value} "
        for option, value in (
            ("spin-us", args.spin_us),
            ("active-probe-us", args.active_probe_us),
        )
        if value is not None
    )

    for host in (args.host_a, args.host_b):
        ssh(host, f"umask 077 && mkdir {shlex.quote(remote_dir)}")
        scp_to(binary, host, f"{remote_dir}/usb4-rdma-ctl")
        ssh(host, f"chmod +x {shlex.quote(remote_dir)}/usb4-rdma-ctl")

    owned_endpoints = [] if create_fresh_endpoints else None
    cfg_local = None
    try:
        endpoints = []
        prefix = args.stream_prefix
        if owned_endpoints is not None:
            # USB4STREAM endpoint names have an eight-byte ConfigFS limit.
            prefix = prefix[:2] + run_tag.rsplit("-", 1)[1][:4]
        for idx, (sa, sb) in enumerate(zip(svcs_a, svcs_b)):
            for _ in range(args.streams_per_cable):
                suffix = f"{len(endpoints):02x}" if owned_endpoints is not None else str(len(endpoints))
                ep_a, ep_b = provision_pair(
                    args.host_a,
                    args.host_b,
                    f"{remote_dir}/usb4-rdma-ctl",
                    sa,
                    sb,
                    prefix + suffix,
                    args.ring_size,
                    throttling_ns,
                    owned_endpoints,
                    busy_poll,
                )
                endpoints.append((ep_a, ep_b, f"ctrl{idx}", f"ctrl{idx}"))

        speed_per_stream = args.speed_mbps // args.streams_per_cable
        cfg_text = build_cluster_toml(epoch, endpoints, speed_per_stream, args.queue_depth)
        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as tf:
            tf.write(cfg_text)
            cfg_local = Path(tf.name)

        for host in (args.host_a, args.host_b):
            scp_to(cfg_local, host, f"{remote_dir}/cluster.toml")

        sock_a = f"{remote_dir}/node0.sock"
        sock_b = f"{remote_dir}/node1.sock"
        for host, node, sock in [(args.host_a, 0, sock_a), (args.host_b, 1, sock_b)]:
            cmd = (
                f"nohup {shlex.quote(remote_dir)}/usb4-rdma-ctl serve "
                f"--config {shlex.quote(remote_dir)}/cluster.toml --node {node} "
                f"--socket {shlex.quote(sock)} --backend {args.backend} {serve_overrides}"
                f">{shlex.quote(remote_dir)}/daemon.log 2>&1 < /dev/null & echo $! > {shlex.quote(remote_dir)}/daemon.pid"
            )
            ssh(host, cmd)

        # Wait until both daemons report fabric_ready=true.
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            r0 = ssh(args.host_a, f"{shlex.quote(remote_dir)}/usb4-rdma-ctl monitor --socket {shlex.quote(sock_a)} --links", check=False)
            r1 = ssh(args.host_b, f"{shlex.quote(remote_dir)}/usb4-rdma-ctl monitor --socket {shlex.quote(sock_b)} --links", check=False)
            if all(r.returncode == 0 and json.loads(r.stdout).get("fabric_ready") is True
                   for r in (r0, r1)):
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("USB4 RDMA daemons did not complete fabric handshake within 20s")

        # Start benchmark server on Host B.
        srv_cmd = (
            f"nohup {shlex.quote(remote_dir)}/usb4-rdma-ctl bench server "
            f"--bind {shlex.quote(args.eth_b)}:{args.port} "
            f"--rdma-socket {shlex.quote(sock_b)} --once "
            f">{shlex.quote(remote_dir)}/bench-server.log 2>&1 < /dev/null & echo $! > {shlex.quote(remote_dir)}/bench-server.pid"
        )
        ssh(args.host_b, srv_cmd)
        time.sleep(0.5)

        # Run benchmark client on Host A.
        cli_cmd = (
            f"{shlex.quote(remote_dir)}/usb4-rdma-ctl bench client "
            f"--peer {shlex.quote(args.eth_b)}:{args.port} "
            f"--rdma-socket {shlex.quote(sock_a)} "
            f"--suite {args.suite} --only {args.only} "
            f"--bw-total-bytes {args.bw_total_bytes} "
            f"--lat-sizes {shlex.quote(args.lat_sizes)} --bw-sizes {shlex.quote(args.bw_sizes)} "
            f"--lat-iters {args.lat_iters} --warmup {args.warmup} "
            f"--json {shlex.quote(remote_dir)}/results.json"
        )
        res = ssh(args.host_a, cli_cmd, timeout=600)
        print(res.stdout)

        if args.output:
            out_path = Path(args.output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            json_text = ssh(args.host_a, f"cat {shlex.quote(remote_dir)}/results.json").stdout
            out_path.write_text(json_text)
            print(f"Saved JSON results to {out_path}")
        return 0
    finally:
        if cfg_local is not None:
            cfg_local.unlink(missing_ok=True)
        failures = []
        for host in (args.host_a, args.host_b):
            try:
                if stop_run(host, remote_dir):
                    continue
            except (OSError, subprocess.SubprocessError):
                pass
            failures.append(host)
        if failures:
            raise RuntimeError(f"inspect {remote_dir} on {failures}: shutdown unverified")
        for host, endpoint in reversed(owned_endpoints or []):
            try:
                ssh(host, f"sudo -n rmdir {shlex.quote(endpoint)}")
            except (OSError, subprocess.SubprocessError):
                failures.append(f"{host}:{endpoint}")
        if failures:
            raise RuntimeError(f"ConfigFS endpoint cleanup failed for {failures}")
        print(f"Benchmark logs retained on both hosts in {remote_dir}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="USB4 RDMA vs Ethernet benchmark orchestrator")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_pre = sub.add_parser("preflight", help="Read-only preflight of USB4STREAM, Ethernet, and disk headroom")
    p_pre.add_argument("--host-a", required=True)
    p_pre.add_argument("--host-b", required=True)

    p_run = sub.add_parser("run", help="Provision endpoints, start daemons, and benchmark USB4 RDMA vs Ethernet")
    p_run.add_argument("--host-a", required=True, help="SSH alias for initiator (Node 0)")
    p_run.add_argument("--host-b", required=True, help="SSH alias for responder (Node 1)")
    p_run.add_argument("--eth-b", required=True, help="Ethernet IP of Host B for TCP benchmark & control")
    p_run.add_argument("--port", type=int, default=19400)
    p_run.add_argument("--services-a", required=True, help="Comma-separated USB4 stream services on Host A (e.g. 0-2.0 or 0-2.0,1-2.0)")
    p_run.add_argument("--services-b", required=True, help="Comma-separated USB4 stream services on Host B (e.g. 0-2.1 or 0-2.1,1-2.1)")
    p_run.add_argument("--artifacts-dir", required=True, help="Local directory containing Linux x86_64 usb4-rdma-ctl")
    p_run.add_argument("--backend", choices=["usb4-busy-poll", "uring"], default="uring")
    p_run.add_argument("--stream-prefix", default="u4rd",
                       help="Endpoint prefix (fresh endpoints use the first two ASCII characters)")
    p_run.add_argument("--fresh-endpoints", action="store_true",
                       help="Create unique ConfigFS endpoints and remove them after verified shutdown")
    p_run.add_argument("--streams-per-cable", type=int, choices=[1, 2], default=1,
                       help="Logical USB4STREAM paths per physical cable (default 1)")
    p_run.add_argument("--ring-size", type=int, default=256)
    p_run.add_argument("--queue-depth", type=int, help="Override the default fabric queue depth")
    p_run.add_argument("--spin-us", type=int, help="Override the daemon's post-progress spin budget")
    p_run.add_argument("--throttling-ns", type=int, help="Override USB4 interrupt moderation")
    p_run.add_argument("--active-probe-us", type=int,
                       metavar="50..1000", help="Override the recent-transfer progress interval")
    p_run.add_argument("--speed-mbps", type=int, default=40000,
                       help="Negotiated speed per physical cable (default 40000)")
    p_run.add_argument("--suite", choices=["all", "latency", "bandwidth", "allreduce"], default="all")
    p_run.add_argument("--only", choices=["both", "rdma", "tcp"], default="both")
    p_run.add_argument("--bw-total-bytes", type=int, default=256 * 1024 * 1024)
    p_run.add_argument("--lat-sizes", default="64,4096,65536,262144,1048576")
    p_run.add_argument("--bw-sizes", default="65536,1048576,4194304,16777216")
    p_run.add_argument("--lat-iters", type=int, default=100)
    p_run.add_argument("--warmup", type=int, default=10)
    p_run.add_argument("--output", help="Local path to save JSON benchmark results")

    args = parser.parse_args()
    if args.cmd == "preflight":
        return cmd_preflight(args)
    if args.queue_depth is not None and not 2 <= args.queue_depth <= 256:
        p_run.error("--queue-depth must be between 2 and 256")
    if args.spin_us is not None and not 0 <= args.spin_us <= 10000:
        p_run.error("--spin-us must be between 0 and 10000")
    if args.active_probe_us is not None and not 50 <= args.active_probe_us <= 1000:
        p_run.error("--active-probe-us must be between 50 and 1000")
    if args.throttling_ns is not None and not 0 <= args.throttling_ns <= 16_776_960:
        p_run.error("--throttling-ns must be between 0 and 16776960")
    return cmd_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
