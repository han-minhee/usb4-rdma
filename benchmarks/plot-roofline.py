"""Render the layered USB4 Gen3x2 roofline and optimization charts."""
import json
from pathlib import Path

from chart_style import (
    BLUE, ETHERNET, GRID, MUTED, ORANGE, PANEL, SLATE, TEAL, TEXT,
    apply_style, chart, horizontal_axis, rounded_bar, save_chart,
)

from matplotlib.ticker import FuncFormatter

apply_style()

RESULTS = Path(__file__).resolve().parent / "results"
DATA = json.loads((RESULTS / "roofline-20261125.json").read_text())

BASELINE_2S = SLATE
BASELINE_4S_BP = ORANGE
BASELINE_4S_USB4 = BLUE
OPTIMIZED = TEAL
ROOF_COLOR = TEXT


def bw_val(run_key, transport, op, size, field="gbits_per_sec"):
    for r in DATA["rdma"][run_key]["bandwidth"]:
        if r["transport"] == transport and r["op"] == op and r["size_bytes"] == size:
            return r[field]
    raise KeyError((run_key, transport, op, size))


def lat_val(run_key, transport, op, size, field="p50_us"):
    for r in DATA["rdma"][run_key]["latency"]:
        if r["transport"] == transport and r["op"] == op and r["size_bytes"] == size:
            return r[field]
    raise KeyError((run_key, transport, op, size))


def ar_val(run_key, transport, byte_len, field="algbw_gbytes_per_sec"):
    for r in DATA["rdma"][run_key]["allreduce"]:
        if r["transport"] == transport and r["bytes"] == byte_len:
            return r[field]
    raise KeyError((run_key, transport, byte_len))


def raw_gbps(key):
    s = DATA["raw_stream"]["four_streams_two_per_cable"][key]
    return sum(x["bytes"] for x in s) / max(x["sender_seconds"] for x in s) * 8.0 / 1e9


def style_axes(ax, grid_axis="x"):
    ax.set_facecolor(PANEL)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(GRID)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=TEXT, labelsize=9.5, length=0, pad=6)
    ax.xaxis.label.set_color(MUTED)
    ax.yaxis.label.set_color(MUTED)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)



def plot_hero():
    write_fig, write = chart("16 MiB one-way writes")
    duplex_fig, duplex = chart("16 MiB full duplex")
    latency_fig, latency = chart("Host latency by operation", height=4.3)

    rows = (
        ("10 GbE TCP", "optimized_vs_ethernet", "ethernet", ETHERNET),
        ("2 streams · readiness", "main_two_streams_readiness", "usb4-rdma", BASELINE_2S),
        ("4 streams · busy polling", "main_four_streams_busy_poll", "usb4-rdma", BASELINE_4S_BP),
        ("4 streams · readiness", "main_four_streams_readiness", "usb4-rdma", BASELINE_4S_USB4),
        ("4 streams · optimized", "optimized_four_streams_busy_poll_qd128",
         "usb4-rdma", OPTIMIZED),
    )
    horizontal_axis(write, [label for label, *_ in rows], 52, "Payload goodput (Gbit/s)")
    horizontal_axis(duplex, [label for label, *_ in rows], 88, "Aggregate goodput (Gbit/s)")
    for index, (_, key, transport, color) in enumerate(rows):
        operations = ("tcp_uni", "tcp_duplex") if transport == "ethernet" else ("rdma_write_uni", "rdma_duplex")
        for axis, operation in zip((write, duplex), operations):
            value = bw_val(key, transport, operation, 16777216)
            rounded_bar(axis, index, value, .47, color)
            axis.annotate(f"{value:.2f}", (value, index), xytext=(6, 0), textcoords="offset points", color=color, va="center", weight="bold",
                          bbox={"facecolor": PANEL, "edgecolor": "none", "pad": .1})

    raw = raw_gbps("throttling_0_busy_poll_256k")
    write.axvline(raw, color=TEXT, linewidth=1.1, linestyle="--", zorder=1,
                  label=f"Device {raw:.1f} Gbit/s")
    write.legend(loc="upper right")

    metrics = (
        ("64 B ping-pong", "rdma_send_recv", "tcp_pingpong", 64),
        ("64 B RDMA read", "rdma_read", None, 64),
        ("64 B RDMA write", "rdma_write", None, 64),
        ("64 KiB RDMA write", "rdma_write", None, 65536),
        ("1 MiB f32 AllReduce", "allreduce", "allreduce", 1048576),
    )
    horizontal_axis(latency, [label for label, *_ in metrics], 8000, "Latency (µs, log scale)")
    latency.set_xscale("log")
    latency.set_xlim(8, 8000)
    latency.set_xticks([10, 100, 1000])
    latency.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
    series = (
        ("10 GbE", "optimized_vs_ethernet", "ethernet", ETHERNET, -.24),
        ("Readiness", "main_four_streams_readiness", "usb4-rdma", BASELINE_4S_USB4, -.08),
        ("Busy polling", "main_four_streams_busy_poll", "usb4-rdma", BASELINE_4S_BP, .08),
        ("Optimized", "optimized_four_streams_busy_poll", "usb4-rdma", OPTIMIZED, .24),
    )
    for index, (_, usb_op, tcp_op, size) in enumerate(metrics):
        for label, key, transport, color, shift in series:
            operation = tcp_op if transport == "ethernet" else usb_op
            if operation is None:
                continue
            value = (ar_val(key, transport, size, "p50_us") if operation == "allreduce"
                     else lat_val(key, transport, operation, size))
            y = index + shift
            latency.plot(value, y, marker="o", linestyle="none", color=color, markersize=5.8,
                         label=label if index == 0 else None)
            if color == OPTIMIZED:
                latency.annotate(f"{value:.1f}", (value, y), xytext=(6, -1), textcoords="offset points",
                                 color=color, va="center", fontsize=9, weight="bold")
    latency.legend(loc="upper right")
    save_chart(write_fig, RESULTS / "roofline-write.png")
    save_chart(duplex_fig, RESULTS / "roofline-duplex.png")
    save_chart(latency_fig, RESULTS / "roofline-latency.png")


def plot_scaling():
    bw_fig, ax_bw = chart("Goodput by payload size")
    lat_fig, ax_lat = chart("RDMA write and TCP ping-pong")
    ar_fig, ax_ar = chart("Host f32 AllReduce")

    def fmt_bytes(x, _):
        if x >= 1048576:
            return f"{x // 1048576} MiB"
        if x >= 1024:
            return f"{x // 1024} KiB"
        return f"{int(x)} B"

    # Panel A: Bandwidth scaling (64 KiB .. 16 MiB)
    sizes_bw = [65536, 1048576, 4194304, 16777216]
    style_axes(ax_bw, grid_axis="y")
    ax_bw.set_xscale("log", base=2)
    ax_bw.set_xticks(sizes_bw)
    ax_bw.xaxis.set_major_formatter(FuncFormatter(fmt_bytes))
    ax_bw.set_ylim(0, 102)
    ax_bw.set_xlabel("Payload size")
    ax_bw.set_ylabel("Goodput (Gbit/s)")

    curves_bw = [
        ("Optimized duplex", "optimized_four_streams_busy_poll_qd128", "usb4-rdma", "rdma_duplex", OPTIMIZED, "-", "o"),
        ("Baseline duplex", "main_four_streams_busy_poll", "usb4-rdma", "rdma_duplex", BASELINE_4S_BP, "--", "s"),
        ("Optimized read", "optimized_four_streams_busy_poll_qd128", "usb4-rdma", "rdma_read_uni", BASELINE_4S_USB4, "-", "^"),
        ("2-stream write", "main_two_streams_readiness", "usb4-rdma", "rdma_write_uni", BASELINE_2S, ":", "d"),
        ("10 GbE send", "optimized_vs_ethernet", "ethernet", "tcp_uni", ETHERNET, "-.", "x"),
    ]
    for label, key, tr, op, color, ls, mk in curves_bw:
        ys = [bw_val(key, tr, op, s) for s in sizes_bw]
        ax_bw.plot(sizes_bw, ys, label=label, color=color, linestyle=ls, marker=mk, linewidth=2.0, markersize=5.2)

    raw_uni = raw_gbps("throttling_0_busy_poll_256k")
    ax_bw.axhline(raw_uni, color=ROOF_COLOR, linestyle=":", linewidth=1.1, alpha=0.8)
    ax_bw.text(sizes_bw[-1], raw_uni + 2.0, f"L3 uni ceiling {raw_uni:.1f} Gb/s", fontsize=8.0, color=ROOF_COLOR, ha="right")
    ax_bw.legend(loc="upper left", ncols=2, fontsize=9.2, columnspacing=.8)

    # Panel B: Latency scaling (64 B .. 1 MiB)
    sizes_lat = [64, 4096, 65536, 262144, 1048576]
    style_axes(ax_lat, grid_axis="y")
    ax_lat.set_xscale("log", base=2)
    ax_lat.set_yscale("log")
    ax_lat.set_xticks([64, 4096, 65536, 1048576])
    ax_lat.xaxis.set_major_formatter(FuncFormatter(fmt_bytes))
    ax_lat.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):g}"))
    ax_lat.set_ylim(5, 4200)
    ax_lat.set_xlabel("Payload size")
    ax_lat.set_ylabel("Median latency (µs, log)")

    curves_lat = [
        ("10 GbE ping-pong", "optimized_vs_ethernet", "ethernet", "tcp_pingpong", ETHERNET, "-.", "x"),
        ("Readiness write", "main_four_streams_readiness", "usb4-rdma", "rdma_write", BASELINE_4S_USB4, "--", "s"),
        ("Busy-poll write", "main_four_streams_busy_poll", "usb4-rdma", "rdma_write", BASELINE_4S_BP, ":", "^"),
        ("Optimized write", "optimized_four_streams_busy_poll", "usb4-rdma", "rdma_write", OPTIMIZED, "-", "o"),
    ]
    for label, key, tr, op, color, ls, mk in curves_lat:
        ys = [lat_val(key, tr, op, s) for s in sizes_lat]
        ax_lat.plot(sizes_lat, ys, label=label, color=color, linestyle=ls, marker=mk, linewidth=2.0, markersize=5.2)

    raw_pts = DATA["raw_stream"]["ping_half_rtt_us"]["throttling_0_busy_poll"]
    ax_lat.plot([64, 4096], [raw_pts["64"]["p50_us"], raw_pts["4096"]["p50_us"]],
                color=ROOF_COLOR, linestyle="none", marker="*", markersize=7.5, label="Raw stream")
    ax_lat.legend(loc="upper left", ncols=2, fontsize=9.2, columnspacing=.8)

    # Panel C: 2-Node Host f32 SUM AllReduce AlgBW
    ar_sizes = [65536, 1048576, 4194304]
    ar_labels = ["64 KiB", "1 MiB", "4 MiB"]
    style_axes(ax_ar, grid_axis="y")
    ax_ar.set_xticks(range(len(ar_sizes)))
    ax_ar.set_xticklabels(ar_labels, fontsize=9.5)
    ax_ar.set_ylim(0, 4.6)
    ax_ar.set_xlabel("AllReduce vector size (f32 SUM)")
    ax_ar.set_ylabel("Algorithmic BW (GB/s)")
    ax_ar.set_title("Two-host f32 SUM AllReduce", loc="left", fontsize=14, weight="bold", pad=10, color=TEXT)

    ar_series = [
        ("10 GbE TCP", "optimized_vs_ethernet", "ethernet", ETHERNET, -0.30),
        ("main 2s usb4", "main_two_streams_readiness", "usb4-rdma", BASELINE_2S, -0.10),
        ("main 4s busy-poll", "main_four_streams_busy_poll", "usb4-rdma", BASELINE_4S_BP, 0.10),
        ("Opt 4s busy-poll", "optimized_four_streams_busy_poll_qd128", "usb4-rdma", OPTIMIZED, 0.30),
    ]
    for label, key, tr, color, shift in ar_series:
        vals = [ar_val(key, tr, s, "algbw_gbytes_per_sec") for s in ar_sizes]
        xs = [i + shift for i in range(len(ar_sizes))]
        ax_ar.bar(xs, vals, width=0.18, color=color, label=label)
        if label.startswith("Opt"):
            for x, value in zip(xs, vals):
                ax_ar.text(x, value + .08, f"{value:.2f}", ha="center", va="bottom",
                           fontsize=10, color=color, weight="bold")

    ax_ar.legend(loc="upper left", ncols=2, frameon=True, facecolor=PANEL, edgecolor="none", fontsize=9.5)

    save_chart(bw_fig, RESULTS / "roofline-goodput-scaling.png")
    save_chart(lat_fig, RESULTS / "roofline-latency-scaling.png")
    save_chart(ar_fig, RESULTS / "roofline-allreduce.png")


if __name__ == "__main__":
    plot_hero()
    plot_scaling()
