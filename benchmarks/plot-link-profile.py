"""Plot ROCm RCCL two-host link scaling."""

import json
from pathlib import Path

from matplotlib.lines import Line2D

from chart_style import ETHERNET, MUTED, PANEL, TEAL, apply_style, chart, horizontal_axis, rounded_bar, save_chart

RESULTS = Path(__file__).resolve().parent / "results"
DATA = json.loads((RESULTS / "link-profile-20261125.json").read_text())
TCP = ("tcp", "10 GbE")
SINGLES = (
    ("single_stream", "1 cable, 1 stream"),
    ("single_cable_two_streams", "1 cable, 2 streams"),
)
POLLING = (
    ("bond_readiness", "2 cables, readiness"),
    ("bond_busy_poll", "2 cables, busy polling"),
)


def percentile_range(axis, y, p50, p99, color, digits):
    axis.plot([p50, p99], [y, y], color=color, alpha=.49, linewidth=2.8)
    axis.plot(p50, y, marker="o", markerfacecolor=PANEL, markeredgecolor=color,
              markeredgewidth=1.6, markersize=6.7)
    axis.plot(p99, y, marker="D", color=color, markersize=5)
    axis.annotate(f"{p99:.{digits}f}", (p99, y), xytext=(7, 0), textcoords="offset points",
                  color=color, va="center", weight="bold")


def legend(axis):
    handles = [
        Line2D([], [], color=MUTED, marker="o", markerfacecolor=PANEL, linestyle="none", label="p50"),
        Line2D([], [], color=MUTED, marker="D", linestyle="none", markersize=5, label="p99"),
    ]
    axis.legend(handles=handles, loc="upper right",
                ncols=len(handles), handletextpad=.4, columnspacing=1.1)


def rccl_plot():
    large_fig, large = chart("16 MiB RCCL AllReduce")
    small_fig, small = chart("8 KiB RCCL AllReduce")
    cases = (TCP, *SINGLES, *POLLING)
    labels = [label for _, label in cases]
    horizontal_axis(large, labels, 19.2, "p50 (ms)")
    horizontal_axis(small, labels, 1.98, "Latency (ms)")
    for index, (case, _) in enumerate(cases):
        ranks = DATA["rccl"]["allreduce_us_by_rank"][case]
        p50 = max(rank["8192"]["p50_us"] for rank in ranks) / 1000
        p99 = max(rank["8192"]["p99_us"] for rank in ranks) / 1000
        p50_large = max(rank["16777216"]["p50_us"] for rank in ranks) / 1000
        color = ETHERNET if case == "tcp" else TEAL
        rounded_bar(large, index, p50_large, .51, color)
        large.annotate(f"{p50_large:.2f}", (p50_large, index), xytext=(7, 0),
                       textcoords="offset points", color=color, va="center", weight="bold")
        percentile_range(small, index, p50, p99, color, 3)

    legend(small)
    save_chart(large_fig, RESULTS / "rccl-link-16mib.png")
    save_chart(small_fig, RESULTS / "rccl-link-8kib.png")


if __name__ == "__main__":
    apply_style()
    rccl_plot()
