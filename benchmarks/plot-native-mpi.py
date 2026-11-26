"""Plot paired native Open MPI benchmark matrices."""

import json
from pathlib import Path
from statistics import median

from chart_style import BLUE, ETHERNET as GRAY, ORANGE, PURPLE, TEAL, apply_style, chart, save_chart

apply_style()


ETHERNET = ("ethernet", "10 GbE TCP", GRAY, "o-")
REFERENCE = (
    ETHERNET,
    ("single-a", "USB4 cable 1", ORANGE, "s--"),
    ("single-b", "USB4 cable 2", BLUE, "D-."),
    ("dual", "Bonded USB4", TEAL, "^-"),
)
EAGER = (
    ETHERNET,
    ("single-a-candidate", "Eager, cable 1", ORANGE, "s--"),
    ("single-b-candidate", "Eager, cable 2", BLUE, "D-."),
    ("bond-base", "Rendezvous, 2 cables", PURPLE, "v--"),
    ("bond-candidate", "Eager, 2 cables", TEAL, "^-"),
)


def draw_panel(axis, runs, modes, metric, field):
    for mode, label, color, style in modes:
        samples = {}
        for series in runs[mode]:
            for row in series["measurements"]:
                if row["metric"] == metric:
                    samples.setdefault(row["bytes"], []).append(row[field])
        sizes = sorted(samples)
        axis.plot(sizes, [median(samples[size]) for size in sizes], style,
                  color=color, label=label, markersize=5, markeredgecolor="white", markeredgewidth=.6)
        axis.fill_between(sizes, [min(samples[size]) for size in sizes],
                          [max(samples[size]) for size in sizes], color=color, alpha=.13)
    axis.set_xscale("log", base=2)
    ticks = {8: "8 B", 4096: "4 KiB", 65536: "64 KiB", 1048576: "1 MiB",
             4194304: "4 MiB", 16777216: "16 MiB", 67108864: "64 MiB"}
    keys = (8, 4096, 65536, 1048576, 67108864) if metric == "half-rtt" else tuple(ticks)[2:]
    axis.set_xticks(keys, [ticks[key] for key in keys])
    axis.tick_params(pad=6)
    axis.grid(axis="y")


def draw(root, name, modes):
    data = json.loads((root / f"{name}.json").read_text())
    for metric, field, ylabel, title, suffix in (
        ("half-rtt", "p50_us", "p50 half-RTT (µs)", "MPI ping-pong", "latency"),
        ("one-way", "gbits_s", "Goodput (Gbit/s)", "MPI one-way goodput", "goodput"),
    ):
        fig, axis = chart(title)
        draw_panel(axis, data["runs"], modes, metric, field)
        axis.set(ylabel=ylabel, xlabel="Message size")
        if metric == "half-rtt":
            axis.set_yscale("log")
        else:
            axis.set_ylim(0, None)
        axis.legend()
        save_chart(fig, root / f"{name}-{suffix}.png")


if __name__ == "__main__":
    root = Path(__file__).resolve().parent / "results"
    draw(root, "native-mpi", REFERENCE)
    if (root / "mpi-eager.json").is_file():
        draw(root, "mpi-eager", EAGER)
