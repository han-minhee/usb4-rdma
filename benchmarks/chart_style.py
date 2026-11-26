"""Shared typography and visual defaults for the benchmark figures."""

from pathlib import Path

import matplotlib
from matplotlib import font_manager, pyplot as plt, style
from matplotlib.patches import FancyBboxPatch

PANEL = "#ffffff"
TEXT = "#14283d"
MUTED = "#475d70"
GRID = "#e1e9ef"
ETHERNET = "#536476"
TEAL = "#00786e"
BLUE = "#295f9e"
ORANGE = "#a7542c"
PURPLE = "#75568d"
SLATE = "#667f8c"


matplotlib.use("Agg")


def apply_style():
    root = Path(__file__).resolve().parent
    for weight in ("Regular", "Bold"):
        font_manager.fontManager.addfont(root / "fonts" / f"SourceSans3-{weight}.otf")
    style.use(root / "usb4.mplstyle")


def chart(title, *, height=3.8):
    figure, axis = plt.subplots(figsize=(7.6, height), layout="constrained")
    axis.set_title(title, loc="left")
    return figure, axis


def save_chart(figure, path):
    figure.savefig(path, metadata={"Software": "usb4-rdma"})
    plt.close(figure)


def horizontal_axis(axis, labels, maximum, unit):
    axis.set(yticks=range(len(labels)), ylim=(len(labels) - .5, -.5),
             xlim=(0, maximum), xlabel=unit)
    axis.set_yticklabels(labels, color=TEXT)
    axis.spines[:].set_visible(False)
    axis.tick_params(axis="y", pad=10)
    axis.grid(axis="x")


def rounded_bar(axis, y, width, height, color):
    # Match the corner radii on screen even when the data scales differ.
    xscale, yscale = axis.transData.get_affine().get_matrix().diagonal()[:2]
    aspect = abs(xscale / yscale)
    radius = min(width / 2, height / (2 * aspect))
    axis.add_patch(FancyBboxPatch(
        (0, y - height / 2), width, height,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        mutation_aspect=aspect, linewidth=0, facecolor=color,
    ))
