"""Chart the paired two-host ROCm inference fixtures."""
import json
from pathlib import Path


from chart_style import ETHERNET, TEAL, apply_style, chart, save_chart


apply_style()


def draw(axis, rows, field, units, labels):
    for index, (mode, label, color) in enumerate((
        ("ethernet", "10 GbE", ETHERNET),
        ("usb4", "Bonded USB4", TEAL),
    )):
        positions = [rank + (index - 0.5) * 0.34 for rank in range(len(rows))]
        values = [row[mode][field] for row in rows]
        bars = axis.barh(positions, values, height=0.32, color=color, label=label)
        axis.bar_label(bars, fmt="%.2f", fontsize=7, padding=3)
    axis.set_yticks(range(len(rows)), labels, fontsize=8)
    axis.invert_yaxis()
    axis.set_xlabel(units)
    axis.set_xlim(0, axis.get_xlim()[1] * 1.13)
    axis.set_axisbelow(True)
    axis.grid(axis="x", color="#cbd5e1", alpha=.4)
    axis.spines[["top", "right"]].set_visible(False)


def main():
    root = Path(__file__).resolve().parent / "results"
    report = json.loads((root / "llm-v01.json").read_text())
    for key, name in (("qwen", "Qwen dummy"), ("gemma", "Gemma 4 31B")):
        rows = report[key]["summary"]
        labels = [f"{row['shape']} tokens (c{row['concurrency']})" for row in rows] if key == "qwen" else [
            "Short (c1)", "Long (c1)", "Short (c4)"
        ]
        for field, metric, units in (
            ("ttft_seconds", "TTFT", "Time to first token (s)"),
            ("throughput_tokens_per_second", "throughput", "Output tokens/s"),
        ):
            figure, axis = chart(f"{name} {metric}", height=3.5)
            draw(axis, rows, field, units, labels)
            axis.legend(loc="upper right", ncols=2)
            save_chart(figure, root / f"llm-v01-{key}-{metric.lower()}.png")


if __name__ == "__main__":
    main()
