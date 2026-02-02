#!/usr/bin/env python3
"""Generate validation figures for simulator vs real GossipSub topology data."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from calibrate_gossipsub import (
    DEFAULT_METRICS,
    DEFAULT_WEIGHTS,
    collect_sizes,
    find_metrics_dir,
    load_calibration_metrics,
    parse_csv_float,
    read_csv_rows,
    relative_error,
    stable_rows,
)


STRUCTURAL_METRICS: list[tuple[str, str]] = [
    ("stable_density_directed", "density"),
    ("stable_mean_degree", "mean degree"),
    ("stable_median_degree", "median degree"),
    ("stable_average_clustering_coefficient", "clustering"),
    ("stable_avg_shortest_path_length", "path length"),
    ("stable_component_count", "components"),
    ("stable_largest_component_size", "giant comp."),
    ("stable_diameter", "diameter"),
]

GLOBAL_PANELS: list[tuple[str, str]] = [
    ("stable_density_directed", "Directed density"),
    ("stable_average_clustering_coefficient", "Average clustering"),
    ("stable_avg_shortest_path_length", "Average shortest path (hops)"),
    ("stable_component_count", "Connected components (count)"),
    ("stable_largest_component_size", "Largest component (nodes)"),
    ("stable_diameter", "Diameter (hops)"),
]

FIGURE_STEMS: list[str] = [
    "global_metrics_by_size",
    "jaccard_timeseries_by_size",
    "metric_ratio_by_size",
    "real_vs_sim_scatter",
    "stable_mean_degree_by_size",
]

OBSOLETE_FIGURE_STEMS: list[str] = [
    "calibration_score_by_size",
    "churn_timeseries_by_size",
    "control_events_by_size",
    "degree_cdf_by_size",
    "degree_timeseries_by_size",
]

_LINEWIDTH_IN: float = 6.30
_FIG_FONT_SIZE: int = 9


def natural_size_key(size: str) -> tuple[int, str]:
    match = re.search(r"\d+", size)
    if match:
        return (int(match.group(0)), size)
    return (10**9, size)


def configure_style() -> None:
    plt.rcParams.update(
        {
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "black",
            "axes.labelcolor": "black",
            "text.color": "black",
            "xtick.color": "black",
            "ytick.color": "black",
            "font.size": _FIG_FONT_SIZE,
            "axes.titlesize": _FIG_FONT_SIZE,
            "axes.labelsize": _FIG_FONT_SIZE,
            "xtick.labelsize": _FIG_FONT_SIZE,
            "ytick.labelsize": _FIG_FONT_SIZE,
            "legend.fontsize": _FIG_FONT_SIZE,
            "legend.frameon": False,
            "axes.grid": True,
            "grid.color": "0.85",
            "grid.linewidth": 0.6,
            "grid.alpha": 0.8,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "lines.linewidth": 1.8,
            "errorbar.capsize": 3,
        }
    )


def load_side(metrics_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    return {
        "metrics_dir": metrics_dir,
        "summary": load_calibration_metrics(
            metrics_dir,
            args.stable_window_fraction,
            args.stable_window_min_rows,
            args.aggregation,
        ),
        "churn_rows": read_csv_rows(metrics_dir / "churn_timeseries.csv"),
    }


def load_all_data(
    sim_root: Path,
    real_root: Path,
    sizes: list[str],
    args: argparse.Namespace,
) -> dict[str, dict[str, Any]]:
    data: dict[str, dict[str, Any]] = {}
    for size in sizes:
        sim_metrics_dir = find_metrics_dir(sim_root, size)
        real_metrics_dir = find_metrics_dir(real_root, size)
        for label, metrics_dir in [("sim", sim_metrics_dir), ("real", real_metrics_dir)]:
            if not (metrics_dir / "summary.json").exists():
                raise SystemExit(f"Missing {label} metrics for {size}: {metrics_dir}")
        data[size] = {
            "sim": load_side(sim_metrics_dir, args),
            "real": load_side(real_metrics_dir, args),
        }
    return data


def numeric_series(
    rows: list[dict[str, str]], x_field: str, y_field: str
) -> tuple[list[float], list[float]]:
    xs: list[float] = []
    ys: list[float] = []
    for row in rows:
        x = parse_csv_float(row.get(x_field))
        y = parse_csv_float(row.get(y_field))
        if x is not None and y is not None:
            xs.append(x)
            ys.append(y)
    return xs, ys


def compute_score_rows(
    data: dict[str, dict[str, Any]],
    sizes: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float]:
    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for size in sizes:
        sim_summary = data[size]["sim"]["summary"]
        real_summary = data[size]["real"]["summary"]
        node_count = int(round(real_summary.get("node_count") or sim_summary.get("node_count") or 0))

        weighted_sum = 0.0
        weight_sum = 0.0
        for metric, label in DEFAULT_METRICS:
            sim_value = sim_summary.get(metric)
            real_value = real_summary.get(metric)
            weight = DEFAULT_WEIGHTS.get(metric, 1.0)
            row: dict[str, Any] = {
                "size": size,
                "metric": metric,
                "label": label,
                "sim": sim_value,
                "real": real_value,
                "abs_diff": None,
                "rel_error": None,
                "weight": weight,
                "weighted_error": None,
            }
            if sim_value is not None and real_value is not None:
                rel_err = relative_error(float(sim_value), float(real_value), metric, node_count)
                row["abs_diff"] = abs(float(sim_value) - float(real_value))
                row["rel_error"] = rel_err
                row["weighted_error"] = rel_err * weight
                weighted_sum += rel_err * weight
                weight_sum += weight
            rows.append(row)

        objective = weighted_sum / weight_sum if weight_sum else 0.0
        summaries.append(
            {
                "size": size,
                "objective": objective,
                "weighted_sum": weighted_sum,
                "weight_sum": weight_sum,
                "node_count": node_count,
            }
        )

    overall = sum(row["objective"] for row in summaries) / len(summaries) if summaries else 0.0
    return rows, summaries, overall


def savefig(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def heartbeat_axis_label(heartbeat_ms: int) -> str:
    if heartbeat_ms % 1000 == 0:
        seconds = heartbeat_ms // 1000
        unit = f"{seconds} s" if seconds != 1 else "1 s"
    else:
        unit = f"{heartbeat_ms} ms"
    return f"heartbeat index ({unit} per heartbeat)"


def plot_stable_degree_bars(
    data: dict[str, dict[str, Any]], sizes: list[str], out_dir: Path
) -> None:
    x = list(range(len(sizes)))
    width = 0.36
    sim_values = [data[size]["sim"]["summary"].get("stable_mean_degree") or 0.0 for size in sizes]
    real_values = [data[size]["real"]["summary"].get("stable_mean_degree") or 0.0 for size in sizes]
    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.0))
    ax.bar([i - width / 2 for i in x], sim_values, width, label="simulator", color="#4C72B0")
    ax.bar([i + width / 2 for i in x], real_values, width, label="real GossipSub", color="#DD8452")
    ax.set_xlabel("network size (nodes)")
    ax.set_ylabel("mean out-degree (directed mesh peers/node)")
    ax.set_xticks(x, [size.replace("-nodes", "") for size in sizes])
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.12), ncol=2)
    savefig(fig, out_dir / "stable_mean_degree_by_size.pdf")


def plot_global_panels(
    data: dict[str, dict[str, Any]], sizes: list[str], out_dir: Path
) -> None:
    fig, axes = plt.subplots(3, 2, figsize=(_LINEWIDTH_IN, 7.5), squeeze=False)
    x = list(range(len(sizes)))
    width = 0.36
    for ax, (metric, title) in zip(axes.flatten(), GLOBAL_PANELS):
        sim_values = [data[size]["sim"]["summary"].get(metric) or 0.0 for size in sizes]
        real_values = [data[size]["real"]["summary"].get(metric) or 0.0 for size in sizes]
        ax.bar([i - width / 2 for i in x], sim_values, width, label="simulator", color="#4C72B0")
        ax.bar([i + width / 2 for i in x], real_values, width, label="real GossipSub", color="#DD8452")
        ax.set_title(title)
        ax.set_xlabel("network size (nodes)")
        ax.set_xticks(x, [size.replace("-nodes", "") for size in sizes])
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.995), ncol=2)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(out_dir / "global_metrics_by_size.pdf", dpi=180, bbox_inches="tight")
    plt.close(fig)


def stable_start(rows: list[dict[str, str]], args: argparse.Namespace) -> float | None:
    selected = stable_rows(rows, args.stable_window_fraction, args.stable_window_min_rows)
    if not selected:
        return None
    return parse_csv_float(selected[0].get("heartbeat_index"))


def plot_timeseries(
    data: dict[str, dict[str, Any]],
    sizes: list[str],
    args: argparse.Namespace,
    rows_key: str,
    value_field: str,
    ylabel: str,
    filename: str,
    out_dir: Path,
    heartbeat_ms: int,
    middle_ylabel_only: bool = False,
) -> None:
    fig, axes = plt.subplots(
        len(sizes), 1,
        figsize=(_LINEWIDTH_IN, 2.3 * len(sizes)),
        sharex=False,
        squeeze=False,
    )
    middle_index = len(sizes) // 2
    for index, (ax, size) in enumerate(zip(axes[:, 0], sizes)):
        for side_name, label, color in [
            ("sim", "simulator", "#4C72B0"),
            ("real", "real GossipSub", "#DD8452"),
        ]:
            rows = data[size][side_name][rows_key]
            xs, ys = numeric_series(rows, "heartbeat_index", value_field)
            if xs and ys:
                ax.plot(xs, ys, label=label, color=color, linewidth=1.8)
        start_candidates = [
            stable_start(data[size][side_name][rows_key], args)
            for side_name in ["sim", "real"]
        ]
        starts = [value for value in start_candidates if value is not None]
        if starts:
            ax.axvline(min(starts), color="#555555", linestyle="--", linewidth=1, alpha=0.7)
        ax.set_title(size)
        if not middle_ylabel_only or index == middle_index:
            ax.set_ylabel(ylabel)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.01), ncol=2)
    axes[-1][0].set_xlabel(heartbeat_axis_label(heartbeat_ms))
    path = out_dir / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=[0, 0, 1, 0.985])
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_metric_ratios(
    data: dict[str, dict[str, Any]], sizes: list[str], out_dir: Path
) -> None:
    fig, axes = plt.subplots(
        len(sizes), 1,
        figsize=(_LINEWIDTH_IN, 2.3 * len(sizes)),
        squeeze=False,
    )
    metric_labels = [label for _, label in STRUCTURAL_METRICS]
    x = list(range(len(STRUCTURAL_METRICS)))
    for ax, size in zip(axes[:, 0], sizes):
        ratios: list[float] = []
        for metric, _ in STRUCTURAL_METRICS:
            sim_value = data[size]["sim"]["summary"].get(metric)
            real_value = data[size]["real"]["summary"].get(metric)
            if sim_value is None or real_value in (None, 0):
                ratios.append(float("nan"))
            else:
                ratios.append(float(sim_value) / float(real_value))
        ax.plot(x, ratios, marker="o", color="#55A868", linewidth=1.8)
        ax.axhline(1.0, color="#333333", linestyle="--", linewidth=1)
        ax.set_title(size)
        ax.set_ylabel("sim / real")
        ax.set_xticks(x, metric_labels, rotation=25, ha="right")
        finite_ratios = [value for value in ratios if math.isfinite(value)]
        if finite_ratios:
            low = max(0.0, min(finite_ratios) - 0.15)
            high = max(finite_ratios) + 0.15
            ax.set_ylim(low, high)
    savefig(fig, out_dir / "metric_ratio_by_size.pdf")


def plot_real_vs_sim_scatter(
    data: dict[str, dict[str, Any]], sizes: list[str], out_dir: Path
) -> None:
    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, _LINEWIDTH_IN))
    colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2", "#937860"]
    all_values: list[float] = []
    for index, size in enumerate(sizes):
        xs: list[float] = []
        ys: list[float] = []
        for metric, _ in STRUCTURAL_METRICS:
            sim_value = data[size]["sim"]["summary"].get(metric)
            real_value = data[size]["real"]["summary"].get(metric)
            if sim_value is None or real_value is None:
                continue
            if float(sim_value) <= 0 or float(real_value) <= 0:
                continue
            xs.append(float(real_value))
            ys.append(float(sim_value))
            all_values.extend([float(real_value), float(sim_value)])
        ax.scatter(xs, ys, label=size, s=55, alpha=0.85, color=colors[index % len(colors)])
    if all_values:
        low = min(all_values) * 0.8
        high = max(all_values) * 1.25
        ax.plot([low, high], [low, high], color="#333333", linestyle="--", linewidth=1)
        ax.set_xlim(low, high)
        ax.set_ylim(low, high)
        ax.set_xscale("log")
        ax.set_yscale("log")
    ax.set_xlabel("real GossipSub value (metric units, log scale)")
    ax.set_ylabel("simulator value (same units, log scale)")
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5))
    savefig(fig, out_dir / "real_vs_sim_scatter.pdf")


def write_score_csv(rows: list[dict[str, Any]], out_dir: Path) -> None:
    path = out_dir / "validation_metric_scores.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_summary(
    out_dir: Path,
    sim_root: Path,
    real_root: Path,
    summaries: list[dict[str, Any]],
    overall: float,
    rows: list[dict[str, Any]],
    args: argparse.Namespace,
    figure_paths: list[Path],
) -> None:
    payload = {
        "sim_root": str(sim_root),
        "real_root": str(real_root),
        "stable_window_fraction": args.stable_window_fraction,
        "stable_window_min_rows": args.stable_window_min_rows,
        "aggregation": args.aggregation,
        "overall_objective": overall,
        "sizes": summaries,
        "metrics": rows,
        "figures": [path.name for path in figure_paths],
    }
    with (out_dir / "validation_summary.json").open("w") as handle:
        json.dump(payload, handle, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sim-root",
        default="/root/virgile/kademlia-simulator/simulator/results/gossipsub_comparison",
        help="Simulator comparison results root.",
    )
    parser.add_argument(
        "--real-root",
        default="/root/virgile/GossipSub-Minimal/prototype/results",
        help="Real Grid5000 GossipSub results root.",
    )
    parser.add_argument(
        "--sizes",
        nargs="*",
        default=["20-nodes", "50-nodes", "100-nodes"],
        help="Size directories to compare.",
    )
    parser.add_argument(
        "--out-dir",
        default="/root/virgile/kademlia-simulator/simulator/results/gossipsub_validation_figures",
        help="Directory where figures and summaries are written.",
    )
    parser.add_argument(
        "--stable-window-fraction",
        type=float,
        default=0.20,
        help="Tail fraction used as the stable comparison window.",
    )
    parser.add_argument(
        "--stable-window-min-rows",
        type=int,
        default=10,
        help="Minimum number of rows in the stable comparison window.",
    )
    parser.add_argument(
        "--aggregation",
        choices=["median", "mean", "last"],
        default="median",
        help="Aggregation used for stable-window topology metrics.",
    )
    parser.add_argument(
        "--heartbeat-ms",
        type=int,
        default=1000,
        help="Heartbeat duration used in time-series axis labels.",
    )
    return parser.parse_args()


def cleanup_generated_outputs(out_dir: Path) -> None:
    for stem in FIGURE_STEMS + OBSOLETE_FIGURE_STEMS:
        for suffix in [".pdf", ".png", ".svg"]:
            path = out_dir / f"{stem}{suffix}"
            if path.exists():
                path.unlink()
    obsolete_markdown = out_dir / "validation_summary.md"
    if obsolete_markdown.exists():
        obsolete_markdown.unlink()


def main() -> None:
    args = parse_args()

    sim_root = Path(args.sim_root)
    real_root = Path(args.real_root)
    out_dir = Path(args.out_dir)
    sizes = sorted(collect_sizes(sim_root, real_root, args.sizes), key=natural_size_key)
    if not sizes:
        raise SystemExit("No overlapping size directories found.")

    data = load_all_data(sim_root, real_root, sizes, args)
    rows, summaries, overall = compute_score_rows(data, sizes)
    out_dir.mkdir(parents=True, exist_ok=True)
    cleanup_generated_outputs(out_dir)

    configure_style()
    plot_stable_degree_bars(data, sizes, out_dir)
    plot_global_panels(data, sizes, out_dir)
    plot_metric_ratios(data, sizes, out_dir)
    plot_real_vs_sim_scatter(data, sizes, out_dir)
    plot_timeseries(
        data,
        sizes,
        args,
        rows_key="churn_rows",
        value_field="jaccard_similarity",
        ylabel="Jaccard similarity",
        filename="jaccard_timeseries_by_size.pdf",
        out_dir=out_dir,
        heartbeat_ms=args.heartbeat_ms,
    )
    figure_paths = sorted(out_dir.glob("*.pdf"))

    write_score_csv(rows, out_dir)
    write_summary(out_dir, sim_root, real_root, summaries, overall, rows, args, figure_paths)

    print(f"Wrote {len(figure_paths)} figures to {out_dir}")
    print(f"Summary: {out_dir / 'validation_summary.json'}")


if __name__ == "__main__":
    main()
