from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


LOGGER = logging.getLogger("plot_inference_results")

ATTACKS = (
    "passive_ego_mesh",
    "passive_coalition",
    "px_flood",
)

_LINEWIDTH_IN: float = 6.30
_FIG_FONT_SIZE: int = 9

DEFAULT_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "results" / "topology_inference"
DEFAULT_OUTPUT_DIR = DEFAULT_SOURCE_ROOT / "figures"
DEFAULT_FORMATS = ("pdf",)

SCENARIO_LABELS = {
    "degraded_baseline_r20_windowed_px_control_dout": "baseline\ncontrol",
    "degraded_scoring_r20_validated": "scoring\nvalidated",
}

SCENARIO_COLORS = {
    "degraded_baseline_r20_windowed_px_control_dout": "#4D4D4D",
    "degraded_scoring_r20_validated": "#D55E00",
}

PX_SCENARIO_LABELS = {
    "degraded_baseline_r20_windowed_px_control_dout": "baseline\n(no scoring)",
    "degraded_scoring_r20_validated": "scoring\nvalidated",
    "px_flood_defended": "baseline\n(defended)",
    "px_flood_scoring_defended": "scoring\n(defended)",
}
PX_SCENARIO_ORDER = list(PX_SCENARIO_LABELS.keys())

PX_COALITION_COLORS = {4: "#0072B2", 8: "#D55E00", 16: "#009E73"}
PX_COALITION_MARKERS = {4: "o", 8: "s", 16: "^"}


def split_scenario_variant(value: str) -> tuple[str, str | None]:
    marker = "_late_join_hb_"
    if marker not in value:
        return value, None
    base, heartbeat = value.rsplit(marker, 1)
    return base, f"late join\nhb {heartbeat}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot static topology-inference figures from aggregated CSV outputs."
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=DEFAULT_SOURCE_ROOT,
        help="Root directory containing results/topology_inference.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where figures will be written.",
    )
    parser.add_argument(
        "--attacks",
        nargs="+",
        choices=[*ATTACKS],
        default=[*ATTACKS],
        help="Subset of attack views to plot.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail immediately if a required CSV or column is missing.",
    )
    parser.add_argument(
        "--defended",
        action="store_true",
        help=(
            "Plot PX flood figures from the defended results "
            "(source_root/px_flood_defended instead of source_root/px_flood). "
            "Writes figures under output_dir/px_flood_defended/."
        ),
    )
    return parser.parse_args()


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
            # Uniform font sizing: all text rendered at _FIG_FONT_SIZE pt when
            # figures are included at [width=\linewidth] on A4 (width=_LINEWIDTH_IN).
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


def scenario_label(value: str) -> str:
    base, variant = split_scenario_variant(value)
    label = SCENARIO_LABELS.get(base, base.replace("_", "\n"))
    if variant:
        return f"{label}\n{variant}"
    return label


def scenario_color(value: str, fallback_index: int = 0) -> str:
    base, _ = split_scenario_variant(value)
    colors = plt.get_cmap("tab10").colors
    return SCENARIO_COLORS.get(base, colors[fallback_index % len(colors)])


def load_csv(path: Path, strict: bool) -> pd.DataFrame | None:
    if not path.exists():
        message = f"Missing CSV: {path}"
        if strict:
            raise FileNotFoundError(message)
        LOGGER.warning("%s; skipping related figures", message)
        return None
    try:
        return pd.read_csv(path)
    except Exception as exc:
        if strict:
            raise
        LOGGER.warning("Failed to read %s: %s; skipping related figures", path, exc)
        return None


def require_columns(df: pd.DataFrame, columns: list[str], context: str, strict: bool) -> bool:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        message = f"{context} is missing required columns: {', '.join(missing)}"
        if strict:
            raise KeyError(message)
        LOGGER.warning("%s; skipping related figures", message)
        return False
    return True


def normalize_columns(df: pd.DataFrame, aliases: dict[str, list[str]]) -> pd.DataFrame:
    renamed = df.copy()
    for canonical_name, candidates in aliases.items():
        if canonical_name in renamed.columns:
            continue
        for candidate in candidates:
            if candidate in renamed.columns:
                renamed = renamed.rename(columns={candidate: canonical_name})
                break
    return renamed


def save_figure(fig: plt.Figure, base_path: Path, formats: list[str]) -> None:
    base_path.parent.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        fig.savefig(base_path.with_suffix(f".{fmt}"), bbox_inches="tight")


def ordered_scenarios(df: pd.DataFrame) -> list[str]:
    available = [str(value) for value in df["scenario"].dropna().unique()]
    preferred = [
        scenario
        for base_scenario in SCENARIO_LABELS
        for scenario in available
        if split_scenario_variant(scenario)[0] == base_scenario
    ]
    remaining = [scenario for scenario in sorted(available) if scenario not in preferred]
    return [*preferred, *remaining]


def collapse_duplicate_scenarios(df: pd.DataFrame, context: str) -> pd.DataFrame:
    duplicated = df["scenario"].duplicated(keep=False)
    if not duplicated.any():
        return df

    duplicate_names = sorted(str(value) for value in df.loc[duplicated, "scenario"].dropna().unique())
    deduped = df.drop_duplicates()
    if not deduped["scenario"].duplicated().any():
        LOGGER.warning("%s: dropping exact duplicate rows for scenarios: %s", context, ", ".join(duplicate_names))
        return deduped

    LOGGER.warning(
        "%s: collapsing non-identical duplicate rows for scenarios: %s",
        context,
        ", ".join(duplicate_names),
    )
    aggregations = {
        column: ("mean" if pd.api.types.is_numeric_dtype(deduped[column]) else "first")
        for column in deduped.columns
        if column != "scenario"
    }
    return deduped.groupby("scenario", as_index=False).agg(aggregations)


def plot_grouped_bars(
    df: pd.DataFrame,
    metrics: list[tuple[str, str, str]],
    *,
    title: str,
    ylabel: str,
    base_path: Path,
    formats: list[str],
) -> None:
    df = collapse_duplicate_scenarios(df, title)
    scenarios = ordered_scenarios(df)
    x = np.arange(len(scenarios))
    width = 0.18
    offsets = np.linspace(-width * (len(metrics) - 1) / 2, width * (len(metrics) - 1) / 2, len(metrics))
    colors = plt.get_cmap("tab10").colors

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.0))
    sub = df.set_index("scenario").reindex(scenarios)
    for index, (value_col, error_col, label) in enumerate(metrics):
        bars = ax.bar(
            x + offsets[index],
            sub[value_col].astype(float).to_numpy(),
            width=width,
            yerr=sub[error_col].astype(float).to_numpy() if error_col in sub.columns else None,
            color=colors[index % len(colors)],
            edgecolor="black",
            linewidth=0.5,
            label=label,
        )


    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels([scenario_label(scenario) for scenario in scenarios])
    ax.set_ylim(bottom=0)
    ax.legend(ncol=2, loc="upper center", bbox_to_anchor=(0.5, 1.18))
    fig.tight_layout()
    save_figure(fig, base_path, formats)
    plt.close(fig)


def plot_lines_vs_k(
    df: pd.DataFrame,
    metrics: list[tuple[str, str, str]],
    *,
    title: str,
    base_path: Path,
    formats: list[str],
) -> None:
    scenarios = ordered_scenarios(df)
    colors = plt.get_cmap("tab10").colors
    single_metric = len(metrics) == 1
    fig, axes = plt.subplots(1, len(metrics), figsize=(_LINEWIDTH_IN, 4.0), sharex=True)
    if len(metrics) == 1:
        axes = [axes]

    for axis, (value_col, error_col, metric_title) in zip(axes, metrics):
        for index, scenario in enumerate(scenarios):
            subset = df[df["scenario"] == scenario].sort_values("coalition_size")
            axis.errorbar(
                subset["coalition_size"],
                subset[value_col],
                yerr=subset[error_col] if error_col in subset.columns else None,
                marker="o",
                color=colors[index % len(colors)],
                label=scenario_label(scenario),
                markersize=4,
                linewidth=1.8,
            )
        if single_metric:
            axis.set_ylabel(metric_title)
        else:
            axis.set_title(metric_title)
        axis.set_xlabel("Coalition size k")
        axis.set_ylim(bottom=0)
        axis.grid(axis="y")

    if not single_metric:
        axes[0].set_ylabel("Score")
    axes[0].legend(loc="best")
    fig.tight_layout()
    save_figure(fig, base_path, formats)
    plt.close(fig)


def plot_topic_quantiles_vs_k(
    df: pd.DataFrame,
    *,
    median_column: str,
    q25_column: str,
    q75_column: str,
    ylabel: str,
    base_path: Path,
    formats: list[str],
    zoom_upper_limit: bool = False,
    y_limit_column: str | None = None,
    min_zoom_upper: float = 0.08,
) -> None:
    scenarios = ordered_scenarios(df)
    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.0))
    quantile_values: list[np.ndarray] = []
    for index, scenario in enumerate(scenarios):
        subset = df[df["scenario"] == scenario].sort_values("coalition_size")
        x_values = subset["coalition_size"].astype(float).to_numpy()
        medians = subset[median_column].astype(float).to_numpy()
        lower = subset[q25_column].astype(float).to_numpy()
        upper = subset[q75_column].astype(float).to_numpy()
        quantile_values.extend((medians, lower, upper))
        color = scenario_color(scenario, index)
        ax.plot(
            x_values,
            medians,
            marker="o",
            color=color,
            label=scenario_label(scenario),
            markersize=4,
        )
        ax.fill_between(x_values, lower, upper, color=color, alpha=0.16, linewidth=0)

    ax.set_xlabel("Coalition size k")
    ax.set_ylabel(ylabel)
    if zoom_upper_limit:
        y_limit_values = (
            [df[y_limit_column].astype(float).to_numpy()]
            if y_limit_column and y_limit_column in df.columns
            else quantile_values
        )
        finite_values = np.concatenate(y_limit_values) if y_limit_values else np.array([])
        finite_values = finite_values[np.isfinite(finite_values)]
        if finite_values.size:
            max_value = max(0.0, float(np.max(finite_values)))
            padding = max(0.02, max_value * 0.12)
            ax.set_ylim(0, min(1.04, max(min_zoom_upper, max_value + padding)))
        else:
            ax.set_ylim(-0.02, 1.04)
    else:
        ax.set_ylim(-0.02, 1.04)
    ax.grid(axis="y")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.22), ncol=2)
    fig.tight_layout()
    save_figure(fig, base_path, formats)
    plt.close(fig)


def plot_passive_ego_mesh(source_root: Path, output_dir: Path, formats: list[str], strict: bool) -> None:
    csv_path = source_root / "passive_ego_mesh" / "summary" / "passive_ego_mesh_scenario_summary.csv"
    df = load_csv(csv_path, strict)
    if df is None:
        plot_passive_ego_mesh_recall_plateau(source_root, output_dir, formats, strict)
        return

    df = normalize_columns(
        df,
        {
            "neighbor_precision_mean": [
                "undirected_precision_micro_mean",
                "combined_precision_micro_mean",
            ],
            "neighbor_precision_std": [
                "undirected_precision_micro_std",
                "combined_precision_micro_std",
            ],
            "neighbor_recall_mean": [
                "undirected_recall_micro_mean",
                "combined_recall_micro_mean",
            ],
            "neighbor_recall_std": [
                "undirected_recall_micro_std",
                "combined_recall_micro_std",
            ],
            "neighbor_f1_mean": [
                "undirected_f1_micro_mean",
                "combined_f1_micro_mean",
            ],
            "neighbor_f1_std": [
                "undirected_f1_micro_std",
                "combined_f1_micro_std",
            ],
            "jaccard_mean": ["undirected_neighbor_jaccard_macro_mean"],
            "jaccard_std": ["undirected_neighbor_jaccard_macro_std"],
        },
    )
    if not require_columns(
        df,
        [
            "scenario",
            "neighbor_precision_mean",
            "neighbor_precision_std",
            "neighbor_recall_mean",
            "neighbor_recall_std",
            "neighbor_f1_mean",
            "neighbor_f1_std",
            "jaccard_mean",
            "jaccard_std",
        ],
        "passive_ego_mesh summary",
        strict,
    ):
        return

    plot_grouped_bars(
        df,
        [
            ("neighbor_precision_mean", "neighbor_precision_std", "Neighbor precision"),
            ("neighbor_recall_mean", "neighbor_recall_std", "Neighbor recall"),
            ("neighbor_f1_mean", "neighbor_f1_std", "Neighbor F1"),
            ("jaccard_mean", "jaccard_std", "Jaccard"),
        ],
        title="Passive ego mesh non-directed neighbor reconstruction by scenario",
        ylabel="Score",
        base_path=output_dir / "passive_ego_mesh" / "passive_ego_mesh_metrics_by_scenario",
        formats=formats,
    )
    plot_passive_ego_mesh_recall_plateau(source_root, output_dir, formats, strict)


def complete_run_recall_timeseries(run_df: pd.DataFrame) -> pd.DataFrame:
    run_df = run_df.copy()
    run_df["heartbeat_index"] = pd.to_numeric(run_df["heartbeat_index"], errors="coerce")
    run_df["recall"] = pd.to_numeric(run_df["recall"], errors="coerce")
    run_df = run_df.dropna(subset=["heartbeat_index", "recall"])
    if run_df.empty:
        return pd.DataFrame()

    run_df["heartbeat_index"] = run_df["heartbeat_index"].astype(int)
    run_df = run_df.sort_values("heartbeat_index").drop_duplicates("heartbeat_index", keep="last")
    max_heartbeat = int(run_df["heartbeat_index"].max())
    full_index = pd.RangeIndex(0, max_heartbeat + 1, name="heartbeat_index")
    completed = run_df.set_index("heartbeat_index").reindex(full_index).ffill().reset_index()
    completed["recall"] = completed["recall"].fillna(0.0)
    for column in ("scenario", "seed", "attacker_index", "coalition_size", "run_dir"):
        if column in completed.columns:
            completed[column] = completed[column].ffill().bfill()
    return completed


def plot_passive_ego_mesh_recall_plateau(
    source_root: Path, output_dir: Path, formats: list[str], strict: bool
) -> None:
    csv_path = (
        source_root
        / "passive_ego_mesh"
        / "summary"
        / "passive_ego_mesh_recall_timeseries.csv"
    )
    df = load_csv(csv_path, strict)
    if df is None:
        return

    required_columns = ["scenario", "seed", "heartbeat_index", "recall", "run_dir"]
    if not require_columns(df, required_columns, "passive_ego_mesh recall timeseries", strict):
        return

    completed_runs: list[pd.DataFrame] = []
    for _, run_df in df.groupby("run_dir", sort=False):
        completed = complete_run_recall_timeseries(run_df)
        if not completed.empty:
            completed_runs.append(completed)
    if not completed_runs:
        LOGGER.warning("No completed passive_ego_mesh recall timeseries data; skipping plateau figure")
        return

    completed_df = pd.concat(completed_runs, ignore_index=True)
    completed_df["recall"] = pd.to_numeric(completed_df["recall"], errors="coerce")
    completed_df = completed_df.dropna(subset=["recall"])
    if completed_df.empty:
        LOGGER.warning("No numeric passive_ego_mesh recall data; skipping plateau figure")
        return

    summary = (
        completed_df.groupby(["scenario", "heartbeat_index"], as_index=False)
        .agg(recall_mean=("recall", "mean"), recall_std=("recall", "std"), runs=("recall", "count"))
        .fillna({"recall_std": 0.0})
    )

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.5), constrained_layout=True)
    scenarios = ordered_scenarios(summary)
    for index, scenario in enumerate(scenarios):
        subset = summary[summary["scenario"] == scenario].sort_values("heartbeat_index")
        if subset.empty:
            continue
        x = subset["heartbeat_index"].astype(float).to_numpy()
        mean = subset["recall_mean"].astype(float).to_numpy()
        std = subset["recall_std"].astype(float).to_numpy()
        color = scenario_color(scenario, index)
        ax.plot(x, mean, color=color, label=scenario_label(scenario))
        ax.fill_between(
            x,
            np.clip(mean - std, 0.0, 1.0),
            np.clip(mean + std, 0.0, 1.0),
            color=color,
            alpha=0.16,
            linewidth=0,
        )

    for threshold in (0.50, 0.90, 0.95, 1.00):
        ax.axhline(threshold, color="0.35", linewidth=0.75, linestyle="--", alpha=0.55)
        ax.text(
            1.002,
            threshold,
            f"{int(threshold * 100)}%",
            transform=ax.get_yaxis_transform(),
            va="center",
            ha="left",
            color="0.25",
        )

    ax.set_xlabel("Heartbeat index")
    ax.set_ylabel("Recall of final ego-mesh neighbors")
    ax.set_ylim(-0.02, 1.04)
    ax.set_xlim(left=0)
    ax.legend(loc="lower right")
    ax.grid(axis="y")
    save_figure(
        fig,
        output_dir / "passive_ego_mesh" / "passive_ego_mesh_recall_plateau_by_scenario",
        formats,
    )
    plt.close(fig)


def plot_passive_coalition(source_root: Path, output_dir: Path, formats: list[str], strict: bool) -> None:
    csv_path = source_root / "passive_coalition" / "summary" / "passive_coalition_scenario_summary.csv"
    df = load_csv(csv_path, strict)
    if df is None:
        return

    df = normalize_columns(
        df,
        {
            "frontier_undirected_f1_mean": [
                "frontier_f1_mean",
                "frontier_directed_f1_mean",
            ],
            "frontier_undirected_f1_std": [
                "frontier_f1_std",
                "frontier_directed_f1_std",
            ],
            "frontier_undirected_recall_mean": [
                "frontier_recall_mean",
                "frontier_directed_recall_mean",
            ],
            "frontier_undirected_recall_std": [
                "frontier_recall_std",
                "frontier_directed_recall_std",
            ],
        },
    )
    if not require_columns(
        df,
        [
            "scenario",
            "coalition_size",
            "frontier_undirected_f1_mean",
            "frontier_undirected_recall_mean",
        ],
        "passive_coalition summary",
        strict,
    ):
        return

    plot_lines_vs_k(
        df,
        [("frontier_undirected_f1_mean", "frontier_undirected_f1_std", "Non-directed Frontier F1")],
        title="",
        base_path=output_dir / "passive_coalition" / "passive_coalition_frontier_undirected_f1_vs_k",
        formats=formats,
    )

    plot_passive_coalition_frontier_recall_plateau(source_root, output_dir, formats, strict)

    topic_csv_path = (
        source_root / "passive_coalition" / "summary" / "passive_coalition_topic_summary.csv"
    )
    topic_df = load_csv(topic_csv_path, strict)
    if topic_df is None:
        return
    if not require_columns(
        topic_df,
        [
            "scenario",
            "coalition_size",
            "topic_undirected_edge_coverage_q25",
            "topic_undirected_edge_coverage_median",
            "topic_undirected_edge_coverage_q75",
            "topic_undirected_node_coverage_q25",
            "topic_undirected_node_coverage_median",
            "topic_undirected_node_coverage_q75",
        ],
        "passive_coalition per-topic summary",
        strict,
    ):
        return

    plot_topic_quantiles_vs_k(
        topic_df,
        median_column="topic_undirected_edge_coverage_median",
        q25_column="topic_undirected_edge_coverage_q25",
        q75_column="topic_undirected_edge_coverage_q75",
        ylabel="Per-topic non-directed edge coverage\n(median and IQR)",
        base_path=output_dir / "passive_coalition" / "passive_coalition_topic_undirected_edge_coverage_vs_k",
        formats=formats,
        zoom_upper_limit=True,
        y_limit_column="topic_undirected_edge_coverage_median",
        min_zoom_upper=0.025,
    )
    plot_topic_quantiles_vs_k(
        topic_df,
        median_column="topic_undirected_node_coverage_median",
        q25_column="topic_undirected_node_coverage_q25",
        q75_column="topic_undirected_node_coverage_q75",
        ylabel="Per-topic non-directed node coverage\n(median and IQR)",
        base_path=output_dir / "passive_coalition" / "passive_coalition_topic_undirected_node_coverage_vs_k",
        formats=formats,
        zoom_upper_limit=True,
    )


def plot_passive_coalition_frontier_recall_plateau(
    source_root: Path, output_dir: Path, formats: list[str], strict: bool
) -> None:
    csv_path = (
        source_root
        / "passive_coalition"
        / "summary"
        / "passive_coalition_frontier_recall_timeseries.csv"
    )
    df = load_csv(csv_path, strict)
    if df is None:
        return

    required_columns = [
        "scenario",
        "seed",
        "coalition_size",
        "heartbeat_index",
        "recall",
        "run_dir",
    ]
    if not require_columns(df, required_columns, "passive_coalition recall timeseries", strict):
        return

    completed_runs: list[pd.DataFrame] = []
    for _, run_df in df.groupby("run_dir", sort=False):
        completed = complete_run_recall_timeseries(run_df)
        if not completed.empty:
            completed_runs.append(completed)
    if not completed_runs:
        LOGGER.warning("No completed passive_coalition recall timeseries data; skipping plateau figure")
        return

    completed_df = pd.concat(completed_runs, ignore_index=True)
    completed_df["coalition_size"] = pd.to_numeric(
        completed_df["coalition_size"], errors="coerce"
    )
    completed_df["recall"] = pd.to_numeric(completed_df["recall"], errors="coerce")
    completed_df = completed_df.dropna(subset=["coalition_size", "recall"])
    if completed_df.empty:
        LOGGER.warning("No numeric passive_coalition recall data; skipping plateau figure")
        return

    completed_df["coalition_size"] = completed_df["coalition_size"].astype(int)
    summary = (
        completed_df.groupby(["coalition_size", "scenario", "heartbeat_index"], as_index=False)
        .agg(recall_mean=("recall", "mean"), recall_std=("recall", "std"), runs=("recall", "count"))
        .fillna({"recall_std": 0.0})
    )
    coalition_sizes = sorted(summary["coalition_size"].dropna().astype(int).unique())
    if not coalition_sizes:
        LOGGER.warning("No passive_coalition coalition sizes in recall data; skipping plateau figure")
        return

    columns = min(2, len(coalition_sizes))
    rows = int(np.ceil(len(coalition_sizes) / columns))
    fig, axes = plt.subplots(
        rows,
        columns,
        figsize=(_LINEWIDTH_IN, 3.5 * rows),
        sharex=True,
        sharey=True,
        squeeze=False,
    )
    flat_axes = axes.ravel()
    scenarios = ordered_scenarios(summary)
    legend_handles: list[plt.Line2D] = []
    legend_labels: list[str] = []

    for panel_index, coalition_size in enumerate(coalition_sizes):
        ax = flat_axes[panel_index]
        for scenario_index, scenario in enumerate(scenarios):
            subset = summary[
                (summary["coalition_size"] == coalition_size) & (summary["scenario"] == scenario)
            ].sort_values("heartbeat_index")
            if subset.empty:
                continue
            x = subset["heartbeat_index"].astype(float).to_numpy()
            mean = subset["recall_mean"].astype(float).to_numpy()
            std = subset["recall_std"].astype(float).to_numpy()
            color = scenario_color(scenario, scenario_index)
            line = ax.plot(x, mean, color=color, label=scenario_label(scenario))[0]
            ax.fill_between(
                x,
                np.clip(mean - std, 0.0, 1.0),
                np.clip(mean + std, 0.0, 1.0),
                color=color,
                alpha=0.16,
                linewidth=0,
            )
            label = scenario_label(scenario)
            if label not in legend_labels:
                legend_handles.append(line)
                legend_labels.append(label)

        for threshold in (0.50, 0.90, 0.95, 1.00):
            ax.axhline(threshold, color="0.35", linewidth=0.75, linestyle="--", alpha=0.55)
        ax.text(
            0.03,
            0.93,
            f"k = {coalition_size}",
            transform=ax.transAxes,
            va="top",
            ha="left",
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.78, "pad": 2.0},
        )
        ax.set_xlim(left=0)
        ax.set_ylim(-0.02, 1.04)
        ax.grid(axis="y")

    for ax in flat_axes[len(coalition_sizes):]:
        ax.set_visible(False)

    for ax in axes[-1, :]:
        if ax.get_visible():
            ax.set_xlabel("Heartbeat index")
    for ax in axes[:, 0]:
        if ax.get_visible():
            ax.set_ylabel("Recall of final coalition frontier")

    visible_axes = [ax for ax in flat_axes[: len(coalition_sizes)] if ax.get_visible()]
    if visible_axes:
        right_ax = visible_axes[min(columns - 1, len(visible_axes) - 1)]
        for threshold in (0.50, 0.90, 0.95, 1.00):
            right_ax.text(
                1.002,
                threshold,
                f"{int(threshold * 100)}%",
                transform=right_ax.get_yaxis_transform(),
                va="center",
                ha="left",
                color="0.25",
            )
    if legend_handles:
        fig.legend(
            legend_handles,
            legend_labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 0.995),
            ncol=min(4, len(legend_labels)),
        )
    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.92), h_pad=2.0, w_pad=1.6)

    save_figure(
        fig,
        output_dir / "passive_coalition" / "passive_coalition_frontier_recall_plateau_by_k_and_scenario",
        formats,
    )
    plt.close(fig)


def _px_load_rows(results_root: Path) -> list[dict] | None:
    if not results_root.exists():
        LOGGER.warning("PX flood results directory not found: %s", results_root)
        return None
    rows: list[dict] = []
    for scenario_dir in sorted(results_root.iterdir()):
        if not scenario_dir.is_dir() or scenario_dir.name == "summary":
            continue
        _dm = re.search(r'_depth(\d+)', scenario_dir.name)
        wave_depth = int(_dm.group(1)) if _dm else 1
        scenario_name = re.sub(r'_depth\d+', '', scenario_dir.name)
        for run_dir in sorted(scenario_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            parts = run_dir.name.split("_")
            try:
                seed = int(parts[1])
                attacker_indices = [int(x) for x in parts[5].split("-")]
                coalition_size = len(attacker_indices)
            except Exception:
                continue
            eval_path = run_dir / "evaluation.json"
            if not eval_path.exists():
                continue
            try:
                ev = json.loads(eval_path.read_text())
            except Exception:
                continue
            agg = ev.get("per_target_neighborhood", {}).get("aggregate", {})
            disc = ev.get("discovery_coverage", {})
            bias = ev.get("degree_bias", {})
            rows.append({
                "scenario": scenario_name,
                "seed": seed,
                "coalition_size": coalition_size,
                "wave_depth": wave_depth,
                "run_dir": str(run_dir),
                "summary": {
                    "discovery_coverage": disc.get("discovery_coverage") or 0.0,
                    "unique_nodes_discovered": disc.get("unique_nodes_discovered") or 0,
                    "discovery_precision": disc.get("discovery_precision") or 0.0,
                    "micro_precision": agg.get("micro_precision") or 0.0,
                    "micro_recall": agg.get("micro_recall") or 0.0,
                    "micro_f1": agg.get("micro_f1") or 0.0,
                    "high_degree_recall": bias.get("high_degree_recall"),
                    "low_degree_recall": bias.get("low_degree_recall"),
                    "discovery_lift": bias.get("discovery_lift"),
                },
            })
    return rows if rows else None


def _px_load_wave_report(run_dir: Path) -> list[dict]:
    path = run_dir / "wave_report.json"
    if not path.exists():
        return []
    with path.open() as f:
        d = json.load(f)
    return d.get("waves", [])


def _px_scenario_label(name: str) -> str:
    return PX_SCENARIO_LABELS.get(name, name)


def _px_group_rows(
    rows: list[dict],
    coalition_sizes: list[int] | None = None,
    scenarios: list[str] | None = None,
    wave_depth: int = 1,
) -> dict[tuple[str, int], list[dict]]:
    groups: dict[tuple[str, int], list[dict]] = {}
    for row in rows:
        if row.get("summary") is None:
            continue
        if coalition_sizes and row["coalition_size"] not in coalition_sizes:
            continue
        if scenarios and row["scenario"] not in scenarios:
            continue
        if row.get("wave_depth", 1) != wave_depth:
            continue
        key = (row["scenario"], row["coalition_size"])
        groups.setdefault(key, []).append(row)
    return groups


def _px_metric_stats(rows: list[dict], key: str) -> tuple[float, float, float]:
    """Return (median, q25, q75) across seeds."""
    values = []
    for row in rows:
        s = row.get("summary") or {}
        v = s.get(key)
        if v is not None:
            values.append(float(v))
    if not values:
        return 0.0, 0.0, 0.0
    arr = np.array(values)
    return float(np.median(arr)), float(np.percentile(arr, 25)), float(np.percentile(arr, 75))


def _px_save(fig: plt.Figure, output_dir: Path, name: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"px_flood_{name}.pdf"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    LOGGER.info("Saved %s", path)
    plt.close(fig)


def _px_depth_suffix(wave_depth: int) -> str:
    return f"_depth{wave_depth}" if wave_depth > 1 else ""


def _px_fig1a_coverage_bar(rows: list[dict], output_dir: Path, wave_depth: int = 1) -> None:
    """Discovery coverage fraction by scenario × coalition size."""
    coalition_sizes = sorted({r["coalition_size"] for r in rows if r.get("summary") and r.get("wave_depth", 1) == wave_depth})
    scenarios = [s for s in PX_SCENARIO_ORDER if s in {r["scenario"] for r in rows}]
    groups = _px_group_rows(rows, coalition_sizes=coalition_sizes, scenarios=scenarios, wave_depth=wave_depth)

    n_scenarios = len(scenarios)
    n_cs = len(coalition_sizes)
    x = np.arange(n_scenarios)
    width = 0.75 / max(n_cs, 1)
    offsets = (np.arange(n_cs) - (n_cs - 1) / 2) * width

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.0))
    for ci, cs in enumerate(coalition_sizes):
        medians, yerr_low, yerr_high = [], [], []
        for scenario in scenarios:
            med, q25, q75 = _px_metric_stats(groups.get((scenario, cs), []), "discovery_coverage")
            medians.append(med)
            yerr_low.append(med - q25)
            yerr_high.append(q75 - med)
        ax.bar(x + offsets[ci], medians, width, yerr=[yerr_low, yerr_high],
               color=PX_COALITION_COLORS.get(cs, "#888"), alpha=0.85,
               label=f"coalition {cs}", capsize=4, error_kw={"linewidth": 1.2})

    ax.set_xticks(x)
    ax.set_xticklabels([_px_scenario_label(s) for s in scenarios])
    ax.set_ylabel("Fraction of mesh discovered")
    ax.set_ylim(0, 1.0)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.5, alpha=0.4)
    handles = [mpatches.Patch(color=PX_COALITION_COLORS.get(cs, "#888"), label=f"coalition {cs}") for cs in coalition_sizes]
    ax.legend(handles=handles, framealpha=0.9)
    fig.tight_layout()
    _px_save(fig, output_dir, f"coverage_bar{_px_depth_suffix(wave_depth)}")


def _px_fig2_wave_progression(rows: list[dict], output_dir: Path, wave_depth: int = 1) -> None:
    """Cumulative discovered nodes per wave, one panel per scenario (2×2 grid)."""
    coalition_sizes = sorted({r["coalition_size"] for r in rows if r.get("summary") and r.get("wave_depth", 1) == wave_depth})
    scenarios = [s for s in PX_SCENARIO_ORDER if s in {r["scenario"] for r in rows}]

    n_rows = 2
    n_cols = (len(scenarios) + 1) // 2
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(_LINEWIDTH_IN, 3.5 * n_rows), sharex=False, sharey=False)
    ax_flat = np.array(axes).flatten()

    for si, scenario in enumerate(scenarios):
        ax = ax_flat[si]
        has_data = False
        for cs in coalition_sizes:
            scenario_rows = [
                r for r in rows
                if r["scenario"] == scenario and r["coalition_size"] == cs
                and r.get("summary") and r.get("wave_depth", 1) == wave_depth
            ]
            if not scenario_rows:
                continue
            all_curves: list[list[int]] = []
            for row in scenario_rows:
                run_dir = row.get("run_dir")
                if not run_dir:
                    continue
                waves = _px_load_wave_report(Path(run_dir))
                if not waves:
                    continue
                seen: set[str] = set()
                curve = []
                for wave in waves:
                    seen.update(wave.get("px_peers_received_union", []))
                    curve.append(len(seen))
                if curve:
                    all_curves.append(curve)
            if not all_curves:
                continue
            has_data = True
            max_len = max(len(c) for c in all_curves)
            padded = np.zeros((len(all_curves), max_len))
            for i, c in enumerate(all_curves):
                padded[i, :len(c)] = c
                padded[i, len(c):] = c[-1]
            median_curve = np.median(padded, axis=0)
            q25_curve = np.percentile(padded, 25, axis=0)
            q75_curve = np.percentile(padded, 75, axis=0)
            x = np.arange(1, max_len + 1)
            color = PX_COALITION_COLORS.get(cs, "#888")
            ax.plot(x, median_curve, color=color, linewidth=2, label=f"cs={cs}",
                    marker=PX_COALITION_MARKERS.get(cs, "o"), markersize=4)
            ax.fill_between(x, q25_curve, q75_curve, color=color, alpha=0.18)

        ax.set_xlabel("Wave number")
        ax.set_ylabel("Cumulative PX peers discovered")
        ax.text(0.02, 0.97, _px_scenario_label(scenario), transform=ax.transAxes,
                va="top", ha="left")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(linewidth=0.4, alpha=0.4)
        if has_data:
            ax.legend(framealpha=0.8)

    for si in range(len(scenarios), len(ax_flat)):
        ax_flat[si].set_visible(False)

    fig.tight_layout()
    _px_save(fig, output_dir, f"wave_progression{_px_depth_suffix(wave_depth)}")


def _px_fig3a_precision_bar(rows: list[dict], output_dir: Path, wave_depth: int = 1) -> None:
    """Per-target neighbourhood micro-precision by scenario × coalition size."""
    coalition_sizes = sorted({r["coalition_size"] for r in rows if r.get("summary") and r.get("wave_depth", 1) == wave_depth})
    scenarios = [s for s in PX_SCENARIO_ORDER if s in {r["scenario"] for r in rows}]
    groups = _px_group_rows(rows, coalition_sizes=coalition_sizes, scenarios=scenarios, wave_depth=wave_depth)

    n_scenarios = len(scenarios)
    n_cs = len(coalition_sizes)
    x = np.arange(n_scenarios)
    width = 0.35 / max(n_cs, 1)
    offsets = (np.arange(n_cs) - (n_cs - 1) / 2) * width

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.0))
    for ci, cs in enumerate(coalition_sizes):
        medians, yerr_low, yerr_high = [], [], []
        for scenario in scenarios:
            med, q25, q75 = _px_metric_stats(groups.get((scenario, cs), []), "micro_precision")
            medians.append(med)
            yerr_low.append(med - q25)
            yerr_high.append(q75 - med)
        ax.bar(x + offsets[ci], medians, width, yerr=[yerr_low, yerr_high],
               color=PX_COALITION_COLORS.get(cs, "#888"), alpha=0.85,
               label=f"cs={cs}", capsize=4, error_kw={"linewidth": 1.2})

    ax.set_xticks(x)
    ax.set_xticklabels([_px_scenario_label(s) for s in scenarios])
    ax.set_ylabel("Micro-precision")
    ax.set_ylim(0, 1.0)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.5, alpha=0.4)
    handles = [mpatches.Patch(color=PX_COALITION_COLORS.get(cs, "#888"), label=f"coalition {cs}") for cs in coalition_sizes]
    ax.legend(handles=handles, framealpha=0.9)
    fig.tight_layout()
    _px_save(fig, output_dir, f"precision_bar{_px_depth_suffix(wave_depth)}")


def _px_fig3b_recall_bar(rows: list[dict], output_dir: Path, wave_depth: int = 1) -> None:
    """Per-target neighbourhood micro-recall by scenario × coalition size."""
    coalition_sizes = sorted({r["coalition_size"] for r in rows if r.get("summary") and r.get("wave_depth", 1) == wave_depth})
    scenarios = [s for s in PX_SCENARIO_ORDER if s in {r["scenario"] for r in rows}]
    groups = _px_group_rows(rows, coalition_sizes=coalition_sizes, scenarios=scenarios, wave_depth=wave_depth)

    n_scenarios = len(scenarios)
    n_cs = len(coalition_sizes)
    x = np.arange(n_scenarios)
    width = 0.35 / max(n_cs, 1)
    offsets = (np.arange(n_cs) - (n_cs - 1) / 2) * width

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.0))
    for ci, cs in enumerate(coalition_sizes):
        medians, yerr_low, yerr_high = [], [], []
        for scenario in scenarios:
            med, q25, q75 = _px_metric_stats(groups.get((scenario, cs), []), "micro_recall")
            medians.append(med)
            yerr_low.append(med - q25)
            yerr_high.append(q75 - med)
        ax.bar(x + offsets[ci], medians, width, yerr=[yerr_low, yerr_high],
               color=PX_COALITION_COLORS.get(cs, "#888"), alpha=0.85,
               label=f"cs={cs}", capsize=4, error_kw={"linewidth": 1.2})

    ax.set_xticks(x)
    ax.set_xticklabels([_px_scenario_label(s) for s in scenarios])
    ax.set_ylabel("Micro-recall")
    ax.set_ylim(0, 1.0)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.5, alpha=0.4)
    handles = [mpatches.Patch(color=PX_COALITION_COLORS.get(cs, "#888"), label=f"coalition {cs}") for cs in coalition_sizes]
    ax.legend(handles=handles, framealpha=0.9)
    fig.tight_layout()
    _px_save(fig, output_dir, f"recall_bar{_px_depth_suffix(wave_depth)}")


def _px_fig5a_drift_bar(rows: list[dict], output_dir: Path, wave_depth: int = 1) -> None:
    """Final topology drift (Jaccard) by scenario × coalition size."""
    coalition_sizes = sorted({r["coalition_size"] for r in rows if r.get("summary") and r.get("wave_depth", 1) == wave_depth})
    scenarios = [s for s in PX_SCENARIO_ORDER if s in {r["scenario"] for r in rows}]

    drift_groups: dict[tuple[str, int], list[float]] = {}
    for row in rows:
        if row.get("summary") is None or not row.get("run_dir"):
            continue
        if row.get("wave_depth", 1) != wave_depth:
            continue
        eval_path = Path(row["run_dir"]) / "evaluation.json"
        if not eval_path.exists():
            continue
        try:
            ev = json.loads(eval_path.read_text())
        except Exception:
            continue
        pert = ev.get("topology_perturbation", {})
        if pert.get("status") != "calculated":
            continue
        drift = pert.get("final_topology_drift")
        if drift is not None:
            drift_groups.setdefault((row["scenario"], row["coalition_size"]), []).append(float(drift))

    cs_with_data = sorted({cs for (_, cs), vals in drift_groups.items() if vals})
    if not cs_with_data:
        LOGGER.warning("No drift data for depth=%d; skipping fig5a", wave_depth)
        return

    n_scenarios = len(scenarios)
    n_cs = len(cs_with_data)
    x = np.arange(n_scenarios)
    width = 0.75 / max(n_cs, 1)
    offsets = (np.arange(n_cs) - (n_cs - 1) / 2) * width

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.0))
    for ci, cs in enumerate(cs_with_data):
        medians, yerr_low, yerr_high = [], [], []
        for scenario in scenarios:
            vals = drift_groups.get((scenario, cs), [])
            if vals:
                med = float(np.median(vals))
                q25 = float(np.percentile(vals, 25))
                q75 = float(np.percentile(vals, 75))
            else:
                med, q25, q75 = 0.0, 0.0, 0.0
            medians.append(med)
            yerr_low.append(med - q25)
            yerr_high.append(q75 - med)
        ax.bar(x + offsets[ci], medians, width, yerr=[yerr_low, yerr_high],
               color=PX_COALITION_COLORS.get(cs, "#888"), alpha=0.85,
               label=f"coalition {cs}", capsize=4, error_kw={"linewidth": 1.2})

    ax.set_xticks(x)
    ax.set_xticklabels([_px_scenario_label(s) for s in scenarios])
    ax.set_ylabel("Final topology drift (Jaccard distance)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.5, alpha=0.4)
    handles = [mpatches.Patch(color=PX_COALITION_COLORS.get(cs, "#888"), label=f"coalition {cs}") for cs in cs_with_data]
    ax.legend(handles=handles, framealpha=0.9)
    fig.tight_layout()
    _px_save(fig, output_dir, f"drift_bar{_px_depth_suffix(wave_depth)}")


_DEGREE_BIAS_CS: dict[str, int] = {
    "degraded_baseline_r20_windowed_px_control_dout": 8,
    "degraded_scoring_r20_validated": 16,
    "px_flood_defended": 8,
    "px_flood_scoring_defended": 16,
}


def _px_fig6_degree_bias_depth2(rows: list[dict], output_dir: Path) -> None:
    """
    Compare discovery recall for high-degree vs low-degree nodes (depth 2).

    For each scenario, three bars are shown:
      - Overall: fraction of all honest mesh nodes discovered (=discovery_coverage).
      - Top 20%:  fraction of the 20% most-connected honest nodes discovered.
      - Bottom 80%: fraction of the remaining 80% discovered.

    A value of Top 20% > Overall confirms that the PX flood attack is biased
    toward high-degree (hub) nodes.
    Uses a per-scenario coalition size: baseline → cs=8, validated → cs=16.
    The coalition size is shown on the x-axis label below the scenario name.
    """
    wave_depth = 2
    scenarios = [s for s in PX_SCENARIO_ORDER if s in {r["scenario"] for r in rows}]

    # Determine per-scenario coalition size
    available_cs_by_scenario = {
        s: sorted({
            r["coalition_size"] for r in rows
            if r["scenario"] == s
            and r.get("wave_depth", 1) == wave_depth
            and r.get("summary")
        })
        for s in scenarios
    }

    scenario_cs: dict[str, int] = {}
    for s in scenarios:
        preferred = _DEGREE_BIAS_CS.get(s)
        available = available_cs_by_scenario.get(s, [])
        if preferred and preferred in available:
            scenario_cs[s] = preferred
        elif available:
            scenario_cs[s] = max(available)
        else:
            LOGGER.warning("No data for degree bias figure (depth=%d, scenario=%s); skipping", wave_depth, s)

    scenarios = [s for s in scenarios if s in scenario_cs]
    if not scenarios:
        LOGGER.warning("No degree_bias data found (depth=%d). "
                       "Re-run evaluate_px_flood.py to populate evaluation.json.", wave_depth)
        return

    # Check that at least some rows have degree_bias data
    candidate_rows = [
        r for r in rows
        if r.get("wave_depth", 1) == wave_depth
        and r["scenario"] in scenario_cs
        and r["coalition_size"] == scenario_cs.get(r["scenario"])
        and r.get("summary", {}).get("high_degree_recall") is not None
    ]
    if not candidate_rows:
        LOGGER.warning(
            "No degree_bias data found (depth=%d). "
            "Re-run evaluate_px_flood.py to populate evaluation.json.",
            wave_depth,
        )
        return

    COLOR_OVERALL = "#999999"
    COLOR_HIGH = "#D55E00"
    COLOR_LOW = "#0072B2"

    n_scenarios = len(scenarios)
    x = np.arange(n_scenarios)
    bar_w = 0.22
    offsets = np.array([-bar_w, 0.0, bar_w])

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 4.0))

    for si, scenario in enumerate(scenarios):
        cs = scenario_cs[scenario]
        scen_rows = [
            r for r in rows
            if r["scenario"] == scenario
            and r["coalition_size"] == cs
            and r.get("wave_depth", 1) == wave_depth
            and r.get("summary")
        ]

        def _stat(key: str, _scen_rows=scen_rows) -> tuple[float, float, float]:
            vals = [r["summary"].get(key) for r in _scen_rows if r["summary"].get(key) is not None]
            if not vals:
                return 0.0, 0.0, 0.0
            arr = np.array(vals, dtype=float)
            med = float(np.median(arr))
            return med, float(np.percentile(arr, 25)), float(np.percentile(arr, 75))

        ov_m, ov_q25, ov_q75 = _stat("discovery_coverage")
        hi_m, hi_q25, hi_q75 = _stat("high_degree_recall")
        lo_m, lo_q25, lo_q75 = _stat("low_degree_recall")

        for j, (m, q25, q75, color) in enumerate([
            (ov_m, ov_q25, ov_q75, COLOR_OVERALL),
            (hi_m, hi_q25, hi_q75, COLOR_HIGH),
            (lo_m, lo_q25, lo_q75, COLOR_LOW),
        ]):
            ax.bar(x[si] + offsets[j], m, bar_w,
                   yerr=[[m - q25], [q75 - m]], color=color, alpha=0.85,
                   capsize=4, error_kw={"linewidth": 1.2})

    ax.set_xticks(x)
    ax.set_xticklabels([
        f"{_px_scenario_label(s)}\n(cs = {scenario_cs[s]})"
        for s in scenarios
    ])
    ax.set_ylabel("Fraction of nodes discovered")
    ax.set_ylim(0, 1.05)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.5, alpha=0.4)

    legend_handles = [
        mpatches.Patch(color=COLOR_OVERALL, label="All nodes (overall)"),
        mpatches.Patch(color=COLOR_HIGH, label="Top 20% most connected"),
        mpatches.Patch(color=COLOR_LOW, label="Bottom 80%"),
    ]
    ax.legend(handles=legend_handles, framealpha=0.9)
    fig.tight_layout()
    _px_save(fig, output_dir, "degree_bias_depth2")


_LIMIT_EXPLORATION_SCENARIOS = [
    ("degraded_scoring_r20_validated", 16, "validated scoring"),
]
_LIMIT_EXPLORATION_SCENARIOS_DEFENDED = [
    ("px_flood_scoring_defended", 16, "scoring (defended)"),
]
_LIMIT_EXPLORATION_COLORS = {
    "degraded_scoring_r20_validated": "#009E73",  # green
    "px_flood_scoring_defended": "#D55E00",        # orange
}

_DEPTH_COLORS = {0: "#333333", 1: "#0072B2", 2: "#009E73", 3: "#D55E00"}
_DEPTH_LABELS = {0: "depth 0 (initial)", 1: "depth 1", 2: "depth 2", 3: "depth 3"}


def _px_fig_limit_exploration(
    rows: list[dict],
    output_dir: Path,
    max_depth: int = 3,
    limit_scenarios: list | None = None,
) -> None:
    """Side-by-side wave progression for the two limit-exploration scenarios.

    Each panel shows cumulative unique PX peers discovered per wave, with
    background shading by depth level.  Thin lines per seed reveal run-to-run
    variation; the bold line is the median.  A horizontal dashed line marks the
    true mesh size (when available from the evaluation).
    """
    if limit_scenarios is None:
        limit_scenarios = _LIMIT_EXPLORATION_SCENARIOS
    # Gather relevant rows: deepest available wave_depth for each scenario/cs pair
    panels = []
    for scenario, cs, label in limit_scenarios:
        # Prefer deeper runs; take the deepest wave_depth that has data
        candidate_rows = [
            r for r in rows
            if r["scenario"] == scenario and r["coalition_size"] == cs
        ]
        if not candidate_rows:
            continue
        best_depth = max(r.get("wave_depth", 1) for r in candidate_rows)
        panel_rows = [r for r in candidate_rows if r.get("wave_depth", 1) == best_depth]
        if not panel_rows:
            continue
        panels.append((scenario, cs, label, best_depth, panel_rows))

    if not panels:
        LOGGER.warning("No data for limit-exploration figure; skipping")
        return

    n_panels = len(panels)
    fig, axes = plt.subplots(1, n_panels, figsize=(_LINEWIDTH_IN, 4.5), sharey=False)
    if n_panels == 1:
        axes = [axes]

    for ax, (scenario, cs, label, best_depth, panel_rows) in zip(axes, panels):
        # Build per-run wave curves with depth annotation
        seed_curves: list[tuple[list[int], list[int]]] = []  # (cumulative_counts, wave_depths)
        for row in panel_rows:
            run_dir = row.get("run_dir")
            if not run_dir:
                continue
            waves = _px_load_wave_report(Path(run_dir))
            if not waves:
                continue
            cum_list: list[int] = []
            dep_list: list[int] = []
            seen: set[str] = set()
            for wave in waves:
                seen.update(wave.get("px_peers_received_union", []))
                cum_list.append(len(seen))
                dep_list.append(wave.get("depth", 0))
            if cum_list:
                seed_curves.append((cum_list, dep_list))

        if not seed_curves:
            continue

        max_len = max(len(c) for c, _ in seed_curves)

        # Draw depth-level background shading using the run with the most waves
        ref_depths = max(seed_curves, key=lambda c: len(c[0]))[1]
        current_depth = ref_depths[0]
        seg_start = 0
        for wi, d in enumerate(ref_depths):
            if d != current_depth or wi == len(ref_depths) - 1:
                seg_end = wi if d != current_depth else wi + 1
                ax.axvspan(
                    seg_start + 0.5, seg_end + 0.5,
                    alpha=0.08,
                    color=_DEPTH_COLORS.get(current_depth, "#888"),
                )
                current_depth = d
                seg_start = wi

        # Plot each seed as a thin line
        padded = np.zeros((len(seed_curves), max_len))
        for i, (cum_list, _) in enumerate(seed_curves):
            arr = np.array(cum_list, dtype=float)
            padded[i, :len(arr)] = arr
            padded[i, len(arr):] = arr[-1]
            x = np.arange(1, len(cum_list) + 1)
            ax.plot(x, cum_list, color="#aaaaaa", linewidth=0.8, alpha=0.7, zorder=2)

        # Bold median line
        median_curve = np.median(padded, axis=0)
        x_full = np.arange(1, max_len + 1)
        ax.plot(x_full, median_curve, color=_LIMIT_EXPLORATION_COLORS.get(scenario, "#333"),
                linewidth=2.2, zorder=3, label="median")

        # Depth transition markers on x-axis
        ref_dep = seed_curves[0][1]
        shown_depths: set[int] = set()
        legend_patches = []
        for wi, d in enumerate(ref_dep):
            if d not in shown_depths:
                shown_depths.add(d)
                legend_patches.append(
                    mpatches.Patch(
                        color=_DEPTH_COLORS.get(d, "#888"),
                        alpha=0.35,
                        label=_DEPTH_LABELS.get(d, f"depth {d}"),
                    )
                )

        # Inference complete marker
        inference_complete = any(
            row.get("summary", {}) is not None
            for row in panel_rows
        )
        last_wave = max(len(c) for c, _ in seed_curves)

        ax.set_title(label, pad=6)
        ax.set_xlabel("Wave number")
        ax.set_ylabel("Cumulative unique peers discovered")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(linewidth=0.4, alpha=0.4)

        if legend_patches:
            ax.legend(handles=legend_patches, framealpha=0.85, loc="upper left")

        depth_text = f"depth {best_depth} | {len(panel_rows)} run(s)"
        ax.text(0.98, 0.04, depth_text, transform=ax.transAxes,
                ha="right", va="bottom", color="#555555")

    fig.tight_layout()
    _px_save(fig, output_dir, "limit_exploration_wave_progression")


def plot_px_flood(source_root: Path, output_dir: Path, strict: bool, defended: bool = False) -> None:
    subdir = "px_flood_defended" if defended else "px_flood"
    results_root = source_root / subdir
    rows = _px_load_rows(results_root)
    if rows is None:
        if strict:
            raise FileNotFoundError(f"PX flood results not found under {results_root}")
        return
    px_output_dir = output_dir / subdir
    LOGGER.info("PX flood: %d runs found, writing figures to %s", len(rows), px_output_dir)

    depth1_rows = [r for r in rows if r.get("wave_depth", 1) == 1]
    if depth1_rows:
        _px_fig1a_coverage_bar(rows, px_output_dir, wave_depth=1)
        _px_fig2_wave_progression(rows, px_output_dir, wave_depth=1)
        _px_fig3a_precision_bar(rows, px_output_dir, wave_depth=1)
        _px_fig3b_recall_bar(rows, px_output_dir, wave_depth=1)
        _px_fig5a_drift_bar(rows, px_output_dir, wave_depth=1)

    depth2_rows = [r for r in rows if r.get("wave_depth", 1) == 2]
    if depth2_rows:
        _px_fig1a_coverage_bar(rows, px_output_dir, wave_depth=2)
        _px_fig6_degree_bias_depth2(rows, px_output_dir)

    # Limit-exploration figure: uses the deepest available depth for the key scenarios
    _limit_spec = _LIMIT_EXPLORATION_SCENARIOS_DEFENDED if defended else _LIMIT_EXPLORATION_SCENARIOS
    limit_scenarios = {s for s, _, _ in _limit_spec}
    limit_rows = [r for r in rows if r["scenario"] in limit_scenarios]
    if limit_rows:
        _px_fig_limit_exploration(rows, px_output_dir, limit_scenarios=_limit_spec)


def main() -> int:
    args = parse_args()
    configure_style()
    source_root = args.source_root.resolve()
    output_dir = args.output_dir.resolve()
    formats = list(DEFAULT_FORMATS)

    if "passive_ego_mesh" in args.attacks:
        plot_passive_ego_mesh(source_root, output_dir, formats, args.strict)
    if "passive_coalition" in args.attacks:
        plot_passive_coalition(source_root, output_dir, formats, args.strict)
    if "px_flood" in args.attacks:
        plot_px_flood(source_root, output_dir, args.strict, defended=args.defended)
    LOGGER.info("Figures written under %s", output_dir)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
