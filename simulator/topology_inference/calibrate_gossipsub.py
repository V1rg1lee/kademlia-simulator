#!/usr/bin/env python3
"""Helpers shared by GossipSub validation and calibration scripts."""

from __future__ import annotations

import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any, Iterable

DEFAULT_METRICS: list[tuple[str, str]] = [
    ("stable_density_directed", "density"),
    ("stable_mean_degree", "mean degree"),
    ("stable_median_degree", "median degree"),
    ("stable_average_clustering_coefficient", "clustering"),
    ("stable_avg_shortest_path_length", "path length"),
    ("stable_component_count", "components"),
    ("stable_largest_component_size", "giant comp."),
    ("stable_diameter", "diameter"),
]

DEFAULT_WEIGHTS: dict[str, float] = {
    metric: 1.0 for metric, _ in DEFAULT_METRICS
}

_UNIT_INTERVAL_METRICS = {
    "stable_density_directed",
    "stable_average_clustering_coefficient",
}

_NODE_SCALED_METRICS = {
    "stable_mean_degree",
    "stable_median_degree",
    "stable_component_count",
    "stable_largest_component_size",
    "stable_diameter",
}


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise SystemExit(f"missing metrics input: {path}")
    with path.open() as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def parse_csv_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(float(value)) else None
    raw = str(value).strip()
    if raw == "" or raw.lower() in {"nan", "inf", "-inf"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def stable_rows(
    rows: list[dict[str, str]],
    stable_window_fraction: float,
    stable_window_min_rows: int,
) -> list[dict[str, str]]:
    if not rows:
        return []
    fraction = max(0.0, min(1.0, stable_window_fraction))
    target = int(math.ceil(len(rows) * fraction)) if fraction > 0 else 0
    window = max(target, stable_window_min_rows)
    window = min(len(rows), max(window, 1))
    return rows[-window:]


def format_value(value: Any, precision: int = 3) -> str:
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return "nan"
        return f"{float(value):.{precision}f}".rstrip("0").rstrip(".")
    return str(value)


def _aggregate(values: list[float], aggregation: str) -> float | None:
    if not values:
        return None
    if aggregation == "last":
        return float(values[-1])
    if aggregation == "mean":
        return float(statistics.mean(values))
    if aggregation == "median":
        return float(statistics.median(values))
    raise ValueError(f"unknown aggregation: {aggregation}")


def _stable_metric(
    rows: list[dict[str, str]],
    field: str,
    stable_window_fraction: float,
    stable_window_min_rows: int,
    aggregation: str,
) -> float | None:
    selected = stable_rows(rows, stable_window_fraction, stable_window_min_rows)
    values: list[float] = []
    for row in selected:
        value = parse_csv_float(row.get(field))
        if value is not None:
            values.append(value)
    return _aggregate(values, aggregation) if values else None


def _coerce_summary(summary: dict[str, Any]) -> dict[str, Any]:
    coerced: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, str):
            parsed = parse_csv_float(value)
            coerced[key] = parsed if parsed is not None else value
        else:
            coerced[key] = value
    return coerced


def load_calibration_metrics(
    metrics_dir: Path,
    stable_window_fraction: float,
    stable_window_min_rows: int,
    aggregation: str,
) -> dict[str, Any]:
    summary_path = metrics_dir / "summary.json"
    if not summary_path.exists():
        raise SystemExit(f"missing summary metrics: {summary_path}")
    summary = _coerce_summary(json.loads(summary_path.read_text()))

    degree_rows = read_csv_rows(metrics_dir / "degree_timeseries.csv")
    global_rows = read_csv_rows(metrics_dir / "global_graph_timeseries.csv")

    summary.update(
        {
            "stable_density_directed": _stable_metric(
                global_rows,
                "density_directed",
                stable_window_fraction,
                stable_window_min_rows,
                aggregation,
            ),
            "stable_mean_degree": _stable_metric(
                degree_rows,
                "mean_degree",
                stable_window_fraction,
                stable_window_min_rows,
                aggregation,
            ),
            "stable_median_degree": _stable_metric(
                degree_rows,
                "median_degree",
                stable_window_fraction,
                stable_window_min_rows,
                aggregation,
            ),
            "stable_average_clustering_coefficient": _stable_metric(
                global_rows,
                "average_clustering_coefficient",
                stable_window_fraction,
                stable_window_min_rows,
                aggregation,
            ),
            "stable_avg_shortest_path_length": _stable_metric(
                global_rows,
                "avg_shortest_path_length",
                stable_window_fraction,
                stable_window_min_rows,
                aggregation,
            ),
            "stable_component_count": _stable_metric(
                global_rows,
                "component_count",
                stable_window_fraction,
                stable_window_min_rows,
                aggregation,
            ),
            "stable_largest_component_size": _stable_metric(
                global_rows,
                "largest_component_size",
                stable_window_fraction,
                stable_window_min_rows,
                aggregation,
            ),
            "stable_diameter": _stable_metric(
                global_rows,
                "diameter",
                stable_window_fraction,
                stable_window_min_rows,
                aggregation,
            ),
        }
    )

    return summary


def find_metrics_dir(root: Path, size: str) -> Path:
    base = Path(size) if size and Path(size).is_absolute() else (root / size if size else root)
    candidates = [
        base,
        base / "metrics",
        base / "topology_pipeline" / "metrics",
    ]
    for candidate in candidates:
        if (candidate / "summary.json").is_file():
            return candidate
    raise FileNotFoundError(
        "missing metrics directory for size "
        f"{size or str(base)}; checked: "
        + ", ".join(str(path) for path in candidates)
    )


def collect_sizes(sim_root: Path, real_root: Path, sizes: Iterable[str] | None) -> list[str]:
    if sizes:
        candidates = list(sizes)
    else:
        sim_sizes = [path.name for path in sim_root.iterdir() if path.is_dir()]
        real_sizes = [path.name for path in real_root.iterdir() if path.is_dir()]
        candidates = sorted(set(sim_sizes) & set(real_sizes))

    results: list[str] = []
    for size in candidates:
        try:
            find_metrics_dir(sim_root, size)
            find_metrics_dir(real_root, size)
        except FileNotFoundError:
            continue
        results.append(size)
    return results


def relative_error(sim_value: float, real_value: float, metric: str, node_count: int) -> float:
    diff = abs(sim_value - real_value)
    if metric in _UNIT_INTERVAL_METRICS:
        return diff
    if metric in _NODE_SCALED_METRICS:
        denom = max(node_count - 1, 1)
        return diff / denom
    if real_value == 0:
        return 0.0 if diff == 0 else diff
    return diff / abs(real_value)
