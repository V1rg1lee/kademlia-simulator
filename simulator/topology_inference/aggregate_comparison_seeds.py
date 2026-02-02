#!/usr/bin/env python3
"""Aggregate per-seed comparison metrics into a single averaged metrics directory.

Usage:
    python3 aggregate_comparison_seeds.py <size_dir>

<size_dir> must contain seed_*/ subdirectories produced by compare_w_gossipsub.sh,
each with topology_pipeline/metrics/{summary.json, degree_timeseries.csv,
global_graph_timeseries.csv, ...}.

Writes the averaged result to <size_dir>/topology_pipeline/metrics/.
"""

from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path
from statistics import median


def _find_seed_dirs(size_dir: Path) -> list[Path]:
    dirs = sorted(p for p in size_dir.iterdir() if p.is_dir() and p.name.startswith("seed_"))
    if not dirs:
        raise SystemExit(f"No seed_* directories found under {size_dir}")
    return dirs


def _metrics_dir(seed_dir: Path) -> Path:
    return seed_dir / "topology_pipeline" / "metrics"


def _average_timeseries(
    seed_dirs: list[Path], filename: str, index_col: str
) -> list[dict[str, str]]:
    all_rows: list[list[dict[str, str]]] = []
    for sd in seed_dirs:
        p = _metrics_dir(sd) / filename
        if not p.exists():
            continue
        with p.open(newline="") as f:
            all_rows.append(list(csv.DictReader(f)))

    if not all_rows:
        return []

    fieldnames = list(all_rows[0][0].keys())
    # Build index: index_col value → list of rows (one per seed)
    by_index: dict[str, list[dict[str, str]]] = {}
    order: list[str] = []
    for rows in all_rows:
        for row in rows:
            key = row[index_col]
            if key not in by_index:
                by_index[key] = []
                order.append(key)
            by_index[key].append(row)

    result: list[dict[str, str]] = []
    for key in order:
        seed_rows = by_index[key]
        averaged: dict[str, str] = {}
        for col in fieldnames:
            vals = []
            for r in seed_rows:
                try:
                    vals.append(float(r[col]))
                except (ValueError, KeyError):
                    pass
            if vals:
                averaged[col] = f"{sum(vals) / len(vals):.6f}"
            else:
                averaged[col] = seed_rows[0].get(col, "")
        result.append(averaged)

    return result


def _average_summary(seed_dirs: list[Path]) -> dict:
    summaries: list[dict] = []
    for sd in seed_dirs:
        p = _metrics_dir(sd) / "summary.json"
        if p.exists():
            summaries.append(json.loads(p.read_text()))

    if not summaries:
        raise SystemExit("No summary.json found in any seed directory")

    merged: dict = {}
    for key in summaries[0]:
        vals = []
        for s in summaries:
            v = s.get(key)
            if v is None:
                continue
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                pass
        if vals:
            merged[key] = sum(vals) / len(vals)
        else:
            merged[key] = summaries[0][key]

    return merged


def _write_timeseries(out_dir: Path, filename: str, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    out_path = out_dir / filename
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(f"Usage: {sys.argv[0]} <size_dir>")

    size_dir = Path(sys.argv[1]).resolve()
    if not size_dir.is_dir():
        raise SystemExit(f"Not a directory: {size_dir}")

    seed_dirs = _find_seed_dirs(size_dir)
    print(f"Aggregating {len(seed_dirs)} seed(s): {[d.name for d in seed_dirs]}")

    out_dir = size_dir / "topology_pipeline" / "metrics"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Average the two timeseries files used by plot_validation_figures
    for fname, idx in [
        ("degree_timeseries.csv", "heartbeat_index"),
        ("global_graph_timeseries.csv", "heartbeat_index"),
        ("churn_timeseries.csv", "heartbeat_index"),
        ("control_timeseries.csv", "heartbeat_index"),
    ]:
        rows = _average_timeseries(seed_dirs, fname, idx)
        if rows:
            _write_timeseries(out_dir, fname, rows)

    # Average summary.json
    summary = _average_summary(seed_dirs)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    # Copy remaining files from the first seed (used for reference, not for validation figures)
    first_metrics = _metrics_dir(seed_dirs[0])
    for p in first_metrics.iterdir():
        dest = out_dir / p.name
        if not dest.exists():
            shutil.copy2(p, dest)

    print(f"Aggregated metrics written to {out_dir}")


if __name__ == "__main__":
    main()
