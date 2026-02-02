from __future__ import annotations

"""
Aggregate px_flood evaluation results into summary CSV files.

Reads every evaluation.json found under the results root and produces:

  px_flood/summary/px_flood_run_metrics.csv        — one row per run
  px_flood/summary/px_flood_scenario_summary.csv   — median + IQR per (scenario, coalition_size)
"""

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np


DEFAULT_RESULTS_ROOT = (
    Path(__file__).resolve().parents[1] / "results" / "topology_inference" / "px_flood"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate px_flood evaluation results.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_RESULTS_ROOT,
        help="Root directory containing the campaign run directories.",
    )
    return parser.parse_args()


def safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def load_evaluation(run_dir: Path) -> dict[str, Any] | None:
    path = run_dir / "evaluation.json"
    if not path.exists():
        return None
    try:
        with path.open() as f:
            return json.load(f)
    except Exception:
        return None


def load_attack_metadata(run_dir: Path) -> dict[str, Any] | None:
    path = run_dir / "attack_metadata.json"
    if not path.exists():
        return None
    try:
        with path.open() as f:
            return json.load(f)
    except Exception:
        return None


def collect_run_rows(results_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for scenario_dir in sorted(results_root.iterdir()):
        if not scenario_dir.is_dir():
            continue
        _dm = re.search(r'_depth(\d+)', scenario_dir.name)
        depth_str = str(int(_dm.group(1))) if _dm else "1"
        scenario_name = re.sub(r'_depth\d+', '', scenario_dir.name)

        for run_dir in sorted(scenario_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            parts = run_dir.name.split("_")
            try:
                seed = int(parts[1])
                target_idx = int(parts[3])
                attacker_indices = [int(x) for x in parts[5].split("-")]
                coalition_size = len(attacker_indices)
            except Exception:
                continue

            meta = load_attack_metadata(run_dir)
            ev = load_evaluation(run_dir)
            if meta is None or ev is None:
                continue

            agg = ev.get("per_target_neighborhood", {}).get("aggregate", {})
            disc = ev.get("discovery_coverage", {})
            pert = ev.get("topology_perturbation", {})

            bias = ev.get("degree_bias", {})

            row: dict[str, Any] = {
                "scenario": scenario_name,
                "seed": seed,
                "coalition_size": coalition_size,
                "initial_target_index": target_idx,
                "wave_depth": int(depth_str),
                "run_dir": str(run_dir),
                # Run-level flags
                "inference_complete": bool(meta.get("inference_complete", False)),
                "waves_completed": safe_int(meta.get("waves_completed")),
                "zero_discovered": int(disc.get("unique_nodes_discovered", 0) or 0) == 0,
                # Neighbourhood inference metrics
                "targets_evaluated": safe_int(agg.get("targets_evaluated")),
                "micro_precision": safe_float(agg.get("micro_precision")),
                "micro_recall": safe_float(agg.get("micro_recall")),
                "micro_f1": safe_float(agg.get("micro_f1")),
                "macro_jaccard": safe_float(agg.get("macro_jaccard")),
                "tp": safe_int(agg.get("tp")),
                "fp": safe_int(agg.get("fp")),
                "fn": safe_int(agg.get("fn")),
                # Discovery metrics
                "unique_nodes_discovered": safe_int(disc.get("unique_nodes_discovered")),
                "true_mesh_size": safe_int(disc.get("true_mesh_size")),
                "targets_flooded": safe_int(disc.get("targets_flooded")),
                "discovery_coverage": safe_float(disc.get("discovery_coverage")),
                "discovery_precision": safe_float(disc.get("discovery_precision")),
                # Topology perturbation
                "perturbation_status": pert.get("status", "skipped"),
                "final_topology_drift": safe_float(pert.get("final_topology_drift")),
                "mean_topology_drift": safe_float(pert.get("mean_topology_drift_during_attack")),
                "perturbation_reference_hb": safe_int(pert.get("reference_heartbeat")),
                "perturbation_compared_hbs": safe_int(pert.get("compared_heartbeat_count")),
                # Degree bias
                "high_degree_recall": safe_float(bias.get("high_degree_recall")),
                "low_degree_recall": safe_float(bias.get("low_degree_recall")),
                "discovery_lift": safe_float(bias.get("discovery_lift")),
                "high_degree_share_in_discovered": safe_float(bias.get("high_degree_share_in_discovered")),
            }
            rows.append(row)

    return rows


def build_scenario_summary(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [
        "micro_precision", "micro_recall", "micro_f1", "macro_jaccard",
        "discovery_coverage", "discovery_precision",
        "unique_nodes_discovered", "waves_completed",
        "final_topology_drift", "mean_topology_drift",
        "high_degree_recall", "low_degree_recall", "discovery_lift",
        "high_degree_share_in_discovered",
    ]
    group_keys = ["scenario", "coalition_size", "wave_depth"]

    summary_rows: list[dict[str, Any]] = []
    for (scenario, cs, depth), grp in df.groupby(group_keys):
        row: dict[str, Any] = {
            "scenario": scenario,
            "coalition_size": cs,
            "wave_depth": depth,
            "run_count": len(grp),
            "inference_complete_count": int(grp["inference_complete"].sum()),
            "zero_discovered_count": int(grp["zero_discovered"].sum()),
        }
        for col in numeric_cols:
            vals = grp[col].dropna()
            if vals.empty:
                row[f"{col}_median"] = None
                row[f"{col}_q25"] = None
                row[f"{col}_q75"] = None
                row[f"{col}_mean"] = None
            else:
                row[f"{col}_median"] = float(np.median(vals))
                row[f"{col}_q25"] = float(np.percentile(vals, 25))
                row[f"{col}_q75"] = float(np.percentile(vals, 75))
                row[f"{col}_mean"] = float(np.mean(vals))
        summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def main() -> int:
    args = parse_args()
    results_root = args.output_root.resolve()

    print(f"Scanning {results_root} ...")
    rows = collect_run_rows(results_root)
    if not rows:
        print("No evaluation.json files found.")
        return 1

    df = pd.DataFrame(rows)
    summary_dir = results_root / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)

    run_csv = summary_dir / "px_flood_run_metrics.csv"
    df.to_csv(run_csv, index=False)
    print(f"Run metrics ({len(df)} rows) → {run_csv}")

    summary_df = build_scenario_summary(df)
    scenario_csv = summary_dir / "px_flood_scenario_summary.csv"
    summary_df.to_csv(scenario_csv, index=False)
    print(f"Scenario summary ({len(summary_df)} rows) → {scenario_csv}")

    print("\nScenario summary (discovery_coverage median | final_topology_drift median):")
    for _, row in summary_df.iterrows():
        cov = row.get("discovery_coverage_median")
        drift = row.get("final_topology_drift_median")
        cov_str = f"{cov:.3f}" if cov is not None else "n/a"
        drift_str = f"{drift:.4f}" if drift is not None else "n/a"
        print(
            f"  {row['scenario'][:35]:35s} cs={int(row['coalition_size']):2d} "
            f"depth={int(row['wave_depth'])} "
            f"coverage={cov_str} drift={drift_str}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
