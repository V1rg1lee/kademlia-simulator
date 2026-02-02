from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate topology-inference run outputs.")
    parser.add_argument("--run-dir", required=True, type=Path, help="Run directory to validate.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_required_files(run_dir: Path) -> None:
    required = [
        "mesh_snapshots.csv",
        "attack_metadata.json",
        "inferred_topology.json",
        "inference_events.csv",
        "evaluation.json",
    ]
    missing = [name for name in required if not (run_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"{run_dir} is missing required outputs: {', '.join(missing)}")


def validate_mesh_snapshots(run_dir: Path) -> pd.DataFrame:
    snapshots_df = pd.read_csv(run_dir / "mesh_snapshots.csv")
    required_columns = {"heartbeat_index", "snapshot_kind", "node_id", "peer_id", "topic"}
    missing = required_columns - set(snapshots_df.columns)
    if missing:
        raise ValueError(f"mesh_snapshots.csv is missing required columns: {', '.join(sorted(missing))}")

    snapshots_df = snapshots_df.copy()
    snapshots_df["heartbeat_index"] = snapshots_df["heartbeat_index"].astype(int)
    snapshots_df["snapshot_kind"] = snapshots_df["snapshot_kind"].astype(str)

    marker_rows = snapshots_df[snapshots_df["snapshot_kind"] == "heartbeat_marker"]
    if marker_rows.empty:
        raise ValueError("mesh_snapshots.csv does not contain heartbeat_marker rows")

    last_heartbeat = int(snapshots_df["heartbeat_index"].max())
    if marker_rows[marker_rows["heartbeat_index"] == last_heartbeat].empty:
        raise ValueError(f"mesh_snapshots.csv is missing heartbeat_marker for final heartbeat {last_heartbeat}")

    edge_rows = snapshots_df[snapshots_df["snapshot_kind"] == "mesh_edge"].copy()
    if edge_rows.empty:
        raise ValueError("mesh_snapshots.csv does not contain mesh_edge rows")

    for column in ("node_id", "peer_id", "topic"):
        values = edge_rows[column]
        if values.isna().any() or values.astype(str).str.strip().eq("").any():
            raise ValueError(f"mesh_snapshots.csv contains empty {column} values in mesh_edge rows")

    duplicate_mask = edge_rows.duplicated(
        subset=["heartbeat_index", "node_id", "peer_id", "topic"],
        keep=False,
    )
    if duplicate_mask.any():
        duplicate_count = int(duplicate_mask.sum())
        raise ValueError(f"mesh_snapshots.csv contains {duplicate_count} duplicated mesh_edge rows")

    return snapshots_df


def validate_inference_events(run_dir: Path) -> pd.DataFrame:
    events_df = pd.read_csv(run_dir / "inference_events.csv")
    required_columns = {
        "heartbeat_index",
        "observer_index",
        "observer_id",
        "observer_role",
        "attack_phase",
        "topic",
        "peer_id",
        "action",
        "cause",
        "inferred_neighbor_present",
    }
    missing = required_columns - set(events_df.columns)
    if missing:
        raise ValueError(f"inference_events.csv is missing required columns: {', '.join(sorted(missing))}")

    if events_df.empty:
        raise ValueError("inference_events.csv does not contain any inference event")

    events_df = events_df.copy()
    events_df["heartbeat_index"] = events_df["heartbeat_index"].astype(int)
    events_df["observer_index"] = events_df["observer_index"].astype(int)
    return events_df


def validate_evaluation_truth_target(evaluation: dict[str, Any], snapshots_df: pd.DataFrame) -> None:
    truth_target = evaluation.get("truth_target")
    if not isinstance(truth_target, dict):
        raise ValueError("evaluation.json is missing truth_target")

    available_heartbeats = set(snapshots_df["heartbeat_index"].astype(int).tolist())
    heartbeat_keys = [
        "resolved_snapshot_heartbeat_index",
        "resolved_before_snapshot_heartbeat_index",
        "resolved_after_snapshot_heartbeat_index",
    ]
    resolved = {
        key: int(truth_target[key])
        for key in heartbeat_keys
        if truth_target.get(key) is not None
    }
    if not resolved:
        raise ValueError("evaluation truth_target does not contain a resolved heartbeat index")

    marker_rows = snapshots_df[snapshots_df["snapshot_kind"].astype(str) == "heartbeat_marker"]
    marker_heartbeats = set(marker_rows["heartbeat_index"].astype(int).tolist())
    for key, heartbeat_index in resolved.items():
        if heartbeat_index not in available_heartbeats:
            raise ValueError(f"{key}={heartbeat_index} is absent from mesh_snapshots.csv")
        if heartbeat_index not in marker_heartbeats:
            raise ValueError(f"{key}={heartbeat_index} has no heartbeat_marker row")


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    validate_required_files(run_dir)
    snapshots_df = validate_mesh_snapshots(run_dir)
    validate_inference_events(run_dir)
    evaluation = load_json(run_dir / "evaluation.json")
    validate_evaluation_truth_target(evaluation, snapshots_df)
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
