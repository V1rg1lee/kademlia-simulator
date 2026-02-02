from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_ROOT = (
    Path(__file__).resolve().parents[1] / "results" / "topology_inference" / "passive_ego_mesh"
)
SUMMARY_DIRNAME = "summary"
DEFAULT_THRESHOLDS = [0.50, 0.90, 0.95, 1.00]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute time-to-knowledge thresholds for passive_ego_mesh from inference_events.csv."
        )
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Root directory containing passive_ego_mesh campaign run folders.",
    )
    parser.add_argument(
        "--heartbeat-ms",
        type=int,
        default=1000,
        help="Duration represented by one heartbeat index, used only for *_ms output columns.",
    )
    parser.add_argument(
        "--thresholds",
        nargs="+",
        type=float,
        default=DEFAULT_THRESHOLDS,
        help="Recall thresholds to report, expressed as fractions in [0, 1].",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def collect_run_dirs(output_root: Path) -> list[Path]:
    event_paths = sorted(output_root.glob("*/seed_*_attacker_*/inference_events.csv"))
    return [path.parent for path in event_paths if (path.parent / "evaluation.json").exists()]


def truth_neighbor_pairs(evaluation: dict[str, Any]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    topics = evaluation.get("topics", {})
    if not isinstance(topics, dict):
        return pairs
    for topic, topic_data in topics.items():
        if not isinstance(topic_data, dict):
            continue
        ground_truth = topic_data.get("ground_truth", {})
        if not isinstance(ground_truth, dict):
            continue
        for peer_id in ground_truth.get("undirected_neighbors", []):
            pairs.add((str(topic), str(peer_id)))
    return pairs


def read_inference_events(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "heartbeat_index",
            "topic",
            "peer_id",
            "inferred_neighbor_present",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"{path} is missing required columns: {', '.join(sorted(missing))}"
            )
        return [dict(row) for row in reader]


def normalize_late_join_event_heartbeats(
    run_dir: Path, events: list[dict[str, str]]
) -> list[dict[str, str]]:
    metadata_path = run_dir / "attack_metadata.json"
    if not events or not metadata_path.exists():
        return events

    metadata = load_json(metadata_path)
    late_join = metadata.get("late_join", {})
    if not isinstance(late_join, dict) or not late_join.get("enabled"):
        return events

    activation_heartbeat = int(late_join.get("activation_heartbeat", -1))
    if activation_heartbeat <= 0:
        return events

    event_heartbeats = [int(event["heartbeat_index"]) for event in events]
    if min(event_heartbeats) >= activation_heartbeat:
        return events

    normalized: list[dict[str, str]] = []
    for event in events:
        row = dict(event)
        local_heartbeat = int(event["heartbeat_index"])
        # Legacy late-join files used the observer-local counter. Activation-side events are
        # logged before that counter increments, and first-heartbeat events are logged after it.
        row["heartbeat_index"] = str(activation_heartbeat + max(local_heartbeat - 1, 0))
        normalized.append(row)
    return normalized


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def prf(predicted: set[tuple[str, str]], truth: set[tuple[str, str]]) -> dict[str, float | int]:
    tp = len(predicted & truth)
    fp = len(predicted - truth)
    fn = len(truth - predicted)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else (1.0 if not predicted else 0.0)
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def threshold_label(threshold: float) -> str:
    percent = threshold * 100.0
    if abs(percent - round(percent)) < 1e-9:
        return str(int(round(percent)))
    return f"{percent:.2f}".rstrip("0").rstrip(".").replace(".", "_")


def first_reach(
    timeseries_rows: list[dict[str, Any]], threshold: float
) -> tuple[int | None, int | None]:
    epsilon = 1e-12
    for row in timeseries_rows:
        if float(row["recall"]) + epsilon >= threshold:
            return int(row["heartbeat_index"]), int(row["time_ms"])
    return None, None


def build_timeseries(
    *,
    run_dir: Path,
    events: list[dict[str, str]],
    truth: set[tuple[str, str]],
    heartbeat_ms: int,
    final_heartbeat_index: int,
) -> list[dict[str, Any]]:
    events_by_heartbeat: dict[int, list[dict[str, str]]] = {}
    for event in events:
        topic = str(event.get("topic", "")).strip()
        peer_id = str(event.get("peer_id", "")).strip()
        if not topic or not peer_id:
            continue
        heartbeat_index = int(event["heartbeat_index"])
        events_by_heartbeat.setdefault(heartbeat_index, []).append(event)

    predicted: set[tuple[str, str]] = set()
    rows: list[dict[str, Any]] = []

    if events_by_heartbeat and min(events_by_heartbeat) > 0:
        metrics = prf(predicted, truth)
        rows.append(
            build_timeseries_row(
                run_dir=run_dir,
                heartbeat_index=0,
                heartbeat_ms=heartbeat_ms,
                predicted=predicted,
                truth=truth,
                metrics=metrics,
            )
        )

    for heartbeat_index in sorted(events_by_heartbeat):
        for event in events_by_heartbeat[heartbeat_index]:
            pair = (str(event["topic"]), str(event["peer_id"]))
            if parse_bool(event["inferred_neighbor_present"]):
                predicted.add(pair)
            else:
                predicted.discard(pair)
        metrics = prf(predicted, truth)
        rows.append(
            build_timeseries_row(
                run_dir=run_dir,
                heartbeat_index=heartbeat_index,
                heartbeat_ms=heartbeat_ms,
                predicted=predicted,
                truth=truth,
                metrics=metrics,
            )
        )

    if not rows or int(rows[-1]["heartbeat_index"]) != final_heartbeat_index:
        metrics = prf(predicted, truth)
        rows.append(
            build_timeseries_row(
                run_dir=run_dir,
                heartbeat_index=final_heartbeat_index,
                heartbeat_ms=heartbeat_ms,
                predicted=predicted,
                truth=truth,
                metrics=metrics,
            )
        )

    return rows


def build_timeseries_row(
    *,
    run_dir: Path,
    heartbeat_index: int,
    heartbeat_ms: int,
    predicted: set[tuple[str, str]],
    truth: set[tuple[str, str]],
    metrics: dict[str, float | int],
) -> dict[str, Any]:
    return {
        "run_dir": str(run_dir),
        "heartbeat_index": heartbeat_index,
        "time_ms": heartbeat_index * heartbeat_ms,
        "predicted_neighbors": len(predicted),
        "true_neighbors": len(truth),
        "tp": int(metrics["tp"]),
        "fp": int(metrics["fp"]),
        "fn": int(metrics["fn"]),
        "precision": float(metrics["precision"]),
        "recall": float(metrics["recall"]),
        "f1": float(metrics["f1"]),
    }


def build_run_row(
    *,
    run_dir: Path,
    evaluation: dict[str, Any],
    timeseries_rows: list[dict[str, Any]],
    thresholds: list[float],
) -> dict[str, Any]:
    scenario = str(evaluation.get("scenario", run_dir.parent.name))
    seed = int(evaluation.get("seed", -1))
    attacker_index = int(evaluation.get("attacker_index", -1))
    truth_target = evaluation.get("truth_target", {})
    final_heartbeat_index = int(truth_target.get("resolved_snapshot_heartbeat_index", -1))

    recalls = [float(row["recall"]) for row in timeseries_rows]
    max_recall = max(recalls) if recalls else 0.0
    final_recall = float(timeseries_rows[-1]["recall"]) if timeseries_rows else 0.0

    row: dict[str, Any] = {
        "scenario": scenario,
        "seed": seed,
        "attacker_index": attacker_index,
        "attacker_node_id": str(evaluation.get("attacker_node_id", "")),
        "final_heartbeat_index": final_heartbeat_index,
        "total_heartbeats": final_heartbeat_index,
        "snapshot_instants": final_heartbeat_index + 1 if final_heartbeat_index >= 0 else "",
        "true_neighbors": int(timeseries_rows[-1]["true_neighbors"]) if timeseries_rows else 0,
        "final_predicted_neighbors": (
            int(timeseries_rows[-1]["predicted_neighbors"]) if timeseries_rows else 0
        ),
        "max_recall": max_recall,
        "final_recall": final_recall,
        "run_dir": str(run_dir),
    }

    max_rows = [r for r in timeseries_rows if abs(float(r["recall"]) - max_recall) < 1e-12]
    if max_rows:
        row["time_to_max_recall_heartbeat"] = int(max_rows[0]["heartbeat_index"])
        row["time_to_max_recall_ms"] = int(max_rows[0]["time_ms"])
    else:
        row["time_to_max_recall_heartbeat"] = ""
        row["time_to_max_recall_ms"] = ""

    for threshold in thresholds:
        label = threshold_label(threshold)
        heartbeat, time_ms = first_reach(timeseries_rows, threshold)
        row[f"reached_{label}_percent"] = heartbeat is not None
        row[f"time_to_{label}_percent"] = "" if heartbeat is None else heartbeat
        row[f"time_to_{label}_percent_heartbeat"] = "" if heartbeat is None else heartbeat
        row[f"time_to_{label}_percent_ms"] = "" if time_ms is None else time_ms
        row[f"time_to_{label}_percent_fraction_of_total"] = (
            ""
            if heartbeat is None or final_heartbeat_index <= 0
            else heartbeat / final_heartbeat_index
        )

    return row


def finite_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    number = float(value)
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def pstdev(values: list[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def aggregate_by_scenario(run_rows: list[dict[str, Any]], thresholds: list[float]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in run_rows:
        grouped.setdefault(str(row["scenario"]), []).append(row)

    scenario_rows: list[dict[str, Any]] = []
    for scenario, rows in sorted(grouped.items()):
        out: dict[str, Any] = {
            "scenario": scenario,
            "runs": len(rows),
            "total_heartbeats_mean": mean(
                [float(row["total_heartbeats"]) for row in rows]
            ),
            "total_heartbeats_std": pstdev(
                [float(row["total_heartbeats"]) for row in rows]
            ),
            "snapshot_instants_mean": mean(
                [float(row["snapshot_instants"]) for row in rows if row["snapshot_instants"] != ""]
            ),
            "snapshot_instants_std": pstdev(
                [float(row["snapshot_instants"]) for row in rows if row["snapshot_instants"] != ""]
            ),
            "true_neighbors_mean": mean(
                [float(row["true_neighbors"]) for row in rows]
            ),
            "true_neighbors_std": pstdev(
                [float(row["true_neighbors"]) for row in rows]
            ),
            "max_recall_mean": mean([float(row["max_recall"]) for row in rows]),
            "max_recall_std": pstdev([float(row["max_recall"]) for row in rows]),
            "final_recall_mean": mean([float(row["final_recall"]) for row in rows]),
            "final_recall_std": pstdev([float(row["final_recall"]) for row in rows]),
        }
        for threshold in thresholds:
            label = threshold_label(threshold)
            reached_key = f"reached_{label}_percent"
            heartbeat_key = f"time_to_{label}_percent_heartbeat"
            ms_key = f"time_to_{label}_percent_ms"
            fraction_key = f"time_to_{label}_percent_fraction_of_total"
            reached = [row for row in rows if bool(row[reached_key])]
            heartbeats = [finite_float(row[heartbeat_key]) for row in reached]
            times_ms = [finite_float(row[ms_key]) for row in reached]
            fractions = [finite_float(row[fraction_key]) for row in reached]
            heartbeat_values = [value for value in heartbeats if value is not None]
            time_values = [value for value in times_ms if value is not None]
            fraction_values = [value for value in fractions if value is not None]
            out[f"reached_{label}_percent_runs"] = len(reached)
            out[f"reached_{label}_percent_rate"] = len(reached) / len(rows) if rows else 0.0
            out[f"time_to_{label}_percent_heartbeat_mean"] = mean(heartbeat_values)
            out[f"time_to_{label}_percent_heartbeat_std"] = pstdev(heartbeat_values)
            out[f"time_to_{label}_percent_ms_mean"] = mean(time_values)
            out[f"time_to_{label}_percent_ms_std"] = pstdev(time_values)
            out[f"time_to_{label}_percent_fraction_of_total_mean"] = mean(fraction_values)
            out[f"time_to_{label}_percent_fraction_of_total_std"] = pstdev(fraction_values)
        scenario_rows.append(out)
    return scenario_rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    output_root = args.output_root.resolve()
    thresholds = sorted(set(args.thresholds))
    for threshold in thresholds:
        if threshold < 0.0 or threshold > 1.0:
            raise ValueError(f"Threshold must be in [0, 1], got {threshold}")

    run_dirs = collect_run_dirs(output_root)
    if not run_dirs:
        raise FileNotFoundError(
            f"No passive_ego_mesh runs with inference_events.csv and evaluation.json found under {output_root}. "
            "Rerun run_passive_ego_mesh_campaign.py first."
        )
    run_rows: list[dict[str, Any]] = []
    timeseries_rows: list[dict[str, Any]] = []

    for run_dir in run_dirs:
        evaluation = load_json(run_dir / "evaluation.json")
        truth = truth_neighbor_pairs(evaluation)
        final_heartbeat_index = int(
            evaluation.get("truth_target", {}).get("resolved_snapshot_heartbeat_index", -1)
        )
        events = normalize_late_join_event_heartbeats(
            run_dir, read_inference_events(run_dir / "inference_events.csv")
        )
        run_timeseries = build_timeseries(
            run_dir=run_dir,
            events=events,
            truth=truth,
            heartbeat_ms=args.heartbeat_ms,
            final_heartbeat_index=final_heartbeat_index,
        )
        scenario = str(evaluation.get("scenario", run_dir.parent.name))
        seed = int(evaluation.get("seed", -1))
        attacker_index = int(evaluation.get("attacker_index", -1))
        for row in run_timeseries:
            row["scenario"] = scenario
            row["seed"] = seed
            row["attacker_index"] = attacker_index
        timeseries_rows.extend(run_timeseries)
        run_rows.append(
            build_run_row(
                run_dir=run_dir,
                evaluation=evaluation,
                timeseries_rows=run_timeseries,
                thresholds=thresholds,
            )
        )

    summary_dir = output_root / SUMMARY_DIRNAME
    run_path = summary_dir / "passive_ego_mesh_time_to_thresholds_run_metrics.csv"
    scenario_path = summary_dir / "passive_ego_mesh_time_to_thresholds_scenario_summary.csv"
    timeseries_path = summary_dir / "passive_ego_mesh_recall_timeseries.csv"
    write_csv(run_path, run_rows)
    write_csv(scenario_path, aggregate_by_scenario(run_rows, thresholds))
    write_csv(timeseries_path, timeseries_rows)

    print(run_path)
    print(scenario_path)
    print(timeseries_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
