from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_ROOT = (
    Path(__file__).resolve().parents[1] / "results" / "topology_inference" / "passive_coalition"
)
SUMMARY_DIRNAME = "summary"
DEFAULT_THRESHOLDS = [0.50, 0.90, 0.95, 1.00]

FrontierEdge = tuple[str, str, str]
Observation = tuple[str, str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute passive_coalition time-to-knowledge thresholds from inference_events.csv."
        )
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Root directory containing passive_coalition campaign run folders.",
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
    event_paths = sorted(output_root.glob("*/seed_*_attackers_*/inference_events.csv"))
    return [path.parent for path in event_paths if (path.parent / "evaluation.json").exists()]


def read_inference_events(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "heartbeat_index",
            "observer_id",
            "topic",
            "peer_id",
            "inferred_neighbor_present",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} is missing required columns: {', '.join(sorted(missing))}")
        return [dict(row) for row in reader]


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def compare_node_ids(left: str, right: str) -> int:
    try:
        left_int = int(left)
        right_int = int(right)
        if left_int < right_int:
            return -1
        if left_int > right_int:
            return 1
        return 0
    except ValueError:
        if left < right:
            return -1
        if left > right:
            return 1
        return 0


def canonical_undirected_edge(left: str, right: str) -> tuple[str, str]:
    return (left, right) if compare_node_ids(left, right) <= 0 else (right, left)


def truth_frontier_edges(evaluation: dict[str, Any]) -> set[FrontierEdge]:
    edges: set[FrontierEdge] = set()
    topics = evaluation.get("coalition_union", {}).get("topics", {})
    if not isinstance(topics, dict):
        return edges
    for topic, topic_data in topics.items():
        if not isinstance(topic_data, dict):
            continue
        ground_truth = topic_data.get("ground_truth", {})
        if not isinstance(ground_truth, dict):
            continue
        for edge in ground_truth.get("undirected_edges", []):
            if not isinstance(edge, list) or len(edge) != 2:
                continue
            left, right = canonical_undirected_edge(str(edge[0]), str(edge[1]))
            edges.add((str(topic), left, right))
    return edges


def global_truth_undirected_edge_count(evaluation: dict[str, Any]) -> int:
    aggregate = evaluation.get("global_coverage", {}).get("aggregate", {})
    if isinstance(aggregate, dict):
        return int(aggregate.get("global_truth_undirected_edges", 0) or 0)
    return 0


def active_frontier_edges(observations: set[Observation]) -> set[FrontierEdge]:
    edges: set[FrontierEdge] = set()
    for topic, observer_id, peer_id in observations:
        left, right = canonical_undirected_edge(observer_id, peer_id)
        edges.add((topic, left, right))
    return edges


def prf(predicted: set[FrontierEdge], truth: set[FrontierEdge]) -> dict[str, float | int]:
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


def build_timeseries_row(
    *,
    run_dir: Path,
    heartbeat_index: int,
    heartbeat_ms: int,
    predicted: set[FrontierEdge],
    truth: set[FrontierEdge],
    global_truth_edges: int,
    metrics: dict[str, float | int],
) -> dict[str, Any]:
    matched_global_edges = len(predicted & truth)
    return {
        "run_dir": str(run_dir),
        "heartbeat_index": heartbeat_index,
        "time_ms": heartbeat_index * heartbeat_ms,
        "predicted_frontier_edges": len(predicted),
        "true_frontier_edges": len(truth),
        "tp": int(metrics["tp"]),
        "fp": int(metrics["fp"]),
        "fn": int(metrics["fn"]),
        "precision": float(metrics["precision"]),
        "recall": float(metrics["recall"]),
        "f1": float(metrics["f1"]),
        "global_truth_undirected_edges": global_truth_edges,
        "matched_global_undirected_edges": matched_global_edges,
        "global_undirected_edge_coverage": (
            matched_global_edges / global_truth_edges if global_truth_edges else 0.0
        ),
    }


def build_timeseries(
    *,
    run_dir: Path,
    events: list[dict[str, str]],
    truth: set[FrontierEdge],
    global_truth_edges: int,
    heartbeat_ms: int,
    final_heartbeat_index: int,
) -> list[dict[str, Any]]:
    events_by_heartbeat: dict[int, list[dict[str, str]]] = {}
    for event in events:
        topic = str(event.get("topic", "")).strip()
        observer_id = str(event.get("observer_id", "")).strip()
        peer_id = str(event.get("peer_id", "")).strip()
        if not topic or not observer_id or not peer_id:
            continue
        heartbeat_index = int(event["heartbeat_index"])
        events_by_heartbeat.setdefault(heartbeat_index, []).append(event)

    observations: set[Observation] = set()
    rows: list[dict[str, Any]] = []

    if events_by_heartbeat and min(events_by_heartbeat) > 0:
        predicted = active_frontier_edges(observations)
        rows.append(
            build_timeseries_row(
                run_dir=run_dir,
                heartbeat_index=0,
                heartbeat_ms=heartbeat_ms,
                predicted=predicted,
                truth=truth,
                global_truth_edges=global_truth_edges,
                metrics=prf(predicted, truth),
            )
        )

    for heartbeat_index in sorted(events_by_heartbeat):
        for event in events_by_heartbeat[heartbeat_index]:
            observation = (
                str(event["topic"]),
                str(event["observer_id"]),
                str(event["peer_id"]),
            )
            if parse_bool(event["inferred_neighbor_present"]):
                observations.add(observation)
            else:
                observations.discard(observation)
        predicted = active_frontier_edges(observations)
        rows.append(
            build_timeseries_row(
                run_dir=run_dir,
                heartbeat_index=heartbeat_index,
                heartbeat_ms=heartbeat_ms,
                predicted=predicted,
                truth=truth,
                global_truth_edges=global_truth_edges,
                metrics=prf(predicted, truth),
            )
        )

    if not rows or int(rows[-1]["heartbeat_index"]) != final_heartbeat_index:
        predicted = active_frontier_edges(observations)
        rows.append(
            build_timeseries_row(
                run_dir=run_dir,
                heartbeat_index=final_heartbeat_index,
                heartbeat_ms=heartbeat_ms,
                predicted=predicted,
                truth=truth,
                global_truth_edges=global_truth_edges,
                metrics=prf(predicted, truth),
            )
        )

    max_matched = max(
        (int(row["matched_global_undirected_edges"]) for row in rows),
        default=0,
    )
    for row in rows:
        row["max_matched_global_undirected_edges"] = max_matched
        row["edge_coverage_progress_to_max"] = (
            int(row["matched_global_undirected_edges"]) / max_matched if max_matched else 0.0
        )
    return rows


def threshold_label(threshold: float) -> str:
    percent = threshold * 100.0
    if abs(percent - round(percent)) < 1e-9:
        return str(int(round(percent)))
    return f"{percent:.2f}".rstrip("0").rstrip(".").replace(".", "_")


def first_reach(
    timeseries_rows: list[dict[str, Any]], threshold: float, metric: str = "recall"
) -> tuple[int | None, int | None]:
    epsilon = 1e-12
    for row in timeseries_rows:
        if float(row[metric]) + epsilon >= threshold:
            return int(row["heartbeat_index"]), int(row["time_ms"])
    return None, None


def build_run_row(
    *,
    run_dir: Path,
    evaluation: dict[str, Any],
    timeseries_rows: list[dict[str, Any]],
    thresholds: list[float],
) -> dict[str, Any]:
    truth_target = evaluation.get("truth_target", {})
    final_heartbeat_index = int(truth_target.get("resolved_snapshot_heartbeat_index", -1))
    recalls = [float(row["recall"]) for row in timeseries_rows]
    max_recall = max(recalls) if recalls else 0.0
    final_recall = float(timeseries_rows[-1]["recall"]) if timeseries_rows else 0.0
    edge_coverage_values = [
        float(row["global_undirected_edge_coverage"]) for row in timeseries_rows
    ]
    max_edge_coverage = max(edge_coverage_values) if edge_coverage_values else 0.0
    final_edge_coverage = edge_coverage_values[-1] if edge_coverage_values else 0.0

    row: dict[str, Any] = {
        "scenario": str(evaluation.get("scenario", run_dir.parent.name)),
        "seed": int(evaluation.get("seed", -1)),
        "coalition_size": int(evaluation.get("coalition_size", -1)),
        "attacker_indices": ",".join(map(str, evaluation.get("attacker_indices", []))),
        "attacker_node_ids": ",".join(map(str, evaluation.get("attacker_node_ids", []))),
        "final_heartbeat_index": final_heartbeat_index,
        "total_heartbeats": final_heartbeat_index,
        "snapshot_instants": final_heartbeat_index + 1 if final_heartbeat_index >= 0 else "",
        "true_frontier_edges": (
            int(timeseries_rows[-1]["true_frontier_edges"]) if timeseries_rows else 0
        ),
        "final_predicted_frontier_edges": (
            int(timeseries_rows[-1]["predicted_frontier_edges"]) if timeseries_rows else 0
        ),
        "max_recall": max_recall,
        "final_recall": final_recall,
        "max_global_undirected_edge_coverage": max_edge_coverage,
        "final_global_undirected_edge_coverage": final_edge_coverage,
        "max_matched_global_undirected_edges": (
            int(timeseries_rows[-1]["max_matched_global_undirected_edges"])
            if timeseries_rows
            else 0
        ),
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
        edge_heartbeat, edge_time_ms = first_reach(
            timeseries_rows, threshold, metric="edge_coverage_progress_to_max"
        )
        row[f"reached_edge_coverage_{label}_percent"] = edge_heartbeat is not None
        row[f"time_to_edge_coverage_{label}_percent_heartbeat"] = (
            "" if edge_heartbeat is None else edge_heartbeat
        )
        row[f"time_to_edge_coverage_{label}_percent_ms"] = (
            "" if edge_time_ms is None else edge_time_ms
        )
        row[f"time_to_edge_coverage_{label}_percent_fraction_of_total"] = (
            ""
            if edge_heartbeat is None or final_heartbeat_index <= 0
            else edge_heartbeat / final_heartbeat_index
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


def aggregate_by_scenario_and_coalition(
    run_rows: list[dict[str, Any]], thresholds: list[float]
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in run_rows:
        key = (str(row["scenario"]), int(row["coalition_size"]))
        grouped.setdefault(key, []).append(row)

    summary_rows: list[dict[str, Any]] = []
    for (scenario, coalition_size), rows in sorted(grouped.items()):
        out: dict[str, Any] = {
            "scenario": scenario,
            "coalition_size": coalition_size,
            "runs": len(rows),
            "total_heartbeats_mean": mean([float(row["total_heartbeats"]) for row in rows]),
            "total_heartbeats_std": pstdev([float(row["total_heartbeats"]) for row in rows]),
            "snapshot_instants_mean": mean(
                [float(row["snapshot_instants"]) for row in rows if row["snapshot_instants"] != ""]
            ),
            "snapshot_instants_std": pstdev(
                [float(row["snapshot_instants"]) for row in rows if row["snapshot_instants"] != ""]
            ),
            "true_frontier_edges_mean": mean(
                [float(row["true_frontier_edges"]) for row in rows]
            ),
            "true_frontier_edges_std": pstdev(
                [float(row["true_frontier_edges"]) for row in rows]
            ),
            "max_recall_mean": mean([float(row["max_recall"]) for row in rows]),
            "max_recall_std": pstdev([float(row["max_recall"]) for row in rows]),
            "final_recall_mean": mean([float(row["final_recall"]) for row in rows]),
            "final_recall_std": pstdev([float(row["final_recall"]) for row in rows]),
            "max_global_undirected_edge_coverage_mean": mean(
                [float(row["max_global_undirected_edge_coverage"]) for row in rows]
            ),
            "max_global_undirected_edge_coverage_std": pstdev(
                [float(row["max_global_undirected_edge_coverage"]) for row in rows]
            ),
            "final_global_undirected_edge_coverage_mean": mean(
                [float(row["final_global_undirected_edge_coverage"]) for row in rows]
            ),
            "final_global_undirected_edge_coverage_std": pstdev(
                [float(row["final_global_undirected_edge_coverage"]) for row in rows]
            ),
        }
        for threshold in thresholds:
            label = threshold_label(threshold)
            reached_key = f"reached_{label}_percent"
            heartbeat_key = f"time_to_{label}_percent_heartbeat"
            ms_key = f"time_to_{label}_percent_ms"
            fraction_key = f"time_to_{label}_percent_fraction_of_total"
            reached = [row for row in rows if bool(row[reached_key])]
            heartbeat_values = [
                value
                for row in reached
                for value in [finite_float(row[heartbeat_key])]
                if value is not None
            ]
            time_values = [
                value
                for row in reached
                for value in [finite_float(row[ms_key])]
                if value is not None
            ]
            fraction_values = [
                value
                for row in reached
                for value in [finite_float(row[fraction_key])]
                if value is not None
            ]
            out[f"reached_{label}_percent_runs"] = len(reached)
            out[f"reached_{label}_percent_rate"] = len(reached) / len(rows) if rows else 0.0
            out[f"time_to_{label}_percent_heartbeat_mean"] = mean(heartbeat_values)
            out[f"time_to_{label}_percent_heartbeat_std"] = pstdev(heartbeat_values)
            out[f"time_to_{label}_percent_ms_mean"] = mean(time_values)
            out[f"time_to_{label}_percent_ms_std"] = pstdev(time_values)
            out[f"time_to_{label}_percent_fraction_of_total_mean"] = mean(fraction_values)
            out[f"time_to_{label}_percent_fraction_of_total_std"] = pstdev(fraction_values)
            edge_reached_key = f"reached_edge_coverage_{label}_percent"
            edge_heartbeat_key = f"time_to_edge_coverage_{label}_percent_heartbeat"
            edge_ms_key = f"time_to_edge_coverage_{label}_percent_ms"
            edge_fraction_key = f"time_to_edge_coverage_{label}_percent_fraction_of_total"
            edge_reached = [row for row in rows if bool(row[edge_reached_key])]
            edge_heartbeat_values = [
                value
                for row in edge_reached
                for value in [finite_float(row[edge_heartbeat_key])]
                if value is not None
            ]
            edge_time_values = [
                value
                for row in edge_reached
                for value in [finite_float(row[edge_ms_key])]
                if value is not None
            ]
            edge_fraction_values = [
                value
                for row in edge_reached
                for value in [finite_float(row[edge_fraction_key])]
                if value is not None
            ]
            out[f"reached_edge_coverage_{label}_percent_runs"] = len(edge_reached)
            out[f"reached_edge_coverage_{label}_percent_rate"] = (
                len(edge_reached) / len(rows) if rows else 0.0
            )
            out[f"time_to_edge_coverage_{label}_percent_heartbeat_mean"] = mean(
                edge_heartbeat_values
            )
            out[f"time_to_edge_coverage_{label}_percent_heartbeat_std"] = pstdev(
                edge_heartbeat_values
            )
            out[f"time_to_edge_coverage_{label}_percent_ms_mean"] = mean(
                edge_time_values
            )
            out[f"time_to_edge_coverage_{label}_percent_ms_std"] = pstdev(
                edge_time_values
            )
            out[f"time_to_edge_coverage_{label}_percent_fraction_of_total_mean"] = mean(
                edge_fraction_values
            )
            out[f"time_to_edge_coverage_{label}_percent_fraction_of_total_std"] = pstdev(
                edge_fraction_values
            )
        summary_rows.append(out)
    return summary_rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
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
            f"No passive_coalition runs with inference_events.csv and evaluation.json found under {output_root}. "
            "Rerun run_passive_coalition_campaign.py first."
        )

    run_rows: list[dict[str, Any]] = []
    timeseries_rows: list[dict[str, Any]] = []
    for run_dir in run_dirs:
        evaluation = load_json(run_dir / "evaluation.json")
        truth = truth_frontier_edges(evaluation)
        final_heartbeat_index = int(
            evaluation.get("truth_target", {}).get("resolved_snapshot_heartbeat_index", -1)
        )
        run_timeseries = build_timeseries(
            run_dir=run_dir,
            events=read_inference_events(run_dir / "inference_events.csv"),
            truth=truth,
            global_truth_edges=global_truth_undirected_edge_count(evaluation),
            heartbeat_ms=args.heartbeat_ms,
            final_heartbeat_index=final_heartbeat_index,
        )
        scenario = str(evaluation.get("scenario", run_dir.parent.name))
        seed = int(evaluation.get("seed", -1))
        coalition_size = int(evaluation.get("coalition_size", -1))
        for row in run_timeseries:
            row["scenario"] = scenario
            row["seed"] = seed
            row["coalition_size"] = coalition_size
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
    run_path = summary_dir / "passive_coalition_time_to_thresholds_run_metrics.csv"
    scenario_path = summary_dir / "passive_coalition_time_to_thresholds_scenario_summary.csv"
    timeseries_path = summary_dir / "passive_coalition_frontier_recall_timeseries.csv"
    write_csv(run_path, run_rows)
    write_csv(scenario_path, aggregate_by_scenario_and_coalition(run_rows, thresholds))
    write_csv(timeseries_path, timeseries_rows)
    print(run_path)
    print(scenario_path)
    print(timeseries_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
