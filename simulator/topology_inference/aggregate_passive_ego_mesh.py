from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_ROOT = (
    Path(__file__).resolve().parents[1] / "results" / "topology_inference" / "passive_ego_mesh"
)
SUMMARY_DIRNAME = "summary"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate passive_ego_mesh campaign results.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Root directory containing campaign run folders.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def mean_or_zero(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def pstdev_or_zero(values: list[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def collect_run_dirs(output_root: Path) -> list[Path]:
    evaluation_paths = sorted(output_root.glob("*/seed_*_attacker_*/evaluation.json"))
    run_dirs: list[Path] = []
    seen: set[Path] = set()
    for path in evaluation_paths:
        run_dir = path.parent
        resolved = run_dir.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        run_dirs.append(run_dir)
    return run_dirs


def count_neighbors(topics: dict[str, Any], key: str) -> int:
    total = 0
    for topic_data in topics.values():
        if not isinstance(topic_data, dict):
            continue
        values = topic_data.get(key, [])
        if isinstance(values, list):
            total += len(values)
    return total


def infer_qc_flags(
    *,
    inferred_topology: dict[str, Any],
    evaluation: dict[str, Any],
) -> list[str]:
    flags: list[str] = []
    topics = inferred_topology.get("topics", {})
    inferred_empty = True
    if isinstance(topics, dict):
        for topic_data in topics.values():
            if not isinstance(topic_data, dict):
                continue
            if topic_data.get("outgoing_neighbors") or topic_data.get("incoming_neighbors"):
                inferred_empty = False
                break
    if inferred_empty:
        flags.append("INFERRED_EMPTY")

    evaluated_topics = int(evaluation.get("aggregate", {}).get("evaluated_topics", 0))
    if evaluated_topics == 0:
        flags.append("NO_TOPICS_EVALUATED")

    aggregate = evaluation.get("aggregate", {})
    undirected = aggregate.get("undirected_neighbors", {}) if isinstance(aggregate, dict) else {}
    jaccard = float(aggregate.get("undirected_neighbor_jaccard_macro", 0.0)) if isinstance(aggregate, dict) else 0.0
    if (
        float(undirected.get("precision", 0.0)) == 0.0
        and float(undirected.get("recall", 0.0)) == 0.0
        and float(undirected.get("f1", 0.0)) == 0.0
        and jaccard == 0.0
    ):
        flags.append("ALL_METRICS_ZERO")

    return flags


def build_run_row(run_dir: Path) -> dict[str, Any]:
    attack_metadata = load_json(run_dir / "attack_metadata.json")
    inferred_topology = load_json(run_dir / "inferred_topology.json")
    evaluation = load_json(run_dir / "evaluation.json")

    aggregate = evaluation["aggregate"]
    topics_eval = evaluation["topics"]
    truth_target = evaluation.get("truth_target", {})
    resolved_snapshot_heartbeat_index = int(truth_target["resolved_snapshot_heartbeat_index"])
    qc_flags = infer_qc_flags(inferred_topology=inferred_topology, evaluation=evaluation)

    neighbor_precisions = []
    neighbor_recalls = []
    neighbor_f1s = []
    jaccards = []
    predicted_neighbors = 0
    true_neighbors = 0

    for topic_data in topics_eval.values():
        metrics = topic_data["metrics"]
        undirected = metrics.get("undirected_neighbors", {})
        neighbor_precisions.append(float(undirected.get("precision", 0.0)))
        neighbor_recalls.append(float(undirected.get("recall", 0.0)))
        neighbor_f1s.append(float(undirected.get("f1", 0.0)))
        jaccards.append(float(metrics["undirected_neighbor_jaccard"]))
        predicted_neighbors += len(topic_data["prediction"]["undirected_neighbors"])
        true_neighbors += len(topic_data["ground_truth"]["undirected_neighbors"])

    undirected = aggregate["undirected_neighbors"]
    directed_diagnostic = aggregate.get("directed_combined", {})
    row = {
        "scenario": evaluation["scenario"],
        "seed": int(evaluation["seed"]),
        "attacker_index": int(evaluation["attacker_index"]),
        "attacker_node_id": evaluation["attacker_node_id"],
        "resolved_snapshot_heartbeat_index": resolved_snapshot_heartbeat_index,
        "evaluated_topics": int(aggregate["evaluated_topics"]),
        "predicted_neighbors": predicted_neighbors,
        "true_neighbors": true_neighbors,
        "neighbor_precision_macro": mean_or_zero(neighbor_precisions),
        "neighbor_recall_macro": mean_or_zero(neighbor_recalls),
        "neighbor_f1_macro": mean_or_zero(neighbor_f1s),
        "undirected_precision_micro": float(undirected["precision"]),
        "undirected_recall_micro": float(undirected["recall"]),
        "undirected_f1_micro": float(undirected["f1"]),
        # Backward-compatible aliases used by older plotting scripts. For passive_ego_mesh,
        # "combined" now means the primary non-directed neighbor reconstruction.
        "combined_precision_micro": float(undirected["precision"]),
        "combined_recall_micro": float(undirected["recall"]),
        "combined_f1_micro": float(undirected["f1"]),
        "graft_direction_precision_micro": float(directed_diagnostic.get("precision", 0.0)),
        "graft_direction_recall_micro": float(directed_diagnostic.get("recall", 0.0)),
        "graft_direction_f1_micro": float(directed_diagnostic.get("f1", 0.0)),
        "undirected_neighbor_jaccard_macro": mean_or_zero(jaccards),
        "qc_flags": "|".join(qc_flags),
        "run_dir": str(run_dir),
    }
    return row


def aggregate_by_scenario(run_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in run_rows:
        grouped.setdefault(str(row["scenario"]), []).append(row)

    scenario_rows: list[dict[str, Any]] = []
    for scenario, rows in sorted(grouped.items()):
        def collect(metric: str) -> list[float]:
            return [float(row[metric]) for row in rows]

        qc_runs = sum(1 for row in rows if str(row["qc_flags"]))
        scenario_rows.append(
            {
                "scenario": scenario,
                "runs": len(rows),
                "resolved_snapshot_heartbeat_index_min": int(
                    min(collect("resolved_snapshot_heartbeat_index"))
                ),
                "resolved_snapshot_heartbeat_index_max": int(
                    max(collect("resolved_snapshot_heartbeat_index"))
                ),
                "evaluated_topics_mean": mean_or_zero(collect("evaluated_topics")),
                "evaluated_topics_std": pstdev_or_zero(collect("evaluated_topics")),
                "predicted_neighbors_mean": mean_or_zero(collect("predicted_neighbors")),
                "predicted_neighbors_std": pstdev_or_zero(collect("predicted_neighbors")),
                "true_neighbors_mean": mean_or_zero(collect("true_neighbors")),
                "true_neighbors_std": pstdev_or_zero(collect("true_neighbors")),
                "neighbor_precision_macro_mean": mean_or_zero(collect("neighbor_precision_macro")),
                "neighbor_precision_macro_std": pstdev_or_zero(collect("neighbor_precision_macro")),
                "neighbor_recall_macro_mean": mean_or_zero(collect("neighbor_recall_macro")),
                "neighbor_recall_macro_std": pstdev_or_zero(collect("neighbor_recall_macro")),
                "neighbor_f1_macro_mean": mean_or_zero(collect("neighbor_f1_macro")),
                "neighbor_f1_macro_std": pstdev_or_zero(collect("neighbor_f1_macro")),
                "undirected_precision_micro_mean": mean_or_zero(collect("undirected_precision_micro")),
                "undirected_precision_micro_std": pstdev_or_zero(collect("undirected_precision_micro")),
                "undirected_recall_micro_mean": mean_or_zero(collect("undirected_recall_micro")),
                "undirected_recall_micro_std": pstdev_or_zero(collect("undirected_recall_micro")),
                "undirected_f1_micro_mean": mean_or_zero(collect("undirected_f1_micro")),
                "undirected_f1_micro_std": pstdev_or_zero(collect("undirected_f1_micro")),
                "combined_precision_micro_mean": mean_or_zero(collect("combined_precision_micro")),
                "combined_precision_micro_std": pstdev_or_zero(collect("combined_precision_micro")),
                "combined_recall_micro_mean": mean_or_zero(collect("combined_recall_micro")),
                "combined_recall_micro_std": pstdev_or_zero(collect("combined_recall_micro")),
                "combined_f1_micro_mean": mean_or_zero(collect("combined_f1_micro")),
                "combined_f1_micro_std": pstdev_or_zero(collect("combined_f1_micro")),
                "graft_direction_f1_micro_mean": mean_or_zero(collect("graft_direction_f1_micro")),
                "graft_direction_f1_micro_std": pstdev_or_zero(collect("graft_direction_f1_micro")),
                "undirected_neighbor_jaccard_macro_mean": mean_or_zero(
                    collect("undirected_neighbor_jaccard_macro")
                ),
                "undirected_neighbor_jaccard_macro_std": pstdev_or_zero(
                    collect("undirected_neighbor_jaccard_macro")
                ),
                "qc_flagged_runs": qc_runs,
            }
        )
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


def build_markdown(run_rows: list[dict[str, Any]], scenario_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Passive Ego Mesh Mini-Campaign",
        "",
        "Validation set:",
        "- scenarios: degraded_baseline_r20_windowed_px_control_dout, degraded_scoring_r20_validated",
        "- seeds: 24680, 13579, 98765",
        "- attacker_index: 0",
        "",
        "## Scenario Summary",
        "",
        "| Scenario | Runs | Neighbor F1 | Neighbor recall | Jaccard | Topics | Pred Neigh | True Neigh | QC |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in scenario_rows:
        lines.append(
            "| {scenario} | {runs} | {undirected_f1_micro_mean:.4f} ± {undirected_f1_micro_std:.4f} | "
            "{undirected_recall_micro_mean:.4f} ± {undirected_recall_micro_std:.4f} | "
            "{undirected_neighbor_jaccard_macro_mean:.4f} ± {undirected_neighbor_jaccard_macro_std:.4f} | "
            "{evaluated_topics_mean:.2f} | {predicted_neighbors_mean:.2f} | {true_neighbors_mean:.2f} | {qc_flagged_runs} |".format(
                **row
            )
        )

    qc_rows = [row for row in run_rows if row["qc_flags"]]
    lines.extend(["", "## Quality Control", ""])
    if not qc_rows:
        lines.append("No QC flags detected.")
    else:
        lines.append("| Scenario | Seed | Flags | Run Dir |")
        lines.append("| --- | ---: | --- | --- |")
        for row in qc_rows:
            lines.append(
                f"| {row['scenario']} | {row['seed']} | {row['qc_flags']} | {row['run_dir']} |"
            )

    lines.extend(["", "## Scientific Check", ""])
    lines.append(
        "This mini-campaign checks that `passive_ego_mesh` produces coherent per-run JSON outputs, "
        "that the evaluation pipeline is reproducible across seeds, and that baseline vs scoring "
        "scenarios can already be contrasted through non-directed ego-mesh neighbor "
        "precision/recall/F1 and local Jaccard."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    output_root = args.output_root.resolve()
    summary_dir = output_root / SUMMARY_DIRNAME
    run_dirs = collect_run_dirs(output_root)
    run_rows = [build_run_row(run_dir) for run_dir in run_dirs]
    scenario_rows = aggregate_by_scenario(run_rows)

    run_csv = summary_dir / "passive_ego_mesh_run_metrics.csv"
    scenario_csv = summary_dir / "passive_ego_mesh_scenario_summary.csv"

    write_csv(run_csv, run_rows)
    write_csv(scenario_csv, scenario_rows)

    print(run_csv)
    print(scenario_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
