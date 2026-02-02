from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_ROOT = (
    Path(__file__).resolve().parents[1] / "results" / "topology_inference" / "passive_coalition"
)
SUMMARY_DIRNAME = "summary"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate passive_coalition campaign results.")
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


def quantile_or_zero(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    if lower_index == upper_index:
        return ordered[lower_index]
    weight = position - lower_index
    return ordered[lower_index] * (1.0 - weight) + ordered[upper_index] * weight


def collect_run_dirs(output_root: Path) -> list[Path]:
    evaluation_paths = sorted(output_root.glob("*/seed_*_attackers_*/evaluation.json"))
    return [path.parent for path in evaluation_paths]


def infer_qc_flags(
    *,
    inferred_topology: dict[str, Any],
    evaluation: dict[str, Any],
) -> list[str]:
    flags: list[str] = []
    coalition_topics = inferred_topology.get("coalition_union", {}).get("topics", {})
    inferred_empty = True
    if isinstance(coalition_topics, dict):
        for topic_data in coalition_topics.values():
            if not isinstance(topic_data, dict):
                continue
            if topic_data.get("directed_edges") or topic_data.get("undirected_edges"):
                inferred_empty = False
                break
    if inferred_empty:
        flags.append("INFERRED_EMPTY")

    coalition_aggregate = evaluation.get("coalition_union", {}).get("aggregate", {})
    if int(coalition_aggregate.get("evaluated_topics", 0)) == 0:
        flags.append("NO_TOPICS_EVALUATED")

    coverage_aggregate = evaluation.get("global_coverage", {}).get("aggregate", {})
    if (
        float(
            coalition_aggregate.get(
                "frontier_undirected_precision",
                coalition_aggregate.get("frontier_directed_precision", 0.0),
            )
        )
        == 0.0
        and float(
            coalition_aggregate.get(
                "frontier_undirected_recall",
                coalition_aggregate.get("frontier_directed_recall", 0.0),
            )
        )
        == 0.0
        and float(
            coalition_aggregate.get(
                "frontier_undirected_f1",
                coalition_aggregate.get("frontier_directed_f1", 0.0),
            )
        )
        == 0.0
        and float(coalition_aggregate.get("frontier_undirected_jaccard", 0.0)) == 0.0
        and float(
            coverage_aggregate.get(
                "global_undirected_edge_coverage",
                coverage_aggregate.get("global_edge_coverage", 0.0),
            )
        )
        == 0.0
    ):
        flags.append("ALL_METRICS_ZERO")

    return flags


def build_run_row(run_dir: Path) -> dict[str, Any]:
    attack_metadata = load_json(run_dir / "attack_metadata.json")
    inferred_topology = load_json(run_dir / "inferred_topology.json")
    evaluation = load_json(run_dir / "evaluation.json")

    coalition = evaluation["coalition_union"]["aggregate"]
    coverage = evaluation["global_coverage"]["aggregate"]
    per_attacker = evaluation.get("per_attacker", {})
    truth_target = evaluation.get("truth_target", {})
    resolved_snapshot_heartbeat_index = int(truth_target["resolved_snapshot_heartbeat_index"])
    qc_flags = infer_qc_flags(inferred_topology=inferred_topology, evaluation=evaluation)

    per_attacker_combined_f1 = []
    per_attacker_undirected_f1 = []
    per_attacker_jaccard = []
    per_attacker_predicted_neighbors = []
    per_attacker_true_neighbors = []
    for attacker_data in per_attacker.values():
        if not isinstance(attacker_data, dict):
            continue
        aggregate = attacker_data.get("aggregate", {})
        combined = aggregate.get("directed_combined", {})
        undirected = aggregate.get("undirected_neighbors", {})
        per_attacker_combined_f1.append(float(combined.get("f1", 0.0)))
        per_attacker_undirected_f1.append(
            float(undirected.get("f1", combined.get("f1", 0.0)))
        )
        per_attacker_jaccard.append(float(aggregate.get("undirected_neighbor_jaccard_macro", 0.0)))
        per_attacker_predicted_neighbors.append(float(aggregate.get("predicted_neighbors", 0.0)))
        per_attacker_true_neighbors.append(float(aggregate.get("true_neighbors", 0.0)))

    return {
        "scenario": evaluation["scenario"],
        "seed": int(evaluation["seed"]),
        "coalition_size": int(evaluation["coalition_size"]),
        "attacker_indices": ",".join(map(str, evaluation.get("attacker_indices", []))),
        "attacker_node_ids": ",".join(map(str, evaluation.get("attacker_node_ids", []))),
        "resolved_snapshot_heartbeat_index": resolved_snapshot_heartbeat_index,
        "evaluated_topics": int(coalition.get("evaluated_topics", 0)),
        "predicted_frontier_edges": int(coalition.get("predicted_frontier_edges", 0)),
        "frontier_truth_edges": int(coalition.get("frontier_truth_edges", 0)),
        "matched_frontier_edges": int(coalition.get("matched_frontier_edges", 0)),
        "predicted_frontier_undirected_edges": int(
            coalition.get("predicted_frontier_undirected_edges", 0)
        ),
        "frontier_undirected_truth_edges": int(
            coalition.get("frontier_undirected_truth_edges", 0)
        ),
        "matched_frontier_undirected_edges": int(
            coalition.get("matched_frontier_undirected_edges", 0)
        ),
        "frontier_precision": float(coalition.get("frontier_directed_precision", 0.0)),
        "frontier_recall": float(coalition.get("frontier_directed_recall", 0.0)),
        "frontier_f1": float(coalition.get("frontier_directed_f1", 0.0)),
        "frontier_undirected_precision": float(
            coalition.get("frontier_undirected_precision", 0.0)
        ),
        "frontier_undirected_recall": float(
            coalition.get("frontier_undirected_recall", 0.0)
        ),
        "frontier_undirected_f1": float(coalition.get("frontier_undirected_f1", 0.0)),
        "frontier_undirected_jaccard": float(coalition.get("frontier_undirected_jaccard", 0.0)),
        "global_edge_coverage": float(coverage.get("global_edge_coverage", 0.0)),
        "global_undirected_edge_coverage": float(
            coverage.get("global_undirected_edge_coverage", 0.0)
        ),
        "frontier_edge_fraction": float(coverage.get("frontier_edge_fraction", 0.0)),
        "frontier_undirected_edge_fraction": float(
            coverage.get("frontier_undirected_edge_fraction", 0.0)
        ),
        "matched_global_node_coverage": float(coverage.get("matched_global_node_coverage", 0.0)),
        "matched_global_undirected_node_coverage": float(
            coverage.get("matched_global_undirected_node_coverage", 0.0)
        ),
        "per_attacker_combined_f1_mean": mean_or_zero(per_attacker_combined_f1),
        "per_attacker_undirected_f1_mean": mean_or_zero(per_attacker_undirected_f1),
        "per_attacker_jaccard_mean": mean_or_zero(per_attacker_jaccard),
        "per_attacker_predicted_neighbors_mean": mean_or_zero(per_attacker_predicted_neighbors),
        "per_attacker_true_neighbors_mean": mean_or_zero(per_attacker_true_neighbors),
        "qc_flags": "|".join(qc_flags),
        "run_dir": str(run_dir),
    }


def build_topic_rows(run_dir: Path) -> list[dict[str, Any]]:
    evaluation = load_json(run_dir / "evaluation.json")
    coverage_topics = evaluation.get("global_coverage", {}).get("topics", {})
    truth_target = evaluation.get("truth_target", {})
    if not isinstance(coverage_topics, dict):
        return []

    topic_rows: list[dict[str, Any]] = []
    for topic, coverage in sorted(coverage_topics.items()):
        if not isinstance(coverage, dict):
            continue
        topic_rows.append(
            {
                "scenario": evaluation["scenario"],
                "seed": int(evaluation["seed"]),
                "coalition_size": int(evaluation["coalition_size"]),
                "attacker_indices": ",".join(map(str, evaluation.get("attacker_indices", []))),
                "topic": str(topic),
                "resolved_snapshot_heartbeat_index": int(
                    truth_target["resolved_snapshot_heartbeat_index"]
                ),
                "global_truth_undirected_edges": int(
                    coverage.get("global_truth_undirected_edges", 0)
                ),
                "frontier_truth_undirected_edges": int(
                    coverage.get("frontier_truth_undirected_edges", 0)
                ),
                "matched_global_undirected_edges": int(
                    coverage.get("matched_global_undirected_edges", 0)
                ),
                "global_undirected_edge_coverage": float(
                    coverage.get("global_undirected_edge_coverage", 0.0)
                ),
                "frontier_undirected_edge_fraction": float(
                    coverage.get("frontier_undirected_edge_fraction", 0.0)
                ),
                "global_truth_nodes": int(coverage.get("global_truth_nodes", 0)),
                "matched_undirected_nodes": int(coverage.get("matched_undirected_nodes", 0)),
                "matched_global_undirected_node_coverage": float(
                    coverage.get("matched_global_undirected_node_coverage", 0.0)
                ),
                "global_truth_edges": int(coverage.get("global_truth_edges", 0)),
                "matched_global_edges": int(coverage.get("matched_global_edges", 0)),
                "global_edge_coverage": float(coverage.get("global_edge_coverage", 0.0)),
                "run_dir": str(run_dir),
            }
        )
    return topic_rows


def aggregate_by_scenario_and_coalition(run_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in run_rows:
        key = (str(row["scenario"]), int(row["coalition_size"]))
        grouped.setdefault(key, []).append(row)

    summary_rows: list[dict[str, Any]] = []
    for (scenario, coalition_size), rows in sorted(grouped.items()):
        def collect(metric: str) -> list[float]:
            return [float(row[metric]) for row in rows]

        qc_runs = sum(1 for row in rows if str(row["qc_flags"]))
        summary_rows.append(
            {
                "scenario": scenario,
                "coalition_size": coalition_size,
                "runs": len(rows),
                "resolved_snapshot_heartbeat_index_min": int(
                    min(collect("resolved_snapshot_heartbeat_index"))
                ),
                "resolved_snapshot_heartbeat_index_max": int(
                    max(collect("resolved_snapshot_heartbeat_index"))
                ),
                "evaluated_topics_mean": mean_or_zero(collect("evaluated_topics")),
                "frontier_precision_mean": mean_or_zero(collect("frontier_precision")),
                "frontier_precision_std": pstdev_or_zero(collect("frontier_precision")),
                "frontier_recall_mean": mean_or_zero(collect("frontier_recall")),
                "frontier_recall_std": pstdev_or_zero(collect("frontier_recall")),
                "frontier_f1_mean": mean_or_zero(collect("frontier_f1")),
                "frontier_f1_std": pstdev_or_zero(collect("frontier_f1")),
                "frontier_undirected_precision_mean": mean_or_zero(
                    collect("frontier_undirected_precision")
                ),
                "frontier_undirected_precision_std": pstdev_or_zero(
                    collect("frontier_undirected_precision")
                ),
                "frontier_undirected_recall_mean": mean_or_zero(
                    collect("frontier_undirected_recall")
                ),
                "frontier_undirected_recall_std": pstdev_or_zero(
                    collect("frontier_undirected_recall")
                ),
                "frontier_undirected_f1_mean": mean_or_zero(
                    collect("frontier_undirected_f1")
                ),
                "frontier_undirected_f1_std": pstdev_or_zero(
                    collect("frontier_undirected_f1")
                ),
                "frontier_undirected_jaccard_mean": mean_or_zero(
                    collect("frontier_undirected_jaccard")
                ),
                "frontier_undirected_jaccard_std": pstdev_or_zero(
                    collect("frontier_undirected_jaccard")
                ),
                "global_edge_coverage_mean": mean_or_zero(collect("global_edge_coverage")),
                "global_edge_coverage_std": pstdev_or_zero(collect("global_edge_coverage")),
                "global_undirected_edge_coverage_mean": mean_or_zero(
                    collect("global_undirected_edge_coverage")
                ),
                "global_undirected_edge_coverage_std": pstdev_or_zero(
                    collect("global_undirected_edge_coverage")
                ),
                "frontier_edge_fraction_mean": mean_or_zero(collect("frontier_edge_fraction")),
                "frontier_edge_fraction_std": pstdev_or_zero(collect("frontier_edge_fraction")),
                "frontier_undirected_edge_fraction_mean": mean_or_zero(
                    collect("frontier_undirected_edge_fraction")
                ),
                "frontier_undirected_edge_fraction_std": pstdev_or_zero(
                    collect("frontier_undirected_edge_fraction")
                ),
                "matched_global_node_coverage_mean": mean_or_zero(
                    collect("matched_global_node_coverage")
                ),
                "matched_global_node_coverage_std": pstdev_or_zero(
                    collect("matched_global_node_coverage")
                ),
                "matched_global_undirected_node_coverage_mean": mean_or_zero(
                    collect("matched_global_undirected_node_coverage")
                ),
                "matched_global_undirected_node_coverage_std": pstdev_or_zero(
                    collect("matched_global_undirected_node_coverage")
                ),
                "per_attacker_combined_f1_mean": mean_or_zero(
                    collect("per_attacker_combined_f1_mean")
                ),
                "per_attacker_combined_f1_std": pstdev_or_zero(
                    collect("per_attacker_combined_f1_mean")
                ),
                "per_attacker_undirected_f1_mean": mean_or_zero(
                    collect("per_attacker_undirected_f1_mean")
                ),
                "per_attacker_undirected_f1_std": pstdev_or_zero(
                    collect("per_attacker_undirected_f1_mean")
                ),
                "qc_flagged_runs": qc_runs,
            }
        )
    return summary_rows


def aggregate_topics_by_scenario_and_coalition(
    topic_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in topic_rows:
        key = (str(row["scenario"]), int(row["coalition_size"]))
        grouped.setdefault(key, []).append(row)

    summary_rows: list[dict[str, Any]] = []
    for (scenario, coalition_size), rows in sorted(grouped.items()):
        def collect(metric: str) -> list[float]:
            return [float(row[metric]) for row in rows]

        edge_coverage = collect("global_undirected_edge_coverage")
        node_coverage = collect("matched_global_undirected_node_coverage")
        frontier_fraction = collect("frontier_undirected_edge_fraction")
        summary_rows.append(
            {
                "scenario": scenario,
                "coalition_size": coalition_size,
                "runs": len({str(row["run_dir"]) for row in rows}),
                "topics": len(rows),
                "topic_undirected_edge_coverage_mean": mean_or_zero(edge_coverage),
                "topic_undirected_edge_coverage_std": pstdev_or_zero(edge_coverage),
                "topic_undirected_edge_coverage_q25": quantile_or_zero(edge_coverage, 0.25),
                "topic_undirected_edge_coverage_median": quantile_or_zero(edge_coverage, 0.50),
                "topic_undirected_edge_coverage_q75": quantile_or_zero(edge_coverage, 0.75),
                "topic_undirected_node_coverage_mean": mean_or_zero(node_coverage),
                "topic_undirected_node_coverage_std": pstdev_or_zero(node_coverage),
                "topic_undirected_node_coverage_q25": quantile_or_zero(node_coverage, 0.25),
                "topic_undirected_node_coverage_median": quantile_or_zero(node_coverage, 0.50),
                "topic_undirected_node_coverage_q75": quantile_or_zero(node_coverage, 0.75),
                "topic_frontier_undirected_edge_fraction_mean": mean_or_zero(
                    frontier_fraction
                ),
                "topic_frontier_undirected_edge_fraction_std": pstdev_or_zero(
                    frontier_fraction
                ),
                "topic_truth_undirected_edges_mean": mean_or_zero(
                    collect("global_truth_undirected_edges")
                ),
                "topic_truth_nodes_mean": mean_or_zero(collect("global_truth_nodes")),
            }
        )
    return summary_rows


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


def build_markdown(run_rows: list[dict[str, Any]], summary_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Passive Coalition Mini-Campaign",
        "",
        "Validation set:",
        "- scenarios: degraded_baseline_r20_windowed_px_control_dout, degraded_scoring_r20_validated",
        "- seeds: 24680, 13579, 98765",
        "- coalition sizes: 1, 2, 4, 8",
        "- attacker selection: deterministic prefix [0..k-1]",
        "",
        "## Scenario Summary",
        "",
        "| Scenario | K | Runs | Frontier F1 | Frontier Recall | Frontier Jaccard | Global Edge Coverage | Frontier Fraction | Matched Node Coverage | QC |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in summary_rows:
        lines.append(
            "| {scenario} | {coalition_size} | {runs} | {frontier_f1_mean:.4f} ± {frontier_f1_std:.4f} | "
            "{frontier_recall_mean:.4f} ± {frontier_recall_std:.4f} | "
            "{frontier_undirected_jaccard_mean:.4f} ± {frontier_undirected_jaccard_std:.4f} | "
            "{global_edge_coverage_mean:.4f} ± {global_edge_coverage_std:.4f} | "
            "{frontier_edge_fraction_mean:.4f} ± {frontier_edge_fraction_std:.4f} | "
            "{matched_global_node_coverage_mean:.4f} ± {matched_global_node_coverage_std:.4f} | "
            "{qc_flagged_runs} |".format(**row)
        )

    qc_rows = [row for row in run_rows if row["qc_flags"]]
    lines.extend(["", "## Quality Control", ""])
    if not qc_rows:
        lines.append("No QC flags detected.")
    else:
        lines.append("| Scenario | K | Seed | Flags | Run Dir |")
        lines.append("| --- | ---: | ---: | --- | --- |")
        for row in qc_rows:
            lines.append(
                f"| {row['scenario']} | {row['coalition_size']} | {row['seed']} | {row['qc_flags']} | {row['run_dir']} |"
            )

    lines.extend(["", "## Scientific Check", ""])
    lines.append(
        "This mini-campaign checks whether a passive coalition improves frontier reconstruction "
        "and global mesh coverage as the number of observation points grows, while keeping the "
        "same conservative wire-only inference semantics as `passive_ego_mesh`."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    output_root = args.output_root.resolve()
    summary_dir = output_root / SUMMARY_DIRNAME
    run_dirs = collect_run_dirs(output_root)
    run_rows = [build_run_row(run_dir) for run_dir in run_dirs]
    summary_rows = aggregate_by_scenario_and_coalition(run_rows)
    topic_rows = [row for run_dir in run_dirs for row in build_topic_rows(run_dir)]
    topic_summary_rows = aggregate_topics_by_scenario_and_coalition(topic_rows)

    run_csv = summary_dir / "passive_coalition_run_metrics.csv"
    scenario_csv = summary_dir / "passive_coalition_scenario_summary.csv"
    topic_csv = summary_dir / "passive_coalition_topic_metrics.csv"
    topic_summary_csv = summary_dir / "passive_coalition_topic_summary.csv"

    write_csv(run_csv, run_rows)
    write_csv(scenario_csv, summary_rows)
    write_csv(topic_csv, topic_rows)
    write_csv(topic_summary_csv, topic_summary_rows)

    print(run_csv)
    print(scenario_csv)
    print(topic_csv)
    print(topic_summary_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
