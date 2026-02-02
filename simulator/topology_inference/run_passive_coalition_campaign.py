from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from simulator.topology_inference.evaluate_passive_coalition import (
    evaluate_inference,
    extract_attacker_frontier_truth,
    extract_global_truth_mesh,
    load_attack_metadata,
    load_inferred_topology,
    load_mesh_snapshots,
    resolve_last_snapshot_heartbeat_index,
    write_evaluation,
)
from simulator.topology_inference.run_passive_coalition import (
    DEFAULT_OUTPUT_ROOT,
    run_single_experiment,
)


DEFAULT_SCENARIOS = [
    "degraded_baseline_r20_windowed_px_control_dout",
    "degraded_scoring_r20_validated",
]
DEFAULT_SEEDS = [24680, 13579, 98765]
DEFAULT_COALITION_SIZES = [1, 2, 4, 8]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a mini-campaign for passive_coalition.")
    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=DEFAULT_SCENARIOS,
        help="Scenario names without the gossip_static_ prefix.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Root directory for experiment outputs.",
    )
    parser.add_argument(
        "--coalition-sizes",
        nargs="+",
        type=int,
        default=DEFAULT_COALITION_SIZES,
        help="Coalition sizes. Attackers are selected deterministically as [0..k-1].",
    )
    parser.add_argument(
        "--late-join",
        action="store_true",
        help="Run passive coalition with delayed observers instead of bootstrap observers.",
    )
    parser.add_argument(
        "--start-heartbeat",
        type=int,
        default=20,
        help="Heartbeat at which --late-join coalition observers become active.",
    )
    parser.add_argument(
        "--late-join-topics",
        nargs="*",
        default=[],
        help="Optional topics joined immediately when a --late-join coalition activates.",
    )
    return parser.parse_args()


def evaluate_run(run_dir: Path) -> dict[str, object]:
    attack_metadata = load_attack_metadata(run_dir)
    inferred_topology = load_inferred_topology(run_dir)
    snapshots_df = load_mesh_snapshots(run_dir)
    snapshot_heartbeat_index = resolve_last_snapshot_heartbeat_index(snapshots_df)
    global_truth = extract_global_truth_mesh(
        snapshots_df, snapshot_heartbeat_index=snapshot_heartbeat_index
    )
    frontier_truth = extract_attacker_frontier_truth(
        global_truth,
        attacker_node_ids=[str(node_id) for node_id in attack_metadata.get("attacker_node_ids", [])],
    )
    evaluation = evaluate_inference(
        inferred_topology=inferred_topology,
        global_truth=global_truth,
        frontier_truth=frontier_truth,
        attack_metadata=attack_metadata,
        snapshot_heartbeat_index=snapshot_heartbeat_index,
    )
    write_evaluation(run_dir, evaluation)
    return evaluation


def inferred_topology_is_empty(inferred_topology: dict[str, object]) -> bool:
    coalition_topics = inferred_topology.get("coalition_union", {}).get("topics", {})
    if not isinstance(coalition_topics, dict) or not coalition_topics:
        return True
    for topic_data in coalition_topics.values():
        if not isinstance(topic_data, dict):
            continue
        if topic_data.get("directed_edges") or topic_data.get("undirected_edges"):
            return False
    return True


def all_metrics_zero(evaluation: dict[str, object]) -> bool:
    coalition = evaluation.get("coalition_union", {}).get("aggregate", {})
    coverage = evaluation.get("global_coverage", {}).get("aggregate", {})
    return (
        float(
            coalition.get(
                "frontier_undirected_precision",
                coalition.get("frontier_directed_precision", 0.0),
            )
        )
        == 0.0
        and float(
            coalition.get(
                "frontier_undirected_recall",
                coalition.get("frontier_directed_recall", 0.0),
            )
        )
        == 0.0
        and float(
            coalition.get("frontier_undirected_f1", coalition.get("frontier_directed_f1", 0.0))
        )
        == 0.0
        and float(coalition.get("frontier_undirected_jaccard", 0.0)) == 0.0
        and float(
            coverage.get(
                "global_undirected_edge_coverage", coverage.get("global_edge_coverage", 0.0)
            )
        )
        == 0.0
    )


def format_qc_messages(
    run_dir: Path, inferred_topology: dict[str, object], evaluation: dict[str, object]
) -> list[str]:
    messages: list[str] = []
    if inferred_topology_is_empty(inferred_topology):
        messages.append(f"[QC] inferred_topology.json is empty for {run_dir}")
    evaluated_topics = int(evaluation.get("coalition_union", {}).get("aggregate", {}).get("evaluated_topics", 0))
    if evaluated_topics == 0:
        messages.append(f"[QC] no coalition topic evaluated for {run_dir}")
    if all_metrics_zero(evaluation):
        messages.append(f"[QC] all coalition aggregate metrics are zero for {run_dir}")
    return messages


def write_campaign_manifest(output_root: Path, rows: list[dict[str, object]]) -> Path:
    manifest_path = output_root / "campaign_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2, sort_keys=False)
        handle.write("\n")
    return manifest_path


def main() -> int:
    args = parse_args()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    coalition_sizes = sorted(dict.fromkeys(args.coalition_sizes))
    manifest_rows: list[dict[str, object]] = []
    failures: list[str] = []

    scenarios = list(dict.fromkeys(args.scenarios))
    for scenario in scenarios:
        for coalition_size in coalition_sizes:
            attacker_indices = list(range(coalition_size))
            for seed in DEFAULT_SEEDS:
                print(
                    f"[RUN] scenario={scenario} seed={seed} coalition_size={coalition_size} "
                    f"attackers={attacker_indices}"
                )
                try:
                    run_dir = run_single_experiment(
                        scenario=scenario,
                        seed=seed,
                        attacker_indices=attacker_indices,
                        output_root=output_root,
                        late_join=args.late_join,
                        start_heartbeat=args.start_heartbeat,
                        late_join_topics=args.late_join_topics,
                    )
                    inferred_topology = load_inferred_topology(run_dir)
                    evaluation = evaluate_run(run_dir)
                    qc_messages = format_qc_messages(run_dir, inferred_topology, evaluation)
                    for message in qc_messages:
                        print(message)
                    if not qc_messages:
                        print(f"[OK] {run_dir}")

                    manifest_rows.append(
                        {
                            "scenario": scenario,
                            "seed": seed,
                            "coalition_size": coalition_size,
                            "attacker_indices": attacker_indices,
                            "late_join": args.late_join,
                            "start_heartbeat": args.start_heartbeat if args.late_join else None,
                            "late_join_topics": args.late_join_topics if args.late_join else [],
                            "run_dir": str(run_dir),
                            "evaluation_path": str(run_dir / "evaluation.json"),
                            "qc_flags": qc_messages,
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    failure = (
                        f"[FAIL] scenario={scenario} seed={seed} coalition_size={coalition_size} "
                        f"attackers={attacker_indices}: {exc}"
                    )
                    print(failure)
                    failures.append(failure)
                    manifest_rows.append(
                        {
                            "scenario": scenario,
                            "seed": seed,
                            "coalition_size": coalition_size,
                            "attacker_indices": attacker_indices,
                            "late_join": args.late_join,
                            "start_heartbeat": args.start_heartbeat if args.late_join else None,
                            "late_join_topics": args.late_join_topics if args.late_join else [],
                            "run_dir": None,
                            "evaluation_path": None,
                            "qc_flags": ["RUN_FAILED"],
                            "error": str(exc),
                        }
                    )

    manifest_path = write_campaign_manifest(output_root, manifest_rows)
    print(f"[DONE] campaign manifest written to {manifest_path}")

    if failures:
        print("[DONE] campaign finished with failures")
        return 1

    print("[DONE] campaign finished successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
