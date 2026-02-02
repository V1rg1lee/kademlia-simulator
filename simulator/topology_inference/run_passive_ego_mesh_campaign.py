from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from simulator.topology_inference.evaluate_passive_ego_mesh import (
    evaluate_inference,
    extract_truth_ego_mesh,
    load_attack_metadata,
    load_inferred_topology,
    load_mesh_snapshots,
    resolve_last_snapshot_heartbeat_index,
    write_evaluation,
)
from simulator.topology_inference.run_passive_ego_mesh import (
    DEFAULT_OUTPUT_ROOT,
    run_single_experiment,
)


DEFAULT_SCENARIOS = [
    "degraded_baseline_r20_windowed_px_control_dout",
    "degraded_scoring_r20_validated",
]
DEFAULT_SEEDS = [24680, 13579, 98765]
DEFAULT_ATTACKER_INDEX = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a mini-campaign for passive_ego_mesh.")
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
        "--attacker-index",
        type=int,
        default=DEFAULT_ATTACKER_INDEX,
        help="Fixed attacker index for the whole campaign.",
    )
    parser.add_argument(
        "--late-join",
        action="store_true",
        help="Run passive ego-mesh with a delayed observer instead of a bootstrap observer.",
    )
    parser.add_argument(
        "--start-heartbeat",
        type=int,
        default=20,
        help="Heartbeat at which --late-join observers become active.",
    )
    parser.add_argument(
        "--late-join-topics",
        nargs="*",
        default=[],
        help="Optional topics joined immediately when a --late-join observer activates.",
    )
    return parser.parse_args()


def evaluate_run(run_dir: Path) -> dict[str, object]:
    attack_metadata = load_attack_metadata(run_dir)
    inferred_topology = load_inferred_topology(run_dir)
    snapshots_df = load_mesh_snapshots(run_dir)
    snapshot_heartbeat_index = resolve_last_snapshot_heartbeat_index(snapshots_df)
    attacker_id = str(attack_metadata["attacker_node_id"])
    truth_topology = extract_truth_ego_mesh(
        snapshots_df, attacker_id=attacker_id, snapshot_heartbeat_index=snapshot_heartbeat_index
    )
    evaluation = evaluate_inference(
        inferred_topology=inferred_topology,
        truth_topology=truth_topology,
        attack_metadata=attack_metadata,
        snapshot_heartbeat_index=snapshot_heartbeat_index,
    )
    write_evaluation(run_dir, evaluation)
    return evaluation


def inferred_topology_is_empty(inferred_topology: dict[str, object]) -> bool:
    topics = inferred_topology.get("topics", {})
    if not isinstance(topics, dict) or not topics:
        return True
    for topic_data in topics.values():
        if not isinstance(topic_data, dict):
            continue
        if topic_data.get("outgoing_neighbors") or topic_data.get("incoming_neighbors"):
            return False
    return True


def all_metrics_zero(evaluation: dict[str, object]) -> bool:
    aggregate = evaluation.get("aggregate", {})
    combined = aggregate.get("undirected_neighbors", {}) if isinstance(aggregate, dict) else {}
    jaccard = aggregate.get("undirected_neighbor_jaccard_macro", 0.0) if isinstance(aggregate, dict) else 0.0
    return (
        float(combined.get("precision", 0.0)) == 0.0
        and float(combined.get("recall", 0.0)) == 0.0
        and float(combined.get("f1", 0.0)) == 0.0
        and float(jaccard) == 0.0
    )


def format_qc_messages(run_dir: Path, inferred_topology: dict[str, object], evaluation: dict[str, object]) -> list[str]:
    messages: list[str] = []
    if inferred_topology_is_empty(inferred_topology):
        messages.append(f"[QC] inferred_topology.json is empty for {run_dir}")
    evaluated_topics = int(evaluation.get("aggregate", {}).get("evaluated_topics", 0))
    if evaluated_topics == 0:
        messages.append(f"[QC] no topic evaluated for {run_dir}")
    if all_metrics_zero(evaluation):
        messages.append(f"[QC] all aggregate metrics are zero for {run_dir}")
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

    manifest_rows: list[dict[str, object]] = []
    failures: list[str] = []

    scenarios = list(dict.fromkeys(args.scenarios))
    for scenario in scenarios:
        for seed in DEFAULT_SEEDS:
            print(f"[RUN] scenario={scenario} seed={seed} attacker_index={args.attacker_index}")
            try:
                run_dir = run_single_experiment(
                    scenario=scenario,
                    seed=seed,
                    attacker_index=args.attacker_index,
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
                        "attacker_index": args.attacker_index,
                        "late_join": args.late_join,
                        "start_heartbeat": args.start_heartbeat if args.late_join else None,
                        "late_join_topics": args.late_join_topics if args.late_join else [],
                        "run_dir": str(run_dir),
                        "evaluation_path": str(run_dir / "evaluation.json"),
                        "qc_flags": qc_messages,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                failure = f"[FAIL] scenario={scenario} seed={seed} attacker_index={args.attacker_index}: {exc}"
                print(failure)
                failures.append(failure)
                manifest_rows.append(
                    {
                        "scenario": scenario,
                        "seed": seed,
                        "attacker_index": args.attacker_index,
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
