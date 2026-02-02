from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate the px_flood inference attack.")
    parser.add_argument("--run-dir", required=True, type=Path, help="Run directory to evaluate.")
    parser.add_argument(
        "--skip-perturbation",
        action="store_true",
        help="Skip topology perturbation computation (faster, uses less memory).",
    )
    return parser.parse_args()


def load_attack_metadata(run_dir: Path) -> dict[str, Any]:
    with (run_dir / "attack_metadata.json").open("r", encoding="utf-8") as f:
        return json.load(f)


def load_inferred_topology(run_dir: Path) -> dict[str, Any]:
    with (run_dir / "inferred_topology.json").open("r", encoding="utf-8") as f:
        return json.load(f)


def load_wave_report(run_dir: Path) -> dict[str, Any]:
    with (run_dir / "wave_report.json").open("r", encoding="utf-8") as f:
        return json.load(f)


def load_mesh_snapshots(run_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(run_dir / "mesh_snapshots.csv")
    df = df[df["snapshot_kind"].astype(str) == "mesh_edge"].copy()
    df["heartbeat_index"] = df["heartbeat_index"].astype(int)
    df["node_id"] = df["node_id"].astype(str)
    df["peer_id"] = df["peer_id"].astype(str)
    df["topic"] = df["topic"].astype(str)
    return df


def write_evaluation(run_dir: Path, evaluation: dict[str, Any]) -> None:
    path = run_dir / "evaluation.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(evaluation, f, indent=2)
        f.write("\n")


def node_sort_key(value: str) -> tuple[int, Any, str]:
    try:
        return (0, int(value), str(value))
    except (TypeError, ValueError):
        return (1, str(value), str(value))


def mesh_neighbors_at(
    snapshots_df: pd.DataFrame, node_id: str, heartbeat: int, topic: str
) -> set[str]:
    """Return the set of mesh peers of node_id at the given heartbeat."""
    # Use the closest available heartbeat <= the requested one.
    available = snapshots_df["heartbeat_index"].unique()
    candidates = [h for h in available if h <= heartbeat]
    if not candidates:
        return set()
    effective_hb = max(candidates)
    at_hb = snapshots_df[
        (snapshots_df["heartbeat_index"] == effective_hb)
        & (snapshots_df["topic"] == topic)
    ]
    outgoing = set(at_hb[at_hb["node_id"] == node_id]["peer_id"].tolist())
    incoming = set(at_hb[at_hb["peer_id"] == node_id]["node_id"].tolist())
    return outgoing | incoming


def all_mesh_nodes_at(
    snapshots_df: pd.DataFrame, heartbeat: int, topic: str
) -> set[str]:
    available = snapshots_df["heartbeat_index"].unique()
    candidates = [h for h in available if h <= heartbeat]
    if not candidates:
        return set()
    effective_hb = max(candidates)
    at_hb = snapshots_df[
        (snapshots_df["heartbeat_index"] == effective_hb)
        & (snapshots_df["topic"] == topic)
    ]
    nodes: set[str] = set()
    nodes.update(at_hb["node_id"].tolist())
    nodes.update(at_hb["peer_id"].tolist())
    return nodes


def prf(predicted: set[Any], truth: set[Any]) -> dict[str, Any]:
    tp = len(predicted & truth)
    fp = len(predicted - truth)
    fn = len(truth - predicted)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "precision": precision, "recall": recall, "f1": f1}


def jaccard(predicted: set[Any], truth: set[Any]) -> float:
    union = predicted | truth
    return len(predicted & truth) / len(union) if union else 0.0


def _extract_honest_directed_edges(
    snapshots_df: pd.DataFrame, heartbeat: int, excluded_ids: set[str]
) -> set[tuple[str, str, str]]:
    """Directed (topic, src, dst) mesh edges at *heartbeat*, excluding attacker nodes."""
    rows = snapshots_df[
        (snapshots_df["heartbeat_index"] == heartbeat)
        & (snapshots_df["snapshot_kind"] == "mesh_edge")
    ]
    return {
        (str(t), str(s), str(d))
        for t, s, d in zip(rows["topic"], rows["node_id"], rows["peer_id"], strict=False)
        if str(s) not in excluded_ids and str(d) not in excluded_ids
    }


def _jaccard_distance(
    left: set[tuple[str, str, str]], right: set[tuple[str, str, str]]
) -> float:
    union = left | right
    return 0.0 if not union else 1.0 - len(left & right) / len(union)


def compute_topology_perturbation(
    *,
    snapshots_df: pd.DataFrame,
    attack_metadata: dict[str, Any],
    wave_report: dict[str, Any],
) -> dict[str, Any]:
    """
    Measure how much the PX flood attack perturbed the honest mesh.

    Method
    ------
    Reference state: the honest subgraph (attacker nodes excluded) at the
    heartbeat just before the first wave activates.

    Active states: the same honest subgraph at every heartbeat captured during
    and immediately after all waves.

    Perturbation = Jaccard distance between reference edges and active edges.
    A value of 0 means the attack left the honest mesh completely unchanged;
    a value of 1 means complete replacement.
    """
    attacker_ids = set(map(str, attack_metadata.get("attacker_node_ids", [])))
    waves = wave_report.get("waves", [])

    if not waves:
        return {"status": "not_calculated", "reason": "no waves in wave_report"}

    first_wave_hb = int(min(w["wave_start_heartbeat"] for w in waves))
    last_collect_hb = int(max(w["collect_heartbeat"] for w in waves))

    available_hbs = sorted(int(h) for h in snapshots_df["heartbeat_index"].unique())

    # Reference: last snapshot strictly before the first wave
    ref_candidates = [h for h in available_hbs if h < first_wave_hb]
    if not ref_candidates:
        return {
            "status": "not_calculated",
            "reason": f"no snapshot available before first wave heartbeat {first_wave_hb}",
        }
    reference_hb = max(ref_candidates)
    reference_edges = _extract_honest_directed_edges(snapshots_df, reference_hb, attacker_ids)

    # Attack window: snapshots from first wave through last collect + small buffer
    buffer = 5
    attack_hbs = [h for h in available_hbs if first_wave_hb <= h <= last_collect_hb + buffer]
    if not attack_hbs:
        return {
            "status": "not_calculated",
            "reason": "no snapshots within attack window",
        }

    drift_by_hb: dict[int, float] = {}
    for hb in attack_hbs:
        active_edges = _extract_honest_directed_edges(snapshots_df, hb, attacker_ids)
        drift_by_hb[hb] = _jaccard_distance(reference_edges, active_edges)

    final_hb = max(drift_by_hb)
    mean_drift = sum(drift_by_hb.values()) / len(drift_by_hb)

    return {
        "status": "calculated",
        "policy": "honest_subgraph_directed_jaccard_distance",
        "edge_semantics": "directed (topic, src, dst) mesh edges with attacker nodes excluded",
        "excluded_attacker_node_ids": sorted(attacker_ids, key=node_sort_key),
        "reference_heartbeat": reference_hb,
        "reference_honest_edge_count": len(reference_edges),
        "first_attack_heartbeat": first_wave_hb,
        "last_attack_heartbeat": last_collect_hb,
        "compared_heartbeat_count": len(drift_by_hb),
        "final_topology_drift": drift_by_hb[final_hb],
        "mean_topology_drift_during_attack": mean_drift,
        "drift_by_heartbeat": {str(h): v for h, v in sorted(drift_by_hb.items())},
    }


def evaluate_per_target(
    *,
    inferred_topology: dict[str, Any],
    wave_report: dict[str, Any],
    snapshots_df: pd.DataFrame,
    topic: str,
) -> dict[str, Any]:
    """
    For each flooded target, compare the PX-inferred neighborhood against the
    ground-truth mesh of that target at the wave's collect_heartbeat.

    The truth is "what was T's actual mesh at the moment we collected the PX".
    """
    per_target_inferred: dict[str, Any] = inferred_topology.get("per_target_inferred_neighbors", {})
    waves: list[dict[str, Any]] = wave_report.get("waves", [])

    # Build a mapping target_id → collect_heartbeat from the wave log.
    collect_hb_by_target: dict[str, int] = {
        w["target_node_id"]: int(w["collect_heartbeat"]) for w in waves
    }

    per_target_results: dict[str, Any] = {}
    tp_total = fp_total = fn_total = 0
    jaccards: list[float] = []
    total_predicted = 0
    total_true = 0

    for target_id, target_data in per_target_inferred.items():
        predicted_neighbors = set(map(str, target_data.get("inferred_mesh_neighbors", [])))
        collect_hb = collect_hb_by_target.get(target_id, -1)
        truth_neighbors = mesh_neighbors_at(snapshots_df, target_id, collect_hb, topic)

        metrics = prf(predicted_neighbors, truth_neighbors)
        j = jaccard(predicted_neighbors, truth_neighbors)

        tp_total += metrics["tp"]
        fp_total += metrics["fp"]
        fn_total += metrics["fn"]
        jaccards.append(j)
        total_predicted += len(predicted_neighbors)
        total_true += len(truth_neighbors)

        per_target_results[target_id] = {
            "collect_heartbeat": collect_hb,
            "predicted_neighbors": sorted(predicted_neighbors, key=node_sort_key),
            "true_neighbors": sorted(truth_neighbors, key=node_sort_key),
            "predicted_count": len(predicted_neighbors),
            "true_count": len(truth_neighbors),
            "metrics": {**metrics, "jaccard": j},
        }

    precision_agg = tp_total / (tp_total + fp_total) if (tp_total + fp_total) else 0.0
    recall_agg = tp_total / (tp_total + fn_total) if (tp_total + fn_total) else 0.0
    f1_agg = (
        (2.0 * precision_agg * recall_agg / (precision_agg + recall_agg))
        if (precision_agg + recall_agg)
        else 0.0
    )
    return {
        "per_target": per_target_results,
        "aggregate": {
            "targets_evaluated": len(per_target_results),
            "total_predicted_neighbors": total_predicted,
            "total_true_neighbors": total_true,
            "micro_precision": precision_agg,
            "micro_recall": recall_agg,
            "micro_f1": f1_agg,
            "macro_jaccard": sum(jaccards) / len(jaccards) if jaccards else 0.0,
            "tp": tp_total,
            "fp": fp_total,
            "fn": fn_total,
        },
    }


def evaluate_discovery_coverage(
    *,
    inferred_topology: dict[str, Any],
    snapshots_df: pd.DataFrame,
    attack_metadata: dict[str, Any],
    topic: str,
) -> dict[str, Any]:
    """
    How many unique nodes did we discover relative to the total mesh?
    Also: are the discovered nodes actually in the mesh?
    """
    attacker_ids = set(map(str, attack_metadata.get("attacker_node_ids", [])))
    flooded_targets = set(
        map(str, inferred_topology.get("per_target_inferred_neighbors", {}).keys())
    )

    discovered_nodes = set(map(str, inferred_topology.get("all_discovered_nodes", [])))
    discovered_non_attacker = discovered_nodes - attacker_ids

    # Get final snapshot truth
    final_hb = int(snapshots_df["heartbeat_index"].max()) if not snapshots_df.empty else 0
    true_mesh_nodes = all_mesh_nodes_at(snapshots_df, final_hb, topic) - attacker_ids

    true_discovered = discovered_non_attacker & true_mesh_nodes
    false_discovered = discovered_non_attacker - true_mesh_nodes

    coverage = len(true_discovered) / len(true_mesh_nodes) if true_mesh_nodes else 0.0
    precision = len(true_discovered) / len(discovered_non_attacker) if discovered_non_attacker else 0.0

    return {
        "final_heartbeat": final_hb,
        "true_mesh_size": len(true_mesh_nodes),
        "targets_flooded": len(flooded_targets),
        "unique_nodes_discovered": len(discovered_non_attacker),
        "true_positive_discoveries": len(true_discovered),
        "false_positive_discoveries": len(false_discovered),
        "discovery_coverage": coverage,
        "discovery_precision": precision,
    }


def compute_degree_bias(
    *,
    snapshots_df: pd.DataFrame,
    inferred_topology: dict[str, Any],
    attack_metadata: dict[str, Any],
    topic: str,
    high_degree_percentile: float = 0.20,
) -> dict[str, Any]:
    """
    Measure whether the PX flood attack preferentially discovers high-degree nodes.

    Method
    ------
    1. Compute each honest node's undirected mesh degree at the final snapshot.
    2. Define "high-degree" as the top `high_degree_percentile` fraction of nodes
       by degree (ties broken by node id).
    3. Compare:
         high_degree_recall = |discovered ∩ top-k%| / |top-k%|
         low_degree_recall  = |discovered ∩ bottom-(1-k)%| / |bottom-(1-k)%|
         discovery_lift     = high_degree_recall / discovery_coverage
                              (> 1 means the attack is biased toward hubs)
    """
    attacker_ids = set(map(str, attack_metadata.get("attacker_node_ids", [])))
    final_hb = int(snapshots_df["heartbeat_index"].max()) if not snapshots_df.empty else 0

    at_hb = snapshots_df[
        (snapshots_df["heartbeat_index"] == final_hb)
        & (snapshots_df["topic"] == topic)
    ]
    honest_rows = at_hb[
        ~at_hb["node_id"].isin(attacker_ids) & ~at_hb["peer_id"].isin(attacker_ids)
    ]

    # Undirected degree: each row (A, B) contributes 1 to both A and B.
    degree: dict[str, int] = {}
    for _, row in honest_rows.iterrows():
        src, dst = str(row["node_id"]), str(row["peer_id"])
        degree[src] = degree.get(src, 0) + 1
        degree[dst] = degree.get(dst, 0) + 1

    if not degree:
        return {"status": "not_calculated", "reason": "no honest mesh edges at final snapshot"}

    # All honest mesh nodes (even zero-degree ones need to be represented).
    all_honest_nodes = set(at_hb["node_id"].tolist()) | set(at_hb["peer_id"].tolist())
    all_honest_nodes -= attacker_ids
    for n in all_honest_nodes:
        if n not in degree:
            degree[n] = 0

    sorted_by_degree = sorted(degree.keys(), key=lambda n: (-degree[n], n))
    n_total = len(sorted_by_degree)
    threshold = max(1, round(n_total * high_degree_percentile))

    high_degree_nodes = set(sorted_by_degree[:threshold])
    low_degree_nodes = set(sorted_by_degree[threshold:])

    discovered_nodes = set(map(str, inferred_topology.get("all_discovered_nodes", [])))
    discovered_honest = discovered_nodes - attacker_ids

    high_discovered = discovered_honest & high_degree_nodes
    low_discovered = discovered_honest & low_degree_nodes

    high_recall = len(high_discovered) / len(high_degree_nodes) if high_degree_nodes else 0.0
    low_recall = len(low_discovered) / len(low_degree_nodes) if low_degree_nodes else 0.0
    overall_coverage = len(discovered_honest & all_honest_nodes) / n_total if n_total else 0.0
    lift = high_recall / overall_coverage if overall_coverage > 0 else None
    high_share = len(high_discovered) / len(discovered_honest) if discovered_honest else 0.0

    return {
        "status": "calculated",
        "high_degree_percentile": high_degree_percentile,
        "total_honest_nodes": n_total,
        "high_degree_node_count": len(high_degree_nodes),
        "low_degree_node_count": len(low_degree_nodes),
        "high_degree_threshold_degree": degree[sorted_by_degree[threshold - 1]],
        "high_degree_recall": high_recall,
        "low_degree_recall": low_recall,
        "discovery_lift": lift,
        "high_degree_share_in_discovered": high_share,
        "expected_high_degree_share": high_degree_percentile,
        "reference_heartbeat": final_hb,
    }


def evaluate_inference(
    *,
    inferred_topology: dict[str, Any],
    wave_report: dict[str, Any],
    snapshots_df: pd.DataFrame,
    attack_metadata: dict[str, Any],
    compute_perturbation: bool = True,
) -> dict[str, Any]:
    topic = str(attack_metadata.get("topic", ""))
    per_target = evaluate_per_target(
        inferred_topology=inferred_topology,
        wave_report=wave_report,
        snapshots_df=snapshots_df,
        topic=topic,
    )
    discovery = evaluate_discovery_coverage(
        inferred_topology=inferred_topology,
        snapshots_df=snapshots_df,
        attack_metadata=attack_metadata,
        topic=topic,
    )
    perturbation: dict[str, Any] = {"status": "skipped"}
    if compute_perturbation:
        perturbation = compute_topology_perturbation(
            snapshots_df=snapshots_df,
            attack_metadata=attack_metadata,
            wave_report=wave_report,
        )
    degree_bias = compute_degree_bias(
        snapshots_df=snapshots_df,
        inferred_topology=inferred_topology,
        attack_metadata=attack_metadata,
        topic=topic,
    )
    return {
        "attack_name": "px_flood",
        "inference_complete": bool(attack_metadata.get("inference_complete", False)),
        "waves_completed": int(attack_metadata.get("waves_completed", 0)),
        "coalition_size": int(attack_metadata.get("coalition_size", 0)),
        "wave_depth": int(attack_metadata.get("wave_depth", 1)),
        "per_target_neighborhood": per_target,
        "discovery_coverage": discovery,
        "topology_perturbation": perturbation,
        "degree_bias": degree_bias,
    }


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()

    attack_metadata = load_attack_metadata(run_dir)
    inferred_topology = load_inferred_topology(run_dir)
    wave_report = load_wave_report(run_dir)
    snapshots_df = load_mesh_snapshots(run_dir)

    evaluation = evaluate_inference(
        inferred_topology=inferred_topology,
        wave_report=wave_report,
        snapshots_df=snapshots_df,
        attack_metadata=attack_metadata,
        compute_perturbation=not args.skip_perturbation,
    )
    write_evaluation(run_dir, evaluation)

    agg = evaluation["per_target_neighborhood"]["aggregate"]
    disc = evaluation["discovery_coverage"]
    pert = evaluation["topology_perturbation"]
    print(
        f"[px_flood eval] targets={agg['targets_evaluated']} "
        f"micro_precision={agg['micro_precision']:.3f} "
        f"micro_recall={agg['micro_recall']:.3f} "
        f"micro_f1={agg['micro_f1']:.3f} "
        f"macro_jaccard={agg['macro_jaccard']:.3f}"
    )
    print(
        f"[px_flood eval] discovered={disc['unique_nodes_discovered']} "
        f"true_mesh_size={disc['true_mesh_size']} "
        f"coverage={disc['discovery_coverage']:.3f} "
        f"discovery_precision={disc['discovery_precision']:.3f}"
    )
    if pert.get("status") == "calculated":
        print(
            f"[px_flood eval] topology_drift final={pert['final_topology_drift']:.4f} "
            f"mean={pert['mean_topology_drift_during_attack']:.4f} "
            f"(reference hb={pert['reference_heartbeat']}, "
            f"{pert['compared_heartbeat_count']} compared heartbeats)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
