from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate the passive_ego_mesh prototype.")
    parser.add_argument("--run-dir", required=True, type=Path, help="Run directory to evaluate.")
    return parser.parse_args()


def load_attack_metadata(run_dir: Path) -> dict[str, Any]:
    with (run_dir / "attack_metadata.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_inferred_topology(run_dir: Path) -> dict[str, Any]:
    with (run_dir / "inferred_topology.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_mesh_snapshots(run_dir: Path) -> pd.DataFrame:
    snapshots_df = pd.read_csv(run_dir / "mesh_snapshots.csv")
    required_columns = {"heartbeat_index", "snapshot_kind", "node_id", "peer_id", "topic"}
    missing = required_columns - set(snapshots_df.columns)
    if missing:
        raise ValueError(f"mesh_snapshots.csv is missing required columns: {', '.join(sorted(missing))}")
    validate_mesh_snapshots(snapshots_df)
    snapshots_df = snapshots_df[snapshots_df["snapshot_kind"].astype(str) == "mesh_edge"].copy()
    snapshots_df["heartbeat_index"] = snapshots_df["heartbeat_index"].astype(int)
    snapshots_df["node_id"] = snapshots_df["node_id"].astype(str)
    snapshots_df["peer_id"] = snapshots_df["peer_id"].astype(str)
    snapshots_df["topic"] = snapshots_df["topic"].astype(str)
    return snapshots_df


def validate_mesh_snapshots(snapshots_df: pd.DataFrame) -> None:
    snapshots_df = snapshots_df.copy()
    snapshots_df["snapshot_kind"] = snapshots_df["snapshot_kind"].astype(str)
    snapshots_df["heartbeat_index"] = snapshots_df["heartbeat_index"].astype(int)

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


def resolve_last_snapshot_heartbeat_index(snapshots_df: pd.DataFrame) -> int:
    if snapshots_df.empty:
        raise ValueError("mesh_snapshots.csv is empty")
    return int(snapshots_df["heartbeat_index"].max())


def extract_truth_ego_mesh(
    snapshots_df: pd.DataFrame,
    *,
    attacker_id: str,
    snapshot_heartbeat_index: int,
) -> dict[str, Any]:
    at_last = snapshots_df[snapshots_df["heartbeat_index"] == snapshot_heartbeat_index].copy()

    truth_topics: dict[str, Any] = {}
    topics = sorted(set(at_last["topic"].dropna().astype(str).tolist()))
    for topic in topics:
        topic_df = at_last[at_last["topic"].astype(str) == topic]
        outgoing = sorted(set(topic_df.loc[topic_df["node_id"] == attacker_id, "peer_id"].tolist()))
        incoming = sorted(set(topic_df.loc[topic_df["peer_id"] == attacker_id, "node_id"].tolist()))
        if not outgoing and not incoming:
            continue
        undirected = sorted(set(outgoing) | set(incoming))
        truth_topics[topic] = {
            "outgoing_neighbors": outgoing,
            "incoming_neighbors": incoming,
            "undirected_neighbors": undirected,
        }

    return {
        "target_layer": "mesh",
        "attacker_node_id": attacker_id,
        "snapshot_heartbeat_index": snapshot_heartbeat_index,
        "topics": truth_topics,
    }


def prf(predicted: set[tuple[str, str]], truth: set[tuple[str, str]]) -> dict[str, Any]:
    tp = len(predicted & truth)
    fp = len(predicted - truth)
    fn = len(truth - predicted)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def canonical_undirected_edge(left: str, right: str) -> tuple[str, str]:
    left = str(left)
    right = str(right)
    return (left, right) if left <= right else (right, left)


def jaccard(predicted: set[str], truth: set[str]) -> float:
    union = predicted | truth
    if not union:
        return 0.0
    return len(predicted & truth) / len(union)


def evaluate_inference(
    *,
    inferred_topology: dict[str, Any],
    truth_topology: dict[str, Any],
    attack_metadata: dict[str, Any],
    snapshot_heartbeat_index: int,
) -> dict[str, Any]:
    attacker_id = str(attack_metadata["attacker_node_id"])
    predicted_topics = inferred_topology.get("topics", {})
    truth_topics = truth_topology.get("topics", {})
    all_topics = sorted(set(predicted_topics.keys()) | set(truth_topics.keys()))

    topics_eval: dict[str, Any] = {}
    directed_total_tp = directed_total_fp = directed_total_fn = 0
    undirected_total_tp = undirected_total_fp = undirected_total_fn = 0
    jaccards: list[float] = []

    for topic in all_topics:
        predicted_topic = predicted_topics.get(topic, {})
        truth_topic = truth_topics.get(topic, {})

        outgoing_pred_neighbors = set(map(str, predicted_topic.get("outgoing_neighbors", [])))
        incoming_pred_neighbors = set(map(str, predicted_topic.get("incoming_neighbors", [])))
        undirected_pred_neighbors = set(map(str, predicted_topic.get("undirected_neighbors", [])))

        outgoing_truth_neighbors = set(map(str, truth_topic.get("outgoing_neighbors", [])))
        incoming_truth_neighbors = set(map(str, truth_topic.get("incoming_neighbors", [])))
        undirected_truth_neighbors = set(map(str, truth_topic.get("undirected_neighbors", [])))

        if (
            not outgoing_pred_neighbors
            and not incoming_pred_neighbors
            and not outgoing_truth_neighbors
            and not incoming_truth_neighbors
        ):
            continue

        outgoing_pred_edges = {(attacker_id, peer) for peer in outgoing_pred_neighbors}
        outgoing_truth_edges = {(attacker_id, peer) for peer in outgoing_truth_neighbors}
        incoming_pred_edges = {(peer, attacker_id) for peer in incoming_pred_neighbors}
        incoming_truth_edges = {(peer, attacker_id) for peer in incoming_truth_neighbors}
        undirected_pred_edges = {
            canonical_undirected_edge(attacker_id, peer) for peer in undirected_pred_neighbors
        }
        undirected_truth_edges = {
            canonical_undirected_edge(attacker_id, peer) for peer in undirected_truth_neighbors
        }

        outgoing_metrics = prf(outgoing_pred_edges, outgoing_truth_edges)
        incoming_metrics = prf(incoming_pred_edges, incoming_truth_edges)
        combined_metrics = prf(
            outgoing_pred_edges | incoming_pred_edges, outgoing_truth_edges | incoming_truth_edges
        )
        undirected_metrics = prf(undirected_pred_edges, undirected_truth_edges)
        undirected_jaccard = jaccard(undirected_pred_neighbors, undirected_truth_neighbors)

        directed_total_tp += combined_metrics["tp"]
        directed_total_fp += combined_metrics["fp"]
        directed_total_fn += combined_metrics["fn"]
        undirected_total_tp += undirected_metrics["tp"]
        undirected_total_fp += undirected_metrics["fp"]
        undirected_total_fn += undirected_metrics["fn"]
        jaccards.append(undirected_jaccard)

        topics_eval[topic] = {
            "ground_truth": {
                "outgoing_neighbors": sorted(outgoing_truth_neighbors),
                "incoming_neighbors": sorted(incoming_truth_neighbors),
                "undirected_neighbors": sorted(undirected_truth_neighbors),
            },
            "prediction": {
                "outgoing_neighbors": sorted(outgoing_pred_neighbors),
                "incoming_neighbors": sorted(incoming_pred_neighbors),
                "undirected_neighbors": sorted(undirected_pred_neighbors),
            },
            "metrics": {
                "directed_outgoing": outgoing_metrics,
                "directed_incoming": incoming_metrics,
                "directed_combined": combined_metrics,
                "undirected_neighbors": undirected_metrics,
                "undirected_neighbor_jaccard": undirected_jaccard,
            },
        }

    directed_precision = (
        directed_total_tp / (directed_total_tp + directed_total_fp)
        if (directed_total_tp + directed_total_fp)
        else 0.0
    )
    directed_recall = (
        directed_total_tp / (directed_total_tp + directed_total_fn)
        if (directed_total_tp + directed_total_fn)
        else 0.0
    )
    directed_f1 = (
        2.0 * directed_precision * directed_recall / (directed_precision + directed_recall)
        if (directed_precision + directed_recall)
        else 0.0
    )
    undirected_precision = (
        undirected_total_tp / (undirected_total_tp + undirected_total_fp)
        if (undirected_total_tp + undirected_total_fp)
        else 0.0
    )
    undirected_recall = (
        undirected_total_tp / (undirected_total_tp + undirected_total_fn)
        if (undirected_total_tp + undirected_total_fn)
        else 0.0
    )
    undirected_f1 = (
        2.0 * undirected_precision * undirected_recall / (undirected_precision + undirected_recall)
        if (undirected_precision + undirected_recall)
        else 0.0
    )
    macro_jaccard = sum(jaccards) / len(jaccards) if jaccards else 0.0

    return {
        "attack_name": attack_metadata.get("attack_name"),
        "observability_profile": attack_metadata.get("observability_profile"),
        "target_layer": attack_metadata.get("target_layer"),
        "scenario": attack_metadata.get("scenario"),
        "seed": attack_metadata.get("seed"),
        "attacker_index": attack_metadata.get("attacker_index"),
        "attacker_node_id": attacker_id,
        "warmup_end_heartbeat": attack_metadata.get("warmup_end_heartbeat"),
        "inference_start_heartbeat": attack_metadata.get("inference_start_heartbeat"),
        "observation_policy_after_heartbeat_only": attack_metadata.get(
            "observation_policy_after_heartbeat_only", False
        ),
        "first_observed_heartbeat": attack_metadata.get("first_observed_heartbeat"),
        "first_evaluated_heartbeat": snapshot_heartbeat_index,
        "truth_target": {
            "policy": attack_metadata.get("truth_target_policy", "final_heartbeat_snapshot"),
            "resolved_snapshot_heartbeat_index": snapshot_heartbeat_index,
        },
        "topics": topics_eval,
        "aggregate": {
            "evaluated_topics": len(topics_eval),
            "primary_metric": "undirected_neighbors",
            "edge_semantics": (
                "passive_ego_mesh reconstructs the attacker's local mesh neighbor set; "
                "GRAFT direction is recorded as evidence but is not treated as a final directed edge"
            ),
            "undirected_neighbors": {
                "tp": undirected_total_tp,
                "fp": undirected_total_fp,
                "fn": undirected_total_fn,
                "precision": undirected_precision,
                "recall": undirected_recall,
                "f1": undirected_f1,
            },
            "directed_combined": {
                "tp": directed_total_tp,
                "fp": directed_total_fp,
                "fn": directed_total_fn,
                "precision": directed_precision,
                "recall": directed_recall,
                "f1": directed_f1,
                "interpretation": (
                    "diagnostic only: direction follows observed GRAFT direction, "
                    "not the undirected ego-mesh reconstruction target"
                ),
            },
            "undirected_neighbor_jaccard_macro": macro_jaccard,
        },
    }


def write_evaluation(run_dir: Path, evaluation: dict[str, Any]) -> None:
    with (run_dir / "evaluation.json").open("w", encoding="utf-8") as handle:
        json.dump(evaluation, handle, indent=2, sort_keys=False)
        handle.write("\n")


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
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
    print(run_dir / "evaluation.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
