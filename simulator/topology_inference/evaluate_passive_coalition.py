from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate the passive_coalition prototype.")
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


def node_sort_key(value: str) -> tuple[int, Any, str]:
    try:
        return (0, int(value), str(value))
    except (TypeError, ValueError):
        return (1, str(value), str(value))


def canonical_undirected_edge(left: str, right: str) -> tuple[str, str]:
    left_str = str(left)
    right_str = str(right)
    if node_sort_key(left_str) <= node_sort_key(right_str):
        return (left_str, right_str)
    return (right_str, left_str)


def sort_nodes(nodes: set[str]) -> list[str]:
    return sorted({str(node) for node in nodes}, key=node_sort_key)


def sort_directed_edges(edges: set[tuple[str, str]]) -> list[list[str]]:
    return [
        [src, dst]
        for src, dst in sorted(
            {(str(src), str(dst)) for src, dst in edges},
            key=lambda edge: (node_sort_key(edge[0]), node_sort_key(edge[1])),
        )
    ]


def sort_undirected_edges(edges: set[tuple[str, str]]) -> list[list[str]]:
    canonical = {canonical_undirected_edge(src, dst) for src, dst in edges}
    return [
        [src, dst]
        for src, dst in sorted(
            canonical,
            key=lambda edge: (node_sort_key(edge[0]), node_sort_key(edge[1])),
        )
    ]


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


def jaccard(predicted: set[Any], truth: set[Any]) -> float:
    union = predicted | truth
    if not union:
        return 0.0
    return len(predicted & truth) / len(union)


def extract_global_truth_mesh(
    snapshots_df: pd.DataFrame, *, snapshot_heartbeat_index: int
) -> dict[str, Any]:
    at_last = snapshots_df[snapshots_df["heartbeat_index"] == snapshot_heartbeat_index].copy()

    topics: dict[str, dict[str, Any]] = {}
    for topic in sorted(set(at_last["topic"].dropna().tolist())):
        topic_df = at_last[at_last["topic"] == topic]
        directed_edges = {
            (str(node_id), str(peer_id))
            for node_id, peer_id in zip(topic_df["node_id"], topic_df["peer_id"], strict=False)
            if str(node_id) and str(peer_id)
        }
        if not directed_edges:
            continue
        undirected_edges = {canonical_undirected_edge(src, dst) for src, dst in directed_edges}
        topic_nodes = {node for edge in directed_edges for node in edge}
        topics[topic] = {
            "directed_edges": directed_edges,
            "undirected_edges": undirected_edges,
            "nodes": topic_nodes,
        }

    global_edges = {
        (topic, src, dst)
        for topic, topic_data in topics.items()
        for src, dst in topic_data["directed_edges"]
    }
    global_undirected_edges = {
        (topic, left, right)
        for topic, topic_data in topics.items()
        for left, right in topic_data["undirected_edges"]
    }
    global_nodes = set().union(*(topic_data["nodes"] for topic_data in topics.values()))
    return {
        "target_layer": "mesh",
        "snapshot_heartbeat_index": snapshot_heartbeat_index,
        "topics": topics,
        "all_edges": global_edges,
        "all_undirected_edges": global_undirected_edges,
        "all_nodes": global_nodes,
    }


def extract_attacker_frontier_truth(
    global_truth: dict[str, Any], *, attacker_node_ids: list[str]
) -> dict[str, Any]:
    attacker_set = {str(node_id) for node_id in attacker_node_ids}
    topics: dict[str, dict[str, Any]] = {}
    frontier_edges: set[tuple[str, str, str]] = set()
    frontier_undirected_edges: set[tuple[str, str, str]] = set()
    frontier_nodes: set[str] = set()
    for topic, topic_data in global_truth["topics"].items():
        directed_edges = {
            edge for edge in topic_data["directed_edges"] if edge[0] in attacker_set or edge[1] in attacker_set
        }
        if not directed_edges:
            continue
        undirected_edges = {canonical_undirected_edge(src, dst) for src, dst in directed_edges}
        topic_nodes = {node for edge in directed_edges for node in edge}
        topics[topic] = {
            "directed_edges": directed_edges,
            "undirected_edges": undirected_edges,
            "nodes": topic_nodes,
        }
        frontier_edges |= {(topic, src, dst) for src, dst in directed_edges}
        frontier_undirected_edges |= {
            (topic, left, right) for left, right in undirected_edges
        }
        frontier_nodes |= topic_nodes
    return {
        "attacker_node_ids": sorted(attacker_set, key=node_sort_key),
        "topics": topics,
        "all_edges": frontier_edges,
        "all_undirected_edges": frontier_undirected_edges,
        "all_nodes": frontier_nodes,
    }


def build_truth_ego_mesh_for_attacker(
    global_truth: dict[str, Any], *, attacker_node_id: str
) -> dict[str, Any]:
    topics: dict[str, Any] = {}
    for topic, topic_data in global_truth["topics"].items():
        outgoing = sorted(
            {dst for src, dst in topic_data["directed_edges"] if src == attacker_node_id},
            key=node_sort_key,
        )
        incoming = sorted(
            {src for src, dst in topic_data["directed_edges"] if dst == attacker_node_id},
            key=node_sort_key,
        )
        if not outgoing and not incoming:
            continue
        undirected = sorted(set(outgoing) | set(incoming), key=node_sort_key)
        topics[topic] = {
            "outgoing_neighbors": outgoing,
            "incoming_neighbors": incoming,
            "undirected_neighbors": undirected,
        }
    return {"attacker_node_id": attacker_node_id, "topics": topics}


def evaluate_per_attacker(
    *,
    inferred_topology: dict[str, Any],
    global_truth: dict[str, Any],
    attack_metadata: dict[str, Any],
    snapshot_heartbeat_index: int,
) -> dict[str, Any]:
    per_attacker_predicted = inferred_topology.get("per_attacker", {})
    attacker_indices = [int(index) for index in attack_metadata.get("attacker_indices", [])]
    attacker_node_ids = [str(node_id) for node_id in attack_metadata.get("attacker_node_ids", [])]

    output: dict[str, Any] = {}
    for attacker_index, attacker_node_id in zip(attacker_indices, attacker_node_ids, strict=False):
        predicted_view = per_attacker_predicted.get(str(attacker_index), {})
        predicted_topics = predicted_view.get("topics", {})
        truth_view = build_truth_ego_mesh_for_attacker(global_truth, attacker_node_id=attacker_node_id)
        truth_topics = truth_view["topics"]
        all_topics = sorted(set(predicted_topics.keys()) | set(truth_topics.keys()))

        topic_results: dict[str, Any] = {}
        outgoing_tp = outgoing_fp = outgoing_fn = 0
        incoming_tp = incoming_fp = incoming_fn = 0
        combined_tp = combined_fp = combined_fn = 0
        undirected_tp = undirected_fp = undirected_fn = 0
        jaccards: list[float] = []
        predicted_neighbors = 0
        true_neighbors = 0

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

            outgoing_pred_edges = {(attacker_node_id, peer) for peer in outgoing_pred_neighbors}
            outgoing_truth_edges = {(attacker_node_id, peer) for peer in outgoing_truth_neighbors}
            incoming_pred_edges = {(peer, attacker_node_id) for peer in incoming_pred_neighbors}
            incoming_truth_edges = {(peer, attacker_node_id) for peer in incoming_truth_neighbors}
            undirected_pred_edges = {
                canonical_undirected_edge(attacker_node_id, peer)
                for peer in undirected_pred_neighbors
            }
            undirected_truth_edges = {
                canonical_undirected_edge(attacker_node_id, peer)
                for peer in undirected_truth_neighbors
            }

            outgoing_metrics = prf(outgoing_pred_edges, outgoing_truth_edges)
            incoming_metrics = prf(incoming_pred_edges, incoming_truth_edges)
            combined_metrics = prf(
                outgoing_pred_edges | incoming_pred_edges,
                outgoing_truth_edges | incoming_truth_edges,
            )
            undirected_metrics = prf(undirected_pred_edges, undirected_truth_edges)
            undirected_jaccard = jaccard(undirected_pred_neighbors, undirected_truth_neighbors)

            outgoing_tp += outgoing_metrics["tp"]
            outgoing_fp += outgoing_metrics["fp"]
            outgoing_fn += outgoing_metrics["fn"]
            incoming_tp += incoming_metrics["tp"]
            incoming_fp += incoming_metrics["fp"]
            incoming_fn += incoming_metrics["fn"]
            combined_tp += combined_metrics["tp"]
            combined_fp += combined_metrics["fp"]
            combined_fn += combined_metrics["fn"]
            undirected_tp += undirected_metrics["tp"]
            undirected_fp += undirected_metrics["fp"]
            undirected_fn += undirected_metrics["fn"]
            jaccards.append(undirected_jaccard)
            predicted_neighbors += len(undirected_pred_neighbors)
            true_neighbors += len(undirected_truth_neighbors)

            topic_results[topic] = {
                "ground_truth": {
                    "outgoing_neighbors": sort_nodes(outgoing_truth_neighbors),
                    "incoming_neighbors": sort_nodes(incoming_truth_neighbors),
                    "undirected_neighbors": sort_nodes(undirected_truth_neighbors),
                },
                "prediction": {
                    "outgoing_neighbors": sort_nodes(outgoing_pred_neighbors),
                    "incoming_neighbors": sort_nodes(incoming_pred_neighbors),
                    "undirected_neighbors": sort_nodes(undirected_pred_neighbors),
                },
                "metrics": {
                    "directed_outgoing": outgoing_metrics,
                    "directed_incoming": incoming_metrics,
                    "directed_combined": combined_metrics,
                    "undirected_neighbors": undirected_metrics,
                    "undirected_neighbor_jaccard": undirected_jaccard,
                },
            }

        output[str(attacker_index)] = {
            "attacker_index": attacker_index,
            "attacker_node_id": attacker_node_id,
            "topics": topic_results,
            "aggregate": {
                "evaluated_topics": len(topic_results),
                "predicted_neighbors": predicted_neighbors,
                "true_neighbors": true_neighbors,
                "primary_metric": "undirected_neighbors",
                "undirected_neighbors": prf_counts(undirected_tp, undirected_fp, undirected_fn),
                "directed_outgoing": prf_counts(outgoing_tp, outgoing_fp, outgoing_fn),
                "directed_incoming": prf_counts(incoming_tp, incoming_fp, incoming_fn),
                "directed_combined": prf_counts(combined_tp, combined_fp, combined_fn),
                "undirected_neighbor_jaccard_macro": (
                    sum(jaccards) / len(jaccards) if jaccards else 0.0
                ),
            },
        }

    return output


def prf_counts(tp: int, fp: int, fn: int) -> dict[str, Any]:
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "precision": precision, "recall": recall, "f1": f1}


def parse_predicted_directed_edges(topic_data: dict[str, Any]) -> set[tuple[str, str]]:
    predicted_edges: set[tuple[str, str]] = set()
    for edge in topic_data.get("directed_edges", []):
        if not isinstance(edge, dict):
            continue
        src = edge.get("src")
        dst = edge.get("dst")
        if src is None or dst is None:
            continue
        predicted_edges.add((str(src), str(dst)))
    return predicted_edges


def parse_predicted_undirected_edges(topic_data: dict[str, Any]) -> set[tuple[str, str]]:
    parsed: set[tuple[str, str]] = set()
    for edge in topic_data.get("undirected_edges", []):
        if not isinstance(edge, list) or len(edge) != 2:
            continue
        parsed.add(canonical_undirected_edge(str(edge[0]), str(edge[1])))
    return parsed


def evaluate_coalition_union(
    *,
    inferred_topology: dict[str, Any],
    global_truth: dict[str, Any],
    frontier_truth: dict[str, Any],
    attack_metadata: dict[str, Any],
    snapshot_heartbeat_index: int,
) -> dict[str, Any]:
    predicted_topics = inferred_topology.get("coalition_union", {}).get("topics", {})
    truth_topics = frontier_truth["topics"]
    all_topics = sorted(set(predicted_topics.keys()) | set(truth_topics.keys()))

    topic_results: dict[str, Any] = {}
    predicted_union_edges: set[tuple[str, str, str]] = set()
    truth_union_edges: set[tuple[str, str, str]] = set()
    predicted_union_undirected: set[tuple[str, str, str]] = set()
    truth_union_undirected: set[tuple[str, str, str]] = set()

    for topic in all_topics:
        predicted_topic = predicted_topics.get(topic, {})
        truth_topic = truth_topics.get(topic, {})

        predicted_edges = parse_predicted_directed_edges(predicted_topic)
        truth_edges = set(truth_topic.get("directed_edges", set()))
        if not predicted_edges and not truth_edges:
            continue

        predicted_undirected = parse_predicted_undirected_edges(predicted_topic)
        truth_undirected = set(truth_topic.get("undirected_edges", set()))
        topic_metrics = prf(predicted_edges, truth_edges)
        topic_undirected_metrics = prf(predicted_undirected, truth_undirected)
        topic_jaccard = jaccard(predicted_undirected, truth_undirected)

        predicted_union_edges |= {(topic, src, dst) for src, dst in predicted_edges}
        truth_union_edges |= {(topic, src, dst) for src, dst in truth_edges}
        predicted_union_undirected |= {
            (topic, left, right) for left, right in predicted_undirected
        }
        truth_union_undirected |= {(topic, left, right) for left, right in truth_undirected}

        topic_results[topic] = {
            "ground_truth": {
                "directed_edges": sort_directed_edges(truth_edges),
                "undirected_edges": sort_undirected_edges(truth_undirected),
            },
            "prediction": {
                "directed_edges": sort_directed_edges(predicted_edges),
                "undirected_edges": sort_undirected_edges(predicted_undirected),
            },
            "metrics": {
                "frontier_directed": topic_metrics,
                "frontier_undirected": topic_undirected_metrics,
                "frontier_undirected_jaccard": topic_jaccard,
            },
        }

    aggregate_metrics = prf(predicted_union_edges, truth_union_edges)
    aggregate_undirected_metrics = prf(predicted_union_undirected, truth_union_undirected)
    return {
        "topics": topic_results,
        "aggregate": {
            "evaluated_topics": len(topic_results),
            "primary_metric": "frontier_undirected",
            "predicted_frontier_edges": len(predicted_union_edges),
            "frontier_truth_edges": len(truth_union_edges),
            "matched_frontier_edges": aggregate_metrics["tp"],
            "frontier_directed_precision": aggregate_metrics["precision"],
            "frontier_directed_recall": aggregate_metrics["recall"],
            "frontier_directed_f1": aggregate_metrics["f1"],
            "predicted_frontier_undirected_edges": len(predicted_union_undirected),
            "frontier_undirected_truth_edges": len(truth_union_undirected),
            "matched_frontier_undirected_edges": aggregate_undirected_metrics["tp"],
            "frontier_undirected_precision": aggregate_undirected_metrics["precision"],
            "frontier_undirected_recall": aggregate_undirected_metrics["recall"],
            "frontier_undirected_f1": aggregate_undirected_metrics["f1"],
            "frontier_undirected_jaccard": jaccard(
                predicted_union_undirected, truth_union_undirected
            ),
        },
    }


def evaluate_global_coverage(
    *,
    inferred_topology: dict[str, Any],
    global_truth: dict[str, Any],
) -> dict[str, Any]:
    predicted_topics = inferred_topology.get("coalition_union", {}).get("topics", {})
    predicted_edges_by_topic = {
        topic: {(topic, src, dst) for src, dst in parse_predicted_directed_edges(topic_data)}
        for topic, topic_data in predicted_topics.items()
    }
    predicted_undirected_edges_by_topic = {
        topic: {
            (topic, left, right)
            for left, right in parse_predicted_undirected_edges(topic_data)
        }
        for topic, topic_data in predicted_topics.items()
    }

    matched_edges: set[tuple[str, str, str]] = set()
    global_truth_edges: set[tuple[str, str, str]] = set()
    matched_undirected_edges: set[tuple[str, str, str]] = set()
    global_truth_undirected_edges: set[tuple[str, str, str]] = set()
    topic_results: dict[str, Any] = {}

    for topic, topic_data in global_truth["topics"].items():
        truth_edges = {(topic, src, dst) for src, dst in topic_data["directed_edges"]}
        topic_matched_edges = predicted_edges_by_topic.get(topic, set()) & truth_edges
        global_truth_edges |= truth_edges
        matched_edges |= topic_matched_edges
        truth_undirected_edges = {
            (topic, left, right) for left, right in topic_data["undirected_edges"]
        }
        topic_matched_undirected_edges = (
            predicted_undirected_edges_by_topic.get(topic, set()) & truth_undirected_edges
        )
        global_truth_undirected_edges |= truth_undirected_edges
        matched_undirected_edges |= topic_matched_undirected_edges

        frontier_topic = global_truth.get("frontier_topics", {}).get(topic, {})
        frontier_topic_edges = {
            (topic, src, dst) for src, dst in frontier_topic.get("directed_edges", set())
        }
        frontier_topic_undirected_edges = {
            (topic, left, right)
            for left, right in frontier_topic.get("undirected_edges", set())
        }
        truth_nodes = set(topic_data.get("nodes", set()))
        matched_topic_nodes = {
            node for _, src, dst in topic_matched_edges for node in (src, dst)
        }
        matched_topic_undirected_nodes = {
            node
            for _, left, right in topic_matched_undirected_edges
            for node in (left, right)
        }

        topic_results[topic] = {
            "global_truth_edges": len(truth_edges),
            "frontier_truth_edges": len(frontier_topic_edges),
            "matched_global_edges": len(topic_matched_edges),
            "global_edge_coverage": (
                len(topic_matched_edges) / len(truth_edges) if truth_edges else 0.0
            ),
            "frontier_edge_fraction": (
                len(frontier_topic_edges) / len(truth_edges) if truth_edges else 0.0
            ),
            "global_truth_undirected_edges": len(truth_undirected_edges),
            "frontier_truth_undirected_edges": len(frontier_topic_undirected_edges),
            "matched_global_undirected_edges": len(topic_matched_undirected_edges),
            "global_undirected_edge_coverage": (
                len(topic_matched_undirected_edges) / len(truth_undirected_edges)
                if truth_undirected_edges
                else 0.0
            ),
            "frontier_undirected_edge_fraction": (
                len(frontier_topic_undirected_edges) / len(truth_undirected_edges)
                if truth_undirected_edges
                else 0.0
            ),
            "global_truth_nodes": len(truth_nodes),
            "matched_nodes": len(matched_topic_nodes),
            "matched_global_node_coverage": (
                len(matched_topic_nodes) / len(truth_nodes) if truth_nodes else 0.0
            ),
            "matched_undirected_nodes": len(matched_topic_undirected_nodes),
            "matched_global_undirected_node_coverage": (
                len(matched_topic_undirected_nodes) / len(truth_nodes)
                if truth_nodes
                else 0.0
            ),
        }

    frontier_truth_edges = {
        (topic, src, dst)
        for topic, topic_data in global_truth.get("frontier_topics", {}).items()
        for src, dst in topic_data["directed_edges"]
    }
    frontier_truth_undirected_edges = {
        (topic, left, right)
        for topic, topic_data in global_truth.get("frontier_topics", {}).items()
        for left, right in topic_data["undirected_edges"]
    }
    matched_nodes = {node for _, src, dst in matched_edges for node in (src, dst)}
    matched_undirected_nodes = {
        node for _, left, right in matched_undirected_edges for node in (left, right)
    }
    global_truth_nodes = set(global_truth["all_nodes"])

    return {
        "topics": topic_results,
        "aggregate": {
            "evaluated_topics": len(global_truth["topics"]),
            "global_truth_edges": len(global_truth_edges),
            "frontier_truth_edges": len(frontier_truth_edges),
            "matched_global_edges": len(matched_edges),
            "global_edge_coverage": (
                len(matched_edges) / len(global_truth_edges) if global_truth_edges else 0.0
            ),
            "frontier_edge_fraction": (
                len(frontier_truth_edges) / len(global_truth_edges)
                if global_truth_edges
                else 0.0
            ),
            "global_truth_undirected_edges": len(global_truth_undirected_edges),
            "frontier_truth_undirected_edges": len(frontier_truth_undirected_edges),
            "matched_global_undirected_edges": len(matched_undirected_edges),
            "global_undirected_edge_coverage": (
                len(matched_undirected_edges) / len(global_truth_undirected_edges)
                if global_truth_undirected_edges
                else 0.0
            ),
            "frontier_undirected_edge_fraction": (
                len(frontier_truth_undirected_edges) / len(global_truth_undirected_edges)
                if global_truth_undirected_edges
                else 0.0
            ),
            "global_truth_nodes": len(global_truth_nodes),
            "matched_nodes": len(matched_nodes),
            "matched_global_node_coverage": (
                len(matched_nodes) / len(global_truth_nodes) if global_truth_nodes else 0.0
            ),
            "matched_undirected_nodes": len(matched_undirected_nodes),
            "matched_global_undirected_node_coverage": (
                len(matched_undirected_nodes) / len(global_truth_nodes)
                if global_truth_nodes
                else 0.0
            ),
        }
    }


def evaluate_inference(
    *,
    inferred_topology: dict[str, Any],
    global_truth: dict[str, Any],
    frontier_truth: dict[str, Any],
    attack_metadata: dict[str, Any],
    snapshot_heartbeat_index: int,
) -> dict[str, Any]:
    global_truth_with_frontier = dict(global_truth)
    global_truth_with_frontier["frontier_topics"] = frontier_truth["topics"]
    return {
        "attack_name": attack_metadata.get("attack_name"),
        "observability_profile": attack_metadata.get("observability_profile"),
        "target_layer": attack_metadata.get("target_layer"),
        "scenario": attack_metadata.get("scenario"),
        "seed": attack_metadata.get("seed"),
        "coalition_size": attack_metadata.get("coalition_size"),
        "attacker_indices": attack_metadata.get("attacker_indices", []),
        "attacker_node_ids": attack_metadata.get("attacker_node_ids", []),
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
        "topic_evaluation_policy": {
            "per_attacker": "union(predicted attacker topics, truth topics where attacker appears in the final heartbeat snapshot)",
            "coalition_union": "union(predicted coalition topics, truth topics where at least one attacker appears in the final heartbeat snapshot)",
            "empty_topic_convention": "Topics with empty prediction and empty truth are excluded from evaluated topic counts and never contribute to macro-style aggregates.",
            "predicted_neighbors_definition": "Per-attacker predicted_neighbors counts non-directed neighbors aggregated locally across evaluated topics.",
            "true_neighbors_definition": "Per-attacker true_neighbors counts non-directed truth neighbors aggregated locally across evaluated topics.",
            "frontier_primary_metric": "coalition frontier reconstruction is reported primarily as non-directed edges; directed metrics remain as a GRAFT-direction diagnostic.",
            "global_coverage_primary_metric": "global non-directed edge coverage aligns passive_coalition with passive_ego_mesh neighbor semantics; directed coverage remains exported separately.",
            "per_topic_coverage": "global_coverage.topics reports one final-mesh coverage row per topic so a single topic mesh is not hidden by the multi-topic union.",
        },
        "per_attacker": evaluate_per_attacker(
            inferred_topology=inferred_topology,
            global_truth=global_truth,
            attack_metadata=attack_metadata,
            snapshot_heartbeat_index=snapshot_heartbeat_index,
        ),
        "coalition_union": evaluate_coalition_union(
            inferred_topology=inferred_topology,
            global_truth=global_truth,
            frontier_truth=frontier_truth,
            attack_metadata=attack_metadata,
            snapshot_heartbeat_index=snapshot_heartbeat_index,
        ),
        "global_coverage": evaluate_global_coverage(
            inferred_topology=inferred_topology,
            global_truth=global_truth_with_frontier,
        ),
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
    global_truth = extract_global_truth_mesh(snapshots_df, snapshot_heartbeat_index=snapshot_heartbeat_index)
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
    print(run_dir / "evaluation.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
