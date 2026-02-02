#!/usr/bin/env python3
"""Topology visualizations for the passive_ego_mesh inference attack."""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, Patch


_LINEWIDTH_IN: float = 6.30
_FIG_FONT_SIZE: int = 9

Edge = tuple[str, str, str]  # (topic, src, dst)

REQUIRED_SNAPSHOT_COLUMNS = {
    "heartbeat_index",
    "snapshot_kind",
    "node_id",
    "peer_id",
    "topic",
}

COLOR_ATTACKER = "#4C78A8"
COLOR_NEUTRAL = "#D9D9D9"
COLOR_OUTGOING = "#0072B2"
COLOR_TP = "#009E73"
COLOR_FN = "#CC3311"
COLOR_FP = "#E69F00"
COLOR_TEXT = "#1F1F1F"


def configure_style() -> None:
    plt.rcParams.update(
        {
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "black",
            "axes.labelcolor": "black",
            "text.color": "black",
            "xtick.color": "black",
            "ytick.color": "black",
            "font.size": _FIG_FONT_SIZE,
            "axes.titlesize": _FIG_FONT_SIZE,
            "axes.labelsize": _FIG_FONT_SIZE,
            "xtick.labelsize": _FIG_FONT_SIZE,
            "ytick.labelsize": _FIG_FONT_SIZE,
            "legend.fontsize": _FIG_FONT_SIZE,
            "legend.frameon": False,
            "axes.grid": True,
            "grid.color": "0.85",
            "grid.linewidth": 0.6,
            "grid.alpha": 0.8,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "lines.linewidth": 1.8,
            "errorbar.capsize": 3,
        }
    )


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required JSON file: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return data


def load_mesh_snapshots(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required snapshot file: {path}")

    snapshots = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = REQUIRED_SNAPSHOT_COLUMNS - set(snapshots.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"{path} is missing required column(s): {missing_text}")

    mesh_edges = snapshots[snapshots["snapshot_kind"] == "mesh_edge"].copy()
    if mesh_edges.empty:
        raise ValueError(f"{path} contains no snapshot_kind == 'mesh_edge' rows")

    mesh_edges["heartbeat_index"] = pd.to_numeric(
        mesh_edges["heartbeat_index"], errors="coerce"
    )
    invalid_heartbeats = mesh_edges["heartbeat_index"].isna().sum()
    if invalid_heartbeats:
        raise ValueError(
            f"{path} contains {invalid_heartbeats} mesh_edge rows with invalid heartbeat_index"
        )
    mesh_edges["heartbeat_index"] = mesh_edges["heartbeat_index"].astype(int)
    return mesh_edges


def node_sort_key(node_id: str) -> tuple[int, int | str]:
    node = str(node_id)
    if node.isdigit():
        return (0, int(node))
    return (1, node)


def letter_label(index: int) -> str:
    """Return spreadsheet-style labels: A, B, ..., Z, AA, AB, ..."""
    if index < 0:
        raise ValueError("label index must be non-negative")
    letters = []
    value = index
    while True:
        value, remainder = divmod(value, 26)
        letters.append(chr(ord("A") + remainder))
        if value == 0:
            break
        value -= 1
    return "".join(reversed(letters))


def build_letter_labels(
    positions: dict[str, tuple[float, float]],
    attacker_node_id: str,
) -> dict[str, str]:
    ordered_nodes = [attacker_node_id]
    ordered_nodes.extend(
        node
        for node in sorted(positions, key=node_sort_key)
        if node != attacker_node_id
    )
    return {node: letter_label(index) for index, node in enumerate(ordered_nodes)}


def sanitize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def ego_edge(topic: str, attacker_node_id: str, peer_node_id: str) -> Edge:
    return (str(topic), str(attacker_node_id), str(peer_node_id))


def resolve_topic_filter(
    snapshots: pd.DataFrame,
    attacker_node_id: str,
    requested_topic: str,
) -> tuple[str | None, str]:
    """Resolve --topic into a concrete topic filter.

    Returns (topic_filter, selection_mode). None means all topics.
    """
    requested = requested_topic.strip()
    if requested.lower() == "all":
        return None, "all"
    if requested.lower() != "auto":
        return requested, "explicit"

    final_heartbeat = int(snapshots["heartbeat_index"].max())
    final_snapshot = snapshots[snapshots["heartbeat_index"] == final_heartbeat]
    incident = final_snapshot[
        (final_snapshot["node_id"] == attacker_node_id)
        | (final_snapshot["peer_id"] == attacker_node_id)
    ].copy()
    if incident.empty:
        raise ValueError(
            f"No final truth ego-mesh edge found for attacker_node_id={attacker_node_id}"
        )

    incident["neighbor_id"] = incident["peer_id"].where(
        incident["node_id"] == attacker_node_id,
        incident["node_id"],
    )
    grouped = (
        incident.groupby("topic")
        .agg(
            edge_count=("topic", "size"),
            neighbor_count=("neighbor_id", "nunique"),
        )
        .reset_index()
    )
    grouped = grouped.sort_values(
        by=["edge_count", "neighbor_count", "topic"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    return str(grouped.iloc[0]["topic"]), "auto"


def extract_final_truth_ego_mesh(
    snapshots: pd.DataFrame,
    attacker_node_id: str,
    topic: str | None = None,
) -> dict[str, Any]:
    final_heartbeat = int(snapshots["heartbeat_index"].max())
    final_snapshot = snapshots[snapshots["heartbeat_index"] == final_heartbeat].copy()
    if topic is not None:
        final_snapshot = final_snapshot[final_snapshot["topic"] == topic]

    if final_snapshot.empty:
        topic_text = topic if topic is not None else "all topics"
        raise ValueError(f"No final snapshot mesh_edge rows found for {topic_text}")

    outgoing_rows = final_snapshot[final_snapshot["node_id"] == attacker_node_id]
    incoming_rows = final_snapshot[final_snapshot["peer_id"] == attacker_node_id]

    edges: set[Edge] = set()
    topics: dict[str, dict[str, set[str]]] = {}

    for row in outgoing_rows.itertuples(index=False):
        edge = ego_edge(str(row.topic), attacker_node_id, str(row.peer_id))
        edges.add(edge)
        topic_data = topics.setdefault(
            str(row.topic),
            {"outgoing_neighbors": set(), "incoming_neighbors": set(), "undirected_neighbors": set()},
        )
        topic_data["outgoing_neighbors"].add(str(row.peer_id))
        topic_data["undirected_neighbors"].add(str(row.peer_id))

    for row in incoming_rows.itertuples(index=False):
        edge = ego_edge(str(row.topic), attacker_node_id, str(row.node_id))
        edges.add(edge)
        topic_data = topics.setdefault(
            str(row.topic),
            {"outgoing_neighbors": set(), "incoming_neighbors": set(), "undirected_neighbors": set()},
        )
        topic_data["incoming_neighbors"].add(str(row.node_id))
        topic_data["undirected_neighbors"].add(str(row.node_id))

    if not edges:
        topic_text = topic if topic is not None else "all topics"
        raise ValueError(
            f"No final truth ego-mesh edge found for attacker_node_id={attacker_node_id} "
            f"on {topic_text}"
        )

    neighbors = {peer for _, _, peer in edges}
    return {
        "attacker_node_id": attacker_node_id,
        "heartbeat_index": final_heartbeat,
        "topic_filter": topic,
        "topics": topics,
        "edges": edges,
        "neighbors": neighbors,
    }


def extract_inferred_ego_mesh(
    inferred_topology: dict[str, Any],
    attacker_node_id: str,
    topic: str | None = None,
) -> dict[str, Any]:
    topics_json = inferred_topology.get("topics")
    if not isinstance(topics_json, dict):
        raise ValueError(
            "inferred_topology.json does not match the expected passive_ego_mesh schema: "
            "missing object field 'topics'"
        )

    selected_topics = (
        [topic] if topic is not None else sorted(topics_json.keys(), key=str)
    )
    edges: set[Edge] = set()
    topics: dict[str, dict[str, set[str]]] = {}

    for topic_name in selected_topics:
        topic_payload = topics_json.get(topic_name, {})
        if topic_payload is None:
            topic_payload = {}
        if not isinstance(topic_payload, dict):
            raise ValueError(
                f"Invalid inferred topic payload for {topic_name!r}: expected object"
            )

        outgoing_neighbors = topic_payload.get("outgoing_neighbors", [])
        incoming_neighbors = topic_payload.get("incoming_neighbors", [])
        if not isinstance(outgoing_neighbors, list) or not isinstance(
            incoming_neighbors, list
        ):
            raise ValueError(
                f"Invalid inferred neighbors for {topic_name!r}: expected lists"
            )

        topic_data = topics.setdefault(
            str(topic_name),
            {"outgoing_neighbors": set(), "incoming_neighbors": set(), "undirected_neighbors": set()},
        )
        for peer_id in outgoing_neighbors:
            peer = str(peer_id)
            edges.add(ego_edge(str(topic_name), attacker_node_id, peer))
            topic_data["outgoing_neighbors"].add(peer)
            topic_data["undirected_neighbors"].add(peer)
        for peer_id in incoming_neighbors:
            peer = str(peer_id)
            edges.add(ego_edge(str(topic_name), attacker_node_id, peer))
            topic_data["incoming_neighbors"].add(peer)
            topic_data["undirected_neighbors"].add(peer)

    neighbors = {peer for _, _, peer in edges}
    return {
        "attacker_node_id": attacker_node_id,
        "topic_filter": topic,
        "topics": topics,
        "edges": edges,
        "neighbors": neighbors,
    }


def build_truth_graph(truth_ego_mesh: dict[str, Any]) -> dict[str, Any]:
    attacker_node_id = str(truth_ego_mesh["attacker_node_id"])
    nodes = {attacker_node_id}
    node_roles = {attacker_node_id: "attacker"}
    for neighbor in sorted(truth_ego_mesh["neighbors"], key=node_sort_key):
        nodes.add(neighbor)
        node_roles[neighbor] = "truth_neighbor"
    return {
        "nodes": nodes,
        "node_roles": node_roles,
        "edges": set(truth_ego_mesh["edges"]),
    }


def build_comparison_graph(
    truth_ego_mesh: dict[str, Any],
    inferred_ego_mesh: dict[str, Any],
) -> dict[str, Any]:
    truth_edges: set[Edge] = set(truth_ego_mesh["edges"])
    pred_edges: set[Edge] = set(inferred_ego_mesh["edges"])
    tp_edges = truth_edges & pred_edges
    fn_edges = truth_edges - pred_edges
    fp_edges = pred_edges - truth_edges

    attacker_node_id = str(truth_ego_mesh["attacker_node_id"])
    truth_neighbors = set(truth_ego_mesh["neighbors"])
    pred_neighbors = set(inferred_ego_mesh["neighbors"])
    tp_neighbors = truth_neighbors & pred_neighbors
    fn_neighbors = truth_neighbors - pred_neighbors
    fp_neighbors = pred_neighbors - truth_neighbors

    nodes = {attacker_node_id}
    node_roles = {attacker_node_id: "attacker"}
    for node in sorted(truth_neighbors | pred_neighbors, key=node_sort_key):
        if node in tp_neighbors:
            role = "tp_neighbor"
        elif node in fn_neighbors:
            role = "fn_neighbor"
        else:
            role = "fp_neighbor"
        nodes.add(node)
        node_roles[node] = role

    return {
        "graph": {"nodes": nodes, "node_roles": node_roles, "edges": truth_edges | pred_edges},
        "truth_edges": truth_edges,
        "pred_edges": pred_edges,
        "tp_edges": tp_edges,
        "fn_edges": fn_edges,
        "fp_edges": fp_edges,
        "pure_fp_edges": fp_edges,
        "truth_neighbors": truth_neighbors,
        "pred_neighbors": pred_neighbors,
        "tp_neighbors": tp_neighbors,
        "fn_neighbors": fn_neighbors,
        "fp_neighbors": fp_neighbors,
    }


def visual_edgelist(edges: set[Edge]) -> list[tuple[str, str]]:
    return sorted(
        {(src, dst) for _, src, dst in edges},
        key=lambda e: (node_sort_key(e[0]), node_sort_key(e[1])),
    )


def compute_layout(
    graph: dict[str, Any],
    attacker_node_id: str,
    layout: str,
) -> dict[str, tuple[float, float]]:
    nodes = sorted((str(node) for node in graph["nodes"]), key=node_sort_key)
    if attacker_node_id not in nodes:
        nodes.insert(0, attacker_node_id)

    if layout in {"radial", "kamada"} or len(nodes) <= 2:
        neighbors = [node for node in nodes if node != attacker_node_id]
        positions: dict[str, tuple[float, float]] = {attacker_node_id: (0.0, 0.0)}
        if len(neighbors) == 1:
            positions[neighbors[0]] = (2.2, 0.0)
        else:
            radius = 2.6 if len(neighbors) <= 12 else 3.2
            for index, node in enumerate(neighbors):
                angle = (2.0 * math.pi * index) / max(len(neighbors), 1)
                positions[node] = (radius * math.cos(angle), radius * math.sin(angle))
        return positions

    if layout == "spring":
        return compute_simple_spring_layout(graph, attacker_node_id)
    raise ValueError(f"Unsupported layout: {layout}")


def compute_simple_spring_layout(
    graph: dict[str, Any],
    attacker_node_id: str,
    iterations: int = 100,
) -> dict[str, tuple[float, float]]:
    nodes = sorted((str(node) for node in graph["nodes"]), key=node_sort_key)
    if attacker_node_id not in nodes:
        nodes.insert(0, attacker_node_id)

    positions = compute_layout(graph, attacker_node_id, "radial")
    if len(nodes) <= 2:
        return positions

    undirected_edges = {
        tuple(sorted((str(src), str(dst)), key=node_sort_key))
        for _, src, dst in graph.get("edges", set())
        if src != dst
    }
    area = 12.0
    k = math.sqrt(area / max(len(nodes), 1))
    temperature = 0.18

    for _ in range(iterations):
        displacement = {node: [0.0, 0.0] for node in nodes}
        for i, node_a in enumerate(nodes):
            for node_b in nodes[i + 1 :]:
                dx = positions[node_a][0] - positions[node_b][0]
                dy = positions[node_a][1] - positions[node_b][1]
                distance = math.hypot(dx, dy) or 0.01
                force = (k * k) / distance
                ux = dx / distance
                uy = dy / distance
                displacement[node_a][0] += ux * force
                displacement[node_a][1] += uy * force
                displacement[node_b][0] -= ux * force
                displacement[node_b][1] -= uy * force

        for node_a, node_b in undirected_edges:
            dx = positions[node_a][0] - positions[node_b][0]
            dy = positions[node_a][1] - positions[node_b][1]
            distance = math.hypot(dx, dy) or 0.01
            force = (distance * distance) / k
            ux = dx / distance
            uy = dy / distance
            displacement[node_a][0] -= ux * force
            displacement[node_a][1] -= uy * force
            displacement[node_b][0] += ux * force
            displacement[node_b][1] += uy * force

        for node in nodes:
            if node == attacker_node_id:
                positions[node] = (0.0, 0.0)
                continue
            dx, dy = displacement[node]
            length = math.hypot(dx, dy) or 0.01
            step = min(length, temperature)
            positions[node] = (
                positions[node][0] + (dx / length) * step,
                positions[node][1] + (dy / length) * step,
            )
        temperature *= 0.96

    return positions


def draw_line_group(
    ax: plt.Axes,
    positions: dict[str, tuple[float, float]],
    edgelist: list[tuple[str, str]] | set[tuple[str, str]],
    *,
    color: str,
    style: str | tuple[int, tuple[int, ...]],
    width: float,
    alpha: float,
    rad: float = 0.0,
) -> None:
    if not edgelist:
        return
    sorted_edges = sorted(edgelist, key=lambda e: (node_sort_key(e[0]), node_sort_key(e[1])))
    for src, dst in sorted_edges:
        if src not in positions or dst not in positions:
            continue
        line = FancyArrowPatch(
            positions[src],
            positions[dst],
            arrowstyle="-",
            linewidth=width,
            linestyle=style,
            color=color,
            alpha=alpha,
            connectionstyle=f"arc3,rad={rad}",
            shrinkA=16,
            shrinkB=16,
            zorder=0,
        )
        ax.add_patch(line)


def draw_node_group(
    ax: plt.Axes,
    positions: dict[str, tuple[float, float]],
    nodes: set[str] | list[str],
    *,
    color: str,
    size: int,
    label: str | None = None,
) -> None:
    if not nodes:
        return
    selected_nodes = [node for node in sorted(set(nodes), key=node_sort_key) if node in positions]
    if not selected_nodes:
        return
    x_values = [positions[node][0] for node in selected_nodes]
    y_values = [positions[node][1] for node in selected_nodes]
    ax.scatter(
        x_values,
        y_values,
        s=size,
        c=color,
        edgecolors="black",
        linewidths=0.8,
        label=label,
        zorder=3,
    )


def draw_labels(
    ax: plt.Axes,
    positions: dict[str, tuple[float, float]],
    attacker_node_id: str,
    max_node_labels: int,
) -> None:
    labels = {attacker_node_id: "A"}
    if len(positions) <= max_node_labels:
        letter_labels = build_letter_labels(positions, attacker_node_id)
        for node in positions:
            if node != attacker_node_id:
                labels[node] = letter_labels[node]
    for node, label in labels.items():
        x_value, y_value = positions[node]
        ax.text(
            x_value,
            y_value,
            label,
            color=COLOR_TEXT,
            ha="center",
            va="center",
            zorder=4,
        )


def draw_truth_ego_mesh(
    truth_ego_mesh: dict[str, Any],
    metadata: dict[str, Any],
    *,
    layout: str,
    topic_mode: str,
    max_node_labels: int,
) -> plt.Figure:
    graph = build_truth_graph(truth_ego_mesh)
    attacker_node_id = str(truth_ego_mesh["attacker_node_id"])
    positions = compute_layout(graph, attacker_node_id, layout)

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 5.5))
    ax.set_axis_off()

    draw_line_group(
        ax,
        positions,
        visual_edgelist(set(truth_ego_mesh["edges"])),
        color=COLOR_OUTGOING,
        style="solid",
        width=1.9,
        alpha=0.9,
        rad=0.0,
    )
    draw_node_group(ax, positions, set(truth_ego_mesh["neighbors"]), color=COLOR_NEUTRAL, size=650)
    draw_node_group(ax, positions, [attacker_node_id], color=COLOR_ATTACKER, size=1100)
    draw_labels(ax, positions, attacker_node_id, max_node_labels)
    ax.set_aspect("equal", adjustable="datalim")
    ax.margins(0.30)
    legend_items = [
        Patch(facecolor=COLOR_ATTACKER, edgecolor="black", label="observer"),
        Patch(facecolor=COLOR_NEUTRAL, edgecolor="black", label="true neighbor"),
        Line2D([0], [0], color=COLOR_OUTGOING, lw=2, label="true ego-mesh relation"),
    ]
    fig.legend(handles=legend_items, loc="lower center", bbox_to_anchor=(0.5, 0.0), ncol=3)
    fig.tight_layout(rect=[0, 0.10, 1, 1])
    return fig


def draw_inference_comparison(
    truth_ego_mesh: dict[str, Any],
    inferred_ego_mesh: dict[str, Any],
    metadata: dict[str, Any],
    evaluation: dict[str, Any],
    *,
    layout: str,
    topic_mode: str,
    max_node_labels: int,
) -> plt.Figure:
    comparison = build_comparison_graph(truth_ego_mesh, inferred_ego_mesh)
    graph = comparison["graph"]
    attacker_node_id = str(truth_ego_mesh["attacker_node_id"])
    positions = compute_layout(graph, attacker_node_id, layout)

    fig, ax = plt.subplots(figsize=(_LINEWIDTH_IN, 5.5))
    ax.set_axis_off()

    draw_line_group(
        ax, positions, visual_edgelist(comparison["fn_edges"]),
        color=COLOR_FN, style="dashed", width=1.7, alpha=0.85, rad=0.0,
    )
    draw_line_group(
        ax, positions, visual_edgelist(comparison["pure_fp_edges"]),
        color=COLOR_FP, style="dotted", width=1.8, alpha=0.9, rad=0.0,
    )
    draw_line_group(
        ax, positions, visual_edgelist(comparison["tp_edges"]),
        color=COLOR_TP, style="solid", width=2.0, alpha=0.9, rad=0.0,
    )
    draw_node_group(ax, positions, comparison["fn_neighbors"], color=COLOR_FN, size=650)
    draw_node_group(ax, positions, comparison["fp_neighbors"], color=COLOR_FP, size=650)
    draw_node_group(ax, positions, comparison["tp_neighbors"], color=COLOR_TP, size=700)
    draw_node_group(ax, positions, [attacker_node_id], color=COLOR_ATTACKER, size=1100)
    draw_labels(ax, positions, attacker_node_id, max_node_labels)
    ax.set_aspect("equal", adjustable="datalim")
    ax.margins(0.30)
    legend_items = [
        Patch(facecolor=COLOR_ATTACKER, edgecolor="black", label="observer"),
        Patch(facecolor=COLOR_TP, edgecolor="black", label="correctly found node"),
        Patch(facecolor=COLOR_FN, edgecolor="black", label="missed true node"),
        Patch(facecolor=COLOR_FP, edgecolor="black", label="false positive node"),
        Line2D([0], [0], color=COLOR_TP, lw=2, label="TP edge"),
        Line2D([0], [0], color=COLOR_FN, lw=2, linestyle="--", label="FN edge"),
        Line2D([0], [0], color=COLOR_FP, lw=2, linestyle=":", label="FP edge"),
    ]
    fig.legend(handles=legend_items, loc="lower center", bbox_to_anchor=(0.5, 0.0), ncol=4)
    fig.tight_layout(rect=[0, 0.13, 1, 1])
    return fig


def save_figure(fig: plt.Figure, output_dir: Path, stem: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{stem}.pdf"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce topology visualizations from passive_ego_mesh run artefacts."
    )
    parser.add_argument("--run-dir", type=Path, required=True, help="Run directory.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults to <run-dir>/figures.",
    )
    parser.add_argument(
        "--layout",
        choices=["radial", "spring", "kamada"],
        default="radial",
    )
    parser.add_argument(
        "--topic",
        default="auto",
        help=(
            "Topic to visualize. Use 'auto' for a readable deterministic slice, "
            "'all' for all topics, or an explicit topic name."
        ),
    )
    parser.add_argument(
        "--max-node-labels",
        type=int,
        default=18,
        help="Draw non-attacker node labels only up to this number of nodes.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_style()
    run_dir = args.run_dir
    if not run_dir.exists():
        raise SystemExit(f"ERROR: run directory does not exist: {run_dir}")

    output_dir = args.output_dir if args.output_dir is not None else run_dir / "figures"

    try:
        metadata = load_json(run_dir / "attack_metadata.json")
        inferred_topology = load_json(run_dir / "inferred_topology.json")
        evaluation = load_json(run_dir / "evaluation.json")
        snapshots = load_mesh_snapshots(run_dir / "mesh_snapshots.csv")
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc

    if metadata.get("attack_name") != "passive_ego_mesh":
        raise SystemExit("ERROR: attack_metadata.json does not describe a passive_ego_mesh run")

    attacker_node_id = str(metadata.get("attacker_node_id", ""))
    if not attacker_node_id:
        raise SystemExit("ERROR: attack_metadata.json is missing attacker_node_id")

    try:
        topic_filter, topic_mode = resolve_topic_filter(snapshots, attacker_node_id, args.topic)
        truth_ego_mesh = extract_final_truth_ego_mesh(snapshots, attacker_node_id, topic=topic_filter)
        inferred_ego_mesh = extract_inferred_ego_mesh(inferred_topology, attacker_node_id, topic=topic_filter)
    except ValueError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc

    truth_stem = "ego_mesh_ground_truth"
    comparison_stem = "ego_mesh_inference_comparison"
    if args.topic not in ("auto", "all"):
        topic_suffix = sanitize_filename(args.topic)
        truth_stem = f"{truth_stem}_{topic_suffix}"
        comparison_stem = f"{comparison_stem}_{topic_suffix}"

    written_paths = [
        save_figure(
            draw_truth_ego_mesh(
                truth_ego_mesh, metadata,
                layout=args.layout, topic_mode=topic_mode, max_node_labels=args.max_node_labels,
            ),
            output_dir,
            truth_stem,
        ),
        save_figure(
            draw_inference_comparison(
                truth_ego_mesh, inferred_ego_mesh, metadata, evaluation,
                layout=args.layout, topic_mode=topic_mode, max_node_labels=args.max_node_labels,
            ),
            output_dir,
            comparison_stem,
        ),
    ]

    topic_label = topic_filter if topic_filter is not None else "all topics"
    print(f"Visualized passive_ego_mesh run: {run_dir}")
    print(f"Topic: {topic_label} ({topic_mode})")
    print(f"Final heartbeat: {truth_ego_mesh['heartbeat_index']}")
    print(f"Truth edges: {len(truth_ego_mesh['edges'])}")
    print(f"Inferred edges: {len(inferred_ego_mesh['edges'])}")
    print("Written figures:")
    for path in written_paths:
        print(f"  {path}")


if __name__ == "__main__":
    main()
