#!/usr/bin/env python3
"""Analyze simulator GossipSub mesh snapshots with the real GossipSub topology pipeline.

The simulator already writes ``mesh_snapshots.csv`` with the shared schema:

    timestamp,heartbeat_index,node_id,peer_id,topic,mesh_size,snapshot_kind

This wrapper prepares a real GossipSub-compatible directory:

    out_dir/snapshots/snapshots.csv
    out_dir/snapshots/nodes.csv
    out_dir/snapshots/trace_events.csv   (best-effort from mesh_events.csv)
    out_dir/metrics/*

Then it calls ``topology_pipeline.py analyze-snapshots`` so real Grid5000 traces
and simulator snapshots are reduced to the same metric CSVs.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import subprocess
from pathlib import Path
from typing import Iterable


SNAPSHOT_FIELDS = [
    "timestamp",
    "heartbeat_index",
    "node_id",
    "peer_id",
    "topic",
    "mesh_size",
    "snapshot_kind",
]

TRACE_EVENT_FIELDS = [
    "timestamp",
    "heartbeat_index",
    "event",
    "topic",
    "source_peer",
    "target_peer",
    "message_count",
    "ihave_count",
    "iwant_count",
    "graft_count",
    "prune_count",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "simulator_log_dir",
        help="Directory containing mesh_snapshots.csv and optionally mesh_events.csv.",
    )
    parser.add_argument("--out-dir", required=True)
    parser.add_argument(
        "--pipeline",
        default="/root/virgile/GossipSub-Minimal/prototype/analysis/topology_pipeline.py",
        help="Path to real GossipSub topology_pipeline.py.",
    )
    parser.add_argument(
        "--topic",
        default="",
        help="Optional topic filter applied while preparing the simulator snapshots.",
    )
    args = parser.parse_args()

    log_dir = Path(args.simulator_log_dir)
    out_dir = Path(args.out_dir)
    pipeline = Path(args.pipeline)

    snapshots_in = log_dir / "mesh_snapshots.csv"
    events_in = log_dir / "mesh_events.csv"
    if not snapshots_in.is_file():
        raise SystemExit(f"missing simulator snapshots: {snapshots_in}")
    if not pipeline.is_file():
        raise SystemExit(f"missing real GossipSub topology pipeline: {pipeline}")

    snapshots_dir = out_dir / "snapshots"
    metrics_dir = out_dir / "metrics"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    snapshots_out = snapshots_dir / "snapshots.csv"
    copy_snapshots(snapshots_in, snapshots_out, args.topic)

    node_ids = sorted(collect_nodes(snapshots_out, events_in if events_in.exists() else None, args.topic))
    write_nodes(snapshots_dir / "nodes.csv", node_ids)

    if events_in.exists():
        convert_mesh_events(events_in, snapshots_dir / "trace_events.csv", args.topic)

    subprocess.run(
        [
            "python3",
            str(pipeline),
            "analyze-snapshots",
            str(snapshots_out),
            "--out-dir",
            str(metrics_dir),
        ],
        check=True,
    )

    print(f"simulator snapshots: {snapshots_out}")
    print(f"simulator metrics: {metrics_dir}")


def copy_snapshots(source: Path, destination: Path, topic_filter: str) -> None:
    if not topic_filter:
        shutil.copyfile(source, destination)
        return

    with source.open() as in_handle, destination.open("w", newline="") as out_handle:
        reader = csv.DictReader(in_handle)
        writer = csv.DictWriter(out_handle, fieldnames=SNAPSHOT_FIELDS)
        writer.writeheader()
        for row in reader:
            if row.get("snapshot_kind") == "heartbeat_marker" or row.get("topic") == topic_filter:
                writer.writerow({field: row.get(field, "") for field in SNAPSHOT_FIELDS})


def collect_nodes(snapshots_path: Path, events_path: Path | None, topic_filter: str) -> set[str]:
    nodes: set[str] = set()
    with snapshots_path.open() as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            add_if_present(nodes, row.get("node_id", ""))
            add_if_present(nodes, row.get("peer_id", ""))

    if events_path is not None:
        with events_path.open() as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if topic_filter and row.get("topic") != topic_filter:
                    continue
                add_if_present(nodes, row.get("node_id", ""))
                add_if_present(nodes, row.get("peer_id", ""))

    return nodes


def write_nodes(path: Path, node_ids: Iterable[str]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["peer_id", "alias", "role"])
        writer.writeheader()
        for node_id in node_ids:
            writer.writerow({
                "peer_id": node_id,
                "alias": node_id,
                "role": infer_role(node_id),
            })


def convert_mesh_events(source: Path, destination: Path, topic_filter: str) -> None:
    with source.open() as in_handle, destination.open("w", newline="") as out_handle:
        reader = csv.DictReader(in_handle)
        writer = csv.DictWriter(out_handle, fieldnames=TRACE_EVENT_FIELDS)
        writer.writeheader()
        for row in reader:
            if topic_filter and row.get("topic") != topic_filter:
                continue

            action = row.get("action", "").strip().upper()
            event = event_name(action)
            if event == "":
                continue

            writer.writerow({
                "timestamp": row.get("timestamp", "0"),
                "heartbeat_index": timestamp_to_bucket(row.get("timestamp", "0")),
                "event": event,
                "topic": row.get("topic", ""),
                "source_peer": row.get("node_id", ""),
                "target_peer": row.get("peer_id", ""),
                "message_count": 0,
                "ihave_count": 0,
                "iwant_count": 0,
                "graft_count": 1 if event == "graft" else 0,
                "prune_count": 1 if event == "prune" else 0,
            })


def event_name(action: str) -> str:
    if action == "GRAFT":
        return "graft"
    if action == "PRUNE":
        return "prune"
    if action == "JOIN":
        return "join"
    if action == "LEAVE":
        return "leave"
    if action == "STATS":
        return "heartbeat"
    return ""


def timestamp_to_bucket(raw_timestamp: str) -> int:
    try:
        timestamp = int(float(raw_timestamp))
    except ValueError:
        timestamp = 0
    return timestamp // 1000 + 1


def add_if_present(nodes: set[str], value: str) -> None:
    value = value.strip()
    if value:
        nodes.add(value)


def infer_role(node_id: str) -> str:
    lowered = node_id.lower()
    if "degraded" in lowered or "evil" in lowered or "dormant" in lowered:
        return "degraded"
    return "node"


if __name__ == "__main__":
    main()
