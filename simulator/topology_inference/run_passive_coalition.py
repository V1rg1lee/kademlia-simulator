from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SIMULATOR_DIR = REPO_ROOT / "simulator"
BASE_CONFIG = "config/gossipdas1k.cfg"
DEFAULT_OUTPUT_ROOT = SIMULATOR_DIR / "results" / "topology_inference" / "passive_coalition"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the passive_coalition prototype.")
    parser.add_argument("--scenario", required=True, help="Scenario name without gossip_static_ prefix.")
    parser.add_argument("--seed", required=True, type=int, help="Random seed.")
    parser.add_argument(
        "--attacker-indices",
        required=True,
        nargs="+",
        type=int,
        help="Attacker network indices resolved internally to real node ids.",
    )
    parser.add_argument(
        "--late-join",
        action="store_true",
        help="Keep coalition observers dormant until --start-heartbeat, then let them join.",
    )
    parser.add_argument(
        "--start-heartbeat",
        type=int,
        default=20,
        help="Heartbeat at which a --late-join coalition becomes active.",
    )
    parser.add_argument(
        "--late-join-topics",
        nargs="*",
        default=[],
        help=(
            "Optional topics joined immediately at activation. If omitted, mono-topic runs join "
            "the configured single topic; DAS multi-topic runs wait for the next block event."
        ),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Root directory for experiment outputs.",
    )
    return parser.parse_args()


def normalize_attacker_indices(values: list[int]) -> list[int]:
    return sorted(dict.fromkeys(values))


def build_run_label(*, scenario: str, late_join: bool, start_heartbeat: int) -> str:
    if not late_join:
        return scenario
    return f"{scenario}_late_join_hb_{start_heartbeat}"


def build_run_dir(
    *,
    scenario: str,
    seed: int,
    attacker_indices: list[int],
    output_root: Path,
    late_join: bool = False,
    start_heartbeat: int = 20,
) -> Path:
    run_label = build_run_label(
        scenario=scenario, late_join=late_join, start_heartbeat=start_heartbeat
    )
    joined = "-".join(str(index) for index in attacker_indices)
    return (output_root / run_label / f"seed_{seed}_attackers_{joined}").resolve()


def build_overlay_path(scenario: str) -> str:
    overlay = SIMULATOR_DIR / "config" / "experiments" / f"gossip_static_{scenario}.cfg"
    if not overlay.exists():
        raise FileNotFoundError(f"Scenario overlay not found: {overlay}")
    return str(overlay.relative_to(SIMULATOR_DIR))


def build_simulator_command(
    *,
    overlay_config: str,
    run_dir: Path,
    seed: int,
    scenario: str,
    attacker_indices: list[int],
    late_join: bool = False,
    start_heartbeat: int = 20,
    late_join_topics: list[str] | None = None,
) -> list[str]:
    attacker_csv = ",".join(str(index) for index in attacker_indices)
    scenario_name = build_run_label(
        scenario=scenario, late_join=late_join, start_heartbeat=start_heartbeat
    )
    cmd = [
        "./run.sh",
        BASE_CONFIG,
        overlay_config,
        f"random.seed={seed}",
        f"control.4.logfolder={run_dir}",
        "gossipsub.inference.passive_coalition.enabled=true",
        f"gossipsub.inference.passive_coalition.attacker_indices={attacker_csv}",
        f"gossipsub.inference.passive_coalition.scenario_name={scenario_name}",
    ]
    if late_join:
        cmd.extend(
            [
                "gossipsub.inference.passive_coalition.late_join=true",
                f"gossipsub.inference.passive_coalition.start_heartbeat={start_heartbeat}",
            ]
        )
        topics = [topic.strip() for topic in (late_join_topics or []) if topic.strip()]
        if topics:
            cmd.append("gossipsub.inference.passive_coalition.join_topics=" + ",".join(topics))
    return cmd


def run_single_experiment(
    *,
    scenario: str,
    seed: int,
    attacker_indices: list[int],
    output_root: Path,
    late_join: bool = False,
    start_heartbeat: int = 20,
    late_join_topics: list[str] | None = None,
) -> Path:
    normalized_indices = normalize_attacker_indices(attacker_indices)
    run_dir = build_run_dir(
        scenario=scenario,
        seed=seed,
        attacker_indices=normalized_indices,
        output_root=output_root,
        late_join=late_join,
        start_heartbeat=start_heartbeat,
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    overlay_config = build_overlay_path(scenario)
    cmd = build_simulator_command(
        overlay_config=overlay_config,
        run_dir=run_dir,
        seed=seed,
        scenario=scenario,
        attacker_indices=normalized_indices,
        late_join=late_join,
        start_heartbeat=start_heartbeat,
        late_join_topics=late_join_topics,
    )
    subprocess.run(cmd, cwd=SIMULATOR_DIR, check=True)

    required = [
        "mesh_snapshots.csv",
        "attack_metadata.json",
        "inferred_topology.json",
        "inference_events.csv",
    ]
    missing = [name for name in required if not (run_dir / name).exists()]
    if missing:
        missing_str = ", ".join(missing)
        raise FileNotFoundError(
            f"Run completed but missing expected outputs in {run_dir}: {missing_str}"
        )
    return run_dir


def main() -> int:
    args = parse_args()
    run_dir = run_single_experiment(
        scenario=args.scenario,
        seed=args.seed,
        attacker_indices=args.attacker_indices,
        output_root=args.output_root,
        late_join=args.late_join,
        start_heartbeat=args.start_heartbeat,
        late_join_topics=args.late_join_topics,
    )
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
