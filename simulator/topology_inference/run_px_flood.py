from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SIMULATOR_DIR = REPO_ROOT / "simulator"
BASE_CONFIG = "config/gossipdas1k.cfg"
DEFAULT_OUTPUT_ROOT = SIMULATOR_DIR / "results" / "topology_inference" / "px_flood"

# Heartbeat step in ms (matches HEARTBEAT_STEP in gossipdas1k.cfg)
HEARTBEAT_STEP_MS = 1000
# Inference warm-up (gossipsub.inference_start_heartbeat default = 11)
INFERENCE_WARMUP_HEARTBEATS = 11
# Buffer heartbeats added after the estimated last wave
BUFFER_HEARTBEATS = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the PX flood topology inference attack.\n\n"
            "A coalition of 8 attacker nodes simultaneously GRAFTs a target, "
            "harvests the PX list from the resulting PRUNE, then recursively floods "
            "each discovered PX peer to map the mesh neighborhood."
        )
    )
    parser.add_argument(
        "--scenario",
        required=True,
        help="Scenario name without the 'gossip_static_' prefix.",
    )
    parser.add_argument("--seed", required=True, type=int, help="Random seed.")
    parser.add_argument(
        "--initial-target-index",
        required=True,
        type=int,
        help="Network index of the first node to flood.",
    )
    parser.add_argument(
        "--attacker-indices",
        required=True,
        nargs="+",
        type=int,
        help="Network indices of the 8 coalition attackers.",
    )
    parser.add_argument(
        "--wave-depth",
        type=int,
        default=1,
        help=(
            "Recursion depth for flooding. "
            "1 = flood initial target + its PX peers (default). "
            "2 = also flood the PX peers of those PX peers."
        ),
    )
    parser.add_argument(
        "--wave-timeout-heartbeats",
        type=int,
        default=2,
        help="Heartbeats to wait after activating a wave before collecting its results (default 2).",
    )
    parser.add_argument(
        "--prune-peers",
        type=int,
        default=16,
        help="gossipsub.prune_peers value (used to estimate SIM_TIME, default 16).",
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


def compute_sim_time_ms(
    wave_depth: int,
    wave_timeout_heartbeats: int,
    prune_peers: int,
    coalition_size: int = 1,
) -> int:
    """Compute a conservative SIM_TIME_MS that covers all waves.

    With larger coalitions, more attackers are rejected and each sends a PRUNE+PX.
    Because wasRecentlyPxAdvertised de-duplicates across PRUNEs, the effective unique
    PX peers per wave grows sub-linearly with coalition_size.  We estimate it as
    prune_peers * ceil(max(1, coalition_size - 2) / 2), capped at 4 * prune_peers.

    Max waves per depth level = effective_prune_peers.
    Total waves = sum over d in [0, wave_depth] of effective_prune_peers^d.
    Each wave occupies (1 + wave_timeout_heartbeats) heartbeats.
    """
    rejections = max(1, coalition_size - 2)  # D_high - D = 2 accepted, rest rejected
    scale = max(1, (rejections + 1) // 2)
    effective_prune_peers = min(prune_peers * scale, prune_peers * 4)
    # Cap per-level wave count to avoid exponential blowup at depth>=2.
    # In practice, mesh overlap makes depth-2 waves far fewer than prune_peers^2.
    max_waves_per_level_cap = max(effective_prune_peers, 100)
    max_waves = sum(
        min(effective_prune_peers**d, max_waves_per_level_cap)
        for d in range(wave_depth + 1)
    )
    heartbeats = (
        INFERENCE_WARMUP_HEARTBEATS
        + max_waves * (1 + wave_timeout_heartbeats)
        + BUFFER_HEARTBEATS
    )
    return heartbeats * HEARTBEAT_STEP_MS


def build_run_dir(
    *,
    scenario: str,
    seed: int,
    initial_target_index: int,
    attacker_indices: list[int],
    wave_depth: int,
    output_root: Path,
) -> Path:
    joined = "-".join(str(i) for i in attacker_indices)
    run_label = f"{scenario}_depth{wave_depth}"
    return (
        output_root / run_label / f"seed_{seed}_target_{initial_target_index}_attackers_{joined}"
    ).resolve()


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
    initial_target_index: int,
    attacker_indices: list[int],
    wave_depth: int,
    wave_timeout_heartbeats: int,
    sim_time_ms: int,
) -> list[str]:
    attacker_csv = ",".join(str(i) for i in attacker_indices)
    cmd = [
        "./run.sh",
        BASE_CONFIG,
        overlay_config,
        f"random.seed={seed}",
        f"control.4.logfolder={run_dir}",
        f"SIM_TIME={sim_time_ms}",
        # Prevent the DAS traffic generator from stopping the simulation after the 2nd block.
        # The gossipsub inference campaigns use a single topic, not DAS topics, so this is safe.
        "control.0traffic.interrupt_after_second_block=false",
        # Always enable PX in GRAFT-rejection PRUNEs regardless of the scenario overlay.
        "gossipsub.graft_rejection_px_enabled=true",
        "gossipsub.inference.px_flood.enabled=true",
        f"gossipsub.inference.px_flood.attacker_indices={attacker_csv}",
        f"gossipsub.inference.px_flood.initial_target_index={initial_target_index}",
        f"gossipsub.inference.px_flood.wave_depth={wave_depth}",
        f"gossipsub.inference.px_flood.wave_timeout_heartbeats={wave_timeout_heartbeats}",
        f"gossipsub.inference.px_flood.scenario_name={scenario}_depth{wave_depth}",
    ]
    return cmd


def run_single_experiment(
    *,
    scenario: str,
    seed: int,
    initial_target_index: int,
    attacker_indices: list[int],
    wave_depth: int,
    wave_timeout_heartbeats: int,
    prune_peers: int,
    output_root: Path,
) -> Path:
    normalized_indices = normalize_attacker_indices(attacker_indices)
    run_dir = build_run_dir(
        scenario=scenario,
        seed=seed,
        initial_target_index=initial_target_index,
        attacker_indices=normalized_indices,
        wave_depth=wave_depth,
        output_root=output_root,
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    overlay_config = build_overlay_path(scenario)

    sim_time_ms = compute_sim_time_ms(
        wave_depth=wave_depth,
        wave_timeout_heartbeats=wave_timeout_heartbeats,
        prune_peers=prune_peers,
        coalition_size=len(normalized_indices),
    )

    cmd = build_simulator_command(
        overlay_config=overlay_config,
        run_dir=run_dir,
        seed=seed,
        scenario=scenario,
        initial_target_index=initial_target_index,
        attacker_indices=normalized_indices,
        wave_depth=wave_depth,
        wave_timeout_heartbeats=wave_timeout_heartbeats,
        sim_time_ms=sim_time_ms,
    )

    print(f"[px_flood] SIM_TIME={sim_time_ms}ms (~{sim_time_ms // 1000} hb, max waves ~{(sim_time_ms // 1000 - 21) // 2})")
    print(f"[px_flood] Output: {run_dir}")
    subprocess.run(cmd, cwd=SIMULATOR_DIR, check=True)

    required = [
        "mesh_snapshots.csv",
        "attack_metadata.json",
        "inferred_topology.json",
        "wave_report.json",
        "inference_events.csv",
    ]
    missing = [name for name in required if not (run_dir / name).exists()]
    if missing:
        raise FileNotFoundError(
            f"Run completed but missing expected outputs in {run_dir}: {', '.join(missing)}"
        )

    # Check inference completion (warn only — partial results are still useful)
    metadata_path = run_dir / "attack_metadata.json"
    try:
        with metadata_path.open() as f:
            metadata = json.load(f)
        if not metadata.get("inference_complete", False):
            waves_done = metadata.get("waves_completed", "?")
            targets = metadata.get("total_targets_flooded", "?")
            print(
                f"[px_flood] WARNING: inference not complete "
                f"(waves_completed={waves_done}, targets_flooded={targets}). "
                "Consider increasing --wave-timeout-heartbeats or the scenario SIM_TIME."
            )
        else:
            waves_done = metadata.get("waves_completed", "?")
            discovered = metadata.get("total_px_peers_discovered", "?")
            print(
                f"[px_flood] Inference complete: {waves_done} waves, "
                f"{discovered} unique peers discovered."
            )
    except Exception as exc:
        print(f"[px_flood] WARNING: could not parse attack_metadata.json: {exc}")

    return run_dir


def main() -> int:
    args = parse_args()
    run_dir = run_single_experiment(
        scenario=args.scenario,
        seed=args.seed,
        initial_target_index=args.initial_target_index,
        attacker_indices=args.attacker_indices,
        wave_depth=args.wave_depth,
        wave_timeout_heartbeats=args.wave_timeout_heartbeats,
        prune_peers=args.prune_peers,
        output_root=args.output_root,
    )
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
