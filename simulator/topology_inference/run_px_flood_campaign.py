from __future__ import annotations

"""
PX flood topology inference campaign.

Runs the attack across all 4 standard scenarios, 3 seeds, and the requested
coalition sizes (default: 4 and 8).  For each run, writes the standard
outputs (attack_metadata.json, inferred_topology.json, wave_report.json,
inference_events.csv) then runs the evaluation to produce evaluation.json.

Usage examples
--------------
# Full thesis campaign (all phases in one command)
python run_px_flood_campaign.py --full-campaign

# Default campaign (coalition 4 and 8, depth 1, all 4 scenarios, 3 seeds)
python run_px_flood_campaign.py

# Deeper exploration (wave_depth 2, coalition 8 only)
python run_px_flood_campaign.py --wave-depth 2 --coalition-sizes 8

# Single scenario quick test
python run_px_flood_campaign.py --scenarios degraded_baseline_r20_windowed_px_control_dout --seeds 42
"""

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from simulator.topology_inference.evaluate_px_flood import (
    evaluate_inference,
    load_attack_metadata,
    load_inferred_topology,
    load_mesh_snapshots,
    load_wave_report,
    write_evaluation,
)
from simulator.topology_inference.run_px_flood import (
    DEFAULT_OUTPUT_ROOT,
    run_single_experiment,
)

SIMULATOR_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT_DEFENDED = (
    SIMULATOR_DIR / "results" / "topology_inference" / "px_flood_defended"
)


SCENARIO_BASELINE = "degraded_baseline_r20_windowed_px_control_dout"
SCENARIO_VALIDATED = "degraded_scoring_r20_validated"
DEFAULT_SCENARIOS = [
    SCENARIO_BASELINE,
    SCENARIO_VALIDATED,
]

# Defended variants — same topology, PX flood defenses enabled.
SCENARIO_BASELINE_DEFENDED = "px_flood_defended"
SCENARIO_VALIDATED_DEFENDED = "px_flood_scoring_defended"
_DEFENDED_MAP = {
    SCENARIO_BASELINE: SCENARIO_BASELINE_DEFENDED,
    SCENARIO_VALIDATED: SCENARIO_VALIDATED_DEFENDED,
}

DEFAULT_SEEDS = [24680, 13579, 98765]
# Coalition sizes requested: 4 and 8.
DEFAULT_COALITION_SIZES = [4, 8]
# Node index used as flood target. Must not be in any coalition (indices 0..k-1).
DEFAULT_INITIAL_TARGET_INDEX = 50
# Wave depth: 1 = flood target + its PX peers.  2 = one level deeper.
DEFAULT_WAVE_DEPTH = 1
DEFAULT_WAVE_TIMEOUT_HEARTBEATS = 2
DEFAULT_PRUNE_PEERS = 16

# Full thesis campaign: ordered list of (scenarios, coalition_sizes, wave_depth) phases.
# Each phase is run with the same seeds and other parameters as the rest of the campaign.
FULL_CAMPAIGN_PHASES = [
    # Phase 1 — depth 1, all scenarios, coalition 4 and 8 (baseline coverage)
    {"scenarios": DEFAULT_SCENARIOS, "coalition_sizes": [4, 8], "wave_depth": 1},
    # Phase 2 — depth 1, validated scenario only, coalition 16
    #            (validated has D_high=15, needs larger coalition to generate rejections)
    {"scenarios": [SCENARIO_VALIDATED], "coalition_sizes": [16], "wave_depth": 1},
    # Phase 3 — depth 2, baseline scenario, coalition 8
    {"scenarios": [SCENARIO_BASELINE], "coalition_sizes": [8], "wave_depth": 2},
    # Phase 4 — depth 2, validated scenario, coalition 16
    {"scenarios": [SCENARIO_VALIDATED], "coalition_sizes": [16], "wave_depth": 2},
    # Phase 5 — depth 3, validated scenario, coalition 16 (limit exploration)
    {"scenarios": [SCENARIO_VALIDATED], "coalition_sizes": [16], "wave_depth": 3},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the PX flood inference campaign over all standard scenarios."
    )
    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=DEFAULT_SCENARIOS,
        help="Scenario names without the gossip_static_ prefix.",
    )
    parser.add_argument(
        "--seeds",
        nargs="+",
        type=int,
        default=DEFAULT_SEEDS,
        help="Random seeds.",
    )
    parser.add_argument(
        "--coalition-sizes",
        nargs="+",
        type=int,
        default=DEFAULT_COALITION_SIZES,
        help="Number of attacker nodes.  Attacker indices are [0..k-1].",
    )
    parser.add_argument(
        "--initial-target-index",
        type=int,
        default=DEFAULT_INITIAL_TARGET_INDEX,
        help="Network index of the first node to flood (default: 50).",
    )
    parser.add_argument(
        "--wave-depth",
        type=int,
        default=DEFAULT_WAVE_DEPTH,
        help=(
            "Recursion depth for flooding.  "
            "1 = target + its PX peers (default).  "
            "2 = also flood the PX peers of those PX peers (slower)."
        ),
    )
    parser.add_argument(
        "--wave-timeout-heartbeats",
        type=int,
        default=DEFAULT_WAVE_TIMEOUT_HEARTBEATS,
        help="Heartbeats to wait after activating a wave before collecting (default 2).",
    )
    parser.add_argument(
        "--prune-peers",
        type=int,
        default=DEFAULT_PRUNE_PEERS,
        help="gossipsub.prune_peers value used for SIM_TIME estimation (default 16).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help=(
            "Root directory for experiment outputs. "
            "Defaults to results/topology_inference/px_flood (or px_flood_defended "
            "when --defended is used)."
        ),
    )
    parser.add_argument(
        "--no-eval",
        action="store_true",
        help="Skip the evaluation step (useful for faster iteration).",
    )
    parser.add_argument(
        "--full-campaign",
        action="store_true",
        help=(
            "Run the complete thesis campaign in five phases: "
            "(1) depth 1, all scenarios, coalition 4+8; "
            "(2) depth 1, validated scenario, coalition 16; "
            "(3) depth 2, baseline scenario, coalition 8; "
            "(4) depth 2, validated scenario, coalition 16; "
            "(5) depth 3, validated scenario, coalition 16. "
            "Ignores --scenarios, --coalition-sizes, and --wave-depth."
        ),
    )
    parser.add_argument(
        "--defended",
        action="store_true",
        help=(
            "Run the defended variants of all scenarios "
            "(px_flood_defended / px_flood_scoring_defended). "
            "Can be combined with --full-campaign."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Evaluation helpers
# ---------------------------------------------------------------------------


def evaluate_run(run_dir: Path) -> dict[str, object]:
    attack_metadata = load_attack_metadata(run_dir)
    inferred_topology = load_inferred_topology(run_dir)
    wave_report = load_wave_report(run_dir)
    snapshots_df = load_mesh_snapshots(run_dir)
    evaluation = evaluate_inference(
        inferred_topology=inferred_topology,
        wave_report=wave_report,
        snapshots_df=snapshots_df,
        attack_metadata=attack_metadata,
        compute_perturbation=True,
    )
    write_evaluation(run_dir, evaluation)
    return evaluation


def format_qc_messages(
    run_dir: Path, attack_metadata: dict[str, object], evaluation: dict[str, object]
) -> list[str]:
    messages: list[str] = []
    if not attack_metadata.get("inference_complete", False):
        messages.append(
            f"[QC] inference_complete=false for {run_dir} "
            f"(waves={attack_metadata.get('waves_completed')})"
        )
    disc = evaluation.get("discovery_coverage", {})
    if int(disc.get("unique_nodes_discovered", 0)) == 0:
        messages.append(f"[QC] zero nodes discovered for {run_dir}")
    agg = evaluation.get("per_target_neighborhood", {}).get("aggregate", {})
    if int(agg.get("targets_evaluated", 0)) == 0:
        messages.append(f"[QC] no targets evaluated for {run_dir}")
    return messages


def format_summary_line(evaluation: dict[str, object]) -> str:
    agg = evaluation.get("per_target_neighborhood", {}).get("aggregate", {})
    disc = evaluation.get("discovery_coverage", {})
    return (
        f"targets={agg.get('targets_evaluated', 0)} "
        f"P={agg.get('micro_precision', 0.0):.3f} "
        f"R={agg.get('micro_recall', 0.0):.3f} "
        f"F1={agg.get('micro_f1', 0.0):.3f} "
        f"J={agg.get('macro_jaccard', 0.0):.3f} | "
        f"discovered={disc.get('unique_nodes_discovered', 0)} "
        f"coverage={disc.get('discovery_coverage', 0.0):.3f} "
        f"disc_precision={disc.get('discovery_precision', 0.0):.3f}"
    )


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def write_campaign_manifest(output_root: Path, rows: list[dict[str, object]]) -> Path:
    manifest_path = output_root / "campaign_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
        f.write("\n")
    return manifest_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_phase(
    *,
    scenarios: list[str],
    coalition_sizes: list[int],
    wave_depth: int,
    seeds: list[int],
    target_index: int,
    wave_timeout_heartbeats: int,
    prune_peers: int,
    output_root: Path,
    no_eval: bool,
    run_offset: int,
    total_runs: int,
) -> tuple[list[dict[str, object]], list[str]]:
    manifest_rows: list[dict[str, object]] = []
    failures: list[str] = []
    run_number = run_offset

    for scenario in scenarios:
        for coalition_size in coalition_sizes:
            attacker_indices = list(range(coalition_size))
            for seed in seeds:
                run_number += 1
                print(
                    f"[RUN {run_number}/{total_runs}] scenario={scenario} "
                    f"seed={seed} coalition={coalition_size} "
                    f"target={target_index} depth={wave_depth}"
                )
                try:
                    run_dir = run_single_experiment(
                        scenario=scenario,
                        seed=seed,
                        initial_target_index=target_index,
                        attacker_indices=attacker_indices,
                        wave_depth=wave_depth,
                        wave_timeout_heartbeats=wave_timeout_heartbeats,
                        prune_peers=prune_peers,
                        output_root=output_root,
                    )

                    attack_metadata = load_attack_metadata(run_dir)

                    if no_eval:
                        print(
                            f"  waves={attack_metadata.get('waves_completed')} "
                            f"discovered={attack_metadata.get('total_px_peers_discovered')} "
                            f"complete={attack_metadata.get('inference_complete')}"
                        )
                        manifest_rows.append(
                            _manifest_row(
                                scenario=scenario,
                                seed=seed,
                                coalition_size=coalition_size,
                                attacker_indices=attacker_indices,
                                target_index=target_index,
                                wave_depth=wave_depth,
                                run_dir=run_dir,
                                qc_flags=[],
                                evaluation=None,
                            )
                        )
                        print(f"[OK] {run_dir}")
                        continue

                    evaluation = evaluate_run(run_dir)
                    qc_messages = format_qc_messages(run_dir, attack_metadata, evaluation)
                    for msg in qc_messages:
                        print(f"  {msg}")
                    summary = format_summary_line(evaluation)
                    print(f"  {summary}")
                    if not qc_messages:
                        print(f"[OK] {run_dir}")

                    manifest_rows.append(
                        _manifest_row(
                            scenario=scenario,
                            seed=seed,
                            coalition_size=coalition_size,
                            attacker_indices=attacker_indices,
                            target_index=target_index,
                            wave_depth=wave_depth,
                            run_dir=run_dir,
                            qc_flags=qc_messages,
                            evaluation=evaluation,
                        )
                    )

                except Exception as exc:  # noqa: BLE001
                    failure = (
                        f"[FAIL] scenario={scenario} seed={seed} "
                        f"coalition={coalition_size} target={target_index}: {exc}"
                    )
                    print(failure)
                    failures.append(failure)
                    manifest_rows.append(
                        _manifest_row(
                            scenario=scenario,
                            seed=seed,
                            coalition_size=coalition_size,
                            attacker_indices=attacker_indices,
                            target_index=target_index,
                            wave_depth=wave_depth,
                            run_dir=None,
                            qc_flags=["RUN_FAILED"],
                            evaluation=None,
                            error=str(exc),
                        )
                    )

    return manifest_rows, failures


def main() -> int:
    args = parse_args()
    if args.output_root is not None:
        output_root = args.output_root.resolve()
    elif args.defended:
        output_root = DEFAULT_OUTPUT_ROOT_DEFENDED.resolve()
    else:
        output_root = DEFAULT_OUTPUT_ROOT.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    seeds = list(dict.fromkeys(args.seeds))
    target_index = args.initial_target_index

    def _maybe_defend(scenario: str) -> str:
        return _DEFENDED_MAP.get(scenario, scenario) if args.defended else scenario

    if args.full_campaign:
        phases = [
            {
                "scenarios": [_maybe_defend(s) for s in p["scenarios"]],
                "coalition_sizes": p["coalition_sizes"],
                "wave_depth": p["wave_depth"],
            }
            for p in FULL_CAMPAIGN_PHASES
        ]
        total_runs = sum(
            len(p["scenarios"]) * len(p["coalition_sizes"]) * len(seeds)
            for p in phases
        )
        print(f"[FULL CAMPAIGN] {len(phases)} phases, {total_runs} total runs")
    else:
        phases = [
            {
                "scenarios": [_maybe_defend(s) for s in dict.fromkeys(args.scenarios)],
                "coalition_sizes": sorted(dict.fromkeys(args.coalition_sizes)),
                "wave_depth": args.wave_depth,
            }
        ]
        total_runs = (
            len(phases[0]["scenarios"]) * len(phases[0]["coalition_sizes"]) * len(seeds)
        )

    all_manifest_rows: list[dict[str, object]] = []
    all_failures: list[str] = []
    run_offset = 0

    for phase_idx, phase in enumerate(phases):
        if args.full_campaign:
            phase_runs = len(phase["scenarios"]) * len(phase["coalition_sizes"]) * len(seeds)
            print(
                f"\n[PHASE {phase_idx + 1}/{len(phases)}] "
                f"depth={phase['wave_depth']} "
                f"coalitions={phase['coalition_sizes']} "
                f"scenarios={len(phase['scenarios'])} "
                f"({phase_runs} runs)"
            )

        rows, failures = run_phase(
            scenarios=phase["scenarios"],
            coalition_sizes=phase["coalition_sizes"],
            wave_depth=phase["wave_depth"],
            seeds=seeds,
            target_index=target_index,
            wave_timeout_heartbeats=args.wave_timeout_heartbeats,
            prune_peers=args.prune_peers,
            output_root=output_root,
            no_eval=args.no_eval,
            run_offset=run_offset,
            total_runs=total_runs,
        )
        all_manifest_rows.extend(rows)
        all_failures.extend(failures)
        run_offset += len(phase["scenarios"]) * len(phase["coalition_sizes"]) * len(seeds)

    manifest_path = write_campaign_manifest(output_root, all_manifest_rows)
    print(f"\n[DONE] campaign manifest → {manifest_path}")

    if all_failures:
        print("[DONE] campaign finished WITH failures:")
        for f in all_failures:
            print(f"  {f}")
        return 1

    print("[DONE] campaign finished successfully")
    return 0


def _manifest_row(
    *,
    scenario: str,
    seed: int,
    coalition_size: int,
    attacker_indices: list[int],
    target_index: int,
    wave_depth: int,
    run_dir: Path | None,
    qc_flags: list[str],
    evaluation: dict[str, object] | None,
    error: str | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "scenario": scenario,
        "seed": seed,
        "coalition_size": coalition_size,
        "attacker_indices": attacker_indices,
        "initial_target_index": target_index,
        "wave_depth": wave_depth,
        "run_dir": str(run_dir) if run_dir else None,
        "evaluation_path": str(run_dir / "evaluation.json") if run_dir else None,
        "qc_flags": qc_flags,
    }
    if error is not None:
        row["error"] = error
    if evaluation is not None:
        agg = evaluation.get("per_target_neighborhood", {}).get("aggregate", {})
        disc = evaluation.get("discovery_coverage", {})
        row["summary"] = {
            "inference_complete": evaluation.get("inference_complete"),
            "waves_completed": evaluation.get("waves_completed"),
            "targets_evaluated": agg.get("targets_evaluated"),
            "micro_precision": agg.get("micro_precision"),
            "micro_recall": agg.get("micro_recall"),
            "micro_f1": agg.get("micro_f1"),
            "macro_jaccard": agg.get("macro_jaccard"),
            "unique_nodes_discovered": disc.get("unique_nodes_discovered"),
            "true_mesh_size": disc.get("true_mesh_size"),
            "discovery_coverage": disc.get("discovery_coverage"),
            "discovery_precision": disc.get("discovery_precision"),
        }
    return row


if __name__ == "__main__":
    raise SystemExit(main())
