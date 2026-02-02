# Topology Inference — Scripts Reference

All commands are run from the **repository root**.

The virtualenv required by the plotting scripts lives at `env/`:

```bash
python3 -m venv env
source env/bin/activate
pip install -r simulator/topology_inference/requirements.txt
```

This repository works in tandem with the `GossipSub-Minimal` prototype repository: https://github.com/V1rg1lee/GossipSub-Minimal/ - the real-deployment comparison and validation pipelines require results from both repositories.

---

## Table of contents

1. [Scenarios](#1-scenarios)
2. [Reproduce the thesis campaign](#2-reproduce-the-thesis-campaign)
3. [`run_passive_ego_mesh_campaign.py`](#3-run_passive_ego_mesh_campaignpy)
4. [`run_passive_coalition_campaign.py`](#4-run_passive_coalition_campaignpy)
5. [`run_px_flood_campaign.py`](#5-run_px_flood_campaignpy)
6. [`run_passive_ego_mesh.py`](#6-run_passive_ego_meshpy)
7. [`run_passive_coalition.py`](#7-run_passive_coalitionpy)
8. [`run_px_flood.py`](#8-run_px_floodpy)
9. [`evaluate_passive_ego_mesh.py`](#9-evaluate_passive_ego_meshpy)
10. [`evaluate_passive_coalition.py`](#10-evaluate_passive_coalitionpy)
11. [`evaluate_px_flood.py`](#11-evaluate_px_floodpy)
12. [`aggregate_passive_ego_mesh.py`](#12-aggregate_passive_ego_meshpy)
13. [`aggregate_passive_coalition.py`](#13-aggregate_passive_coalitionpy)
14. [`aggregate_px_flood.py`](#14-aggregate_px_floodpy)
15. [`analyze_passive_ego_mesh_time_to_thresholds.py`](#15-analyze_passive_ego_mesh_time_to_thresholdspy)
16. [`analyze_passive_coalition_time_to_thresholds.py`](#16-analyze_passive_coalition_time_to_thresholdspy)
17. [`validate_topology_inference_run.py`](#17-validate_topology_inference_runpy)
18. [`plot_inference_results.py`](#18-plot_inference_resultspy)
19. [`plot_topology_visuals.py`](#19-plot_topology_visualspy)
20. [`analyze_simulator_gossipsub_topology.py`, `aggregate_comparison_seeds.py`, and `plot_validation_figures.py`](#20-analyze_simulator_gossipsub_topologypy-aggregate_comparison_seedspy-and-plot_validation_figurespy)
21. [`compare_scenarios.py`](#21-compare_scenariospy)

---

## 1. Scenarios

The two scenarios used in all attack campaigns are:

```
degraded_baseline_r20_windowed_px_control_dout
degraded_scoring_r20_validated
```

They are identified by the suffix after the `gossip_static_` prefix in the config file name. Both run in mono-topic mode on the shared topic `gossipsub-inference`.

The `degraded_scoring_r20_validated` scenario uses `D=8, D_high=15` (wider mesh bounds) and oracle-penalty scoring. The baseline uses `D=6, D_high=8` with scoring disabled.

### Defended PX flood scenarios

Two additional scenarios enable all three PX flood defenses (per-requester backoff, global rate limit, and flood detection). They are only used by the defended campaign (see §2.9 and §5).

| Defended scenario name | Base scenario | Rate-limit budget |
|---|---|---|
| `px_flood_defended` | `degraded_baseline_r20_windowed_px_control_dout` | 24 peers / 30 hb |
| `px_flood_scoring_defended` | `degraded_scoring_r20_validated` | 45 peers / 30 hb |

Config files: `config/experiments/gossip_static_px_flood_defended.cfg` and `config/experiments/gossip_static_px_flood_scoring_defended.cfg`.

All three defenses default to **off** in the Java simulator, so every existing experiment (passive ego-mesh, passive coalition, and undefended PX flood) is completely unaffected.

---

## 2. Reproduce the thesis campaign

This section gives the exact sequence of commands needed to regenerate every result from scratch.

### 2.1 Build the simulator

```bash
cd simulator && mvn -q -DskipTests package && cd ..
```

### 2.2 Run the three attack campaigns

The passive attacks require `--late-join --start-heartbeat 11` so that observers only join the mesh after the network has stabilised (heartbeat 11 is when the honest mesh reaches steady state).

```bash
# Passive Ego Mesh — one attacker (index 0), late-join at heartbeat 11
python3 simulator/topology_inference/run_passive_ego_mesh_campaign.py \
  --late-join \
  --start-heartbeat 11

# Passive Coalition — coalition sizes 1 2 4 8, late-join at heartbeat 11
python3 simulator/topology_inference/run_passive_coalition_campaign.py \
  --late-join \
  --start-heartbeat 11

# PX Flood — full thesis campaign (6 phases, 36 runs total)
python3 simulator/topology_inference/run_px_flood_campaign.py --full-campaign
```

For PX flood, the six phases run automatically in order:

| Phase | Depth | Coalition sizes | Scenarios |
|---|---|---|---|
| 1 | 1 | 4, 8 | All 2 |
| 2 | 1 | 16 | `degraded_scoring_r20_validated` only |
| 3 | 2 | 8 | `degraded_baseline_r20_windowed_px_control_dout` only |
| 4 | 2 | 16 | `degraded_scoring_r20_validated` only |
| 5 | 3 | 16 | `degraded_scoring_r20_validated` only |

Phase 5 is the limit-exploration run: simulation time is extended to ~816 s (validated) so that the inference can exhaust the network without hitting the time budget.


Each campaign evaluates every run automatically. Outputs land under:

```
simulator/results/topology_inference/passive_ego_mesh/
simulator/results/topology_inference/passive_coalition/
simulator/results/topology_inference/px_flood/
```

### 2.3 Aggregate results into summary CSV files

```bash
python3 simulator/topology_inference/aggregate_passive_ego_mesh.py
python3 simulator/topology_inference/aggregate_passive_coalition.py
python3 simulator/topology_inference/aggregate_px_flood.py
```

Outputs:

```
passive_ego_mesh/summary/passive_ego_mesh_run_metrics.csv
passive_ego_mesh/summary/passive_ego_mesh_scenario_summary.csv
passive_coalition/summary/passive_coalition_run_metrics.csv
passive_coalition/summary/passive_coalition_scenario_summary.csv
passive_coalition/summary/passive_coalition_topic_metrics.csv
passive_coalition/summary/passive_coalition_topic_summary.csv
px_flood/summary/px_flood_run_metrics.csv
px_flood/summary/px_flood_scenario_summary.csv
```

### 2.4 Compute time-to-knowledge analyses

```bash
python3 simulator/topology_inference/analyze_passive_ego_mesh_time_to_thresholds.py
python3 simulator/topology_inference/analyze_passive_coalition_time_to_thresholds.py
```

Outputs:

```
passive_ego_mesh/summary/passive_ego_mesh_time_to_thresholds_run_metrics.csv
passive_ego_mesh/summary/passive_ego_mesh_time_to_thresholds_scenario_summary.csv
passive_ego_mesh/summary/passive_ego_mesh_recall_timeseries.csv
passive_coalition/summary/passive_coalition_time_to_thresholds_run_metrics.csv
passive_coalition/summary/passive_coalition_time_to_thresholds_scenario_summary.csv
passive_coalition/summary/passive_coalition_frontier_recall_timeseries.csv
```

### 2.5 Generate attack figures

```bash
source env/bin/activate
python3 simulator/topology_inference/plot_inference_results.py
```

Figures are written under `simulator/results/topology_inference/figures/`.

### 2.6 Run the defended PX flood campaign (optional)

This reproduces the attack under all three countermeasures and stores results in a separate directory tree, independent of the undefended campaign above.

```bash
# Run the defended campaign (same phases as the undefended one)
python3 simulator/topology_inference/run_px_flood_campaign.py --full-campaign --defended

# Aggregate defended results separately
python3 simulator/topology_inference/aggregate_px_flood.py \
  --output-root simulator/results/topology_inference/px_flood_defended

# Plot defended figures alongside the undefended ones
source env/bin/activate
python3 simulator/topology_inference/plot_inference_results.py --attacks px_flood --defended
```

Outputs land under:

```
simulator/results/topology_inference/px_flood_defended/
```

The defended scenarios (`px_flood_defended`, `px_flood_scoring_defended`) appear as additional columns in the PX flood coverage bar chart when `--defended` is passed to the plot script.

### 2.7 Measure resource usage (optional)

The following commands reproduce the performance measurements reported in the thesis (section "Simulator Environment"). They wrap the JVM process directly with `/usr/bin/time -v`, which reports the peak resident set size (RSS) as tracked by the Linux kernel. Run from the **repository root**:

```bash
cd simulator
for seed in 24680 13579 98765; do
  mkdir -p /tmp/px_bench_$seed
  echo "=== seed=$seed ==="
  { /usr/bin/time -v ./run.sh \
    config/gossipdas1k.cfg \
    "config/experiments/gossip_static_degraded_baseline_r20_windowed_px_control_dout.cfg" \
    "random.seed=$seed" \
    "control.4.logfolder=/tmp/px_bench_$seed" \
    "SIM_TIME=168000" \
    "control.0traffic.interrupt_after_second_block=false" \
    "gossipsub.graft_rejection_px_enabled=true" \
    "gossipsub.inference.px_flood.enabled=true" \
    "gossipsub.inference.px_flood.attacker_indices=0,1,2,3,4,5,6,7" \
    "gossipsub.inference.px_flood.initial_target_index=50" \
    "gossipsub.inference.px_flood.wave_depth=1" \
    "gossipsub.inference.px_flood.wave_timeout_heartbeats=2" \
    "gossipsub.inference.px_flood.scenario_name=degraded_baseline_depth1" \
    2>&1; } 2>&1 | grep -E "Maximum resident|Elapsed|User time|System time"
done
cd ..
```

**Reference measurements** (1 000-node network, depth-1 PX flood, coalition size 8, OpenJDK 17.0.18, x86-64):

| Seed | Real time | CPU time | Ratio | Peak RSS |
|---|---|---|---|---|
| 24680 | 50 s | 46 s | 0.92 | 6.0 GB |
| 13579 | 54 s | 48 s | 0.89 | 6.7 GB |
| 98765 | 50 s | 47 s | 0.94 | 4.7 GB |
| **Mean** | **51 s** | **47 s** | **0.92** | **5.8 GB** |

~5–8 MB peak RSS per simulated node. Wall-clock time depends on CPU single-thread performance and is not reproducible across machines; peak RSS is stable to within ~30 % for the same JDK version.

### 2.8 Generate simulator-vs-real validation figures (separate pipeline)

```bash
./compare_w_gossipsub.sh 20 50 100
source env/bin/activate
python3 simulator/topology_inference/plot_validation_figures.py \
  --sim-root simulator/results/gossipsub_comparison \
  --real-root /root/virgile/GossipSub-Minimal/results \
  --sizes 20-nodes 50-nodes 100-nodes
```
`real-root` should point to the directory containing the real Grid5000 GossipSub results from the `GossipSub-Minimal` repository.

`compare_w_gossipsub.sh` runs every requested simulator size over the configured seed set, writes one `seed_<seed>/` directory per run, and then calls `aggregate_comparison_seeds.py` to create the averaged simulator metrics used by the validation figures.

Figures are written under `simulator/results/gossipsub_validation_figures/`.

---

## 3. `run_passive_ego_mesh_campaign.py`

Launches the full passive ego-mesh campaign over all standard scenarios and seeds, evaluates each run, and writes `campaign_manifest.json`.

**Defaults:** 3 scenarios · 3 seeds (24680, 13579, 98765) · attacker index 0 · no late-join.

```
usage: run_passive_ego_mesh_campaign.py [-h]
  [--scenarios SCENARIOS [SCENARIOS ...]]
  [--output-root OUTPUT_ROOT]
  [--attacker-index ATTACKER_INDEX]
  [--late-join]
  [--start-heartbeat START_HEARTBEAT]
  [--late-join-topics [LATE_JOIN_TOPICS ...]]
```

| Option | Description |
|---|---|
| `--scenarios` | Subset of the 4 standard scenario names (no `gossip_static_` prefix). |
| `--output-root` | Root output directory (default: `simulator/results/topology_inference/passive_ego_mesh`). |
| `--attacker-index` | Fixed attacker network index for the whole campaign (default: `0`). |
| `--late-join` | Keep the observer dormant until `--start-heartbeat`, then join. **Required for thesis results.** |
| `--start-heartbeat` | Heartbeat at which the late-join observer activates (default: `10`). **Use `11` for the thesis.** |
| `--late-join-topics` | Topics joined immediately on activation. If omitted, the configured mono-topic is used. |

**Typical usage:**

```bash
# Thesis reproduction
python3 simulator/topology_inference/run_passive_ego_mesh_campaign.py \
  --late-join --start-heartbeat 11

# Only two scenarios, custom output
python3 simulator/topology_inference/run_passive_ego_mesh_campaign.py \
  --scenarios degraded_baseline_r20_windowed_px_control_dout degraded_scoring_r20_validated \
  --late-join --start-heartbeat 11 \
  --output-root simulator/results/my_ego_mesh_run
```

---

## 4. `run_passive_coalition_campaign.py`

Launches the full passive coalition campaign. Evaluates each run automatically.

**Defaults:** 3 scenarios · 3 seeds · coalition sizes 1, 2, 4, 8 · attacker indices `[0..k-1]` · no late-join.

```
usage: run_passive_coalition_campaign.py [-h]
  [--scenarios SCENARIOS [SCENARIOS ...]]
  [--output-root OUTPUT_ROOT]
  [--coalition-sizes COALITION_SIZES [COALITION_SIZES ...]]
  [--late-join]
  [--start-heartbeat START_HEARTBEAT]
  [--late-join-topics [LATE_JOIN_TOPICS ...]]
```

| Option | Description |
|---|---|
| `--scenarios` | Subset of the 4 standard scenario names. |
| `--output-root` | Root output directory (default: `simulator/results/topology_inference/passive_coalition`). |
| `--coalition-sizes` | One or more coalition sizes; attacker indices are `[0..k-1]` (default: `1 2 4 8`). |
| `--late-join` | Keep observers dormant until `--start-heartbeat`. **Required for thesis results.** |
| `--start-heartbeat` | Heartbeat at which the coalition activates (default: `10`). **Use `11` for the thesis.** |
| `--late-join-topics` | Topics joined on activation. |

**Typical usage:**

```bash
# Thesis reproduction
python3 simulator/topology_inference/run_passive_coalition_campaign.py \
  --late-join --start-heartbeat 11

# Only coalition sizes 4 and 8
python3 simulator/topology_inference/run_passive_coalition_campaign.py \
  --coalition-sizes 4 8 \
  --late-join --start-heartbeat 11
```

---

## 5. `run_px_flood_campaign.py`

Launches the full PX flood campaign. Evaluates each run automatically.

**Defaults:** 3 scenarios · 3 seeds · coalition sizes 4, 8 · initial target index 50 · wave depth 1 · wave timeout 2.

```
usage: run_px_flood_campaign.py [-h]
  [--scenarios SCENARIOS [SCENARIOS ...]]
  [--seeds SEEDS [SEEDS ...]]
  [--coalition-sizes COALITION_SIZES [COALITION_SIZES ...]]
  [--initial-target-index INITIAL_TARGET_INDEX]
  [--wave-depth WAVE_DEPTH]
  [--wave-timeout-heartbeats WAVE_TIMEOUT_HEARTBEATS]
  [--prune-peers PRUNE_PEERS]
  [--output-root OUTPUT_ROOT]
  [--no-eval]
  [--full-campaign]
  [--defended]
```

| Option | Description |
|---|---|
| `--scenarios` | Subset of the 4 standard scenario names. |
| `--seeds` | Random seeds (default: `24680 13579 98765`). |
| `--coalition-sizes` | Number of attacker nodes; indices are `[0..k-1]` (default: `4 8`). |
| `--initial-target-index` | Network index of the first flooded node (default: `50`). |
| `--wave-depth` | `1` = flood target + PX peers; `2` = one more level; `3` = two more levels (default: `1`). |
| `--wave-timeout-heartbeats` | Heartbeats to wait before collecting PX results (default: `2`). |
| `--prune-peers` | Used only for `SIM_TIME` estimation (default: `16`). |
| `--output-root` | Root output directory. Defaults to `px_flood/` or `px_flood_defended/` depending on `--defended`. |
| `--no-eval` | Skip evaluation after each run (useful for faster iteration). |
| `--full-campaign` | Run all six thesis phases automatically (see below). Ignores `--scenarios`, `--coalition-sizes`, and `--wave-depth`. |
| `--defended` | Switch every scenario to its defended counterpart (`px_flood_defended` / `px_flood_scoring_defended`) and write results under `simulator/results/topology_inference/px_flood_defended/`. All three PX flood defenses (per-requester backoff, global rate limit, flood detection) are activated via the corresponding config files. |

**`--full-campaign` phases (27 runs total):**

| Phase | Depth | Coalitions | Scenarios |
|---|---|---|---|
| 1 | 1 | 4, 8 | All 2 |
| 2 | 1 | 16 | `degraded_scoring_r20_validated` only |
| 3 | 2 | 8 | `degraded_baseline_r20_windowed_px_control_dout` only |
| 4 | 2 | 16 | `degraded_scoring_r20_validated` only |
| 5 | 3 | 16 | `degraded_scoring_r20_validated` only |

Phase 5 is the limit-exploration run: `SIM_TIME` is extended to ~816 s (validated) so that the inference exhausts the reachable subgraph without hitting the time budget.

**Typical usage:**

```bash
# Thesis reproduction — all phases in one command
python3 simulator/topology_inference/run_px_flood_campaign.py --full-campaign

# Default only (depth 1, coalition 4 and 8, all scenarios)
python3 simulator/topology_inference/run_px_flood_campaign.py

# One-off: coalition 16, validated scenario, depth 1
python3 simulator/topology_inference/run_px_flood_campaign.py \
  --scenarios degraded_scoring_r20_validated \
  --coalition-sizes 16

# Wave depth 2, coalition 8 only, baseline scenario
python3 simulator/topology_inference/run_px_flood_campaign.py \
  --scenarios degraded_baseline_r20_windowed_px_control_dout \
  --coalition-sizes 8 \
  --wave-depth 2
```

---

## 6. `run_passive_ego_mesh.py`

Runs a single passive ego-mesh experiment. Mostly useful for debugging or testing a specific parameter combination.

```
usage: run_passive_ego_mesh.py [-h]
  --scenario SCENARIO
  --seed SEED
  --attacker-index ATTACKER_INDEX
  [--late-join]
  [--start-heartbeat START_HEARTBEAT]
  [--late-join-topics [LATE_JOIN_TOPICS ...]]
  [--output-root OUTPUT_ROOT]
```

| Option | Description |
|---|---|
| `--scenario` | Scenario name (required, no `gossip_static_` prefix). |
| `--seed` | Random seed (required). |
| `--attacker-index` | Attacker network index (required). |
| `--late-join` | Delay observer activation. |
| `--start-heartbeat` | Heartbeat at which a late-join observer activates. |
| `--output-root` | Root output directory. |

**Example:**

```bash
python3 simulator/topology_inference/run_passive_ego_mesh.py \
  --scenario degraded_scoring_r20_validated \
  --seed 24680 \
  --attacker-index 0 \
  --late-join \
  --start-heartbeat 11
```

---

## 7. `run_passive_coalition.py`

Runs a single passive coalition experiment.

```
usage: run_passive_coalition.py [-h]
  --scenario SCENARIO
  --seed SEED
  --attacker-indices ATTACKER_INDICES [ATTACKER_INDICES ...]
  [--late-join]
  [--start-heartbeat START_HEARTBEAT]
  [--late-join-topics [LATE_JOIN_TOPICS ...]]
  [--output-root OUTPUT_ROOT]
```

| Option | Description |
|---|---|
| `--scenario` | Scenario name (required). |
| `--seed` | Random seed (required). |
| `--attacker-indices` | Space-separated network indices of the coalition members (required). |
| `--late-join` | Delay all coalition observers until `--start-heartbeat`. |
| `--start-heartbeat` | Activation heartbeat for late-join. |
| `--output-root` | Root output directory. |

**Example:**

```bash
python3 simulator/topology_inference/run_passive_coalition.py \
  --scenario degraded_scoring_r20_validated \
  --seed 24680 \
  --attacker-indices 0 1 2 3 \
  --late-join \
  --start-heartbeat 11
```

---

## 8. `run_px_flood.py`

Runs a single PX flood experiment. The script computes `simulation.endtime` automatically from coalition size and wave depth. The config flag `gossipsub.graft_rejection_px_enabled=true` is injected automatically.

```
usage: run_px_flood.py [-h]
  --scenario SCENARIO
  --seed SEED
  --initial-target-index INITIAL_TARGET_INDEX
  --attacker-indices ATTACKER_INDICES [ATTACKER_INDICES ...]
  [--wave-depth WAVE_DEPTH]
  [--wave-timeout-heartbeats WAVE_TIMEOUT_HEARTBEATS]
  [--prune-peers PRUNE_PEERS]
  [--output-root OUTPUT_ROOT]
```

| Option | Description |
|---|---|
| `--scenario` | Scenario name (required). |
| `--seed` | Random seed (required). |
| `--initial-target-index` | Network index of the first node to flood (required). |
| `--attacker-indices` | Network indices of the coalition attackers (required). |
| `--wave-depth` | Recursion depth: `1` = target + PX peers; `2` = one more level; `3` = two more levels (default: `1`). |
| `--wave-timeout-heartbeats` | Wait before collecting PX results (default: `2`). |
| `--prune-peers` | Used to estimate `SIM_TIME` (default: `16`). |
| `--output-root` | Root output directory. |

**Example:**

```bash
python3 simulator/topology_inference/run_px_flood.py \
  --scenario degraded_baseline_r20_windowed_px_control_dout \
  --seed 24680 \
  --initial-target-index 50 \
  --attacker-indices 0 1 2 3 4 5 6 7 \
  --wave-depth 1
```

**Output artifacts:**

| File | Content |
|---|---|
| `attack_metadata.json` | Attack parameters, `inference_complete` flag, wave statistics. |
| `inferred_topology.json` | Per-target PX-discovered neighbours and coalition union. |
| `wave_report.json` | Per-wave details: target, heartbeat, PX count per attacker. |
| `inference_events.csv` | Event-level log (one row per PX peer discovery). |
| `mesh_snapshots.csv` | Standard heartbeat mesh snapshot (always produced). |

---

## 9. `evaluate_passive_ego_mesh.py`

Recomputes `evaluation.json` for a single ego-mesh run. The campaign script calls this automatically; use it only when re-evaluating an existing run directory.

```
usage: evaluate_passive_ego_mesh.py [-h] --run-dir RUN_DIR
```

**Example:**

```bash
python3 simulator/topology_inference/evaluate_passive_ego_mesh.py \
  --run-dir simulator/results/topology_inference/passive_ego_mesh/degraded_scoring_r20_validated/seed_24680_attacker_0
```

---

## 10. `evaluate_passive_coalition.py`

Recomputes `evaluation.json` for a single coalition run.

```
usage: evaluate_passive_coalition.py [-h] --run-dir RUN_DIR
```

**Example:**

```bash
python3 simulator/topology_inference/evaluate_passive_coalition.py \
  --run-dir simulator/results/topology_inference/passive_coalition/degraded_scoring_r20_validated/seed_24680_attackers_0-1-2-3
```

---

## 11. `evaluate_px_flood.py`

Computes `evaluation.json` for a single PX flood run. The campaign script calls this automatically.

```
usage: evaluate_px_flood.py [-h]
  --run-dir RUN_DIR
  [--skip-perturbation]
```

| Option | Description |
|---|---|
| `--run-dir` | Run directory to evaluate (required). |
| `--skip-perturbation` | Skip the topology perturbation computation (faster, uses less memory). |

**Key metrics in `evaluation.json`:**

| Metric | Meaning |
|---|---|
| `per_target_neighborhood.aggregate.micro_precision` | Fraction of PX-inferred neighbours that are true direct mesh neighbours (PX reveals connected-reserve peers too, so typically 10–25 %). |
| `per_target_neighborhood.aggregate.micro_recall` | Fraction of a target's true mesh neighbours recovered. |
| `discovery_coverage.discovery_coverage` | Fraction of the global mesh reached across all waves. |
| `discovery_coverage.discovery_precision` | Fraction of discovered nodes that are genuine mesh participants (consistently 100 %; PX never invents nodes). |
| `topology_perturbation.final_topology_drift` | Jaccard distance between the honest mesh just before the first wave and just after the last wave. 0 = no change; near 1 = near-complete replacement. |
| `topology_perturbation.mean_topology_drift_during_attack` | Average drift across all heartbeats during the attack window. |
| `degree_bias.high_degree_recall` | Fraction of the top 20% most-connected honest nodes discovered. Values above `discovery_coverage` indicate hub preference. |
| `degree_bias.low_degree_recall` | Fraction of the bottom 80% least-connected honest nodes discovered. |
| `degree_bias.discovery_lift` | `high_degree_recall / discovery_coverage`. Values > 1 mean the attack finds hubs at a higher rate than the overall coverage. |
| `degree_bias.high_degree_share_in_discovered` | Fraction of all discovered nodes that belong to the top 20%. Values above 0.20 confirm hub bias. |

**Example:**

```bash
python3 simulator/topology_inference/evaluate_px_flood.py \
  --run-dir simulator/results/topology_inference/px_flood/degraded_baseline_r20_windowed_px_control_dout_depth1/seed_24680_target_50_attackers_0-1-2-3-4-5-6-7
```

---

## 12. `aggregate_passive_ego_mesh.py`

Reads all `evaluation.json` files in the ego-mesh campaign directory and writes summary CSV files.

```
usage: aggregate_passive_ego_mesh.py [-h] [--output-root OUTPUT_ROOT]
```

| Option | Description |
|---|---|
| `--output-root` | Campaign root directory (default: `simulator/results/topology_inference/passive_ego_mesh`). |

**Outputs under `<output-root>/summary/`:**

```
passive_ego_mesh_run_metrics.csv       — one row per run
passive_ego_mesh_scenario_summary.csv  — median + IQR per scenario
```

---

## 13. `aggregate_passive_coalition.py`

Reads all `evaluation.json` files in the coalition campaign directory and writes summary CSV files.

```
usage: aggregate_passive_coalition.py [-h] [--output-root OUTPUT_ROOT]
```

| Option | Description |
|---|---|
| `--output-root` | Campaign root directory (default: `simulator/results/topology_inference/passive_coalition`). |

**Outputs under `<output-root>/summary/`:**

```
passive_coalition_run_metrics.csv      — one row per run
passive_coalition_scenario_summary.csv — median + IQR per (scenario, coalition_size)
passive_coalition_topic_metrics.csv    — per-topic coverage metrics per run
passive_coalition_topic_summary.csv    — median + IQR per (scenario, coalition_size, topic)
```

---

## 14. `aggregate_px_flood.py`

Reads all `evaluation.json` files in the PX flood campaign directory and writes summary CSV files.

```
usage: aggregate_px_flood.py [-h] [--output-root OUTPUT_ROOT]
```

| Option | Description |
|---|---|
| `--output-root` | Campaign root directory (default: `simulator/results/topology_inference/px_flood`). |

**Outputs under `<output-root>/summary/`:**

```
px_flood_run_metrics.csv        — one row per run
px_flood_scenario_summary.csv   — median + IQR per (scenario, coalition_size, wave_depth)
```

---

## 15. `analyze_passive_ego_mesh_time_to_thresholds.py`

Reads `inference_events.csv` from each ego-mesh run and computes the heartbeat at which the observer first crosses each recall threshold.

```
usage: analyze_passive_ego_mesh_time_to_thresholds.py [-h]
  [--output-root OUTPUT_ROOT]
  [--heartbeat-ms HEARTBEAT_MS]
  [--thresholds THRESHOLDS [THRESHOLDS ...]]
```

| Option | Description |
|---|---|
| `--output-root` | Campaign root directory. |
| `--heartbeat-ms` | Duration of one heartbeat in milliseconds, used to compute `*_ms` columns (default: `1000`). |
| `--thresholds` | Recall thresholds as fractions in [0, 1] (default: `0.5 0.9 0.95 1.0`). |

**Outputs under `<output-root>/summary/`:**

```
passive_ego_mesh_time_to_thresholds_run_metrics.csv
passive_ego_mesh_time_to_thresholds_scenario_summary.csv
passive_ego_mesh_recall_timeseries.csv
```

The columns `time_to_50_percent`, `time_to_90_percent`, `time_to_95_percent`, `time_to_100_percent` give the heartbeat index; `*_heartbeat` and `*_ms` aliases are also exported.

---

## 16. `analyze_passive_coalition_time_to_thresholds.py`

Reads `inference_events.csv` from each coalition run and reconstructs the non-directed frontier-edge union known by the coalition over time, then computes threshold crossing times.

```
usage: analyze_passive_coalition_time_to_thresholds.py [-h]
  [--output-root OUTPUT_ROOT]
  [--heartbeat-ms HEARTBEAT_MS]
  [--thresholds THRESHOLDS [THRESHOLDS ...]]
```

Same options as the ego-mesh version. Results are grouped by both scenario and coalition size because different `k` values change the reference frontier.

**Outputs under `<output-root>/summary/`:**

```
passive_coalition_time_to_thresholds_run_metrics.csv
passive_coalition_time_to_thresholds_scenario_summary.csv
passive_coalition_frontier_recall_timeseries.csv
```

---

## 17. `validate_topology_inference_run.py`

Checks that a run directory contains the expected output files and that snapshots and evaluation data are internally coherent.

```
usage: validate_topology_inference_run.py [-h] --run-dir RUN_DIR
```

Expected files:

```
mesh_snapshots.csv
attack_metadata.json
inferred_topology.json
inference_events.csv
evaluation.json
```

**Example:**

```bash
python3 simulator/topology_inference/validate_topology_inference_run.py \
  --run-dir simulator/results/topology_inference/passive_ego_mesh/degraded_scoring_r20_validated/seed_24680_attacker_0
```

---

## 18. `plot_inference_results.py`

Produces all static inference figures from the aggregated CSV files. Requires the virtualenv.

```
usage: plot_inference_results.py [-h]
  [--source-root SOURCE_ROOT]
  [--output-dir OUTPUT_DIR]
  [--attacks {passive_ego_mesh,passive_coalition,px_flood} [...]]
  [--strict]
  [--defended]
```

| Option | Description |
|---|---|
| `--source-root` | Root containing `results/topology_inference/` (default: repo root). |
| `--output-dir` | Directory where figures are written (default: `simulator/results/topology_inference/figures`). |
| `--attacks` | Subset of attacks to plot. Omit to plot all three. |
| `--strict` | Fail immediately if a required CSV or column is missing. |
| `--defended` | Read PX flood results from `px_flood_defended/` instead of `px_flood/`, and include `px_flood_defended` and `px_flood_scoring_defended` as labelled columns in the coverage/precision/recall/drift bar charts. |

**Figures produced (PDF):**

*Passive ego-mesh (`figures/passive_ego_mesh/`):*
- `passive_ego_mesh_f1_by_scenario.pdf` — Undirected F1 by scenario
- `passive_ego_mesh_recall_plateau.pdf` — Recall plateau over time
- `passive_ego_mesh_time_to_thresholds.pdf` — Time-to-knowledge curves

*Passive coalition (`figures/passive_coalition/`):*
- `passive_coalition_frontier_recall_plateau_by_k_and_scenario.pdf` — Frontier recall over time per coalition size
- `passive_coalition_frontier_undirected_f1_vs_k.pdf` — F1 vs coalition size
- `passive_coalition_topic_undirected_edge_coverage_vs_k.pdf` — Edge coverage vs coalition size
- `passive_coalition_topic_undirected_node_coverage_vs_k.pdf` — Node coverage vs coalition size

*PX flood (`figures/px_flood/`):*
- `px_flood_coverage_bar.pdf` / `px_flood_coverage_bar_depth2.pdf` — Discovery coverage by scenario and coalition size
- `px_flood_wave_progression.pdf` — Cumulative unique peers discovered per wave (cross-wave deduplicated)
- `px_flood_precision_bar.pdf` — Per-target neighborhood precision
- `px_flood_recall_bar.pdf` — Per-target neighborhood recall
- `px_flood_drift_bar.pdf` — Network topology perturbation drift
- `px_flood_degree_bias_depth2.pdf` — High-degree vs low-degree node discovery rate for depth-2 PX flood only. Depth-1 degree-bias figures are intentionally not produced.
- `px_flood_limit_exploration_wave_progression.pdf` — Depth-3 limit-exploration curve for `validated` (cs=16); shows how far the attack reaches when simulation time is not the bottleneck

**Typical usage:**

```bash
source env/bin/activate

# All attacks (undefended)
python3 simulator/topology_inference/plot_inference_results.py

# PX flood with defended scenarios included
python3 simulator/topology_inference/plot_inference_results.py --attacks px_flood --defended

# One attack only
python3 simulator/topology_inference/plot_inference_results.py --attacks passive_coalition

# Custom paths
python3 simulator/topology_inference/plot_inference_results.py \
  --source-root simulator/results/my_campaign \
  --output-dir simulator/results/my_campaign/figures \
  --attacks passive_ego_mesh
```

---

## 19. `plot_topology_visuals.py`

Produces detailed per-run visualizations for a single passive ego-mesh run. Requires the virtualenv.

```
usage: plot_topology_visuals.py [-h]
  --run-dir RUN_DIR
  [--output-dir OUTPUT_DIR]
  [--layout {radial,spring,kamada}]
  [--topic TOPIC]
  [--max-node-labels MAX_NODE_LABELS]
```

| Option | Description |
|---|---|
| `--run-dir` | Run directory containing `inferred_topology.json` and `mesh_snapshots.csv` (required). |
| `--output-dir` | Output directory (default: `<run-dir>/figures`). |
| `--layout` | Graph layout algorithm: `radial` (default), `spring`, or `kamada`. |
| `--topic` | Topic to visualize: `auto` (readable deterministic slice), `all` (every topic), or an explicit topic name. |
| `--max-node-labels` | Draw non-attacker labels only up to this many nodes (default: `30`). |

Two figures are produced per topic:
- **ground-truth ego-mesh** — the attacker's true mesh neighborhood at simulation end.
- **inference comparison** — side-by-side of ground truth vs. inferred neighbors, color-coded by correctness (true positive, false positive, false negative).

**Example:**

```bash
source env/bin/activate
python3 simulator/topology_inference/plot_topology_visuals.py \
  --run-dir simulator/results/topology_inference/passive_ego_mesh/degraded_scoring_r20_validated/seed_24680_attacker_0 \
  --layout radial \
  --topic auto
```

---

## 20. `analyze_simulator_gossipsub_topology.py`, `aggregate_comparison_seeds.py`, and `plot_validation_figures.py`

These scripts form the simulator-vs-real-GossipSub validation pipeline. They are independent of the three inference attacks.

### `analyze_simulator_gossipsub_topology.py`

Converts a simulator raw output directory into the common topology format and runs the topology analysis pipeline (compatible with real Grid5000 traces).

```
usage: analyze_simulator_gossipsub_topology.py [-h]
  simulator_log_dir
  --out-dir OUT_DIR
  [--pipeline PIPELINE]
  [--topic TOPIC]
```

| Argument / Option | Description |
|---|---|
| `simulator_log_dir` | Directory containing `mesh_snapshots.csv` (positional, required). |
| `--out-dir` | Output directory (required). |
| `--pipeline` | Path to the real GossipSub `topology_pipeline.py` (optional). |
| `--topic` | Topic filter applied when preparing snapshots. |

The simplest path is the top-level shell script which runs the simulator at several network sizes and then calls this script automatically:

```bash
./compare_w_gossipsub.sh 20 50 100
```

To run manually on an existing simulator output:

```bash
python3 simulator/topology_inference/analyze_simulator_gossipsub_topology.py \
  simulator/results/gossipsub_comparison/20-nodes/raw \
  --out-dir simulator/results/gossipsub_comparison/20-nodes/topology_pipeline \
  --topic gossipsub-scoring
```

### `aggregate_comparison_seeds.py`

Aggregates per-seed simulator comparison results for one network size into the common averaged metrics directory consumed by `plot_validation_figures.py`.

```
usage: aggregate_comparison_seeds.py <size_dir>
```

| Argument | Description |
|---|---|
| `size_dir` | Directory containing `seed_*/topology_pipeline/metrics/` subdirectories, for example `simulator/results/gossipsub_comparison/50-nodes`. |

Expected input layout:

```
<size_dir>/
  seed_24680/topology_pipeline/metrics/
  seed_13579/topology_pipeline/metrics/
  seed_98765/topology_pipeline/metrics/
```

The script writes averaged simulator metrics to:

```
<size_dir>/topology_pipeline/metrics/
```

Files averaged across seeds:
- `summary.json`
- `degree_timeseries.csv`
- `global_graph_timeseries.csv`
- `churn_timeseries.csv`
- `control_timeseries.csv`

Numeric values are averaged arithmetically over all available seeds. Non-numeric fields are copied from the first available seed. Remaining reference files not explicitly averaged are also copied from the first seed if they do not already exist in the aggregate output directory.

This script is normally called automatically by `compare_w_gossipsub.sh` after all seeds for a given size have completed:

```bash
./compare_w_gossipsub.sh 20 50 100
```

Manual example:

```bash
python3 simulator/topology_inference/aggregate_comparison_seeds.py \
  simulator/results/gossipsub_comparison/50-nodes
```

### `plot_validation_figures.py`

Produces the five validation figures comparing simulator and real GossipSub topology metrics across network sizes. Requires the virtualenv.

```
usage: plot_validation_figures.py [-h]
  [--sim-root SIM_ROOT]
  [--real-root REAL_ROOT]
  [--sizes [SIZES ...]]
  [--out-dir OUT_DIR]
  [--stable-window-fraction STABLE_WINDOW_FRACTION]
  [--stable-window-min-rows STABLE_WINDOW_MIN_ROWS]
  [--aggregation {median,mean,last}]
  [--heartbeat-ms HEARTBEAT_MS]
```

| Option | Description |
|---|---|
| `--sim-root` | Root directory of simulator comparison results (default: `simulator/results/gossipsub_comparison`). |
| `--real-root` | Root directory of real Grid5000 GossipSub results. |
| `--sizes` | Size directories to compare, e.g. `20-nodes 50-nodes 100-nodes`. |
| `--out-dir` | Output directory (default: `simulator/results/gossipsub_validation_figures`). |
| `--stable-window-fraction` | Tail fraction used as the stable comparison window (default: `0.3`). |
| `--stable-window-min-rows` | Minimum rows in the stable window (default: `5`). |
| `--aggregation` | Aggregation over the stable window: `median` (default), `mean`, or `last`. |
| `--heartbeat-ms` | Heartbeat duration for time-series axis labels (default: `1000`). |

**Figures produced:**

```
global_metrics_by_size.pdf       — mesh size, degree, connectivity panels
jaccard_timeseries_by_size.pdf   — Jaccard distance over time
metric_ratio_by_size.pdf         — simulator/real ratio per metric
real_vs_sim_scatter.pdf          — scatter plot of paired metric values
stable_mean_degree_by_size.pdf   — stable-window mean degree comparison
```

The simulator side of these figures is expected to come from the aggregated metrics produced by `aggregate_comparison_seeds.py`, so the plotted simulator values are seed averages rather than a single run.

**Example:**

```bash
source env/bin/activate
python3 simulator/topology_inference/plot_validation_figures.py \
  --sim-root simulator/results/gossipsub_comparison \
  --real-root /root/virgile/GossipSub-Minimal/prototype/results \
  --sizes 20-nodes 50-nodes 100-nodes
```

`real-root` should point to the directory containing the real Grid5000 GossipSub results from the `GossipSub-Minimal` repository.
---

## 21. `compare_scenarios.py`

Compares the three scenarios on purely structural topology metrics (no inference), quantifying the topological differences that justify running experiments across scenarios. Requires the virtualenv.

```
usage: compare_scenarios.py [-h]
  [--results-root RESULTS_ROOT]
  [--output-dir OUTPUT_DIR]
```

| Option | Description |
|---|---|
| `--results-root` | Root of the `topology_inference` results directory (default: `simulator/results/topology_inference`). |
| `--output-dir` | Directory where figures and JSON are written (default: `<results-root>/scenario_comparison`). |

**Data source:** reads `mesh_snapshots.csv` from each `passive_ego_mesh/<scenario>_late_join_hb_11/seed_<S>_attacker_0/` run directory (3 seeds per scenario, heartbeats 2–60).

**Figures produced (PDF):**

| File | Content |
|---|---|
| `scenario_jaccard_timeseries.pdf` | Jaccard stability between consecutive heartbeats — shows how quickly each scenario converges and whether it is truly stable at steady state. |
| `scenario_degree_timeseries.pdf` | Mean mesh degree over time — shows the structural difference between scenarios (baseline D≈5, scoring D≈5.5, validated D≈7.6). |
| `scenario_degree_variance_timeseries.pdf` | Degree standard deviation over time — higher variance in scoring scenarios reflects the asymmetry introduced by selective mesh maintenance. |
| `scenario_steady_state_summary.pdf` | Bar chart of mean degree, degree std, and Jaccard stability in the stable window (heartbeats 40–60). |

Also writes `scenario_comparison_summary.json` with the per-scenario stable-window statistics.

**Typical usage:**

```bash
source env/bin/activate
python3 simulator/topology_inference/compare_scenarios.py
```

---

## Quick script reference

| Script | Role | Typical use |
|---|---|---|
| `run_passive_ego_mesh_campaign.py` | Full ego-mesh campaign | Campaign |
| `run_passive_coalition_campaign.py` | Full coalition campaign | Campaign |
| `run_px_flood_campaign.py` | Full PX flood campaign | Campaign |
| `run_passive_ego_mesh.py` | Single ego-mesh run | Debug |
| `run_passive_coalition.py` | Single coalition run | Debug |
| `run_px_flood.py` | Single PX flood run | Debug |
| `evaluate_passive_ego_mesh.py` | Recompute ego-mesh `evaluation.json` | Re-evaluation |
| `evaluate_passive_coalition.py` | Recompute coalition `evaluation.json` | Re-evaluation |
| `evaluate_px_flood.py` | Compute PX flood `evaluation.json` | After each run |
| `aggregate_passive_ego_mesh.py` | Summary CSV files for ego-mesh | After campaign |
| `aggregate_passive_coalition.py` | Summary CSV files for coalition | After campaign |
| `aggregate_px_flood.py` | Summary CSV files for PX flood | After campaign |
| `aggregate_comparison_seeds.py` | Average simulator-vs-real validation metrics across seeds | After `compare_w_gossipsub.sh` per size |
| `analyze_passive_ego_mesh_time_to_thresholds.py` | Recall threshold crossing times | After aggregation |
| `analyze_passive_coalition_time_to_thresholds.py` | Coalition frontier threshold times | After aggregation |
| `validate_topology_inference_run.py` | Artifact sanity check for one run | Quality control |
| `plot_inference_results.py` | All inference figures (from CSV) | After aggregation |
| `plot_topology_visuals.py` | Per-run topology figures | Inspection |
| `analyze_simulator_gossipsub_topology.py` | Convert simulator snapshots for validation | Simulator validation |
| `plot_validation_figures.py` | Simulator-vs-real GossipSub figures | Simulator validation |
| `compare_scenarios.py` | Structural topology comparison across scenarios | After campaign |

## AI utilization

The topology inference campaign and analysis scripts and the enhanced simulator were developed with the assistance of AI.