#!/bin/bash
set -euo pipefail

cd /root/virgile/kademlia-simulator

(cd simulator && mvn -q -DskipTests package)

COMPARISON_ROOT="${GOSSIPSUB_COMPARISON_ROOT:-simulator/results/gossipsub_comparison}"
GOSSIPSUB_SEEDS="${GOSSIPSUB_SEEDS:-24680 13579 98765}"
GOSSIPSUB_D="${GOSSIPSUB_D:-8}"
GOSSIPSUB_D_LOW="${GOSSIPSUB_D_LOW:-6}"
GOSSIPSUB_D_HIGH="${GOSSIPSUB_D_HIGH:-15}"
GOSSIPSUB_D_SCORE="${GOSSIPSUB_D_SCORE:-6}"
GOSSIPSUB_D_OUT="${GOSSIPSUB_D_OUT:-2}"
GOSSIPSUB_AMBIENT_INTERVAL_HEARTBEATS="${GOSSIPSUB_AMBIENT_INTERVAL_HEARTBEATS:-10}"
GOSSIPSUB_AMBIENT_PEERS_PER_INTERVAL="${GOSSIPSUB_AMBIENT_PEERS_PER_INTERVAL:-1}"

run_sim () {
  N="$1"
  END_MS="$2"
  NAME="$3"
  TOPIC="$4"
  SEED="$5"

  RAW="${COMPARISON_ROOT}/${NAME}/seed_${SEED}/raw"
  OUT="${COMPARISON_ROOT}/${NAME}/seed_${SEED}/topology_pipeline"

  echo "========== Simulator GossipSub mono-topic: ${NAME} (seed=${SEED}) =========="
  echo "nodes=${N}"
  echo "duration_ms=${END_MS}"
  echo "topic=${TOPIC}"

  rm -rf "$RAW" "$OUT"
  mkdir -p "$RAW" "$OUT"

  (
    cd simulator
    ./run.sh \
      config/gossipdas1k.cfg \
      config/experiments/gossip_static_degraded_scoring_r20_validated.cfg \
      random.seed="$SEED" \
      SIZE="$N" \
      network.size="$N" \
      protocol.1uniftr.size="$N" \
      protocol.2unreltr.size="$N" \
      SIM_TIME="$END_MS" \
      simulation.endtime="$END_MS" \
      control.5.step="$END_MS" \
      control.0traffic.interrupt_after_second_block=false \
      gossipsub.score.enabled=true \
      gossipsub.degraded.peer.ratio=0.20 \
      gossipsub.degraded.peer.withhold.probability=1.0 \
      gossipsub.degraded.peer.forward_drop.probability=1.0 \
      gossipsub.single_topic.enabled=true \
      gossipsub.single_topic.name="$TOPIC" \
      gossipsub.d="$GOSSIPSUB_D" \
      gossipsub.d_low="$GOSSIPSUB_D_LOW" \
      gossipsub.d_high="$GOSSIPSUB_D_HIGH" \
      gossipsub.d_score="$GOSSIPSUB_D_SCORE" \
      gossipsub.d_out="$GOSSIPSUB_D_OUT" \
      gossipsub.candidate_ttl_heartbeats=300 \
      gossipsub.candidate_pool_max_per_topic=512 \
      gossipsub.local_connect_attempts_per_topic_per_heartbeat=12 \
      gossipsub.local_connectable_registry_activity_ttl_heartbeats=300 \
      gossipsub.discovery.ambient_interval_heartbeats="$GOSSIPSUB_AMBIENT_INTERVAL_HEARTBEATS" \
      gossipsub.discovery.ambient_peers_per_interval="$GOSSIPSUB_AMBIENT_PEERS_PER_INTERVAL" \
      gossipsub.score.weight_broken_promises=8.0 \
      gossipsub.score.weight_first_deliveries=0.1 \
      gossipsub.score.weight_time_in_mesh=0.0 \
      gossipsub.score.degraded_peer_penalty=100.0 \
      gossipsub.score.promise_timeout_heartbeats=2 \
      gossipsub.score.decay_broken_promises=0.999 \
      gossipsub.opportunistic_graft.ticks=5 \
      gossipsub.opportunistic_graft.peers=2 \
      gossipsub.opportunistic_graft.threshold=0.25 \
      control.4.logfolder="../$RAW"
  )

  python3 simulator/topology_inference/analyze_simulator_gossipsub_topology.py \
    "$RAW" \
    --out-dir "$OUT" \
    --topic "$TOPIC"
}

run_size () {
  case "$1" in
    20)  N=20;  END_MS=182000; NAME=20-nodes;  TOPIC=gossipsub-scoring ;;
    50)  N=50;  END_MS=303000; NAME=50-nodes;  TOPIC=gossipsub-scoring-50 ;;
    100) N=100; END_MS=300000; NAME=100-nodes; TOPIC=gossipsub-scoring-100 ;;
    *)
      echo "Unknown size '$1'. Expected one of: 20, 50, 100" >&2
      exit 1
      ;;
  esac

  for SEED in $GOSSIPSUB_SEEDS; do
    run_sim "$N" "$END_MS" "$NAME" "$TOPIC" "$SEED"
  done

  python3 simulator/topology_inference/aggregate_comparison_seeds.py \
    "${COMPARISON_ROOT}/${NAME}"
}

if [ "$#" -eq 0 ]; then
  run_size 20
  run_size 50
  run_size 100
else
  for size in "$@"; do
    run_size "$size"
  done
fi
