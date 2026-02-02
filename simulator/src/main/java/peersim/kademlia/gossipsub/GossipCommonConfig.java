package peersim.kademlia.gossipsub;

import peersim.config.Configuration;

/**
 * Fixed Parameters of a kademlia network. They have a default value and can be configured at
 * startup of the network, once only.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class GossipCommonConfig {

  /** Length of Id */
  public static int BITS = 256;

  /** Dimension of k-buckets */
  public static int D_low = 4;

  public static int D = 6;

  public static int D_high = 8;

  // GossipSub v1.1 outbound mesh quota. The default of 2 matches the
  // lightweight v1.1 topology target and stays compatible with D=6.
  public static int D_out = 2;

  // When oversubscribed, retain at least D_score peers by score and fill the
  // remaining survivor slots randomly up to D.
  public static int D_score = 4;

  public static int ttl = 36000;

  // Lightweight PRUNE backoff, expressed in heartbeat ticks.
  // With HEARTBEAT_STEP=1000, the default of 60 approximates the v1.1
  // recommendation of 1 minute.
  public static long pruneBackoffHeartbeats = 60;

  // PX-related PRUNE parameters.
  // PrunePeers matches the v1.1 notion of bounding the number of peers
  // advertised in a PRUNE. AcceptPXThreshold is kept positive, but scaled to
  // the simulator's simplified score range so trusted senders can be observed.
  public static int prunePeers = 16;
  public static double acceptPXThreshold = 0.25;

  // When true, GRAFT-rejection PRUNEs (GRAFT_REJECTION_AT_CAPACITY) also include a PX peer list,
  // matching the real GossipSub spec. Default false to preserve existing experiment behaviour.
  public static boolean graftRejectionPxEnabled = false;

  // PX flood defenses — all default false/0 to preserve existing experiment behaviour.
  // Defense 1: per-requester PX backoff — after giving PX to peer X, block X for N heartbeats.
  public static boolean pxRequesterBackoffEnabled = false;
  public static long pxRequesterBackoffHeartbeats = 200;
  // Defense 2: global rate limit per topic — max budget of PX peers per time window.
  public static boolean pxGlobalRateLimitEnabled = false;
  public static int pxGlobalRateLimitBudget = 45;
  public static long pxGlobalRateLimitWindowHeartbeats = 30;
  // Defense 3: flood detection — if >= threshold simultaneous GRAFTs arrive, suppress PX.
  public static boolean pxFloodDetectionEnabled = false;
  public static int pxFloodDetectionGraftThreshold = 4;
  public static long pxFloodDetectionWindowHeartbeats = 2;

  // Lightweight local candidate-pool aging.
  public static long candidateTtlHeartbeats = 120;
  public static long candidatePxHintTtlHeartbeats = 30;
  public static long candidateSelectedTtlHeartbeats = 10;
  public static long candidateCleanupIntervalHeartbeats = 1;
  public static int candidatePoolMaxPerTopic = 256;
  public static int localConnectAttemptsPerTopicPerHeartbeat = 1;
  public static long localConnectRetryBaseHeartbeats = 4;
  public static long localConnectRetryMaxHeartbeats = 32;
  public static long localConnectedDirectActivityTtlHeartbeats = 20;
  public static long localConnectedPxActivityTtlHeartbeats = 4;
  public static long localConnectedMeshActivityTtlHeartbeats = 4;
  public static long localConnectedFanoutActivityTtlHeartbeats = 4;
  public static long localConnectableDirectActivityTtlHeartbeats = 8;
  public static long localConnectableMeshActivityTtlHeartbeats = 6;
  public static long localConnectableConnectedActivityTtlHeartbeats = 4;
  public static long localConnectablePxActivityTtlHeartbeats = 2;
  public static long localConnectableRegistryActivityTtlHeartbeats = 1;
  public static int localConnectedReserveTargetPerTopic = 1;
  public static long localConnectedMinLifetimeHeartbeats = 3;
  public static long localConnectedMinHoldHeartbeats = 8;
  public static long localReserveMaturationHeartbeats = 8;
  public static boolean localReserveDisconnectProtectionDuringMaturation = true;
  public static int localConnectedIdleConsecutiveThreshold = 3;
  public static long localIdleDisconnectRetryPenaltyHeartbeats = 8;
  public static boolean localConnectedIdleDisconnectEnabled = true;
  public static long pxAdvertiseCooldownHeartbeats = 20;
  public static long localKnownOnlyTtlHeartbeats = 60;
  public static int localPxAdmissionBudgetPerTopicPerHeartbeat = 2;
  public static int localPxKnownOnlyMaxPerTopic = 16;

  // Minimal explicit bootstrap-assisted discovery and dial boundary.
  public static boolean bootstrapAssistEnabled = true;
  public static long bootstrapWindowHeartbeats = 10;
  public static int bootstrapInitialPeersPerNode = 3;
  public static int bootstrapCandidatesPerHeartbeat = 2;
  // Tiny post-warm-up network-level discovery trickle. It models ambient
  // application/libp2p discovery without handing GossipSub a topic-filtered
  // catalogue after bootstrap-assisted formation.
  public static boolean ambientDiscoveryEnabled = true;
  public static long ambientDiscoveryIntervalHeartbeats = 10;
  public static int ambientDiscoveryPeersPerInterval = 1;
  public static int discoveryMaxDialsPerHeartbeat = 2;
  public static double discoveryDialSuccessProbability = 1.0;

  // Attack inference ignores bootstrap-assisted warm-up observations before
  // this global GossipSub heartbeat index.
  public static long inferenceStartHeartbeat = 11;

  // Debug/compatibility escape hatches for the pre-discovery-manager behavior.
  public static boolean discoveryLegacyRegistryBootstrapEnabled = false;
  public static boolean discoveryLegacyLocalConnectEnabled = false;

  // Experimental local-repair relaxation: allow a very small number of
  // received-PRUNE-backed candidates to bypass backoff only for D_out repair.
  public static boolean repairRelaxReceivedPruneForDOut = false;
  public static int repairRelaxReceivedPruneBudgetPerHeartbeat = 1;
  public static double repairRelaxReceivedPruneMinScore = 0.0;
  public static double repairRelaxReceivedPruneMinAgeFraction = 0.0;
  public static boolean receiverRelaxGraftBackoffForDOut = false;
  public static int receiverRelaxGraftBackoffBudgetPerHeartbeat = 1;
  public static double receiverRelaxGraftBackoffMinScore = 0.0;
  public static double receiverRelaxGraftBackoffMinAgeFraction = 0.0;
  public static boolean repairRelaxReceivedPruneLocalGraceEnabled = false;
  public static boolean dOutInplacePromotionEnabled = false;
  public static boolean dOutForcedReseedEnabled = false;

  // Lightweight local peer scoring (heartbeat-driven)
  public static double scoreWeightTimeInMesh = 0.01;
  public static double scoreWeightFirstDeliveries = 1.0;
  public static double scoreWeightInvalidDeliveries = 0.0;
  public static double scoreWeightBrokenPromises = 2.0;
  public static double scoreDegradedPeerPenalty = 0.0;

  // Heartbeat count after which an IWANT promise is considered broken
  public static long scorePromiseTimeoutHeartbeats = 3;

  // Deterministic heartbeat decay for dynamic counters
  public static double scoreDecayFirstDeliveries = 0.98;
  public static double scoreDecayBrokenPromises = 0.995;

  // Upper bound for monotonic mesh heartbeat credit per (topic, peer)
  public static double scoreMeshHeartbeatCap = 100.0;

  // Topic-aware score thresholds used in topology decisions.
  // In GossipSub v1.1 the baseline mesh retention threshold is 0 and the
  // gossip threshold is negative.
  public static double meshScoreThreshold = 0.0;
  public static double gossipScoreThreshold = -1.0;

  // Opportunistic grafting parameters.
  // With HEARTBEAT_STEP=1000, 60 ticks approximates the v1.1 recommendation
  // of 1 minute, and 2 peers matches the default v1.1 behavior.
  public static long opportunisticGraftTicks = 60;
  public static int opportunisticGraftPeers = 2;
  public static double opportunisticGraftThreshold = 0.25;

  // Experiment support toggles
  public static boolean scoreEnabled = true;
  public static double degradedPeerRatio = 0.0;
  public static double degradedPeerWithholdProbability = 0.0;
  public static double degradedPeerForwardDropProbability = 0.0;
  public static boolean singleTopicEnabled = false;
  public static String singleTopicName = "gossipsub-scoring";

  /** Loads experiment-specific GossipSub options from configuration. */
  public static void loadFromConfig() {
    D = Math.max(0, Configuration.getInt("gossipsub.d", D));
    D_low = Math.max(0, Configuration.getInt("gossipsub.d_low", D_low));
    D_high = Math.max(D, Configuration.getInt("gossipsub.d_high", D_high));
    D_score = Math.max(0, Configuration.getInt("gossipsub.d_score", D_score));
    scoreEnabled = Configuration.getBoolean("gossipsub.score.enabled", scoreEnabled);
    scoreWeightTimeInMesh =
        Configuration.getDouble("gossipsub.score.weight_time_in_mesh", scoreWeightTimeInMesh);
    scoreWeightFirstDeliveries =
        Configuration.getDouble(
            "gossipsub.score.weight_first_deliveries", scoreWeightFirstDeliveries);
    scoreWeightInvalidDeliveries =
        Configuration.getDouble(
            "gossipsub.score.weight_invalid_deliveries", scoreWeightInvalidDeliveries);
    scoreWeightBrokenPromises =
        Configuration.getDouble(
            "gossipsub.score.weight_broken_promises", scoreWeightBrokenPromises);
    scoreDegradedPeerPenalty =
        Math.max(
            0.0,
            Configuration.getDouble(
                "gossipsub.score.degraded_peer_penalty", scoreDegradedPeerPenalty));
    scorePromiseTimeoutHeartbeats =
        Math.max(
            1L,
            Configuration.getLong(
                "gossipsub.score.promise_timeout_heartbeats", scorePromiseTimeoutHeartbeats));
    scoreDecayFirstDeliveries =
        Configuration.getDouble(
            "gossipsub.score.decay_first_deliveries", scoreDecayFirstDeliveries);
    scoreDecayBrokenPromises =
        Configuration.getDouble("gossipsub.score.decay_broken_promises", scoreDecayBrokenPromises);
    scoreMeshHeartbeatCap =
        Configuration.getDouble("gossipsub.score.mesh_heartbeat_cap", scoreMeshHeartbeatCap);
    meshScoreThreshold =
        Configuration.getDouble("gossipsub.score.mesh_threshold", meshScoreThreshold);
    gossipScoreThreshold =
        Configuration.getDouble("gossipsub.score.gossip_threshold", gossipScoreThreshold);
    degradedPeerRatio =
        Math.max(
            0.0,
            Math.min(
                1.0, Configuration.getDouble("gossipsub.degraded.peer.ratio", degradedPeerRatio)));
    degradedPeerWithholdProbability =
        Math.max(
            0.0,
            Math.min(
                1.0,
                Configuration.getDouble(
                    "gossipsub.degraded.peer.withhold.probability",
                    degradedPeerWithholdProbability)));
    degradedPeerForwardDropProbability =
        Math.max(
            0.0,
            Math.min(
                1.0,
                Configuration.getDouble(
                    "gossipsub.degraded.peer.forward_drop.probability",
                    degradedPeerForwardDropProbability)));
    singleTopicEnabled =
        Configuration.getBoolean("gossipsub.single_topic.enabled", singleTopicEnabled);
    singleTopicName = Configuration.getString("gossipsub.single_topic.name", singleTopicName);
    opportunisticGraftTicks =
        Math.max(
            1L,
            Configuration.getLong("gossipsub.opportunistic_graft.ticks", opportunisticGraftTicks));
    opportunisticGraftPeers =
        Math.max(
            0,
            Configuration.getInt("gossipsub.opportunistic_graft.peers", opportunisticGraftPeers));
    opportunisticGraftThreshold =
        Configuration.getDouble(
            "gossipsub.opportunistic_graft.threshold", opportunisticGraftThreshold);
    D_out = Math.max(0, Configuration.getInt("gossipsub.d_out", D_out));
    prunePeers = Math.max(0, Configuration.getInt("gossipsub.prune_peers", prunePeers));
    acceptPXThreshold = Configuration.getDouble("gossipsub.accept_px_threshold", acceptPXThreshold);
    graftRejectionPxEnabled =
        Configuration.getBoolean("gossipsub.graft_rejection_px_enabled", graftRejectionPxEnabled);
    pxRequesterBackoffEnabled =
        Configuration.getBoolean(
            "gossipsub.px_defense.requester_backoff_enabled", pxRequesterBackoffEnabled);
    pxRequesterBackoffHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.px_defense.requester_backoff_heartbeats", pxRequesterBackoffHeartbeats));
    pxGlobalRateLimitEnabled =
        Configuration.getBoolean(
            "gossipsub.px_defense.rate_limit_enabled", pxGlobalRateLimitEnabled);
    pxGlobalRateLimitBudget =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.px_defense.rate_limit_budget", pxGlobalRateLimitBudget));
    pxGlobalRateLimitWindowHeartbeats =
        Math.max(
            1L,
            Configuration.getLong(
                "gossipsub.px_defense.rate_limit_window_heartbeats",
                pxGlobalRateLimitWindowHeartbeats));
    pxFloodDetectionEnabled =
        Configuration.getBoolean(
            "gossipsub.px_defense.flood_detection_enabled", pxFloodDetectionEnabled);
    pxFloodDetectionGraftThreshold =
        Math.max(
            1,
            Configuration.getInt(
                "gossipsub.px_defense.flood_detection_graft_threshold",
                pxFloodDetectionGraftThreshold));
    pxFloodDetectionWindowHeartbeats =
        Math.max(
            1L,
            Configuration.getLong(
                "gossipsub.px_defense.flood_detection_window_heartbeats",
                pxFloodDetectionWindowHeartbeats));
    candidateTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong("gossipsub.candidate_ttl_heartbeats", candidateTtlHeartbeats));
    candidatePxHintTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.candidate_px_hint_ttl_heartbeats", candidatePxHintTtlHeartbeats));
    candidateSelectedTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.candidate_selected_ttl_heartbeats", candidateSelectedTtlHeartbeats));
    candidateCleanupIntervalHeartbeats =
        Math.max(
            1L,
            Configuration.getLong(
                "gossipsub.candidate_cleanup_interval_heartbeats",
                candidateCleanupIntervalHeartbeats));
    candidatePoolMaxPerTopic =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.candidate_pool_max_per_topic", candidatePoolMaxPerTopic));
    bootstrapAssistEnabled =
        Configuration.getBoolean("gossipsub.bootstrap_assist_enabled", bootstrapAssistEnabled);
    bootstrapWindowHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.bootstrap_window_heartbeats", bootstrapWindowHeartbeats));
    bootstrapInitialPeersPerNode =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.bootstrap_initial_peers_per_node", bootstrapInitialPeersPerNode));
    bootstrapCandidatesPerHeartbeat =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.bootstrap_candidates_per_heartbeat", bootstrapCandidatesPerHeartbeat));
    ambientDiscoveryEnabled =
        Configuration.getBoolean("gossipsub.discovery.ambient_enabled", ambientDiscoveryEnabled);
    ambientDiscoveryIntervalHeartbeats =
        Math.max(
            1L,
            Configuration.getLong(
                "gossipsub.discovery.ambient_interval_heartbeats",
                ambientDiscoveryIntervalHeartbeats));
    ambientDiscoveryPeersPerInterval =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.discovery.ambient_peers_per_interval",
                ambientDiscoveryPeersPerInterval));
    inferenceStartHeartbeat =
        Math.max(
            0L,
            Configuration.getLong("gossipsub.inference_start_heartbeat", inferenceStartHeartbeat));
    discoveryMaxDialsPerHeartbeat =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.discovery.max_dials_per_heartbeat", discoveryMaxDialsPerHeartbeat));
    discoveryDialSuccessProbability =
        Math.max(
            0.0,
            Math.min(
                1.0,
                Configuration.getDouble(
                    "gossipsub.discovery.dial_success_probability",
                    discoveryDialSuccessProbability)));
    discoveryLegacyRegistryBootstrapEnabled =
        Configuration.getBoolean(
            "gossipsub.discovery.legacy_registry_bootstrap_enabled",
            discoveryLegacyRegistryBootstrapEnabled);
    discoveryLegacyLocalConnectEnabled =
        Configuration.getBoolean(
            "gossipsub.discovery.legacy_local_connect_enabled", discoveryLegacyLocalConnectEnabled);
    localConnectAttemptsPerTopicPerHeartbeat =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.local_connect_attempts_per_topic_per_heartbeat",
                localConnectAttemptsPerTopicPerHeartbeat));
    localConnectRetryBaseHeartbeats =
        Math.max(
            1L,
            Configuration.getLong(
                "gossipsub.local_connect_retry_base_heartbeats", localConnectRetryBaseHeartbeats));
    localConnectRetryMaxHeartbeats =
        Math.max(
            localConnectRetryBaseHeartbeats,
            Configuration.getLong(
                "gossipsub.local_connect_retry_max_heartbeats", localConnectRetryMaxHeartbeats));
    localConnectedDirectActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connected_direct_activity_ttl_heartbeats",
                localConnectedDirectActivityTtlHeartbeats));
    localConnectedPxActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connected_px_activity_ttl_heartbeats",
                localConnectedPxActivityTtlHeartbeats));
    localConnectedMeshActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connected_mesh_activity_ttl_heartbeats",
                localConnectedMeshActivityTtlHeartbeats));
    localConnectedFanoutActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connected_fanout_activity_ttl_heartbeats",
                localConnectedFanoutActivityTtlHeartbeats));
    localConnectableDirectActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connectable_direct_activity_ttl_heartbeats",
                localConnectableDirectActivityTtlHeartbeats));
    localConnectableMeshActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connectable_mesh_activity_ttl_heartbeats",
                localConnectableMeshActivityTtlHeartbeats));
    localConnectableConnectedActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connectable_connected_activity_ttl_heartbeats",
                localConnectableConnectedActivityTtlHeartbeats));
    localConnectablePxActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connectable_px_activity_ttl_heartbeats",
                localConnectablePxActivityTtlHeartbeats));
    localConnectableRegistryActivityTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connectable_registry_activity_ttl_heartbeats",
                localConnectableRegistryActivityTtlHeartbeats));
    localConnectedReserveTargetPerTopic =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.local_connected_reserve_target_per_topic",
                localConnectedReserveTargetPerTopic));
    localConnectedMinLifetimeHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connected_min_lifetime_heartbeats",
                localConnectedMinLifetimeHeartbeats));
    localConnectedMinHoldHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_connected_min_hold_heartbeats", localConnectedMinHoldHeartbeats));
    localReserveMaturationHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_reserve_maturation_heartbeats", localReserveMaturationHeartbeats));
    localReserveDisconnectProtectionDuringMaturation =
        Configuration.getBoolean(
            "gossipsub.local_reserve_disconnect_protection_during_maturation",
            localReserveDisconnectProtectionDuringMaturation);
    localConnectedIdleConsecutiveThreshold =
        Math.max(
            1,
            Configuration.getInt(
                "gossipsub.local_connected_idle_consecutive_threshold",
                localConnectedIdleConsecutiveThreshold));
    localIdleDisconnectRetryPenaltyHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_idle_disconnect_retry_penalty_heartbeats",
                localIdleDisconnectRetryPenaltyHeartbeats));
    localConnectedIdleDisconnectEnabled =
        Configuration.getBoolean(
            "gossipsub.local_connected_idle_disconnect_enabled",
            localConnectedIdleDisconnectEnabled);
    pxAdvertiseCooldownHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.px_advertise_cooldown_heartbeats", pxAdvertiseCooldownHeartbeats));
    localKnownOnlyTtlHeartbeats =
        Math.max(
            0L,
            Configuration.getLong(
                "gossipsub.local_known_only_ttl_heartbeats", localKnownOnlyTtlHeartbeats));
    localPxAdmissionBudgetPerTopicPerHeartbeat =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.local_px_admission_budget_per_topic_per_heartbeat",
                localPxAdmissionBudgetPerTopicPerHeartbeat));
    localPxKnownOnlyMaxPerTopic =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.local_px_known_only_max_per_topic", localPxKnownOnlyMaxPerTopic));
    repairRelaxReceivedPruneForDOut =
        Configuration.getBoolean(
            "gossipsub.repair_relax_received_prune_for_d_out", repairRelaxReceivedPruneForDOut);
    repairRelaxReceivedPruneBudgetPerHeartbeat =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.repair_relax_received_prune_budget_per_heartbeat",
                repairRelaxReceivedPruneBudgetPerHeartbeat));
    repairRelaxReceivedPruneMinScore =
        Configuration.getDouble(
            "gossipsub.repair_relax_received_prune_min_score", repairRelaxReceivedPruneMinScore);
    repairRelaxReceivedPruneMinAgeFraction =
        Math.max(
            0.0,
            Math.min(
                1.0,
                Configuration.getDouble(
                    "gossipsub.repair_relax_received_prune_min_age_fraction",
                    repairRelaxReceivedPruneMinAgeFraction)));
    receiverRelaxGraftBackoffForDOut =
        Configuration.getBoolean(
            "gossipsub.receiver_relax_graft_backoff_for_d_out", receiverRelaxGraftBackoffForDOut);
    receiverRelaxGraftBackoffBudgetPerHeartbeat =
        Math.max(
            0,
            Configuration.getInt(
                "gossipsub.receiver_relax_graft_backoff_budget_per_heartbeat",
                receiverRelaxGraftBackoffBudgetPerHeartbeat));
    receiverRelaxGraftBackoffMinScore =
        Configuration.getDouble(
            "gossipsub.receiver_relax_graft_backoff_min_score", receiverRelaxGraftBackoffMinScore);
    receiverRelaxGraftBackoffMinAgeFraction =
        Math.max(
            0.0,
            Math.min(
                1.0,
                Configuration.getDouble(
                    "gossipsub.receiver_relax_graft_backoff_min_age_fraction",
                    receiverRelaxGraftBackoffMinAgeFraction)));
    repairRelaxReceivedPruneLocalGraceEnabled =
        Configuration.getBoolean(
            "gossipsub.repair_relax_received_prune_local_grace_enabled",
            repairRelaxReceivedPruneLocalGraceEnabled);
    dOutInplacePromotionEnabled =
        Configuration.getBoolean(
            "gossipsub.d_out_inplace_promotion_enabled", dOutInplacePromotionEnabled);
    dOutForcedReseedEnabled =
        Configuration.getBoolean("gossipsub.d_out_forced_reseed_enabled", dOutForcedReseedEnabled);
  }

  /**
   * Provides short information about current Kademlia configuration
   *
   * @return a string containing the current configuration
   */
  public static String info() {
    return String.format(
        "[D=%d][D_low=%d][D_high=%d][D_score=%d][D_out=%d][BITS=%d]",
        D, D_low, D_high, D_score, D_out, BITS);
  }

  public static boolean isInferenceObservationActive(long heartbeatIndex) {
    return heartbeatIndex >= inferenceStartHeartbeat;
  }

  public static long inferenceWarmupEndHeartbeat() {
    return inferenceStartHeartbeat - 1L;
  }
}
