package peersim.kademlia.gossipsub.inference.px;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.gossipsub.GossipCommonConfig;
import peersim.kademlia.gossipsub.GossipSubProtocol;

/**
 * Registry for the PX flood inference attack.
 *
 * <p>A coalition of 8 attacker nodes simultaneously GRAFTs a target on a given topic. The target's
 * mesh saturates and it replies with PRUNE+PX (when {@code gossipsub.graft_rejection_px_enabled} is
 * true). Each PX list reveals the target's mesh neighborhood. The registry then queues those
 * discovered peers as the next wave of flood targets, recursively mapping the mesh topology.
 *
 * <p>Wave timing: each wave occupies exactly one heartbeat. The registry activates attackers at the
 * start of heartbeat H; messages are exchanged within that heartbeat step; PX results are collected
 * at the start of heartbeat H+wave_timeout (default H+2 for safety margin).
 */
public final class PxFloodRegistry {

  private static final String PREFIX = "gossipsub.inference.px_flood";
  private static final String ATTACK_NAME = "px_flood";
  private static final String ATTACK_VERSION = "v1";
  private static final String OBSERVABILITY_PROFILE = "active_px_flood";
  private static final String TARGET_LAYER = "mesh";
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Comparator<String> NODE_ID_COMPARATOR =
      PxFloodRegistry::compareNodeIdStrings;

  private static boolean enabled = false;
  private static long currentSeed = -1L;
  private static String scenarioName = "unknown";
  private static Integer gossipProtocolPid = null;
  private static List<Integer> attackerIndices = List.of();
  private static int initialTargetIndex = -1;
  private static int waveDepth = 1;
  private static int waveTimeoutHeartbeats = 2;
  private static String topic = "";

  /** Resolved node IDs */
  private static TreeMap<Integer, BigInteger> attackerNodeIdsByIndex = new TreeMap<>();

  private static TreeMap<BigInteger, PxFloodAttack> attacksByNodeId = new TreeMap<>();

  /** Wave orchestration state */
  private static boolean waveActive = false;

  private static long waveStartHeartbeat = -1L;
  private static BigInteger currentWaveTarget = null;
  private static int currentWaveDepth = 0;
  private static boolean isDone = false;

  /** Pending targets per depth level: depth → queue of (target nodeId, depth) */
  private static final Queue<WaveTarget> pendingTargets = new ArrayDeque<>();

  private static final TreeSet<String> floodedTargets = new TreeSet<>(NODE_ID_COMPARATOR);

  /** Accumulated wave results for output */
  private static final ArrayList<WaveResult> waveResults = new ArrayList<>();

  /** Union of all PX-discovered edges: target → set of discovered PX peers */
  private static final TreeMap<String, TreeSet<String>> discoveredEdges =
      new TreeMap<>(NODE_ID_COMPARATOR);

  private PxFloodRegistry() {}

  public static void prepareForExperiment(long seed) {
    enabled = Configuration.getBoolean(PREFIX + ".enabled", false);
    currentSeed = seed;
    scenarioName = Configuration.getString(PREFIX + ".scenario_name", "unknown");
    attackerIndices = parseAttackerIndices();
    initialTargetIndex = Configuration.getInt(PREFIX + ".initial_target_index", -1);
    waveDepth = Math.max(1, Configuration.getInt(PREFIX + ".wave_depth", 1));
    waveTimeoutHeartbeats =
        Math.max(1, Configuration.getInt(PREFIX + ".wave_timeout_heartbeats", 2));
    topic = Configuration.getString(PREFIX + ".topic", "");
    gossipProtocolPid = null;
    attackerNodeIdsByIndex = new TreeMap<>();
    attacksByNodeId = new TreeMap<>();
    waveActive = false;
    waveStartHeartbeat = -1L;
    currentWaveTarget = null;
    currentWaveDepth = 0;
    isDone = false;
    pendingTargets.clear();
    floodedTargets.clear();
    waveResults.clear();
    discoveredEdges.clear();
  }

  public static void reset() {
    enabled = false;
    currentSeed = -1L;
    scenarioName = "unknown";
    gossipProtocolPid = null;
    attackerIndices = List.of();
    initialTargetIndex = -1;
    waveDepth = 1;
    waveTimeoutHeartbeats = 2;
    topic = "";
    attackerNodeIdsByIndex = new TreeMap<>();
    attacksByNodeId = new TreeMap<>();
    waveActive = false;
    waveStartHeartbeat = -1L;
    currentWaveTarget = null;
    currentWaveDepth = 0;
    isDone = false;
    pendingTargets.clear();
    floodedTargets.clear();
    waveResults.clear();
    discoveredEdges.clear();
  }

  public static boolean isEnabled() {
    return enabled;
  }

  public static boolean isComplete() {
    return isDone;
  }

  public static boolean isAttacker(BigInteger nodeId) {
    if (!enabled || nodeId == null) return false;
    ensureAttacksInitialized();
    return attacksByNodeId.containsKey(nodeId);
  }

  /** Attacker nodes are always visible — they actively join and GRAFT. */
  public static boolean shouldSuppressNode(BigInteger nodeId) {
    return false;
  }

  public static void onGlobalHeartbeatStart(long heartbeatIndex) {
    if (!enabled) return;
    ensureAttacksInitialized();
    if (isDone) return;
    // Do not start waves before the inference warm-up is complete.
    if (heartbeatIndex < GossipCommonConfig.inferenceStartHeartbeat) return;

    if (waveActive) {
      if (heartbeatIndex >= waveStartHeartbeat + waveTimeoutHeartbeats) {
        collectWaveResults(heartbeatIndex);
        waveActive = false;
      } else {
        return;
      }
    }

    if (isDone) return;

    if (!waveActive) {
      if (pendingTargets.isEmpty()) {
        isDone = true;
        return;
      }
      WaveTarget next = pendingTargets.poll();
      startWave(next.targetNodeId, next.depth, heartbeatIndex);
    }
  }

  public static void observeReceivedPruneWithPx(
      BigInteger observerNodeId,
      String topic,
      BigInteger senderId,
      List<BigInteger> pxPeers,
      long heartbeat) {
    if (!enabled || observerNodeId == null || pxPeers == null || pxPeers.isEmpty()) return;
    ensureAttacksInitialized();
    PxFloodAttack attack = attacksByNodeId.get(observerNodeId);
    if (attack != null) {
      attack.observeReceivedPruneWithPx(topic, senderId, pxPeers, heartbeat);
    }
  }

  public static void writeOutputs(String logFolder) throws IOException {
    if (!enabled) return;
    ensureAttacksInitialized();

    Path outputDir = Path.of(logFolder);
    Files.createDirectories(outputDir);
    writeJson(outputDir.resolve("attack_metadata.json"), buildAttackMetadata());
    writeJson(outputDir.resolve("inferred_topology.json"), buildInferredTopology());
    writeJson(outputDir.resolve("wave_report.json"), buildWaveReport());
    writeInferenceEventsCsv(outputDir.resolve("inference_events.csv"));
  }

  private static void writeJson(Path path, Map<String, Object> payload) throws IOException {
    try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      GSON.toJson(payload, writer);
    }
  }

  private static void writeInferenceEventsCsv(Path path) throws IOException {
    try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      writer.write(
          "heartbeat_index,observer_index,observer_id,topic,target_id,discovered_peer_id,action,cause\n");
      for (PxFloodAttack attack : attacksByNodeId.values()) {
        for (PxFloodAttack.PxInferenceEvent event : attack.snapshotInferenceEvents()) {
          writer.write(
              event.heartbeatIndex
                  + ","
                  + event.observerIndex
                  + ","
                  + event.observerId
                  + ","
                  + event.topic
                  + ","
                  + event.targetId
                  + ","
                  + event.discoveredPeerId
                  + ","
                  + event.action
                  + ","
                  + event.cause
                  + "\n");
        }
      }
    }
  }

  private static void startWave(BigInteger targetNodeId, int depth, long heartbeatIndex) {
    if (targetNodeId == null) {
      isDone = true;
      return;
    }
    String targetKey = targetNodeId.toString();
    if (floodedTargets.contains(targetKey)) {
      isDone = pendingTargets.isEmpty();
      return;
    }
    floodedTargets.add(targetKey);
    currentWaveTarget = targetNodeId;
    currentWaveDepth = depth;
    waveStartHeartbeat = heartbeatIndex;
    waveActive = true;

    String effectiveTopic = effectiveTopic();
    for (Integer attackerIndex : attackerIndices) {
      GossipSubProtocol protocol = resolveGossipProtocol(attackerIndex.intValue());
      if (protocol != null) {
        protocol.activatePxFloodWave(effectiveTopic, targetNodeId);
      }
    }
  }

  private static void collectWaveResults(long heartbeatIndex) {
    if (currentWaveTarget == null) return;
    String targetKey = currentWaveTarget.toString();
    String effectiveTopic = effectiveTopic();

    TreeSet<String> unionPxPeers = new TreeSet<>(NODE_ID_COMPARATOR);
    TreeMap<String, List<String>> pxPerAttacker = new TreeMap<>();

    for (PxFloodAttack attack : attacksByNodeId.values()) {
      TreeSet<String> attackerPx =
          attack.discoveredPxPeersFromTarget(effectiveTopic, currentWaveTarget);
      pxPerAttacker.put(
          attack.getAttackerNodeId() == null ? "" : attack.getAttackerNodeId().toString(),
          new ArrayList<>(attackerPx));
      unionPxPeers.addAll(attackerPx);
    }

    TreeSet<String> attackerIdSet = new TreeSet<>(NODE_ID_COMPARATOR);
    for (BigInteger id : attacksByNodeId.keySet()) {
      attackerIdSet.add(id.toString());
    }
    unionPxPeers.removeAll(attackerIdSet);
    unionPxPeers.remove(targetKey);

    waveResults.add(
        new WaveResult(
            currentWaveDepth,
            waveStartHeartbeat,
            heartbeatIndex,
            targetKey,
            effectiveTopic,
            new ArrayList<>(unionPxPeers),
            pxPerAttacker));

    if (!unionPxPeers.isEmpty()) {
      discoveredEdges
          .computeIfAbsent(targetKey, ignored -> new TreeSet<>(NODE_ID_COMPARATOR))
          .addAll(unionPxPeers);
    }

    if (currentWaveDepth < waveDepth) {
      for (String pxPeer : unionPxPeers) {
        if (!floodedTargets.contains(pxPeer)) {
          try {
            BigInteger pxNodeId = new BigInteger(pxPeer);
            pendingTargets.add(new WaveTarget(pxNodeId, currentWaveDepth + 1));
          } catch (NumberFormatException ignored) {
            // skip invalid IDs
          }
        }
      }
    }

    if (pendingTargets.isEmpty()) {
      isDone = true;
    }
  }

  private static void ensureAttacksInitialized() {
    if (!enabled || !attacksByNodeId.isEmpty()) return;
    for (Integer attackerIndex : attackerIndices) {
      BigInteger nodeId = resolveNodeId(attackerIndex.intValue());
      if (nodeId == null) continue;
      attackerNodeIdsByIndex.put(attackerIndex, nodeId);
      attacksByNodeId.put(
          nodeId, new PxFloodAttack(attackerIndex.intValue(), nodeId, scenarioName, currentSeed));
    }
    if (!attacksByNodeId.isEmpty() && !pendingTargets.isEmpty()) return;
    BigInteger initialTarget = resolveNodeId(initialTargetIndex);
    if (initialTarget != null) {
      pendingTargets.add(new WaveTarget(initialTarget, 0));
    }
  }

  private static BigInteger resolveNodeId(int nodeIndex) {
    if (nodeIndex < 0 || nodeIndex >= Network.size()) return null;
    int pid = resolveGossipProtocolPid();
    Node node = Network.get(nodeIndex);
    if (node == null) return null;
    Object protocol = node.getProtocol(pid);
    if (!(protocol instanceof GossipSubProtocol)) return null;
    return ((GossipSubProtocol) protocol).getGossipNode().getId();
  }

  private static GossipSubProtocol resolveGossipProtocol(int nodeIndex) {
    if (nodeIndex < 0 || nodeIndex >= Network.size()) return null;
    Node node = Network.get(nodeIndex);
    if (node == null) return null;
    Object protocol = node.getProtocol(resolveGossipProtocolPid());
    if (!(protocol instanceof GossipSubProtocol)) return null;
    return (GossipSubProtocol) protocol;
  }

  private static int resolveGossipProtocolPid() {
    if (gossipProtocolPid != null) return gossipProtocolPid.intValue();
    String[] keys = {"control.4.protocol", "control.3.protocol", "control.1protocol.protocol"};
    for (String key : keys) {
      if (Configuration.contains(key)) {
        gossipProtocolPid = Integer.valueOf(Configuration.getPid(key));
        return gossipProtocolPid.intValue();
      }
    }
    gossipProtocolPid = Integer.valueOf(3);
    return gossipProtocolPid.intValue();
  }

  private static String effectiveTopic() {
    if (topic != null && !topic.isBlank()) return topic;
    if (GossipCommonConfig.singleTopicEnabled) return GossipCommonConfig.singleTopicName;
    return "";
  }

  private static List<Integer> parseAttackerIndices() {
    String raw = Configuration.getString(PREFIX + ".attacker_indices", "");
    if (raw == null || raw.trim().isEmpty()) return List.of();
    LinkedHashSet<Integer> unique = new LinkedHashSet<>();
    for (String token : raw.split(",")) {
      String trimmed = token.trim();
      if (trimmed.isEmpty()) continue;
      unique.add(Integer.valueOf(Integer.parseInt(trimmed)));
    }
    return new ArrayList<>(unique);
  }

  private static Map<String, Object> buildAttackMetadata() {
    LinkedHashMap<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("attack_name", ATTACK_NAME);
    metadata.put("attack_version", ATTACK_VERSION);
    metadata.put("observability_profile", OBSERVABILITY_PROFILE);
    metadata.put("target_layer", TARGET_LAYER);
    metadata.put("scenario", scenarioName);
    metadata.put("seed", currentSeed);
    metadata.put("coalition_size", attackerIndices.size());
    metadata.put("attacker_indices", new ArrayList<>(attackerIndices));
    metadata.put("attacker_node_ids", buildAttackerNodeIdList());
    metadata.put("initial_target_index", initialTargetIndex);
    metadata.put("initial_target_node_id", buildInitialTargetNodeId());
    metadata.put("wave_depth", waveDepth);
    metadata.put("wave_timeout_heartbeats", waveTimeoutHeartbeats);
    metadata.put("topic", effectiveTopic());
    metadata.put("inference_complete", isDone);
    metadata.put("waves_completed", waveResults.size());
    metadata.put("total_targets_flooded", floodedTargets.size());
    metadata.put("total_px_peers_discovered", totalDiscoveredPeers());
    metadata.put("graft_rejection_px_enabled", GossipCommonConfig.graftRejectionPxEnabled);
    metadata.put("inference_start_heartbeat", GossipCommonConfig.inferenceStartHeartbeat);
    metadata.put("per_attacker_observation_counts", buildPerAttackerObservationCounts());
    return metadata;
  }

  private static Map<String, Object> buildInferredTopology() {
    LinkedHashMap<String, Object> root = new LinkedHashMap<>();
    root.put("attack_name", ATTACK_NAME);
    root.put("observability_profile", OBSERVABILITY_PROFILE);
    root.put("target_layer", TARGET_LAYER);
    root.put("topic", effectiveTopic());
    root.put("inference_complete", isDone);

    LinkedHashMap<String, Object> topology = new LinkedHashMap<>();
    for (Map.Entry<String, TreeSet<String>> entry : discoveredEdges.entrySet()) {
      LinkedHashMap<String, Object> targetView = new LinkedHashMap<>();
      targetView.put("inferred_mesh_neighbors", new ArrayList<>(entry.getValue()));
      targetView.put("neighbor_count", entry.getValue().size());
      topology.put(entry.getKey(), targetView);
    }
    root.put("per_target_inferred_neighbors", topology);

    TreeSet<String> allDiscoveredNodes = new TreeSet<>(NODE_ID_COMPARATOR);
    for (TreeSet<String> peers : discoveredEdges.values()) {
      allDiscoveredNodes.addAll(peers);
    }
    root.put("all_discovered_nodes", new ArrayList<>(allDiscoveredNodes));
    root.put("total_discovered_nodes", allDiscoveredNodes.size());

    ArrayList<List<String>> directedEdges = new ArrayList<>();
    for (Map.Entry<String, TreeSet<String>> entry : discoveredEdges.entrySet()) {
      for (String peer : entry.getValue()) {
        directedEdges.add(List.of(entry.getKey(), peer));
      }
    }
    root.put("inferred_directed_edges", directedEdges);
    root.put("per_attacker", buildPerAttackerViews());
    return root;
  }

  private static Map<String, Object> buildWaveReport() {
    LinkedHashMap<String, Object> report = new LinkedHashMap<>();
    report.put("total_waves", waveResults.size());
    report.put("inference_complete", isDone);
    ArrayList<Map<String, Object>> waves = new ArrayList<>();
    for (WaveResult result : waveResults) {
      LinkedHashMap<String, Object> waveJson = new LinkedHashMap<>();
      waveJson.put("depth", result.depth);
      waveJson.put("wave_start_heartbeat", result.startHeartbeat);
      waveJson.put("collect_heartbeat", result.collectHeartbeat);
      waveJson.put("target_node_id", result.targetNodeId);
      waveJson.put("topic", result.topic);
      waveJson.put("px_peers_received_union", result.pxPeersUnion);
      waveJson.put("px_peers_count", result.pxPeersUnion.size());
      waveJson.put("px_per_attacker", result.pxPerAttacker);
      waves.add(waveJson);
    }
    report.put("waves", waves);
    return report;
  }

  private static Map<String, Object> buildPerAttackerViews() {
    LinkedHashMap<String, Object> perAttacker = new LinkedHashMap<>();
    for (Map.Entry<Integer, BigInteger> entry : attackerNodeIdsByIndex.entrySet()) {
      PxFloodAttack attack = attacksByNodeId.get(entry.getValue());
      if (attack != null) {
        perAttacker.put(String.valueOf(entry.getKey()), attack.buildPerAttackerView());
      }
    }
    return perAttacker;
  }

  private static List<String> buildAttackerNodeIdList() {
    ArrayList<String> nodeIds = new ArrayList<>();
    for (Integer idx : attackerIndices) {
      BigInteger nodeId = attackerNodeIdsByIndex.get(idx);
      if (nodeId != null) nodeIds.add(nodeId.toString());
    }
    return nodeIds;
  }

  private static String buildInitialTargetNodeId() {
    BigInteger id = resolveNodeId(initialTargetIndex);
    return id == null ? null : id.toString();
  }

  private static Map<String, Object> buildPerAttackerObservationCounts() {
    LinkedHashMap<String, Object> counts = new LinkedHashMap<>();
    for (Map.Entry<Integer, BigInteger> entry : attackerNodeIdsByIndex.entrySet()) {
      PxFloodAttack attack = attacksByNodeId.get(entry.getValue());
      if (attack != null) {
        counts.put(String.valueOf(entry.getKey()), attack.buildObservationCounts());
      }
    }
    return counts;
  }

  private static long totalDiscoveredPeers() {
    TreeSet<String> all = new TreeSet<>(NODE_ID_COMPARATOR);
    for (TreeSet<String> peers : discoveredEdges.values()) {
      all.addAll(peers);
    }
    return all.size();
  }

  private static int compareNodeIdStrings(String left, String right) {
    if (left == null && right == null) return 0;
    if (left == null) return -1;
    if (right == null) return 1;
    try {
      int cmp = new BigInteger(left).compareTo(new BigInteger(right));
      if (cmp != 0) return cmp;
    } catch (NumberFormatException ignored) {
      // fall through
    }
    return left.compareTo(right);
  }

  private static final class WaveTarget {
    final BigInteger targetNodeId;
    final int depth;

    WaveTarget(BigInteger targetNodeId, int depth) {
      this.targetNodeId = targetNodeId;
      this.depth = depth;
    }
  }

  private static final class WaveResult {
    final int depth;
    final long startHeartbeat;
    final long collectHeartbeat;
    final String targetNodeId;
    final String topic;
    final List<String> pxPeersUnion;
    final Map<String, List<String>> pxPerAttacker;

    WaveResult(
        int depth,
        long startHeartbeat,
        long collectHeartbeat,
        String targetNodeId,
        String topic,
        List<String> pxPeersUnion,
        Map<String, List<String>> pxPerAttacker) {
      this.depth = depth;
      this.startHeartbeat = startHeartbeat;
      this.collectHeartbeat = collectHeartbeat;
      this.targetNodeId = targetNodeId;
      this.topic = topic;
      this.pxPeersUnion = pxPeersUnion;
      this.pxPerAttacker = pxPerAttacker;
    }
  }
}
