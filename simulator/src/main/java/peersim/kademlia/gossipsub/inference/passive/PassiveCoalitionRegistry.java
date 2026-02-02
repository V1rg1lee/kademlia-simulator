package peersim.kademlia.gossipsub.inference.passive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.gossipsub.GossipCommonConfig;
import peersim.kademlia.gossipsub.GossipSubProtocol;
import peersim.kademlia.gossipsub.inference.InferenceEventCsvWriter;

/**
 * Registry for the passive coalition prototype.
 *
 * <p>This registry deliberately reuses {@link PassiveEgoMeshAttack} as the only local inference
 * engine. It only resolves multiple attackers, routes locally observable hooks, aggregates the
 * union of locally inferred edges, and exports the coalition view.
 */
public final class PassiveCoalitionRegistry {

  private static final String PREFIX = "gossipsub.inference.passive_coalition";
  private static final String ATTACK_NAME = "passive_coalition";
  private static final String ATTACK_VERSION = "phase1_prototype_v1";
  private static final String OBSERVABILITY_PROFILE = "wire_only";
  private static final String TARGET_LAYER = "mesh";
  private static final String TRUTH_TARGET_POLICY = "final_heartbeat_snapshot";
  private static final String COALITION_POLICY = "union_of_local_passive_observations";
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Comparator<String> NODE_ID_COMPARATOR =
      PassiveCoalitionRegistry::compareNodeIdStrings;

  private static boolean enabled = false;
  private static long currentSeed = -1L;
  private static String scenarioName = "unknown";
  private static Integer gossipProtocolPid = null;
  private static List<Integer> attackerIndices = List.of();
  private static TreeMap<Integer, BigInteger> attackerNodeIdsByIndex = new TreeMap<>();
  private static TreeMap<BigInteger, PassiveEgoMeshAttack> attacksByNodeId = new TreeMap<>();
  private static boolean lateJoinEnabled = false;
  private static long startHeartbeat = -1L;
  private static boolean lateJoinActivated = true;
  private static long activationHeartbeat = -1L;
  private static List<String> joinTopics = List.of();

  private PassiveCoalitionRegistry() {}

  public static void prepareForExperiment(long seed) {
    enabled = Configuration.getBoolean(PREFIX + ".enabled", false);
    currentSeed = seed;
    scenarioName = Configuration.getString(PREFIX + ".scenario_name", "unknown");
    attackerIndices = parseAttackerIndices();
    lateJoinEnabled = Configuration.getBoolean(PREFIX + ".late_join", false);
    startHeartbeat = Configuration.getLong(PREFIX + ".start_heartbeat", -1L);
    joinTopics = parseTopics(Configuration.getString(PREFIX + ".join_topics", ""));
    lateJoinActivated = !lateJoinEnabled;
    activationHeartbeat = lateJoinActivated ? 0L : -1L;
    if (lateJoinEnabled && startHeartbeat < 0L) {
      startHeartbeat = 0L;
    }
    gossipProtocolPid = null;
    attackerNodeIdsByIndex = new TreeMap<>();
    attacksByNodeId = new TreeMap<>();
  }

  public static void reset() {
    enabled = false;
    currentSeed = -1L;
    scenarioName = "unknown";
    attackerIndices = List.of();
    gossipProtocolPid = null;
    attackerNodeIdsByIndex = new TreeMap<>();
    attacksByNodeId = new TreeMap<>();
    lateJoinEnabled = false;
    startHeartbeat = -1L;
    lateJoinActivated = true;
    activationHeartbeat = -1L;
    joinTopics = List.of();
  }

  public static boolean isEnabled() {
    return enabled;
  }

  public static boolean isAttacker(BigInteger nodeId) {
    if (!enabled || nodeId == null) return false;
    ensureAttacksInitialized();
    return attacksByNodeId.containsKey(nodeId);
  }

  public static boolean shouldSuppressNode(BigInteger nodeId) {
    return enabled && lateJoinEnabled && !lateJoinActivated && isAttacker(nodeId);
  }

  public static boolean shouldSuppressDASProtocol(BigInteger nodeId) {
    return shouldSuppressNode(nodeId);
  }

  public static boolean shouldSuppressApplicationCallback(BigInteger nodeId) {
    return shouldSuppressNode(nodeId);
  }

  public static boolean isDiscoverablePeer(BigInteger nodeId) {
    return !shouldSuppressNode(nodeId);
  }

  public static void onGlobalHeartbeatStart(long heartbeatIndex) {
    if (!enabled || !lateJoinEnabled || lateJoinActivated) return;
    if (startHeartbeat >= 0L && heartbeatIndex >= startHeartbeat) {
      activateLateJoinCoalition(heartbeatIndex);
    }
  }

  public static void observeSentGraft(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack attack = ensureAttackForObserver(observerNodeId);
    if (attack != null) attack.observeSentGraft(topic, peerId, heartbeat);
  }

  public static void observeAcceptedIncomingGraft(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack attack = ensureAttackForObserver(observerNodeId);
    if (attack != null) attack.observeAcceptedIncomingGraft(topic, peerId, heartbeat);
  }

  public static void observeSentPrune(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack attack = ensureAttackForObserver(observerNodeId);
    if (attack != null) attack.observeSentPrune(topic, peerId, heartbeat);
  }

  public static void observeReceivedPrune(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack attack = ensureAttackForObserver(observerNodeId);
    if (attack != null) attack.observeReceivedPrune(topic, peerId, heartbeat);
  }

  public static void observeReceivedIHave(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack attack = ensureAttackForObserver(observerNodeId);
    if (attack != null) attack.observeReceivedIHave(topic, peerId, heartbeat);
  }

  public static void observeReceivedIWant(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack attack = ensureAttackForObserver(observerNodeId);
    if (attack != null) attack.observeReceivedIWant(topic, peerId, heartbeat);
  }

  public static void observeReceivedMessage(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack attack = ensureAttackForObserver(observerNodeId);
    if (attack != null) attack.observeReceivedMessage(topic, peerId, heartbeat);
  }

  public static void observeLocalLeave(BigInteger observerNodeId, String topic, long heartbeat) {
    PassiveEgoMeshAttack attack = ensureAttackForObserver(observerNodeId);
    if (attack != null) attack.observeLocalLeave(topic, heartbeat);
  }

  public static void writeOutputs(String logFolder) throws IOException {
    if (!enabled) return;
    ensureAttacksInitialized();
    if (attacksByNodeId.size() != attackerIndices.size()) {
      throw new IOException(
          "passive_coalition enabled but not all attackers could be resolved from attacker_indices="
              + attackerIndices);
    }

    Path outputDir = Path.of(logFolder);
    Files.createDirectories(outputDir);
    writeJson(outputDir.resolve("attack_metadata.json"), buildAttackMetadata());
    writeJson(outputDir.resolve("inferred_topology.json"), buildInferredTopology());
    InferenceEventCsvWriter.write(
        outputDir.resolve("inference_events.csv"),
        attacksByNodeId.values(),
        event -> "COALITION_OBSERVER",
        event ->
            lateJoinEnabled
                ? (lateJoinActivated ? "late_join_active" : "late_join_waiting")
                : "passive");
  }

  private static void writeJson(Path path, Map<String, Object> payload) throws IOException {
    try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      GSON.toJson(payload, writer);
    }
  }

  private static PassiveEgoMeshAttack ensureAttackForObserver(BigInteger observerNodeId) {
    if (!enabled || observerNodeId == null) return null;
    ensureAttacksInitialized();
    if (lateJoinEnabled && !lateJoinActivated) return null;
    return attacksByNodeId.get(observerNodeId);
  }

  private static void ensureAttacksInitialized() {
    if (!enabled || !attacksByNodeId.isEmpty()) return;
    for (Integer attackerIndex : attackerIndices) {
      BigInteger attackerNodeId = resolveAttackerNodeId(attackerIndex.intValue());
      if (attackerNodeId == null) continue;
      attackerNodeIdsByIndex.put(attackerIndex, attackerNodeId);
      attacksByNodeId.put(
          attackerNodeId,
          new PassiveEgoMeshAttack(
              attackerIndex.intValue(), attackerNodeId, scenarioName, currentSeed));
    }
  }

  private static List<Integer> parseAttackerIndices() {
    String raw =
        Configuration.getString(
            PREFIX + ".attacker_indices", Configuration.getString(PREFIX + ".attacker_index", ""));
    if (raw == null || raw.trim().isEmpty()) {
      return List.of();
    }
    LinkedHashSet<Integer> unique = new LinkedHashSet<>();
    for (String token : raw.split(",")) {
      String trimmed = token.trim();
      if (trimmed.isEmpty()) continue;
      unique.add(Integer.valueOf(Integer.parseInt(trimmed)));
    }
    return new ArrayList<>(unique);
  }

  private static BigInteger resolveAttackerNodeId(int attackerIndex) {
    BigInteger cached = attackerNodeIdsByIndex.get(Integer.valueOf(attackerIndex));
    if (cached != null) return cached;
    if (attackerIndex < 0 || attackerIndex >= Network.size()) return null;

    int pid = resolveGossipProtocolPid();
    Node node = Network.get(attackerIndex);
    if (node == null) return null;
    Object protocol = node.getProtocol(pid);
    if (!(protocol instanceof GossipSubProtocol)) return null;
    return ((GossipSubProtocol) protocol).getGossipNode().getId();
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

  private static Map<String, Object> buildAttackMetadata() {
    LinkedHashMap<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("attack_name", ATTACK_NAME);
    metadata.put("attack_version", ATTACK_VERSION);
    metadata.put("observability_profile", OBSERVABILITY_PROFILE);
    metadata.put("target_layer", TARGET_LAYER);
    metadata.put("truth_target_policy", TRUTH_TARGET_POLICY);
    metadata.put("scenario", scenarioName);
    metadata.put("seed", currentSeed);
    metadata.put("coalition_size", attackerIndices.size());
    metadata.put("attacker_indices", new ArrayList<>(attackerIndices));
    metadata.put("attacker_node_ids", buildAttackerNodeIdList());
    metadata.put("active_attack", false);
    metadata.put("coalition_policy", COALITION_POLICY);
    PassiveEgoMeshAttack.appendObservationPolicyMetadata(metadata);
    metadata.put("first_observed_heartbeat", firstObservedHeartbeat());
    metadata.put("inference_semantics", buildInferenceSemantics());
    metadata.put("attackers", buildAttackerMetadata());
    metadata.put("coalition_observation_totals", buildCoalitionObservationTotals());
    metadata.put("late_join", buildLateJoinMetadata());
    return metadata;
  }

  private static Map<String, Object> buildLateJoinMetadata() {
    LinkedHashMap<String, Object> lateJoin = new LinkedHashMap<>();
    lateJoin.put("enabled", lateJoinEnabled);
    lateJoin.put("start_heartbeat", startHeartbeat);
    lateJoin.put("activated", lateJoinActivated);
    lateJoin.put("activation_heartbeat", activationHeartbeat);
    lateJoin.put("join_topics", new ArrayList<>(joinTopics));
    lateJoin.put(
        "mode",
        lateJoinEnabled
            ? "coalition_suppressed_until_start_heartbeat_then_join"
            : "coalition_present_from_bootstrap");
    return lateJoin;
  }

  private static Map<String, Object> buildInferenceSemantics() {
    LinkedHashMap<String, Object> semantics = new LinkedHashMap<>();
    semantics.put("outgoing_present_on", List.of("GRAFT_SENT"));
    semantics.put("outgoing_absent_on", List.of("PRUNE_SENT", "PRUNE_RECEIVED"));
    semantics.put("incoming_present_on", List.of("ACCEPTED_INCOMING_GRAFT"));
    semantics.put("incoming_absent_on", List.of("PRUNE_SENT", "PRUNE_RECEIVED"));
    semantics.put(
        "evidence_only_messages", List.of("IHAVE_RECEIVED", "IWANT_RECEIVED", "MESSAGE_RECEIVED"));
    semantics.put(
        "prototype_conventions",
        List.of(
            "Incoming peer->attacker edges are inferred only after a locally accepted incoming GRAFT.",
            "A sent or received PRUNE conservatively removes both attacker->peer and peer->attacker adjacency for that topic.",
            "Coalition union is only the union of locally observed passive views and never invents edges between non-attackers."));
    return semantics;
  }

  private static List<String> buildAttackerNodeIdList() {
    ArrayList<String> nodeIds = new ArrayList<>();
    for (Integer attackerIndex : attackerIndices) {
      BigInteger nodeId = attackerNodeIdsByIndex.get(attackerIndex);
      if (nodeId != null) nodeIds.add(nodeId.toString());
    }
    return nodeIds;
  }

  private static List<Map<String, Object>> buildAttackerMetadata() {
    ArrayList<Map<String, Object>> attackers = new ArrayList<>();
    for (Integer attackerIndex : attackerIndices) {
      BigInteger nodeId = attackerNodeIdsByIndex.get(attackerIndex);
      PassiveEgoMeshAttack attack = nodeId == null ? null : attacksByNodeId.get(nodeId);
      if (attack == null) continue;

      LinkedHashMap<String, Object> attacker = new LinkedHashMap<>();
      attacker.put("attacker_index", attackerIndex);
      attacker.put("attacker_node_id", nodeId.toString());
      attacker.put("observation_counts", attack.buildObservationCounts());
      attackers.add(attacker);
    }
    return attackers;
  }

  private static Map<String, Object> buildCoalitionObservationTotals() {
    LinkedHashMap<String, Object> totals = new LinkedHashMap<>();
    TreeSet<String> unionTopics = new TreeSet<>();
    long graftSent = 0L;
    long acceptedIncomingGraft = 0L;
    long pruneSent = 0L;
    long pruneReceived = 0L;
    long ihaveReceived = 0L;
    long iwantReceived = 0L;
    long messageReceived = 0L;

    for (PassiveEgoMeshAttack attack : attacksByNodeId.values()) {
      Map<String, Object> counts = attack.buildObservationCounts();
      unionTopics.addAll(attack.buildTopicsJson().keySet());
      graftSent += asLong(counts.get("graft_sent"));
      acceptedIncomingGraft += asLong(counts.get("accepted_incoming_graft"));
      pruneSent += asLong(counts.get("prune_sent"));
      pruneReceived += asLong(counts.get("prune_received"));
      ihaveReceived += asLong(counts.get("ihave_received"));
      iwantReceived += asLong(counts.get("iwant_received"));
      messageReceived += asLong(counts.get("message_received"));
    }

    totals.put("topics_seen_union", unionTopics.size());
    totals.put("graft_sent", graftSent);
    totals.put("accepted_incoming_graft", acceptedIncomingGraft);
    totals.put("prune_sent", pruneSent);
    totals.put("prune_received", pruneReceived);
    totals.put("ihave_received", ihaveReceived);
    totals.put("iwant_received", iwantReceived);
    totals.put("message_received", messageReceived);
    return totals;
  }

  private static long firstObservedHeartbeat() {
    long firstObserved = -1L;
    for (PassiveEgoMeshAttack attack : attacksByNodeId.values()) {
      long attackFirstObserved = attack.getFirstObservedHeartbeat();
      if (attackFirstObserved >= 0L
          && (firstObserved < 0L || attackFirstObserved < firstObserved)) {
        firstObserved = attackFirstObserved;
      }
    }
    return firstObserved;
  }

  private static Map<String, Object> buildInferredTopology() {
    LinkedHashMap<String, Object> root = new LinkedHashMap<>();
    root.put("attack_name", ATTACK_NAME);
    root.put("observability_profile", OBSERVABILITY_PROFILE);
    root.put("target_layer", TARGET_LAYER);
    root.put("coalition_size", attackerIndices.size());
    root.put("attacker_indices", new ArrayList<>(attackerIndices));
    root.put("attacker_node_ids", buildAttackerNodeIdList());
    root.put("per_attacker", buildPerAttackerViews());
    root.put("coalition_union", buildCoalitionUnion());
    return root;
  }

  private static void activateLateJoinCoalition(long heartbeatIndex) {
    ensureAttacksInitialized();
    if (attacksByNodeId.size() != attackerIndices.size()) return;
    lateJoinActivated = true;
    activationHeartbeat = heartbeatIndex;
    for (Integer attackerIndex : attackerIndices) {
      GossipSubProtocol protocol = resolveGossipProtocol(attackerIndex.intValue());
      if (protocol == null) continue;
      for (String topic : effectiveJoinTopics()) {
        protocol.activatePassiveEgoMeshObserver(topic);
      }
    }
  }

  private static GossipSubProtocol resolveGossipProtocol(int attackerIndex) {
    if (attackerIndex < 0 || attackerIndex >= Network.size()) return null;
    Node node = Network.get(attackerIndex);
    if (node == null) return null;
    Object protocol = node.getProtocol(resolveGossipProtocolPid());
    if (!(protocol instanceof GossipSubProtocol)) return null;
    return (GossipSubProtocol) protocol;
  }

  private static List<String> effectiveJoinTopics() {
    if (!joinTopics.isEmpty()) return joinTopics;
    if (GossipCommonConfig.singleTopicEnabled) {
      return List.of(GossipCommonConfig.singleTopicName);
    }
    return List.of();
  }

  private static List<String> parseTopics(String raw) {
    if (raw == null || raw.trim().isEmpty()) return List.of();
    ArrayList<String> topics = new ArrayList<>();
    for (String token : raw.split(",")) {
      String topic = token.trim();
      if (!topic.isEmpty() && !topics.contains(topic)) {
        topics.add(topic);
      }
    }
    return topics;
  }

  private static Map<String, Object> buildPerAttackerViews() {
    LinkedHashMap<String, Object> perAttacker = new LinkedHashMap<>();
    for (Integer attackerIndex : attackerIndices) {
      BigInteger nodeId = attackerNodeIdsByIndex.get(attackerIndex);
      PassiveEgoMeshAttack attack = nodeId == null ? null : attacksByNodeId.get(nodeId);
      if (attack != null) {
        perAttacker.put(String.valueOf(attackerIndex), attack.buildPerAttackerView());
      }
    }
    return perAttacker;
  }

  private static Map<String, Object> buildCoalitionUnion() {
    TreeMap<String, CoalitionTopicAccumulator> topics = new TreeMap<>();

    for (Integer attackerIndex : attackerIndices) {
      BigInteger attackerNodeId = attackerNodeIdsByIndex.get(attackerIndex);
      PassiveEgoMeshAttack attack =
          attackerNodeId == null ? null : attacksByNodeId.get(attackerNodeId);
      if (attack == null) continue;

      String attackerNodeIdString = attackerNodeId.toString();
      Map<String, List<String>> outgoingByTopic = attack.snapshotOutgoingNeighborsByTopic();
      for (Map.Entry<String, List<String>> entry : outgoingByTopic.entrySet()) {
        CoalitionTopicAccumulator accumulator =
            topics.computeIfAbsent(entry.getKey(), ignored -> new CoalitionTopicAccumulator());
        for (String peerId : entry.getValue()) {
          accumulator.addDirectedEdge(
              attackerNodeIdString,
              peerId,
              attackerIndex.intValue(),
              attackerNodeIdString,
              "GRAFT_SENT");
        }
      }

      Map<String, List<String>> incomingByTopic = attack.snapshotIncomingNeighborsByTopic();
      for (Map.Entry<String, List<String>> entry : incomingByTopic.entrySet()) {
        CoalitionTopicAccumulator accumulator =
            topics.computeIfAbsent(entry.getKey(), ignored -> new CoalitionTopicAccumulator());
        for (String peerId : entry.getValue()) {
          accumulator.addDirectedEdge(
              peerId,
              attackerNodeIdString,
              attackerIndex.intValue(),
              attackerNodeIdString,
              "ACCEPTED_INCOMING_GRAFT");
        }
      }
    }

    LinkedHashMap<String, Object> topicsJson = new LinkedHashMap<>();
    for (Map.Entry<String, CoalitionTopicAccumulator> entry : topics.entrySet()) {
      Map<String, Object> topicJson = entry.getValue().toJson();
      if (asLong(topicJson.get("edge_count")) > 0L) {
        topicsJson.put(entry.getKey(), topicJson);
      }
    }

    LinkedHashMap<String, Object> coalitionUnion = new LinkedHashMap<>();
    coalitionUnion.put("topics", topicsJson);
    return coalitionUnion;
  }

  private static long asLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value == null) {
      return 0L;
    }
    return Long.parseLong(String.valueOf(value));
  }

  private static int compareNodeIdStrings(String left, String right) {
    if (left == null && right == null) return 0;
    if (left == null) return -1;
    if (right == null) return 1;
    try {
      int cmp = new BigInteger(left).compareTo(new BigInteger(right));
      if (cmp != 0) return cmp;
    } catch (NumberFormatException ignored) {
      // Fall back to stable lexicographic ordering when ids are not numeric.
    }
    return left.compareTo(right);
  }

  private static final class CoalitionTopicAccumulator {
    private final TreeMap<DirectedEdgeKey, AggregatedDirectedEdge> directedEdges = new TreeMap<>();

    private void addDirectedEdge(
        String src, String dst, int attackerIndex, String attackerNodeId, String evidenceLabel) {
      if (src == null || dst == null) return;
      DirectedEdgeKey key = new DirectedEdgeKey(src, dst);
      AggregatedDirectedEdge edge =
          directedEdges.computeIfAbsent(key, ignored -> new AggregatedDirectedEdge(src, dst));
      edge.observe(attackerIndex, attackerNodeId, evidenceLabel);
    }

    private Map<String, Object> toJson() {
      LinkedHashMap<String, Object> out = new LinkedHashMap<>();
      ArrayList<Map<String, Object>> directedEdgeList = new ArrayList<>();
      TreeSet<UndirectedEdgeKey> undirectedEdges = new TreeSet<>();
      TreeSet<String> frontierNodes = new TreeSet<>(NODE_ID_COMPARATOR);

      for (AggregatedDirectedEdge edge : directedEdges.values()) {
        directedEdgeList.add(edge.toJson());
        undirectedEdges.add(UndirectedEdgeKey.of(edge.src, edge.dst));
        frontierNodes.add(edge.src);
        frontierNodes.add(edge.dst);
      }

      ArrayList<List<String>> undirectedEdgeList = new ArrayList<>();
      for (UndirectedEdgeKey edge : undirectedEdges) {
        undirectedEdgeList.add(List.of(edge.minId, edge.maxId));
      }

      out.put("directed_edges", directedEdgeList);
      out.put("undirected_edges", undirectedEdgeList);
      out.put("frontier_nodes", new ArrayList<>(frontierNodes));
      out.put("edge_count", directedEdgeList.size());
      return out;
    }
  }

  private static final class DirectedEdgeKey implements Comparable<DirectedEdgeKey> {
    private final String src;
    private final String dst;

    private DirectedEdgeKey(String src, String dst) {
      this.src = src;
      this.dst = dst;
    }

    @Override
    public int compareTo(DirectedEdgeKey other) {
      int cmp = NODE_ID_COMPARATOR.compare(this.src, other.src);
      if (cmp != 0) return cmp;
      return NODE_ID_COMPARATOR.compare(this.dst, other.dst);
    }
  }

  private static final class UndirectedEdgeKey implements Comparable<UndirectedEdgeKey> {
    private final String minId;
    private final String maxId;

    private UndirectedEdgeKey(String minId, String maxId) {
      this.minId = minId;
      this.maxId = maxId;
    }

    private static UndirectedEdgeKey of(String left, String right) {
      if (NODE_ID_COMPARATOR.compare(left, right) <= 0) {
        return new UndirectedEdgeKey(left, right);
      }
      return new UndirectedEdgeKey(right, left);
    }

    @Override
    public int compareTo(UndirectedEdgeKey other) {
      int cmp = NODE_ID_COMPARATOR.compare(this.minId, other.minId);
      if (cmp != 0) return cmp;
      return NODE_ID_COMPARATOR.compare(this.maxId, other.maxId);
    }
  }

  private static final class AggregatedDirectedEdge {
    private final String src;
    private final String dst;
    private final TreeSet<Integer> observedByAttackerIndices = new TreeSet<>();
    private final TreeSet<String> observedByAttackerNodeIds = new TreeSet<>(NODE_ID_COMPARATOR);
    private final TreeSet<String> evidence = new TreeSet<>();

    private AggregatedDirectedEdge(String src, String dst) {
      this.src = src;
      this.dst = dst;
    }

    private void observe(int attackerIndex, String attackerNodeId, String evidenceLabel) {
      observedByAttackerIndices.add(Integer.valueOf(attackerIndex));
      if (attackerNodeId != null) observedByAttackerNodeIds.add(attackerNodeId);
      if (evidenceLabel != null && !evidenceLabel.isEmpty()) evidence.add(evidenceLabel);
    }

    private Map<String, Object> toJson() {
      LinkedHashMap<String, Object> out = new LinkedHashMap<>();
      out.put("src", src);
      out.put("dst", dst);
      out.put("observed_by_attacker_indices", new ArrayList<>(observedByAttackerIndices));
      out.put("observed_by_attacker_node_ids", new ArrayList<>(observedByAttackerNodeIds));
      out.put("evidence", new ArrayList<>(evidence));
      return out;
    }
  }
}
