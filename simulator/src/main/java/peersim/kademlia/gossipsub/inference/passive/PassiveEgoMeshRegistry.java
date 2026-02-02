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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.gossipsub.GossipCommonConfig;
import peersim.kademlia.gossipsub.GossipSubProtocol;
import peersim.kademlia.gossipsub.inference.InferenceEventCsvWriter;

/** Registry for the single-attacker passive ego-mesh prototype. */
public final class PassiveEgoMeshRegistry {

  private static final String PREFIX = "gossipsub.inference.passive_ego_mesh";
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private static boolean enabled = false;
  private static int attackerIndex = -1;
  private static long currentSeed = -1L;
  private static String scenarioName = "unknown";
  private static Integer gossipProtocolPid = null;
  private static BigInteger attackerNodeId = null;
  private static PassiveEgoMeshAttack attack = null;
  private static boolean lateJoinEnabled = false;
  private static long startHeartbeat = -1L;
  private static boolean lateJoinActivated = true;
  private static long activationHeartbeat = -1L;
  private static List<String> joinTopics = List.of();

  private PassiveEgoMeshRegistry() {}

  public static void prepareForExperiment(long seed) {
    enabled = Configuration.getBoolean(PREFIX + ".enabled", false);
    attackerIndex = Configuration.getInt(PREFIX + ".attacker_index", -1);
    scenarioName = Configuration.getString(PREFIX + ".scenario_name", "unknown");
    lateJoinEnabled = Configuration.getBoolean(PREFIX + ".late_join", false);
    startHeartbeat = Configuration.getLong(PREFIX + ".start_heartbeat", -1L);
    joinTopics = parseTopics(Configuration.getString(PREFIX + ".join_topics", ""));
    lateJoinActivated = !lateJoinEnabled;
    activationHeartbeat = lateJoinActivated ? 0L : -1L;
    if (lateJoinEnabled && startHeartbeat < 0L) {
      startHeartbeat = 0L;
    }
    currentSeed = seed;
    gossipProtocolPid = null;
    attackerNodeId = null;
    attack = null;
  }

  public static void reset() {
    enabled = false;
    attackerIndex = -1;
    currentSeed = -1L;
    scenarioName = "unknown";
    gossipProtocolPid = null;
    attackerNodeId = null;
    attack = null;
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
    PassiveEgoMeshAttack current = ensureAttackInitialized();
    return current != null && nodeId.equals(attackerNodeId);
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
      activateLateJoinObserver(heartbeatIndex);
    }
  }

  public static void observeSentGraft(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack current = ensureAttackForObserver(observerNodeId);
    if (current != null) current.observeSentGraft(topic, peerId, heartbeat);
  }

  public static void observeAcceptedIncomingGraft(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack current = ensureAttackForObserver(observerNodeId);
    if (current != null) current.observeAcceptedIncomingGraft(topic, peerId, heartbeat);
  }

  public static void observeSentPrune(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack current = ensureAttackForObserver(observerNodeId);
    if (current != null) current.observeSentPrune(topic, peerId, heartbeat);
  }

  public static void observeReceivedPrune(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack current = ensureAttackForObserver(observerNodeId);
    if (current != null) current.observeReceivedPrune(topic, peerId, heartbeat);
  }

  public static void observeReceivedIHave(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack current = ensureAttackForObserver(observerNodeId);
    if (current != null) current.observeReceivedIHave(topic, peerId, heartbeat);
  }

  public static void observeReceivedIWant(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack current = ensureAttackForObserver(observerNodeId);
    if (current != null) current.observeReceivedIWant(topic, peerId, heartbeat);
  }

  public static void observeReceivedMessage(
      BigInteger observerNodeId, String topic, BigInteger peerId, long heartbeat) {
    PassiveEgoMeshAttack current = ensureAttackForObserver(observerNodeId);
    if (current != null) current.observeReceivedMessage(topic, peerId, heartbeat);
  }

  public static void observeLocalLeave(BigInteger observerNodeId, String topic, long heartbeat) {
    PassiveEgoMeshAttack current = ensureAttackForObserver(observerNodeId);
    if (current != null) current.observeLocalLeave(topic, heartbeat);
  }

  public static void writeOutputs(String logFolder) throws IOException {
    if (!enabled) return;
    PassiveEgoMeshAttack current = ensureAttackInitialized();
    if (current == null) {
      throw new IOException(
          "passive_ego_mesh enabled but attacker could not be resolved from attacker_index="
              + attackerIndex);
    }

    Path outputDir = Path.of(logFolder);
    Files.createDirectories(outputDir);
    try (Writer writer =
        Files.newBufferedWriter(
            outputDir.resolve("attack_metadata.json"), StandardCharsets.UTF_8)) {
      GSON.toJson(buildAttackMetadataWithRegistryState(current), writer);
    }
    try (Writer writer =
        Files.newBufferedWriter(
            outputDir.resolve("inferred_topology.json"), StandardCharsets.UTF_8)) {
      GSON.toJson(current.buildInferredTopology(), writer);
    }
    InferenceEventCsvWriter.write(
        outputDir.resolve("inference_events.csv"),
        List.of(current),
        event -> "OBSERVER",
        event ->
            lateJoinEnabled
                ? (lateJoinActivated ? "late_join_active" : "late_join_waiting")
                : "passive");
  }

  private static Map<String, Object> buildAttackMetadataWithRegistryState(
      PassiveEgoMeshAttack current) {
    LinkedHashMap<String, Object> metadata = new LinkedHashMap<>(current.buildAttackMetadata());
    LinkedHashMap<String, Object> lateJoin = new LinkedHashMap<>();
    lateJoin.put("enabled", lateJoinEnabled);
    lateJoin.put("start_heartbeat", startHeartbeat);
    lateJoin.put("activated", lateJoinActivated);
    lateJoin.put("activation_heartbeat", activationHeartbeat);
    lateJoin.put("join_topics", new ArrayList<>(joinTopics));
    lateJoin.put(
        "mode",
        lateJoinEnabled
            ? "observer_suppressed_until_start_heartbeat_then_join"
            : "observer_present_from_bootstrap");
    metadata.put("late_join", lateJoin);
    metadata.put("first_observed_heartbeat", current.getFirstObservedHeartbeat());
    return metadata;
  }

  private static PassiveEgoMeshAttack ensureAttackForObserver(BigInteger observerNodeId) {
    if (!enabled || observerNodeId == null) return null;
    PassiveEgoMeshAttack current = ensureAttackInitialized();
    if (current == null) return null;
    if (!observerNodeId.equals(attackerNodeId)) return null;
    if (lateJoinEnabled && !lateJoinActivated) return null;
    return current;
  }

  private static PassiveEgoMeshAttack ensureAttackInitialized() {
    if (!enabled) return null;
    if (attack != null) return attack;
    attackerNodeId = resolveAttackerNodeId();
    if (attackerNodeId == null) return null;
    attack = new PassiveEgoMeshAttack(attackerIndex, attackerNodeId, scenarioName, currentSeed);
    return attack;
  }

  private static BigInteger resolveAttackerNodeId() {
    if (attackerNodeId != null) return attackerNodeId;
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

  private static void activateLateJoinObserver(long heartbeatIndex) {
    PassiveEgoMeshAttack current = ensureAttackInitialized();
    if (current == null) return;
    lateJoinActivated = true;
    activationHeartbeat = heartbeatIndex;

    GossipSubProtocol protocol = resolveGossipProtocol();
    if (protocol == null) return;
    for (String topic : effectiveJoinTopics()) {
      protocol.activatePassiveEgoMeshObserver(topic);
    }
  }

  private static GossipSubProtocol resolveGossipProtocol() {
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
}
