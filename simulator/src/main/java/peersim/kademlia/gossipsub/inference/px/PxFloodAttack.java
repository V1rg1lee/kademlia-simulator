package peersim.kademlia.gossipsub.inference.px;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Per-attacker local state for the PX flood inference.
 *
 * <p>Each of the 8 coalition attackers maintains one instance. For every PRUNE+PX message received
 * from a flooded target, we record the set of PX peers advertised. These are the target's known
 * mesh neighbors, constituting the inferred neighborhood.
 */
public final class PxFloodAttack {

  private static final String ATTACK_NAME = "px_flood";
  private static final String ATTACK_VERSION = "v1";

  private final int attackerIndex;
  private final BigInteger attackerNodeId;
  private final String scenarioName;
  private final long seed;

  /** topic → target → list of PX events received */
  private final TreeMap<String, TreeMap<String, List<PxPruneEvent>>> pxByTopicTarget;

  private final ArrayList<PxInferenceEvent> inferenceEvents;

  public PxFloodAttack(
      int attackerIndex, BigInteger attackerNodeId, String scenarioName, long seed) {
    this.attackerIndex = attackerIndex;
    this.attackerNodeId = attackerNodeId;
    this.scenarioName = scenarioName == null ? "" : scenarioName;
    this.seed = seed;
    this.pxByTopicTarget = new TreeMap<>();
    this.inferenceEvents = new ArrayList<>();
  }

  public BigInteger getAttackerNodeId() {
    return attackerNodeId;
  }

  public int getAttackerIndex() {
    return attackerIndex;
  }

  /**
   * Records a PRUNE+PX received from {@code senderId} (the flooded target) carrying {@code
   * pxPeers}.
   */
  public void observeReceivedPruneWithPx(
      String topic, BigInteger senderId, List<BigInteger> pxPeers, long heartbeat) {
    if (topic == null || senderId == null || pxPeers == null || pxPeers.isEmpty()) return;

    String targetKey = senderId.toString();
    TreeMap<String, List<PxPruneEvent>> byTarget =
        pxByTopicTarget.computeIfAbsent(topic, ignored -> new TreeMap<>());
    List<PxPruneEvent> events = byTarget.computeIfAbsent(targetKey, ignored -> new ArrayList<>());

    ArrayList<String> pxPeerStrings = new ArrayList<>();
    for (BigInteger px : pxPeers) {
      if (px != null) pxPeerStrings.add(px.toString());
    }
    events.add(new PxPruneEvent(heartbeat, pxPeerStrings));

    for (String pxPeer : pxPeerStrings) {
      inferenceEvents.add(
          new PxInferenceEvent(
              heartbeat,
              attackerIndex,
              attackerNodeId == null ? "" : attackerNodeId.toString(),
              topic,
              targetKey,
              pxPeer,
              "PX_PEER_DISCOVERED",
              "PX_PRUNE_RECEIVED"));
    }
  }

  /** Returns all PX peers received from {@code targetId} on {@code topic}, deduplicated. */
  public TreeSet<String> discoveredPxPeersFromTarget(String topic, BigInteger targetId) {
    if (topic == null || targetId == null) return new TreeSet<>();
    TreeMap<String, List<PxPruneEvent>> byTarget = pxByTopicTarget.get(topic);
    if (byTarget == null) return new TreeSet<>();
    List<PxPruneEvent> events = byTarget.get(targetId.toString());
    if (events == null) return new TreeSet<>();
    TreeSet<String> result = new TreeSet<>();
    for (PxPruneEvent event : events) {
      result.addAll(event.pxPeers);
    }
    return result;
  }

  public List<PxInferenceEvent> snapshotInferenceEvents() {
    return new ArrayList<>(inferenceEvents);
  }

  public Map<String, Object> buildPerAttackerView() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("attacker_index", attackerIndex);
    out.put("attacker_node_id", attackerNodeId == null ? null : attackerNodeId.toString());
    out.put("px_by_topic_target", buildPxByTopicTargetJson());
    return out;
  }

  public Map<String, Object> buildObservationCounts() {
    LinkedHashMap<String, Object> counts = new LinkedHashMap<>();
    long totalPxPrunesReceived = 0L;
    long totalPxPeersDiscovered = 0L;
    long uniqueTargets = 0L;
    for (TreeMap<String, List<PxPruneEvent>> byTarget : pxByTopicTarget.values()) {
      uniqueTargets += byTarget.size();
      for (List<PxPruneEvent> events : byTarget.values()) {
        totalPxPrunesReceived += events.size();
        for (PxPruneEvent event : events) {
          totalPxPeersDiscovered += event.pxPeers.size();
        }
      }
    }
    counts.put("topics_seen", pxByTopicTarget.size());
    counts.put("unique_targets_flooded", uniqueTargets);
    counts.put("px_prunes_received", totalPxPrunesReceived);
    counts.put("px_peers_discovered_total", totalPxPeersDiscovered);
    return counts;
  }

  private Map<String, Object> buildPxByTopicTargetJson() {
    LinkedHashMap<String, Object> topicsJson = new LinkedHashMap<>();
    for (Map.Entry<String, TreeMap<String, List<PxPruneEvent>>> topicEntry :
        pxByTopicTarget.entrySet()) {
      LinkedHashMap<String, Object> targetsJson = new LinkedHashMap<>();
      for (Map.Entry<String, List<PxPruneEvent>> targetEntry : topicEntry.getValue().entrySet()) {
        ArrayList<Map<String, Object>> eventsJson = new ArrayList<>();
        TreeSet<String> allPxPeers = new TreeSet<>();
        for (PxPruneEvent event : targetEntry.getValue()) {
          LinkedHashMap<String, Object> eventJson = new LinkedHashMap<>();
          eventJson.put("heartbeat", event.heartbeat);
          eventJson.put("px_peers", new ArrayList<>(event.pxPeers));
          eventsJson.add(eventJson);
          allPxPeers.addAll(event.pxPeers);
        }
        LinkedHashMap<String, Object> targetJson = new LinkedHashMap<>();
        targetJson.put("events", eventsJson);
        targetJson.put("all_px_peers_union", new ArrayList<>(allPxPeers));
        targetsJson.put(targetEntry.getKey(), targetJson);
      }
      topicsJson.put(topicEntry.getKey(), targetsJson);
    }
    return topicsJson;
  }

  public static final class PxPruneEvent {
    public final long heartbeat;
    public final List<String> pxPeers;

    private PxPruneEvent(long heartbeat, List<String> pxPeers) {
      this.heartbeat = heartbeat;
      this.pxPeers = pxPeers;
    }
  }

  public static final class PxInferenceEvent {
    public final long heartbeatIndex;
    public final int observerIndex;
    public final String observerId;
    public final String topic;
    public final String targetId;
    public final String discoveredPeerId;
    public final String action;
    public final String cause;

    private PxInferenceEvent(
        long heartbeatIndex,
        int observerIndex,
        String observerId,
        String topic,
        String targetId,
        String discoveredPeerId,
        String action,
        String cause) {
      this.heartbeatIndex = heartbeatIndex;
      this.observerIndex = observerIndex;
      this.observerId = observerId;
      this.topic = topic;
      this.targetId = targetId;
      this.discoveredPeerId = discoveredPeerId;
      this.action = action;
      this.cause = cause;
    }
  }
}
