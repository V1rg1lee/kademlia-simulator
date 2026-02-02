package peersim.kademlia.gossipsub.inference.passive;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import peersim.kademlia.gossipsub.GossipCommonConfig;

/**
 * Minimal passive ego-mesh inference attack for phase 1 experiments.
 *
 * <p>The attack is intentionally conservative:
 *
 * <ul>
 *   <li>Only GRAFT/PRUNE modify the inferred topology.
 *   <li>IHAVE/IWANT/MESSAGE are tracked as evidence only.
 *   <li>An incoming edge {@code peer -> attacker} is inferred only when the incoming GRAFT is
 *       accepted locally, not when it is merely received.
 *   <li>A sent or received PRUNE removes both {@code attacker -> peer} and {@code peer -> attacker}
 *       as a conservative prototype convention because it clearly ends local mesh adjacency from
 *       the attacker's non-directed viewpoint.
 * </ul>
 */
public final class PassiveEgoMeshAttack {

  private static final String ATTACK_NAME = "passive_ego_mesh";
  private static final String ATTACK_VERSION = "phase1_prototype_v1";
  private static final String OBSERVABILITY_PROFILE = "wire_only";
  private static final String TARGET_LAYER = "mesh";
  private static final String TRUTH_TARGET_POLICY = "final_heartbeat_snapshot";

  private final int attackerIndex;
  private final BigInteger attackerNodeId;
  private final String scenarioName;
  private final long seed;
  private final TreeMap<String, TopicState> topics;
  private final ArrayList<InferenceEvent> inferenceEvents;

  public PassiveEgoMeshAttack(
      int attackerIndex, BigInteger attackerNodeId, String scenarioName, long seed) {
    this.attackerIndex = attackerIndex;
    this.attackerNodeId = attackerNodeId;
    this.scenarioName = scenarioName == null ? "" : scenarioName;
    this.seed = seed;
    this.topics = new TreeMap<>();
    this.inferenceEvents = new ArrayList<>();
  }

  public BigInteger getAttackerNodeId() {
    return attackerNodeId;
  }

  public int getAttackerIndex() {
    return attackerIndex;
  }

  public void observeSentGraft(String topic, BigInteger peerId, long heartbeat) {
    if (topic == null || peerId == null || !isInferenceObservationActive(heartbeat)) return;
    boolean present =
        getOrCreateTopicState(topic).setOutgoingPresent(peerId, heartbeat, "GRAFT_SENT");
    recordEvent(heartbeat, topic, peerId, "ADD_OUTGOING_EVIDENCE", "GRAFT_SENT", present);
  }

  public void observeAcceptedIncomingGraft(String topic, BigInteger peerId, long heartbeat) {
    if (topic == null || peerId == null || !isInferenceObservationActive(heartbeat)) return;
    boolean present =
        getOrCreateTopicState(topic)
            .setIncomingPresent(peerId, heartbeat, "ACCEPTED_INCOMING_GRAFT");
    recordEvent(
        heartbeat, topic, peerId, "ADD_INCOMING_EVIDENCE", "ACCEPTED_INCOMING_GRAFT", present);
  }

  public void observeSentPrune(String topic, BigInteger peerId, long heartbeat) {
    if (topic == null || peerId == null || !isInferenceObservationActive(heartbeat)) return;
    boolean present = getOrCreateTopicState(topic).applySentPrune(peerId, heartbeat);
    recordEvent(heartbeat, topic, peerId, "REMOVE_ALL_EVIDENCE", "PRUNE_SENT", present);
  }

  /**
   * Conservative prototype convention: a PRUNE removes both local outgoing and incoming adjacency
   * for the peer on that topic.
   */
  public void observeReceivedPrune(String topic, BigInteger peerId, long heartbeat) {
    if (topic == null || peerId == null || !isInferenceObservationActive(heartbeat)) return;
    TopicState topicState = getOrCreateTopicState(topic);
    boolean present = topicState.applyReceivedPrune(peerId, heartbeat);
    recordEvent(heartbeat, topic, peerId, "REMOVE_ALL_EVIDENCE", "PRUNE_RECEIVED", present);
  }

  public void observeReceivedIHave(String topic, BigInteger peerId, long heartbeat) {
    if (topic == null || peerId == null || !isInferenceObservationActive(heartbeat)) return;
    boolean present =
        getOrCreateTopicState(topic).noteEvidence(peerId, heartbeat, EvidenceType.IHAVE_RECEIVED);
    recordEvent(heartbeat, topic, peerId, "EVIDENCE_ONLY", "IHAVE_RECEIVED", present);
  }

  public void observeReceivedIWant(String topic, BigInteger peerId, long heartbeat) {
    if (topic == null || peerId == null || !isInferenceObservationActive(heartbeat)) return;
    boolean present =
        getOrCreateTopicState(topic).noteEvidence(peerId, heartbeat, EvidenceType.IWANT_RECEIVED);
    recordEvent(heartbeat, topic, peerId, "EVIDENCE_ONLY", "IWANT_RECEIVED", present);
  }

  public void observeReceivedMessage(String topic, BigInteger peerId, long heartbeat) {
    if (topic == null || peerId == null || !isInferenceObservationActive(heartbeat)) return;
    boolean present =
        getOrCreateTopicState(topic).noteEvidence(peerId, heartbeat, EvidenceType.MESSAGE_RECEIVED);
    recordEvent(heartbeat, topic, peerId, "EVIDENCE_ONLY", "MESSAGE_RECEIVED", present);
  }

  /** Clears the current inferred local topology for a topic when the attacker leaves locally. */
  public void observeLocalLeave(String topic, long heartbeat) {
    if (topic == null || !isInferenceObservationActive(heartbeat)) return;
    List<String> affectedPeers = getOrCreateTopicState(topic).clearInferredTopology(heartbeat);
    for (String peerId : affectedPeers) {
      recordEvent(heartbeat, topic, peerId, "REMOVE_ALL_EVIDENCE", "LOCAL_LEAVE", false);
    }
  }

  public Map<String, Object> buildAttackMetadata() {
    LinkedHashMap<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("attack_name", ATTACK_NAME);
    metadata.put("attack_version", ATTACK_VERSION);
    metadata.put("observability_profile", OBSERVABILITY_PROFILE);
    metadata.put("target_layer", TARGET_LAYER);
    metadata.put("truth_target_policy", TRUTH_TARGET_POLICY);
    metadata.put("scenario", scenarioName);
    metadata.put("seed", seed);
    metadata.put("attacker_index", attackerIndex);
    metadata.put("attacker_node_id", attackerNodeId == null ? null : attackerNodeId.toString());
    metadata.put("active_attack", false);
    appendObservationPolicyMetadata(metadata);

    LinkedHashMap<String, Object> semantics = new LinkedHashMap<>();
    semantics.put("primary_inference_target", "non_directed_local_mesh_neighbors");
    semantics.put("outgoing_evidence_on", List.of("GRAFT_SENT"));
    semantics.put("outgoing_evidence_absent_on", List.of("PRUNE_SENT", "PRUNE_RECEIVED"));
    semantics.put("incoming_evidence_on", List.of("ACCEPTED_INCOMING_GRAFT"));
    semantics.put("incoming_evidence_absent_on", List.of("PRUNE_SENT", "PRUNE_RECEIVED"));
    semantics.put(
        "evidence_only_messages", List.of("IHAVE_RECEIVED", "IWANT_RECEIVED", "MESSAGE_RECEIVED"));
    semantics.put(
        "prototype_conventions",
        List.of(
            "Incoming peer->attacker evidence is recorded only after a locally accepted incoming GRAFT.",
            "The scientific evaluation collapses outgoing and incoming evidence into one non-directed ego-mesh neighbor relation.",
            "A sent or received PRUNE conservatively removes both attacker->peer and peer->attacker adjacency evidence for that topic.",
            "IHAVE, IWANT, and MESSAGE are evidence only and never create mesh neighbor relations."));
    metadata.put("inference_semantics", semantics);

    metadata.put("observation_counts", buildObservationCounts());

    return metadata;
  }

  public Map<String, Object> buildInferredTopology() {
    LinkedHashMap<String, Object> root = new LinkedHashMap<>();
    root.put("attack_name", ATTACK_NAME);
    root.put("observability_profile", OBSERVABILITY_PROFILE);
    root.put("target_layer", TARGET_LAYER);
    root.put("attacker_index", attackerIndex);
    root.put("attacker_node_id", attackerNodeId == null ? null : attackerNodeId.toString());

    root.put("topics", buildTopicsJson());
    return root;
  }

  public Map<String, Object> buildPerAttackerView() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("attacker_index", attackerIndex);
    out.put("attacker_node_id", attackerNodeId == null ? null : attackerNodeId.toString());
    out.put("topics", buildTopicsJson());
    return out;
  }

  public Map<String, Object> buildObservationCounts() {
    LinkedHashMap<String, Object> counts = new LinkedHashMap<>();
    long graftSent = 0L;
    long acceptedIncomingGraft = 0L;
    long pruneSent = 0L;
    long pruneReceived = 0L;
    long ihaveReceived = 0L;
    long iwantReceived = 0L;
    long messageReceived = 0L;
    for (TopicState topicState : topics.values()) {
      graftSent += topicState.graftSentCount;
      acceptedIncomingGraft += topicState.acceptedIncomingGraftCount;
      pruneSent += topicState.pruneSentCount;
      pruneReceived += topicState.pruneReceivedCount;
      ihaveReceived += topicState.ihaveReceivedCount;
      iwantReceived += topicState.iwantReceivedCount;
      messageReceived += topicState.messageReceivedCount;
    }
    counts.put("topics_seen", topics.size());
    counts.put("graft_sent", graftSent);
    counts.put("accepted_incoming_graft", acceptedIncomingGraft);
    counts.put("prune_sent", pruneSent);
    counts.put("prune_received", pruneReceived);
    counts.put("ihave_received", ihaveReceived);
    counts.put("iwant_received", iwantReceived);
    counts.put("message_received", messageReceived);
    return counts;
  }

  public Map<String, Object> buildTopicsJson() {
    LinkedHashMap<String, Object> topicsJson = new LinkedHashMap<>();
    for (Map.Entry<String, TopicState> entry : topics.entrySet()) {
      topicsJson.put(entry.getKey(), entry.getValue().toJson());
    }
    return topicsJson;
  }

  public Map<String, List<String>> snapshotOutgoingNeighborsByTopic() {
    LinkedHashMap<String, List<String>> out = new LinkedHashMap<>();
    for (Map.Entry<String, TopicState> entry : topics.entrySet()) {
      out.put(entry.getKey(), entry.getValue().snapshotOutgoingNeighbors());
    }
    return out;
  }

  public Map<String, List<String>> snapshotIncomingNeighborsByTopic() {
    LinkedHashMap<String, List<String>> out = new LinkedHashMap<>();
    for (Map.Entry<String, TopicState> entry : topics.entrySet()) {
      out.put(entry.getKey(), entry.getValue().snapshotIncomingNeighbors());
    }
    return out;
  }

  public List<InferenceEvent> snapshotInferenceEvents() {
    return new ArrayList<>(inferenceEvents);
  }

  public long getFirstObservedHeartbeat() {
    long firstObservedHeartbeat = -1L;
    for (InferenceEvent event : inferenceEvents) {
      if (firstObservedHeartbeat < 0L || event.heartbeatIndex < firstObservedHeartbeat) {
        firstObservedHeartbeat = event.heartbeatIndex;
      }
    }
    return firstObservedHeartbeat;
  }

  public static void appendObservationPolicyMetadata(Map<String, Object> metadata) {
    metadata.put("warmup_end_heartbeat", GossipCommonConfig.inferenceWarmupEndHeartbeat());
    metadata.put("inference_start_heartbeat", GossipCommonConfig.inferenceStartHeartbeat);
    metadata.put("observation_policy_after_heartbeat_only", true);
  }

  private void recordEvent(
      long heartbeat,
      String topic,
      BigInteger peerId,
      String action,
      String cause,
      boolean inferredNeighborPresent) {
    recordEvent(
        heartbeat,
        topic,
        peerId == null ? "" : peerId.toString(),
        action,
        cause,
        inferredNeighborPresent);
  }

  private void recordEvent(
      long heartbeat,
      String topic,
      String peerId,
      String action,
      String cause,
      boolean inferredNeighborPresent) {
    inferenceEvents.add(
        new InferenceEvent(
            heartbeat,
            attackerIndex,
            attackerNodeId == null ? "" : attackerNodeId.toString(),
            topic == null ? "" : topic,
            peerId == null ? "" : peerId,
            action == null ? "" : action,
            cause == null ? "" : cause,
            inferredNeighborPresent));
  }

  private TopicState getOrCreateTopicState(String topic) {
    return topics.computeIfAbsent(topic, ignored -> new TopicState());
  }

  private static boolean isInferenceObservationActive(long heartbeat) {
    return GossipCommonConfig.isInferenceObservationActive(heartbeat);
  }

  private enum EvidenceType {
    IHAVE_RECEIVED,
    IWANT_RECEIVED,
    MESSAGE_RECEIVED
  }

  public static final class InferenceEvent {
    public final long heartbeatIndex;
    public final int observerIndex;
    public final String observerId;
    public final String topic;
    public final String peerId;
    public final String action;
    public final String cause;
    public final boolean inferredNeighborPresent;

    private InferenceEvent(
        long heartbeatIndex,
        int observerIndex,
        String observerId,
        String topic,
        String peerId,
        String action,
        String cause,
        boolean inferredNeighborPresent) {
      this.heartbeatIndex = heartbeatIndex;
      this.observerIndex = observerIndex;
      this.observerId = observerId;
      this.topic = topic;
      this.peerId = peerId;
      this.action = action;
      this.cause = cause;
      this.inferredNeighborPresent = inferredNeighborPresent;
    }
  }

  private static final class TopicState {
    private final TreeSet<String> outgoingNeighbors = new TreeSet<>();
    private final TreeSet<String> incomingNeighbors = new TreeSet<>();
    private final TreeMap<String, PeerEvidence> peerState = new TreeMap<>();
    private long graftSentCount = 0L;
    private long acceptedIncomingGraftCount = 0L;
    private long pruneSentCount = 0L;
    private long pruneReceivedCount = 0L;
    private long ihaveReceivedCount = 0L;
    private long iwantReceivedCount = 0L;
    private long messageReceivedCount = 0L;

    private boolean setOutgoingPresent(BigInteger peerId, long heartbeat, String cause) {
      String key = peerId.toString();
      outgoingNeighbors.add(key);
      PeerEvidence evidence = getOrCreatePeerEvidence(key);
      evidence.noteObservedHeartbeat(heartbeat);
      evidence.outgoingPresent = true;
      evidence.lastEdgeChangeHeartbeat = heartbeat;
      evidence.lastEdgeChangeCause = cause;
      evidence.graftSent++;
      graftSentCount++;
      return isNeighborPresent(key);
    }

    private boolean setIncomingPresent(BigInteger peerId, long heartbeat, String cause) {
      String key = peerId.toString();
      incomingNeighbors.add(key);
      PeerEvidence evidence = getOrCreatePeerEvidence(key);
      evidence.noteObservedHeartbeat(heartbeat);
      evidence.incomingPresent = true;
      evidence.lastEdgeChangeHeartbeat = heartbeat;
      evidence.lastEdgeChangeCause = cause;
      evidence.acceptedIncomingGraft++;
      acceptedIncomingGraftCount++;
      return isNeighborPresent(key);
    }

    private boolean applySentPrune(BigInteger peerId, long heartbeat) {
      String key = peerId.toString();
      outgoingNeighbors.remove(key);
      incomingNeighbors.remove(key);
      PeerEvidence evidence = getOrCreatePeerEvidence(key);
      evidence.noteObservedHeartbeat(heartbeat);
      evidence.outgoingPresent = false;
      evidence.incomingPresent = false;
      evidence.lastEdgeChangeHeartbeat = heartbeat;
      evidence.lastEdgeChangeCause = "PRUNE_SENT";
      evidence.pruneSent++;
      pruneSentCount++;
      return isNeighborPresent(key);
    }

    private boolean applyReceivedPrune(BigInteger peerId, long heartbeat) {
      String key = peerId.toString();
      outgoingNeighbors.remove(key);
      incomingNeighbors.remove(key);
      PeerEvidence evidence = getOrCreatePeerEvidence(key);
      evidence.noteObservedHeartbeat(heartbeat);
      evidence.outgoingPresent = false;
      evidence.incomingPresent = false;
      evidence.lastEdgeChangeHeartbeat = heartbeat;
      evidence.lastEdgeChangeCause = "PRUNE_RECEIVED";
      evidence.pruneReceived++;
      pruneReceivedCount++;
      return isNeighborPresent(key);
    }

    private boolean noteEvidence(BigInteger peerId, long heartbeat, EvidenceType type) {
      String key = peerId.toString();
      PeerEvidence evidence = getOrCreatePeerEvidence(key);
      evidence.noteObservedHeartbeat(heartbeat);
      switch (type) {
        case IHAVE_RECEIVED:
          evidence.ihaveReceived++;
          ihaveReceivedCount++;
          break;
        case IWANT_RECEIVED:
          evidence.iwantReceived++;
          iwantReceivedCount++;
          break;
        case MESSAGE_RECEIVED:
          evidence.messageReceived++;
          messageReceivedCount++;
          break;
        default:
          break;
      }
      return isNeighborPresent(key);
    }

    private List<String> clearInferredTopology(long heartbeat) {
      TreeSet<String> affectedPeers = new TreeSet<>(outgoingNeighbors);
      affectedPeers.addAll(incomingNeighbors);
      outgoingNeighbors.clear();
      incomingNeighbors.clear();
      for (PeerEvidence evidence : peerState.values()) {
        evidence.outgoingPresent = false;
        evidence.incomingPresent = false;
        evidence.noteObservedHeartbeat(heartbeat);
        evidence.lastEdgeChangeHeartbeat = heartbeat;
        evidence.lastEdgeChangeCause = "LOCAL_LEAVE";
      }
      return new ArrayList<>(affectedPeers);
    }

    private boolean isNeighborPresent(String peerId) {
      return outgoingNeighbors.contains(peerId) || incomingNeighbors.contains(peerId);
    }

    private PeerEvidence getOrCreatePeerEvidence(String peerId) {
      return peerState.computeIfAbsent(peerId, ignored -> new PeerEvidence());
    }

    private Map<String, Object> toJson() {
      LinkedHashMap<String, Object> out = new LinkedHashMap<>();
      out.put("outgoing_neighbors", new ArrayList<>(outgoingNeighbors));
      out.put("incoming_neighbors", new ArrayList<>(incomingNeighbors));
      TreeSet<String> undirected = new TreeSet<>(outgoingNeighbors);
      undirected.addAll(incomingNeighbors);
      out.put("undirected_neighbors", new ArrayList<>(undirected));

      LinkedHashMap<String, Object> peerJson = new LinkedHashMap<>();
      for (Map.Entry<String, PeerEvidence> entry : peerState.entrySet()) {
        peerJson.put(entry.getKey(), entry.getValue().toJson());
      }
      out.put("peer_state", peerJson);
      return out;
    }

    private List<String> snapshotOutgoingNeighbors() {
      return new ArrayList<>(outgoingNeighbors);
    }

    private List<String> snapshotIncomingNeighbors() {
      return new ArrayList<>(incomingNeighbors);
    }
  }

  private static final class PeerEvidence {
    private boolean outgoingPresent = false;
    private boolean incomingPresent = false;
    private long firstObservedHeartbeat = -1L;
    private long lastObservedHeartbeat = -1L;
    private long lastDistinctObservedHeartbeat = -1L;
    private long distinctObservationHeartbeats = 0L;
    private long lastEdgeChangeHeartbeat = -1L;
    private String lastEdgeChangeCause = "";
    private long graftSent = 0L;
    private long acceptedIncomingGraft = 0L;
    private long pruneSent = 0L;
    private long pruneReceived = 0L;
    private long ihaveReceived = 0L;
    private long iwantReceived = 0L;
    private long messageReceived = 0L;

    private Map<String, Object> toJson() {
      LinkedHashMap<String, Object> out = new LinkedHashMap<>();
      out.put("outgoing_present", outgoingPresent);
      out.put("incoming_present", incomingPresent);
      out.put("first_observed_heartbeat", firstObservedHeartbeat);
      out.put("last_observed_heartbeat", lastObservedHeartbeat);
      out.put("distinct_observation_heartbeats", distinctObservationHeartbeats);
      out.put("last_edge_change_heartbeat", lastEdgeChangeHeartbeat);
      out.put("last_edge_change_cause", lastEdgeChangeCause);

      LinkedHashMap<String, Object> evidenceCounts = new LinkedHashMap<>();
      evidenceCounts.put("graft_sent", graftSent);
      evidenceCounts.put("accepted_incoming_graft", acceptedIncomingGraft);
      evidenceCounts.put("prune_sent", pruneSent);
      evidenceCounts.put("prune_received", pruneReceived);
      evidenceCounts.put("ihave_received", ihaveReceived);
      evidenceCounts.put("iwant_received", iwantReceived);
      evidenceCounts.put("message_received", messageReceived);
      out.put("evidence_counts", evidenceCounts);
      return out;
    }

    private void noteObservedHeartbeat(long heartbeat) {
      if (firstObservedHeartbeat < 0) {
        firstObservedHeartbeat = heartbeat;
      }
      if (lastDistinctObservedHeartbeat != heartbeat) {
        distinctObservationHeartbeats++;
        lastDistinctObservedHeartbeat = heartbeat;
      }
      lastObservedHeartbeat = heartbeat;
    }
  }
}
