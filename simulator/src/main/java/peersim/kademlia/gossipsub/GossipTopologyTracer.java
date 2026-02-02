package peersim.kademlia.gossipsub;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import peersim.core.CommonState;

/**
 * Tracer for GossipSub mesh topology changes over time. Records GRAFT/PRUNE events and periodic
 * mesh snapshots for visualization and analysis.
 */
public class GossipTopologyTracer {

  /** Log of mesh events (GRAFT/PRUNE) */
  private static List<Map<String, Object>> meshEvents = new ArrayList<>();

  /** Periodic snapshots of complete mesh state */
  private static List<Map<String, Object>> meshSnapshots = new ArrayList<>();

  /** Last snapshot time to avoid duplicates */
  private static long lastSnapshotTime = -1;

  /** Event sequence counter for ordering */
  private static long eventSequence = 0;
  /** Snapshot interval in simulation time units */
  private static final long SNAPSHOT_INTERVAL = 1000;

  /**
   * Records a GRAFT event (peer added to mesh)
   *
   * @param nodeId ID of the node performing the GRAFT
   * @param peerId ID of the peer being grafted
   * @param topic Topic for which the graft occurred
   */
  public static void recordGraft(BigInteger nodeId, BigInteger peerId, String topic) {
    Map<String, Object> event = new HashMap<>();
    event.put("timestamp", eventSequence++);
    event.put("node_id", nodeId.toString());
    event.put("peer_id", peerId.toString());
    event.put("topic", topic);
    event.put("action", "GRAFT");
    synchronized (meshEvents) {
      meshEvents.add(event);
    }
  }

  /**
   * Records a PRUNE event (peer removed from mesh)
   *
   * @param nodeId ID of the node performing the PRUNE
   * @param peerId ID of the peer being pruned
   * @param topic Topic for which the prune occurred
   */
  public static void recordPrune(BigInteger nodeId, BigInteger peerId, String topic) {
    Map<String, Object> event = new HashMap<>();
    event.put("timestamp", eventSequence++);
    event.put("node_id", nodeId.toString());
    event.put("peer_id", peerId.toString());
    event.put("topic", topic);
    event.put("action", "PRUNE");
    synchronized (meshEvents) {
      meshEvents.add(event);
    }
  }

  /**
   * Records a JOIN event (node joins a topic)
   *
   * @param nodeId ID of the node joining
   * @param topic Topic being joined
   * @param initialMeshSize Size of initial mesh
   */
  public static void recordJoin(BigInteger nodeId, String topic, int initialMeshSize) {
    Map<String, Object> event = new HashMap<>();
    event.put("timestamp", CommonState.getTime());
    event.put("node_id", nodeId.toString());
    event.put("peer_id", ""); // No specific peer for JOIN
    event.put("topic", topic);
    event.put("action", "JOIN");
    event.put("mesh_size", initialMeshSize);
    synchronized (meshEvents) {
      meshEvents.add(event);
    }
  }

  /**
   * Records a LEAVE event (node leaves a topic)
   *
   * @param nodeId ID of the node leaving
   * @param topic Topic being left
   */
  public static void recordLeave(BigInteger nodeId, String topic) {
    Map<String, Object> event = new HashMap<>();
    event.put("timestamp", CommonState.getTime());
    event.put("node_id", nodeId.toString());
    event.put("peer_id", ""); // No specific peer for LEAVE
    event.put("topic", topic);
    event.put("action", "LEAVE");
    synchronized (meshEvents) {
      meshEvents.add(event);
    }
  }

  /**
   * Records a periodic snapshot of the entire mesh topology. Should be called during heartbeat at
   * regular intervals.
   *
   * @param allMeshes Map of all nodes' mesh states: nodeId -> (topic -> Set of peer IDs)
   */
  public static void recordSnapshot(
      long heartbeatIndex,
      Map<BigInteger, Map<String, HashSet<BigInteger>>> allMeshes,
      boolean forceEveryHeartbeat) {
    long currentTime = CommonState.getTime();

    // Only take snapshots at defined intervals
    if (!forceEveryHeartbeat && currentTime - lastSnapshotTime < SNAPSHOT_INTERVAL) {
      return;
    }

    lastSnapshotTime = currentTime;

    synchronized (meshSnapshots) {
      meshSnapshots.removeIf(
          snapshot ->
              Long.toString(heartbeatIndex)
                  .equals(snapshot.getOrDefault("heartbeat_index", "-1").toString()));

      Map<String, Object> heartbeatMarker = new HashMap<>();
      heartbeatMarker.put("timestamp", currentTime);
      heartbeatMarker.put("heartbeat_index", heartbeatIndex);
      heartbeatMarker.put("node_id", "");
      heartbeatMarker.put("peer_id", "");
      heartbeatMarker.put("topic", "");
      heartbeatMarker.put("mesh_size", 0);
      heartbeatMarker.put("snapshot_kind", "heartbeat_marker");
      meshSnapshots.add(heartbeatMarker);

      for (Map.Entry<BigInteger, Map<String, HashSet<BigInteger>>> nodeEntry :
          allMeshes.entrySet()) {
        BigInteger nodeId = nodeEntry.getKey();
        Map<String, HashSet<BigInteger>> nodeMeshes = nodeEntry.getValue();

        for (Map.Entry<String, HashSet<BigInteger>> topicEntry : nodeMeshes.entrySet()) {
          String topic = topicEntry.getKey();
          HashSet<BigInteger> peers = topicEntry.getValue();

          // Record each edge in the mesh
          for (BigInteger peerId : peers) {
            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("timestamp", currentTime);
            snapshot.put("heartbeat_index", heartbeatIndex);
            snapshot.put("node_id", nodeId.toString());
            snapshot.put("peer_id", peerId.toString());
            snapshot.put("topic", topic);
            snapshot.put("mesh_size", peers.size());
            snapshot.put("snapshot_kind", "mesh_edge");
            meshSnapshots.add(snapshot);
          }
        }
      }
    }
  }

  public static void recordSnapshot(Map<BigInteger, Map<String, HashSet<BigInteger>>> allMeshes) {
    recordSnapshot(-1L, allMeshes, false);
  }

  /**
   * Records mesh statistics for a specific node at current time
   *
   * @param nodeId ID of the node
   * @param topic Topic name
   * @param meshSize Current mesh size
   * @param fanoutSize Current fanout size (if applicable)
   */
  public static void recordMeshStats(
      BigInteger nodeId, String topic, int meshSize, int fanoutSize) {
    Map<String, Object> event = new HashMap<>();
    event.put("timestamp", CommonState.getTime());
    event.put("node_id", nodeId.toString());
    event.put("topic", topic);
    event.put("action", "STATS");
    event.put("mesh_size", meshSize);
    event.put("fanout_size", fanoutSize);
    synchronized (meshEvents) {
      meshEvents.add(event);
    }
  }

  /**
   * @return List of all recorded mesh events
   */
  public static List<Map<String, Object>> getMeshEvents() {
    synchronized (meshEvents) {
      return new ArrayList<>(meshEvents);
    }
  }

  /**
   * @return List of all recorded mesh snapshots
   */
  public static List<Map<String, Object>> getMeshSnapshots() {
    synchronized (meshSnapshots) {
      return new ArrayList<>(meshSnapshots);
    }
  }

  /** Clears all recorded events and snapshots */
  public static void clear() {
    synchronized (meshEvents) {
      meshEvents.clear();
    }
    synchronized (meshSnapshots) {
      meshSnapshots.clear();
    }
    lastSnapshotTime = -1;
    eventSequence = 0;
  }

  /**
   * @return Number of recorded events
   */
  public static int getEventCount() {
    synchronized (meshEvents) {
      return meshEvents.size();
    }
  }

  /**
   * @return Number of recorded snapshots
   */
  public static int getSnapshotCount() {
    synchronized (meshSnapshots) {
      return meshSnapshots.size();
    }
  }
}
