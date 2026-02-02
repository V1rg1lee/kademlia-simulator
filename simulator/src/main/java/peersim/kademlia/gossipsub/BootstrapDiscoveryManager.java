package peersim.kademlia.gossipsub;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import peersim.core.CommonState;

/**
 * Minimal explicit discovery/dial boundary for GossipSub.
 *
 * <p>The bootstrap backend is intentionally bounded and only available during the configured
 * warm-up. Outside that window, this manager only arbitrates dials for peers already learned
 * through a non-oracle path such as accepted PX.
 */
public class BootstrapDiscoveryManager {

  public enum DialResult {
    SUCCESS,
    FAILURE,
    BUDGET_EXHAUSTED
  }

  private static class TopicState {
    boolean initialBootstrapServed;
    long lastBootstrapHeartbeat;
    int bootstrapServedThisHeartbeat;
    long lastAmbientHeartbeat;

    TopicState() {
      initialBootstrapServed = false;
      lastBootstrapHeartbeat = Long.MIN_VALUE;
      bootstrapServedThisHeartbeat = 0;
      lastAmbientHeartbeat = Long.MIN_VALUE;
    }
  }

  private final HashMap<String, TopicState> topicStates;
  private long dialBudgetHeartbeat;
  private int dialsThisHeartbeat;

  public BootstrapDiscoveryManager() {
    this.topicStates = new HashMap<>();
    this.dialBudgetHeartbeat = Long.MIN_VALUE;
    this.dialsThisHeartbeat = 0;
  }

  public boolean isBootstrapWarmupActive(long heartbeat) {
    return GossipCommonConfig.bootstrapAssistEnabled
        && heartbeat <= GossipCommonConfig.bootstrapWindowHeartbeats;
  }

  public boolean isAmbientDiscoveryActive(long heartbeat) {
    return GossipCommonConfig.ambientDiscoveryEnabled
        && heartbeat > GossipCommonConfig.bootstrapWindowHeartbeats;
  }

  public HashSet<BigInteger> discoverBootstrapPeers(
      PeerTable bootstrapPeers,
      String topic,
      BigInteger localPeerId,
      Collection<BigInteger> excludedPeers,
      long heartbeat) {
    HashSet<BigInteger> discovered = new HashSet<>();
    if (topic == null || bootstrapPeers == null || !isBootstrapWarmupActive(heartbeat)) {
      return discovered;
    }

    TopicState state = topicStates.computeIfAbsent(topic, unused -> new TopicState());
    if (state.lastBootstrapHeartbeat != heartbeat) {
      state.lastBootstrapHeartbeat = heartbeat;
      state.bootstrapServedThisHeartbeat = 0;
    }

    int limit;
    if (!state.initialBootstrapServed) {
      limit = GossipCommonConfig.bootstrapInitialPeersPerNode;
      state.initialBootstrapServed = true;
    } else {
      limit =
          Math.max(
              0,
              GossipCommonConfig.bootstrapCandidatesPerHeartbeat
                  - state.bootstrapServedThisHeartbeat);
    }
    if (limit <= 0) {
      return discovered;
    }

    // Warm-up bootstrap only returns generic network entry points. The topic
    // still scopes the local budget, but it must not filter the backend sample.
    discovered.addAll(sampleGenericPeers(bootstrapPeers, localPeerId, excludedPeers, limit));

    state.bootstrapServedThisHeartbeat += discovered.size();
    return discovered;
  }

  public HashSet<BigInteger> discoverAmbientPeers(
      PeerTable ambientPeers,
      String topic,
      BigInteger localPeerId,
      Collection<BigInteger> excludedPeers,
      long heartbeat) {
    HashSet<BigInteger> discovered = new HashSet<>();
    if (topic == null
        || ambientPeers == null
        || !isAmbientDiscoveryActive(heartbeat)
        || GossipCommonConfig.ambientDiscoveryPeersPerInterval <= 0) {
      return discovered;
    }

    long ticksAfterWarmup = heartbeat - GossipCommonConfig.bootstrapWindowHeartbeats - 1L;
    if (ticksAfterWarmup < 0
        || ticksAfterWarmup % GossipCommonConfig.ambientDiscoveryIntervalHeartbeats != 0) {
      return discovered;
    }

    TopicState state = topicStates.computeIfAbsent(topic, unused -> new TopicState());
    if (state.lastAmbientHeartbeat == heartbeat) {
      return discovered;
    }
    state.lastAmbientHeartbeat = heartbeat;

    // Topic scopes the local rate limit only; the population sample remains a
    // generic network-level entry-point sample.
    discovered.addAll(
        sampleGenericPeers(
            ambientPeers,
            localPeerId,
            excludedPeers,
            GossipCommonConfig.ambientDiscoveryPeersPerInterval));
    return discovered;
  }

  public DialResult requestDial(
      String topic,
      BigInteger localPeerId,
      BigInteger remotePeerId,
      String reason,
      long heartbeat) {
    if (topic == null
        || localPeerId == null
        || remotePeerId == null
        || localPeerId.equals(remotePeerId)) {
      return DialResult.FAILURE;
    }
    resetDialBudgetIfNeeded(heartbeat);
    if (dialsThisHeartbeat >= GossipCommonConfig.discoveryMaxDialsPerHeartbeat) {
      return DialResult.BUDGET_EXHAUSTED;
    }

    dialsThisHeartbeat++;
    if (CommonState.r.nextDouble() <= GossipCommonConfig.discoveryDialSuccessProbability) {
      return DialResult.SUCCESS;
    }
    return DialResult.FAILURE;
  }

  public void onDialSuccess(String topic, BigInteger peerId, String reason, long heartbeat) {
    // Hook for a later external discovery/connection model.
  }

  public void onDialFailure(String topic, BigInteger peerId, String reason, long heartbeat) {
    // Hook for a later external discovery/connection model.
  }

  private void resetDialBudgetIfNeeded(long heartbeat) {
    if (dialBudgetHeartbeat != heartbeat) {
      dialBudgetHeartbeat = heartbeat;
      dialsThisHeartbeat = 0;
    }
  }

  private HashSet<BigInteger> sampleGenericPeers(
      PeerTable peerTable,
      BigInteger localPeerId,
      Collection<BigInteger> excludedPeers,
      int limit) {
    HashSet<BigInteger> discovered = new HashSet<>();
    if (peerTable == null || limit <= 0) {
      return discovered;
    }

    HashSet<BigInteger> visiblePeers = peerTable.getBootstrapPeers();
    if (visiblePeers == null || visiblePeers.isEmpty()) {
      return discovered;
    }

    HashSet<BigInteger> excluded = new HashSet<>();
    if (excludedPeers != null) {
      excluded.addAll(excludedPeers);
    }
    if (localPeerId != null) {
      excluded.add(localPeerId);
    }

    List<BigInteger> candidates = new ArrayList<>(visiblePeers);
    Collections.shuffle(candidates, CommonState.r);
    for (BigInteger peerId : candidates) {
      if (peerId == null || excluded.contains(peerId)) {
        continue;
      }
      discovered.add(peerId);
      if (discovered.size() >= limit) {
        break;
      }
    }
    return discovered;
  }
}
