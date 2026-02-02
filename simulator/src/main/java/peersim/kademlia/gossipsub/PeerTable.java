package peersim.kademlia.gossipsub;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import peersim.kademlia.gossipsub.inference.passive.PassiveCoalitionRegistry;
import peersim.kademlia.gossipsub.inference.passive.PassiveEgoMeshRegistry;

public class PeerTable {

  private HashMap<String, HashSet<BigInteger>> peerMap; // , samplesIndexed;
  private HashSet<BigInteger> bootstrapPeers;

  public PeerTable() {

    peerMap = new HashMap<>();
    bootstrapPeers = new HashSet<>();
  }

  public void addPeer(String topic, BigInteger peer) {
    if (topic == null || peer == null) return;
    if (!isDiscoverablePeer(peer)) return;
    if (peerMap.get(topic) != null) {
      HashSet<BigInteger> nodes = peerMap.get(topic);
      nodes.add(peer);
    } else {
      HashSet<BigInteger> nodes = new HashSet<>();
      nodes.add(peer);
      peerMap.put(topic, nodes);
    }
  }

  public HashSet<BigInteger> getPeers(String topic) {
    HashSet<BigInteger> topicPeers = peerMap.get(topic);
    if (topicPeers == null) return null;
    HashSet<BigInteger> visible = new HashSet<>();
    for (BigInteger peer : topicPeers) {
      if (isDiscoverablePeer(peer)) {
        visible.add(peer);
      }
    }
    return visible;
  }

  public void addBootstrapPeer(BigInteger peer) {
    if (peer == null || !isDiscoverablePeer(peer)) return;
    bootstrapPeers.add(peer);
  }

  public HashSet<BigInteger> getBootstrapPeers() {
    HashSet<BigInteger> visible = new HashSet<>();
    for (BigInteger peer : bootstrapPeers) {
      if (isDiscoverablePeer(peer)) {
        visible.add(peer);
      }
    }
    return visible;
  }

  public HashSet<BigInteger> getNPeers(String topic, int n, HashSet<BigInteger> peers) {
    HashSet<BigInteger> nodes = new HashSet<>();
    if (peerMap.get(topic) != null) {
      HashSet<BigInteger> topicPeers = peerMap.get(topic);
      for (BigInteger id : topicPeers) {
        if (!isDiscoverablePeer(id)) continue;
        if (!peers.contains(id)) nodes.add(id);
        if (nodes.size() == n) break;
      }
    }

    return nodes;
  }

  private boolean isDiscoverablePeer(BigInteger peer) {
    return PassiveCoalitionRegistry.isDiscoverablePeer(peer)
        && PassiveEgoMeshRegistry.isDiscoverablePeer(peer);
  }
}
