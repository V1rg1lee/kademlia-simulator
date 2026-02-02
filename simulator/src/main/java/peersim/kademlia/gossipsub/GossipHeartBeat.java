package peersim.kademlia.gossipsub;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.gossipsub.inference.passive.PassiveCoalitionRegistry;
import peersim.kademlia.gossipsub.inference.passive.PassiveEgoMeshRegistry;
import peersim.kademlia.gossipsub.inference.px.PxFloodRegistry;

public class GossipHeartBeat implements Control {

  private String prefix;
  private static long globalHeartbeatIndex = 0L;

  private static final String PAR_PROT = "protocol";
  protected final int pid;

  // ______________________________________________________________________________________________
  public GossipHeartBeat(String prefix) {
    this.prefix = prefix;

    pid = Configuration.getPid(prefix + "." + PAR_PROT);
  }

  public static void resetGlobalHeartbeatIndex() {
    globalHeartbeatIndex = 0L;
  }

  public static long getGlobalHeartbeatIndex() {
    return globalHeartbeatIndex;
  }

  public static Map<BigInteger, Map<String, HashSet<BigInteger>>> collectAllMeshes(int pid) {
    Map<BigInteger, Map<String, HashSet<BigInteger>>> allMeshes = new HashMap<>();
    for (int i = 0; i < Network.size(); i++) {
      Node iNode = Network.get(i);
      if (iNode.getFailState() != Node.OK) {
        continue;
      }

      GossipSubProtocol iKad = (GossipSubProtocol) iNode.getProtocol(pid);
      if (iKad == null) {
        continue;
      }

      Map<String, HashSet<BigInteger>> nodeMesh = iKad.getMeshState();
      if (nodeMesh != null && !nodeMesh.isEmpty()) {
        allMeshes.put(iKad.getGossipNode().getId(), nodeMesh);
      }
    }
    return allMeshes;
  }

  // ______________________________________________________________________________________________
  public boolean execute() {
    globalHeartbeatIndex++;
    PassiveEgoMeshRegistry.onGlobalHeartbeatStart(globalHeartbeatIndex);
    PassiveCoalitionRegistry.onGlobalHeartbeatStart(globalHeartbeatIndex);
    PxFloodRegistry.onGlobalHeartbeatStart(globalHeartbeatIndex);

    for (int i = 0; i < Network.size(); i++) {
      Node iNode = Network.get(i);
      if (iNode.getFailState() == Node.OK) {

        GossipSubProtocol iKad = (GossipSubProtocol) iNode.getProtocol(pid);
        if (iKad != null) {
          iKad.heartBeat();
        }
      }
    }

    Map<BigInteger, Map<String, HashSet<BigInteger>>> allMeshes = collectAllMeshes(pid);

    // Record periodic snapshot of all meshes
    GossipTopologyTracer.recordSnapshot(globalHeartbeatIndex, allMeshes, false);

    return false;
  }
} // End of class
