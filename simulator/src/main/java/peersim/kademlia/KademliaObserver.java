package peersim.kademlia;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.das.Neighbour;
import peersim.kademlia.das.SearchTable;
import peersim.kademlia.gossipsub.GossipCommonConfig;
import peersim.kademlia.gossipsub.GossipHeartBeat;
import peersim.kademlia.gossipsub.GossipSubProtocol;
import peersim.kademlia.gossipsub.GossipTopologyTracer;
import peersim.kademlia.gossipsub.inference.passive.PassiveCoalitionRegistry;
import peersim.kademlia.gossipsub.inference.passive.PassiveEgoMeshRegistry;
import peersim.kademlia.gossipsub.inference.px.PxFloodRegistry;
import peersim.kademlia.operations.Operation;
import peersim.util.IncrementalStats;

/**
 * This class implements a simple observer of search time and hop average in finding a node in the
 * network
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KademliaObserver implements Control {

  /** Configuration strings to read */
  private static final String PAR_STEP = "step";

  private static final String PAR_FOLDER = "logfolder";

  private static final String PAR_PROTOCOL = "protocol";

  /** keep statistics of the number of hops of every message delivered. */
  public static IncrementalStats hopStore = new IncrementalStats();

  /** keep statistics of the time every message delivered. */
  public static IncrementalStats timeStore = new IncrementalStats();

  /** keep statistic of number of message delivered */
  public static IncrementalStats msg_deliv = new IncrementalStats();

  /** keep statistic of number of find operation */
  public static IncrementalStats find_op = new IncrementalStats();

  /** Parameter of the protocol we want to observe */
  private static final String PAR_PROT = "protocol";

  /** Successfull find operations */
  public static IncrementalStats find_ok = new IncrementalStats();

  /** Messages exchanged in the Kademlia network */
  private static HashMap<Long, Map<String, Object>> messages =
      new HashMap<Long, Map<String, Object>>();

  /** Log of operations in the Kademlia network */
  private static HashMap<Long, Map<String, Object>> operations =
      new HashMap<Long, Map<String, Object>>();

  /** Log of operations in the Kademlia network */
  private static HashMap<Long, Map<String, Object>> peerDiscoveries =
      new HashMap<Long, Map<String, Object>>();

  private static HashMap<BigInteger, Integer> msgsIn = new HashMap<>();
  private static HashMap<BigInteger, Integer> msgsOut = new HashMap<>();
  private static HashMap<BigInteger, Integer> bytesIn = new HashMap<>();
  private static HashMap<BigInteger, Integer> bytesOut = new HashMap<>();
  /** Name of the folder where experiment logs are written */
  private static String baseLogFolderName;

  private static String experimentLogFolderName;
  private static int currentExperimentIndex = 0;
  private static long currentExperimentSeed = -1;
  private static int totalExperiments = 1;

  private static BigInteger builderId =
      new BigInteger(
          "83814183170291850251680823880522715558189094423550585243365458794131648333116");
  /** The time granularity of reporting metrics */
  private static int observerStep;

  private static int kademliaid;
  /**
   * Constructor to initialize the observer.
   *
   * @param prefix the configuration prefix
   */
  public KademliaObserver(String prefix) {
    observerStep = Configuration.getInt(prefix + "." + PAR_STEP);

    baseLogFolderName = Configuration.getString(prefix + "." + PAR_FOLDER, "./logs");
    totalExperiments = Configuration.getInt(peersim.Simulator.PAR_EXPS, 1);
    experimentLogFolderName = buildExperimentLogFolder();
    kademliaid = Configuration.getPid(prefix + "." + PAR_PROTOCOL);

    // System.out.println("Logfolder: " + logFolderName);
  }

  private static void writeLogs(Map<Long, Map<String, Object>> map, String filename) {
    try (FileWriter writer = new FileWriter(filename)) {
      Set<String> keySet = new HashSet<String>();
      for (Map<String, Object> m : map.values())
        if (m.keySet().size() > keySet.size()) keySet = m.keySet();

      String header = "";

      // If keySet is empty, write default headers based on filename
      if (keySet.isEmpty()) {
        if (filename.contains("operation")) {
          header = "src,dst,timestamp,stopTime,id\n";
        } else if (filename.contains("peerDiscoveries")) {
          header = "time,message_id,dst_id,src_id,total_peers,total_peers_alive,peers_in_message\n";
        } else {
          header = "\n";
        }
        writer.write(header);
      } else {
        // Build header from keys
        for (Object key : keySet) {
          header += key + ",";
        }

        // Write the comma seperated keys as the header of the file
        // If keySet is empty, write an empty header row
        if (header.length() > 0) {
          header = header.substring(0, header.length() - 1);
        }
        header += "\n";
        writer.write(header);
      }
      // Iterate through each find operation and write its data to the file
      for (Map<String, Object> entry : map.values()) {
        String line = "";
        for (Object key : keySet) {
          if (entry.get(key) != null) line += entry.get(key).toString() + ",";
          else line += ",";
        }

        // Remove the last comma and add a newline character
        if (line.length() > 0) {
          line = line.substring(0, line.length() - 1);
        }
        line += "\n";
        writer.write(line);
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    map.clear();
  }

  /** Writes log data to files. */
  public static void writeOut() {
    // System.out.println("Writing out");

    File directory = new File(getCurrentLogFolderName());
    if (!directory.exists()) {
      directory.mkdirs();
    }

    HashMap<Long, Map<String, Object>> msgsInOut = writeMessages();
    // Write messages log to file if not empty
    if (!msgsInOut.isEmpty()) {
      writeLogs(msgsInOut, getCurrentLogFolderName() + "/" + "messages.csv");
    }
    // Always write operations file (even if empty for GossipSub simulations)
    writeLogs(operations, getCurrentLogFolderName() + "/" + "operation.csv");
    // Always write peerDiscoveries file (even if empty)
    if (!peerDiscoveries.isEmpty()) {
      writeLogs(peerDiscoveries, getCurrentLogFolderName() + "/" + "peerDiscoveries.csv");
    }

    // Write GossipSub topology logs
    writeTopologyLogs();
    writeRunInfo();
    writeScoreSummary();
    try {
      PassiveEgoMeshRegistry.writeOutputs(getCurrentLogFolderName());
      PassiveCoalitionRegistry.writeOutputs(getCurrentLogFolderName());
      PxFloodRegistry.writeOutputs(getCurrentLogFolderName());
    } catch (IOException e) {
      throw new RuntimeException("Failed to write inference outputs", e);
    }
  }

  /** Writes GossipSub mesh topology logs to CSV files. */
  private static void writeTopologyLogs() {
    GossipTopologyTracer.recordSnapshot(
        GossipHeartBeat.getGlobalHeartbeatIndex(),
        GossipHeartBeat.collectAllMeshes(kademliaid),
        true);

    // Write mesh events (GRAFT/PRUNE/JOIN/LEAVE)
    List<Map<String, Object>> events = GossipTopologyTracer.getMeshEvents();
    if (!events.isEmpty()) {
      try (FileWriter writer =
          new FileWriter(getCurrentLogFolderName() + "/" + "mesh_events.csv")) {
        // Write header
        writer.write("topic,action,mesh_size,timestamp,node_id,peer_id\n");

        // Write each event with proper column ordering
        for (Map<String, Object> event : events) {
          String topic = event.getOrDefault("topic", "").toString();
          String action = event.getOrDefault("action", "").toString();
          String meshSize = event.getOrDefault("mesh_size", "0").toString();
          String timestamp = event.getOrDefault("timestamp", "0").toString();
          String nodeId = event.getOrDefault("node_id", "").toString();
          String peerId = event.getOrDefault("peer_id", "").toString();

          // Write line with proper escaping for special characters
          writer.write(
              String.format(
                  "%s,%s,%s,%s,%s,%s\n", topic, action, meshSize, timestamp, nodeId, peerId));
        }
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // Write mesh snapshots
    List<Map<String, Object>> snapshots = GossipTopologyTracer.getMeshSnapshots();
    if (!snapshots.isEmpty()) {
      try (FileWriter writer =
          new FileWriter(getCurrentLogFolderName() + "/" + "mesh_snapshots.csv")) {
        // Write header
        writer.write("timestamp,heartbeat_index,node_id,peer_id,topic,mesh_size,snapshot_kind\n");

        // Write each snapshot with proper column ordering
        for (Map<String, Object> snapshot : snapshots) {
          String timestamp = snapshot.getOrDefault("timestamp", "0").toString();
          String heartbeatIndex = snapshot.getOrDefault("heartbeat_index", "-1").toString();
          String nodeId = snapshot.getOrDefault("node_id", "").toString();
          String peerId = snapshot.getOrDefault("peer_id", "").toString();
          String topic = snapshot.getOrDefault("topic", "").toString();
          String meshSize = snapshot.getOrDefault("mesh_size", "0").toString();
          String snapshotKind = snapshot.getOrDefault("snapshot_kind", "mesh_edge").toString();

          writer.write(
              String.format(
                  "%s,%s,%s,%s,%s,%s,%s\n",
                  timestamp, heartbeatIndex, nodeId, peerId, topic, meshSize, snapshotKind));
        }
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    System.out.println(
        "Topology logs written: "
            + events.size()
            + " events, "
            + snapshots.size()
            + " snapshot entries");
  }

  /**
   * Print the statistical snapshot of the current situation.
   *
   * @return always false
   */
  public boolean execute() {
    // Get the real network size
    int sz = Network.size();
    for (int i = 0; i < Network.size(); i++) {
      if (!Network.get(i).isUp()) {
        sz--;
      }
    }

    System.gc();
    String s =
        String.format(
            "[time=%d]:[N=%d current nodes UP] [D=%f msg deliv] [%f min h] [%f average h] [%f max h] [%d min l] [%d msec average l] [%d max l] [%d find msg sent]",
            CommonState.getTime(),
            sz,
            msg_deliv.getSum(),
            hopStore.getMin(),
            hopStore.getAverage(),
            hopStore.getMax(),
            (int) timeStore.getMin(),
            (int) timeStore.getAverage(),
            (int) timeStore.getMax(),
            (int) find_op.getSum());

    // Check if this is the last execution cycle of the experiment
    if (CommonState.getEndTime() <= (observerStep + CommonState.getTime())) {
      // Write out the logs to disk/permanent storage
      writeOut();
      // System.err.println(s);
    }

    return false;
  }

  /**
   * Reports a message, adding it to the message log if it has a source.
   *
   * @param m The message to report
   * @param sent a boolean indicating whether the message was sent or received.
   */
  public static void reportMsg(Message m, boolean sent, BigInteger id) {
    // Messages without a source are control messages sent by the traffic control,
    // so we don't want to log them.

    // BigInteger id2 = new
    // BigInteger("83814183170291850251680823880522715558189094423550585243365458794131648333116");
    // if(id.compareTo(id2)==0){
    /*if (sent) {
      if (m.getType() == Message.MSG_GET_SAMPLE)
        System.out.println(id + " sending sample request " + m.getSize());
      else System.out.println(id + " sending sample response " + m.getSize());
    } else {
      if (m.getType() == Message.MSG_GET_SAMPLE)
        System.out.println(id + " receiving sample request " + m.getSize());
      else System.out.println(id + " receiving sample response " + m.getSize());
    }*/
    // }
    if (m.src == null) {
      return;
    }
    // System.out.println("Reporting msg " + m);

    // Add the message to the message log, but first check if it hasn't already been added
    // assert (!messages.keySet().contains(m.id));
    // messages.put(m.id, m.toMap(sent));
    if (sent) {
      if (msgsOut.get(id) == null) {
        msgsOut.put(id, 1);
        bytesOut.put(id, m.getSize());
      } else {
        int msgs = msgsOut.get(id);
        int bytes = bytesOut.get(id);
        bytes += m.getSize();
        msgs++;
        msgsOut.put(id, msgs);
        bytesOut.put(id, bytes);
      }
    } else {
      if (msgsIn.get(id) == null) {
        msgsIn.put(id, 1);
        bytesIn.put(id, m.getSize());
      } else {
        int msgs = msgsIn.get(id);
        int bytes = bytesIn.get(id);
        msgs++;
        bytes += m.getSize();
        msgsIn.put(id, msgs);
        bytesIn.put(id, bytes);
      }
    }
  }

  /**
   * Reports an operation, adding it to the operation log.
   *
   * @param op The operation to report.
   */
  public static void reportOperation(Operation op) {
    // messages without source are control messages sent by the traffic control
    // Calculate the operation stop time and then add the opearation to the operation log.
    if (operations.keySet().contains(op.getId())) return;
    op.setStopTime(CommonState.getTime() - op.getTimestamp());
    operations.put(op.getId(), op.toMap());
  }

  public static void reportPeerDiscovery(Message m, SearchTable st) {

    if (m.src == null) return;
    // Add the message to the message log, but first check if it hasn't already been added
    assert (!peerDiscoveries.keySet().contains(m.id));
    Map<String, Object> result = new HashMap<String, Object>();
    Neighbour[] neighs = (Neighbour[]) m.value;

    int notKnown = 0;
    for (Neighbour n : neighs) {
      if (!st.isNeighbourKnown(n)) notKnown++;
    }
    result.put("time", CommonState.getTime());
    result.put("message_id", m.id);
    result.put("dst_id", m.dst.getId());
    result.put("src_id", m.src.getId());
    result.put("total_peers", st.getAllNeighboursCount());
    result.put("total_peers_alive", st.getAllAliveNeighboursCount());
    result.put("peers_in_message", neighs.length);
    result.put("peers_not_known", notKnown);
    result.put("malicious_peers", st.getMaliciousNeighboursCount());
    result.put("validators_discovered", st.getValidatorsNeighboursCount());
    peerDiscoveries.put(m.id, result);
  }

  public static void prepareForExperiment(int experimentIndex, long seed) {
    currentExperimentIndex = experimentIndex;
    currentExperimentSeed = seed;
    totalExperiments = Configuration.getInt(peersim.Simulator.PAR_EXPS, 1);
    experimentLogFolderName = buildExperimentLogFolder();
    resetObserverState();
    GossipTopologyTracer.clear();
    GossipSubProtocol.resetExperimentState();
    GossipHeartBeat.resetGlobalHeartbeatIndex();
    PassiveEgoMeshRegistry.prepareForExperiment(seed);
    PassiveCoalitionRegistry.prepareForExperiment(seed);
    PxFloodRegistry.prepareForExperiment(seed);
    Message.resetIdGenerator();
    Operation.resetIdGenerator();
  }

  private static HashMap<Long, Map<String, Object>> writeMessages() {
    HashMap<Long, Map<String, Object>> msgs = new HashMap<>();

    Long msgId = (long) 0;
    for (BigInteger id : msgsIn.keySet()) {
      // System.out.println("Writing messages log " + id);
      Map<String, Object> result = new HashMap<String, Object>();
      result.put("id", id);
      boolean builder = false;
      if (id.compareTo(builderId) == 0) {
        builder = true;
      }

      result.put("msgsIn", msgsIn.get(id));
      result.put("msgsOut", msgsOut.get(id));
      result.put("bytesIn", bytesIn.get(id));
      result.put("bytesOut", bytesOut.get(id));
      if (builder) result.put("nodeType", "builder");
      else result.put("nodeType", "validator");
      // else result.put("nodeType", "regular");
      msgs.put(msgId, result);
      msgId++;
    }
    for (BigInteger id : msgsOut.keySet()) {
      if (msgsIn.keySet().contains(id)) {
        continue;
      }
      Map<String, Object> result = new HashMap<String, Object>();
      result.put("id", id);
      boolean builder = false;
      if (id.compareTo(builderId) == 0) {
        builder = true;
      }
      result.put("msgsIn", msgsIn.get(id));
      result.put("msgsOut", msgsOut.get(id));
      result.put("bytesIn", bytesIn.get(id));
      result.put("bytesOut", bytesOut.get(id));
      if (builder) result.put("nodeType", "builder");
      else result.put("nodeType", "validator");
      // else result.put("nodeType", "regular");
      msgs.put(msgId, result);
      msgId++;
    }
    return msgs;
  }

  private static void resetObserverState() {
    messages.clear();
    operations.clear();
    peerDiscoveries.clear();
    msgsIn.clear();
    msgsOut.clear();
    bytesIn.clear();
    bytesOut.clear();
    hopStore.reset();
    timeStore.reset();
    msg_deliv.reset();
    find_op.reset();
    find_ok.reset();
  }

  private static String buildExperimentLogFolder() {
    if (baseLogFolderName == null) return "./logs";
    if (totalExperiments <= 1) return baseLogFolderName;
    return String.format(
        "%s/exp_%03d_seed_%d", baseLogFolderName, currentExperimentIndex, currentExperimentSeed);
  }

  private static String getCurrentLogFolderName() {
    if (experimentLogFolderName == null) {
      experimentLogFolderName = buildExperimentLogFolder();
    }
    return experimentLogFolderName;
  }

  private static void writeRunInfo() {
    int upNodes = 0;
    long opportunisticGraftEvaluations = 0;
    long opportunisticGraftTriggers = 0;
    long opportunisticGraftPeers = 0;
    long pxPruneMessagesSent = 0;
    long pxCandidatesAdvertised = 0;
    long pxFinalPotentialCandidateEntries = 0;
    long pxFinalPotentialAdvertisableEntries = 0;
    long pxFinalPotentialTopicsWithCandidates = 0;
    long pxFinalPotentialNodesWithCandidates = 0;
    long pxCandidatesSeen = 0;
    long pxCandidatesRejectedSenderThreshold = 0;
    long pxCandidatesRejectedAlreadyKnown = 0;
    long pxCandidatesRejectedAlreadyConnected = 0;
    long pxCandidatesRejectedInMesh = 0;
    long pxCandidatesRejectedPruneBackoff = 0;
    long pxCandidatesRejectedScore = 0;
    long pxCandidatesRejectedExpiredOrStale = 0;
    long pxCandidatesRejectedCapacity = 0;
    long pxCandidatesPassedSenderThreshold = 0;
    long pxCandidatesAdmittedToKnown = 0;
    long pxCandidatesAdmittedAndLaterConnected = 0;
    long pxCandidatesAccepted = 0;
    long pxCandidatesRejected = 0;
    long pxAcceptedFromHighScoreSenders = 0;
    long pxPeersSkippedRecentlyAdvertised = 0;
    long pxPeersSelectedAfterNoveltyFiltering = 0;
    long pxSendersSeen = 0;
    long pxSendersAboveAcceptThreshold = 0;
    long pxSendersBelowAcceptThreshold = 0;
    double pxSendersScoreSum = 0.0;
    long pxSendersScoreCount = 0;
    double pxSendersScoreMin = Double.POSITIVE_INFINITY;
    double pxSendersScoreMax = Double.NEGATIVE_INFINITY;
    long pxSendersScoreLtThresholdMinusOne = 0;
    long pxSendersScoreThresholdMinusOneToMinusHalf = 0;
    long pxSendersScoreThresholdMinusHalfToThreshold = 0;
    long pxSendersScoreGeThreshold = 0;
    long dOutMeshOutboundPeers = 0;
    long dOutGraftsAcceptedAtCapacity = 0;
    long dOutGraftsRejectedAtCapacity = 0;
    long dOutOutboundMeshPeerSum = 0;
    long dOutOutboundMeshPeerObservationCount = 0;
    long dOutOutboundMeshPeerMin = Long.MAX_VALUE;
    long dOutHeartbeatDeficitSum = 0;
    long dOutHeartbeatDeficitNonzeroCount = 0;
    long dOutRepairAttemptsForDeficit = 0;
    long dOutRepairSuccesses = 0;
    long dOutSteadyStateTopupAttempts = 0;
    long dOutSteadyStateTopupSuccesses = 0;
    long dOutSteadyStateReplacements = 0;
    long dOutSteadyStatePrunes = 0;
    long dOutSteadyStateGrafts = 0;
    long dOutHeartbeatObservationCount = 0;
    long dOutZeroOutboundObservationCount = 0;
    long dOutDeficitObservationCount = 0;
    long dOutPostWarmupObservationCount = 0;
    long dOutPostWarmupZeroOutboundCount = 0;
    long dOutPostWarmupDeficitCount = 0;
    long dOutPostWarmupOutboundMeshPeerMin = Long.MAX_VALUE;
    long dOutMaxConsecutiveDeficitHeartbeats = 0;
    long dOutMaxConsecutiveZeroOutboundHeartbeats = 0;
    long dOutEscalationAttempts = 0;
    long dOutEscalationSuccesses = 0;
    long dOutZeroOutboundEmergencyAttempts = 0;
    long dOutZeroOutboundEmergencySuccesses = 0;
    long dOutInplacePromotionAttempts = 0;
    long dOutInplacePromotionSuccesses = 0;
    long dOutInplaceZeroOutboundPromotions = 0;
    long dOutInplaceDeficitPromotions = 0;
    long dOutForcedReseedAttempts = 0;
    long dOutForcedReseedSuccesses = 0;
    long dOutForcedReseedGrafts = 0;
    long dOutForcedReseedPrunes = 0;
    long dOutForcedReseedSkippedNoCandidate = 0;
    long dOutForcedReseedSkippedNoPrunableInbound = 0;
    long dOutForcedReseedEligibleCount = 0;
    long dOutForcedReseedBlockedNotPostWarmup = 0;
    long dOutForcedReseedBlockedConsecutiveZeroBelowThreshold = 0;
    long dOutForcedReseedBlockedOutboundBeforePositive = 0;
    long dOutForcedReseedBlockedDeficitBeforeZero = 0;
    long dOutForcedReseedBlockedAfterInplaceNoRemainingDeficit = 0;
    long dOutForcedReseedBlockedAfterInplaceNotPersistent = 0;
    long dOutForcedReseedReachedCallsiteCount = 0;
    long dOutForcedReseedZeroOutboundEmergencyCount = 0;
    long dOutForcedReseedInplaceSuccessDuringZeroOutboundCount = 0;
    long dOutForcedReseedAfterInplaceRemainingDeficitCount = 0;
    long dOutForcedReseedReturnedBeforeCallsiteCount = 0;
    long dOutForcedReseedEligibleByDeficit = 0;
    long dOutForcedReseedReachedCallsiteAfterInplace = 0;
    long dOutForcedReseedEligibleByZeroRemainingDeficit = 0;
    long dOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit = 0;
    long dOutMaxConsecutiveZeroRemainingDeficitHeartbeats = 0;
    long candidatePoolEntries = 0;
    long candidatePoolActiveSum = 0;
    long candidatePoolKnownActiveSum = 0;
    long candidatePoolPxActiveSum = 0;
    long candidatePoolMeshRepairActiveSum = 0;
    long candidatePoolRegistryActiveSum = 0;
    long candidatePoolObservationCount = 0;
    long candidatePoolExpiredRemoved = 0;
    long candidatePoolPxExpiredRemoved = 0;
    long candidatePoolCappedRemoved = 0;
    long candidatePoolBackoffFiltered = 0;
    long candidatePoolScoreFiltered = 0;
    long candidatePoolEmptySelectionCount = 0;
    long discoveryFinalKnownViaBootstrapEntries = 0;
    long discoveryFinalKnownViaAmbientEntries = 0;
    long discoveryFinalKnownViaPxEntries = 0;
    long discoveryFinalKnownViaObservationEntries = 0;
    long discoveryFinalConnectedEntries = 0;
    long discoveryFinalMeshEntries = 0;
    long repairFilterSelectionCalls = 0;
    long repairFilterCandidatePoolRawTotal = 0;
    long repairFilterCandidatePoolEmptyCount = 0;
    long repairFilterRejectedByBackoff = 0;
    long repairFilterRejectedByScore = 0;
    long repairFilterRejectedByAlreadyInMesh = 0;
    long repairFilterRejectedByExcluded = 0;
    long repairFilterNoCandidateAfterFiltering = 0;
    long pruneBackoffSetFromReceivedPrune = 0;
    long pruneBackoffSetFromOversubscriptionPrune = 0;
    long pruneBackoffSetFromLowScorePrune = 0;
    long pruneBackoffSetFromGraftRejection = 0;
    long repairRejectedByBackoffFromReceivedPrune = 0;
    long repairRejectedByBackoffFromOversubscriptionPrune = 0;
    long repairRejectedByBackoffFromLowScorePrune = 0;
    long repairRejectedByBackoffFromGraftRejection = 0;
    long repairCandidatesBlockedByBackoffPositiveScore = 0;
    long repairCandidatesBlockedByBackoffNegativeScore = 0;
    long repairRelaxedReceivedPruneConsidered = 0;
    long repairRelaxedReceivedPruneSelected = 0;
    long repairRelaxedReceivedPruneSuccesses = 0;
    long repairRelaxedReceivedPrunePositiveScoreSelected = 0;
    long repairRelaxedReceivedPruneNegativeScoreSelected = 0;
    long repairRelaxedReceivedPruneBlockedByBudget = 0;
    long repairRelaxedReceivedPruneBlockedNotPostWarmup = 0;
    long repairRelaxedReceivedPruneBlockedNoDOutDeficit = 0;
    long repairRelaxedReceivedPruneGrafted = 0;
    long repairRelaxedReceivedPruneGraftAccepted = 0;
    long repairRelaxedReceivedPruneGraftRejected = 0;
    long repairRelaxedReceivedPrunePruned = 0;
    long repairRelaxedReceivedPruneBackoffReset = 0;
    long repairRelaxedReceivedPruneSurvived1Heartbeat = 0;
    long repairRelaxedReceivedPruneSurvived5Heartbeats = 0;
    long repairRelaxedReceivedPruneSurvived10Heartbeats = 0;
    long repairRelaxedReceivedPruneLocalGraceActive = 0;
    long repairRelaxedReceivedPruneLocalPrunePreventedLowScore = 0;
    long repairRelaxedReceivedPruneLocalPrunePreventedOversubscription = 0;
    long repairRelaxedReceivedPruneRemovedByReceivedPrune = 0;
    long repairRelaxedReceivedPruneRemovedByLocalLowScore = 0;
    long repairRelaxedReceivedPruneRemovedByLocalOversubscription = 0;
    long repairRelaxedReceivedPruneRemovedByOther = 0;
    long repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity = 0;
    long repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore = 0;
    long repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff = 0;
    long repairRelaxedReceivedPruneReceivedPruneReasonLowScore = 0;
    long repairRelaxedReceivedPruneReceivedPruneReasonOversubscription = 0;
    long repairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe = 0;
    long repairRelaxedReceivedPruneReceivedPruneReasonOther = 0;
    long repairRelaxedReceivedPruneReceivedPruneAfterGraft = 0;
    long repairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat = 0;
    long repairRelaxedReceivedPruneSelectedPruneAgeSum = 0;
    long repairRelaxedReceivedPruneSelectedPruneAgeCount = 0;
    long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum = 0;
    long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount = 0;
    long repairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff = 0;
    long repairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff = 0;
    long repairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff = 0;
    long repairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff = 0;
    long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff = 0;
    long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff = 0;
    long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff = 0;
    long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff = 0;
    long repairRelaxReceivedPrunePrefilterCandidatesSeen = 0;
    long repairRelaxReceivedPrunePrefilterAgeKnown = 0;
    long repairRelaxReceivedPrunePrefilterAgeUnknown = 0;
    long repairRelaxReceivedPrunePrefilterPassScore = 0;
    long repairRelaxReceivedPrunePrefilterBlockedByAge = 0;
    long repairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff = 0;
    long repairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff = 0;
    long repairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff = 0;
    long repairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff = 0;
    long receiverRelaxGraftBackoffConsidered = 0;
    long receiverRelaxGraftBackoffSelected = 0;
    long receiverRelaxGraftBackoffAccepted = 0;
    long receiverRelaxGraftBackoffBlockedByBudget = 0;
    long receiverRelaxGraftBackoffBlockedByScore = 0;
    long receiverRelaxGraftBackoffBlockedByAge = 0;
    long receiverRelaxGraftBackoffBlockedNotDOut = 0;
    long receiverRelaxGraftBackoffBlockedOtherReason = 0;
    long receiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff = 0;
    long receiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff = 0;
    long receiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff = 0;
    long receiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff = 0;
    long receiverRelaxGraftBackoffPrefilterCandidatesEncountered = 0;
    long receiverRelaxGraftBackoffPrefilterAgeKnown = 0;
    long receiverRelaxGraftBackoffPrefilterAgeUnknown = 0;
    long receiverRelaxGraftBackoffPrefilterPassScore = 0;
    long receiverRelaxGraftBackoffPrefilterBlockedByAge = 0;
    long receiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff = 0;
    long receiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff = 0;
    long receiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff = 0;
    long receiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff = 0;
    double receiverRelaxGraftBackoffScoreSum = 0.0;
    long receiverRelaxGraftBackoffScoreCount = 0;
    double receiverRelaxGraftBackoffScoreMin = Double.POSITIVE_INFINITY;
    double receiverRelaxGraftBackoffScoreMax = Double.NEGATIVE_INFINITY;
    long receiverRelaxGraftBackoffScoreLtThresholdMinusOne = 0;
    long receiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf = 0;
    long receiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold = 0;
    long receiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf = 0;
    long receiverRelaxGraftBackoffScoreGeThresholdPlusHalf = 0;
    long receiverGraftScoreRejectionDecompositionCount = 0;
    double receiverGraftScoreRejectionDecompositionTotalScoreSum = 0.0;
    double receiverGraftScoreRejectionDecompositionMeshThresholdSum = 0.0;
    double receiverGraftScoreRejectionDecompositionScoreMinusMeshThresholdSum = 0.0;
    double receiverGraftScoreRejectionDecompositionTimeInMeshContributionSum = 0.0;
    double receiverGraftScoreRejectionDecompositionFirstDeliveriesContributionSum = 0.0;
    double receiverGraftScoreRejectionDecompositionLocalInvalidDeliveriesPenaltySum = 0.0;
    double receiverGraftScoreRejectionDecompositionLocalBrokenPromisesPenaltySum = 0.0;
    double receiverGraftScoreRejectionDecompositionGlobalInvalidDeliveriesPenaltySum = 0.0;
    double receiverGraftScoreRejectionDecompositionGlobalBrokenPromisesPenaltySum = 0.0;
    long remotePruneOutcomeDecompositionCount = 0;
    double remotePruneOutcomeDecompositionTotalScoreSum = 0.0;
    double remotePruneOutcomeDecompositionMeshThresholdSum = 0.0;
    double remotePruneOutcomeDecompositionScoreMinusMeshThresholdSum = 0.0;
    double remotePruneOutcomeDecompositionTimeInMeshContributionSum = 0.0;
    double remotePruneOutcomeDecompositionFirstDeliveriesContributionSum = 0.0;
    double remotePruneOutcomeDecompositionLocalInvalidDeliveriesPenaltySum = 0.0;
    double remotePruneOutcomeDecompositionLocalBrokenPromisesPenaltySum = 0.0;
    double remotePruneOutcomeDecompositionGlobalInvalidDeliveriesPenaltySum = 0.0;
    double remotePruneOutcomeDecompositionGlobalBrokenPromisesPenaltySum = 0.0;
    long localTopologyObservationCount = 0;
    long localKnownPeerSum = 0;
    long localConnectablePeerSum = 0;
    long localConnectedPeerSum = 0;
    long localConnectedNonMeshPeerSum = 0;
    long localReserveConnectedPeerSum = 0;
    long localReserveConnectedImmatureSum = 0;
    long localReserveConnectedMatureSum = 0;
    double localMeshFractionOfConnectedSum = 0.0;
    long localConnectedHoldActiveSum = 0;
    long localConnectedHoldForReserveSum = 0;
    long localKnownOnlyPeerSum = 0;
    long localKnownNonConnectablePeerSum = 0;
    long localConnectableButDisconnectedPeerSum = 0;
    long localKnownFromPxSum = 0;
    long localKnownFromDirectSum = 0;
    long localPxKnownFreshSum = 0;
    long localPxKnownStaleSum = 0;
    long localMeshPeerSum = 0;
    long localEligibleCandidateSum = 0;
    long localEligibleConnectableCandidateSum = 0;
    long localEligibleConnectedCandidateSum = 0;
    long localEligibleKnownDisconnectedCandidateSum = 0;
    long localConnectableFromDirectSum = 0;
    long localConnectableFromMeshSum = 0;
    long localConnectableFromConnectedSum = 0;
    long localConnectableFromRegistrySum = 0;
    long localConnectableFromPxSum = 0;
    long localKnownNonConnectableFromPxSum = 0;
    long localKnownNonConnectableFromRegistrySum = 0;
    long localConnectedDirectFreshSum = 0;
    long localConnectedPxFreshSum = 0;
    long localConnectedMeshFreshSum = 0;
    long localConnectedFanoutFreshSum = 0;
    long localConnectedReserveFreshSum = 0;
    long localConnectedReserveRegistryOnlySum = 0;
    long localConnectedTargetSum = 0;
    long localConnectedSlackTargetSum = 0;
    long localConnectedReserveGapSum = 0;
    long localReserveGapSum = 0;
    long localConnectedNonMeshTargetGapSum = 0;
    long localConnectedNonMeshRetainedSum = 0;
    long localKnownRetainedAfterDisconnectSum = 0;
    long localConnectedPxOnlySum = 0;
    long localKnownPxOnlySum = 0;
    long localPxRetainedKnownOnlySum = 0;
    long localOutboundConnectedPeerSum = 0;
    long localInboundConnectedPeerSum = 0;
    long localDisconnectedKnownPeerSum = 0;
    long localRegistryBootstrapAdded = 0;
    long localCatalogExpiredRegistryRemoved = 0;
    long localCatalogExpiredKnownRemoved = 0;
    long localCatalogExpiredMeshRepairRemoved = 0;
    long localCatalogExpiredConnectedRemoved = 0;
    long localConnectAttempts = 0;
    long localConnectSuccesses = 0;
    long localConnectFailures = 0;
    long localConnectAttemptsFromDirect = 0;
    long localConnectAttemptsFromPx = 0;
    long localConnectAttemptsFromMeshRepair = 0;
    long localConnectAttemptsForReserve = 0;
    long localConnectSuccessesFromDirect = 0;
    long localConnectSuccessesFromPx = 0;
    long localConnectSuccessesFromMeshRepair = 0;
    long localConnectSuccessesForReserve = 0;
    long localConnectFailuresForReserve = 0;
    long localReserveConnectedPromotedToMesh = 0;
    long localReserveConnectedConsumedByMesh = 0;
    long localReserveConnectedConsumedBeforeMaturity = 0;
    long localReserveConnectedDisconnectedBeforePromotion = 0;
    long localReserveConnectedDisconnectedBeforeMaturity = 0;
    long localReserveConnectedDisconnectedAfterMaturity = 0;
    long localMeshSelectionSkippedReservedPeer = 0;
    long localPxPromotedToConnected = 0;
    long localPxAdmissionAttempts = 0;
    long localPxAdmissionSuccesses = 0;
    long localPxAdmissionRejectionsTotal = 0;
    long localConnectRetryBackoffActiveSum = 0;
    long localConnectionTransitionOutbound = 0;
    long localConnectionTransitionInbound = 0;
    long localConnectionTransitionDisconnected = 0;
    long localConnectionDisconnectedByReceivedPrune = 0;
    long localConnectionDisconnectedByLocalPrune = 0;
    long localConnectionDisconnectedByScore = 0;
    long localConnectionDisconnectedByCapacity = 0;
    long localConnectionDisconnectedByIdle = 0;
    long localConnectionDisconnectedByExpiry = 0;
    long localConnectionDisconnectedByOther = 0;
    long localConnectionPreservedAfterReceivedPrune = 0;
    long localConnectionPreservedAfterLocalOversubscriptionPrune = 0;
    long localConnectionPreservedAfterLocalLowScorePrune = 0;
    long localConnectedNonMeshSurvivedAfterPrune = 0;
    long localMeshRemovedButStillConnectedCount = 0;
    long localConnectedStaleDropCount = 0;
    long localConnectedNonMeshUsedForGossip = 0;
    long localConnectedNonMeshUsedForFanout = 0;
    long localConnectedNonMeshPromotedToMesh = 0;
    long localConnectedNonMeshDisconnectedByCapacity = 0;
    long localConnectedNonMeshDisconnectedByScore = 0;
    long localConnectedNonMeshDisconnectedByExpiry = 0;
    long localConnectionLifetimeSum = 0;
    long localConnectionLifetimeCount = 0;
    long localConnectedNonMeshLifetimeSum = 0;
    long localConnectedNonMeshLifetimeCount = 0;
    long localReserveConnectedLifetimeSum = 0;
    long localReserveConnectedLifetimeCount = 0;
    long localReserveConnectedLifetimeBeforePromotionSum = 0;
    long localReserveConnectedLifetimeBeforePromotionCount = 0;
    long localConnectionReconnectSamePeerWithin5Heartbeats = 0;
    long localConnectionReconnectSamePeerWithin10Heartbeats = 0;
    long localConnectionIdleDisconnectsBeforeMinLifetime = 0;
    long localConnectionIdleDisconnectsAfterMinLifetime = 0;
    long localConnectionDisconnectionBlockedByHold = 0;
    long localConnectionHoldExpiredDisconnects = 0;
    long localConnectionConsecutiveIdleThresholdHits = 0;
    long localConnectionRetryDelaySum = 0;
    long localConnectionRetryDelayCount = 0;
    long localConnectionRetryDelayMax = 0;
    long localConnectionFlappingPeerCount = 0;
    long pxSenderTrustDecompositionCount = 0;
    double pxSenderTrustDecompositionTotalScoreSum = 0.0;
    double pxSenderTrustDecompositionThresholdSum = 0.0;
    double pxSenderTrustDecompositionScoreMinusThresholdSum = 0.0;
    double pxSenderTrustDecompositionTimeInMeshContributionSum = 0.0;
    double pxSenderTrustDecompositionFirstDeliveriesContributionSum = 0.0;
    double pxSenderTrustDecompositionLocalInvalidDeliveriesPenaltySum = 0.0;
    double pxSenderTrustDecompositionLocalBrokenPromisesPenaltySum = 0.0;
    double pxSenderTrustDecompositionGlobalInvalidDeliveriesPenaltySum = 0.0;
    double pxSenderTrustDecompositionGlobalBrokenPromisesPenaltySum = 0.0;
    for (int i = 0; i < Network.size(); i++) {
      if (Network.get(i).isUp()) upNodes++;
      Object protocol = Network.get(i).getProtocol(kademliaid);
      if (protocol instanceof GossipSubProtocol) {
        GossipSubProtocol gossipProtocol = (GossipSubProtocol) protocol;
        opportunisticGraftEvaluations += gossipProtocol.getOpportunisticGraftEvaluationCount();
        opportunisticGraftTriggers += gossipProtocol.getOpportunisticGraftTriggerCount();
        opportunisticGraftPeers += gossipProtocol.getOpportunisticGraftPeerCount();
        pxPruneMessagesSent += gossipProtocol.getPxPruneMessagesSent();
        pxCandidatesAdvertised += gossipProtocol.getPxCandidatesAdvertised();
        pxCandidatesSeen += gossipProtocol.getPxCandidatesSeen();
        pxCandidatesRejectedSenderThreshold +=
            gossipProtocol.getPxCandidatesRejectedSenderThreshold();
        pxCandidatesRejectedAlreadyKnown += gossipProtocol.getPxCandidatesRejectedAlreadyKnown();
        pxCandidatesRejectedAlreadyConnected +=
            gossipProtocol.getPxCandidatesRejectedAlreadyConnected();
        pxCandidatesRejectedInMesh += gossipProtocol.getPxCandidatesRejectedInMesh();
        pxCandidatesRejectedPruneBackoff += gossipProtocol.getPxCandidatesRejectedPruneBackoff();
        pxCandidatesRejectedScore += gossipProtocol.getPxCandidatesRejectedScore();
        pxCandidatesRejectedExpiredOrStale +=
            gossipProtocol.getPxCandidatesRejectedExpiredOrStale();
        pxCandidatesRejectedCapacity += gossipProtocol.getPxCandidatesRejectedCapacity();
        pxCandidatesPassedSenderThreshold += gossipProtocol.getPxCandidatesPassedSenderThreshold();
        pxCandidatesAdmittedToKnown += gossipProtocol.getPxCandidatesAdmittedToKnown();
        pxCandidatesAdmittedAndLaterConnected +=
            gossipProtocol.getPxCandidatesAdmittedAndLaterConnected();
        pxCandidatesAccepted += gossipProtocol.getPxCandidatesAccepted();
        pxCandidatesRejected += gossipProtocol.getPxCandidatesRejected();
        pxAcceptedFromHighScoreSenders += gossipProtocol.getPxAcceptedFromHighScoreSenders();
        pxPeersSkippedRecentlyAdvertised += gossipProtocol.getPxPeersSkippedRecentlyAdvertised();
        pxPeersSelectedAfterNoveltyFiltering +=
            gossipProtocol.getPxPeersSelectedAfterNoveltyFiltering();
        pxSendersSeen += gossipProtocol.getPxSendersSeen();
        pxSendersAboveAcceptThreshold += gossipProtocol.getPxSendersAboveAcceptThreshold();
        pxSendersBelowAcceptThreshold += gossipProtocol.getPxSendersBelowAcceptThreshold();
        pxSendersScoreSum += gossipProtocol.getPxSendersScoreSum();
        pxSendersScoreCount += gossipProtocol.getPxSendersScoreCount();
        if (gossipProtocol.getPxSendersScoreCount() > 0) {
          pxSendersScoreMin = Math.min(pxSendersScoreMin, gossipProtocol.getPxSendersScoreMin());
          pxSendersScoreMax = Math.max(pxSendersScoreMax, gossipProtocol.getPxSendersScoreMax());
        }
        pxSendersScoreLtThresholdMinusOne += gossipProtocol.getPxSendersScoreLtThresholdMinusOne();
        pxSendersScoreThresholdMinusOneToMinusHalf +=
            gossipProtocol.getPxSendersScoreThresholdMinusOneToMinusHalf();
        pxSendersScoreThresholdMinusHalfToThreshold +=
            gossipProtocol.getPxSendersScoreThresholdMinusHalfToThreshold();
        pxSendersScoreGeThreshold += gossipProtocol.getPxSendersScoreGeThreshold();
        dOutMeshOutboundPeers += gossipProtocol.getCurrentOutboundMeshPeerCount();
        dOutGraftsAcceptedAtCapacity += gossipProtocol.getDOutGraftsAcceptedAtCapacity();
        dOutGraftsRejectedAtCapacity += gossipProtocol.getDOutGraftsRejectedAtCapacity();
        dOutOutboundMeshPeerSum += gossipProtocol.getObservedOutboundMeshPeerSum();
        dOutOutboundMeshPeerObservationCount += gossipProtocol.getObservedOutboundMeshPeerCount();
        dOutHeartbeatDeficitSum += gossipProtocol.getDOutHeartbeatDeficitSum();
        dOutHeartbeatDeficitNonzeroCount += gossipProtocol.getDOutHeartbeatDeficitNonzeroCount();
        dOutRepairAttemptsForDeficit += gossipProtocol.getDOutRepairAttemptsForDeficit();
        dOutRepairSuccesses += gossipProtocol.getDOutRepairSuccesses();
        dOutSteadyStateTopupAttempts += gossipProtocol.getDOutSteadyStateTopupAttempts();
        dOutSteadyStateTopupSuccesses += gossipProtocol.getDOutSteadyStateTopupSuccesses();
        dOutSteadyStateReplacements += gossipProtocol.getDOutSteadyStateReplacements();
        dOutSteadyStatePrunes += gossipProtocol.getDOutSteadyStatePrunes();
        dOutSteadyStateGrafts += gossipProtocol.getDOutSteadyStateGrafts();
        dOutHeartbeatObservationCount += gossipProtocol.getDOutHeartbeatObservationCount();
        dOutZeroOutboundObservationCount += gossipProtocol.getDOutZeroOutboundObservationCount();
        dOutDeficitObservationCount += gossipProtocol.getDOutDeficitObservationCount();
        dOutPostWarmupObservationCount += gossipProtocol.getDOutPostWarmupObservationCount();
        dOutPostWarmupZeroOutboundCount += gossipProtocol.getDOutPostWarmupZeroOutboundCount();
        dOutPostWarmupDeficitCount += gossipProtocol.getDOutPostWarmupDeficitCount();
        dOutMaxConsecutiveDeficitHeartbeats =
            Math.max(
                dOutMaxConsecutiveDeficitHeartbeats,
                gossipProtocol.getDOutMaxConsecutiveDeficitHeartbeats());
        dOutMaxConsecutiveZeroOutboundHeartbeats =
            Math.max(
                dOutMaxConsecutiveZeroOutboundHeartbeats,
                gossipProtocol.getDOutMaxConsecutiveZeroOutboundHeartbeats());
        dOutEscalationAttempts += gossipProtocol.getDOutEscalationAttempts();
        dOutEscalationSuccesses += gossipProtocol.getDOutEscalationSuccesses();
        dOutZeroOutboundEmergencyAttempts += gossipProtocol.getDOutZeroOutboundEmergencyAttempts();
        dOutZeroOutboundEmergencySuccesses +=
            gossipProtocol.getDOutZeroOutboundEmergencySuccesses();
        dOutInplacePromotionAttempts += gossipProtocol.getDOutInplacePromotionAttempts();
        dOutInplacePromotionSuccesses += gossipProtocol.getDOutInplacePromotionSuccesses();
        dOutInplaceZeroOutboundPromotions += gossipProtocol.getDOutInplaceZeroOutboundPromotions();
        dOutInplaceDeficitPromotions += gossipProtocol.getDOutInplaceDeficitPromotions();
        dOutForcedReseedAttempts += gossipProtocol.getDOutForcedReseedAttempts();
        dOutForcedReseedSuccesses += gossipProtocol.getDOutForcedReseedSuccesses();
        dOutForcedReseedGrafts += gossipProtocol.getDOutForcedReseedGrafts();
        dOutForcedReseedPrunes += gossipProtocol.getDOutForcedReseedPrunes();
        dOutForcedReseedSkippedNoCandidate +=
            gossipProtocol.getDOutForcedReseedSkippedNoCandidate();
        dOutForcedReseedSkippedNoPrunableInbound +=
            gossipProtocol.getDOutForcedReseedSkippedNoPrunableInbound();
        dOutForcedReseedEligibleCount += gossipProtocol.getDOutForcedReseedEligibleCount();
        dOutForcedReseedBlockedNotPostWarmup +=
            gossipProtocol.getDOutForcedReseedBlockedNotPostWarmup();
        dOutForcedReseedBlockedConsecutiveZeroBelowThreshold +=
            gossipProtocol.getDOutForcedReseedBlockedConsecutiveZeroBelowThreshold();
        dOutForcedReseedBlockedOutboundBeforePositive +=
            gossipProtocol.getDOutForcedReseedBlockedOutboundBeforePositive();
        dOutForcedReseedBlockedDeficitBeforeZero +=
            gossipProtocol.getDOutForcedReseedBlockedDeficitBeforeZero();
        dOutForcedReseedBlockedAfterInplaceNoRemainingDeficit +=
            gossipProtocol.getDOutForcedReseedBlockedAfterInplaceNoRemainingDeficit();
        dOutForcedReseedBlockedAfterInplaceNotPersistent +=
            gossipProtocol.getDOutForcedReseedBlockedAfterInplaceNotPersistent();
        dOutForcedReseedReachedCallsiteCount +=
            gossipProtocol.getDOutForcedReseedReachedCallsiteCount();
        dOutForcedReseedZeroOutboundEmergencyCount +=
            gossipProtocol.getDOutForcedReseedZeroOutboundEmergencyCount();
        dOutForcedReseedInplaceSuccessDuringZeroOutboundCount +=
            gossipProtocol.getDOutForcedReseedInplaceSuccessDuringZeroOutboundCount();
        dOutForcedReseedAfterInplaceRemainingDeficitCount +=
            gossipProtocol.getDOutForcedReseedAfterInplaceRemainingDeficitCount();
        dOutForcedReseedReturnedBeforeCallsiteCount +=
            gossipProtocol.getDOutForcedReseedReturnedBeforeCallsiteCount();
        dOutForcedReseedEligibleByDeficit += gossipProtocol.getDOutForcedReseedEligibleByDeficit();
        dOutForcedReseedReachedCallsiteAfterInplace +=
            gossipProtocol.getDOutForcedReseedReachedCallsiteAfterInplace();
        dOutForcedReseedEligibleByZeroRemainingDeficit +=
            gossipProtocol.getDOutForcedReseedEligibleByZeroRemainingDeficit();
        dOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit +=
            gossipProtocol.getDOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit();
        dOutMaxConsecutiveZeroRemainingDeficitHeartbeats =
            Math.max(
                dOutMaxConsecutiveZeroRemainingDeficitHeartbeats,
                gossipProtocol.getDOutMaxConsecutiveZeroRemainingDeficitHeartbeats());
        candidatePoolEntries += gossipProtocol.getCurrentCandidatePoolEntryCount();
        candidatePoolActiveSum += gossipProtocol.getCandidatePoolActiveSum();
        candidatePoolKnownActiveSum += gossipProtocol.getCandidatePoolKnownActiveSum();
        candidatePoolPxActiveSum += gossipProtocol.getCandidatePoolPxActiveSum();
        candidatePoolMeshRepairActiveSum += gossipProtocol.getCandidatePoolMeshRepairActiveSum();
        candidatePoolRegistryActiveSum += gossipProtocol.getCandidatePoolRegistryActiveSum();
        candidatePoolObservationCount += gossipProtocol.getCandidatePoolObservationCount();
        candidatePoolExpiredRemoved += gossipProtocol.getCandidatePoolExpiredRemoved();
        candidatePoolPxExpiredRemoved += gossipProtocol.getCandidatePoolPxExpiredRemoved();
        candidatePoolCappedRemoved += gossipProtocol.getCandidatePoolCappedRemoved();
        candidatePoolBackoffFiltered += gossipProtocol.getCandidatePoolBackoffFiltered();
        candidatePoolScoreFiltered += gossipProtocol.getCandidatePoolScoreFiltered();
        candidatePoolEmptySelectionCount += gossipProtocol.getCandidatePoolEmptySelectionCount();
        long currentPotentialPxCandidateEntries =
            gossipProtocol.getCurrentPotentialPrunePxCandidateEntryCount();
        pxFinalPotentialCandidateEntries += currentPotentialPxCandidateEntries;
        pxFinalPotentialAdvertisableEntries +=
            gossipProtocol.getCurrentPotentialPrunePxAdvertisableEntryCount();
        pxFinalPotentialTopicsWithCandidates +=
            gossipProtocol.getCurrentPotentialPrunePxTopicCount();
        if (currentPotentialPxCandidateEntries > 0) {
          pxFinalPotentialNodesWithCandidates++;
        }
        discoveryFinalKnownViaBootstrapEntries +=
            gossipProtocol.getCurrentBootstrapKnownEntryCount();
        discoveryFinalKnownViaAmbientEntries += gossipProtocol.getCurrentAmbientKnownEntryCount();
        discoveryFinalKnownViaPxEntries += gossipProtocol.getCurrentPxKnownEntryCount();
        discoveryFinalKnownViaObservationEntries +=
            gossipProtocol.getCurrentObservedKnownEntryCount();
        discoveryFinalConnectedEntries += gossipProtocol.getCurrentConnectedPeerEntryCount();
        discoveryFinalMeshEntries += gossipProtocol.getCurrentMeshPeerEntryCount();
        repairFilterSelectionCalls += gossipProtocol.getRepairFilterSelectionCalls();
        repairFilterCandidatePoolRawTotal += gossipProtocol.getRepairFilterCandidatePoolRawTotal();
        repairFilterCandidatePoolEmptyCount +=
            gossipProtocol.getRepairFilterCandidatePoolEmptyCount();
        repairFilterRejectedByBackoff += gossipProtocol.getRepairFilterRejectedByBackoff();
        repairFilterRejectedByScore += gossipProtocol.getRepairFilterRejectedByScore();
        repairFilterRejectedByAlreadyInMesh +=
            gossipProtocol.getRepairFilterRejectedByAlreadyInMesh();
        repairFilterRejectedByExcluded += gossipProtocol.getRepairFilterRejectedByExcluded();
        repairFilterNoCandidateAfterFiltering +=
            gossipProtocol.getRepairFilterNoCandidateAfterFiltering();
        pruneBackoffSetFromReceivedPrune += gossipProtocol.getPruneBackoffSetFromReceivedPrune();
        pruneBackoffSetFromOversubscriptionPrune +=
            gossipProtocol.getPruneBackoffSetFromOversubscriptionPrune();
        pruneBackoffSetFromLowScorePrune += gossipProtocol.getPruneBackoffSetFromLowScorePrune();
        pruneBackoffSetFromGraftRejection += gossipProtocol.getPruneBackoffSetFromGraftRejection();
        repairRejectedByBackoffFromReceivedPrune +=
            gossipProtocol.getRepairRejectedByBackoffFromReceivedPrune();
        repairRejectedByBackoffFromOversubscriptionPrune +=
            gossipProtocol.getRepairRejectedByBackoffFromOversubscriptionPrune();
        repairRejectedByBackoffFromLowScorePrune +=
            gossipProtocol.getRepairRejectedByBackoffFromLowScorePrune();
        repairRejectedByBackoffFromGraftRejection +=
            gossipProtocol.getRepairRejectedByBackoffFromGraftRejection();
        repairCandidatesBlockedByBackoffPositiveScore +=
            gossipProtocol.getRepairCandidatesBlockedByBackoffPositiveScore();
        repairCandidatesBlockedByBackoffNegativeScore +=
            gossipProtocol.getRepairCandidatesBlockedByBackoffNegativeScore();
        repairRelaxedReceivedPruneConsidered +=
            gossipProtocol.getRepairRelaxedReceivedPruneConsidered();
        repairRelaxedReceivedPruneSelected +=
            gossipProtocol.getRepairRelaxedReceivedPruneSelected();
        repairRelaxedReceivedPruneSuccesses +=
            gossipProtocol.getRepairRelaxedReceivedPruneSuccesses();
        repairRelaxedReceivedPrunePositiveScoreSelected +=
            gossipProtocol.getRepairRelaxedReceivedPrunePositiveScoreSelected();
        repairRelaxedReceivedPruneNegativeScoreSelected +=
            gossipProtocol.getRepairRelaxedReceivedPruneNegativeScoreSelected();
        repairRelaxedReceivedPruneBlockedByBudget +=
            gossipProtocol.getRepairRelaxedReceivedPruneBlockedByBudget();
        repairRelaxedReceivedPruneBlockedNotPostWarmup +=
            gossipProtocol.getRepairRelaxedReceivedPruneBlockedNotPostWarmup();
        repairRelaxedReceivedPruneBlockedNoDOutDeficit +=
            gossipProtocol.getRepairRelaxedReceivedPruneBlockedNoDOutDeficit();
        repairRelaxedReceivedPruneGrafted += gossipProtocol.getRepairRelaxedReceivedPruneGrafted();
        repairRelaxedReceivedPruneGraftAccepted +=
            gossipProtocol.getRepairRelaxedReceivedPruneGraftAccepted();
        repairRelaxedReceivedPruneGraftRejected +=
            gossipProtocol.getRepairRelaxedReceivedPruneGraftRejected();
        repairRelaxedReceivedPrunePruned += gossipProtocol.getRepairRelaxedReceivedPrunePruned();
        repairRelaxedReceivedPruneBackoffReset +=
            gossipProtocol.getRepairRelaxedReceivedPruneBackoffReset();
        repairRelaxedReceivedPruneSurvived1Heartbeat +=
            gossipProtocol.getRepairRelaxedReceivedPruneSurvived1Heartbeat();
        repairRelaxedReceivedPruneSurvived5Heartbeats +=
            gossipProtocol.getRepairRelaxedReceivedPruneSurvived5Heartbeats();
        repairRelaxedReceivedPruneSurvived10Heartbeats +=
            gossipProtocol.getRepairRelaxedReceivedPruneSurvived10Heartbeats();
        repairRelaxedReceivedPruneLocalGraceActive +=
            gossipProtocol.getRepairRelaxedReceivedPruneLocalGraceActive();
        repairRelaxedReceivedPruneLocalPrunePreventedLowScore +=
            gossipProtocol.getRepairRelaxedReceivedPruneLocalPrunePreventedLowScore();
        repairRelaxedReceivedPruneLocalPrunePreventedOversubscription +=
            gossipProtocol.getRepairRelaxedReceivedPruneLocalPrunePreventedOversubscription();
        repairRelaxedReceivedPruneRemovedByReceivedPrune +=
            gossipProtocol.getRepairRelaxedReceivedPruneRemovedByReceivedPrune();
        repairRelaxedReceivedPruneRemovedByLocalLowScore +=
            gossipProtocol.getRepairRelaxedReceivedPruneRemovedByLocalLowScore();
        repairRelaxedReceivedPruneRemovedByLocalOversubscription +=
            gossipProtocol.getRepairRelaxedReceivedPruneRemovedByLocalOversubscription();
        repairRelaxedReceivedPruneRemovedByOther +=
            gossipProtocol.getRepairRelaxedReceivedPruneRemovedByOther();
        repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity +=
            gossipProtocol
                .getRepairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity();
        repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore +=
            gossipProtocol.getRepairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore();
        repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff +=
            gossipProtocol.getRepairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff();
        repairRelaxedReceivedPruneReceivedPruneReasonLowScore +=
            gossipProtocol.getRepairRelaxedReceivedPruneReceivedPruneReasonLowScore();
        repairRelaxedReceivedPruneReceivedPruneReasonOversubscription +=
            gossipProtocol.getRepairRelaxedReceivedPruneReceivedPruneReasonOversubscription();
        repairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe +=
            gossipProtocol.getRepairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe();
        repairRelaxedReceivedPruneReceivedPruneReasonOther +=
            gossipProtocol.getRepairRelaxedReceivedPruneReceivedPruneReasonOther();
        repairRelaxedReceivedPruneReceivedPruneAfterGraft +=
            gossipProtocol.getRepairRelaxedReceivedPruneReceivedPruneAfterGraft();
        repairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat +=
            gossipProtocol.getRepairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat();
        repairRelaxedReceivedPruneSelectedPruneAgeSum +=
            gossipProtocol.getRepairRelaxedReceivedPruneSelectedPruneAgeSum();
        repairRelaxedReceivedPruneSelectedPruneAgeCount +=
            gossipProtocol.getRepairRelaxedReceivedPruneSelectedPruneAgeCount();
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum +=
            gossipProtocol.getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum();
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount +=
            gossipProtocol.getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount();
        repairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff +=
            gossipProtocol.getRepairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff();
        repairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff +=
            gossipProtocol.getRepairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff();
        repairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff +=
            gossipProtocol
                .getRepairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff();
        repairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff +=
            gossipProtocol.getRepairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff();
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff +=
            gossipProtocol
                .getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff();
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff +=
            gossipProtocol
                .getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff();
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff +=
            gossipProtocol
                .getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff();
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff +=
            gossipProtocol
                .getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff();
        repairRelaxReceivedPrunePrefilterCandidatesSeen +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterCandidatesSeen();
        repairRelaxReceivedPrunePrefilterAgeKnown +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterAgeKnown();
        repairRelaxReceivedPrunePrefilterAgeUnknown +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterAgeUnknown();
        repairRelaxReceivedPrunePrefilterPassScore +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterPassScore();
        repairRelaxReceivedPrunePrefilterBlockedByAge +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterBlockedByAge();
        repairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff();
        repairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff();
        repairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff();
        repairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff +=
            gossipProtocol.getRepairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff();
        receiverRelaxGraftBackoffConsidered +=
            gossipProtocol.getReceiverRelaxGraftBackoffConsidered();
        receiverRelaxGraftBackoffSelected += gossipProtocol.getReceiverRelaxGraftBackoffSelected();
        receiverRelaxGraftBackoffAccepted += gossipProtocol.getReceiverRelaxGraftBackoffAccepted();
        receiverRelaxGraftBackoffBlockedByBudget +=
            gossipProtocol.getReceiverRelaxGraftBackoffBlockedByBudget();
        receiverRelaxGraftBackoffBlockedByScore +=
            gossipProtocol.getReceiverRelaxGraftBackoffBlockedByScore();
        receiverRelaxGraftBackoffBlockedByAge +=
            gossipProtocol.getReceiverRelaxGraftBackoffBlockedByAge();
        receiverRelaxGraftBackoffBlockedNotDOut +=
            gossipProtocol.getReceiverRelaxGraftBackoffBlockedNotDOut();
        receiverRelaxGraftBackoffBlockedOtherReason +=
            gossipProtocol.getReceiverRelaxGraftBackoffBlockedOtherReason();
        receiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff +=
            gossipProtocol.getReceiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff();
        receiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff +=
            gossipProtocol.getReceiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff();
        receiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff +=
            gossipProtocol.getReceiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff();
        receiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff +=
            gossipProtocol.getReceiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff();
        receiverRelaxGraftBackoffPrefilterCandidatesEncountered +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterCandidatesEncountered();
        receiverRelaxGraftBackoffPrefilterAgeKnown +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterAgeKnown();
        receiverRelaxGraftBackoffPrefilterAgeUnknown +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterAgeUnknown();
        receiverRelaxGraftBackoffPrefilterPassScore +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterPassScore();
        receiverRelaxGraftBackoffPrefilterBlockedByAge +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterBlockedByAge();
        receiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff();
        receiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff();
        receiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff();
        receiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff +=
            gossipProtocol.getReceiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff();
        receiverRelaxGraftBackoffScoreSum += gossipProtocol.getReceiverRelaxGraftBackoffScoreSum();
        receiverRelaxGraftBackoffScoreCount +=
            gossipProtocol.getReceiverRelaxGraftBackoffScoreCount();
        if (gossipProtocol.getReceiverRelaxGraftBackoffScoreCount() > 0) {
          receiverRelaxGraftBackoffScoreMin =
              Math.min(
                  receiverRelaxGraftBackoffScoreMin,
                  gossipProtocol.getReceiverRelaxGraftBackoffScoreMin());
          receiverRelaxGraftBackoffScoreMax =
              Math.max(
                  receiverRelaxGraftBackoffScoreMax,
                  gossipProtocol.getReceiverRelaxGraftBackoffScoreMax());
        }
        receiverRelaxGraftBackoffScoreLtThresholdMinusOne +=
            gossipProtocol.getReceiverRelaxGraftBackoffScoreLtThresholdMinusOne();
        receiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf +=
            gossipProtocol.getReceiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf();
        receiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold +=
            gossipProtocol.getReceiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold();
        receiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf +=
            gossipProtocol.getReceiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf();
        receiverRelaxGraftBackoffScoreGeThresholdPlusHalf +=
            gossipProtocol.getReceiverRelaxGraftBackoffScoreGeThresholdPlusHalf();
        receiverGraftScoreRejectionDecompositionCount +=
            gossipProtocol.getReceiverGraftScoreRejectionDecompositionCount();
        receiverGraftScoreRejectionDecompositionTotalScoreSum +=
            gossipProtocol.getReceiverGraftScoreRejectionDecompositionTotalScoreSum();
        receiverGraftScoreRejectionDecompositionMeshThresholdSum +=
            gossipProtocol.getReceiverGraftScoreRejectionDecompositionMeshThresholdSum();
        receiverGraftScoreRejectionDecompositionScoreMinusMeshThresholdSum +=
            gossipProtocol.getReceiverGraftScoreRejectionDecompositionScoreMinusMeshThresholdSum();
        receiverGraftScoreRejectionDecompositionTimeInMeshContributionSum +=
            gossipProtocol.getReceiverGraftScoreRejectionDecompositionTimeInMeshContributionSum();
        receiverGraftScoreRejectionDecompositionFirstDeliveriesContributionSum +=
            gossipProtocol
                .getReceiverGraftScoreRejectionDecompositionFirstDeliveriesContributionSum();
        receiverGraftScoreRejectionDecompositionLocalInvalidDeliveriesPenaltySum +=
            gossipProtocol
                .getReceiverGraftScoreRejectionDecompositionLocalInvalidDeliveriesPenaltySum();
        receiverGraftScoreRejectionDecompositionLocalBrokenPromisesPenaltySum +=
            gossipProtocol
                .getReceiverGraftScoreRejectionDecompositionLocalBrokenPromisesPenaltySum();
        receiverGraftScoreRejectionDecompositionGlobalInvalidDeliveriesPenaltySum +=
            gossipProtocol
                .getReceiverGraftScoreRejectionDecompositionGlobalInvalidDeliveriesPenaltySum();
        receiverGraftScoreRejectionDecompositionGlobalBrokenPromisesPenaltySum +=
            gossipProtocol
                .getReceiverGraftScoreRejectionDecompositionGlobalBrokenPromisesPenaltySum();
        remotePruneOutcomeDecompositionCount +=
            gossipProtocol.getRemotePruneOutcomeDecompositionCount();
        remotePruneOutcomeDecompositionTotalScoreSum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionTotalScoreSum();
        remotePruneOutcomeDecompositionMeshThresholdSum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionMeshThresholdSum();
        remotePruneOutcomeDecompositionScoreMinusMeshThresholdSum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionScoreMinusMeshThresholdSum();
        remotePruneOutcomeDecompositionTimeInMeshContributionSum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionTimeInMeshContributionSum();
        remotePruneOutcomeDecompositionFirstDeliveriesContributionSum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionFirstDeliveriesContributionSum();
        remotePruneOutcomeDecompositionLocalInvalidDeliveriesPenaltySum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionLocalInvalidDeliveriesPenaltySum();
        remotePruneOutcomeDecompositionLocalBrokenPromisesPenaltySum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionLocalBrokenPromisesPenaltySum();
        remotePruneOutcomeDecompositionGlobalInvalidDeliveriesPenaltySum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionGlobalInvalidDeliveriesPenaltySum();
        remotePruneOutcomeDecompositionGlobalBrokenPromisesPenaltySum +=
            gossipProtocol.getRemotePruneOutcomeDecompositionGlobalBrokenPromisesPenaltySum();
        localTopologyObservationCount += gossipProtocol.getLocalTopologyObservationCount();
        localKnownPeerSum += gossipProtocol.getLocalKnownPeerSum();
        localConnectablePeerSum += gossipProtocol.getLocalConnectablePeerSum();
        localConnectedPeerSum += gossipProtocol.getLocalConnectedPeerSum();
        localConnectedNonMeshPeerSum += gossipProtocol.getLocalConnectedNonMeshPeerSum();
        localReserveConnectedPeerSum += gossipProtocol.getLocalReserveConnectedPeerSum();
        localReserveConnectedImmatureSum += gossipProtocol.getLocalReserveConnectedImmatureSum();
        localReserveConnectedMatureSum += gossipProtocol.getLocalReserveConnectedMatureSum();
        localMeshFractionOfConnectedSum += gossipProtocol.getLocalMeshFractionOfConnectedSum();
        localConnectedHoldActiveSum += gossipProtocol.getLocalConnectedHoldActiveSum();
        localConnectedHoldForReserveSum += gossipProtocol.getLocalConnectedHoldForReserveSum();
        localKnownOnlyPeerSum += gossipProtocol.getLocalKnownOnlyPeerSum();
        localKnownNonConnectablePeerSum += gossipProtocol.getLocalKnownNonConnectablePeerSum();
        localConnectableButDisconnectedPeerSum +=
            gossipProtocol.getLocalConnectableButDisconnectedPeerSum();
        localKnownFromPxSum += gossipProtocol.getLocalKnownFromPxSum();
        localKnownFromDirectSum += gossipProtocol.getLocalKnownFromDirectSum();
        localPxKnownFreshSum += gossipProtocol.getLocalPxKnownFreshSum();
        localPxKnownStaleSum += gossipProtocol.getLocalPxKnownStaleSum();
        localMeshPeerSum += gossipProtocol.getLocalMeshPeerSum();
        localEligibleCandidateSum += gossipProtocol.getLocalEligibleCandidateSum();
        localEligibleConnectableCandidateSum +=
            gossipProtocol.getLocalEligibleConnectableCandidateSum();
        localEligibleConnectedCandidateSum +=
            gossipProtocol.getLocalEligibleConnectedCandidateSum();
        localEligibleKnownDisconnectedCandidateSum +=
            gossipProtocol.getLocalEligibleKnownDisconnectedCandidateSum();
        localConnectableFromDirectSum += gossipProtocol.getLocalConnectableFromDirectSum();
        localConnectableFromMeshSum += gossipProtocol.getLocalConnectableFromMeshSum();
        localConnectableFromConnectedSum += gossipProtocol.getLocalConnectableFromConnectedSum();
        localConnectableFromRegistrySum += gossipProtocol.getLocalConnectableFromRegistrySum();
        localConnectableFromPxSum += gossipProtocol.getLocalConnectableFromPxSum();
        localKnownNonConnectableFromPxSum += gossipProtocol.getLocalKnownNonConnectableFromPxSum();
        localKnownNonConnectableFromRegistrySum +=
            gossipProtocol.getLocalKnownNonConnectableFromRegistrySum();
        localConnectedDirectFreshSum += gossipProtocol.getLocalConnectedDirectFreshSum();
        localConnectedPxFreshSum += gossipProtocol.getLocalConnectedPxFreshSum();
        localConnectedMeshFreshSum += gossipProtocol.getLocalConnectedMeshFreshSum();
        localConnectedFanoutFreshSum += gossipProtocol.getLocalConnectedFanoutFreshSum();
        localConnectedReserveFreshSum += gossipProtocol.getLocalConnectedReserveFreshSum();
        localConnectedReserveRegistryOnlySum +=
            gossipProtocol.getLocalConnectedReserveRegistryOnlySum();
        localConnectedTargetSum += gossipProtocol.getLocalConnectedTargetSum();
        localConnectedSlackTargetSum += gossipProtocol.getLocalConnectedSlackTargetSum();
        localConnectedReserveGapSum += gossipProtocol.getLocalConnectedReserveGapSum();
        localReserveGapSum += gossipProtocol.getLocalReserveGapSum();
        localConnectedNonMeshTargetGapSum += gossipProtocol.getLocalConnectedNonMeshTargetGapSum();
        localConnectedNonMeshRetainedSum += gossipProtocol.getLocalConnectedNonMeshRetainedSum();
        localKnownRetainedAfterDisconnectSum +=
            gossipProtocol.getLocalKnownRetainedAfterDisconnectSum();
        localConnectedPxOnlySum += gossipProtocol.getLocalConnectedPxOnlySum();
        localKnownPxOnlySum += gossipProtocol.getLocalKnownPxOnlySum();
        localPxRetainedKnownOnlySum += gossipProtocol.getLocalPxRetainedKnownOnlySum();
        localOutboundConnectedPeerSum += gossipProtocol.getLocalOutboundConnectedPeerSum();
        localInboundConnectedPeerSum += gossipProtocol.getLocalInboundConnectedPeerSum();
        localDisconnectedKnownPeerSum += gossipProtocol.getLocalDisconnectedKnownPeerSum();
        localRegistryBootstrapAdded += gossipProtocol.getLocalRegistryBootstrapAdded();
        localCatalogExpiredRegistryRemoved +=
            gossipProtocol.getLocalCatalogExpiredRegistryRemoved();
        localCatalogExpiredKnownRemoved += gossipProtocol.getLocalCatalogExpiredKnownRemoved();
        localCatalogExpiredMeshRepairRemoved +=
            gossipProtocol.getLocalCatalogExpiredMeshRepairRemoved();
        localCatalogExpiredConnectedRemoved +=
            gossipProtocol.getLocalCatalogExpiredConnectedRemoved();
        localConnectAttempts += gossipProtocol.getLocalConnectAttempts();
        localConnectSuccesses += gossipProtocol.getLocalConnectSuccesses();
        localConnectFailures += gossipProtocol.getLocalConnectFailures();
        localConnectAttemptsFromDirect += gossipProtocol.getLocalConnectAttemptsFromDirect();
        localConnectAttemptsFromPx += gossipProtocol.getLocalConnectAttemptsFromPx();
        localConnectAttemptsFromMeshRepair +=
            gossipProtocol.getLocalConnectAttemptsFromMeshRepair();
        localConnectAttemptsForReserve += gossipProtocol.getLocalConnectAttemptsForReserve();
        localConnectSuccessesFromDirect += gossipProtocol.getLocalConnectSuccessesFromDirect();
        localConnectSuccessesFromPx += gossipProtocol.getLocalConnectSuccessesFromPx();
        localConnectSuccessesFromMeshRepair +=
            gossipProtocol.getLocalConnectSuccessesFromMeshRepair();
        localConnectSuccessesForReserve += gossipProtocol.getLocalConnectSuccessesForReserve();
        localConnectFailuresForReserve += gossipProtocol.getLocalConnectFailuresForReserve();
        localReserveConnectedPromotedToMesh +=
            gossipProtocol.getLocalReserveConnectedPromotedToMesh();
        localReserveConnectedConsumedByMesh +=
            gossipProtocol.getLocalReserveConnectedConsumedByMesh();
        localReserveConnectedConsumedBeforeMaturity +=
            gossipProtocol.getLocalReserveConnectedConsumedBeforeMaturity();
        localReserveConnectedDisconnectedBeforePromotion +=
            gossipProtocol.getLocalReserveConnectedDisconnectedBeforePromotion();
        localReserveConnectedDisconnectedBeforeMaturity +=
            gossipProtocol.getLocalReserveConnectedDisconnectedBeforeMaturity();
        localReserveConnectedDisconnectedAfterMaturity +=
            gossipProtocol.getLocalReserveConnectedDisconnectedAfterMaturity();
        localMeshSelectionSkippedReservedPeer +=
            gossipProtocol.getLocalMeshSelectionSkippedReservedPeer();
        localPxPromotedToConnected += gossipProtocol.getLocalPxPromotedToConnected();
        localPxAdmissionAttempts += gossipProtocol.getLocalPxAdmissionAttempts();
        localPxAdmissionSuccesses += gossipProtocol.getLocalPxAdmissionSuccesses();
        localPxAdmissionRejectionsTotal += gossipProtocol.getLocalPxAdmissionRejectionsTotal();
        localConnectRetryBackoffActiveSum += gossipProtocol.getLocalConnectRetryBackoffActiveSum();
        localConnectionTransitionOutbound += gossipProtocol.getLocalConnectionTransitionOutbound();
        localConnectionTransitionInbound += gossipProtocol.getLocalConnectionTransitionInbound();
        localConnectionTransitionDisconnected +=
            gossipProtocol.getLocalConnectionTransitionDisconnected();
        localConnectionDisconnectedByReceivedPrune +=
            gossipProtocol.getLocalConnectionDisconnectedByReceivedPrune();
        localConnectionDisconnectedByLocalPrune +=
            gossipProtocol.getLocalConnectionDisconnectedByLocalPrune();
        localConnectionDisconnectedByScore +=
            gossipProtocol.getLocalConnectionDisconnectedByScore();
        localConnectionDisconnectedByCapacity +=
            gossipProtocol.getLocalConnectionDisconnectedByCapacity();
        localConnectionDisconnectedByIdle += gossipProtocol.getLocalConnectionDisconnectedByIdle();
        localConnectionDisconnectedByExpiry +=
            gossipProtocol.getLocalConnectionDisconnectedByExpiry();
        localConnectionDisconnectedByOther +=
            gossipProtocol.getLocalConnectionDisconnectedByOther();
        localConnectionPreservedAfterReceivedPrune +=
            gossipProtocol.getLocalConnectionPreservedAfterReceivedPrune();
        localConnectionPreservedAfterLocalOversubscriptionPrune +=
            gossipProtocol.getLocalConnectionPreservedAfterLocalOversubscriptionPrune();
        localConnectionPreservedAfterLocalLowScorePrune +=
            gossipProtocol.getLocalConnectionPreservedAfterLocalLowScorePrune();
        localConnectedNonMeshSurvivedAfterPrune +=
            gossipProtocol.getLocalConnectedNonMeshSurvivedAfterPrune();
        localMeshRemovedButStillConnectedCount +=
            gossipProtocol.getLocalMeshRemovedButStillConnectedCount();
        localConnectedStaleDropCount += gossipProtocol.getLocalConnectedStaleDropCount();
        localConnectedNonMeshUsedForGossip +=
            gossipProtocol.getLocalConnectedNonMeshUsedForGossip();
        localConnectedNonMeshUsedForFanout +=
            gossipProtocol.getLocalConnectedNonMeshUsedForFanout();
        localConnectedNonMeshPromotedToMesh +=
            gossipProtocol.getLocalConnectedNonMeshPromotedToMesh();
        localConnectedNonMeshDisconnectedByCapacity +=
            gossipProtocol.getLocalConnectedNonMeshDisconnectedByCapacity();
        localConnectedNonMeshDisconnectedByScore +=
            gossipProtocol.getLocalConnectedNonMeshDisconnectedByScore();
        localConnectedNonMeshDisconnectedByExpiry +=
            gossipProtocol.getLocalConnectedNonMeshDisconnectedByExpiry();
        localConnectionLifetimeSum += gossipProtocol.getLocalConnectionLifetimeSum();
        localConnectionLifetimeCount += gossipProtocol.getLocalConnectionLifetimeCount();
        localConnectedNonMeshLifetimeSum += gossipProtocol.getLocalConnectedNonMeshLifetimeSum();
        localConnectedNonMeshLifetimeCount +=
            gossipProtocol.getLocalConnectedNonMeshLifetimeCount();
        localReserveConnectedLifetimeSum += gossipProtocol.getLocalReserveConnectedLifetimeSum();
        localReserveConnectedLifetimeCount +=
            gossipProtocol.getLocalReserveConnectedLifetimeCount();
        localReserveConnectedLifetimeBeforePromotionSum +=
            gossipProtocol.getLocalReserveConnectedLifetimeBeforePromotionSum();
        localReserveConnectedLifetimeBeforePromotionCount +=
            gossipProtocol.getLocalReserveConnectedLifetimeBeforePromotionCount();
        localConnectionReconnectSamePeerWithin5Heartbeats +=
            gossipProtocol.getLocalConnectionReconnectSamePeerWithin5Heartbeats();
        localConnectionReconnectSamePeerWithin10Heartbeats +=
            gossipProtocol.getLocalConnectionReconnectSamePeerWithin10Heartbeats();
        localConnectionIdleDisconnectsBeforeMinLifetime +=
            gossipProtocol.getLocalConnectionIdleDisconnectsBeforeMinLifetime();
        localConnectionIdleDisconnectsAfterMinLifetime +=
            gossipProtocol.getLocalConnectionIdleDisconnectsAfterMinLifetime();
        localConnectionDisconnectionBlockedByHold +=
            gossipProtocol.getLocalConnectionDisconnectionBlockedByHold();
        localConnectionHoldExpiredDisconnects +=
            gossipProtocol.getLocalConnectionHoldExpiredDisconnects();
        localConnectionConsecutiveIdleThresholdHits +=
            gossipProtocol.getLocalConnectionConsecutiveIdleThresholdHits();
        localConnectionRetryDelaySum += gossipProtocol.getLocalConnectionRetryDelaySum();
        localConnectionRetryDelayCount += gossipProtocol.getLocalConnectionRetryDelayCount();
        localConnectionRetryDelayMax =
            Math.max(
                localConnectionRetryDelayMax, gossipProtocol.getLocalConnectionRetryDelayMax());
        localConnectionFlappingPeerCount += gossipProtocol.getLocalConnectionFlappingPeerCount();
        pxSenderTrustDecompositionCount += gossipProtocol.getPxSenderTrustDecompositionCount();
        pxSenderTrustDecompositionTotalScoreSum +=
            gossipProtocol.getPxSenderTrustDecompositionTotalScoreSum();
        pxSenderTrustDecompositionThresholdSum +=
            gossipProtocol.getPxSenderTrustDecompositionThresholdSum();
        pxSenderTrustDecompositionScoreMinusThresholdSum +=
            gossipProtocol.getPxSenderTrustDecompositionScoreMinusThresholdSum();
        pxSenderTrustDecompositionTimeInMeshContributionSum +=
            gossipProtocol.getPxSenderTrustDecompositionTimeInMeshContributionSum();
        pxSenderTrustDecompositionFirstDeliveriesContributionSum +=
            gossipProtocol.getPxSenderTrustDecompositionFirstDeliveriesContributionSum();
        pxSenderTrustDecompositionLocalInvalidDeliveriesPenaltySum +=
            gossipProtocol.getPxSenderTrustDecompositionLocalInvalidDeliveriesPenaltySum();
        pxSenderTrustDecompositionLocalBrokenPromisesPenaltySum +=
            gossipProtocol.getPxSenderTrustDecompositionLocalBrokenPromisesPenaltySum();
        pxSenderTrustDecompositionGlobalInvalidDeliveriesPenaltySum +=
            gossipProtocol.getPxSenderTrustDecompositionGlobalInvalidDeliveriesPenaltySum();
        pxSenderTrustDecompositionGlobalBrokenPromisesPenaltySum +=
            gossipProtocol.getPxSenderTrustDecompositionGlobalBrokenPromisesPenaltySum();
        if (gossipProtocol.getObservedOutboundMeshPeerCount() > 0) {
          dOutOutboundMeshPeerMin =
              Math.min(dOutOutboundMeshPeerMin, gossipProtocol.getObservedOutboundMeshPeerMin());
        }
        if (gossipProtocol.getDOutPostWarmupObservationCount() > 0) {
          dOutPostWarmupOutboundMeshPeerMin =
              Math.min(
                  dOutPostWarmupOutboundMeshPeerMin,
                  gossipProtocol.getDOutPostWarmupOutboundMeshPeerMin());
        }
      }
    }

    double dOutOutboundMeshPeerMean =
        dOutOutboundMeshPeerObservationCount > 0
            ? (double) dOutOutboundMeshPeerSum / dOutOutboundMeshPeerObservationCount
            : 0.0;
    long dOutOutboundMeshPeerMinValue =
        dOutOutboundMeshPeerObservationCount > 0 ? dOutOutboundMeshPeerMin : 0;
    long dOutPostWarmupOutboundMeshPeerMinValue =
        dOutPostWarmupObservationCount > 0 ? dOutPostWarmupOutboundMeshPeerMin : 0;
    double candidatePoolActiveMean =
        candidatePoolObservationCount > 0
            ? (double) candidatePoolActiveSum / candidatePoolObservationCount
            : 0.0;
    double candidatePoolKnownActiveMean =
        candidatePoolObservationCount > 0
            ? (double) candidatePoolKnownActiveSum / candidatePoolObservationCount
            : 0.0;
    double candidatePoolPxActiveMean =
        candidatePoolObservationCount > 0
            ? (double) candidatePoolPxActiveSum / candidatePoolObservationCount
            : 0.0;
    double candidatePoolMeshRepairActiveMean =
        candidatePoolObservationCount > 0
            ? (double) candidatePoolMeshRepairActiveSum / candidatePoolObservationCount
            : 0.0;
    double candidatePoolRegistryActiveMean =
        candidatePoolObservationCount > 0
            ? (double) candidatePoolRegistryActiveSum / candidatePoolObservationCount
            : 0.0;
    double localKnownPeerMean =
        localTopologyObservationCount > 0
            ? (double) localKnownPeerSum / localTopologyObservationCount
            : 0.0;
    double localConnectablePeerMean =
        localTopologyObservationCount > 0
            ? (double) localConnectablePeerSum / localTopologyObservationCount
            : 0.0;
    double localConnectedPeerMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedPeerSum / localTopologyObservationCount
            : 0.0;
    double localConnectedNonMeshPeerMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedNonMeshPeerSum / localTopologyObservationCount
            : 0.0;
    double localReserveConnectedPeerMean =
        localTopologyObservationCount > 0
            ? (double) localReserveConnectedPeerSum / localTopologyObservationCount
            : 0.0;
    double localReserveConnectedImmatureMean =
        localTopologyObservationCount > 0
            ? (double) localReserveConnectedImmatureSum / localTopologyObservationCount
            : 0.0;
    double localReserveConnectedMatureMean =
        localTopologyObservationCount > 0
            ? (double) localReserveConnectedMatureSum / localTopologyObservationCount
            : 0.0;
    double localMeshFractionOfConnectedMean =
        localTopologyObservationCount > 0
            ? localMeshFractionOfConnectedSum / localTopologyObservationCount
            : 0.0;
    double localConnectedHoldActiveMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedHoldActiveSum / localTopologyObservationCount
            : 0.0;
    double localConnectedHoldForReserveMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedHoldForReserveSum / localTopologyObservationCount
            : 0.0;
    double localKnownOnlyPeerMean =
        localTopologyObservationCount > 0
            ? (double) localKnownOnlyPeerSum / localTopologyObservationCount
            : 0.0;
    double localKnownNonConnectablePeerMean =
        localTopologyObservationCount > 0
            ? (double) localKnownNonConnectablePeerSum / localTopologyObservationCount
            : 0.0;
    double localConnectableButDisconnectedPeerMean =
        localTopologyObservationCount > 0
            ? (double) localConnectableButDisconnectedPeerSum / localTopologyObservationCount
            : 0.0;
    double localKnownFromPxMean =
        localTopologyObservationCount > 0
            ? (double) localKnownFromPxSum / localTopologyObservationCount
            : 0.0;
    double localKnownFromDirectMean =
        localTopologyObservationCount > 0
            ? (double) localKnownFromDirectSum / localTopologyObservationCount
            : 0.0;
    double localPxKnownFreshMean =
        localTopologyObservationCount > 0
            ? (double) localPxKnownFreshSum / localTopologyObservationCount
            : 0.0;
    double localPxKnownStaleMean =
        localTopologyObservationCount > 0
            ? (double) localPxKnownStaleSum / localTopologyObservationCount
            : 0.0;
    double localMeshPeerMean =
        localTopologyObservationCount > 0
            ? (double) localMeshPeerSum / localTopologyObservationCount
            : 0.0;
    double localEligibleCandidateMean =
        localTopologyObservationCount > 0
            ? (double) localEligibleCandidateSum / localTopologyObservationCount
            : 0.0;
    double localEligibleConnectableCandidateMean =
        localTopologyObservationCount > 0
            ? (double) localEligibleConnectableCandidateSum / localTopologyObservationCount
            : 0.0;
    double localEligibleConnectedCandidateMean =
        localTopologyObservationCount > 0
            ? (double) localEligibleConnectedCandidateSum / localTopologyObservationCount
            : 0.0;
    double localEligibleKnownDisconnectedCandidateMean =
        localTopologyObservationCount > 0
            ? (double) localEligibleKnownDisconnectedCandidateSum / localTopologyObservationCount
            : 0.0;
    double localConnectedDirectFreshMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedDirectFreshSum / localTopologyObservationCount
            : 0.0;
    double localConnectableFromDirectMean =
        localTopologyObservationCount > 0
            ? (double) localConnectableFromDirectSum / localTopologyObservationCount
            : 0.0;
    double localConnectableFromMeshMean =
        localTopologyObservationCount > 0
            ? (double) localConnectableFromMeshSum / localTopologyObservationCount
            : 0.0;
    double localConnectableFromConnectedMean =
        localTopologyObservationCount > 0
            ? (double) localConnectableFromConnectedSum / localTopologyObservationCount
            : 0.0;
    double localConnectableFromRegistryMean =
        localTopologyObservationCount > 0
            ? (double) localConnectableFromRegistrySum / localTopologyObservationCount
            : 0.0;
    double localConnectableFromPxMean =
        localTopologyObservationCount > 0
            ? (double) localConnectableFromPxSum / localTopologyObservationCount
            : 0.0;
    double localKnownNonConnectableFromPxMean =
        localTopologyObservationCount > 0
            ? (double) localKnownNonConnectableFromPxSum / localTopologyObservationCount
            : 0.0;
    double localKnownNonConnectableFromRegistryMean =
        localTopologyObservationCount > 0
            ? (double) localKnownNonConnectableFromRegistrySum / localTopologyObservationCount
            : 0.0;
    double localConnectedPxFreshMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedPxFreshSum / localTopologyObservationCount
            : 0.0;
    double localConnectedMeshFreshMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedMeshFreshSum / localTopologyObservationCount
            : 0.0;
    double localConnectedFanoutFreshMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedFanoutFreshSum / localTopologyObservationCount
            : 0.0;
    double localConnectedReserveFreshMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedReserveFreshSum / localTopologyObservationCount
            : 0.0;
    double localConnectedReserveRegistryOnlyMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedReserveRegistryOnlySum / localTopologyObservationCount
            : 0.0;
    double localConnectedTargetMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedTargetSum / localTopologyObservationCount
            : 0.0;
    double localConnectedSlackTargetMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedSlackTargetSum / localTopologyObservationCount
            : 0.0;
    double localConnectedReserveGapMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedReserveGapSum / localTopologyObservationCount
            : 0.0;
    double localReserveGapMean =
        localTopologyObservationCount > 0
            ? (double) localReserveGapSum / localTopologyObservationCount
            : 0.0;
    double localConnectedNonMeshTargetGapMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedNonMeshTargetGapSum / localTopologyObservationCount
            : 0.0;
    double localConnectedNonMeshRetainedMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedNonMeshRetainedSum / localTopologyObservationCount
            : 0.0;
    double localReserveConnectedMeanLifetime =
        localReserveConnectedLifetimeCount > 0
            ? (double) localReserveConnectedLifetimeSum / localReserveConnectedLifetimeCount
            : 0.0;
    double localReserveConnectedMeanLifetimeBeforePromotion =
        localReserveConnectedLifetimeBeforePromotionCount > 0
            ? (double) localReserveConnectedLifetimeBeforePromotionSum
                / localReserveConnectedLifetimeBeforePromotionCount
            : 0.0;
    double localKnownRetainedAfterDisconnectMean =
        localTopologyObservationCount > 0
            ? (double) localKnownRetainedAfterDisconnectSum / localTopologyObservationCount
            : 0.0;
    double localConnectedPxOnlyMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedPxOnlySum / localTopologyObservationCount
            : 0.0;
    double localKnownPxOnlyMean =
        localTopologyObservationCount > 0
            ? (double) localKnownPxOnlySum / localTopologyObservationCount
            : 0.0;
    double localPxRetainedKnownOnlyMean =
        localTopologyObservationCount > 0
            ? (double) localPxRetainedKnownOnlySum / localTopologyObservationCount
            : 0.0;
    double localOutboundConnectedPeerMean =
        localTopologyObservationCount > 0
            ? (double) localOutboundConnectedPeerSum / localTopologyObservationCount
            : 0.0;
    double localInboundConnectedPeerMean =
        localTopologyObservationCount > 0
            ? (double) localInboundConnectedPeerSum / localTopologyObservationCount
            : 0.0;
    double localDisconnectedKnownPeerMean =
        localTopologyObservationCount > 0
            ? (double) localDisconnectedKnownPeerSum / localTopologyObservationCount
            : 0.0;
    double localConnectRetryBackoffActiveMean =
        localTopologyObservationCount > 0
            ? (double) localConnectRetryBackoffActiveSum / localTopologyObservationCount
            : 0.0;
    double localConnectionLifetimeMean =
        localConnectionLifetimeCount > 0
            ? (double) localConnectionLifetimeSum / localConnectionLifetimeCount
            : 0.0;
    double localConnectedNonMeshLifetimeMean =
        localConnectedNonMeshLifetimeCount > 0
            ? (double) localConnectedNonMeshLifetimeSum / localConnectedNonMeshLifetimeCount
            : 0.0;
    double localConnectedNonMeshUsedForGossipMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedNonMeshUsedForGossip / localTopologyObservationCount
            : 0.0;
    double localConnectedNonMeshUsedForFanoutMean =
        localTopologyObservationCount > 0
            ? (double) localConnectedNonMeshUsedForFanout / localTopologyObservationCount
            : 0.0;
    double localConnectionRetryDelayMean =
        localConnectionRetryDelayCount > 0
            ? (double) localConnectionRetryDelaySum / localConnectionRetryDelayCount
            : 0.0;

    String repairRelaxReceivedPruneRaw =
        Configuration.getString("gossipsub.repair_relax_received_prune_for_d_out", "");
    String repairRelaxReceivedPruneLocalGraceRaw =
        Configuration.getString("gossipsub.repair_relax_received_prune_local_grace_enabled", "");
    String receiverRelaxGraftBackoffRaw =
        Configuration.getString("gossipsub.receiver_relax_graft_backoff_for_d_out", "");
    String dOutInplacePromotionRaw =
        Configuration.getString("gossipsub.d_out_inplace_promotion_enabled", "");
    String dOutForcedReseedRaw =
        Configuration.getString("gossipsub.d_out_forced_reseed_enabled", "");

    try (FileWriter writer = new FileWriter(getCurrentLogFolderName() + "/" + "run_info.json")) {
      writer.write("{\n");
      writer.write(String.format("  \"experiment_index\": %d,\n", currentExperimentIndex));
      writer.write(String.format("  \"random_seed\": %d,\n", currentExperimentSeed));
      writer.write(String.format("  \"total_experiments\": %d,\n", totalExperiments));
      writer.write(
          String.format(
              "  \"score_enabled\": %s,\n", Boolean.toString(GossipCommonConfig.scoreEnabled)));
      writer.write(
          String.format("  \"degraded_peer_ratio\": %s,\n", GossipCommonConfig.degradedPeerRatio));
      writer.write(
          String.format(
              "  \"degraded_peer_withhold_probability\": %s,\n",
              GossipCommonConfig.degradedPeerWithholdProbability));
      writer.write(String.format("  \"d_out\": %d,\n", GossipCommonConfig.D_out));
      writer.write(
          String.format(
              "  \"d_out_target\": %d,\n",
              Math.min(GossipCommonConfig.D_out, GossipCommonConfig.D)));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_for_d_out\": %s,\n",
              GossipCommonConfig.repairRelaxReceivedPruneForDOut));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_for_d_out_config_present\": %s,\n",
              Configuration.contains("gossipsub.repair_relax_received_prune_for_d_out")));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_for_d_out_config_raw\": \"%s\",\n",
              escapeJson(repairRelaxReceivedPruneRaw)));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_budget_per_heartbeat\": %d,\n",
              GossipCommonConfig.repairRelaxReceivedPruneBudgetPerHeartbeat));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_min_score\": %s,\n",
              GossipCommonConfig.repairRelaxReceivedPruneMinScore));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_min_age_fraction\": %s,\n",
              GossipCommonConfig.repairRelaxReceivedPruneMinAgeFraction));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_for_d_out\": %s,\n",
              GossipCommonConfig.receiverRelaxGraftBackoffForDOut));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_for_d_out_config_present\": %s,\n",
              Configuration.contains("gossipsub.receiver_relax_graft_backoff_for_d_out")));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_for_d_out_config_raw\": \"%s\",\n",
              escapeJson(receiverRelaxGraftBackoffRaw)));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_budget_per_heartbeat\": %d,\n",
              GossipCommonConfig.receiverRelaxGraftBackoffBudgetPerHeartbeat));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_min_score\": %s,\n",
              GossipCommonConfig.receiverRelaxGraftBackoffMinScore));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_min_age_fraction\": %s,\n",
              GossipCommonConfig.receiverRelaxGraftBackoffMinAgeFraction));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_local_grace_enabled\": %s,\n",
              GossipCommonConfig.repairRelaxReceivedPruneLocalGraceEnabled));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_local_grace_enabled_config_present\": %s,\n",
              Configuration.contains("gossipsub.repair_relax_received_prune_local_grace_enabled")));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_local_grace_enabled_config_raw\": \"%s\",\n",
              escapeJson(repairRelaxReceivedPruneLocalGraceRaw)));
      writer.write(
          String.format(
              "  \"d_out_inplace_promotion_enabled\": %s,\n",
              GossipCommonConfig.dOutInplacePromotionEnabled));
      writer.write(
          String.format(
              "  \"d_out_inplace_promotion_enabled_config_present\": %s,\n",
              Configuration.contains("gossipsub.d_out_inplace_promotion_enabled")));
      writer.write(
          String.format(
              "  \"d_out_inplace_promotion_enabled_config_raw\": \"%s\",\n",
              escapeJson(dOutInplacePromotionRaw)));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_enabled\": %s,\n",
              GossipCommonConfig.dOutForcedReseedEnabled));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_enabled_config_present\": %s,\n",
              Configuration.contains("gossipsub.d_out_forced_reseed_enabled")));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_enabled_config_raw\": \"%s\",\n",
              escapeJson(dOutForcedReseedRaw)));
      writer.write(
          String.format("  \"accept_px_threshold\": %s,\n", GossipCommonConfig.acceptPXThreshold));
      writer.write(String.format("  \"prune_peers\": %d,\n", GossipCommonConfig.prunePeers));
      writer.write(
          String.format(
              "  \"opportunistic_graft_ticks\": %d,\n",
              GossipCommonConfig.opportunisticGraftTicks));
      writer.write(
          String.format(
              "  \"opportunistic_graft_threshold\": %s,\n",
              GossipCommonConfig.opportunisticGraftThreshold));
      writer.write(
          String.format(
              "  \"opportunistic_graft_peers\": %d,\n",
              GossipCommonConfig.opportunisticGraftPeers));
      writer.write(String.format("  \"network_size\": %d,\n", Network.size()));
      writer.write(String.format("  \"up_nodes\": %d,\n", upNodes));
      writer.write(
          String.format(
              "  \"opportunistic_graft_evaluations\": %d,\n", opportunisticGraftEvaluations));
      writer.write(
          String.format("  \"opportunistic_graft_triggers\": %d,\n", opportunisticGraftTriggers));
      writer.write(
          String.format("  \"opportunistic_graft_peers_grafted\": %d,\n", opportunisticGraftPeers));
      writer.write(String.format("  \"px_prune_messages_sent\": %d,\n", pxPruneMessagesSent));
      writer.write(String.format("  \"px_candidates_advertised\": %d,\n", pxCandidatesAdvertised));
      writer.write(
          String.format(
              "  \"px_final_potential_candidate_entries\": %d,\n",
              pxFinalPotentialCandidateEntries));
      writer.write(
          String.format(
              "  \"px_final_potential_advertisable_entries\": %d,\n",
              pxFinalPotentialAdvertisableEntries));
      writer.write(
          String.format(
              "  \"px_final_potential_topics_with_candidates\": %d,\n",
              pxFinalPotentialTopicsWithCandidates));
      writer.write(
          String.format(
              "  \"px_final_potential_nodes_with_candidates\": %d,\n",
              pxFinalPotentialNodesWithCandidates));
      writer.write(String.format("  \"px_candidates_seen\": %d,\n", pxCandidatesSeen));
      writer.write(
          String.format(
              "  \"px_candidates_rejected_sender_threshold\": %d,\n",
              pxCandidatesRejectedSenderThreshold));
      writer.write(
          String.format(
              "  \"px_candidates_rejected_already_known\": %d,\n",
              pxCandidatesRejectedAlreadyKnown));
      writer.write(
          String.format(
              "  \"px_candidates_rejected_already_connected\": %d,\n",
              pxCandidatesRejectedAlreadyConnected));
      writer.write(
          String.format("  \"px_candidates_rejected_in_mesh\": %d,\n", pxCandidatesRejectedInMesh));
      writer.write(
          String.format(
              "  \"px_candidates_rejected_prune_backoff\": %d,\n",
              pxCandidatesRejectedPruneBackoff));
      writer.write(
          String.format("  \"px_candidates_rejected_score\": %d,\n", pxCandidatesRejectedScore));
      writer.write(
          String.format(
              "  \"px_candidates_rejected_expired_or_stale\": %d,\n",
              pxCandidatesRejectedExpiredOrStale));
      writer.write(
          String.format(
              "  \"px_candidates_rejected_capacity\": %d,\n", pxCandidatesRejectedCapacity));
      writer.write(
          String.format(
              "  \"px_candidates_passed_sender_threshold\": %d,\n",
              pxCandidatesPassedSenderThreshold));
      writer.write(
          String.format(
              "  \"px_candidates_admitted_to_known\": %d,\n", pxCandidatesAdmittedToKnown));
      writer.write(
          String.format(
              "  \"px_candidates_admitted_and_later_connected\": %d,\n",
              pxCandidatesAdmittedAndLaterConnected));
      writer.write(String.format("  \"px_candidates_accepted\": %d,\n", pxCandidatesAccepted));
      writer.write(String.format("  \"px_candidates_rejected\": %d,\n", pxCandidatesRejected));
      writer.write(
          String.format(
              "  \"px_accepted_from_high_score_senders\": %d,\n", pxAcceptedFromHighScoreSenders));
      writer.write(
          String.format(
              "  \"px_peers_skipped_recently_advertised\": %d,\n",
              pxPeersSkippedRecentlyAdvertised));
      writer.write(
          String.format(
              "  \"px_peers_selected_after_novelty_filtering\": %d,\n",
              pxPeersSelectedAfterNoveltyFiltering));
      writer.write(String.format("  \"px_senders_seen\": %d,\n", pxSendersSeen));
      writer.write(
          String.format(
              "  \"px_senders_above_accept_threshold\": %d,\n", pxSendersAboveAcceptThreshold));
      writer.write(
          String.format(
              "  \"px_senders_below_accept_threshold\": %d,\n", pxSendersBelowAcceptThreshold));
      writer.write(String.format("  \"px_senders_score_sum\": %.6f,\n", pxSendersScoreSum));
      writer.write(String.format("  \"px_senders_score_count\": %d,\n", pxSendersScoreCount));
      writer.write(
          String.format(
              "  \"px_senders_score_min\": %.6f,\n",
              pxSendersScoreCount > 0 ? pxSendersScoreMin : 0.0));
      writer.write(
          String.format(
              "  \"px_senders_score_max\": %.6f,\n",
              pxSendersScoreCount > 0 ? pxSendersScoreMax : 0.0));
      writer.write(
          String.format(
              "  \"px_senders_score_lt_threshold_minus_1\": %d,\n",
              pxSendersScoreLtThresholdMinusOne));
      writer.write(
          String.format(
              "  \"px_senders_score_threshold_minus_1_to_threshold_minus_half\": %d,\n",
              pxSendersScoreThresholdMinusOneToMinusHalf));
      writer.write(
          String.format(
              "  \"px_senders_score_threshold_minus_half_to_threshold\": %d,\n",
              pxSendersScoreThresholdMinusHalfToThreshold));
      writer.write(
          String.format("  \"px_senders_score_ge_threshold\": %d,\n", pxSendersScoreGeThreshold));
      writer.write(String.format("  \"d_out_mesh_outbound_peers\": %d,\n", dOutMeshOutboundPeers));
      writer.write(
          String.format(
              "  \"d_out_grafts_accepted_at_capacity\": %d,\n", dOutGraftsAcceptedAtCapacity));
      writer.write(
          String.format(
              "  \"d_out_grafts_rejected_at_capacity\": %d,\n", dOutGraftsRejectedAtCapacity));
      writer.write(
          String.format("  \"d_out_outbound_mesh_peers_mean\": %s,\n", dOutOutboundMeshPeerMean));
      writer.write(
          String.format(
              "  \"d_out_outbound_mesh_peers_min\": %d,\n", dOutOutboundMeshPeerMinValue));
      writer.write(
          String.format("  \"d_out_heartbeat_deficit_sum\": %d,\n", dOutHeartbeatDeficitSum));
      writer.write(
          String.format(
              "  \"d_out_heartbeat_deficit_nonzero_count\": %d,\n",
              dOutHeartbeatDeficitNonzeroCount));
      writer.write(
          String.format(
              "  \"d_out_repair_attempts_for_deficit\": %d,\n", dOutRepairAttemptsForDeficit));
      writer.write(String.format("  \"d_out_repair_successes\": %d,\n", dOutRepairSuccesses));
      writer.write(
          String.format(
              "  \"d_out_steady_state_topup_attempts\": %d,\n", dOutSteadyStateTopupAttempts));
      writer.write(
          String.format(
              "  \"d_out_steady_state_topup_successes\": %d,\n", dOutSteadyStateTopupSuccesses));
      writer.write(
          String.format(
              "  \"d_out_steady_state_replacements\": %d,\n", dOutSteadyStateReplacements));
      writer.write(String.format("  \"d_out_steady_state_prunes\": %d,\n", dOutSteadyStatePrunes));
      writer.write(String.format("  \"d_out_steady_state_grafts\": %d,\n", dOutSteadyStateGrafts));
      writer.write(
          String.format(
              "  \"d_out_heartbeat_observation_count\": %d,\n", dOutHeartbeatObservationCount));
      writer.write(
          String.format(
              "  \"d_out_zero_outbound_observation_count\": %d,\n",
              dOutZeroOutboundObservationCount));
      writer.write(
          String.format(
              "  \"d_out_deficit_observation_count\": %d,\n", dOutDeficitObservationCount));
      writer.write(
          String.format(
              "  \"d_out_post_warmup_observation_count\": %d,\n", dOutPostWarmupObservationCount));
      writer.write(
          String.format(
              "  \"d_out_post_warmup_zero_outbound_count\": %d,\n",
              dOutPostWarmupZeroOutboundCount));
      writer.write(
          String.format(
              "  \"d_out_post_warmup_deficit_count\": %d,\n", dOutPostWarmupDeficitCount));
      writer.write(
          String.format(
              "  \"d_out_post_warmup_outbound_mesh_peers_min\": %d,\n",
              dOutPostWarmupOutboundMeshPeerMinValue));
      writer.write(
          String.format(
              "  \"d_out_max_consecutive_deficit_heartbeats\": %d,\n",
              dOutMaxConsecutiveDeficitHeartbeats));
      writer.write(
          String.format(
              "  \"d_out_max_consecutive_zero_outbound_heartbeats\": %d,\n",
              dOutMaxConsecutiveZeroOutboundHeartbeats));
      writer.write(String.format("  \"d_out_escalation_attempts\": %d,\n", dOutEscalationAttempts));
      writer.write(
          String.format("  \"d_out_escalation_successes\": %d,\n", dOutEscalationSuccesses));
      writer.write(
          String.format(
              "  \"d_out_zero_outbound_emergency_attempts\": %d,\n",
              dOutZeroOutboundEmergencyAttempts));
      writer.write(
          String.format(
              "  \"d_out_zero_outbound_emergency_successes\": %d,\n",
              dOutZeroOutboundEmergencySuccesses));
      writer.write(
          String.format(
              "  \"d_out_inplace_promotion_attempts\": %d,\n", dOutInplacePromotionAttempts));
      writer.write(
          String.format(
              "  \"d_out_inplace_promotion_successes\": %d,\n", dOutInplacePromotionSuccesses));
      writer.write(
          String.format(
              "  \"d_out_inplace_zero_outbound_promotions\": %d,\n",
              dOutInplaceZeroOutboundPromotions));
      writer.write(
          String.format(
              "  \"d_out_inplace_deficit_promotions\": %d,\n", dOutInplaceDeficitPromotions));
      writer.write(
          String.format("  \"d_out_forced_reseed_attempts\": %d,\n", dOutForcedReseedAttempts));
      writer.write(
          String.format("  \"d_out_forced_reseed_successes\": %d,\n", dOutForcedReseedSuccesses));
      writer.write(
          String.format("  \"d_out_forced_reseed_grafts\": %d,\n", dOutForcedReseedGrafts));
      writer.write(
          String.format("  \"d_out_forced_reseed_prunes\": %d,\n", dOutForcedReseedPrunes));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_skipped_no_candidate\": %d,\n",
              dOutForcedReseedSkippedNoCandidate));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_skipped_no_prunable_inbound\": %d,\n",
              dOutForcedReseedSkippedNoPrunableInbound));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_eligible_count\": %d,\n", dOutForcedReseedEligibleCount));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_blocked_not_post_warmup\": %d,\n",
              dOutForcedReseedBlockedNotPostWarmup));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_blocked_consecutive_zero_below_threshold\": %d,\n",
              dOutForcedReseedBlockedConsecutiveZeroBelowThreshold));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_blocked_outbound_before_positive\": %d,\n",
              dOutForcedReseedBlockedOutboundBeforePositive));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_blocked_deficit_before_zero\": %d,\n",
              dOutForcedReseedBlockedDeficitBeforeZero));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_blocked_after_inplace_no_remaining_deficit\": %d,\n",
              dOutForcedReseedBlockedAfterInplaceNoRemainingDeficit));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_blocked_after_inplace_not_persistent\": %d,\n",
              dOutForcedReseedBlockedAfterInplaceNotPersistent));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_reached_callsite_count\": %d,\n",
              dOutForcedReseedReachedCallsiteCount));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_zero_outbound_emergency_count\": %d,\n",
              dOutForcedReseedZeroOutboundEmergencyCount));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_inplace_success_during_zero_outbound_count\": %d,\n",
              dOutForcedReseedInplaceSuccessDuringZeroOutboundCount));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_after_inplace_remaining_deficit_count\": %d,\n",
              dOutForcedReseedAfterInplaceRemainingDeficitCount));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_returned_before_callsite_count\": %d,\n",
              dOutForcedReseedReturnedBeforeCallsiteCount));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_eligible_by_deficit\": %d,\n",
              dOutForcedReseedEligibleByDeficit));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_reached_callsite_after_inplace\": %d,\n",
              dOutForcedReseedReachedCallsiteAfterInplace));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_eligible_by_zero_remaining_deficit\": %d,\n",
              dOutForcedReseedEligibleByZeroRemainingDeficit));
      writer.write(
          String.format(
              "  \"d_out_forced_reseed_reached_callsite_after_zero_remaining_deficit\": %d,\n",
              dOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit));
      writer.write(
          String.format(
              "  \"d_out_max_consecutive_zero_remaining_deficit_heartbeats\": %d,\n",
              dOutMaxConsecutiveZeroRemainingDeficitHeartbeats));
      writer.write(String.format("  \"candidate_pool_entries\": %d,\n", candidatePoolEntries));
      writer.write(
          String.format("  \"candidate_pool_active_mean\": %s,\n", candidatePoolActiveMean));
      writer.write(
          String.format(
              "  \"candidate_pool_known_active_mean\": %s,\n", candidatePoolKnownActiveMean));
      writer.write(
          String.format("  \"candidate_pool_px_active_mean\": %s,\n", candidatePoolPxActiveMean));
      writer.write(
          String.format(
              "  \"candidate_pool_mesh_repair_active_mean\": %s,\n",
              candidatePoolMeshRepairActiveMean));
      writer.write(
          String.format(
              "  \"candidate_pool_registry_active_mean\": %s,\n", candidatePoolRegistryActiveMean));
      writer.write(
          String.format(
              "  \"candidate_pool_expired_removed\": %d,\n", candidatePoolExpiredRemoved));
      writer.write(
          String.format(
              "  \"candidate_pool_px_expired_removed\": %d,\n", candidatePoolPxExpiredRemoved));
      writer.write(
          String.format("  \"candidate_pool_capped_removed\": %d,\n", candidatePoolCappedRemoved));
      writer.write(
          String.format(
              "  \"candidate_pool_backoff_filtered\": %d,\n", candidatePoolBackoffFiltered));
      writer.write(
          String.format("  \"candidate_pool_score_filtered\": %d,\n", candidatePoolScoreFiltered));
      writer.write(
          String.format(
              "  \"candidate_pool_empty_selection_count\": %d,\n",
              candidatePoolEmptySelectionCount));
      writer.write(
          String.format(
              "  \"discovery_final_known_via_bootstrap_entries\": %d,\n",
              discoveryFinalKnownViaBootstrapEntries));
      writer.write(
          String.format(
              "  \"discovery_final_known_via_ambient_entries\": %d,\n",
              discoveryFinalKnownViaAmbientEntries));
      writer.write(
          String.format(
              "  \"discovery_final_known_via_px_entries\": %d,\n",
              discoveryFinalKnownViaPxEntries));
      writer.write(
          String.format(
              "  \"discovery_final_known_via_observation_entries\": %d,\n",
              discoveryFinalKnownViaObservationEntries));
      writer.write(
          String.format(
              "  \"discovery_final_connected_entries\": %d,\n", discoveryFinalConnectedEntries));
      writer.write(
          String.format("  \"discovery_final_mesh_entries\": %d,\n", discoveryFinalMeshEntries));
      writer.write(
          String.format(
              "  \"local_topology_observation_count\": %d,\n", localTopologyObservationCount));
      writer.write(String.format("  \"local_known_peer_mean\": %s,\n", localKnownPeerMean));
      writer.write(
          String.format("  \"local_connectable_peer_mean\": %s,\n", localConnectablePeerMean));
      writer.write(String.format("  \"local_connected_peer_mean\": %s,\n", localConnectedPeerMean));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_peer_mean\": %s,\n", localConnectedNonMeshPeerMean));
      writer.write(
          String.format(
              "  \"local_reserve_connected_peer_mean\": %s,\n", localReserveConnectedPeerMean));
      writer.write(
          String.format(
              "  \"local_reserve_connected_immature_mean\": %s,\n",
              localReserveConnectedImmatureMean));
      writer.write(
          String.format(
              "  \"local_reserve_connected_mature_mean\": %s,\n", localReserveConnectedMatureMean));
      writer.write(
          String.format(
              "  \"local_mesh_fraction_of_connected_mean\": %s,\n",
              localMeshFractionOfConnectedMean));
      writer.write(
          String.format(
              "  \"local_connected_hold_active_mean\": %s,\n", localConnectedHoldActiveMean));
      writer.write(
          String.format(
              "  \"local_connected_hold_for_reserve_mean\": %s,\n",
              localConnectedHoldForReserveMean));
      writer.write(
          String.format("  \"local_connected_target_mean\": %s,\n", localConnectedTargetMean));
      writer.write(
          String.format(
              "  \"local_connected_slack_target_mean\": %s,\n", localConnectedSlackTargetMean));
      writer.write(
          String.format(
              "  \"local_connected_reserve_target\": %d,\n",
              GossipCommonConfig.localConnectedReserveTargetPerTopic));
      writer.write(
          String.format(
              "  \"local_connected_reserve_gap_mean\": %s,\n", localConnectedReserveGapMean));
      writer.write(String.format("  \"local_reserve_gap_mean\": %s,\n", localReserveGapMean));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_target_gap_mean\": %s,\n",
              localConnectedNonMeshTargetGapMean));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_retained_mean\": %s,\n",
              localConnectedNonMeshRetainedMean));
      writer.write(
          String.format("  \"local_known_only_peer_mean\": %s,\n", localKnownOnlyPeerMean));
      writer.write(
          String.format(
              "  \"local_known_non_connectable_peer_mean\": %s,\n",
              localKnownNonConnectablePeerMean));
      writer.write(
          String.format(
              "  \"local_connectable_but_disconnected_peer_mean\": %s,\n",
              localConnectableButDisconnectedPeerMean));
      writer.write(String.format("  \"local_known_from_px_mean\": %s,\n", localKnownFromPxMean));
      writer.write(
          String.format("  \"local_known_from_direct_mean\": %s,\n", localKnownFromDirectMean));
      writer.write(String.format("  \"local_px_known_fresh_mean\": %s,\n", localPxKnownFreshMean));
      writer.write(String.format("  \"local_px_known_stale_mean\": %s,\n", localPxKnownStaleMean));
      writer.write(String.format("  \"local_mesh_peer_mean\": %s,\n", localMeshPeerMean));
      writer.write(
          String.format("  \"local_eligible_candidate_mean\": %s,\n", localEligibleCandidateMean));
      writer.write(
          String.format(
              "  \"local_eligible_connectable_candidate_mean\": %s,\n",
              localEligibleConnectableCandidateMean));
      writer.write(
          String.format(
              "  \"local_eligible_connected_candidate_mean\": %s,\n",
              localEligibleConnectedCandidateMean));
      writer.write(
          String.format(
              "  \"local_eligible_known_disconnected_candidate_mean\": %s,\n",
              localEligibleKnownDisconnectedCandidateMean));
      writer.write(
          String.format(
              "  \"local_connectable_from_direct_mean\": %s,\n", localConnectableFromDirectMean));
      writer.write(
          String.format(
              "  \"local_connectable_from_mesh_mean\": %s,\n", localConnectableFromMeshMean));
      writer.write(
          String.format(
              "  \"local_connectable_from_connected_mean\": %s,\n",
              localConnectableFromConnectedMean));
      writer.write(
          String.format(
              "  \"local_connectable_from_registry_mean\": %s,\n",
              localConnectableFromRegistryMean));
      writer.write(
          String.format("  \"local_connectable_from_px_mean\": %s,\n", localConnectableFromPxMean));
      writer.write(
          String.format(
              "  \"local_known_non_connectable_from_px_mean\": %s,\n",
              localKnownNonConnectableFromPxMean));
      writer.write(
          String.format(
              "  \"local_known_non_connectable_from_registry_mean\": %s,\n",
              localKnownNonConnectableFromRegistryMean));
      writer.write(
          String.format(
              "  \"local_connected_direct_fresh_mean\": %s,\n", localConnectedDirectFreshMean));
      writer.write(
          String.format(
              "  \"local_disconnect_by_received_prune\": %d,\n",
              localConnectionDisconnectedByReceivedPrune));
      writer.write(
          String.format(
              "  \"local_disconnect_by_local_prune\": %d,\n",
              localConnectionDisconnectedByLocalPrune));
      writer.write(
          String.format(
              "  \"local_disconnect_by_score\": %d,\n", localConnectionDisconnectedByScore));
      writer.write(
          String.format(
              "  \"local_disconnect_by_idle\": %d,\n", localConnectionDisconnectedByIdle));
      writer.write(
          String.format(
              "  \"local_disconnect_by_expiry\": %d,\n", localConnectionDisconnectedByExpiry));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_used_for_gossip_mean\": %s,\n",
              localConnectedNonMeshUsedForGossipMean));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_used_for_fanout_mean\": %s,\n",
              localConnectedNonMeshUsedForFanoutMean));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_promoted_to_mesh\": %d,\n",
              localConnectedNonMeshPromotedToMesh));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_disconnected_by_capacity\": %d,\n",
              localConnectedNonMeshDisconnectedByCapacity));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_disconnected_by_score\": %d,\n",
              localConnectedNonMeshDisconnectedByScore));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_disconnected_by_expiry\": %d,\n",
              localConnectedNonMeshDisconnectedByExpiry));
      writer.write(
          String.format(
              "  \"local_connection_preserved_after_received_prune\": %d,\n",
              localConnectionPreservedAfterReceivedPrune));
      writer.write(
          String.format(
              "  \"local_connection_preserved_after_local_oversubscription_prune\": %d,\n",
              localConnectionPreservedAfterLocalOversubscriptionPrune));
      writer.write(
          String.format(
              "  \"local_connection_preserved_after_local_low_score_prune\": %d,\n",
              localConnectionPreservedAfterLocalLowScorePrune));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_survived_after_prune\": %d,\n",
              localConnectedNonMeshSurvivedAfterPrune));
      writer.write(
          String.format(
              "  \"local_mesh_removed_but_still_connected_count\": %d,\n",
              localMeshRemovedButStillConnectedCount));
      writer.write(
          String.format("  \"local_connected_px_fresh_mean\": %s,\n", localConnectedPxFreshMean));
      writer.write(
          String.format(
              "  \"local_connected_mesh_fresh_mean\": %s,\n", localConnectedMeshFreshMean));
      writer.write(
          String.format(
              "  \"local_connected_fanout_fresh_mean\": %s,\n", localConnectedFanoutFreshMean));
      writer.write(
          String.format(
              "  \"local_connected_reserve_fresh_mean\": %s,\n", localConnectedReserveFreshMean));
      writer.write(
          String.format(
              "  \"local_connected_reserve_registry_only_mean\": %s,\n",
              localConnectedReserveRegistryOnlyMean));
      writer.write(
          String.format(
              "  \"local_known_retained_after_disconnect_mean\": %s,\n",
              localKnownRetainedAfterDisconnectMean));
      writer.write(
          String.format("  \"local_connected_px_only_mean\": %s,\n", localConnectedPxOnlyMean));
      writer.write(String.format("  \"local_known_px_only_mean\": %s,\n", localKnownPxOnlyMean));
      writer.write(
          String.format(
              "  \"local_px_retained_known_only_mean\": %s,\n", localPxRetainedKnownOnlyMean));
      writer.write(
          String.format(
              "  \"local_outbound_connected_peer_mean\": %s,\n", localOutboundConnectedPeerMean));
      writer.write(
          String.format(
              "  \"local_inbound_connected_peer_mean\": %s,\n", localInboundConnectedPeerMean));
      writer.write(
          String.format(
              "  \"local_disconnected_known_peer_mean\": %s,\n", localDisconnectedKnownPeerMean));
      writer.write(
          String.format(
              "  \"local_registry_bootstrap_added\": %d,\n", localRegistryBootstrapAdded));
      writer.write(
          String.format(
              "  \"local_catalog_expired_registry_removed\": %d,\n",
              localCatalogExpiredRegistryRemoved));
      writer.write(
          String.format(
              "  \"local_catalog_expired_known_removed\": %d,\n", localCatalogExpiredKnownRemoved));
      writer.write(
          String.format(
              "  \"local_catalog_expired_mesh_repair_removed\": %d,\n",
              localCatalogExpiredMeshRepairRemoved));
      writer.write(
          String.format(
              "  \"local_catalog_expired_connected_removed\": %d,\n",
              localCatalogExpiredConnectedRemoved));
      writer.write(String.format("  \"local_connect_attempts\": %d,\n", localConnectAttempts));
      writer.write(String.format("  \"local_connect_successes\": %d,\n", localConnectSuccesses));
      writer.write(String.format("  \"local_connect_failures\": %d,\n", localConnectFailures));
      writer.write(
          String.format(
              "  \"local_connect_attempts_from_direct\": %d,\n", localConnectAttemptsFromDirect));
      writer.write(
          String.format("  \"local_connect_attempts_from_px\": %d,\n", localConnectAttemptsFromPx));
      writer.write(
          String.format(
              "  \"local_connect_attempts_from_mesh_repair\": %d,\n",
              localConnectAttemptsFromMeshRepair));
      writer.write(
          String.format(
              "  \"local_connect_successes_from_direct\": %d,\n", localConnectSuccessesFromDirect));
      writer.write(
          String.format(
              "  \"local_connect_successes_from_px\": %d,\n", localConnectSuccessesFromPx));
      writer.write(
          String.format(
              "  \"local_connect_successes_from_mesh_repair\": %d,\n",
              localConnectSuccessesFromMeshRepair));
      writer.write(
          String.format(
              "  \"local_connect_attempts_for_reserve\": %d,\n", localConnectAttemptsForReserve));
      writer.write(
          String.format(
              "  \"local_connect_successes_for_reserve\": %d,\n", localConnectSuccessesForReserve));
      writer.write(
          String.format(
              "  \"local_connect_failures_for_reserve\": %d,\n", localConnectFailuresForReserve));
      writer.write(
          String.format(
              "  \"local_reserve_connected_promoted_to_mesh\": %d,\n",
              localReserveConnectedPromotedToMesh));
      writer.write(
          String.format(
              "  \"local_reserve_connected_consumed_by_mesh\": %d,\n",
              localReserveConnectedConsumedByMesh));
      writer.write(
          String.format(
              "  \"local_reserve_connected_consumed_before_maturity\": %d,\n",
              localReserveConnectedConsumedBeforeMaturity));
      writer.write(
          String.format(
              "  \"local_reserve_connected_disconnected_before_promotion\": %d,\n",
              localReserveConnectedDisconnectedBeforePromotion));
      writer.write(
          String.format(
              "  \"local_reserve_connected_disconnected_before_maturity\": %d,\n",
              localReserveConnectedDisconnectedBeforeMaturity));
      writer.write(
          String.format(
              "  \"local_reserve_connected_disconnected_after_maturity\": %d,\n",
              localReserveConnectedDisconnectedAfterMaturity));
      writer.write(
          String.format(
              "  \"local_reserve_connected_mean_lifetime\": %s,\n",
              localReserveConnectedMeanLifetime));
      writer.write(
          String.format(
              "  \"local_reserve_connected_mean_lifetime_before_promotion\": %s,\n",
              localReserveConnectedMeanLifetimeBeforePromotion));
      writer.write(
          String.format(
              "  \"local_mesh_selection_skipped_reserved_peer\": %d,\n",
              localMeshSelectionSkippedReservedPeer));
      writer.write(
          String.format("  \"local_px_promoted_to_connected\": %d,\n", localPxPromotedToConnected));
      writer.write(
          String.format("  \"local_px_admission_attempts\": %d,\n", localPxAdmissionAttempts));
      writer.write(
          String.format("  \"local_px_admission_successes\": %d,\n", localPxAdmissionSuccesses));
      writer.write(
          String.format(
              "  \"local_px_admission_rejections_total\": %d,\n", localPxAdmissionRejectionsTotal));
      writer.write(
          String.format(
              "  \"local_connect_retry_backoff_active_mean\": %s,\n",
              localConnectRetryBackoffActiveMean));
      writer.write(
          String.format(
              "  \"local_connection_transition_outbound\": %d,\n",
              localConnectionTransitionOutbound));
      writer.write(
          String.format(
              "  \"local_connection_transition_inbound\": %d,\n",
              localConnectionTransitionInbound));
      writer.write(
          String.format(
              "  \"local_connection_transition_disconnected\": %d,\n",
              localConnectionTransitionDisconnected));
      writer.write(
          String.format(
              "  \"local_connection_disconnected_by_received_prune\": %d,\n",
              localConnectionDisconnectedByReceivedPrune));
      writer.write(
          String.format(
              "  \"local_connection_disconnected_by_local_prune\": %d,\n",
              localConnectionDisconnectedByLocalPrune));
      writer.write(
          String.format(
              "  \"local_connection_disconnected_by_score\": %d,\n",
              localConnectionDisconnectedByScore));
      writer.write(
          String.format(
              "  \"local_connection_disconnected_by_capacity\": %d,\n",
              localConnectionDisconnectedByCapacity));
      writer.write(
          String.format(
              "  \"local_connection_disconnected_by_idle\": %d,\n",
              localConnectionDisconnectedByIdle));
      writer.write(
          String.format(
              "  \"local_connection_disconnected_by_expiry\": %d,\n",
              localConnectionDisconnectedByExpiry));
      writer.write(
          String.format(
              "  \"local_connection_disconnected_by_other\": %d,\n",
              localConnectionDisconnectedByOther));
      writer.write(
          String.format(
              "  \"local_connected_stale_drop_count\": %d,\n", localConnectedStaleDropCount));
      writer.write(
          String.format("  \"local_connection_lifetime_sum\": %d,\n", localConnectionLifetimeSum));
      writer.write(
          String.format(
              "  \"local_connection_lifetime_count\": %d,\n", localConnectionLifetimeCount));
      writer.write(
          String.format(
              "  \"local_connection_lifetime_mean\": %s,\n", localConnectionLifetimeMean));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_lifetime_mean\": %s,\n",
              localConnectedNonMeshLifetimeMean));
      writer.write(
          String.format(
              "  \"local_connected_non_mesh_mean_lifetime\": %s,\n",
              localConnectedNonMeshLifetimeMean));
      writer.write(
          String.format(
              "  \"local_connection_connected_lifetime_count\": %d,\n",
              localConnectionLifetimeCount));
      writer.write(
          String.format(
              "  \"local_connection_connected_lifetime_mean\": %s,\n",
              localConnectionLifetimeMean));
      writer.write(
          String.format(
              "  \"local_connection_reconnect_same_peer_within_5_heartbeats\": %d,\n",
              localConnectionReconnectSamePeerWithin5Heartbeats));
      writer.write(
          String.format(
              "  \"local_connection_reconnect_same_peer_within_10_heartbeats\": %d,\n",
              localConnectionReconnectSamePeerWithin10Heartbeats));
      writer.write(
          String.format(
              "  \"local_connection_idle_disconnects\": %d,\n", localConnectionDisconnectedByIdle));
      writer.write(
          String.format(
              "  \"local_connection_idle_disconnects_before_min_lifetime\": %d,\n",
              localConnectionIdleDisconnectsBeforeMinLifetime));
      writer.write(
          String.format(
              "  \"local_connection_idle_disconnects_after_min_lifetime\": %d,\n",
              localConnectionIdleDisconnectsAfterMinLifetime));
      writer.write(
          String.format(
              "  \"local_connected_disconnection_blocked_by_hold\": %d,\n",
              localConnectionDisconnectionBlockedByHold));
      writer.write(
          String.format(
              "  \"local_connected_hold_expired_disconnects\": %d,\n",
              localConnectionHoldExpiredDisconnects));
      writer.write(
          String.format(
              "  \"local_connection_consecutive_idle_threshold_hits\": %d,\n",
              localConnectionConsecutiveIdleThresholdHits));
      writer.write(
          String.format(
              "  \"local_connection_retry_delay_mean\": %s,\n", localConnectionRetryDelayMean));
      writer.write(
          String.format(
              "  \"local_connection_retry_delay_max\": %d,\n", localConnectionRetryDelayMax));
      writer.write(
          String.format(
              "  \"local_connection_flapping_peer_count\": %d,\n",
              localConnectionFlappingPeerCount));
      writer.write(
          String.format("  \"repair_filter_selection_calls\": %d,\n", repairFilterSelectionCalls));
      writer.write(
          String.format(
              "  \"repair_filter_candidate_pool_raw_total\": %d,\n",
              repairFilterCandidatePoolRawTotal));
      writer.write(
          String.format(
              "  \"repair_filter_candidate_pool_empty_count\": %d,\n",
              repairFilterCandidatePoolEmptyCount));
      writer.write(
          String.format(
              "  \"repair_filter_rejected_by_backoff\": %d,\n", repairFilterRejectedByBackoff));
      writer.write(
          String.format(
              "  \"repair_filter_rejected_by_score\": %d,\n", repairFilterRejectedByScore));
      writer.write(
          String.format(
              "  \"repair_filter_rejected_by_already_in_mesh\": %d,\n",
              repairFilterRejectedByAlreadyInMesh));
      writer.write(
          String.format(
              "  \"repair_filter_rejected_by_excluded\": %d,\n", repairFilterRejectedByExcluded));
      writer.write(
          String.format(
              "  \"repair_filter_no_candidate_after_filtering\": %d,\n",
              repairFilterNoCandidateAfterFiltering));
      writer.write(
          String.format(
              "  \"prune_backoff_set_from_received_prune\": %d,\n",
              pruneBackoffSetFromReceivedPrune));
      writer.write(
          String.format(
              "  \"prune_backoff_set_from_oversubscription_prune\": %d,\n",
              pruneBackoffSetFromOversubscriptionPrune));
      writer.write(
          String.format(
              "  \"prune_backoff_set_from_low_score_prune\": %d,\n",
              pruneBackoffSetFromLowScorePrune));
      writer.write(
          String.format(
              "  \"prune_backoff_set_from_graft_rejection\": %d,\n",
              pruneBackoffSetFromGraftRejection));
      writer.write(
          String.format(
              "  \"repair_rejected_by_backoff_from_received_prune\": %d,\n",
              repairRejectedByBackoffFromReceivedPrune));
      writer.write(
          String.format(
              "  \"repair_rejected_by_backoff_from_oversubscription_prune\": %d,\n",
              repairRejectedByBackoffFromOversubscriptionPrune));
      writer.write(
          String.format(
              "  \"repair_rejected_by_backoff_from_low_score_prune\": %d,\n",
              repairRejectedByBackoffFromLowScorePrune));
      writer.write(
          String.format(
              "  \"repair_rejected_by_backoff_from_graft_rejection\": %d,\n",
              repairRejectedByBackoffFromGraftRejection));
      writer.write(
          String.format(
              "  \"repair_candidates_blocked_by_backoff_positive_score\": %d,\n",
              repairCandidatesBlockedByBackoffPositiveScore));
      writer.write(
          String.format(
              "  \"repair_candidates_blocked_by_backoff_negative_score\": %d,\n",
              repairCandidatesBlockedByBackoffNegativeScore));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_considered\": %d,\n",
              repairRelaxedReceivedPruneConsidered));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_selected\": %d,\n",
              repairRelaxedReceivedPruneSelected));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_successes\": %d,\n",
              repairRelaxedReceivedPruneSuccesses));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_positive_score_selected\": %d,\n",
              repairRelaxedReceivedPrunePositiveScoreSelected));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_negative_score_selected\": %d,\n",
              repairRelaxedReceivedPruneNegativeScoreSelected));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_blocked_by_budget\": %d,\n",
              repairRelaxedReceivedPruneBlockedByBudget));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_blocked_not_post_warmup\": %d,\n",
              repairRelaxedReceivedPruneBlockedNotPostWarmup));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_blocked_no_dout_deficit\": %d,\n",
              repairRelaxedReceivedPruneBlockedNoDOutDeficit));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_grafted\": %d,\n",
              repairRelaxedReceivedPruneGrafted));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_graft_accepted\": %d,\n",
              repairRelaxedReceivedPruneGraftAccepted));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_graft_rejected\": %d,\n",
              repairRelaxedReceivedPruneGraftRejected));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_pruned\": %d,\n",
              repairRelaxedReceivedPrunePruned));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_backoff_reset\": %d,\n",
              repairRelaxedReceivedPruneBackoffReset));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_survived_1_heartbeat\": %d,\n",
              repairRelaxedReceivedPruneSurvived1Heartbeat));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_survived_5_heartbeats\": %d,\n",
              repairRelaxedReceivedPruneSurvived5Heartbeats));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_survived_10_heartbeats\": %d,\n",
              repairRelaxedReceivedPruneSurvived10Heartbeats));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_local_grace_active\": %d,\n",
              repairRelaxedReceivedPruneLocalGraceActive));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_local_prune_prevented_low_score\": %d,\n",
              repairRelaxedReceivedPruneLocalPrunePreventedLowScore));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_local_prune_prevented_oversubscription\": %d,\n",
              repairRelaxedReceivedPruneLocalPrunePreventedOversubscription));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_removed_by_received_prune\": %d,\n",
              repairRelaxedReceivedPruneRemovedByReceivedPrune));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_removed_by_local_low_score\": %d,\n",
              repairRelaxedReceivedPruneRemovedByLocalLowScore));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_removed_by_local_oversubscription\": %d,\n",
              repairRelaxedReceivedPruneRemovedByLocalOversubscription));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_removed_by_other\": %d,\n",
              repairRelaxedReceivedPruneRemovedByOther));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_reason_graft_rejection_at_capacity\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_reason_graft_rejection_score\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_reason_graft_rejection_backoff\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_reason_low_score\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneReasonLowScore));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_reason_oversubscription\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneReasonOversubscription));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_reason_leave_or_unsubscribe\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_reason_other\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneReasonOther));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_after_graft\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneAfterGraft));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_received_prune_before_survived_1_heartbeat\": %d,\n",
              repairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_selected_prune_age_sum\": %d,\n",
              repairRelaxedReceivedPruneSelectedPruneAgeSum));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_selected_prune_age_count\": %d,\n",
              repairRelaxedReceivedPruneSelectedPruneAgeCount));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_remote_backoff_rejection_prune_age_sum\": %d,\n",
              repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_remote_backoff_rejection_prune_age_count\": %d,\n",
              repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_selected_prune_age_lt_quarter_backoff\": %d,\n",
              repairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_selected_prune_age_quarter_to_half_backoff\": %d,\n",
              repairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_selected_prune_age_half_to_three_quarters_backoff\": %d,\n",
              repairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_selected_prune_age_ge_three_quarters_backoff\": %d,\n",
              repairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_remote_backoff_rejection_prune_age_lt_quarter_backoff\": %d,\n",
              repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_remote_backoff_rejection_prune_age_quarter_to_half_backoff\": %d,\n",
              repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_remote_backoff_rejection_prune_age_half_to_three_quarters_backoff\": %d,\n",
              repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"repair_relaxed_received_prune_remote_backoff_rejection_prune_age_ge_three_quarters_backoff\": %d,\n",
              repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_candidates_seen\": %d,\n",
              repairRelaxReceivedPrunePrefilterCandidatesSeen));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_age_known\": %d,\n",
              repairRelaxReceivedPrunePrefilterAgeKnown));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_age_unknown\": %d,\n",
              repairRelaxReceivedPrunePrefilterAgeUnknown));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_pass_score\": %d,\n",
              repairRelaxReceivedPrunePrefilterPassScore));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_blocked_by_age\": %d,\n",
              repairRelaxReceivedPrunePrefilterBlockedByAge));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_age_lt_quarter_backoff\": %d,\n",
              repairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_age_quarter_to_half_backoff\": %d,\n",
              repairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_age_half_to_three_quarters_backoff\": %d,\n",
              repairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"repair_relax_received_prune_prefilter_age_ge_three_quarters_backoff\": %d,\n",
              repairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_considered\": %d,\n",
              receiverRelaxGraftBackoffConsidered));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_selected\": %d,\n",
              receiverRelaxGraftBackoffSelected));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_accepted\": %d,\n",
              receiverRelaxGraftBackoffAccepted));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_blocked_by_budget\": %d,\n",
              receiverRelaxGraftBackoffBlockedByBudget));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_blocked_by_score\": %d,\n",
              receiverRelaxGraftBackoffBlockedByScore));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_blocked_by_age\": %d,\n",
              receiverRelaxGraftBackoffBlockedByAge));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_blocked_not_dout\": %d,\n",
              receiverRelaxGraftBackoffBlockedNotDOut));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_blocked_other_reason\": %d,\n",
              receiverRelaxGraftBackoffBlockedOtherReason));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_accepted_age_lt_quarter_backoff\": %d,\n",
              receiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_accepted_age_quarter_to_half_backoff\": %d,\n",
              receiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_accepted_age_half_to_three_quarters_backoff\": %d,\n",
              receiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_accepted_age_ge_three_quarters_backoff\": %d,\n",
              receiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_candidates_encountered\": %d,\n",
              receiverRelaxGraftBackoffPrefilterCandidatesEncountered));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_age_known\": %d,\n",
              receiverRelaxGraftBackoffPrefilterAgeKnown));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_age_unknown\": %d,\n",
              receiverRelaxGraftBackoffPrefilterAgeUnknown));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_pass_score\": %d,\n",
              receiverRelaxGraftBackoffPrefilterPassScore));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_blocked_by_age\": %d,\n",
              receiverRelaxGraftBackoffPrefilterBlockedByAge));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_age_lt_quarter_backoff\": %d,\n",
              receiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_age_quarter_to_half_backoff\": %d,\n",
              receiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_age_half_to_three_quarters_backoff\": %d,\n",
              receiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_prefilter_age_ge_three_quarters_backoff\": %d,\n",
              receiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_sum\": %.6f,\n",
              receiverRelaxGraftBackoffScoreSum));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_count\": %d,\n",
              receiverRelaxGraftBackoffScoreCount));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_min\": %.6f,\n",
              receiverRelaxGraftBackoffScoreCount > 0 ? receiverRelaxGraftBackoffScoreMin : 0.0));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_max\": %.6f,\n",
              receiverRelaxGraftBackoffScoreCount > 0 ? receiverRelaxGraftBackoffScoreMax : 0.0));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_lt_threshold_minus_one\": %d,\n",
              receiverRelaxGraftBackoffScoreLtThresholdMinusOne));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_threshold_minus_one_to_minus_half\": %d,\n",
              receiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_threshold_minus_half_to_threshold\": %d,\n",
              receiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_threshold_to_threshold_plus_half\": %d,\n",
              receiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf));
      writer.write(
          String.format(
              "  \"receiver_relax_graft_backoff_score_ge_threshold_plus_half\": %d,\n",
              receiverRelaxGraftBackoffScoreGeThresholdPlusHalf));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_count\": %d,\n", pxSenderTrustDecompositionCount));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_total_score_sum\": %.6f,\n",
              pxSenderTrustDecompositionTotalScoreSum));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_threshold_sum\": %.6f,\n",
              pxSenderTrustDecompositionThresholdSum));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_score_minus_threshold_sum\": %.6f,\n",
              pxSenderTrustDecompositionScoreMinusThresholdSum));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_time_in_mesh_contribution_sum\": %.6f,\n",
              pxSenderTrustDecompositionTimeInMeshContributionSum));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_first_deliveries_contribution_sum\": %.6f,\n",
              pxSenderTrustDecompositionFirstDeliveriesContributionSum));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_local_invalid_deliveries_penalty_sum\": %.6f,\n",
              pxSenderTrustDecompositionLocalInvalidDeliveriesPenaltySum));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_local_broken_promises_penalty_sum\": %.6f,\n",
              pxSenderTrustDecompositionLocalBrokenPromisesPenaltySum));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_global_invalid_deliveries_penalty_sum\": %.6f,\n",
              pxSenderTrustDecompositionGlobalInvalidDeliveriesPenaltySum));
      writer.write(
          String.format(
              "  \"px_sender_trust_decomposition_global_broken_promises_penalty_sum\": %.6f,\n",
              pxSenderTrustDecompositionGlobalBrokenPromisesPenaltySum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_count\": %d,\n",
              receiverGraftScoreRejectionDecompositionCount));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_total_score_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionTotalScoreSum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_mesh_threshold_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionMeshThresholdSum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_score_minus_mesh_threshold_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionScoreMinusMeshThresholdSum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_time_in_mesh_contribution_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionTimeInMeshContributionSum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_first_deliveries_contribution_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionFirstDeliveriesContributionSum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_local_invalid_deliveries_penalty_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionLocalInvalidDeliveriesPenaltySum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_local_broken_promises_penalty_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionLocalBrokenPromisesPenaltySum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_global_invalid_deliveries_penalty_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionGlobalInvalidDeliveriesPenaltySum));
      writer.write(
          String.format(
              "  \"receiver_graft_score_rejection_decomposition_global_broken_promises_penalty_sum\": %.6f,\n",
              receiverGraftScoreRejectionDecompositionGlobalBrokenPromisesPenaltySum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_count\": %d,\n",
              remotePruneOutcomeDecompositionCount));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_total_score_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionTotalScoreSum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_mesh_threshold_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionMeshThresholdSum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_score_minus_mesh_threshold_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionScoreMinusMeshThresholdSum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_time_in_mesh_contribution_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionTimeInMeshContributionSum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_first_deliveries_contribution_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionFirstDeliveriesContributionSum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_local_invalid_deliveries_penalty_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionLocalInvalidDeliveriesPenaltySum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_local_broken_promises_penalty_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionLocalBrokenPromisesPenaltySum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_global_invalid_deliveries_penalty_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionGlobalInvalidDeliveriesPenaltySum));
      writer.write(
          String.format(
              "  \"remote_prune_outcome_decomposition_global_broken_promises_penalty_sum\": %.6f,\n",
              remotePruneOutcomeDecompositionGlobalBrokenPromisesPenaltySum));
      writer.write(
          String.format("  \"log_folder\": \"%s\"\n", escapeJson(getCurrentLogFolderName())));
      writer.write("}\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void writeScoreSummary() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < Network.size(); i++) {
      Node n = Network.get(i);
      Object protocol = n.getProtocol(kademliaid);
      if (!(protocol instanceof GossipSubProtocol)) continue;
      GossipSubProtocol gossipProtocol = (GossipSubProtocol) protocol;
      rows.addAll(gossipProtocol.getScoreSummaryRows());
    }

    rows.sort(
        Comparator.comparing((Map<String, Object> row) -> row.get("node_id").toString())
            .thenComparing(row -> row.get("topic").toString())
            .thenComparing(row -> row.get("peerId").toString()));

    try (FileWriter writer =
        new FileWriter(getCurrentLogFolderName() + "/" + "score_summary.csv")) {
      writer.write(
          "node_id,topic,peerId,score_final,meshHeartbeats,firstDeliveries,brokenPromises,degradedPeer,peerIsDegraded\n");
      for (Map<String, Object> row : rows) {
        writer.write(
            String.format(
                "%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                row.get("node_id"),
                row.get("topic"),
                row.get("peerId"),
                row.get("score_final"),
                row.get("meshHeartbeats"),
                row.get("firstDeliveries"),
                row.get("brokenPromises"),
                row.get("degradedPeer"),
                row.get("peerIsDegraded")));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String escapeJson(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
