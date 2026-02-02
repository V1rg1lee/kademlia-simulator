package peersim.kademlia.gossipsub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.kademlia.KademliaNode;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.Message;
import peersim.kademlia.SimpleEvent;
import peersim.kademlia.das.Sample;
import peersim.kademlia.gossipsub.inference.passive.PassiveCoalitionRegistry;
import peersim.kademlia.gossipsub.inference.passive.PassiveEgoMeshRegistry;
import peersim.kademlia.gossipsub.inference.px.PxFloodRegistry;
import peersim.transport.Transport;

public class GossipSubProtocol implements Cloneable, EDProtocol {

  /** Prefix for configuration parameters. */
  protected static String prefix = null;

  /** UnreliableTransport object used for communication. */
  protected Transport transport;

  /** The parameter name for transport. */
  private static final String PAR_TRANSPORT = "transport";

  private static final long D_OUT_ESCALATION_HEARTBEATS = 8L;
  private static final long D_OUT_ZERO_OUTBOUND_RESEED_HEARTBEATS = 8L;

  /** Identifier for the tranport protocol (used in the sendMessage method) */
  protected int tid;

  /** Unique ID for this Kademlia node/network */
  protected int gossipid;

  /** Indicates if the service initializer has already been called. */
  private static boolean _ALREADY_INSTALLED = false;

  /** Kademlia node instance. */
  public KademliaNode node;

  /** Logging handler. */
  protected Logger logger;

  protected static PeerTable peers = new PeerTable();

  private BootstrapDiscoveryManager discoveryManager;

  protected HashMap<String, HashSet<BigInteger>> mesh;

  private HashMap<String, HashSet<BigInteger>> fanout;

  private HashMap<String, Long> fanoutExpirations;

  protected MCache mCache;

  protected HashMap<String, List<BigInteger>> seen;

  private static class PeerScoreState {
    double meshHeartbeats;
    double firstDeliveries;
    double invalidDeliveries;
    double brokenPromises;
  }

  private static class ScoreDecompositionSnapshot {
    double totalScore;
    double meshThreshold;
    double scoreMinusMeshThreshold;
    double timeInMeshContribution;
    double firstDeliveriesContribution;
    double localInvalidDeliveriesPenalty;
    double localBrokenPromisesPenalty;
    double globalInvalidDeliveriesPenalty;
    double globalBrokenPromisesPenalty;
  }

  private static class ScoreDecompositionAggregate {
    long count;
    double totalScoreSum;
    double meshThresholdSum;
    double scoreMinusMeshThresholdSum;
    double timeInMeshContributionSum;
    double firstDeliveriesContributionSum;
    double localInvalidDeliveriesPenaltySum;
    double localBrokenPromisesPenaltySum;
    double globalInvalidDeliveriesPenaltySum;
    double globalBrokenPromisesPenaltySum;

    void reset() {
      count = 0;
      totalScoreSum = 0.0;
      meshThresholdSum = 0.0;
      scoreMinusMeshThresholdSum = 0.0;
      timeInMeshContributionSum = 0.0;
      firstDeliveriesContributionSum = 0.0;
      localInvalidDeliveriesPenaltySum = 0.0;
      localBrokenPromisesPenaltySum = 0.0;
      globalInvalidDeliveriesPenaltySum = 0.0;
      globalBrokenPromisesPenaltySum = 0.0;
    }

    void observe(ScoreDecompositionSnapshot snapshot) {
      if (snapshot == null) return;
      count++;
      totalScoreSum += snapshot.totalScore;
      meshThresholdSum += snapshot.meshThreshold;
      scoreMinusMeshThresholdSum += snapshot.scoreMinusMeshThreshold;
      timeInMeshContributionSum += snapshot.timeInMeshContribution;
      firstDeliveriesContributionSum += snapshot.firstDeliveriesContribution;
      localInvalidDeliveriesPenaltySum += snapshot.localInvalidDeliveriesPenalty;
      localBrokenPromisesPenaltySum += snapshot.localBrokenPromisesPenalty;
      globalInvalidDeliveriesPenaltySum += snapshot.globalInvalidDeliveriesPenalty;
      globalBrokenPromisesPenaltySum += snapshot.globalBrokenPromisesPenalty;
    }
  }

  private static class TopicPeerKey {
    String topic;
    BigInteger peerId;

    TopicPeerKey(String topic, BigInteger peerId) {
      this.topic = topic;
      this.peerId = peerId;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      TopicPeerKey that = (TopicPeerKey) obj;
      return topic.equals(that.topic) && peerId.equals(that.peerId);
    }

    @Override
    public int hashCode() {
      int result = topic.hashCode();
      result = 31 * result + peerId.hashCode();
      return result;
    }
  }

  private static class PromiseKey {
    String topic;
    BigInteger peerId;
    BigInteger msgId;

    PromiseKey(String topic, BigInteger peerId, BigInteger msgId) {
      this.topic = topic;
      this.peerId = peerId;
      this.msgId = msgId;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      PromiseKey that = (PromiseKey) obj;
      return topic.equals(that.topic) && peerId.equals(that.peerId) && msgId.equals(that.msgId);
    }

    @Override
    public int hashCode() {
      int result = topic.hashCode();
      result = 31 * result + peerId.hashCode();
      result = 31 * result + msgId.hashCode();
      return result;
    }
  }

  private static class PendingPromise {
    long expireAtHeartbeat;

    PendingPromise(long expireAtHeartbeat) {
      this.expireAtHeartbeat = expireAtHeartbeat;
    }
  }

  private enum MeshPeerDirection {
    INBOUND,
    OUTBOUND
  }

  private enum CandidateSource {
    KNOWN((byte) 0x01),
    PX_HINT((byte) 0x02),
    MESH_REPAIR((byte) 0x04),
    REGISTRY_BOOTSTRAP((byte) 0x08),
    AMBIENT_DISCOVERY((byte) 0x10);

    private final byte mask;

    CandidateSource(byte mask) {
      this.mask = mask;
    }
  }

  private enum PruneBackoffOrigin {
    RECEIVED_PRUNE,
    OVERSUBSCRIPTION_PRUNE,
    LOW_SCORE_PRUNE,
    GRAFT_REJECTION,
    OTHER
  }

  private enum LocalConnectionState {
    DISCONNECTED,
    INBOUND_CONNECTED,
    OUTBOUND_CONNECTED
  }

  private enum LocalConnectionTransitionReason {
    DIRECT_INBOUND_ACTIVITY,
    LOCAL_CONNECT_FOR_REPAIR,
    LOCAL_CONNECT_FOR_RESERVE,
    LOCAL_CONNECT_FOR_FANOUT,
    LOCAL_CONNECT_FOR_GOSSIP,
    OTHER
  }

  private enum LocalDisconnectReason {
    RECEIVED_PRUNE,
    LOCAL_PRUNE,
    SCORE,
    CAPACITY,
    IDLE,
    EXPIRY,
    OTHER
  }

  private enum LocalCandidateDominantSource {
    DIRECT,
    PX,
    MESH_REPAIR,
    REGISTRY,
    OTHER
  }

  private enum LocalConnectableSource {
    DIRECT,
    MESH,
    CONNECTED,
    PX,
    REGISTRY,
    OTHER
  }

  private enum LocalKnownNonConnectableSource {
    PX,
    REGISTRY,
    OTHER
  }

  private static class CandidateMeta {
    byte sourceMask;
    long lastDirectSeenHeartbeat;
    long lastPxHintHeartbeat;
    long lastSelectedHeartbeat;
    long lastRegistryBootstrapHeartbeat;
    long lastAmbientDiscoveryHeartbeat;
    LocalConnectionState connectionState;
    long connectedSinceHeartbeat;
    long holdUntilHeartbeat;
    boolean holdForReserve;
    boolean reserveConnected;
    long reserveMatureAtHeartbeat;
    long lastConnectionObservedHeartbeat;
    long lastConnectionTransitionHeartbeat;
    LocalConnectionTransitionReason lastConnectionTransitionReason;
    long lastDisconnectedHeartbeat;
    long lastDirectActivityHeartbeat;
    long lastPxActivityHeartbeat;
    long lastMeshActivityHeartbeat;
    long lastFanoutActivityHeartbeat;
    long lastConnectAttemptHeartbeat;
    long retryAfterHeartbeat;
    int consecutiveConnectFailures;
    int consecutiveIdleHeartbeats;
    int consecutiveIdleDisconnects;
    LocalDisconnectReason lastDisconnectReason;

    CandidateMeta() {
      sourceMask = 0;
      lastDirectSeenHeartbeat = -1L;
      lastPxHintHeartbeat = -1L;
      lastSelectedHeartbeat = -1L;
      lastRegistryBootstrapHeartbeat = -1L;
      lastAmbientDiscoveryHeartbeat = -1L;
      connectionState = LocalConnectionState.DISCONNECTED;
      connectedSinceHeartbeat = -1L;
      holdUntilHeartbeat = -1L;
      holdForReserve = false;
      reserveConnected = false;
      reserveMatureAtHeartbeat = -1L;
      lastConnectionObservedHeartbeat = -1L;
      lastConnectionTransitionHeartbeat = -1L;
      lastConnectionTransitionReason = null;
      lastDisconnectedHeartbeat = -1L;
      lastDirectActivityHeartbeat = -1L;
      lastPxActivityHeartbeat = -1L;
      lastMeshActivityHeartbeat = -1L;
      lastFanoutActivityHeartbeat = -1L;
      lastConnectAttemptHeartbeat = -1L;
      retryAfterHeartbeat = -1L;
      consecutiveConnectFailures = 0;
      consecutiveIdleHeartbeats = 0;
      consecutiveIdleDisconnects = 0;
      lastDisconnectReason = null;
    }
  }

  private static class RelaxedReceivedPruneLifecycle {
    long selectedAtHeartbeat;
    long lastLocalGraceObservedHeartbeat;
    RelaxedReceivedPruneRemovalCause removalCause;
    Message.PruneReasonTag firstReceivedPruneReason;
    boolean grafted;
    boolean graftAccepted;
    boolean graftRejected;
    boolean pruned;
    boolean backoffReset;
    boolean survived1Heartbeat;
    boolean survived5Heartbeats;
    boolean survived10Heartbeats;

    RelaxedReceivedPruneLifecycle(long selectedAtHeartbeat) {
      this.selectedAtHeartbeat = selectedAtHeartbeat;
      lastLocalGraceObservedHeartbeat = -1L;
      removalCause = null;
      firstReceivedPruneReason = null;
      grafted = false;
      graftAccepted = false;
      graftRejected = false;
      pruned = false;
      backoffReset = false;
      survived1Heartbeat = false;
      survived5Heartbeats = false;
      survived10Heartbeats = false;
    }
  }

  private enum RelaxedReceivedPruneRemovalCause {
    RECEIVED_PRUNE,
    LOCAL_LOW_SCORE,
    LOCAL_OVERSUBSCRIPTION,
    OTHER
  }

  private HashMap<TopicPeerKey, PeerScoreState> topicPeerScores;
  private HashMap<BigInteger, PeerScoreState> globalPenaltyScores;
  private HashMap<PromiseKey, PendingPromise> pendingPromises;
  private HashMap<TopicPeerKey, Long> pruneBackoffs;
  private HashMap<TopicPeerKey, Long> lastReceivedPruneHeartbeats;
  private HashMap<TopicPeerKey, PruneBackoffOrigin> pruneBackoffOrigins;
  private HashMap<TopicPeerKey, MeshPeerDirection> meshPeerDirections;
  private HashMap<String, HashSet<BigInteger>> pxPeerHints;
  private HashMap<TopicPeerKey, CandidateMeta> topicCandidates;
  private HashSet<String> dOutWarmTopics;
  private HashMap<String, Long> dOutConsecutiveDeficitHeartbeatsByTopic;
  private HashMap<String, Long> dOutConsecutiveZeroOutboundHeartbeatsByTopic;
  private HashMap<String, Long> dOutConsecutiveZeroRemainingDeficitByTopic;
  private HashMap<String, Integer> repairRelaxReceivedPruneUsedByTopic;
  private HashMap<String, Integer> receiverRelaxGraftBackoffUsedByTopic;
  private HashMap<String, Integer> localConnectAttemptsUsedByTopic;
  private HashMap<String, Integer> localPxAdmissionUsedByTopic;
  private HashMap<TopicPeerKey, Long> lastPxAdvertisedHeartbeatByTopicPeer;
  // PX flood defense state (per-node)
  private HashMap<TopicPeerKey, Long> pxRequesterBackoffEnd;
  private HashMap<String, Long> pxRateLimitWindowStart;
  private HashMap<String, Integer> pxRateLimitBudgetUsed;
  private HashMap<String, ArrayList<Long>> inboundGraftTimestamps;
  private HashMap<String, Long> pxFloodSuppressedUntil;
  private HashMap<String, Integer> repairRelaxReceivedPrunePendingSelectionsByTopic;
  private HashMap<TopicPeerKey, RelaxedReceivedPruneLifecycle> repairRelaxReceivedPruneLifecycles;
  private long heartbeatCounter;
  private boolean degradedPeer;
  private long opportunisticGraftEvaluationCount;
  private long opportunisticGraftTriggerCount;
  private long opportunisticGraftPeerCount;
  private long pxPruneMessagesSent;
  private long pxCandidatesAdvertised;
  private long pxCandidatesSeen;
  private long pxCandidatesRejectedSenderThreshold;
  private long pxCandidatesRejectedAlreadyKnown;
  private long pxCandidatesRejectedAlreadyConnected;
  private long pxCandidatesRejectedInMesh;
  private long pxCandidatesRejectedPruneBackoff;
  private long pxCandidatesRejectedScore;
  private long pxCandidatesRejectedExpiredOrStale;
  private long pxCandidatesRejectedCapacity;
  private long pxCandidatesAdmittedToKnown;
  private long pxCandidatesPassedSenderThreshold;
  private long pxCandidatesAdmittedAndLaterConnected;
  private long pxCandidatesAccepted;
  private long pxCandidatesRejected;
  private long pxAcceptedFromHighScoreSenders;
  private long pxPeersSkippedRecentlyAdvertised;
  private long pxPeersSelectedAfterNoveltyFiltering;
  private long pxSendersSeen;
  private long pxSendersAboveAcceptThreshold;
  private long pxSendersBelowAcceptThreshold;
  private double pxSendersScoreSum;
  private long pxSendersScoreCount;
  private double pxSendersScoreMin;
  private double pxSendersScoreMax;
  private long pxSendersScoreLtThresholdMinusOne;
  private long pxSendersScoreThresholdMinusOneToMinusHalf;
  private long pxSendersScoreThresholdMinusHalfToThreshold;
  private long pxSendersScoreGeThreshold;
  private long dOutGraftsAcceptedAtCapacity;
  private long dOutGraftsRejectedAtCapacity;
  private long dOutOutboundMeshPeerSum;
  private long dOutOutboundMeshPeerObservationCount;
  private long dOutOutboundMeshPeerMinObserved;
  private long dOutHeartbeatDeficitSum;
  private long dOutHeartbeatDeficitNonzeroCount;
  private long dOutRepairAttemptsForDeficit;
  private long dOutRepairSuccesses;
  private long dOutSteadyStateTopupAttempts;
  private long dOutSteadyStateTopupSuccesses;
  private long dOutSteadyStateReplacements;
  private long dOutSteadyStatePrunes;
  private long dOutSteadyStateGrafts;
  private long dOutHeartbeatObservationCount;
  private long dOutZeroOutboundObservationCount;
  private long dOutDeficitObservationCount;
  private long dOutPostWarmupObservationCount;
  private long dOutPostWarmupZeroOutboundCount;
  private long dOutPostWarmupDeficitCount;
  private long dOutPostWarmupOutboundMeshPeerMinObserved;
  private long dOutMaxConsecutiveDeficitHeartbeats;
  private long dOutMaxConsecutiveZeroOutboundHeartbeats;
  private long dOutEscalationAttempts;
  private long dOutEscalationSuccesses;
  private long dOutZeroOutboundEmergencyAttempts;
  private long dOutZeroOutboundEmergencySuccesses;
  private long dOutInplacePromotionAttempts;
  private long dOutInplacePromotionSuccesses;
  private long dOutInplaceZeroOutboundPromotions;
  private long dOutInplaceDeficitPromotions;
  private long dOutForcedReseedAttempts;
  private long dOutForcedReseedSuccesses;
  private long dOutForcedReseedGrafts;
  private long dOutForcedReseedPrunes;
  private long dOutForcedReseedSkippedNoCandidate;
  private long dOutForcedReseedSkippedNoPrunableInbound;
  private long dOutForcedReseedEligibleCount;
  private long dOutForcedReseedBlockedNotPostWarmup;
  private long dOutForcedReseedBlockedConsecutiveZeroBelowThreshold;
  private long dOutForcedReseedBlockedOutboundBeforePositive;
  private long dOutForcedReseedBlockedDeficitBeforeZero;
  private long dOutForcedReseedBlockedAfterInplaceNoRemainingDeficit;
  private long dOutForcedReseedBlockedAfterInplaceNotPersistent;
  private long dOutForcedReseedReachedCallsiteCount;
  private long dOutForcedReseedZeroOutboundEmergencyCount;
  private long dOutForcedReseedInplaceSuccessDuringZeroOutboundCount;
  private long dOutForcedReseedAfterInplaceRemainingDeficitCount;
  private long dOutForcedReseedReturnedBeforeCallsiteCount;
  private long dOutForcedReseedEligibleByDeficit;
  private long dOutForcedReseedReachedCallsiteAfterInplace;
  private long dOutForcedReseedEligibleByZeroRemainingDeficit;
  private long dOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit;
  private long dOutMaxConsecutiveZeroRemainingDeficitHeartbeats;
  private long candidatePoolActiveSum;
  private long candidatePoolKnownActiveSum;
  private long candidatePoolPxActiveSum;
  private long candidatePoolMeshRepairActiveSum;
  private long candidatePoolObservationCount;
  private long candidatePoolExpiredRemoved;
  private long candidatePoolPxExpiredRemoved;
  private long candidatePoolCappedRemoved;
  private long candidatePoolBackoffFiltered;
  private long candidatePoolScoreFiltered;
  private long candidatePoolEmptySelectionCount;
  private long repairFilterSelectionCalls;
  private long repairFilterCandidatePoolRawTotal;
  private long repairFilterCandidatePoolEmptyCount;
  private long repairFilterRejectedByBackoff;
  private long repairFilterRejectedByScore;
  private long repairFilterRejectedByAlreadyInMesh;
  private long repairFilterRejectedByExcluded;
  private long repairFilterNoCandidateAfterFiltering;
  private long pruneBackoffSetFromReceivedPrune;
  private long pruneBackoffSetFromOversubscriptionPrune;
  private long pruneBackoffSetFromLowScorePrune;
  private long pruneBackoffSetFromGraftRejection;
  private long repairRejectedByBackoffFromReceivedPrune;
  private long repairRejectedByBackoffFromOversubscriptionPrune;
  private long repairRejectedByBackoffFromLowScorePrune;
  private long repairRejectedByBackoffFromGraftRejection;
  private long repairCandidatesBlockedByBackoffPositiveScore;
  private long repairCandidatesBlockedByBackoffNegativeScore;
  private long repairRelaxedReceivedPruneConsidered;
  private long repairRelaxedReceivedPruneSelected;
  private long repairRelaxedReceivedPruneSuccesses;
  private long repairRelaxedReceivedPrunePositiveScoreSelected;
  private long repairRelaxedReceivedPruneNegativeScoreSelected;
  private long repairRelaxedReceivedPruneBlockedByBudget;
  private long repairRelaxedReceivedPruneBlockedNotPostWarmup;
  private long repairRelaxedReceivedPruneBlockedNoDOutDeficit;
  private long repairRelaxedReceivedPruneGrafted;
  private long repairRelaxedReceivedPruneGraftAccepted;
  private long repairRelaxedReceivedPruneGraftRejected;
  private long repairRelaxedReceivedPrunePruned;
  private long repairRelaxedReceivedPruneBackoffReset;
  private long repairRelaxedReceivedPruneSurvived1Heartbeat;
  private long repairRelaxedReceivedPruneSurvived5Heartbeats;
  private long repairRelaxedReceivedPruneSurvived10Heartbeats;
  private long repairRelaxedReceivedPruneLocalGraceActive;
  private long repairRelaxedReceivedPruneLocalPrunePreventedLowScore;
  private long repairRelaxedReceivedPruneLocalPrunePreventedOversubscription;
  private long repairRelaxedReceivedPruneRemovedByReceivedPrune;
  private long repairRelaxedReceivedPruneRemovedByLocalLowScore;
  private long repairRelaxedReceivedPruneRemovedByLocalOversubscription;
  private long repairRelaxedReceivedPruneRemovedByOther;
  private long repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity;
  private long repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore;
  private long repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff;
  private long repairRelaxedReceivedPruneReceivedPruneReasonLowScore;
  private long repairRelaxedReceivedPruneReceivedPruneReasonOversubscription;
  private long repairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe;
  private long repairRelaxedReceivedPruneReceivedPruneReasonOther;
  private long repairRelaxedReceivedPruneReceivedPruneAfterGraft;
  private long repairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat;
  private long repairRelaxedReceivedPruneSelectedPruneAgeSum;
  private long repairRelaxedReceivedPruneSelectedPruneAgeCount;
  private long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum;
  private long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount;
  private long repairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff;
  private long repairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff;
  private long repairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff;
  private long repairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff;
  private long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff;
  private long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff;
  private long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff;
  private long repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff;
  private long repairRelaxReceivedPrunePrefilterCandidatesSeen;
  private long repairRelaxReceivedPrunePrefilterAgeKnown;
  private long repairRelaxReceivedPrunePrefilterAgeUnknown;
  private long repairRelaxReceivedPrunePrefilterPassScore;
  private long repairRelaxReceivedPrunePrefilterBlockedByAge;
  private long repairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff;
  private long repairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff;
  private long repairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff;
  private long repairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff;
  private long receiverRelaxGraftBackoffConsidered;
  private long receiverRelaxGraftBackoffSelected;
  private long receiverRelaxGraftBackoffAccepted;
  private long receiverRelaxGraftBackoffBlockedByBudget;
  private long receiverRelaxGraftBackoffBlockedByScore;
  private long receiverRelaxGraftBackoffBlockedByAge;
  private long receiverRelaxGraftBackoffBlockedNotDOut;
  private long receiverRelaxGraftBackoffBlockedOtherReason;
  private long receiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff;
  private long receiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff;
  private long receiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff;
  private long receiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff;
  private long receiverRelaxGraftBackoffPrefilterCandidatesEncountered;
  private long receiverRelaxGraftBackoffPrefilterAgeKnown;
  private long receiverRelaxGraftBackoffPrefilterAgeUnknown;
  private long receiverRelaxGraftBackoffPrefilterPassScore;
  private long receiverRelaxGraftBackoffPrefilterBlockedByAge;
  private long receiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff;
  private long receiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff;
  private long receiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff;
  private long receiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff;
  private double receiverRelaxGraftBackoffScoreSum;
  private long receiverRelaxGraftBackoffScoreCount;
  private double receiverRelaxGraftBackoffScoreMin;
  private double receiverRelaxGraftBackoffScoreMax;
  private long receiverRelaxGraftBackoffScoreLtThresholdMinusOne;
  private long receiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf;
  private long receiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold;
  private long receiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf;
  private long receiverRelaxGraftBackoffScoreGeThresholdPlusHalf;
  private long candidatePoolRegistryActiveSum;
  private long localTopologyObservationCount;
  private long localKnownPeerSum;
  private long localConnectablePeerSum;
  private long localConnectedPeerSum;
  private long localConnectedNonMeshPeerSum;
  private long localReserveConnectedPeerSum;
  private long localReserveConnectedImmatureSum;
  private long localReserveConnectedMatureSum;
  private double localMeshFractionOfConnectedSum;
  private long localConnectedHoldActiveSum;
  private long localConnectedHoldForReserveSum;
  private long localKnownOnlyPeerSum;
  private long localKnownNonConnectablePeerSum;
  private long localConnectableButDisconnectedPeerSum;
  private long localKnownFromPxSum;
  private long localKnownFromDirectSum;
  private long localPxKnownFreshSum;
  private long localPxKnownStaleSum;
  private long localMeshPeerSum;
  private long localEligibleCandidateSum;
  private long localEligibleConnectableCandidateSum;
  private long localEligibleConnectedCandidateSum;
  private long localEligibleKnownDisconnectedCandidateSum;
  private long localConnectableFromDirectSum;
  private long localConnectableFromMeshSum;
  private long localConnectableFromConnectedSum;
  private long localConnectableFromRegistrySum;
  private long localConnectableFromPxSum;
  private long localKnownNonConnectableFromPxSum;
  private long localKnownNonConnectableFromRegistrySum;
  private long localConnectedDirectFreshSum;
  private long localConnectedPxFreshSum;
  private long localConnectedMeshFreshSum;
  private long localConnectedFanoutFreshSum;
  private long localConnectedReserveFreshSum;
  private long localConnectedReserveRegistryOnlySum;
  private long localConnectedTargetSum;
  private long localConnectedSlackTargetSum;
  private long localConnectedReserveGapSum;
  private long localReserveGapSum;
  private long localConnectedNonMeshTargetGapSum;
  private long localConnectedNonMeshRetainedSum;
  private long localKnownRetainedAfterDisconnectSum;
  private long localConnectedPxOnlySum;
  private long localKnownPxOnlySum;
  private long localPxRetainedKnownOnlySum;
  private long localOutboundConnectedPeerSum;
  private long localInboundConnectedPeerSum;
  private long localDisconnectedKnownPeerSum;
  private long localRegistryBootstrapAdded;
  private long localCatalogExpiredRegistryRemoved;
  private long localCatalogExpiredKnownRemoved;
  private long localCatalogExpiredMeshRepairRemoved;
  private long localCatalogExpiredConnectedRemoved;
  private long localConnectAttempts;
  private long localConnectSuccesses;
  private long localConnectFailures;
  private long localConnectAttemptsFromDirect;
  private long localConnectAttemptsFromPx;
  private long localConnectAttemptsFromMeshRepair;
  private long localConnectAttemptsForReserve;
  private long localConnectSuccessesFromDirect;
  private long localConnectSuccessesFromPx;
  private long localConnectSuccessesFromMeshRepair;
  private long localConnectSuccessesForReserve;
  private long localConnectFailuresForReserve;
  private long localReserveConnectedPromotedToMesh;
  private long localReserveConnectedConsumedByMesh;
  private long localReserveConnectedConsumedBeforeMaturity;
  private long localReserveConnectedDisconnectedBeforePromotion;
  private long localReserveConnectedDisconnectedBeforeMaturity;
  private long localReserveConnectedDisconnectedAfterMaturity;
  private long localMeshSelectionSkippedReservedPeer;
  private long localPxPromotedToConnected;
  private long localPxAdmissionAttempts;
  private long localPxAdmissionSuccesses;
  private long localPxAdmissionRejectionsTotal;
  private long localConnectRetryBackoffActiveSum;
  private long localConnectionTransitionOutbound;
  private long localConnectionTransitionInbound;
  private long localConnectionTransitionDisconnected;
  private long localConnectionDisconnectedByReceivedPrune;
  private long localConnectionDisconnectedByLocalPrune;
  private long localConnectionDisconnectedByScore;
  private long localConnectionDisconnectedByCapacity;
  private long localConnectionDisconnectedByIdle;
  private long localConnectionDisconnectedByExpiry;
  private long localConnectionDisconnectedByOther;
  private long localConnectionPreservedAfterReceivedPrune;
  private long localConnectionPreservedAfterLocalOversubscriptionPrune;
  private long localConnectionPreservedAfterLocalLowScorePrune;
  private long localConnectedNonMeshSurvivedAfterPrune;
  private long localMeshRemovedButStillConnectedCount;
  private long localConnectedStaleDropCount;
  private long localConnectedNonMeshUsedForGossip;
  private long localConnectedNonMeshUsedForFanout;
  private long localConnectedNonMeshPromotedToMesh;
  private long localConnectedNonMeshDisconnectedByCapacity;
  private long localConnectedNonMeshDisconnectedByScore;
  private long localConnectedNonMeshDisconnectedByExpiry;
  private long localConnectionLifetimeSum;
  private long localConnectionLifetimeCount;
  private long localConnectedNonMeshLifetimeSum;
  private long localConnectedNonMeshLifetimeCount;
  private long localReserveConnectedLifetimeSum;
  private long localReserveConnectedLifetimeCount;
  private long localReserveConnectedLifetimeBeforePromotionSum;
  private long localReserveConnectedLifetimeBeforePromotionCount;
  private long localConnectionReconnectSamePeerWithin5Heartbeats;
  private long localConnectionReconnectSamePeerWithin10Heartbeats;
  private long localConnectionIdleDisconnectsBeforeMinLifetime;
  private long localConnectionIdleDisconnectsAfterMinLifetime;
  private long localConnectionDisconnectionBlockedByHold;
  private long localConnectionHoldExpiredDisconnects;
  private long localConnectionConsecutiveIdleThresholdHits;
  private long localConnectionRetryDelaySum;
  private long localConnectionRetryDelayCount;
  private long localConnectionRetryDelayMax;
  private HashSet<TopicPeerKey> localConnectionFlappingPeers;
  private ScoreDecompositionAggregate pxSenderTrustDecomposition;
  private ScoreDecompositionAggregate receiverGraftScoreRejectionDecomposition;
  private ScoreDecompositionAggregate remotePruneOutcomeDecomposition;

  /** Callback for Gossip events. */
  private GossipEvent callback;
  /**
   * Replicate this object by returning an identical copy. It is called by the initializer and do
   * not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    GossipSubProtocol dolly = new GossipSubProtocol(GossipSubProtocol.prefix);
    return dolly;
  }

  /**
   * Constructor for KademliaProtocol. It is only used by the initializer when creating the
   * prototype. Every other instance calls CLONE to create a new object.
   *
   * @param prefix String: the prefix for configuration parameters
   */
  public GossipSubProtocol(String prefix) {
    this.node = null; // empty nodeId
    GossipSubProtocol.prefix = prefix;

    _init();

    tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);

    seen = new HashMap<>();

    mesh = new HashMap<>();

    fanout = new HashMap<>();

    mCache = new MCache(100);

    topicPeerScores = new HashMap<>();
    globalPenaltyScores = new HashMap<>();
    pendingPromises = new HashMap<>();
    pruneBackoffs = new HashMap<>();
    lastReceivedPruneHeartbeats = new HashMap<>();
    pruneBackoffOrigins = new HashMap<>();
    meshPeerDirections = new HashMap<>();
    pxPeerHints = new HashMap<>();
    topicCandidates = new HashMap<>();
    discoveryManager = new BootstrapDiscoveryManager();
    dOutWarmTopics = new HashSet<>();
    dOutConsecutiveDeficitHeartbeatsByTopic = new HashMap<>();
    dOutConsecutiveZeroOutboundHeartbeatsByTopic = new HashMap<>();
    dOutConsecutiveZeroRemainingDeficitByTopic = new HashMap<>();
    repairRelaxReceivedPruneUsedByTopic = new HashMap<>();
    receiverRelaxGraftBackoffUsedByTopic = new HashMap<>();
    localConnectAttemptsUsedByTopic = new HashMap<>();
    localPxAdmissionUsedByTopic = new HashMap<>();
    lastPxAdvertisedHeartbeatByTopicPeer = new HashMap<>();
    pxRequesterBackoffEnd = new HashMap<>();
    pxRateLimitWindowStart = new HashMap<>();
    pxRateLimitBudgetUsed = new HashMap<>();
    inboundGraftTimestamps = new HashMap<>();
    pxFloodSuppressedUntil = new HashMap<>();
    repairRelaxReceivedPrunePendingSelectionsByTopic = new HashMap<>();
    repairRelaxReceivedPruneLifecycles = new HashMap<>();
    heartbeatCounter = 0;
    degradedPeer = false;
    opportunisticGraftEvaluationCount = 0;
    opportunisticGraftTriggerCount = 0;
    opportunisticGraftPeerCount = 0;
    pxPruneMessagesSent = 0;
    pxCandidatesAdvertised = 0;
    pxCandidatesSeen = 0;
    pxCandidatesRejectedSenderThreshold = 0;
    pxCandidatesRejectedAlreadyKnown = 0;
    pxCandidatesRejectedAlreadyConnected = 0;
    pxCandidatesRejectedInMesh = 0;
    pxCandidatesRejectedPruneBackoff = 0;
    pxCandidatesRejectedScore = 0;
    pxCandidatesRejectedExpiredOrStale = 0;
    pxCandidatesRejectedCapacity = 0;
    pxCandidatesAdmittedToKnown = 0;
    pxCandidatesPassedSenderThreshold = 0;
    pxCandidatesAdmittedAndLaterConnected = 0;
    pxCandidatesAccepted = 0;
    pxCandidatesRejected = 0;
    pxAcceptedFromHighScoreSenders = 0;
    pxPeersSkippedRecentlyAdvertised = 0;
    pxPeersSelectedAfterNoveltyFiltering = 0;
    pxSendersSeen = 0;
    pxSendersAboveAcceptThreshold = 0;
    pxSendersBelowAcceptThreshold = 0;
    pxSendersScoreSum = 0.0;
    pxSendersScoreCount = 0;
    pxSendersScoreMin = Double.POSITIVE_INFINITY;
    pxSendersScoreMax = Double.NEGATIVE_INFINITY;
    pxSendersScoreLtThresholdMinusOne = 0;
    pxSendersScoreThresholdMinusOneToMinusHalf = 0;
    pxSendersScoreThresholdMinusHalfToThreshold = 0;
    pxSendersScoreGeThreshold = 0;
    dOutGraftsAcceptedAtCapacity = 0;
    dOutGraftsRejectedAtCapacity = 0;
    dOutOutboundMeshPeerSum = 0;
    dOutOutboundMeshPeerObservationCount = 0;
    dOutOutboundMeshPeerMinObserved = Long.MAX_VALUE;
    dOutHeartbeatDeficitSum = 0;
    dOutHeartbeatDeficitNonzeroCount = 0;
    dOutRepairAttemptsForDeficit = 0;
    dOutRepairSuccesses = 0;
    dOutSteadyStateTopupAttempts = 0;
    dOutSteadyStateTopupSuccesses = 0;
    dOutSteadyStateReplacements = 0;
    dOutSteadyStatePrunes = 0;
    dOutSteadyStateGrafts = 0;
    dOutHeartbeatObservationCount = 0;
    dOutZeroOutboundObservationCount = 0;
    dOutDeficitObservationCount = 0;
    dOutPostWarmupObservationCount = 0;
    dOutPostWarmupZeroOutboundCount = 0;
    dOutPostWarmupDeficitCount = 0;
    dOutPostWarmupOutboundMeshPeerMinObserved = Long.MAX_VALUE;
    dOutMaxConsecutiveDeficitHeartbeats = 0;
    dOutMaxConsecutiveZeroOutboundHeartbeats = 0;
    dOutEscalationAttempts = 0;
    dOutEscalationSuccesses = 0;
    dOutZeroOutboundEmergencyAttempts = 0;
    dOutZeroOutboundEmergencySuccesses = 0;
    dOutInplacePromotionAttempts = 0;
    dOutInplacePromotionSuccesses = 0;
    dOutInplaceZeroOutboundPromotions = 0;
    dOutInplaceDeficitPromotions = 0;
    dOutForcedReseedAttempts = 0;
    dOutForcedReseedSuccesses = 0;
    dOutForcedReseedGrafts = 0;
    dOutForcedReseedPrunes = 0;
    dOutForcedReseedSkippedNoCandidate = 0;
    dOutForcedReseedSkippedNoPrunableInbound = 0;
    dOutForcedReseedEligibleCount = 0;
    dOutForcedReseedBlockedNotPostWarmup = 0;
    dOutForcedReseedBlockedConsecutiveZeroBelowThreshold = 0;
    dOutForcedReseedBlockedOutboundBeforePositive = 0;
    dOutForcedReseedBlockedDeficitBeforeZero = 0;
    dOutForcedReseedBlockedAfterInplaceNoRemainingDeficit = 0;
    dOutForcedReseedBlockedAfterInplaceNotPersistent = 0;
    dOutForcedReseedReachedCallsiteCount = 0;
    dOutForcedReseedZeroOutboundEmergencyCount = 0;
    dOutForcedReseedInplaceSuccessDuringZeroOutboundCount = 0;
    dOutForcedReseedAfterInplaceRemainingDeficitCount = 0;
    dOutForcedReseedReturnedBeforeCallsiteCount = 0;
    dOutForcedReseedEligibleByDeficit = 0;
    dOutForcedReseedReachedCallsiteAfterInplace = 0;
    dOutForcedReseedEligibleByZeroRemainingDeficit = 0;
    dOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit = 0;
    dOutMaxConsecutiveZeroRemainingDeficitHeartbeats = 0;
    candidatePoolActiveSum = 0;
    candidatePoolKnownActiveSum = 0;
    candidatePoolPxActiveSum = 0;
    candidatePoolMeshRepairActiveSum = 0;
    candidatePoolRegistryActiveSum = 0;
    candidatePoolObservationCount = 0;
    candidatePoolExpiredRemoved = 0;
    candidatePoolPxExpiredRemoved = 0;
    candidatePoolCappedRemoved = 0;
    candidatePoolBackoffFiltered = 0;
    candidatePoolScoreFiltered = 0;
    candidatePoolEmptySelectionCount = 0;
    repairFilterSelectionCalls = 0;
    repairFilterCandidatePoolRawTotal = 0;
    repairFilterCandidatePoolEmptyCount = 0;
    repairFilterRejectedByBackoff = 0;
    repairFilterRejectedByScore = 0;
    repairFilterRejectedByAlreadyInMesh = 0;
    repairFilterRejectedByExcluded = 0;
    repairFilterNoCandidateAfterFiltering = 0;
    pruneBackoffSetFromReceivedPrune = 0;
    pruneBackoffSetFromOversubscriptionPrune = 0;
    pruneBackoffSetFromLowScorePrune = 0;
    pruneBackoffSetFromGraftRejection = 0;
    repairRejectedByBackoffFromReceivedPrune = 0;
    repairRejectedByBackoffFromOversubscriptionPrune = 0;
    repairRejectedByBackoffFromLowScorePrune = 0;
    repairRejectedByBackoffFromGraftRejection = 0;
    repairCandidatesBlockedByBackoffPositiveScore = 0;
    repairCandidatesBlockedByBackoffNegativeScore = 0;
    repairRelaxedReceivedPruneConsidered = 0;
    repairRelaxedReceivedPruneSelected = 0;
    repairRelaxedReceivedPruneSuccesses = 0;
    repairRelaxedReceivedPrunePositiveScoreSelected = 0;
    repairRelaxedReceivedPruneNegativeScoreSelected = 0;
    repairRelaxedReceivedPruneBlockedByBudget = 0;
    repairRelaxedReceivedPruneBlockedNotPostWarmup = 0;
    repairRelaxedReceivedPruneBlockedNoDOutDeficit = 0;
    repairRelaxedReceivedPruneGrafted = 0;
    repairRelaxedReceivedPruneGraftAccepted = 0;
    repairRelaxedReceivedPruneGraftRejected = 0;
    repairRelaxedReceivedPrunePruned = 0;
    repairRelaxedReceivedPruneBackoffReset = 0;
    repairRelaxedReceivedPruneSurvived1Heartbeat = 0;
    repairRelaxedReceivedPruneSurvived5Heartbeats = 0;
    repairRelaxedReceivedPruneSurvived10Heartbeats = 0;
    repairRelaxedReceivedPruneLocalGraceActive = 0;
    repairRelaxedReceivedPruneLocalPrunePreventedLowScore = 0;
    repairRelaxedReceivedPruneLocalPrunePreventedOversubscription = 0;
    repairRelaxedReceivedPruneRemovedByReceivedPrune = 0;
    repairRelaxedReceivedPruneRemovedByLocalLowScore = 0;
    repairRelaxedReceivedPruneRemovedByLocalOversubscription = 0;
    repairRelaxedReceivedPruneRemovedByOther = 0;
    repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity = 0;
    repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore = 0;
    repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff = 0;
    repairRelaxedReceivedPruneReceivedPruneReasonLowScore = 0;
    repairRelaxedReceivedPruneReceivedPruneReasonOversubscription = 0;
    repairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe = 0;
    repairRelaxedReceivedPruneReceivedPruneReasonOther = 0;
    repairRelaxedReceivedPruneReceivedPruneAfterGraft = 0;
    repairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat = 0;
    repairRelaxedReceivedPruneSelectedPruneAgeSum = 0;
    repairRelaxedReceivedPruneSelectedPruneAgeCount = 0;
    repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum = 0;
    repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount = 0;
    repairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff = 0;
    repairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff = 0;
    repairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff = 0;
    repairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff = 0;
    repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff = 0;
    repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff = 0;
    repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff = 0;
    repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff = 0;
    repairRelaxReceivedPrunePrefilterCandidatesSeen = 0;
    repairRelaxReceivedPrunePrefilterAgeKnown = 0;
    repairRelaxReceivedPrunePrefilterAgeUnknown = 0;
    repairRelaxReceivedPrunePrefilterPassScore = 0;
    repairRelaxReceivedPrunePrefilterBlockedByAge = 0;
    repairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff = 0;
    repairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff = 0;
    repairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff = 0;
    repairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff = 0;
    receiverRelaxGraftBackoffConsidered = 0;
    receiverRelaxGraftBackoffSelected = 0;
    receiverRelaxGraftBackoffAccepted = 0;
    receiverRelaxGraftBackoffBlockedByBudget = 0;
    receiverRelaxGraftBackoffBlockedByScore = 0;
    receiverRelaxGraftBackoffBlockedByAge = 0;
    receiverRelaxGraftBackoffBlockedNotDOut = 0;
    receiverRelaxGraftBackoffBlockedOtherReason = 0;
    receiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff = 0;
    receiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff = 0;
    receiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff = 0;
    receiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff = 0;
    receiverRelaxGraftBackoffPrefilterCandidatesEncountered = 0;
    receiverRelaxGraftBackoffPrefilterAgeKnown = 0;
    receiverRelaxGraftBackoffPrefilterAgeUnknown = 0;
    receiverRelaxGraftBackoffPrefilterPassScore = 0;
    receiverRelaxGraftBackoffPrefilterBlockedByAge = 0;
    receiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff = 0;
    receiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff = 0;
    receiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff = 0;
    receiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff = 0;
    receiverRelaxGraftBackoffScoreSum = 0.0;
    receiverRelaxGraftBackoffScoreCount = 0;
    receiverRelaxGraftBackoffScoreMin = Double.POSITIVE_INFINITY;
    receiverRelaxGraftBackoffScoreMax = Double.NEGATIVE_INFINITY;
    receiverRelaxGraftBackoffScoreLtThresholdMinusOne = 0;
    receiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf = 0;
    receiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold = 0;
    receiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf = 0;
    receiverRelaxGraftBackoffScoreGeThresholdPlusHalf = 0;
    localTopologyObservationCount = 0;
    localKnownPeerSum = 0;
    localConnectablePeerSum = 0;
    localConnectedPeerSum = 0;
    localConnectedNonMeshPeerSum = 0;
    localReserveConnectedPeerSum = 0;
    localReserveConnectedImmatureSum = 0;
    localReserveConnectedMatureSum = 0;
    localMeshFractionOfConnectedSum = 0.0;
    localConnectedHoldActiveSum = 0;
    localConnectedHoldForReserveSum = 0;
    localKnownOnlyPeerSum = 0;
    localKnownNonConnectablePeerSum = 0;
    localConnectableButDisconnectedPeerSum = 0;
    localKnownFromPxSum = 0;
    localKnownFromDirectSum = 0;
    localPxKnownFreshSum = 0;
    localPxKnownStaleSum = 0;
    localMeshPeerSum = 0;
    localEligibleCandidateSum = 0;
    localEligibleConnectableCandidateSum = 0;
    localEligibleConnectedCandidateSum = 0;
    localEligibleKnownDisconnectedCandidateSum = 0;
    localConnectableFromDirectSum = 0;
    localConnectableFromMeshSum = 0;
    localConnectableFromConnectedSum = 0;
    localConnectableFromRegistrySum = 0;
    localConnectableFromPxSum = 0;
    localKnownNonConnectableFromPxSum = 0;
    localKnownNonConnectableFromRegistrySum = 0;
    localConnectedDirectFreshSum = 0;
    localConnectedPxFreshSum = 0;
    localConnectedMeshFreshSum = 0;
    localConnectedFanoutFreshSum = 0;
    localConnectedReserveFreshSum = 0;
    localConnectedReserveRegistryOnlySum = 0;
    localConnectedTargetSum = 0;
    localConnectedSlackTargetSum = 0;
    localConnectedReserveGapSum = 0;
    localReserveGapSum = 0;
    localConnectedNonMeshTargetGapSum = 0;
    localConnectedNonMeshRetainedSum = 0;
    localKnownRetainedAfterDisconnectSum = 0;
    localConnectedPxOnlySum = 0;
    localKnownPxOnlySum = 0;
    localPxRetainedKnownOnlySum = 0;
    localOutboundConnectedPeerSum = 0;
    localInboundConnectedPeerSum = 0;
    localDisconnectedKnownPeerSum = 0;
    localRegistryBootstrapAdded = 0;
    localCatalogExpiredRegistryRemoved = 0;
    localCatalogExpiredKnownRemoved = 0;
    localCatalogExpiredMeshRepairRemoved = 0;
    localCatalogExpiredConnectedRemoved = 0;
    localConnectAttempts = 0;
    localConnectSuccesses = 0;
    localConnectFailures = 0;
    localConnectAttemptsFromDirect = 0;
    localConnectAttemptsFromPx = 0;
    localConnectAttemptsFromMeshRepair = 0;
    localConnectAttemptsForReserve = 0;
    localConnectSuccessesFromDirect = 0;
    localConnectSuccessesFromPx = 0;
    localConnectSuccessesFromMeshRepair = 0;
    localConnectSuccessesForReserve = 0;
    localConnectFailuresForReserve = 0;
    localReserveConnectedPromotedToMesh = 0;
    localReserveConnectedConsumedByMesh = 0;
    localReserveConnectedConsumedBeforeMaturity = 0;
    localReserveConnectedDisconnectedBeforePromotion = 0;
    localReserveConnectedDisconnectedBeforeMaturity = 0;
    localReserveConnectedDisconnectedAfterMaturity = 0;
    localMeshSelectionSkippedReservedPeer = 0;
    localPxPromotedToConnected = 0;
    localPxAdmissionAttempts = 0;
    localPxAdmissionSuccesses = 0;
    localPxAdmissionRejectionsTotal = 0;
    localConnectRetryBackoffActiveSum = 0;
    localConnectionTransitionOutbound = 0;
    localConnectionTransitionInbound = 0;
    localConnectionTransitionDisconnected = 0;
    localConnectionDisconnectedByReceivedPrune = 0;
    localConnectionDisconnectedByLocalPrune = 0;
    localConnectionDisconnectedByScore = 0;
    localConnectionDisconnectedByCapacity = 0;
    localConnectionDisconnectedByIdle = 0;
    localConnectionDisconnectedByExpiry = 0;
    localConnectionDisconnectedByOther = 0;
    localConnectionPreservedAfterReceivedPrune = 0;
    localConnectionPreservedAfterLocalOversubscriptionPrune = 0;
    localConnectionPreservedAfterLocalLowScorePrune = 0;
    localConnectedNonMeshSurvivedAfterPrune = 0;
    localMeshRemovedButStillConnectedCount = 0;
    localConnectedStaleDropCount = 0;
    localConnectedNonMeshUsedForGossip = 0;
    localConnectedNonMeshUsedForFanout = 0;
    localConnectedNonMeshPromotedToMesh = 0;
    localConnectedNonMeshDisconnectedByCapacity = 0;
    localConnectedNonMeshDisconnectedByScore = 0;
    localConnectedNonMeshDisconnectedByExpiry = 0;
    localConnectionLifetimeSum = 0;
    localConnectionLifetimeCount = 0;
    localConnectedNonMeshLifetimeSum = 0;
    localConnectedNonMeshLifetimeCount = 0;
    localReserveConnectedLifetimeSum = 0;
    localReserveConnectedLifetimeCount = 0;
    localReserveConnectedLifetimeBeforePromotionSum = 0;
    localReserveConnectedLifetimeBeforePromotionCount = 0;
    localConnectionReconnectSamePeerWithin5Heartbeats = 0;
    localConnectionReconnectSamePeerWithin10Heartbeats = 0;
    localConnectionIdleDisconnectsBeforeMinLifetime = 0;
    localConnectionIdleDisconnectsAfterMinLifetime = 0;
    localConnectionDisconnectionBlockedByHold = 0;
    localConnectionHoldExpiredDisconnects = 0;
    localConnectionConsecutiveIdleThresholdHits = 0;
    localConnectionRetryDelaySum = 0;
    localConnectionRetryDelayCount = 0;
    localConnectionRetryDelayMax = 0;
    localConnectionFlappingPeers = new HashSet<>();
    pxSenderTrustDecomposition = new ScoreDecompositionAggregate();
    pxSenderTrustDecomposition.reset();
    receiverGraftScoreRejectionDecomposition = new ScoreDecompositionAggregate();
    receiverGraftScoreRejectionDecomposition.reset();
    remotePruneOutcomeDecomposition = new ScoreDecompositionAggregate();
    remotePruneOutcomeDecomposition.reset();

    // System.out.println("New kademliaprotocol");
  }

  /**
   * This procedure is called only once and allows to initialize the internal state of
   * KademliaProtocol. Every node shares the same configuration, so it is sufficient to call this
   * routine once.
   */
  private void _init() {
    // execute once
    if (_ALREADY_INSTALLED) return;

    GossipCommonConfig.loadFromConfig();
    _ALREADY_INSTALLED = true;
  }

  /**
   * Search through the network for a node with a specific node ID, using binary search based on the
   * ordering of the network. If the binary search does not find a node with the given ID, a
   * traditional search is performed for more reliability (in case the network is not ordered).
   *
   * @param searchNodeId the ID of the node to search for
   * @return the node with the given ID, or null if not found
   */
  public static Node nodeIdtoNode(BigInteger searchNodeId, int myPid) {
    // If the given searchNodeId is null, return null
    if (searchNodeId == null) return null;

    // Set the initial search range to cover the entire network
    int inf = 0;
    int sup = Network.size() - 1;
    int m;

    // Perform binary search until the search range is empty
    while (inf <= sup) {
      // Calculate the midpoint of the search range
      m = (inf + sup) / 2;

      // Get the ID of the node at the midpoint
      GossipSubProtocol prot = (GossipSubProtocol) Network.get(m).getProtocol(myPid);
      if (prot == null) return null;
      BigInteger mId = prot.getGossipNode().getId();

      // If the midpoint node has the desired ID, return it
      if (mId.equals(searchNodeId)) return Network.get(m);

      // If the midpoint node has a smaller ID than the desired ID, narrow the search range to the
      // upper half of the current range
      if (mId.compareTo(searchNodeId) < 0) inf = m + 1;
      // Otherwise, narrow the search range to the lower half of the current range
      else sup = m - 1;
    }

    // If the binary search did not find a node with the desired ID, perform a traditional search
    // through the network
    BigInteger mId;
    for (int i = Network.size() - 1; i >= 0; i--) {
      mId = ((GossipSubProtocol) Network.get(i).getProtocol(myPid)).getGossipNode().getId();
      if (mId.equals(searchNodeId)) return Network.get(i);
    }

    // If no node with the desired ID was found, return null
    return null;
  }

  /**
   * Gets the node associated with this Kademlia protocol instance by calling nodeIdtoNode method
   * with the ID of this KademliaNod.
   *
   * @return the node associated with this Kademlia protocol instance,
   */
  public Node getNode() {
    return nodeIdtoNode(this.getGossipNode().getId(), gossipid);
  }

  protected void sendGraftMessage(BigInteger id, String topic) {
    if (!isConnectedLocalPeer(topic, id)) {
      logger.warning("sendGraftMessage skipped disconnected peer " + id + " for topic " + topic);
      return;
    }
    observeConnectedNonMeshPromotedToMesh(topic, id);
    observeReserveConnectedPromotedToMesh(topic, id);
    recordCandidateSeen(topic, id, CandidateSource.MESH_REPAIR);
    observeRepairRelaxReceivedPruneGrafted(topic, id);
    markMeshPeerOutbound(topic, id);
    Message m = Message.makeGraftMessage(topic);
    m.src = this.node;
    m.dst = ((GossipSubProtocol) nodeIdtoNode(id, gossipid).getProtocol(gossipid)).getGossipNode();
    long inferenceHeartbeat = inferenceHeartbeatIndex();
    PassiveEgoMeshRegistry.observeSentGraft(this.node.getId(), topic, id, inferenceHeartbeat);
    PassiveCoalitionRegistry.observeSentGraft(this.node.getId(), topic, id, inferenceHeartbeat);
    sendMessage(m, id, gossipid);
    // Log topology change
    GossipTopologyTracer.recordGraft(this.node.getId(), id, topic);
  }

  protected void sendIHaveMessage(String topic, BigInteger id, List<BigInteger> ids) {
    Message m = Message.makeIHaveMessage(topic, ids);
    m.src = this.node;
    m.dst = ((GossipSubProtocol) nodeIdtoNode(id, gossipid).getProtocol(gossipid)).getGossipNode();
    sendMessage(m, id, gossipid);
  }

  protected void sendPruneMessage(BigInteger id, String topic) {
    sendPruneMessage(id, topic, PruneBackoffOrigin.OTHER, Message.PruneReasonTag.OTHER);
  }

  protected void sendPruneMessage(BigInteger id, String topic, PruneBackoffOrigin origin) {
    sendPruneMessage(id, topic, origin, pruneReasonForOrigin(origin));
  }

  protected void sendPruneMessage(
      BigInteger id, String topic, PruneBackoffOrigin origin, Message.PruneReasonTag pruneReason) {
    observeRepairRelaxReceivedPrunePruned(topic, id);
    applyPruneBackoff(topic, id, GossipCommonConfig.pruneBackoffHeartbeats, origin);
    boolean pxEligible =
        origin == PruneBackoffOrigin.OVERSUBSCRIPTION_PRUNE
            || (origin == PruneBackoffOrigin.GRAFT_REJECTION
                && GossipCommonConfig.graftRejectionPxEnabled);
    List<BigInteger> pxPeers =
        (pxEligible && !isPxSuppressedByDefense(topic, id))
            ? selectPrunePxPeers(topic, id)
            : Collections.emptyList();
    if (!pxPeers.isEmpty()) {
      pxPruneMessagesSent++;
      pxCandidatesAdvertised += pxPeers.size();
      recordPxGiven(topic, id, pxPeers.size());
    }
    Message m =
        Message.makePruneMessage(
            topic, GossipCommonConfig.pruneBackoffHeartbeats, pxPeers, pruneReason);
    m.src = this.node;
    m.dst = ((GossipSubProtocol) nodeIdtoNode(id, gossipid).getProtocol(gossipid)).getGossipNode();
    long inferenceHeartbeat = inferenceHeartbeatIndex();
    PassiveEgoMeshRegistry.observeSentPrune(this.node.getId(), topic, id, inferenceHeartbeat);
    PassiveCoalitionRegistry.observeSentPrune(this.node.getId(), topic, id, inferenceHeartbeat);
    sendMessage(m, id, gossipid);
    // Log topology change
    GossipTopologyTracer.recordPrune(this.node.getId(), id, topic);
  }

  protected void sendIWantMessage(String topic, BigInteger id, List<BigInteger> ids) {
    if (isScoringEnabled()) {
      for (BigInteger msgId : ids) {
        pendingPromises.put(
            new PromiseKey(topic, id, msgId),
            new PendingPromise(
                heartbeatCounter + GossipCommonConfig.scorePromiseTimeoutHeartbeats));
      }
    }
    Message m = Message.makeIWantMessage(topic, ids);
    m.src = this.node;
    m.dst = ((GossipSubProtocol) nodeIdtoNode(id, gossipid).getProtocol(gossipid)).getGossipNode();
    sendMessage(m, id, gossipid);
  }

  /**
   * Get the current KademliaNode object.
   *
   * @return The current KademliaNode object.
   */
  public KademliaNode getGossipNode() {
    return this.node;
  }

  /**
   * Set the protocol ID for this node.
   *
   * @param protocolID The protocol ID to set.
   */
  public void setProtocolID(int protocolID) {
    this.gossipid = protocolID;
  }

  /**
   * Sends a message using the current transport layer and starts the timeout timer if the message
   * is a request.
   *
   * @param m the message to send
   * @param destId the ID of the destination node
   * @param myPid the sender process ID (Todo: verify what myPid stand for!!!)
   */
  protected void sendMessage(Message m, BigInteger destId, int myPid) {

    // Assert that message source and destination nodes are not null
    assert m.src != null;
    assert m.dst != null;

    // Get source and destination nodes
    Node src = nodeIdtoNode(this.getGossipNode().getId(), gossipid);
    Node dest = nodeIdtoNode(destId, gossipid);
    KademliaObserver.reportMsg(m, true, this.getGossipNode().getId());

    // destpid = dest.getKademliaProtocol().getProtocolID();

    /*logger.warning(
    "Sending message "
        + m.getType()
        + " to "
        + destId
        + " "
        + ((GossipSubProtocol) dest.getProtocol(myPid)).getGossipNode().getId()
        + " from "
        + this.getGossipNode().getId()
        + " "
        + ((GossipSubProtocol) src.getProtocol(myPid)).getGossipNode().getId()
        + " "
        + m.getType());*/
    // Get the transport protocol
    // m.nrHops++;
    if (transport == null) transport = (Transport) (Network.prototype).getProtocol(tid);

    // Send the message
    transport.send(src, dest, m, gossipid);
  }

  /**
   * Get the protocol ID for this node.
   *
   * @return The protocol ID for this node.
   */
  public int getProtocolID() {
    return this.gossipid;
  }

  /**
   * Sets the current Kademlia node and its routing table.
   *
   * @param node The KademliaNode object to set.
   */
  public void setNode(KademliaNode node) {
    this.node = node;

    // Initialize the logger with the node ID as its name
    this.logger = Logger.getLogger(node.getId().toString());
  }

  /**
   * Get the logger associated with this Kademlia node.
   *
   * @return The logger object.
   */
  public Logger getLogger() {
    return this.logger;
  }

  public void setTransport(Transport transport) {
    this.transport = transport;
  }

  @Override
  public void processEvent(Node node, int pid, Object event) {
    if (shouldSuppressNode()) {
      return;
    }
    // Set the Kademlia ID as the current process ID - assuming Pid stands for process ID.
    this.gossipid = pid;

    Message m;

    // If the event is a message, report the message to the Kademlia observer.
    if (event instanceof Message) {
      m = (Message) event;
      // KademliaObserver.reportMsg(m, false);

      if (m.src != null) {
        logger.fine("Message received " + m.getType() + " from " + m.src.getId());
      } else {
        logger.fine("Message src null " + m.getType());
      }
      KademliaObserver.reportMsg(m, false, this.getGossipNode().getId());
    }

    // Handle the event based on its type.
    switch (((SimpleEvent) event).getType()) {
        /*case Message.MSG_JOIN:
        m = (Message) event;
        // sentMsg.remove(m.ackId);
        handleJoin(m, pid);
        break;*/
      case Message.MSG_LEAVE:
        m = (Message) event;
        // sentMsg.remove(m.ackId);
        handleLeave(m, pid);
        break;
        /*case Message.MSG_PUBLISH:
        m = (Message) event;
        handlePublish(m, pid);
        break;*/
      case Message.MSG_MESSAGE:
        m = (Message) event;
        handleMessage(m, pid);
        // KademliaObserver.reportMsg(m, false);
        break;
      case Message.MSG_GRAFT:
        m = (Message) event;
        handleGraft(m, pid);
        break;
      case Message.MSG_IHAVE:
        m = (Message) event;
        handleIHave(m, pid);
        break;
      case Message.MSG_IWANT:
        m = (Message) event;
        handleIWant(m, pid);
        break;
      case Message.MSG_PRUNE:
        m = (Message) event;
        handlePrune(m, pid);
        break;
    }
  }

  public void heartBeat() {
    if (shouldSuppressNode()) {
      return;
    }
    heartbeatCounter++;
    refreshCurrentMeshAndFanoutActivity();
    repairRelaxReceivedPruneUsedByTopic.clear();
    receiverRelaxGraftBackoffUsedByTopic.clear();
    localConnectAttemptsUsedByTopic.clear();
    localPxAdmissionUsedByTopic.clear();
    repairRelaxReceivedPrunePendingSelectionsByTopic.clear();
    clearExpiredPruneBackoffs();
    HashSet<String> topicsWithLocalKnowledge = new HashSet<>();
    topicsWithLocalKnowledge.addAll(mesh.keySet());
    topicsWithLocalKnowledge.addAll(fanout.keySet());
    for (TopicPeerKey key : topicCandidates.keySet()) {
      topicsWithLocalKnowledge.add(key.topic);
    }
    for (String topic : topicsWithLocalKnowledge) {
      disconnectIdleLocalPeersForTopic(topic);
    }
    cleanupCandidatePool();

    if (isScoringEnabled()) {
      for (Map.Entry<String, HashSet<BigInteger>> meshEntry : mesh.entrySet()) {
        String topic = meshEntry.getKey();
        HashSet<BigInteger> topicPeers = meshEntry.getValue();
        for (BigInteger peerId : topicPeers) {
          PeerScoreState state = getOrCreateTopicPeerScore(topic, peerId);
          state.meshHeartbeats =
              Math.min(state.meshHeartbeats + 1.0, GossipCommonConfig.scoreMeshHeartbeatCap);
        }
      }

      applyScoreDecay();
      applyBrokenPromisePenalties();
    }

    logger.finer("heartbeat execute");
    HashSet<String> activeTopics = new HashSet<>();
    activeTopics.addAll(mesh.keySet());
    activeTopics.addAll(fanout.keySet());
    for (String topic : activeTopics) {
      maybeBootstrapTopicKnowledge(topic);
    }
    for (String topic : mesh.keySet()) {
      HashSet<BigInteger> topicMesh = mesh.get(topic);
      logger.finer("heartbeat execute " + topicMesh.size() + " " + topic);

      int outboundDeficitBeforeRepair = outboundDeficit(topic, topicMesh);
      if (isDOutTrackingEnabled()) {
        dOutHeartbeatDeficitSum += outboundDeficitBeforeRepair;
        if (outboundDeficitBeforeRepair > 0) {
          dOutHeartbeatDeficitNonzeroCount++;
        }
      }

      if (topicMesh.size() < GossipCommonConfig.D_low) {
        int repairCount = GossipCommonConfig.D - topicMesh.size();
        boolean attemptedDeficitRepair =
            isDOutTrackingEnabled() && outboundDeficitBeforeRepair > 0 && repairCount > 0;
        if (attemptedDeficitRepair) {
          dOutRepairAttemptsForDeficit++;
        }
        HashSet<BigInteger> nodes = selectMeshRepairPeers(topic, repairCount, topicMesh);
        topicMesh.addAll(nodes);
        for (BigInteger id : nodes) {
          sendGraftMessage(id, topic);
        }
        recordRepairRelaxReceivedPruneSuccessIfAny(topic, outboundDeficitBeforeRepair, topicMesh);
        if (attemptedDeficitRepair
            && outboundDeficit(topic, topicMesh) < outboundDeficitBeforeRepair) {
          dOutRepairSuccesses++;
        }
      }
      pruneLowScorePeers(topic);
      maybeRepairOutboundDeficit(topic, topicMesh);
      if (topicMesh.size() > GossipCommonConfig.D_high) {
        HashSet<BigInteger> survivors = selectMeshSurvivors(topic, topicMesh);
        List<BigInteger> toPrune = new ArrayList<>();
        for (BigInteger node : new HashSet<>(topicMesh)) {
          if (!survivors.contains(node)) {
            toPrune.add(node);
          }
        }
        for (BigInteger node : toPrune) {
          if (isRepairRelaxReceivedPruneLocalGraceActive(topic, node)) {
            observeRepairRelaxReceivedPruneLocalGraceActive(topic, node);
            repairRelaxedReceivedPruneLocalPrunePreventedOversubscription++;
            continue;
          }
          observeRepairRelaxReceivedPruneRemoval(
              topic, node, RelaxedReceivedPruneRemovalCause.LOCAL_OVERSUBSCRIPTION);
          topicMesh.remove(node);
          clearMeshPeerDirection(topic, node);
          logger.fine("Pruning node " + node);
          sendPruneMessage(node, topic, PruneBackoffOrigin.OVERSUBSCRIPTION_PRUNE);
          observeMeshConnectionSeparationAfterPrune(
              topic, node, PruneBackoffOrigin.OVERSUBSCRIPTION_PRUNE, true);
        }
      }
      maybeOpportunisticGraft(topic);
      observeRepairRelaxReceivedPruneLifecycle(topic, topicMesh);
    }
    for (String topic : activeTopics) {
      maybeMaintainConnectedReserve(topic);
    }
    for (String topic : fanout.keySet()) {
      if (fanoutExpirations.get(topic) > CommonState.getTime()) {
        fanout.remove(topic);
        fanoutExpirations.remove(topic);
      } else {
        if (fanout.get(topic).size() < GossipCommonConfig.D) {
          HashSet<BigInteger> excludedPeers =
              mesh.get(topic) == null ? new HashSet<>() : new HashSet<>(mesh.get(topic));
          excludedPeers.addAll(fanout.get(topic));
          HashSet<BigInteger> nodes =
              selectFanoutPeers(
                  topic, GossipCommonConfig.D - fanout.get(topic).size(), excludedPeers);
          fanout.get(topic).addAll(nodes);
          for (BigInteger id : nodes) {
            noteFanoutActivity(topic, id);
          }
        }
      }
    }
    HashSet<String> allTopics = new HashSet<>();
    allTopics.addAll(mesh.keySet());
    allTopics.addAll(fanout.keySet());

    for (String topic : allTopics) {
      List<BigInteger> msgs = seen.get(topic);
      if (msgs != null) {
        List<BigInteger> recipients = selectLocalGossipRecipients(topic, GossipCommonConfig.D);

        logger.finer(
            "Sending gossip msgs "
                + msgs.size()
                + " "
                + recipients.size()
                + " "
                + (mesh.get(topic) == null ? 0 : mesh.get(topic).size()));

        for (BigInteger id : recipients) {
          logger.finer("Sending gossip to " + id);
          sendIHaveMessage(topic, id, msgs);
        }
      }
    }

    for (String topic : activeTopics) {
      trimConnectedNonMeshAboveSlack(topic);
    }

    // Record mesh statistics for this node
    for (String topic : mesh.keySet()) {
      int fanoutSize = fanout.containsKey(topic) ? fanout.get(topic).size() : 0;
      GossipTopologyTracer.recordMeshStats(
          this.node.getId(), topic, mesh.get(topic).size(), fanoutSize);
      observeOutboundMeshPeers(topic);
    }
    observeCandidatePool();
    observeLocalTopologyCatalogue();
  }

  public void Join(String topic) {
    if (shouldSuppressNode()) {
      return;
    }
    logger.fine("Handlejoin received " + topic);
    maybeBootstrapTopicKnowledge(topic);

    if (mesh.get(topic) != null) return;
    if (fanout.get(topic) != null) {
      HashSet<BigInteger> p = fanout.get(topic);
      mesh.put(topic, p);
      fanout.remove(topic);
      p.removeIf(peerId -> isMeshCandidateIneligible(topic, peerId, null));
      if (p.size() < GossipCommonConfig.D) {
        HashSet<BigInteger> p2 =
            selectMeshRepairPeers(topic, GossipCommonConfig.D - p.size(), mesh.get(topic));
        for (BigInteger id : p2) {
          mesh.get(topic).add(id);
        }
      }
    } else if (mesh.get(topic) == null) {
      mesh.put(topic, new HashSet<BigInteger>());
      HashSet<BigInteger> p = selectMeshRepairPeers(topic, GossipCommonConfig.D, mesh.get(topic));

      if (p != null) {
        for (BigInteger id : p) {
          logger.fine("Adding " + id + " to mesh");
          mesh.get(topic).add(id);
        }
      }
    }
    if (mesh.get(topic) != null) {
      HashSet<BigInteger> p = mesh.get(topic);
      for (BigInteger id : p) {
        sendGraftMessage(id, topic);
      }
      // Log JOIN event with initial mesh size
      GossipTopologyTracer.recordJoin(this.node.getId(), topic, p.size());
    }
  }

  private long inferenceHeartbeatIndex() {
    return GossipHeartBeat.getGlobalHeartbeatIndex();
  }

  private void handleLeave(Message m, int myPid) {
    String topic = (String) m.body;
    logger.fine("Handleleave received " + topic);
    long inferenceHeartbeat = inferenceHeartbeatIndex();
    PassiveEgoMeshRegistry.observeLocalLeave(this.node.getId(), topic, inferenceHeartbeat);
    PassiveCoalitionRegistry.observeLocalLeave(this.node.getId(), topic, inferenceHeartbeat);
    if (mesh.get(topic) != null) {
      HashSet<BigInteger> p = mesh.get(topic);
      for (BigInteger id : p) {
        sendPruneMessage(
            id, topic, PruneBackoffOrigin.OTHER, Message.PruneReasonTag.LEAVE_OR_UNSUBSCRIBE);
      }
      clearMeshPeerDirections(topic, p);
      mesh.remove(topic);
      clearDOutTopicStabilityState(topic);
      // Log LEAVE event
      GossipTopologyTracer.recordLeave(this.node.getId(), topic);
    }
  }

  private void handleGraft(Message m, int myPid) {
    String topic = (String) m.body;
    BigInteger peerId = m.src.getId();
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    boolean receiverRelaxedBackoffSelected = false;
    logger.fine("handleGraft received " + topic + " from:" + m.src.getId());
    if (GossipCommonConfig.pxFloodDetectionEnabled) {
      recordInboundGraftForFloodDetection(topic);
    }
    recordCandidateSeen(topic, peerId, CandidateSource.KNOWN);
    if (isInPruneBackoff(topic, peerId)) {
      if (GossipCommonConfig.receiverRelaxGraftBackoffForDOut) {
        observeReceiverRelaxGraftBackoffPrefilterCandidate(topic, peerId);
        receiverRelaxGraftBackoffConsidered++;
      }
      if (tryReceiverRelaxGraftBackoff(topic, peerId, topicMesh)) {
        receiverRelaxedBackoffSelected = true;
      } else {
        sendPruneMessage(
            peerId,
            topic,
            PruneBackoffOrigin.GRAFT_REJECTION,
            Message.PruneReasonTag.GRAFT_REJECTION_BACKOFF);
        return;
      }
    }
    if (isScoringEnabled() && scoreOf(topic, peerId) < GossipCommonConfig.meshScoreThreshold) {
      observeReceiverGraftScoreRejectionDecomposition(topic, peerId);
      if (receiverRelaxedBackoffSelected) {
        receiverRelaxGraftBackoffBlockedOtherReason++;
      }
      sendPruneMessage(
          peerId,
          topic,
          PruneBackoffOrigin.GRAFT_REJECTION,
          Message.PruneReasonTag.GRAFT_REJECTION_SCORE);
      return;
    }
    if (topicMesh != null && topicMesh.contains(peerId)) {
      return;
    }

    boolean atCapacity = topicMesh != null && topicMesh.size() >= GossipCommonConfig.D_high;
    if (atCapacity && !canAcceptIncomingGraftAtCapacity(topic, topicMesh)) {
      if (isDOutTrackingEnabled()) {
        dOutGraftsRejectedAtCapacity++;
      }
      if (receiverRelaxedBackoffSelected) {
        receiverRelaxGraftBackoffBlockedOtherReason++;
      }
      sendPruneMessage(
          peerId,
          topic,
          PruneBackoffOrigin.GRAFT_REJECTION,
          Message.PruneReasonTag.GRAFT_REJECTION_AT_CAPACITY);
      return;
    }

    if (topicMesh != null) {
      markPeerConnected(
          topic,
          peerId,
          LocalConnectionState.INBOUND_CONNECTED,
          LocalConnectionTransitionReason.DIRECT_INBOUND_ACTIVITY);
      if (receiverRelaxedBackoffSelected) {
        receiverRelaxGraftBackoffAccepted++;
        observeReceiverRelaxGraftBackoffAcceptedAge(topic, peerId);
      }
      observeConnectedNonMeshPromotedToMesh(topic, peerId);
      boolean addedToMesh = topicMesh.add(peerId);
      observeReserveConnectedPromotedToMesh(topic, peerId);
      markMeshPeerInbound(topic, peerId);
      if (addedToMesh) {
        long inferenceHeartbeat = inferenceHeartbeatIndex();
        PassiveEgoMeshRegistry.observeAcceptedIncomingGraft(
            this.node.getId(), topic, peerId, inferenceHeartbeat);
        PassiveCoalitionRegistry.observeAcceptedIncomingGraft(
            this.node.getId(), topic, peerId, inferenceHeartbeat);
      }
      if (atCapacity && isDOutTrackingEnabled()) {
        dOutGraftsAcceptedAtCapacity++;
      }
    }
    if (topicMesh == null && receiverRelaxedBackoffSelected) {
      receiverRelaxGraftBackoffAccepted++;
      observeReceiverRelaxGraftBackoffAcceptedAge(topic, peerId);
    }
  }

  private void handlePrune(Message m, int myPid) {

    String topic = (String) m.body;
    long backoffHeartbeats = pruneBackoffFromMessage(m);
    Message.PruneReasonTag pruneReason = pruneReasonFromMessage(m);
    List<BigInteger> pxPeers = prunePxFromMessage(m);

    logger.fine("handlePrune received " + topic + " " + m.src.getId());
    recordCandidateSeen(topic, m.src.getId(), CandidateSource.KNOWN);
    observeRemotePruneOutcomeDecomposition(topic, m.src.getId());
    observeRepairRelaxReceivedPruneGraftRejected(topic, m.src.getId());
    observeRepairRelaxReceivedPrunePruned(topic, m.src.getId());
    observeRepairRelaxReceivedPruneReceivedPruneReason(topic, m.src.getId(), pruneReason);
    observeRepairRelaxReceivedPruneRemoval(
        topic, m.src.getId(), RelaxedReceivedPruneRemovalCause.RECEIVED_PRUNE);

    applyPruneBackoff(topic, m.src.getId(), backoffHeartbeats, PruneBackoffOrigin.RECEIVED_PRUNE);
    boolean removedFromMesh = false;
    if (mesh.get(topic) != null) {
      removedFromMesh = mesh.get(topic).remove(m.src.getId());
      clearMeshPeerDirection(topic, m.src.getId());
    } else {
      logger.fine("handlePrune not found");
    }
    observeMeshConnectionSeparationAfterPrune(
        topic, m.src.getId(), PruneBackoffOrigin.RECEIVED_PRUNE, removedFromMesh);
    long inferenceHeartbeat = inferenceHeartbeatIndex();
    PassiveEgoMeshRegistry.observeReceivedPrune(
        this.node.getId(), topic, m.src.getId(), inferenceHeartbeat);
    PassiveCoalitionRegistry.observeReceivedPrune(
        this.node.getId(), topic, m.src.getId(), inferenceHeartbeat);
    PxFloodRegistry.observeReceivedPruneWithPx(
        this.node.getId(), topic, m.src.getId(), pxPeers, inferenceHeartbeat);
    maybeAcceptPxPeers(topic, m.src.getId(), pxPeers);
  }

  private void handleIHave(Message m, int myPid) {
    String topic = (String) m.body;
    recordCandidateSeen(topic, m.src.getId(), CandidateSource.KNOWN);
    long inferenceHeartbeat = inferenceHeartbeatIndex();
    PassiveEgoMeshRegistry.observeReceivedIHave(
        this.node.getId(), topic, m.src.getId(), inferenceHeartbeat);
    PassiveCoalitionRegistry.observeReceivedIHave(
        this.node.getId(), topic, m.src.getId(), inferenceHeartbeat);

    List<BigInteger> msgIds = (List<BigInteger>) m.value;
    List<BigInteger> iwants = new ArrayList<>();
    List<BigInteger> have = seen.get(topic);

    logger.finer("handleIHave received " + topic + " " + msgIds.size());

    if (have != null) {
      for (BigInteger msg : msgIds) {
        if (!have.contains(msg)) iwants.add(msg);
      }
    } else {
      iwants.addAll(msgIds);
    }
    if (iwants.size() > 0) sendIWantMessage(topic, m.src.getId(), iwants);
  }

  private void handleIWant(Message m, int myPid) {
    logger.finer("handleIWant received " + m.body);
    String topic = (String) m.body;
    recordCandidateSeen(topic, m.src.getId(), CandidateSource.KNOWN);
    long inferenceHeartbeat = inferenceHeartbeatIndex();
    PassiveEgoMeshRegistry.observeReceivedIWant(
        this.node.getId(), topic, m.src.getId(), inferenceHeartbeat);
    PassiveCoalitionRegistry.observeReceivedIWant(
        this.node.getId(), topic, m.src.getId(), inferenceHeartbeat);
    List<BigInteger> ids = (List<BigInteger>) m.value;

    for (BigInteger id : ids) {
      if (mCache.get(id) != null) {
        if (degradedPeer
            && CommonState.r.nextDouble() < GossipCommonConfig.degradedPeerWithholdProbability) {
          continue;
        }
        Message msg = Message.makeMessage((String) m.body, mCache.get(id));
        msg.src = this.node;
        msg.dst = m.src;
        // BigInteger cid = ((Sample[]) msg.value)[0].getId();

        // logger.info("sending message iwant " + cid + " " + msg.id + " to " + msg.dst.getId());

        sendMessage(msg, m.src.getId(), myPid);
      }
    }
    //
  }

  public void Publish(Message m, int myPid) {

    String topic = (String) m.body;
    Sample[] samples = (Sample[]) m.value;
    // BigInteger id = (BigInteger) m.value;
    logger.fine("Publish message " + topic + " " + gossipid);

    if (seen.get(topic) == null) seen.put(topic, new ArrayList<BigInteger>());

    // BigInteger cid = getValueId(m.value);
    // BigInteger cid = s.getId();
    seen.get(topic).add(samples[0].getId());
    mCache.put(samples[0].getId(), samples);

    // mCache.put(s.getIdByColumn(), s);
    if (mesh.get(topic) != null) {
      HashSet<BigInteger> nodesToSend = new HashSet<>(mesh.get(topic));
      nodesToSend.remove(this.node.getId());
      for (BigInteger n : nodesToSend) {
        Message msg = Message.makeMessage(topic, samples);
        msg.src = this.node;
        msg.dst = nodeIdtoNode(n, gossipid).getGossipProtocol().getGossipNode();
        sendMessage(msg, n, gossipid);
      }
    }
  }

  protected void handleMessage(Message m, int myPid) {

    String topic = (String) m.body;
    recordCandidateSeen(topic, m.src.getId(), CandidateSource.KNOWN);
    long inferenceHeartbeat = inferenceHeartbeatIndex();
    PassiveEgoMeshRegistry.observeReceivedMessage(
        this.node.getId(), topic, m.src.getId(), inferenceHeartbeat);
    PassiveCoalitionRegistry.observeReceivedMessage(
        this.node.getId(), topic, m.src.getId(), inferenceHeartbeat);

    Sample[] samples = (Sample[]) m.value;
    mCache.put(samples[0].getId(), samples);

    logger.fine(
        "handleMessage received "
            + topic
            + " "
            + m.id
            + " "
            + m.src.getId()
            + " "
            + samples.length);

    if (seen.get(topic) == null) seen.put(topic, new ArrayList<BigInteger>());
    if (seen.get(topic).contains(samples[0].getId())) return;

    if (isScoringEnabled()) {
      getOrCreateTopicPeerScore(topic, m.src.getId()).firstDeliveries++;
      pendingPromises.remove(new PromiseKey(topic, m.src.getId(), samples[0].getId()));
    }

    if (!shouldSuppressApplicationCallback()) {
      this.callback.messageReceived(m);
    }
    seen.get(topic).add(samples[0].getId());

    if (degradedPeer
        && CommonState.r.nextDouble() < GossipCommonConfig.degradedPeerForwardDropProbability) {
      return;
    }

    if (mesh.get(topic) != null) {
      HashSet<BigInteger> nodesToSend = new HashSet<>(mesh.get(topic));
      nodesToSend.remove(m.src.getId());
      nodesToSend.remove(this.node.getId());
      for (BigInteger n : nodesToSend) {
        Message mbis = m.copy();
        mbis.dst =
            ((GossipSubProtocol) nodeIdtoNode(n, gossipid).getProtocol(myPid)).getGossipNode();
        mbis.src = this.node;
        logger.finer("handleMessage resending " + mbis.body + " " + mbis.value);
        sendMessage(mbis, n, myPid);
      }
    }
  }

  private BigInteger getValueId(Object obj) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos;
    byte[] hash;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(obj);
      oos.flush();
      byte[] data = bos.toByteArray();
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      hash = digest.digest(data);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return null;
    }

    return new BigInteger(1, hash);
  }

  public static PeerTable getTable() {
    return peers;
  }

  public static void registerBootstrapPeer(String topic, BigInteger peerId) {
    peers.addPeer(topic, peerId);
    peers.addBootstrapPeer(peerId);
  }

  public void activatePassiveEgoMeshObserver(String topic) {
    if (topic == null) return;
    Join(topic);
  }

  /** Called by PxFloodRegistry to make this attacker GRAFT the given target on the topic. */
  public void activatePxFloodWave(String topic, BigInteger targetPeerId) {
    if (topic == null) return;
    if (targetPeerId != null) {
      // Register as PX_HINT so the target is connectable even when
      // gossipsub.discovery.legacy_local_connect_enabled=false.
      recordCandidateSeen(topic, targetPeerId, CandidateSource.PX_HINT);
    }
    if (mesh.get(topic) == null) {
      // Attacker not yet on this topic; Join will select target via repair.
      Join(topic);
      return;
    }
    if (targetPeerId == null) return;
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    if (topicMesh.contains(targetPeerId)) return;
    // Attacker already has a mesh on this topic — force-connect and GRAFT the target.
    if (!isConnectedLocalPeer(topic, targetPeerId)) {
      CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, targetPeerId));
      if (meta != null) {
        attemptLocalConnect(
            topic, targetPeerId, meta, LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_REPAIR);
      }
    }
    if (!isConnectedLocalPeer(topic, targetPeerId)) return;
    topicMesh.add(targetPeerId);
    sendGraftMessage(targetPeerId, topic);
  }

  private PeerScoreState getOrCreateTopicPeerScore(String topic, BigInteger peerId) {
    TopicPeerKey key = new TopicPeerKey(topic, peerId);
    PeerScoreState state = topicPeerScores.get(key);
    if (state == null) {
      state = new PeerScoreState();
      topicPeerScores.put(key, state);
    }
    return state;
  }

  private PeerScoreState getOrCreateGlobalPenaltyScore(BigInteger peerId) {
    PeerScoreState state = globalPenaltyScores.get(peerId);
    if (state == null) {
      state = new PeerScoreState();
      globalPenaltyScores.put(peerId, state);
    }
    return state;
  }

  private double scoreOf(String topic, BigInteger peerId) {
    if (!isScoringEnabled()) return 0.0;
    PeerScoreState state = topicPeerScores.get(new TopicPeerKey(topic, peerId));
    double localScore =
        state == null
            ? 0.0
            : (state.meshHeartbeats * GossipCommonConfig.scoreWeightTimeInMesh)
                + (state.firstDeliveries * GossipCommonConfig.scoreWeightFirstDeliveries)
                - (state.invalidDeliveries * GossipCommonConfig.scoreWeightInvalidDeliveries)
                - (state.brokenPromises * GossipCommonConfig.scoreWeightBrokenPromises);
    PeerScoreState global = globalPenaltyScores.get(peerId);
    double globalPenalty =
        global == null
            ? 0.0
            : (global.invalidDeliveries * GossipCommonConfig.scoreWeightInvalidDeliveries)
                + (global.brokenPromises * GossipCommonConfig.scoreWeightBrokenPromises);
    double degradedPenalty =
        isPeerDegraded(peerId) ? GossipCommonConfig.scoreDegradedPeerPenalty : 0.0;
    return localScore - globalPenalty - degradedPenalty;
  }

  private ScoreDecompositionSnapshot scoreDecompositionOf(
      String topic, BigInteger peerId, double threshold) {
    ScoreDecompositionSnapshot snapshot = new ScoreDecompositionSnapshot();
    snapshot.meshThreshold = threshold;
    if (!isScoringEnabled()) {
      snapshot.totalScore = 0.0;
      snapshot.scoreMinusMeshThreshold = -snapshot.meshThreshold;
      return snapshot;
    }

    PeerScoreState localState = topicPeerScores.get(new TopicPeerKey(topic, peerId));
    PeerScoreState globalState = globalPenaltyScores.get(peerId);

    snapshot.timeInMeshContribution =
        localState == null
            ? 0.0
            : localState.meshHeartbeats * GossipCommonConfig.scoreWeightTimeInMesh;
    snapshot.firstDeliveriesContribution =
        localState == null
            ? 0.0
            : localState.firstDeliveries * GossipCommonConfig.scoreWeightFirstDeliveries;
    snapshot.localInvalidDeliveriesPenalty =
        localState == null
            ? 0.0
            : localState.invalidDeliveries * GossipCommonConfig.scoreWeightInvalidDeliveries;
    snapshot.localBrokenPromisesPenalty =
        localState == null
            ? 0.0
            : localState.brokenPromises * GossipCommonConfig.scoreWeightBrokenPromises;
    snapshot.globalInvalidDeliveriesPenalty =
        globalState == null
            ? 0.0
            : globalState.invalidDeliveries * GossipCommonConfig.scoreWeightInvalidDeliveries;
    snapshot.globalBrokenPromisesPenalty =
        globalState == null
            ? 0.0
            : globalState.brokenPromises * GossipCommonConfig.scoreWeightBrokenPromises;

    snapshot.totalScore =
        snapshot.timeInMeshContribution
            + snapshot.firstDeliveriesContribution
            - snapshot.localInvalidDeliveriesPenalty
            - snapshot.localBrokenPromisesPenalty
            - snapshot.globalInvalidDeliveriesPenalty
            - snapshot.globalBrokenPromisesPenalty;
    snapshot.scoreMinusMeshThreshold = snapshot.totalScore - snapshot.meshThreshold;
    return snapshot;
  }

  private ScoreDecompositionSnapshot scoreDecompositionOf(String topic, BigInteger peerId) {
    return scoreDecompositionOf(topic, peerId, GossipCommonConfig.meshScoreThreshold);
  }

  private void observePxSenderTrustDecomposition(String topic, BigInteger peerId) {
    pxSenderTrustDecomposition.observe(
        scoreDecompositionOf(topic, peerId, GossipCommonConfig.acceptPXThreshold));
  }

  private void observePxSenderTrust(String topic, BigInteger senderId) {
    if (topic == null || senderId == null) return;

    double senderScore = scoreOf(topic, senderId);
    pxSendersSeen++;
    pxSendersScoreSum += senderScore;
    pxSendersScoreCount++;
    if (senderScore < pxSendersScoreMin) {
      pxSendersScoreMin = senderScore;
    }
    if (senderScore > pxSendersScoreMax) {
      pxSendersScoreMax = senderScore;
    }

    double threshold = GossipCommonConfig.acceptPXThreshold;
    if (senderScore < threshold - 1.0) {
      pxSendersScoreLtThresholdMinusOne++;
    } else if (senderScore < threshold - 0.5) {
      pxSendersScoreThresholdMinusOneToMinusHalf++;
    } else if (senderScore < threshold) {
      pxSendersScoreThresholdMinusHalfToThreshold++;
    } else {
      pxSendersScoreGeThreshold++;
    }

    if (senderScore >= threshold) {
      pxSendersAboveAcceptThreshold++;
    } else {
      pxSendersBelowAcceptThreshold++;
    }

    observePxSenderTrustDecomposition(topic, senderId);
  }

  private void observeReceiverGraftScoreRejectionDecomposition(String topic, BigInteger peerId) {
    receiverGraftScoreRejectionDecomposition.observe(scoreDecompositionOf(topic, peerId));
  }

  private void observeRemotePruneOutcomeDecomposition(String topic, BigInteger peerId) {
    remotePruneOutcomeDecomposition.observe(scoreDecompositionOf(topic, peerId));
  }

  private void filterPeersByScore(
      String topic, HashSet<BigInteger> peersToFilter, double threshold) {
    Iterator<BigInteger> it = peersToFilter.iterator();
    while (it.hasNext()) {
      BigInteger peerId = it.next();
      if (scoreOf(topic, peerId) < threshold) {
        it.remove();
      }
    }
  }

  private BigInteger lowestScoredPeer(String topic, HashSet<BigInteger> candidatePeers) {
    if (!isScoringEnabled()) {
      Iterator<BigInteger> iterator = candidatePeers.iterator();
      return iterator.hasNext() ? iterator.next() : null;
    }
    BigInteger lowestPeer = null;
    double lowestScore = Double.MAX_VALUE;
    for (BigInteger peerId : candidatePeers) {
      double score = scoreOf(topic, peerId);
      if (lowestPeer == null || score < lowestScore) {
        lowestPeer = peerId;
        lowestScore = score;
      }
    }
    return lowestPeer;
  }

  private void pruneLowScorePeers(String topic) {
    if (!isScoringEnabled()) return;
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    if (topicMesh == null) return;
    List<BigInteger> toPrune = new ArrayList<>();
    for (BigInteger peerId : topicMesh) {
      if (scoreOf(topic, peerId) < GossipCommonConfig.meshScoreThreshold) {
        if (isRepairRelaxReceivedPruneLocalGraceActive(topic, peerId)) {
          observeRepairRelaxReceivedPruneLocalGraceActive(topic, peerId);
          repairRelaxedReceivedPruneLocalPrunePreventedLowScore++;
          continue;
        }
        toPrune.add(peerId);
      }
    }
    for (BigInteger peerId : toPrune) {
      observeRepairRelaxReceivedPruneRemoval(
          topic, peerId, RelaxedReceivedPruneRemovalCause.LOCAL_LOW_SCORE);
      topicMesh.remove(peerId);
      clearMeshPeerDirection(topic, peerId);
      markPeerDisconnected(topic, peerId, LocalDisconnectReason.SCORE);
      sendPruneMessage(peerId, topic, PruneBackoffOrigin.LOW_SCORE_PRUNE);
      observeMeshConnectionSeparationAfterPrune(
          topic, peerId, PruneBackoffOrigin.LOW_SCORE_PRUNE, true);
    }
  }

  private void applyBrokenPromisePenalties() {
    Iterator<Map.Entry<PromiseKey, PendingPromise>> it = pendingPromises.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<PromiseKey, PendingPromise> entry = it.next();
      PromiseKey key = entry.getKey();
      PendingPromise promise = entry.getValue();
      if (promise.expireAtHeartbeat <= heartbeatCounter) {
        getOrCreateTopicPeerScore(key.topic, key.peerId).brokenPromises++;
        getOrCreateGlobalPenaltyScore(key.peerId).brokenPromises += 0.0;
        it.remove();
      }
    }
  }

  private void applyScoreDecay() {
    for (PeerScoreState state : topicPeerScores.values()) {
      state.firstDeliveries *= GossipCommonConfig.scoreDecayFirstDeliveries;
      state.brokenPromises *= GossipCommonConfig.scoreDecayBrokenPromises;
      state.invalidDeliveries *= GossipCommonConfig.scoreDecayBrokenPromises;
    }
    for (PeerScoreState state : globalPenaltyScores.values()) {
      state.brokenPromises *= GossipCommonConfig.scoreDecayBrokenPromises;
      state.invalidDeliveries *= GossipCommonConfig.scoreDecayBrokenPromises;
    }
  }

  /**
   * Get the current mesh state for topology tracking.
   *
   * @return HashMap of topic to set of peer IDs in mesh
   */
  public Map<String, HashSet<BigInteger>> getMeshState() {
    Map<String, HashSet<BigInteger>> snapshot = new HashMap<>();
    for (Map.Entry<String, HashSet<BigInteger>> entry : mesh.entrySet()) {
      snapshot.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }
    return snapshot;
  }

  private long pruneBackoffFromMessage(Message m) {
    if (m.value instanceof Message.PrunePayload) {
      long backoffHeartbeats = ((Message.PrunePayload) m.value).getBackoffHeartbeats();
      if (backoffHeartbeats > 0) {
        return backoffHeartbeats;
      }
    }
    if (m.value instanceof Number) {
      long backoffHeartbeats = ((Number) m.value).longValue();
      if (backoffHeartbeats > 0) {
        return backoffHeartbeats;
      }
    }
    return GossipCommonConfig.pruneBackoffHeartbeats;
  }

  private Message.PruneReasonTag pruneReasonForOrigin(PruneBackoffOrigin origin) {
    if (origin == null) return Message.PruneReasonTag.OTHER;
    switch (origin) {
      case LOW_SCORE_PRUNE:
        return Message.PruneReasonTag.LOW_SCORE_PRUNE;
      case OVERSUBSCRIPTION_PRUNE:
        return Message.PruneReasonTag.OVERSUBSCRIPTION_PRUNE;
      default:
        return Message.PruneReasonTag.OTHER;
    }
  }

  private Message.PruneReasonTag pruneReasonFromMessage(Message m) {
    if (m.value instanceof Message.PrunePayload) {
      return ((Message.PrunePayload) m.value).getPruneReason();
    }
    return Message.PruneReasonTag.OTHER;
  }

  private List<BigInteger> prunePxFromMessage(Message m) {
    if (m.value instanceof Message.PrunePayload) {
      return ((Message.PrunePayload) m.value).getPxPeers();
    }
    return Collections.emptyList();
  }

  private void applyPruneBackoff(
      String topic, BigInteger peerId, long backoffHeartbeats, PruneBackoffOrigin origin) {
    if (peerId == null || topic == null) return;
    long duration = Math.max(backoffHeartbeats, GossipCommonConfig.pruneBackoffHeartbeats);
    TopicPeerKey key = new TopicPeerKey(topic, peerId);
    pruneBackoffs.put(key, heartbeatCounter + duration);
    pruneBackoffOrigins.put(key, origin);
    if (origin == PruneBackoffOrigin.RECEIVED_PRUNE) {
      lastReceivedPruneHeartbeats.put(key, heartbeatCounter);
    }
    observeRepairRelaxReceivedPruneBackoffReset(topic, peerId);
    switch (origin) {
      case RECEIVED_PRUNE:
        pruneBackoffSetFromReceivedPrune++;
        break;
      case OVERSUBSCRIPTION_PRUNE:
        pruneBackoffSetFromOversubscriptionPrune++;
        break;
      case LOW_SCORE_PRUNE:
        pruneBackoffSetFromLowScorePrune++;
        break;
      case GRAFT_REJECTION:
        pruneBackoffSetFromGraftRejection++;
        break;
      default:
        break;
    }
  }

  private boolean isInPruneBackoff(String topic, BigInteger peerId) {
    TopicPeerKey key = new TopicPeerKey(topic, peerId);
    Long expireAt = pruneBackoffs.get(key);
    if (expireAt == null) return false;
    if (expireAt <= heartbeatCounter) {
      pruneBackoffs.remove(key);
      lastReceivedPruneHeartbeats.remove(key);
      pruneBackoffOrigins.remove(key);
      return false;
    }
    return true;
  }

  private void clearExpiredPruneBackoffs() {
    Iterator<Map.Entry<TopicPeerKey, Long>> it = pruneBackoffs.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TopicPeerKey, Long> entry = it.next();
      if (entry.getValue() <= heartbeatCounter) {
        lastReceivedPruneHeartbeats.remove(entry.getKey());
        pruneBackoffOrigins.remove(entry.getKey());
        it.remove();
      }
    }
  }

  private PruneBackoffOrigin pruneBackoffOrigin(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return PruneBackoffOrigin.OTHER;
    TopicPeerKey key = new TopicPeerKey(topic, peerId);
    Long expireAt = pruneBackoffs.get(key);
    if (expireAt == null || expireAt <= heartbeatCounter) {
      pruneBackoffs.remove(key);
      lastReceivedPruneHeartbeats.remove(key);
      pruneBackoffOrigins.remove(key);
      return PruneBackoffOrigin.OTHER;
    }
    return pruneBackoffOrigins.getOrDefault(key, PruneBackoffOrigin.OTHER);
  }

  private long lastReceivedPruneAge(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return -1L;
    Long receivedAt = lastReceivedPruneHeartbeats.get(new TopicPeerKey(topic, peerId));
    if (receivedAt == null) return -1L;
    return Math.max(0L, heartbeatCounter - receivedAt);
  }

  private long pruneBackoffAgeWindow() {
    return Math.max(1L, GossipCommonConfig.pruneBackoffHeartbeats);
  }

  private long receiverRelaxGraftBackoffMinAgeThreshold() {
    return (long)
        Math.ceil(
            GossipCommonConfig.receiverRelaxGraftBackoffMinAgeFraction
                * (double) pruneBackoffAgeWindow());
  }

  private long repairRelaxReceivedPruneMinAgeThreshold() {
    return (long)
        Math.ceil(
            GossipCommonConfig.repairRelaxReceivedPruneMinAgeFraction
                * (double) pruneBackoffAgeWindow());
  }

  private boolean meetsRepairRelaxReceivedPruneMinAge(String topic, BigInteger peerId) {
    long age = lastReceivedPruneAge(topic, peerId);
    if (age < 0) return false;
    return age >= repairRelaxReceivedPruneMinAgeThreshold();
  }

  private int remainingReceiverRelaxGraftBackoffBudget(String topic) {
    if (!GossipCommonConfig.receiverRelaxGraftBackoffForDOut) return 0;
    return Math.max(
        0,
        GossipCommonConfig.receiverRelaxGraftBackoffBudgetPerHeartbeat
            - receiverRelaxGraftBackoffUsedByTopic.getOrDefault(topic, 0));
  }

  private boolean meetsReceiverRelaxGraftBackoffScore(String topic, BigInteger peerId) {
    if (!isScoringEnabled()) return true;
    double minScore =
        Math.max(
            GossipCommonConfig.receiverRelaxGraftBackoffMinScore,
            GossipCommonConfig.meshScoreThreshold);
    return scoreOf(topic, peerId) >= minScore;
  }

  private boolean meetsReceiverRelaxGraftBackoffMinAge(String topic, BigInteger peerId) {
    long age = lastReceivedPruneAge(topic, peerId);
    if (age < 0) return false;
    return age >= receiverRelaxGraftBackoffMinAgeThreshold();
  }

  private void observeRepairRelaxReceivedPrunePrefilterAgeBucket(long age) {
    if (age < 0) return;
    double fraction = age / (double) pruneBackoffAgeWindow();
    if (fraction < 0.25) {
      repairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff++;
    } else if (fraction < 0.50) {
      repairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff++;
    } else if (fraction < 0.75) {
      repairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff++;
    } else {
      repairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff++;
    }
  }

  private void observeRepairRelaxReceivedPrunePrefilterCandidate(String topic, BigInteger peerId) {
    repairRelaxReceivedPrunePrefilterCandidatesSeen++;
    long age = lastReceivedPruneAge(topic, peerId);
    if (age < 0) {
      repairRelaxReceivedPrunePrefilterAgeUnknown++;
    } else {
      repairRelaxReceivedPrunePrefilterAgeKnown++;
      observeRepairRelaxReceivedPrunePrefilterAgeBucket(age);
    }

    if (meetsRepairRelaxReceivedPruneScore(topic, peerId)) {
      repairRelaxReceivedPrunePrefilterPassScore++;
      if (age >= 0 && age < repairRelaxReceivedPruneMinAgeThreshold()) {
        repairRelaxReceivedPrunePrefilterBlockedByAge++;
      }
    }
  }

  private void observeReceiverRelaxGraftBackoffPrefilterAgeBucket(long age) {
    if (age < 0) return;
    double fraction = age / (double) pruneBackoffAgeWindow();
    if (fraction < 0.25) {
      receiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff++;
    } else if (fraction < 0.50) {
      receiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff++;
    } else if (fraction < 0.75) {
      receiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff++;
    } else {
      receiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff++;
    }
  }

  private void observeReceiverRelaxGraftBackoffScore(double score) {
    receiverRelaxGraftBackoffScoreSum += score;
    receiverRelaxGraftBackoffScoreCount++;
    receiverRelaxGraftBackoffScoreMin = Math.min(receiverRelaxGraftBackoffScoreMin, score);
    receiverRelaxGraftBackoffScoreMax = Math.max(receiverRelaxGraftBackoffScoreMax, score);

    double threshold = GossipCommonConfig.meshScoreThreshold;
    if (score < threshold - 1.0) {
      receiverRelaxGraftBackoffScoreLtThresholdMinusOne++;
    } else if (score < threshold - 0.5) {
      receiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf++;
    } else if (score < threshold) {
      receiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold++;
    } else if (score < threshold + 0.5) {
      receiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf++;
    } else {
      receiverRelaxGraftBackoffScoreGeThresholdPlusHalf++;
    }
  }

  private void observeReceiverRelaxGraftBackoffPrefilterCandidate(String topic, BigInteger peerId) {
    receiverRelaxGraftBackoffPrefilterCandidatesEncountered++;
    double score = scoreOf(topic, peerId);
    observeReceiverRelaxGraftBackoffScore(score);
    long age = lastReceivedPruneAge(topic, peerId);
    if (age < 0) {
      receiverRelaxGraftBackoffPrefilterAgeUnknown++;
    } else {
      receiverRelaxGraftBackoffPrefilterAgeKnown++;
      observeReceiverRelaxGraftBackoffPrefilterAgeBucket(age);
    }

    if (!isScoringEnabled()
        || score
            >= Math.max(
                GossipCommonConfig.receiverRelaxGraftBackoffMinScore,
                GossipCommonConfig.meshScoreThreshold)) {
      receiverRelaxGraftBackoffPrefilterPassScore++;
      if (age >= 0 && age < receiverRelaxGraftBackoffMinAgeThreshold()) {
        receiverRelaxGraftBackoffPrefilterBlockedByAge++;
      }
    }
  }

  private void observeReceiverRelaxGraftBackoffAcceptedAge(String topic, BigInteger peerId) {
    long age = lastReceivedPruneAge(topic, peerId);
    if (age < 0) return;
    double fraction = age / (double) pruneBackoffAgeWindow();
    if (fraction < 0.25) {
      receiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff++;
    } else if (fraction < 0.50) {
      receiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff++;
    } else if (fraction < 0.75) {
      receiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff++;
    } else {
      receiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff++;
    }
  }

  private boolean tryReceiverRelaxGraftBackoff(
      String topic, BigInteger peerId, HashSet<BigInteger> topicMesh) {
    if (!GossipCommonConfig.receiverRelaxGraftBackoffForDOut) {
      return false;
    }
    if (!isDOutTrackingEnabled()) {
      receiverRelaxGraftBackoffBlockedNotDOut++;
      return false;
    }
    if (!meetsReceiverRelaxGraftBackoffScore(topic, peerId)) {
      observeReceiverGraftScoreRejectionDecomposition(topic, peerId);
      receiverRelaxGraftBackoffBlockedByScore++;
      return false;
    }
    if (!meetsReceiverRelaxGraftBackoffMinAge(topic, peerId)) {
      receiverRelaxGraftBackoffBlockedByAge++;
      return false;
    }
    if (remainingReceiverRelaxGraftBackoffBudget(topic) <= 0) {
      receiverRelaxGraftBackoffBlockedByBudget++;
      return false;
    }
    receiverRelaxGraftBackoffUsedByTopic.put(
        topic, receiverRelaxGraftBackoffUsedByTopic.getOrDefault(topic, 0) + 1);
    receiverRelaxGraftBackoffSelected++;
    return true;
  }

  private void observeRepairBackoffOrigin(String topic, BigInteger peerId) {
    PruneBackoffOrigin origin = pruneBackoffOrigin(topic, peerId);
    switch (origin) {
      case RECEIVED_PRUNE:
        repairRejectedByBackoffFromReceivedPrune++;
        break;
      case OVERSUBSCRIPTION_PRUNE:
        repairRejectedByBackoffFromOversubscriptionPrune++;
        break;
      case LOW_SCORE_PRUNE:
        repairRejectedByBackoffFromLowScorePrune++;
        break;
      case GRAFT_REJECTION:
        repairRejectedByBackoffFromGraftRejection++;
        break;
      default:
        break;
    }
    if (isScoringEnabled()) {
      double score = scoreOf(topic, peerId);
      if (score >= 0.0) {
        repairCandidatesBlockedByBackoffPositiveScore++;
      } else {
        repairCandidatesBlockedByBackoffNegativeScore++;
      }
    }
  }

  private void markMeshPeerOutbound(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return;
    meshPeerDirections.put(new TopicPeerKey(topic, peerId), MeshPeerDirection.OUTBOUND);
  }

  private void markMeshPeerInbound(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return;
    TopicPeerKey key = new TopicPeerKey(topic, peerId);
    if (meshPeerDirections.get(key) != MeshPeerDirection.OUTBOUND) {
      meshPeerDirections.put(key, MeshPeerDirection.INBOUND);
    }
  }

  private void clearMeshPeerDirection(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return;
    meshPeerDirections.remove(new TopicPeerKey(topic, peerId));
  }

  private void clearMeshPeerDirections(String topic, Iterable<BigInteger> peerIds) {
    if (peerIds == null) return;
    for (BigInteger peerId : peerIds) {
      clearMeshPeerDirection(topic, peerId);
    }
  }

  private void clearDOutTopicStabilityState(String topic) {
    if (topic == null) return;
    dOutWarmTopics.remove(topic);
    dOutConsecutiveDeficitHeartbeatsByTopic.remove(topic);
    dOutConsecutiveZeroOutboundHeartbeatsByTopic.remove(topic);
    dOutConsecutiveZeroRemainingDeficitByTopic.remove(topic);
  }

  private CandidateMeta getOrCreateCandidateMeta(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return null;
    TopicPeerKey key = new TopicPeerKey(topic, peerId);
    CandidateMeta meta = topicCandidates.get(key);
    if (meta == null) {
      meta = new CandidateMeta();
      topicCandidates.put(key, meta);
    }
    return meta;
  }

  private int localBootstrapTargetPerTopic() {
    int target = Math.max(GossipCommonConfig.prunePeers, GossipCommonConfig.D_high * 3);
    if (GossipCommonConfig.candidatePoolMaxPerTopic > 0) {
      target = Math.min(target, GossipCommonConfig.candidatePoolMaxPerTopic);
    }
    return Math.max(GossipCommonConfig.D, target);
  }

  private LocalConnectionState activeConnectionState(CandidateMeta meta) {
    if (meta == null) {
      return LocalConnectionState.DISCONNECTED;
    }
    return meta.connectionState;
  }

  private long lastKnownRefreshHeartbeat(CandidateMeta meta) {
    if (meta == null) {
      return -1L;
    }
    long lastRefresh = meta.lastDirectSeenHeartbeat;
    lastRefresh = Math.max(lastRefresh, meta.lastPxHintHeartbeat);
    lastRefresh = Math.max(lastRefresh, meta.lastSelectedHeartbeat);
    lastRefresh = Math.max(lastRefresh, meta.lastRegistryBootstrapHeartbeat);
    lastRefresh = Math.max(lastRefresh, meta.lastAmbientDiscoveryHeartbeat);
    return lastRefresh;
  }

  private boolean knownOnlyEntryExpired(String topic, BigInteger peerId, CandidateMeta meta) {
    if (topic == null || peerId == null || meta == null) return false;
    if (GossipCommonConfig.localKnownOnlyTtlHeartbeats <= 0) return false;
    if (activeConnectionState(meta) != LocalConnectionState.DISCONNECTED) return false;
    if (isPeerInMeshOrFanout(topic, peerId)) return false;
    long lastRefresh = lastKnownRefreshHeartbeat(meta);
    return lastRefresh >= 0
        && heartbeatCounter - lastRefresh > GossipCommonConfig.localKnownOnlyTtlHeartbeats;
  }

  private byte effectiveCandidateSourceMask(String topic, BigInteger peerId, CandidateMeta meta) {
    byte activeMask = activeCandidateSourceMask(meta);
    if (activeMask == 0) {
      return 0;
    }
    return knownOnlyEntryExpired(topic, peerId, meta) ? 0 : activeMask;
  }

  private boolean isHeartbeatFresh(long lastHeartbeat, long ttlHeartbeats) {
    return ttlHeartbeats > 0
        && lastHeartbeat >= 0
        && heartbeatCounter - lastHeartbeat <= ttlHeartbeats;
  }

  private CandidateMeta noteDirectActivity(String topic, BigInteger peerId) {
    CandidateMeta meta = getOrCreateCandidateMeta(topic, peerId);
    if (meta == null) return null;
    meta.lastDirectActivityHeartbeat = heartbeatCounter;
    meta.consecutiveIdleHeartbeats = 0;
    meta.consecutiveIdleDisconnects = 0;
    return meta;
  }

  private CandidateMeta notePxActivity(String topic, BigInteger peerId) {
    CandidateMeta meta = getOrCreateCandidateMeta(topic, peerId);
    if (meta == null) return null;
    meta.lastPxActivityHeartbeat = heartbeatCounter;
    meta.consecutiveIdleHeartbeats = 0;
    return meta;
  }

  private CandidateMeta noteMeshActivity(String topic, BigInteger peerId) {
    CandidateMeta meta = getOrCreateCandidateMeta(topic, peerId);
    if (meta == null) return null;
    meta.lastMeshActivityHeartbeat = heartbeatCounter;
    meta.consecutiveIdleHeartbeats = 0;
    meta.consecutiveIdleDisconnects = 0;
    return meta;
  }

  private CandidateMeta noteFanoutActivity(String topic, BigInteger peerId) {
    CandidateMeta meta = getOrCreateCandidateMeta(topic, peerId);
    if (meta == null) return null;
    meta.lastFanoutActivityHeartbeat = heartbeatCounter;
    meta.consecutiveIdleHeartbeats = 0;
    return meta;
  }

  private boolean isConnectedDirectFresh(CandidateMeta meta) {
    return meta != null
        && isHeartbeatFresh(
            meta.lastDirectActivityHeartbeat,
            GossipCommonConfig.localConnectedDirectActivityTtlHeartbeats);
  }

  private boolean isConnectedPxFresh(CandidateMeta meta) {
    return meta != null
        && isHeartbeatFresh(
            meta.lastPxActivityHeartbeat, GossipCommonConfig.localConnectedPxActivityTtlHeartbeats);
  }

  private boolean isConnectedMeshFresh(CandidateMeta meta) {
    return meta != null
        && isHeartbeatFresh(
            meta.lastMeshActivityHeartbeat,
            GossipCommonConfig.localConnectedMeshActivityTtlHeartbeats);
  }

  private boolean isConnectedFanoutFresh(CandidateMeta meta) {
    return meta != null
        && isHeartbeatFresh(
            meta.lastFanoutActivityHeartbeat,
            GossipCommonConfig.localConnectedFanoutActivityTtlHeartbeats);
  }

  private boolean isConnectionFresh(CandidateMeta meta) {
    if (meta == null) return false;
    return isConnectedDirectFresh(meta)
        || isConnectedPxFresh(meta)
        || isConnectedMeshFresh(meta)
        || isConnectedFanoutFresh(meta);
  }

  private boolean isConnectableDirectFresh(CandidateMeta meta) {
    if (meta == null) return false;
    long lastDirectEvidence =
        Math.max(meta.lastDirectSeenHeartbeat, meta.lastDirectActivityHeartbeat);
    return isHeartbeatFresh(
        lastDirectEvidence, GossipCommonConfig.localConnectableDirectActivityTtlHeartbeats);
  }

  private boolean isConnectableMeshFresh(CandidateMeta meta) {
    if (meta == null) return false;
    return isHeartbeatFresh(
        meta.lastMeshActivityHeartbeat,
        GossipCommonConfig.localConnectableMeshActivityTtlHeartbeats);
  }

  private boolean isConnectableConnectedFresh(CandidateMeta meta) {
    if (meta == null) return false;
    long lastConnectedEvidence =
        Math.max(meta.lastConnectionObservedHeartbeat, meta.lastConnectionTransitionHeartbeat);
    return isHeartbeatFresh(
        lastConnectedEvidence, GossipCommonConfig.localConnectableConnectedActivityTtlHeartbeats);
  }

  private boolean isConnectableRegistryFresh(CandidateMeta meta) {
    if (meta == null) return false;
    return isHeartbeatFresh(
        meta.lastRegistryBootstrapHeartbeat,
        GossipCommonConfig.localConnectableRegistryActivityTtlHeartbeats);
  }

  private boolean isConnectablePxFresh(CandidateMeta meta) {
    if (meta == null) return false;
    long lastPxEvidence = Math.max(meta.lastPxHintHeartbeat, meta.lastPxActivityHeartbeat);
    return isHeartbeatFresh(
        lastPxEvidence, GossipCommonConfig.localConnectablePxActivityTtlHeartbeats);
  }

  private boolean isLocallyConnectable(String topic, BigInteger peerId, CandidateMeta meta) {
    if (topic == null || peerId == null || meta == null) return false;
    if (!candidateEntryActive(topic, peerId, meta)) return false;
    if (candidateEntryExpired(topic, peerId, meta)) return false;
    if (activeConnectionState(meta) != LocalConnectionState.DISCONNECTED) {
      return isConnectionFresh(meta) || isConnectableConnectedFresh(meta);
    }
    if (!GossipCommonConfig.discoveryLegacyLocalConnectEnabled) {
      byte activeMask = effectiveCandidateSourceMask(topic, peerId, meta);
      boolean learnedFromPx = (activeMask & CandidateSource.PX_HINT.mask) != 0;
      boolean learnedFromBootstrap =
          (activeMask & CandidateSource.REGISTRY_BOOTSTRAP.mask) != 0
              && discoveryManager.isBootstrapWarmupActive(heartbeatCounter);
      boolean learnedFromAmbient =
          (activeMask & CandidateSource.AMBIENT_DISCOVERY.mask) != 0
              && discoveryManager.isAmbientDiscoveryActive(heartbeatCounter);
      return learnedFromPx || learnedFromBootstrap || learnedFromAmbient;
    }
    return isConnectableDirectFresh(meta)
        || isConnectableMeshFresh(meta)
        || isConnectableConnectedFresh(meta)
        || isConnectablePxFresh(meta)
        || isConnectableRegistryFresh(meta);
  }

  private LocalConnectableSource dominantConnectableSource(CandidateMeta meta) {
    if (meta == null) {
      return LocalConnectableSource.OTHER;
    }
    if (isConnectableDirectFresh(meta)) {
      return LocalConnectableSource.DIRECT;
    }
    if (isConnectableMeshFresh(meta)) {
      return LocalConnectableSource.MESH;
    }
    if (isConnectableConnectedFresh(meta)) {
      return LocalConnectableSource.CONNECTED;
    }
    if (isConnectablePxFresh(meta)) {
      return LocalConnectableSource.PX;
    }
    if (isConnectableRegistryFresh(meta)) {
      return LocalConnectableSource.REGISTRY;
    }
    return LocalConnectableSource.OTHER;
  }

  private LocalKnownNonConnectableSource dominantKnownNonConnectableSource(CandidateMeta meta) {
    if (meta == null) {
      return LocalKnownNonConnectableSource.OTHER;
    }
    LocalCandidateDominantSource dominantSource = dominantCandidateSource(meta);
    if (dominantSource == LocalCandidateDominantSource.PX) {
      return LocalKnownNonConnectableSource.PX;
    }
    if (dominantSource == LocalCandidateDominantSource.REGISTRY) {
      return LocalKnownNonConnectableSource.REGISTRY;
    }
    return LocalKnownNonConnectableSource.OTHER;
  }

  private LocalCandidateDominantSource dominantCandidateSource(CandidateMeta meta) {
    if (meta == null) {
      return LocalCandidateDominantSource.OTHER;
    }
    byte activeMask = activeCandidateSourceMask(meta);
    if ((activeMask & CandidateSource.KNOWN.mask) != 0) {
      return LocalCandidateDominantSource.DIRECT;
    }
    if ((activeMask & CandidateSource.PX_HINT.mask) != 0) {
      return LocalCandidateDominantSource.PX;
    }
    if ((activeMask & CandidateSource.MESH_REPAIR.mask) != 0) {
      return LocalCandidateDominantSource.MESH_REPAIR;
    }
    if ((activeMask & CandidateSource.REGISTRY_BOOTSTRAP.mask) != 0) {
      return LocalCandidateDominantSource.REGISTRY;
    }
    if ((activeMask & CandidateSource.AMBIENT_DISCOVERY.mask) != 0) {
      return LocalCandidateDominantSource.REGISTRY;
    }
    return LocalCandidateDominantSource.OTHER;
  }

  private int localConnectPreferenceRank(CandidateMeta meta) {
    if (meta == null) return 6;
    switch (dominantConnectableSource(meta)) {
      case DIRECT:
        return 0;
      case MESH:
        return 1;
      case CONNECTED:
        return 2;
      case PX:
        return 3;
      case REGISTRY:
        return 4;
      default:
        return 5;
    }
  }

  private long localConnectableRecency(CandidateMeta meta) {
    if (meta == null) {
      return -1L;
    }
    long recency = lastKnownRefreshHeartbeat(meta);
    recency = Math.max(recency, meta.lastDirectActivityHeartbeat);
    recency = Math.max(recency, meta.lastMeshActivityHeartbeat);
    recency = Math.max(recency, meta.lastConnectionObservedHeartbeat);
    recency = Math.max(recency, meta.lastConnectionTransitionHeartbeat);
    recency = Math.max(recency, meta.lastPxActivityHeartbeat);
    recency = Math.max(recency, meta.lastFanoutActivityHeartbeat);
    return recency;
  }

  private Comparator<BigInteger> localReserveTargetPreferenceComparator(String topic) {
    Comparator<BigInteger> bySourceRank =
        Comparator.comparingInt(
            (BigInteger peerId) -> {
              CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
              return localConnectPreferenceRank(meta);
            });
    Comparator<BigInteger> byLastDisconnect =
        Comparator.comparingLong(
            (BigInteger peerId) -> {
              CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
              return meta == null ? Long.MAX_VALUE : meta.lastDisconnectedHeartbeat;
            });
    Comparator<BigInteger> byFreshness =
        Comparator.comparingLong(
                (BigInteger peerId) -> {
                  CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
                  return localConnectableRecency(meta);
                })
            .reversed();
    if (!isScoringEnabled()) {
      return bySourceRank.thenComparing(byLastDisconnect).thenComparing(byFreshness);
    }
    Comparator<BigInteger> byScoreDescending =
        Comparator.comparingDouble((BigInteger peerId) -> scoreOf(topic, peerId)).reversed();
    return bySourceRank
        .thenComparing(byLastDisconnect)
        .thenComparing(byFreshness)
        .thenComparing(byScoreDescending);
  }

  private void observeLocalConnectAttemptSource(LocalCandidateDominantSource source) {
    if (source == null) return;
    switch (source) {
      case DIRECT:
        localConnectAttemptsFromDirect++;
        break;
      case PX:
        localConnectAttemptsFromPx++;
        break;
      case MESH_REPAIR:
        localConnectAttemptsFromMeshRepair++;
        break;
      default:
        break;
    }
  }

  private void observeLocalConnectSuccessSource(LocalCandidateDominantSource source) {
    if (source == null) return;
    switch (source) {
      case DIRECT:
        localConnectSuccessesFromDirect++;
        break;
      case PX:
        localConnectSuccessesFromPx++;
        localPxPromotedToConnected++;
        pxCandidatesAdmittedAndLaterConnected++;
        break;
      case MESH_REPAIR:
        localConnectSuccessesFromMeshRepair++;
        break;
      default:
        break;
    }
  }

  private long pxAdvertisementRecency(CandidateMeta meta) {
    if (meta == null) {
      return -1L;
    }
    long recency = lastKnownRefreshHeartbeat(meta);
    recency = Math.max(recency, meta.lastDirectActivityHeartbeat);
    recency = Math.max(recency, meta.lastPxActivityHeartbeat);
    recency = Math.max(recency, meta.lastMeshActivityHeartbeat);
    recency = Math.max(recency, meta.lastFanoutActivityHeartbeat);
    recency = Math.max(recency, meta.lastConnectionObservedHeartbeat);
    return recency;
  }

  private boolean wasRecentlyPxAdvertised(String topic, BigInteger peerId) {
    if (topic == null || peerId == null || GossipCommonConfig.pxAdvertiseCooldownHeartbeats <= 0) {
      return false;
    }
    Long lastAdvertised = lastPxAdvertisedHeartbeatByTopicPeer.get(new TopicPeerKey(topic, peerId));
    return lastAdvertised != null
        && heartbeatCounter - lastAdvertised < GossipCommonConfig.pxAdvertiseCooldownHeartbeats;
  }

  private void markPxAdvertised(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return;
    lastPxAdvertisedHeartbeatByTopicPeer.put(new TopicPeerKey(topic, peerId), heartbeatCounter);
  }

  private void refreshCurrentMeshAndFanoutActivity() {
    for (Map.Entry<String, HashSet<BigInteger>> entry : mesh.entrySet()) {
      String topic = entry.getKey();
      HashSet<BigInteger> topicMesh = entry.getValue();
      if (topicMesh == null) continue;
      for (BigInteger peerId : topicMesh) {
        noteMeshActivity(topic, peerId);
      }
    }
    for (Map.Entry<String, HashSet<BigInteger>> entry : fanout.entrySet()) {
      String topic = entry.getKey();
      HashSet<BigInteger> topicFanout = entry.getValue();
      if (topicFanout == null) continue;
      for (BigInteger peerId : topicFanout) {
        noteFanoutActivity(topic, peerId);
      }
    }
  }

  private void disconnectIdleLocalPeersForTopic(String topic) {
    if (!GossipCommonConfig.localConnectedIdleDisconnectEnabled || topic == null) {
      return;
    }

    HashSet<BigInteger> retainedSlackPeers = retainedConnectedSlackPeersRaw(topic);
    List<TopicPeerKey> stalePeers = new ArrayList<>();
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!topic.equals(key.topic)) continue;
      if (activeConnectionState(meta) == LocalConnectionState.DISCONNECTED) continue;
      if (isConnectionFresh(meta)) {
        meta.consecutiveIdleHeartbeats = 0;
        continue;
      }

      meta.consecutiveIdleHeartbeats++;
      int idleThreshold = localIdleDisconnectThreshold(meta);
      if (meta.consecutiveIdleHeartbeats < idleThreshold) {
        continue;
      }

      localConnectionConsecutiveIdleThresholdHits++;
      long connectedLifetime =
          meta.connectedSinceHeartbeat >= 0
              ? Math.max(0L, heartbeatCounter - meta.connectedSinceHeartbeat)
              : 0L;
      long minLifetime = localConnectedMinLifetime(meta);
      if (connectedLifetime < minLifetime) {
        continue;
      }
      if (shouldRetainConnectedNonMeshSlack(topic, key.peerId, meta, retainedSlackPeers)) {
        meta.consecutiveIdleHeartbeats = 0;
        continue;
      }
      if (shouldProtectReserveDuringMaturation(meta)) {
        localConnectionDisconnectionBlockedByHold++;
        continue;
      }
      if (hasActiveConnectionHold(topic, key.peerId, meta)) {
        localConnectionDisconnectionBlockedByHold++;
        continue;
      }
      stalePeers.add(key);
    }

    if (stalePeers.isEmpty()) {
      return;
    }

    HashSet<BigInteger> topicMesh = mesh.get(topic);
    HashSet<BigInteger> topicFanout = fanout.get(topic);
    for (TopicPeerKey key : stalePeers) {
      if (topicMesh != null) {
        topicMesh.remove(key.peerId);
      }
      if (topicFanout != null) {
        topicFanout.remove(key.peerId);
      }
      clearMeshPeerDirection(topic, key.peerId);
      localConnectedStaleDropCount++;
      CandidateMeta meta = topicCandidates.get(key);
      if (meta != null) {
        applyIdleDisconnectRetryPenalty(meta);
      }
      markPeerDisconnected(topic, key.peerId, LocalDisconnectReason.IDLE);
    }
  }

  private int remainingLocalConnectBudget(String topic) {
    if (topic == null) return 0;
    int used = localConnectAttemptsUsedByTopic.getOrDefault(topic, 0);
    return Math.max(0, GossipCommonConfig.localConnectAttemptsPerTopicPerHeartbeat - used);
  }

  private long nextLocalConnectRetryDelayHeartbeats(int failures) {
    long delay = Math.max(1L, GossipCommonConfig.localConnectRetryBaseHeartbeats);
    for (int i = 1;
        i < failures && delay < GossipCommonConfig.localConnectRetryMaxHeartbeats;
        i++) {
      delay = Math.min(GossipCommonConfig.localConnectRetryMaxHeartbeats, delay * 2L);
    }
    return delay;
  }

  private void observeLocalConnectionRetryDelay(long delayHeartbeats) {
    if (delayHeartbeats <= 0) {
      return;
    }
    localConnectionRetryDelaySum += delayHeartbeats;
    localConnectionRetryDelayCount++;
    localConnectionRetryDelayMax = Math.max(localConnectionRetryDelayMax, delayHeartbeats);
  }

  private int localIdleDisconnectThreshold(CandidateMeta meta) {
    int threshold = Math.max(1, GossipCommonConfig.localConnectedIdleConsecutiveThreshold);
    if (meta == null) {
      return threshold;
    }
    switch (dominantConnectableSource(meta)) {
      case DIRECT:
      case MESH:
        return threshold + 1;
      case REGISTRY:
        return Math.max(1, threshold - 1);
      default:
        return threshold;
    }
  }

  private long localConnectedMinLifetime(CandidateMeta meta) {
    long lifetime = Math.max(0L, GossipCommonConfig.localConnectedMinLifetimeHeartbeats);
    if (meta == null) {
      return lifetime;
    }
    switch (dominantConnectableSource(meta)) {
      case DIRECT:
      case MESH:
        return lifetime + 1L;
      case REGISTRY:
        return Math.max(0L, lifetime - 1L);
      default:
        return lifetime;
    }
  }

  private boolean hasActiveConnectionHold(String topic, BigInteger peerId, CandidateMeta meta) {
    if (topic == null || peerId == null || meta == null) {
      return false;
    }
    if (activeConnectionState(meta) == LocalConnectionState.DISCONNECTED) {
      return false;
    }
    if (!meta.holdForReserve) {
      return false;
    }
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    if (topicMesh != null && topicMesh.contains(peerId)) {
      return false;
    }
    return meta.holdUntilHeartbeat >= 0 && heartbeatCounter < meta.holdUntilHeartbeat;
  }

  private boolean isReserveConnectedTagged(CandidateMeta meta) {
    return meta != null
        && meta.reserveConnected
        && activeConnectionState(meta) != LocalConnectionState.DISCONNECTED;
  }

  private boolean isReserveConnectedPeer(String topic, BigInteger peerId, CandidateMeta meta) {
    if (topic == null || peerId == null || meta == null) {
      return false;
    }
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    return isReserveConnectedTagged(meta) && (topicMesh == null || !topicMesh.contains(peerId));
  }

  private boolean isReserveConnectedMature(CandidateMeta meta) {
    if (!isReserveConnectedTagged(meta)) {
      return false;
    }
    return meta.reserveMatureAtHeartbeat < 0 || heartbeatCounter >= meta.reserveMatureAtHeartbeat;
  }

  private boolean isReserveConnectedImmature(CandidateMeta meta) {
    return isReserveConnectedTagged(meta) && !isReserveConnectedMature(meta);
  }

  private void clearReserveConnectedTag(CandidateMeta meta) {
    if (meta == null) {
      return;
    }
    meta.reserveConnected = false;
    meta.reserveMatureAtHeartbeat = -1L;
    meta.holdUntilHeartbeat = -1L;
    meta.holdForReserve = false;
  }

  private long reserveMaturationTargetHeartbeat() {
    return heartbeatCounter + Math.max(0L, GossipCommonConfig.localReserveMaturationHeartbeats);
  }

  private boolean shouldProtectReserveDuringMaturation(CandidateMeta meta) {
    return GossipCommonConfig.localReserveDisconnectProtectionDuringMaturation
        && isReserveConnectedImmature(meta);
  }

  private long reserveConnectedLifetime(CandidateMeta meta) {
    if (meta == null
        || meta.connectedSinceHeartbeat < 0
        || heartbeatCounter < meta.connectedSinceHeartbeat) {
      return 0L;
    }
    return heartbeatCounter - meta.connectedSinceHeartbeat;
  }

  private int reserveConnectedCount(String topic) {
    if (topic == null) {
      return 0;
    }
    int count = 0;
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      if (!topic.equals(key.topic)) {
        continue;
      }
      if (isReserveConnectedPeer(key.topic, key.peerId, entry.getValue())) {
        count++;
      }
    }
    return count;
  }

  private boolean isReserveConnectedMeshPromotable(
      String topic,
      BigInteger peerId,
      CandidateMeta meta,
      int reserveConnectedCount,
      boolean allowNoAlternativeFallback) {
    if (!isReserveConnectedPeer(topic, peerId, meta)) {
      return true;
    }
    if (!isReserveConnectedMature(meta)) {
      return false;
    }
    if (reserveConnectedCount > connectedReserveTargetPerTopic()) {
      return true;
    }
    return allowNoAlternativeFallback;
  }

  private void observeReserveConnectedPromotedToMesh(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) {
      return;
    }
    CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
    if (!isReserveConnectedTagged(meta)) {
      return;
    }
    long reserveLifetime = reserveConnectedLifetime(meta);
    localReserveConnectedLifetimeSum += reserveLifetime;
    localReserveConnectedLifetimeCount++;
    localReserveConnectedLifetimeBeforePromotionSum += reserveLifetime;
    localReserveConnectedLifetimeBeforePromotionCount++;
    localReserveConnectedConsumedByMesh++;
    if (!isReserveConnectedMature(meta)) {
      localReserveConnectedConsumedBeforeMaturity++;
    }
    localReserveConnectedPromotedToMesh++;
    clearReserveConnectedTag(meta);
  }

  private void applyIdleDisconnectRetryPenalty(CandidateMeta meta) {
    if (meta == null) {
      return;
    }
    meta.consecutiveIdleDisconnects++;
    int penaltyFailures = Math.max(1, meta.consecutiveIdleDisconnects);
    long delay =
        Math.max(
            GossipCommonConfig.localIdleDisconnectRetryPenaltyHeartbeats,
            nextLocalConnectRetryDelayHeartbeats(penaltyFailures));
    meta.retryAfterHeartbeat = heartbeatCounter + delay;
    observeLocalConnectionRetryDelay(delay);
  }

  private boolean canAttemptLocalConnect(String topic, BigInteger peerId, CandidateMeta meta) {
    if (topic == null || peerId == null || meta == null) return false;
    if (this.node != null && peerId.equals(this.node.getId())) return false;
    if (activeConnectionState(meta) != LocalConnectionState.DISCONNECTED) return false;
    if (!candidateEntryActive(topic, peerId, meta)) return false;
    if (candidateEntryExpired(topic, peerId, meta)) return false;
    if (!isLocallyConnectable(topic, peerId, meta)) return false;
    return meta.retryAfterHeartbeat < 0 || heartbeatCounter >= meta.retryAfterHeartbeat;
  }

  private boolean attemptLocalConnect(
      String topic, BigInteger peerId, CandidateMeta meta, LocalConnectionTransitionReason reason) {
    if (topic == null || peerId == null || meta == null || reason == null) return false;
    if (remainingLocalConnectBudget(topic) <= 0) return false;

    LocalCandidateDominantSource dominantSource = dominantCandidateSource(meta);
    localConnectAttemptsUsedByTopic.merge(topic, 1, Integer::sum);
    localConnectAttempts++;
    if (reason == LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_RESERVE) {
      localConnectAttemptsForReserve++;
    }
    observeLocalConnectAttemptSource(dominantSource);
    meta.lastConnectAttemptHeartbeat = heartbeatCounter;

    if (!canAttemptLocalConnect(topic, peerId, meta)) {
      meta.consecutiveConnectFailures++;
      long retryDelay = nextLocalConnectRetryDelayHeartbeats(meta.consecutiveConnectFailures);
      meta.retryAfterHeartbeat = heartbeatCounter + retryDelay;
      observeLocalConnectionRetryDelay(retryDelay);
      localConnectFailures++;
      if (reason == LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_RESERVE) {
        localConnectFailuresForReserve++;
      }
      return false;
    }

    boolean dialSucceeded = GossipCommonConfig.discoveryLegacyLocalConnectEnabled;
    if (!dialSucceeded) {
      BootstrapDiscoveryManager.DialResult dialResult =
          discoveryManager.requestDial(
              topic,
              this.node == null ? null : this.node.getId(),
              peerId,
              reason.name(),
              heartbeatCounter);
      dialSucceeded = dialResult == BootstrapDiscoveryManager.DialResult.SUCCESS;
    }
    if (!dialSucceeded) {
      discoveryManager.onDialFailure(topic, peerId, reason.name(), heartbeatCounter);
      meta.consecutiveConnectFailures++;
      long retryDelay = nextLocalConnectRetryDelayHeartbeats(meta.consecutiveConnectFailures);
      meta.retryAfterHeartbeat = heartbeatCounter + retryDelay;
      observeLocalConnectionRetryDelay(retryDelay);
      localConnectFailures++;
      if (reason == LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_RESERVE) {
        localConnectFailuresForReserve++;
      }
      return false;
    }

    markPeerConnected(topic, peerId, LocalConnectionState.OUTBOUND_CONNECTED, reason);
    discoveryManager.onDialSuccess(topic, peerId, reason.name(), heartbeatCounter);
    meta.consecutiveConnectFailures = 0;
    meta.retryAfterHeartbeat = -1L;
    localConnectSuccesses++;
    if (reason == LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_RESERVE) {
      localConnectSuccessesForReserve++;
    }
    observeLocalConnectSuccessSource(dominantSource);
    return true;
  }

  private int connectedReserveTargetPerTopic() {
    return Math.max(0, GossipCommonConfig.localConnectedReserveTargetPerTopic);
  }

  private int connectedSlackTargetPerTopic() {
    return connectedReserveTargetPerTopic();
  }

  private int connectedTargetForTopic(String topic) {
    if (topic == null) {
      return connectedSlackTargetPerTopic();
    }
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    int meshCount = topicMesh == null ? 0 : topicMesh.size();
    return meshCount + connectedSlackTargetPerTopic();
  }

  private boolean isConnectedNonMeshPeer(String topic, BigInteger peerId, CandidateMeta meta) {
    if (topic == null || peerId == null || meta == null) {
      return false;
    }
    if (activeConnectionState(meta) == LocalConnectionState.DISCONNECTED) {
      return false;
    }
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    return topicMesh == null || !topicMesh.contains(peerId);
  }

  private List<BigInteger> connectedNonMeshPeersRaw(String topic) {
    List<BigInteger> peersForTopic = new ArrayList<>();
    if (topic == null) {
      return peersForTopic;
    }
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!topic.equals(key.topic)) {
        continue;
      }
      if (isConnectedNonMeshPeer(key.topic, key.peerId, meta)) {
        peersForTopic.add(key.peerId);
      }
    }
    return peersForTopic;
  }

  private int connectedNonMeshCountRaw(String topic) {
    return connectedNonMeshPeersRaw(topic).size();
  }

  private HashSet<BigInteger> retainedConnectedSlackPeersRaw(String topic) {
    HashSet<BigInteger> retainedPeers = new HashSet<>();
    int slackTarget = connectedSlackTargetPerTopic();
    if (topic == null || slackTarget <= 0) {
      return retainedPeers;
    }

    List<BigInteger> connectedNonMeshPeers = connectedNonMeshPeersRaw(topic);
    if (connectedNonMeshPeers.isEmpty()) {
      return retainedPeers;
    }

    connectedNonMeshPeers.sort(localReserveTargetPreferenceComparator(topic));
    for (BigInteger peerId : connectedNonMeshPeers) {
      if (retainedPeers.size() >= slackTarget) {
        break;
      }
      retainedPeers.add(peerId);
    }
    return retainedPeers;
  }

  private boolean shouldRetainConnectedNonMeshSlack(
      String topic, BigInteger peerId, CandidateMeta meta, HashSet<BigInteger> retainedPeers) {
    if (!isConnectedNonMeshPeer(topic, peerId, meta)) {
      return false;
    }
    return retainedPeers != null && retainedPeers.contains(peerId);
  }

  private int connectedNonMeshCount(String topic) {
    if (topic == null) {
      return 0;
    }
    disconnectIdleLocalPeersForTopic(topic);
    return connectedNonMeshCountRaw(topic);
  }

  private void maybeMaintainConnectedReserve(String topic) {
    if (topic == null) {
      return;
    }
    maybeBootstrapTopicKnowledge(topic);
    int slackTarget = connectedSlackTargetPerTopic();
    if (slackTarget <= 0 || remainingLocalConnectBudget(topic) <= 0) {
      return;
    }

    int reserveGap = Math.max(0, slackTarget - connectedNonMeshCount(topic));
    if (reserveGap <= 0) {
      return;
    }

    HashSet<BigInteger> excludedPeers = new HashSet<>();
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    if (topicMesh != null) {
      excludedPeers.addAll(topicMesh);
    }
    HashSet<BigInteger> topicFanout = fanout.get(topic);
    if (topicFanout != null) {
      excludedPeers.addAll(topicFanout);
    }

    List<BigInteger> candidates =
        new ArrayList<>(eligibleConnectableDisconnectedCandidatesForTopic(topic, excludedPeers));
    if (candidates.isEmpty()) {
      return;
    }

    if (isScoringEnabled()) {
      candidates.sort(localReserveTargetPreferenceComparator(topic));
    } else {
      Collections.shuffle(candidates, CommonState.r);
      candidates.sort(localReserveTargetPreferenceComparator(topic));
    }

    for (BigInteger peerId : candidates) {
      if (reserveGap <= 0 || remainingLocalConnectBudget(topic) <= 0) {
        break;
      }
      CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
      if (meta == null || !canAttemptLocalConnect(topic, peerId, meta)) {
        continue;
      }
      if (attemptLocalConnect(
          topic, peerId, meta, LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_RESERVE)) {
        reserveGap--;
      }
    }
  }

  private void trimConnectedNonMeshAboveSlack(String topic) {
    if (topic == null) {
      return;
    }
    int slackTarget = connectedSlackTargetPerTopic();
    int excess = connectedNonMeshCount(topic) - slackTarget;
    if (excess <= 0) {
      return;
    }

    List<BigInteger> candidates = connectedNonMeshPeersRaw(topic);
    if (candidates.isEmpty()) {
      return;
    }

    Comparator<BigInteger> weakestFirst = localReserveTargetPreferenceComparator(topic).reversed();
    candidates.sort(weakestFirst);

    HashSet<BigInteger> topicFanout = fanout.get(topic);
    for (BigInteger peerId : candidates) {
      if (excess <= 0) {
        break;
      }
      CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
      if (meta == null) {
        continue;
      }
      if (shouldProtectReserveDuringMaturation(meta)
          || hasActiveConnectionHold(topic, peerId, meta)) {
        continue;
      }
      if (!isConnectedNonMeshPeer(topic, peerId, meta)) {
        continue;
      }
      if (topicFanout != null) {
        topicFanout.remove(peerId);
      }
      markPeerDisconnected(topic, peerId, LocalDisconnectReason.CAPACITY);
      excess--;
    }
  }

  private void observeConnectedNonMeshPromotedToMesh(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) {
      return;
    }
    CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
    if (isConnectedNonMeshPeer(topic, peerId, meta)) {
      localConnectedNonMeshPromotedToMesh++;
    }
  }

  private void observeConnectedNonMeshUsedForFanout(Collection<BigInteger> peerIds) {
    if (peerIds == null || peerIds.isEmpty()) {
      return;
    }
    localConnectedNonMeshUsedForFanout += peerIds.size();
  }

  private void observeConnectedNonMeshUsedForGossip(Collection<BigInteger> peerIds) {
    if (peerIds == null || peerIds.isEmpty()) {
      return;
    }
    localConnectedNonMeshUsedForGossip += peerIds.size();
  }

  private void markPeerConnected(
      String topic,
      BigInteger peerId,
      LocalConnectionState state,
      LocalConnectionTransitionReason reason) {
    if (topic == null
        || peerId == null
        || state == null
        || reason == null
        || state == LocalConnectionState.DISCONNECTED) {
      return;
    }
    CandidateMeta meta = getOrCreateCandidateMeta(topic, peerId);
    if (meta == null) return;

    LocalConnectionState previousState = activeConnectionState(meta);
    if (previousState != state) {
      meta.lastConnectionTransitionHeartbeat = heartbeatCounter;
      meta.lastConnectionTransitionReason = reason;
      if (state == LocalConnectionState.OUTBOUND_CONNECTED) {
        localConnectionTransitionOutbound++;
      } else if (state == LocalConnectionState.INBOUND_CONNECTED) {
        localConnectionTransitionInbound++;
      }
    }
    if (previousState == LocalConnectionState.DISCONNECTED) {
      if (meta.lastDisconnectedHeartbeat >= 0
          && heartbeatCounter >= meta.lastDisconnectedHeartbeat) {
        long reconnectDelay = heartbeatCounter - meta.lastDisconnectedHeartbeat;
        if (reconnectDelay <= 5L) {
          localConnectionReconnectSamePeerWithin5Heartbeats++;
          localConnectionFlappingPeers.add(new TopicPeerKey(topic, peerId));
        }
        if (reconnectDelay <= 10L) {
          localConnectionReconnectSamePeerWithin10Heartbeats++;
          localConnectionFlappingPeers.add(new TopicPeerKey(topic, peerId));
        }
      }
      meta.connectedSinceHeartbeat = heartbeatCounter;
      if (reason == LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_RESERVE
          && (GossipCommonConfig.localConnectedMinHoldHeartbeats > 0
              || GossipCommonConfig.localReserveMaturationHeartbeats > 0)) {
        meta.reserveConnected = true;
        meta.reserveMatureAtHeartbeat = reserveMaturationTargetHeartbeat();
        long holdUntil = heartbeatCounter + GossipCommonConfig.localConnectedMinHoldHeartbeats;
        if (GossipCommonConfig.localReserveDisconnectProtectionDuringMaturation) {
          holdUntil = Math.max(holdUntil, meta.reserveMatureAtHeartbeat);
        }
        meta.holdUntilHeartbeat = holdUntil;
        meta.holdForReserve = holdUntil > heartbeatCounter;
      } else {
        meta.reserveConnected = false;
        meta.reserveMatureAtHeartbeat = -1L;
        meta.holdUntilHeartbeat = -1L;
        meta.holdForReserve = false;
      }
    }
    meta.connectionState = state;
    meta.lastConnectionObservedHeartbeat = heartbeatCounter;
    if (reason == LocalConnectionTransitionReason.DIRECT_INBOUND_ACTIVITY) {
      meta.lastDirectActivityHeartbeat = heartbeatCounter;
      meta.consecutiveIdleDisconnects = 0;
    }
    meta.consecutiveIdleHeartbeats = 0;
    meta.lastDisconnectReason = null;
    meta.consecutiveConnectFailures = 0;
    meta.retryAfterHeartbeat = -1L;
  }

  private void markPeerDisconnected(String topic, BigInteger peerId, LocalDisconnectReason reason) {
    if (topic == null || peerId == null || reason == null) return;
    CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
    if (meta == null) return;

    LocalConnectionState previousState = activeConnectionState(meta);
    if (previousState == LocalConnectionState.DISCONNECTED) {
      meta.connectionState = LocalConnectionState.DISCONNECTED;
      meta.lastConnectionTransitionHeartbeat = heartbeatCounter;
      meta.lastDisconnectedHeartbeat = heartbeatCounter;
      meta.consecutiveIdleHeartbeats = 0;
      meta.lastDisconnectReason = reason;
      return;
    }

    localConnectionTransitionDisconnected++;
    long connectedLifetime = 0L;
    boolean reserveConnectedPeer = isReserveConnectedPeer(topic, peerId, meta);
    boolean reserveConnectedMature = isReserveConnectedMature(meta);
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    boolean connectedNonMeshPeer = topicMesh == null || !topicMesh.contains(peerId);
    if (meta.connectedSinceHeartbeat >= 0 && heartbeatCounter >= meta.connectedSinceHeartbeat) {
      connectedLifetime = heartbeatCounter - meta.connectedSinceHeartbeat;
      localConnectionLifetimeSum += connectedLifetime;
      localConnectionLifetimeCount++;
      if (connectedNonMeshPeer) {
        localConnectedNonMeshLifetimeSum += connectedLifetime;
        localConnectedNonMeshLifetimeCount++;
      }
    }
    if (reserveConnectedPeer) {
      localReserveConnectedLifetimeSum += connectedLifetime;
      localReserveConnectedLifetimeCount++;
      localReserveConnectedDisconnectedBeforePromotion++;
      if (reserveConnectedMature) {
        localReserveConnectedDisconnectedAfterMaturity++;
      } else {
        localReserveConnectedDisconnectedBeforeMaturity++;
      }
    }

    switch (reason) {
      case RECEIVED_PRUNE:
        localConnectionDisconnectedByReceivedPrune++;
        break;
      case LOCAL_PRUNE:
        localConnectionDisconnectedByLocalPrune++;
        break;
      case SCORE:
        localConnectionDisconnectedByScore++;
        if (connectedNonMeshPeer) {
          localConnectedNonMeshDisconnectedByScore++;
        }
        break;
      case CAPACITY:
        localConnectionDisconnectedByCapacity++;
        if (connectedNonMeshPeer) {
          localConnectedNonMeshDisconnectedByCapacity++;
        }
        break;
      case IDLE:
        localConnectionDisconnectedByIdle++;
        if (meta.holdForReserve
            && meta.holdUntilHeartbeat >= 0
            && heartbeatCounter >= meta.holdUntilHeartbeat) {
          localConnectionHoldExpiredDisconnects++;
        }
        if (connectedLifetime < localConnectedMinLifetime(meta)) {
          localConnectionIdleDisconnectsBeforeMinLifetime++;
        } else {
          localConnectionIdleDisconnectsAfterMinLifetime++;
        }
        break;
      case EXPIRY:
        localConnectionDisconnectedByExpiry++;
        if (connectedNonMeshPeer) {
          localConnectedNonMeshDisconnectedByExpiry++;
        }
        break;
      default:
        localConnectionDisconnectedByOther++;
        break;
    }

    meta.connectionState = LocalConnectionState.DISCONNECTED;
    meta.connectedSinceHeartbeat = -1L;
    clearReserveConnectedTag(meta);
    meta.lastConnectionObservedHeartbeat = -1L;
    meta.lastConnectionTransitionHeartbeat = heartbeatCounter;
    meta.lastDisconnectedHeartbeat = heartbeatCounter;
    meta.consecutiveIdleHeartbeats = 0;
    if (reason != LocalDisconnectReason.IDLE) {
      meta.consecutiveIdleDisconnects = 0;
    }
    meta.lastDisconnectReason = reason;
  }

  private void observeMeshConnectionSeparationAfterPrune(
      String topic, BigInteger peerId, PruneBackoffOrigin origin, boolean removedFromMesh) {
    if (topic == null || peerId == null || origin == null || !removedFromMesh) {
      return;
    }
    if (!isConnectedLocalPeer(topic, peerId)) {
      return;
    }
    localMeshRemovedButStillConnectedCount++;
    localConnectedNonMeshSurvivedAfterPrune++;
    switch (origin) {
      case RECEIVED_PRUNE:
        localConnectionPreservedAfterReceivedPrune++;
        break;
      case OVERSUBSCRIPTION_PRUNE:
        localConnectionPreservedAfterLocalOversubscriptionPrune++;
        break;
      case LOW_SCORE_PRUNE:
        localConnectionPreservedAfterLocalLowScorePrune++;
        break;
      default:
        break;
    }
  }

  private void recordCandidateSeen(String topic, BigInteger peerId, CandidateSource source) {
    if (topic == null || peerId == null || source == null) return;
    if (this.node != null && peerId.equals(this.node.getId())) return;
    if (!isDiscoverablePeer(peerId)) return;

    CandidateMeta meta = getOrCreateCandidateMeta(topic, peerId);
    if (meta == null) return;

    meta.sourceMask |= source.mask;
    switch (source) {
      case KNOWN:
        meta.lastDirectSeenHeartbeat = heartbeatCounter;
        meta.lastDirectActivityHeartbeat = heartbeatCounter;
        break;
      case PX_HINT:
        meta.lastPxHintHeartbeat = heartbeatCounter;
        meta.lastPxActivityHeartbeat = heartbeatCounter;
        break;
      case MESH_REPAIR:
        meta.lastSelectedHeartbeat = heartbeatCounter;
        break;
      case REGISTRY_BOOTSTRAP:
        meta.lastRegistryBootstrapHeartbeat = heartbeatCounter;
        break;
      case AMBIENT_DISCOVERY:
        meta.lastAmbientDiscoveryHeartbeat = heartbeatCounter;
        break;
    }
  }

  private byte activeCandidateSourceMask(CandidateMeta meta) {
    if (meta == null) return 0;

    byte mask = 0;
    if (GossipCommonConfig.candidateTtlHeartbeats > 0
        && meta.lastDirectSeenHeartbeat >= 0
        && heartbeatCounter - meta.lastDirectSeenHeartbeat
            <= GossipCommonConfig.candidateTtlHeartbeats) {
      mask |= CandidateSource.KNOWN.mask;
    }
    if (GossipCommonConfig.candidateTtlHeartbeats > 0
        && meta.lastRegistryBootstrapHeartbeat >= 0
        && heartbeatCounter - meta.lastRegistryBootstrapHeartbeat
            <= GossipCommonConfig.candidateTtlHeartbeats) {
      mask |= CandidateSource.REGISTRY_BOOTSTRAP.mask;
    }
    if (GossipCommonConfig.candidateTtlHeartbeats > 0
        && meta.lastAmbientDiscoveryHeartbeat >= 0
        && heartbeatCounter - meta.lastAmbientDiscoveryHeartbeat
            <= GossipCommonConfig.candidateTtlHeartbeats) {
      mask |= CandidateSource.AMBIENT_DISCOVERY.mask;
    }
    if (GossipCommonConfig.candidatePxHintTtlHeartbeats > 0
        && meta.lastPxHintHeartbeat >= 0
        && heartbeatCounter - meta.lastPxHintHeartbeat
            <= GossipCommonConfig.candidatePxHintTtlHeartbeats) {
      mask |= CandidateSource.PX_HINT.mask;
    }
    if (GossipCommonConfig.candidateSelectedTtlHeartbeats > 0
        && meta.lastSelectedHeartbeat >= 0
        && heartbeatCounter - meta.lastSelectedHeartbeat
            <= GossipCommonConfig.candidateSelectedTtlHeartbeats) {
      mask |= CandidateSource.MESH_REPAIR.mask;
    }

    return mask;
  }

  private boolean candidateEntryActive(String topic, BigInteger peerId, CandidateMeta meta) {
    if (topic == null || peerId == null || meta == null) return false;
    return effectiveCandidateSourceMask(topic, peerId, meta) != 0
        || activeConnectionState(meta) != LocalConnectionState.DISCONNECTED;
  }

  private boolean isPeerInMeshOrFanout(String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return false;
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    if (topicMesh != null && topicMesh.contains(peerId)) return true;
    HashSet<BigInteger> topicFanout = fanout.get(topic);
    return topicFanout != null && topicFanout.contains(peerId);
  }

  private boolean candidateEntryExpired(String topic, BigInteger peerId, CandidateMeta meta) {
    if (topic == null || peerId == null || meta == null) return true;
    return effectiveCandidateSourceMask(topic, peerId, meta) == 0
        && activeConnectionState(meta) == LocalConnectionState.DISCONNECTED
        && !isPeerInMeshOrFanout(topic, peerId);
  }

  private void cleanupCandidatePool() {
    if (heartbeatCounter % GossipCommonConfig.candidateCleanupIntervalHeartbeats != 0) return;

    HashMap<String, List<Map.Entry<TopicPeerKey, CandidateMeta>>> activeEntriesByTopic =
        new HashMap<>();

    Iterator<Map.Entry<TopicPeerKey, CandidateMeta>> it = topicCandidates.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TopicPeerKey, CandidateMeta> entry = it.next();
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      byte previousMask = meta.sourceMask;
      byte activeMask = effectiveCandidateSourceMask(key.topic, key.peerId, meta);
      LocalConnectionState previousConnectionState = meta.connectionState;
      LocalConnectionState currentConnectionState = activeConnectionState(meta);

      if (previousConnectionState != LocalConnectionState.DISCONNECTED
          && activeMask == 0
          && !isPeerInMeshOrFanout(key.topic, key.peerId)
          && currentConnectionState != LocalConnectionState.DISCONNECTED) {
        localCatalogExpiredConnectedRemoved++;
        markPeerDisconnected(key.topic, key.peerId, LocalDisconnectReason.EXPIRY);
      }

      if ((previousMask & CandidateSource.REGISTRY_BOOTSTRAP.mask) != 0
          && (activeMask & CandidateSource.REGISTRY_BOOTSTRAP.mask) == 0) {
        localCatalogExpiredRegistryRemoved++;
      }
      if ((previousMask & CandidateSource.KNOWN.mask) != 0
          && (activeMask & CandidateSource.KNOWN.mask) == 0) {
        localCatalogExpiredKnownRemoved++;
      }

      if ((previousMask & CandidateSource.PX_HINT.mask) != 0
          && (activeMask & CandidateSource.PX_HINT.mask) == 0) {
        candidatePoolPxExpiredRemoved++;
        HashSet<BigInteger> pxHints = pxPeerHints.get(key.topic);
        if (pxHints != null) {
          pxHints.remove(key.peerId);
        }
      }
      if ((previousMask & CandidateSource.MESH_REPAIR.mask) != 0
          && (activeMask & CandidateSource.MESH_REPAIR.mask) == 0) {
        localCatalogExpiredMeshRepairRemoved++;
      }

      meta.sourceMask = activeMask;
      if (candidateEntryExpired(key.topic, key.peerId, meta)) {
        HashSet<BigInteger> pxHints = pxPeerHints.get(key.topic);
        if (pxHints != null) {
          pxHints.remove(key.peerId);
        }
        lastPxAdvertisedHeartbeatByTopicPeer.remove(key);
        it.remove();
        candidatePoolExpiredRemoved++;
        continue;
      }

      if (candidateEntryActive(key.topic, key.peerId, meta)) {
        activeEntriesByTopic.computeIfAbsent(key.topic, unused -> new ArrayList<>()).add(entry);
      }
    }

    if (GossipCommonConfig.candidatePoolMaxPerTopic <= 0) return;

    for (Map.Entry<String, List<Map.Entry<TopicPeerKey, CandidateMeta>>> topicEntry :
        activeEntriesByTopic.entrySet()) {
      List<Map.Entry<TopicPeerKey, CandidateMeta>> activeEntries = topicEntry.getValue();
      int overflow = activeEntries.size() - GossipCommonConfig.candidatePoolMaxPerTopic;
      if (overflow <= 0) continue;

      List<Map.Entry<TopicPeerKey, CandidateMeta>> removableEntries = new ArrayList<>();
      for (Map.Entry<TopicPeerKey, CandidateMeta> entry : activeEntries) {
        TopicPeerKey key = entry.getKey();
        if (!isPeerInMeshOrFanout(key.topic, key.peerId)
            && activeConnectionState(entry.getValue()) == LocalConnectionState.DISCONNECTED) {
          removableEntries.add(entry);
        }
      }

      removableEntries.sort(
          (left, right) -> {
            CandidateMeta leftMeta = left.getValue();
            CandidateMeta rightMeta = right.getValue();
            byte leftMask =
                effectiveCandidateSourceMask(left.getKey().topic, left.getKey().peerId, leftMeta);
            byte rightMask =
                effectiveCandidateSourceMask(
                    right.getKey().topic, right.getKey().peerId, rightMeta);

            boolean leftPxOnly =
                (leftMask & CandidateSource.PX_HINT.mask) != 0
                    && (leftMask & CandidateSource.KNOWN.mask) == 0;
            boolean rightPxOnly =
                (rightMask & CandidateSource.PX_HINT.mask) != 0
                    && (rightMask & CandidateSource.KNOWN.mask) == 0;
            if (leftPxOnly != rightPxOnly) {
              return leftPxOnly ? -1 : 1;
            }

            int pxHintAgeOrder =
                Long.compare(leftMeta.lastPxHintHeartbeat, rightMeta.lastPxHintHeartbeat);
            if (pxHintAgeOrder != 0) {
              return pxHintAgeOrder;
            }

            boolean leftNeverSelected = leftMeta.lastSelectedHeartbeat < 0;
            boolean rightNeverSelected = rightMeta.lastSelectedHeartbeat < 0;
            if (leftNeverSelected != rightNeverSelected) {
              return leftNeverSelected ? -1 : 1;
            }

            int directAgeOrder =
                Long.compare(leftMeta.lastDirectSeenHeartbeat, rightMeta.lastDirectSeenHeartbeat);
            if (directAgeOrder != 0) {
              return directAgeOrder;
            }

            return Long.compare(leftMeta.lastSelectedHeartbeat, rightMeta.lastSelectedHeartbeat);
          });

      for (Map.Entry<TopicPeerKey, CandidateMeta> removableEntry : removableEntries) {
        if (overflow <= 0) break;
        if (topicCandidates.remove(removableEntry.getKey()) != null) {
          HashSet<BigInteger> pxHints = pxPeerHints.get(removableEntry.getKey().topic);
          if (pxHints != null) {
            pxHints.remove(removableEntry.getKey().peerId);
          }
          lastPxAdvertisedHeartbeatByTopicPeer.remove(removableEntry.getKey());
          candidatePoolCappedRemoved++;
          overflow--;
        }
      }
    }
  }

  private void observeCandidatePool() {
    HashMap<String, long[]> countsByTopic = new HashMap<>();
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      byte activeMask = effectiveCandidateSourceMask(key.topic, key.peerId, meta);
      if (!candidateEntryActive(key.topic, key.peerId, meta)) continue;

      long[] counts = countsByTopic.computeIfAbsent(key.topic, unused -> new long[5]);
      counts[0]++;
      if ((activeMask & CandidateSource.KNOWN.mask) != 0) {
        counts[1]++;
      }
      if ((activeMask & CandidateSource.PX_HINT.mask) != 0) {
        counts[2]++;
      }
      if ((activeMask & CandidateSource.MESH_REPAIR.mask) != 0) {
        counts[3]++;
      }
      if ((activeMask & CandidateSource.REGISTRY_BOOTSTRAP.mask) != 0) {
        counts[4]++;
      }
    }

    HashSet<String> allTopics = new HashSet<>();
    allTopics.addAll(mesh.keySet());
    allTopics.addAll(fanout.keySet());
    allTopics.addAll(countsByTopic.keySet());

    for (String topic : allTopics) {
      long[] counts = countsByTopic.getOrDefault(topic, new long[5]);
      candidatePoolActiveSum += counts[0];
      candidatePoolKnownActiveSum += counts[1];
      candidatePoolPxActiveSum += counts[2];
      candidatePoolMeshRepairActiveSum += counts[3];
      candidatePoolRegistryActiveSum += counts[4];
      candidatePoolObservationCount++;
    }
  }

  private void maybeBootstrapTopicKnowledge(String topic) {
    if (topic == null) return;
    if (!GossipCommonConfig.discoveryLegacyRegistryBootstrapEnabled) {
      HashSet<BigInteger> activeKnown = new HashSet<>();
      for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
        TopicPeerKey key = entry.getKey();
        if (!topic.equals(key.topic)) continue;
        if (candidateEntryActive(key.topic, key.peerId, entry.getValue())) {
          activeKnown.add(key.peerId);
        }
      }
      HashSet<BigInteger> bootstrapPeers =
          discoveryManager.discoverBootstrapPeers(
              peers,
              topic,
              this.node == null ? null : this.node.getId(),
              activeKnown,
              heartbeatCounter);
      for (BigInteger peerId : bootstrapPeers) {
        recordCandidateSeen(topic, peerId, CandidateSource.REGISTRY_BOOTSTRAP);
        localRegistryBootstrapAdded++;
      }
      activeKnown.addAll(bootstrapPeers);
      HashSet<BigInteger> ambientPeers =
          discoveryManager.discoverAmbientPeers(
              peers,
              topic,
              this.node == null ? null : this.node.getId(),
              activeKnown,
              heartbeatCounter);
      for (BigInteger peerId : ambientPeers) {
        recordCandidateSeen(topic, peerId, CandidateSource.AMBIENT_DISCOVERY);
      }
      return;
    }

    HashSet<BigInteger> registryPeers = peers.getPeers(topic);
    if (registryPeers == null || registryPeers.isEmpty()) return;

    int activeKnown = 0;
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      if (!topic.equals(key.topic)) continue;
      if (candidateEntryActive(key.topic, key.peerId, entry.getValue())) {
        activeKnown++;
      }
    }

    int deficit = localBootstrapTargetPerTopic() - activeKnown;
    if (deficit <= 0) return;

    List<BigInteger> candidates = new ArrayList<>(registryPeers);
    Collections.shuffle(candidates, CommonState.r);
    for (BigInteger peerId : candidates) {
      if (deficit <= 0) break;
      if (peerId == null) continue;
      if (this.node != null && peerId.equals(this.node.getId())) continue;
      if (!isDiscoverablePeer(peerId)) continue;
      CandidateMeta meta = getOrCreateCandidateMeta(topic, peerId);
      if (meta == null) continue;
      boolean alreadyActive =
          candidateEntryActive(topic, peerId, meta)
              || (activeCandidateSourceMask(meta) & CandidateSource.REGISTRY_BOOTSTRAP.mask) != 0;
      if (!alreadyActive) {
        recordCandidateSeen(topic, peerId, CandidateSource.REGISTRY_BOOTSTRAP);
        localRegistryBootstrapAdded++;
        deficit--;
      }
    }
  }

  private void observeLocalTopologyCatalogue() {
    HashSet<String> allTopics = new HashSet<>();
    allTopics.addAll(mesh.keySet());
    allTopics.addAll(fanout.keySet());
    for (TopicPeerKey key : topicCandidates.keySet()) {
      allTopics.add(key.topic);
    }

    for (String topic : allTopics) {
      HashSet<BigInteger> topicMesh = mesh.get(topic);
      long knownCount = 0;
      long connectableCount = 0;
      long connectedCount = 0;
      long connectedNonMeshCount = 0;
      long reserveConnectedCount = 0;
      long reserveConnectedImmatureCount = 0;
      long reserveConnectedMatureCount = 0;
      long knownOnlyCount = 0;
      long knownNonConnectableCount = 0;
      long connectableButDisconnectedCount = 0;
      long knownRetainedAfterDisconnectCount = 0;
      long knownFromPxCount = 0;
      long knownFromDirectCount = 0;
      long knownPxOnlyCount = 0;
      long connectedPxOnlyCount = 0;
      long pxRetainedKnownOnlyCount = 0;
      long pxKnownFreshCount = 0;
      long pxKnownStaleCount = 0;
      long eligibleCount = 0;
      long eligibleConnectableCount = 0;
      long eligibleConnectedCount = 0;
      long eligibleKnownDisconnectedCount = 0;
      long connectableFromDirectCount = 0;
      long connectableFromMeshCount = 0;
      long connectableFromConnectedCount = 0;
      long connectableFromRegistryCount = 0;
      long connectableFromPxCount = 0;
      long knownNonConnectableFromPxCount = 0;
      long knownNonConnectableFromRegistryCount = 0;
      long outboundConnectedCount = 0;
      long inboundConnectedCount = 0;
      long disconnectedKnownCount = 0;
      long directFreshConnectedCount = 0;
      long pxFreshConnectedCount = 0;
      long meshFreshConnectedCount = 0;
      long fanoutFreshConnectedCount = 0;
      long connectedReserveFreshCount = 0;
      long connectedReserveRegistryOnlyCount = 0;
      long connectedHoldActiveCount = 0;
      long connectedHoldForReserveCount = 0;
      long retryBackoffActiveCount = 0;

      for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
        TopicPeerKey key = entry.getKey();
        if (!topic.equals(key.topic)) continue;
        CandidateMeta meta = entry.getValue();
        if (!candidateEntryActive(key.topic, key.peerId, meta)) continue;

        knownCount++;
        byte activeMask = effectiveCandidateSourceMask(key.topic, key.peerId, meta);
        LocalCandidateDominantSource dominantSource = dominantCandidateSource(meta);
        boolean connectable = isLocallyConnectable(key.topic, key.peerId, meta);
        if (meta.lastDirectSeenHeartbeat >= 0) {
          knownFromDirectCount++;
        }
        if (meta.lastPxHintHeartbeat >= 0) {
          knownFromPxCount++;
          if ((activeMask & CandidateSource.PX_HINT.mask) != 0) {
            pxKnownFreshCount++;
          } else {
            pxKnownStaleCount++;
          }
        }
        if (dominantSource == LocalCandidateDominantSource.PX) {
          knownPxOnlyCount++;
        }
        if (meta.retryAfterHeartbeat >= 0 && heartbeatCounter < meta.retryAfterHeartbeat) {
          retryBackoffActiveCount++;
        }
        if (connectable) {
          connectableCount++;
          switch (dominantConnectableSource(meta)) {
            case DIRECT:
              connectableFromDirectCount++;
              break;
            case MESH:
              connectableFromMeshCount++;
              break;
            case CONNECTED:
              connectableFromConnectedCount++;
              break;
            case REGISTRY:
              connectableFromRegistryCount++;
              break;
            case PX:
              connectableFromPxCount++;
              break;
            default:
              break;
          }
        }
        LocalConnectionState state = activeConnectionState(meta);
        if (state != LocalConnectionState.DISCONNECTED) {
          connectedCount++;
          boolean inMesh = topicMesh != null && topicMesh.contains(key.peerId);
          if (!inMesh) {
            connectedNonMeshCount++;
            if (isReserveConnectedPeer(key.topic, key.peerId, meta)) {
              reserveConnectedCount++;
              if (isReserveConnectedMature(meta)) {
                reserveConnectedMatureCount++;
              } else {
                reserveConnectedImmatureCount++;
              }
              if (isConnectionFresh(meta)) {
                connectedReserveFreshCount++;
              }
              if (dominantCandidateSource(meta) == LocalCandidateDominantSource.REGISTRY) {
                connectedReserveRegistryOnlyCount++;
              }
            }
            if (hasActiveConnectionHold(key.topic, key.peerId, meta)) {
              connectedHoldActiveCount++;
              if (meta.holdForReserve) {
                connectedHoldForReserveCount++;
              }
            }
          }
          if (dominantSource == LocalCandidateDominantSource.PX) {
            connectedPxOnlyCount++;
          }
          if (isConnectedDirectFresh(meta)) {
            directFreshConnectedCount++;
          }
          if (isConnectedPxFresh(meta)) {
            pxFreshConnectedCount++;
          }
          if (isConnectedMeshFresh(meta)) {
            meshFreshConnectedCount++;
          }
          if (isConnectedFanoutFresh(meta)) {
            fanoutFreshConnectedCount++;
          }
          if (state == LocalConnectionState.OUTBOUND_CONNECTED) {
            outboundConnectedCount++;
          } else if (state == LocalConnectionState.INBOUND_CONNECTED) {
            inboundConnectedCount++;
          }
        } else {
          disconnectedKnownCount++;
          knownOnlyCount++;
          if (connectable) {
            connectableButDisconnectedCount++;
          } else {
            knownNonConnectableCount++;
            switch (dominantKnownNonConnectableSource(meta)) {
              case PX:
                knownNonConnectableFromPxCount++;
                break;
              case REGISTRY:
                knownNonConnectableFromRegistryCount++;
                break;
              default:
                break;
            }
          }
          if (dominantSource == LocalCandidateDominantSource.PX) {
            pxRetainedKnownOnlyCount++;
          }
          if (meta.lastDisconnectReason != null) {
            knownRetainedAfterDisconnectCount++;
          }
        }
      }

      eligibleConnectableCount += eligibleConnectableCandidatesForTopic(topic, null).size();
      eligibleConnectedCount += eligibleConnectedCandidatesForTopic(topic, null).size();
      eligibleKnownDisconnectedCount +=
          eligibleKnownDisconnectedCandidatesForTopic(topic, null).size();
      eligibleCount = eligibleConnectedCount + eligibleKnownDisconnectedCount;

      long meshConnectedCount = connectedCount - connectedNonMeshCount;
      double meshFractionOfConnected =
          connectedCount > 0 ? (double) meshConnectedCount / connectedCount : 0.0;
      long connectedSlackTarget = connectedSlackTargetPerTopic();
      long connectedTarget = (topicMesh == null ? 0 : topicMesh.size()) + connectedSlackTarget;
      long connectedNonMeshTargetGap = Math.max(0L, connectedSlackTarget - connectedNonMeshCount);
      long connectedNonMeshRetained = Math.min(connectedNonMeshCount, connectedSlackTarget);
      long connectedReserveGap =
          Math.max(0L, connectedReserveTargetPerTopic() - connectedNonMeshCount);
      long reserveGap = Math.max(0L, connectedReserveTargetPerTopic() - reserveConnectedCount);
      localKnownPeerSum += knownCount;
      localConnectablePeerSum += connectableCount;
      localConnectedPeerSum += connectedCount;
      localConnectedNonMeshPeerSum += connectedNonMeshCount;
      localReserveConnectedPeerSum += reserveConnectedCount;
      localReserveConnectedImmatureSum += reserveConnectedImmatureCount;
      localReserveConnectedMatureSum += reserveConnectedMatureCount;
      localMeshFractionOfConnectedSum += meshFractionOfConnected;
      localConnectedTargetSum += connectedTarget;
      localConnectedSlackTargetSum += connectedSlackTarget;
      localConnectedHoldActiveSum += connectedHoldActiveCount;
      localConnectedHoldForReserveSum += connectedHoldForReserveCount;
      localConnectedNonMeshTargetGapSum += connectedNonMeshTargetGap;
      localConnectedNonMeshRetainedSum += connectedNonMeshRetained;
      localKnownOnlyPeerSum += knownOnlyCount;
      localKnownNonConnectablePeerSum += knownNonConnectableCount;
      localConnectableButDisconnectedPeerSum += connectableButDisconnectedCount;
      localKnownFromPxSum += knownFromPxCount;
      localKnownFromDirectSum += knownFromDirectCount;
      localPxKnownFreshSum += pxKnownFreshCount;
      localPxKnownStaleSum += pxKnownStaleCount;
      localMeshPeerSum += topicMesh == null ? 0 : topicMesh.size();
      localEligibleCandidateSum += eligibleCount;
      localEligibleConnectableCandidateSum += eligibleConnectableCount;
      localEligibleConnectedCandidateSum += eligibleConnectedCount;
      localEligibleKnownDisconnectedCandidateSum += eligibleKnownDisconnectedCount;
      localConnectableFromDirectSum += connectableFromDirectCount;
      localConnectableFromMeshSum += connectableFromMeshCount;
      localConnectableFromConnectedSum += connectableFromConnectedCount;
      localConnectableFromRegistrySum += connectableFromRegistryCount;
      localConnectableFromPxSum += connectableFromPxCount;
      localKnownNonConnectableFromPxSum += knownNonConnectableFromPxCount;
      localKnownNonConnectableFromRegistrySum += knownNonConnectableFromRegistryCount;
      localConnectedDirectFreshSum += directFreshConnectedCount;
      localConnectedPxFreshSum += pxFreshConnectedCount;
      localConnectedMeshFreshSum += meshFreshConnectedCount;
      localConnectedFanoutFreshSum += fanoutFreshConnectedCount;
      localConnectedReserveFreshSum += connectedReserveFreshCount;
      localConnectedReserveRegistryOnlySum += connectedReserveRegistryOnlyCount;
      localConnectedReserveGapSum += connectedReserveGap;
      localReserveGapSum += reserveGap;
      localKnownRetainedAfterDisconnectSum += knownRetainedAfterDisconnectCount;
      localConnectedPxOnlySum += connectedPxOnlyCount;
      localKnownPxOnlySum += knownPxOnlyCount;
      localPxRetainedKnownOnlySum += pxRetainedKnownOnlyCount;
      localOutboundConnectedPeerSum += outboundConnectedCount;
      localInboundConnectedPeerSum += inboundConnectedCount;
      localDisconnectedKnownPeerSum += disconnectedKnownCount;
      localConnectRetryBackoffActiveSum += retryBackoffActiveCount;
      localTopologyObservationCount++;
    }
  }

  private boolean isOutboundMeshPeer(String topic, BigInteger peerId) {
    return meshPeerDirections.get(new TopicPeerKey(topic, peerId)) == MeshPeerDirection.OUTBOUND;
  }

  private boolean isInboundMeshPeer(String topic, BigInteger peerId) {
    return meshPeerDirections.get(new TopicPeerKey(topic, peerId)) == MeshPeerDirection.INBOUND;
  }

  private int countOutboundMeshPeers(String topic, Iterable<BigInteger> peerIds) {
    int count = 0;
    if (peerIds == null) return count;
    for (BigInteger peerId : peerIds) {
      if (isOutboundMeshPeer(topic, peerId)) {
        count++;
      }
    }
    return count;
  }

  private boolean isDOutTrackingEnabled() {
    return GossipCommonConfig.D_out > 0;
  }

  private int dOutTarget() {
    if (!isDOutTrackingEnabled()) return 0;
    return Math.min(GossipCommonConfig.D_out, GossipCommonConfig.D);
  }

  private int outboundDeficit(String topic, HashSet<BigInteger> topicMesh) {
    if (!isDOutTrackingEnabled() || topicMesh == null) return 0;
    return Math.max(0, dOutTarget() - countOutboundMeshPeers(topic, topicMesh));
  }

  private boolean isRepairRelaxReceivedPruneTopicEligible(
      String topic, HashSet<BigInteger> topicMesh) {
    if (!GossipCommonConfig.repairRelaxReceivedPruneForDOut) return false;
    if (!isDOutTrackingEnabled()) return false;
    if (topicMesh == null) return false;
    if (!dOutWarmTopics.contains(topic)) return false;
    return outboundDeficit(topic, topicMesh) > 0;
  }

  private int remainingRepairRelaxReceivedPruneBudget(String topic) {
    if (!GossipCommonConfig.repairRelaxReceivedPruneForDOut) return 0;
    return Math.max(
        0,
        GossipCommonConfig.repairRelaxReceivedPruneBudgetPerHeartbeat
            - repairRelaxReceivedPruneUsedByTopic.getOrDefault(topic, 0));
  }

  private boolean meetsRepairRelaxReceivedPruneScore(String topic, BigInteger peerId) {
    if (!isScoringEnabled()) return true;
    double minScore =
        Math.max(
            GossipCommonConfig.repairRelaxReceivedPruneMinScore,
            GossipCommonConfig.meshScoreThreshold);
    return scoreOf(topic, peerId) >= minScore;
  }

  private void observeRepairRelaxReceivedPruneAgeBucket(long age, boolean remoteBackoffRejection) {
    if (age < 0) return;
    double fraction = age / (double) pruneBackoffAgeWindow();
    if (remoteBackoffRejection) {
      if (fraction < 0.25) {
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff++;
      } else if (fraction < 0.50) {
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff++;
      } else if (fraction < 0.75) {
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff++;
      } else {
        repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff++;
      }
    } else {
      if (fraction < 0.25) {
        repairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff++;
      } else if (fraction < 0.50) {
        repairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff++;
      } else if (fraction < 0.75) {
        repairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff++;
      } else {
        repairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff++;
      }
    }
  }

  private void observeRepairRelaxReceivedPruneSelectedPruneAge(String topic, BigInteger peerId) {
    long age = lastReceivedPruneAge(topic, peerId);
    if (age < 0) return;
    repairRelaxedReceivedPruneSelectedPruneAgeSum += age;
    repairRelaxedReceivedPruneSelectedPruneAgeCount++;
    observeRepairRelaxReceivedPruneAgeBucket(age, false);
  }

  private void observeRepairRelaxReceivedPruneRemoteBackoffRejectionPruneAge(
      String topic, BigInteger peerId) {
    long age = lastReceivedPruneAge(topic, peerId);
    if (age < 0) return;
    repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum += age;
    repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount++;
    observeRepairRelaxReceivedPruneAgeBucket(age, true);
  }

  private void markRepairRelaxReceivedPruneSelected(String topic, BigInteger peerId) {
    repairRelaxReceivedPruneUsedByTopic.put(
        topic, repairRelaxReceivedPruneUsedByTopic.getOrDefault(topic, 0) + 1);
    repairRelaxReceivedPrunePendingSelectionsByTopic.put(
        topic, repairRelaxReceivedPrunePendingSelectionsByTopic.getOrDefault(topic, 0) + 1);
    observeRepairRelaxReceivedPruneSelectedPruneAge(topic, peerId);
    repairRelaxReceivedPruneLifecycles.put(
        new TopicPeerKey(topic, peerId), new RelaxedReceivedPruneLifecycle(heartbeatCounter));
    repairRelaxedReceivedPruneSelected++;
    if (isScoringEnabled() && scoreOf(topic, peerId) < 0.0) {
      repairRelaxedReceivedPruneNegativeScoreSelected++;
    } else {
      repairRelaxedReceivedPrunePositiveScoreSelected++;
    }
  }

  private void recordRepairRelaxReceivedPruneSuccessIfAny(
      String topic, int deficitBefore, HashSet<BigInteger> topicMesh) {
    if (topic == null || topicMesh == null) return;
    Integer pendingSelections = repairRelaxReceivedPrunePendingSelectionsByTopic.remove(topic);
    if (pendingSelections == null || pendingSelections <= 0) return;
    if (outboundDeficit(topic, topicMesh) < deficitBefore) {
      repairRelaxedReceivedPruneSuccesses += pendingSelections;
    }
  }

  private RelaxedReceivedPruneLifecycle relaxedReceivedPruneLifecycle(
      String topic, BigInteger peerId) {
    if (topic == null || peerId == null) return null;
    return repairRelaxReceivedPruneLifecycles.get(new TopicPeerKey(topic, peerId));
  }

  private boolean isRepairRelaxReceivedPruneLocalGraceActive(String topic, BigInteger peerId) {
    if (!GossipCommonConfig.repairRelaxReceivedPruneLocalGraceEnabled) return false;
    RelaxedReceivedPruneLifecycle lifecycle = relaxedReceivedPruneLifecycle(topic, peerId);
    if (lifecycle == null) return false;
    long age = heartbeatCounter - lifecycle.selectedAtHeartbeat;
    return age >= 0 && age <= 1;
  }

  private void observeRepairRelaxReceivedPruneLocalGraceActive(String topic, BigInteger peerId) {
    RelaxedReceivedPruneLifecycle lifecycle = relaxedReceivedPruneLifecycle(topic, peerId);
    if (lifecycle == null) return;
    if (!isRepairRelaxReceivedPruneLocalGraceActive(topic, peerId)) return;
    if (lifecycle.lastLocalGraceObservedHeartbeat == heartbeatCounter) return;
    lifecycle.lastLocalGraceObservedHeartbeat = heartbeatCounter;
    repairRelaxedReceivedPruneLocalGraceActive++;
  }

  private void observeRepairRelaxReceivedPruneRemoval(
      String topic, BigInteger peerId, RelaxedReceivedPruneRemovalCause cause) {
    RelaxedReceivedPruneLifecycle lifecycle = relaxedReceivedPruneLifecycle(topic, peerId);
    if (lifecycle == null || cause == null) return;
    if (lifecycle.removalCause != null) {
      return;
    }
    lifecycle.removalCause = cause;
    switch (cause) {
      case RECEIVED_PRUNE:
        repairRelaxedReceivedPruneRemovedByReceivedPrune++;
        break;
      case LOCAL_LOW_SCORE:
        repairRelaxedReceivedPruneRemovedByLocalLowScore++;
        break;
      case LOCAL_OVERSUBSCRIPTION:
        repairRelaxedReceivedPruneRemovedByLocalOversubscription++;
        break;
      case OTHER:
        repairRelaxedReceivedPruneRemovedByOther++;
        break;
      default:
        break;
    }
  }

  private void observeRepairRelaxReceivedPruneReceivedPruneReason(
      String topic, BigInteger peerId, Message.PruneReasonTag pruneReason) {
    RelaxedReceivedPruneLifecycle lifecycle = relaxedReceivedPruneLifecycle(topic, peerId);
    if (lifecycle == null || lifecycle.firstReceivedPruneReason != null) return;

    Message.PruneReasonTag reason =
        pruneReason == null ? Message.PruneReasonTag.OTHER : pruneReason;
    lifecycle.firstReceivedPruneReason = reason;

    if (lifecycle.grafted) {
      repairRelaxedReceivedPruneReceivedPruneAfterGraft++;
    }
    if (!lifecycle.survived1Heartbeat) {
      repairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat++;
    }

    switch (reason) {
      case GRAFT_REJECTION_AT_CAPACITY:
        repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity++;
        break;
      case GRAFT_REJECTION_SCORE:
        repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore++;
        break;
      case GRAFT_REJECTION_BACKOFF:
        observeRepairRelaxReceivedPruneRemoteBackoffRejectionPruneAge(topic, peerId);
        repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff++;
        break;
      case LOW_SCORE_PRUNE:
        repairRelaxedReceivedPruneReceivedPruneReasonLowScore++;
        break;
      case OVERSUBSCRIPTION_PRUNE:
        repairRelaxedReceivedPruneReceivedPruneReasonOversubscription++;
        break;
      case LEAVE_OR_UNSUBSCRIBE:
        repairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe++;
        break;
      case OTHER:
      default:
        repairRelaxedReceivedPruneReceivedPruneReasonOther++;
        break;
    }
  }

  private void observeRepairRelaxReceivedPruneGrafted(String topic, BigInteger peerId) {
    RelaxedReceivedPruneLifecycle lifecycle = relaxedReceivedPruneLifecycle(topic, peerId);
    if (lifecycle == null || lifecycle.grafted) return;
    lifecycle.grafted = true;
    repairRelaxedReceivedPruneGrafted++;
  }

  private void observeRepairRelaxReceivedPruneGraftRejected(String topic, BigInteger peerId) {
    RelaxedReceivedPruneLifecycle lifecycle = relaxedReceivedPruneLifecycle(topic, peerId);
    if (lifecycle == null || lifecycle.graftRejected) return;
    lifecycle.graftRejected = true;
    repairRelaxedReceivedPruneGraftRejected++;
  }

  private void observeRepairRelaxReceivedPrunePruned(String topic, BigInteger peerId) {
    RelaxedReceivedPruneLifecycle lifecycle = relaxedReceivedPruneLifecycle(topic, peerId);
    if (lifecycle == null || lifecycle.pruned) return;
    lifecycle.pruned = true;
    repairRelaxedReceivedPrunePruned++;
  }

  private void observeRepairRelaxReceivedPruneBackoffReset(String topic, BigInteger peerId) {
    RelaxedReceivedPruneLifecycle lifecycle = relaxedReceivedPruneLifecycle(topic, peerId);
    if (lifecycle == null || lifecycle.backoffReset) return;
    lifecycle.backoffReset = true;
    repairRelaxedReceivedPruneBackoffReset++;
  }

  private void observeRepairRelaxReceivedPruneLifecycle(
      String topic, HashSet<BigInteger> topicMesh) {
    if (topic == null) return;
    Iterator<Map.Entry<TopicPeerKey, RelaxedReceivedPruneLifecycle>> it =
        repairRelaxReceivedPruneLifecycles.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TopicPeerKey, RelaxedReceivedPruneLifecycle> entry = it.next();
      TopicPeerKey key = entry.getKey();
      if (!topic.equals(key.topic)) {
        continue;
      }

      RelaxedReceivedPruneLifecycle lifecycle = entry.getValue();
      boolean inMesh = topicMesh != null && topicMesh.contains(key.peerId);
      long age = heartbeatCounter - lifecycle.selectedAtHeartbeat;

      if (inMesh) {
        observeRepairRelaxReceivedPruneLocalGraceActive(topic, key.peerId);
        if (!lifecycle.graftAccepted) {
          lifecycle.graftAccepted = true;
          repairRelaxedReceivedPruneGraftAccepted++;
        }
        if (age >= 1 && !lifecycle.survived1Heartbeat) {
          lifecycle.survived1Heartbeat = true;
          repairRelaxedReceivedPruneSurvived1Heartbeat++;
        }
        if (age >= 5 && !lifecycle.survived5Heartbeats) {
          lifecycle.survived5Heartbeats = true;
          repairRelaxedReceivedPruneSurvived5Heartbeats++;
        }
        if (age >= 10 && !lifecycle.survived10Heartbeats) {
          lifecycle.survived10Heartbeats = true;
          repairRelaxedReceivedPruneSurvived10Heartbeats++;
        }
      } else if (age >= 0 && lifecycle.removalCause == null) {
        observeRepairRelaxReceivedPruneRemoval(
            topic, key.peerId, RelaxedReceivedPruneRemovalCause.OTHER);
      }

      if (!inMesh && age > 10) {
        it.remove();
      }
    }
  }

  private long updateConsecutiveObservation(
      HashMap<String, Long> consecutiveByTopic, String topic, boolean active) {
    long nextValue = active ? consecutiveByTopic.getOrDefault(topic, 0L) + 1L : 0L;
    consecutiveByTopic.put(topic, nextValue);
    return nextValue;
  }

  private HashSet<BigInteger> selectMeshRepairPeers(
      String topic, int count, HashSet<BigInteger> topicMesh) {
    LinkedHashSet<BigInteger> selected = new LinkedHashSet<>();
    if (count <= 0) return new HashSet<>(selected);

    int prioritizedOutboundCount = Math.min(count, outboundDeficit(topic, topicMesh));
    if (prioritizedOutboundCount > 0) {
      HashSet<BigInteger> excluded = topicMesh == null ? new HashSet<>() : new HashSet<>(topicMesh);
      selected.addAll(selectPreferredGraftPeers(topic, prioritizedOutboundCount, excluded));
    }

    int remaining = count - selected.size();
    if (remaining > 0) {
      HashSet<BigInteger> excluded = topicMesh == null ? new HashSet<>() : new HashSet<>(topicMesh);
      excluded.addAll(selected);
      selected.addAll(selectPreferredGraftPeers(topic, remaining, excluded));
    }

    return new HashSet<>(selected);
  }

  private HashSet<BigInteger> selectPreferredGraftPeers(
      String topic, int count, HashSet<BigInteger> excludedPeers) {
    HashSet<BigInteger> selected = new HashSet<>();
    if (count <= 0) return selected;

    repairFilterSelectionCalls++;
    HashSet<BigInteger> topicPeers = candidatePoolForTopic(topic);
    repairFilterCandidatePoolRawTotal += topicPeers.size();
    if (topicPeers.isEmpty()) {
      repairFilterCandidatePoolEmptyCount++;
      return selected;
    }

    HashSet<BigInteger> topicMesh = mesh.get(topic);
    List<BigInteger> connectedCandidates = new ArrayList<>();
    List<BigInteger> connectableDisconnectedCandidates = new ArrayList<>();
    List<BigInteger> promotableReserveConnectedCandidates = new ArrayList<>();
    List<BigInteger> deferredMatureReserveConnectedCandidates = new ArrayList<>();
    List<BigInteger> immatureReserveConnectedCandidates = new ArrayList<>();
    List<BigInteger> receivedPruneBackoffCandidates = new ArrayList<>();
    int reserveConnectedCount = reserveConnectedCount(topic);
    for (BigInteger peerId : topicPeers) {
      if (peerId == null) continue;
      if (this.node != null && peerId.equals(this.node.getId())) continue;
      if (topicMesh != null && topicMesh.contains(peerId)) {
        repairFilterRejectedByAlreadyInMesh++;
        continue;
      }
      if (excludedPeers != null && excludedPeers.contains(peerId)) {
        repairFilterRejectedByExcluded++;
        continue;
      }
      if (isInPruneBackoff(topic, peerId)) {
        repairFilterRejectedByBackoff++;
        observeRepairBackoffOrigin(topic, peerId);
        if (pruneBackoffOrigin(topic, peerId) == PruneBackoffOrigin.RECEIVED_PRUNE) {
          receivedPruneBackoffCandidates.add(peerId);
        }
        continue;
      }
      if (isScoringEnabled() && scoreOf(topic, peerId) < GossipCommonConfig.meshScoreThreshold) {
        repairFilterRejectedByScore++;
        continue;
      }
      CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
      if (isConnectedLocalPeer(topic, peerId)) {
        if (isReserveConnectedPeer(topic, peerId, meta)) {
          if (!isReserveConnectedMature(meta)) {
            immatureReserveConnectedCandidates.add(peerId);
            localMeshSelectionSkippedReservedPeer++;
          } else if (isReserveConnectedMeshPromotable(
              topic, peerId, meta, reserveConnectedCount, false)) {
            promotableReserveConnectedCandidates.add(peerId);
          } else {
            deferredMatureReserveConnectedCandidates.add(peerId);
            localMeshSelectionSkippedReservedPeer++;
          }
        } else {
          connectedCandidates.add(peerId);
        }
      } else if (isLocallyConnectable(topic, peerId, meta)) {
        connectableDisconnectedCandidates.add(peerId);
      }
    }

    if (isScoringEnabled()) {
      connectedCandidates.sort(localCandidatePreferenceComparator(topic, true));
      connectableDisconnectedCandidates.sort(localConnectTargetPreferenceComparator(topic));
      promotableReserveConnectedCandidates.sort(localCandidatePreferenceComparator(topic, true));
      deferredMatureReserveConnectedCandidates.sort(
          localCandidatePreferenceComparator(topic, true));
      immatureReserveConnectedCandidates.sort(localCandidatePreferenceComparator(topic, true));
    } else {
      Collections.shuffle(connectedCandidates, CommonState.r);
      connectedCandidates.sort(localCandidatePreferenceComparator(topic, true));
      Collections.shuffle(connectableDisconnectedCandidates, CommonState.r);
      connectableDisconnectedCandidates.sort(localConnectTargetPreferenceComparator(topic));
      Collections.shuffle(promotableReserveConnectedCandidates, CommonState.r);
      promotableReserveConnectedCandidates.sort(localCandidatePreferenceComparator(topic, true));
      Collections.shuffle(deferredMatureReserveConnectedCandidates, CommonState.r);
      deferredMatureReserveConnectedCandidates.sort(
          localCandidatePreferenceComparator(topic, true));
      Collections.shuffle(immatureReserveConnectedCandidates, CommonState.r);
      immatureReserveConnectedCandidates.sort(localCandidatePreferenceComparator(topic, true));
    }

    for (BigInteger peerId : connectedCandidates) {
      selected.add(peerId);
      if (selected.size() >= count) {
        return selected;
      }
    }

    for (BigInteger peerId : connectableDisconnectedCandidates) {
      if (selected.size() >= count || remainingLocalConnectBudget(topic) <= 0) {
        break;
      }
      CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
      if (meta == null) continue;
      if (attemptLocalConnect(
          topic, peerId, meta, LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_REPAIR)) {
        selected.add(peerId);
      }
    }

    for (BigInteger peerId : promotableReserveConnectedCandidates) {
      if (selected.size() >= count) {
        break;
      }
      selected.add(peerId);
    }

    for (BigInteger peerId : deferredMatureReserveConnectedCandidates) {
      if (selected.size() >= count) {
        break;
      }
      selected.add(peerId);
    }

    if (!selected.isEmpty()) {
      return selected;
    }

    repairFilterNoCandidateAfterFiltering++;
    if (connectedCandidates.isEmpty() && connectableDisconnectedCandidates.isEmpty()) {
      if (!GossipCommonConfig.repairRelaxReceivedPruneForDOut
          || receivedPruneBackoffCandidates.isEmpty()) {
        return selected;
      }

      for (BigInteger peerId : receivedPruneBackoffCandidates) {
        observeRepairRelaxReceivedPrunePrefilterCandidate(topic, peerId);
      }

      if (topicMesh == null || !dOutWarmTopics.contains(topic)) {
        repairRelaxedReceivedPruneBlockedNotPostWarmup += receivedPruneBackoffCandidates.size();
        return selected;
      }

      int deficitBeforeSelection = outboundDeficit(topic, topicMesh);
      if (!isDOutTrackingEnabled() || deficitBeforeSelection <= 0) {
        repairRelaxedReceivedPruneBlockedNoDOutDeficit += receivedPruneBackoffCandidates.size();
        return selected;
      }

      int remainingBudget = remainingRepairRelaxReceivedPruneBudget(topic);
      if (remainingBudget <= 0) {
        repairRelaxedReceivedPruneBlockedByBudget += receivedPruneBackoffCandidates.size();
        return selected;
      }

      List<BigInteger> relaxedCandidates = new ArrayList<>();
      for (BigInteger peerId : receivedPruneBackoffCandidates) {
        if (!meetsRepairRelaxReceivedPruneScore(topic, peerId)) {
          continue;
        }
        if (!meetsRepairRelaxReceivedPruneMinAge(topic, peerId)) {
          continue;
        }
        repairRelaxedReceivedPruneConsidered++;
        relaxedCandidates.add(peerId);
      }

      if (relaxedCandidates.isEmpty()) {
        return selected;
      }

      if (isScoringEnabled()) {
        relaxedCandidates.sort(localConnectTargetPreferenceComparator(topic));
      } else {
        Collections.shuffle(relaxedCandidates, CommonState.r);
        relaxedCandidates.sort(localConnectTargetPreferenceComparator(topic));
      }

      int relaxedSelectionCount =
          Math.min(Math.min(count, remainingBudget), relaxedCandidates.size());
      for (int i = 0; i < relaxedSelectionCount; i++) {
        BigInteger peerId = relaxedCandidates.get(i);
        if (!isConnectedLocalPeer(topic, peerId)) {
          if (remainingLocalConnectBudget(topic) <= 0) {
            break;
          }
          CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
          if (meta == null
              || !attemptLocalConnect(
                  topic, peerId, meta, LocalConnectionTransitionReason.LOCAL_CONNECT_FOR_REPAIR)) {
            continue;
          }
        }
        selected.add(peerId);
        markRepairRelaxReceivedPruneSelected(topic, peerId);
      }
      return selected;
    }
    return selected;
  }

  private HashSet<BigInteger> protectedScorePeers(String topic, HashSet<BigInteger> topicMesh) {
    HashSet<BigInteger> protectedPeers = new HashSet<>();
    if (!isScoringEnabled() || topicMesh == null || topicMesh.isEmpty()) {
      return protectedPeers;
    }

    List<BigInteger> scoredPeers = new ArrayList<>(topicMesh);
    scoredPeers.sort(
        Comparator.comparingDouble((BigInteger peerId) -> scoreOf(topic, peerId)).reversed());

    int protectedCount = Math.min(GossipCommonConfig.D_score, scoredPeers.size());
    for (int i = 0; i < protectedCount; i++) {
      protectedPeers.add(scoredPeers.get(i));
    }
    return protectedPeers;
  }

  private List<BigInteger> selectSteadyStatePrunePeers(
      String topic, HashSet<BigInteger> topicMesh, int count) {
    if (count <= 0 || topicMesh == null || topicMesh.isEmpty()) {
      return Collections.emptyList();
    }

    HashSet<BigInteger> protectedPeers = protectedScorePeers(topic, topicMesh);
    List<BigInteger> candidates = new ArrayList<>();
    List<BigInteger> fallbackCandidates = new ArrayList<>();
    for (BigInteger peerId : topicMesh) {
      if (isOutboundMeshPeer(topic, peerId)) {
        continue;
      }
      if (isRepairRelaxReceivedPruneLocalGraceActive(topic, peerId)) {
        observeRepairRelaxReceivedPruneLocalGraceActive(topic, peerId);
        repairRelaxedReceivedPruneLocalPrunePreventedOversubscription++;
        continue;
      }
      fallbackCandidates.add(peerId);
      if (!protectedPeers.contains(peerId)) {
        candidates.add(peerId);
      }
    }

    if (candidates.isEmpty()) {
      candidates = fallbackCandidates;
    }
    if (candidates.isEmpty()) {
      return Collections.emptyList();
    }

    if (isScoringEnabled()) {
      candidates.sort(Comparator.comparingDouble((BigInteger peerId) -> scoreOf(topic, peerId)));
    } else {
      Collections.shuffle(candidates, CommonState.r);
    }

    if (candidates.size() > count) {
      return new ArrayList<>(candidates.subList(0, count));
    }
    return candidates;
  }

  private BigInteger selectInPlacePromotionPeer(String topic, HashSet<BigInteger> topicMesh) {
    if (topicMesh == null || topicMesh.isEmpty()) return null;

    List<BigInteger> inboundPeers = new ArrayList<>();
    for (BigInteger peerId : topicMesh) {
      if (isInboundMeshPeer(topic, peerId)) {
        inboundPeers.add(peerId);
      }
    }

    if (inboundPeers.isEmpty()) {
      return null;
    }

    if (isScoringEnabled()) {
      inboundPeers.sort(
          Comparator.comparingDouble((BigInteger peerId) -> scoreOf(topic, peerId)).reversed());
    } else {
      Collections.shuffle(inboundPeers, CommonState.r);
    }

    return inboundPeers.get(0);
  }

  private boolean tryForcedOutboundReseed(
      String topic, HashSet<BigInteger> topicMesh, int deficitBefore) {
    if (topicMesh == null) return false;

    dOutForcedReseedAttempts++;

    HashSet<BigInteger> excludedPeers = new HashSet<>(topicMesh);
    HashSet<BigInteger> replacementPeers = selectPreferredGraftPeers(topic, 1, excludedPeers);
    if (replacementPeers.isEmpty()) {
      dOutForcedReseedSkippedNoCandidate++;
      return false;
    }

    BigInteger graftPeer = replacementPeers.iterator().next();
    int grafts = 0;
    int prunes = 0;

    if (topicMesh.size() >= GossipCommonConfig.D_high) {
      List<BigInteger> prunePeers = selectSteadyStatePrunePeers(topic, topicMesh, 1);
      if (prunePeers.isEmpty()) {
        dOutForcedReseedSkippedNoPrunableInbound++;
        return false;
      }
      BigInteger prunePeer = prunePeers.get(0);
      if (topicMesh.remove(prunePeer)) {
        observeRepairRelaxReceivedPruneRemoval(
            topic, prunePeer, RelaxedReceivedPruneRemovalCause.LOCAL_OVERSUBSCRIPTION);
        clearMeshPeerDirection(topic, prunePeer);
        sendPruneMessage(prunePeer, topic, PruneBackoffOrigin.OVERSUBSCRIPTION_PRUNE);
        observeMeshConnectionSeparationAfterPrune(
            topic, prunePeer, PruneBackoffOrigin.OVERSUBSCRIPTION_PRUNE, true);
        prunes++;
      }
    }

    if (topicMesh.add(graftPeer)) {
      sendGraftMessage(graftPeer, topic);
      grafts++;
    }

    int deficitAfter = outboundDeficit(topic, topicMesh);
    recordRepairRelaxReceivedPruneSuccessIfAny(topic, deficitBefore, topicMesh);
    boolean succeeded = deficitAfter < deficitBefore;

    if (grafts > 0 || prunes > 0) {
      dOutSteadyStateTopupAttempts++;
      dOutSteadyStateGrafts += grafts;
      dOutSteadyStatePrunes += prunes;
      dOutSteadyStateReplacements += prunes;
      if (deficitAfter < deficitBefore) {
        dOutSteadyStateTopupSuccesses++;
      }
    }

    dOutForcedReseedGrafts += grafts;
    dOutForcedReseedPrunes += prunes;
    if (succeeded) {
      dOutForcedReseedSuccesses++;
    }

    return succeeded;
  }

  private void maybeRepairOutboundDeficit(String topic, HashSet<BigInteger> topicMesh) {
    if (!isDOutTrackingEnabled() || topicMesh == null) return;

    int deficitBefore = outboundDeficit(topic, topicMesh);
    if (deficitBefore <= 0) {
      updateConsecutiveObservation(dOutConsecutiveZeroRemainingDeficitByTopic, topic, false);
      dOutForcedReseedBlockedDeficitBeforeZero++;
      return;
    }

    int outboundBefore = countOutboundMeshPeers(topic, topicMesh);
    boolean postWarmup = dOutWarmTopics.contains(topic);
    long consecutiveDeficit = dOutConsecutiveDeficitHeartbeatsByTopic.getOrDefault(topic, 0L) + 1L;
    long consecutiveZeroOutbound =
        dOutConsecutiveZeroOutboundHeartbeatsByTopic.getOrDefault(topic, 0L) + 1L;
    boolean zeroOutboundEmergency = postWarmup && outboundBefore <= 0;
    if (zeroOutboundEmergency) {
      dOutForcedReseedZeroOutboundEmergencyCount++;
    }
    boolean escalationEligible =
        postWarmup && outboundBefore > 0 && consecutiveDeficit >= D_OUT_ESCALATION_HEARTBEATS;
    boolean forcedReseedEligible =
        postWarmup
            && outboundBefore <= 0
            && consecutiveZeroOutbound >= D_OUT_ZERO_OUTBOUND_RESEED_HEARTBEATS;
    if (forcedReseedEligible) {
      dOutForcedReseedEligibleCount++;
    } else if (outboundBefore > 0) {
      dOutForcedReseedBlockedOutboundBeforePositive++;
    } else if (!postWarmup) {
      dOutForcedReseedBlockedNotPostWarmup++;
    } else if (consecutiveZeroOutbound < D_OUT_ZERO_OUTBOUND_RESEED_HEARTBEATS) {
      dOutForcedReseedBlockedConsecutiveZeroBelowThreshold++;
    }

    boolean inPlacePromotionSucceeded = false;
    if (GossipCommonConfig.dOutInplacePromotionEnabled
        && (zeroOutboundEmergency || escalationEligible)) {
      dOutInplacePromotionAttempts++;
      BigInteger promotedPeer = selectInPlacePromotionPeer(topic, topicMesh);
      if (promotedPeer != null) {
        markMeshPeerOutbound(topic, promotedPeer);
        inPlacePromotionSucceeded = true;
        dOutInplacePromotionSuccesses++;
        if (zeroOutboundEmergency) {
          dOutInplaceZeroOutboundPromotions++;
          dOutForcedReseedInplaceSuccessDuringZeroOutboundCount++;
        } else {
          dOutInplaceDeficitPromotions++;
        }
      }
    }

    int deficitAfterInPlacePromotion = outboundDeficit(topic, topicMesh);
    boolean zeroRemainingDeficitEpisode =
        postWarmup && outboundBefore <= 0 && deficitAfterInPlacePromotion > 0;
    long consecutiveZeroRemainingDeficit =
        updateConsecutiveObservation(
            dOutConsecutiveZeroRemainingDeficitByTopic, topic, zeroRemainingDeficitEpisode);
    if (zeroRemainingDeficitEpisode) {
      dOutMaxConsecutiveZeroRemainingDeficitHeartbeats =
          Math.max(
              dOutMaxConsecutiveZeroRemainingDeficitHeartbeats, consecutiveZeroRemainingDeficit);
    }
    if (zeroOutboundEmergency && inPlacePromotionSucceeded && deficitAfterInPlacePromotion > 0) {
      dOutForcedReseedAfterInplaceRemainingDeficitCount++;
    }
    if (inPlacePromotionSucceeded && deficitAfterInPlacePromotion <= 0) {
      if (zeroOutboundEmergency) {
        dOutForcedReseedBlockedAfterInplaceNoRemainingDeficit++;
        dOutForcedReseedReturnedBeforeCallsiteCount++;
      }
      return;
    }

    boolean forcedReseedEligibleByDeficit =
        GossipCommonConfig.dOutForcedReseedEnabled
            && zeroOutboundEmergency
            && postWarmup
            && deficitAfterInPlacePromotion > 0
            && consecutiveZeroRemainingDeficit >= D_OUT_ZERO_OUTBOUND_RESEED_HEARTBEATS;
    if (forcedReseedEligibleByDeficit) {
      dOutForcedReseedEligibleByDeficit++;
      dOutForcedReseedEligibleByZeroRemainingDeficit++;
    }

    if (inPlacePromotionSucceeded && !forcedReseedEligibleByDeficit) {
      if (zeroOutboundEmergency) {
        dOutForcedReseedBlockedAfterInplaceNotPersistent++;
        dOutForcedReseedReturnedBeforeCallsiteCount++;
      }
      return;
    }

    if (forcedReseedEligibleByDeficit) {
      dOutForcedReseedReachedCallsiteCount++;
      dOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit++;
      if (inPlacePromotionSucceeded) {
        dOutForcedReseedReachedCallsiteAfterInplace++;
      }
      if (tryForcedOutboundReseed(topic, topicMesh, deficitAfterInPlacePromotion)) {
        return;
      }
    }

    int repairBudget = Math.min(deficitBefore, 2);
    int grafts = 0;
    int prunes = 0;
    int replacements = 0;

    HashSet<BigInteger> excludedPeers = new HashSet<>(topicMesh);
    int room = Math.max(0, GossipCommonConfig.D_high - topicMesh.size());
    int directTopups = Math.min(repairBudget, room);
    if (directTopups > 0) {
      HashSet<BigInteger> topupPeers =
          selectPreferredGraftPeers(topic, directTopups, excludedPeers);
      for (BigInteger peerId : topupPeers) {
        if (topicMesh.add(peerId)) {
          sendGraftMessage(peerId, topic);
          excludedPeers.add(peerId);
          grafts++;
        }
      }
    }

    int remainingDeficit = outboundDeficit(topic, topicMesh);
    boolean emergencyAttempted = zeroOutboundEmergency;
    if (emergencyAttempted) {
      dOutZeroOutboundEmergencyAttempts++;
    }

    boolean escalationAttempted = false;
    if (remainingDeficit > 0
        && topicMesh.size() >= GossipCommonConfig.D_high
        && (zeroOutboundEmergency || escalationEligible)) {
      int replacementBudget = 1;
      escalationAttempted = !zeroOutboundEmergency && escalationEligible;
      if (escalationAttempted) {
        dOutEscalationAttempts++;
      }

      HashSet<BigInteger> replacementPeers =
          selectPreferredGraftPeers(topic, replacementBudget, excludedPeers);
      List<BigInteger> prunePeers =
          selectSteadyStatePrunePeers(topic, topicMesh, replacementPeers.size());

      int replacementCount = Math.min(replacementPeers.size(), prunePeers.size());
      List<BigInteger> replacementList = new ArrayList<>(replacementPeers);
      for (int i = 0; i < replacementCount; i++) {
        BigInteger graftPeer = replacementList.get(i);
        BigInteger prunePeer = prunePeers.get(i);
        if (topicMesh.add(graftPeer)) {
          sendGraftMessage(graftPeer, topic);
          grafts++;
        }
        if (topicMesh.remove(prunePeer)) {
          observeRepairRelaxReceivedPruneRemoval(
              topic, prunePeer, RelaxedReceivedPruneRemovalCause.LOCAL_OVERSUBSCRIPTION);
          clearMeshPeerDirection(topic, prunePeer);
          sendPruneMessage(prunePeer, topic, PruneBackoffOrigin.OVERSUBSCRIPTION_PRUNE);
          observeMeshConnectionSeparationAfterPrune(
              topic, prunePeer, PruneBackoffOrigin.OVERSUBSCRIPTION_PRUNE, true);
          prunes++;
          replacements++;
        }
      }
    }

    int outboundAfter = countOutboundMeshPeers(topic, topicMesh);
    int deficitAfter = outboundDeficit(topic, topicMesh);
    recordRepairRelaxReceivedPruneSuccessIfAny(topic, deficitBefore, topicMesh);
    if (emergencyAttempted && outboundBefore <= 0 && outboundAfter > 0) {
      dOutZeroOutboundEmergencySuccesses++;
    }
    if (escalationAttempted && deficitAfter < remainingDeficit) {
      dOutEscalationSuccesses++;
    }

    if (grafts > 0 || prunes > 0) {
      dOutSteadyStateTopupAttempts++;
      dOutSteadyStateGrafts += grafts;
      dOutSteadyStatePrunes += prunes;
      dOutSteadyStateReplacements += replacements;
      if (deficitAfter < deficitBefore) {
        dOutSteadyStateTopupSuccesses++;
      }
    }
  }

  private void observeOutboundMeshPeers(String topic) {
    if (!isDOutTrackingEnabled()) return;
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    if (topicMesh == null) return;
    int outboundPeers = countOutboundMeshPeers(topic, topicMesh);
    int deficit = outboundDeficit(topic, topicMesh);
    boolean zeroOutbound = outboundPeers <= 0;
    boolean hasDeficit = deficit > 0;

    dOutOutboundMeshPeerSum += outboundPeers;
    dOutOutboundMeshPeerObservationCount++;
    dOutOutboundMeshPeerMinObserved = Math.min(dOutOutboundMeshPeerMinObserved, outboundPeers);
    dOutHeartbeatObservationCount++;
    if (zeroOutbound) {
      dOutZeroOutboundObservationCount++;
    }
    if (hasDeficit) {
      dOutDeficitObservationCount++;
    }

    long consecutiveDeficit =
        updateConsecutiveObservation(dOutConsecutiveDeficitHeartbeatsByTopic, topic, hasDeficit);
    long consecutiveZeroOutbound =
        updateConsecutiveObservation(
            dOutConsecutiveZeroOutboundHeartbeatsByTopic, topic, zeroOutbound);
    dOutMaxConsecutiveDeficitHeartbeats =
        Math.max(dOutMaxConsecutiveDeficitHeartbeats, consecutiveDeficit);
    dOutMaxConsecutiveZeroOutboundHeartbeats =
        Math.max(dOutMaxConsecutiveZeroOutboundHeartbeats, consecutiveZeroOutbound);

    if (topicMesh.size() >= GossipCommonConfig.D_low) {
      dOutWarmTopics.add(topic);
    }
    if (dOutWarmTopics.contains(topic)) {
      dOutPostWarmupObservationCount++;
      if (zeroOutbound) {
        dOutPostWarmupZeroOutboundCount++;
      }
      if (hasDeficit) {
        dOutPostWarmupDeficitCount++;
      }
      dOutPostWarmupOutboundMeshPeerMinObserved =
          Math.min(dOutPostWarmupOutboundMeshPeerMinObserved, outboundPeers);
    }
  }

  private boolean canAcceptIncomingGraftAtCapacity(String topic, HashSet<BigInteger> topicMesh) {
    if (topicMesh == null || topicMesh.size() < GossipCommonConfig.D_high) {
      return true;
    }
    int requiredOutbound = Math.min(GossipCommonConfig.D_out, GossipCommonConfig.D);
    if (requiredOutbound <= 0) {
      return true;
    }
    return countOutboundMeshPeers(topic, topicMesh) >= requiredOutbound;
  }

  private boolean isMeshCandidateIneligible(
      String topic, BigInteger peerId, HashSet<BigInteger> excludedPeers) {
    return isMeshCandidateIneligible(topic, peerId, excludedPeers, false);
  }

  private boolean isMeshCandidateIneligible(
      String topic,
      BigInteger peerId,
      HashSet<BigInteger> excludedPeers,
      boolean countCandidatePoolFilters) {
    if (peerId == null) return true;
    if (this.node != null && peerId.equals(this.node.getId())) return true;
    if (excludedPeers != null && excludedPeers.contains(peerId)) return true;
    if (isInPruneBackoff(topic, peerId)) {
      if (countCandidatePoolFilters) {
        candidatePoolBackoffFiltered++;
      }
      return true;
    }
    if (isScoringEnabled() && scoreOf(topic, peerId) < GossipCommonConfig.meshScoreThreshold) {
      if (countCandidatePoolFilters) {
        candidatePoolScoreFiltered++;
      }
      return true;
    }
    return false;
  }

  private boolean isConnectedLocalPeer(String topic, BigInteger peerId) {
    CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
    return activeConnectionState(meta) != LocalConnectionState.DISCONNECTED;
  }

  private HashSet<BigInteger> connectedPeersForTopic(String topic) {
    HashSet<BigInteger> peersForTopic = new HashSet<>();
    if (topic == null) return peersForTopic;
    disconnectIdleLocalPeersForTopic(topic);
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      if (!topic.equals(key.topic)) continue;
      if (activeConnectionState(entry.getValue()) != LocalConnectionState.DISCONNECTED) {
        peersForTopic.add(key.peerId);
      }
    }
    return peersForTopic;
  }

  private HashSet<BigInteger> connectablePeersForTopic(String topic) {
    HashSet<BigInteger> peersForTopic = new HashSet<>();
    if (topic == null) return peersForTopic;
    disconnectIdleLocalPeersForTopic(topic);
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      if (!topic.equals(key.topic)) continue;
      if (isLocallyConnectable(key.topic, key.peerId, entry.getValue())) {
        peersForTopic.add(key.peerId);
      }
    }
    return peersForTopic;
  }

  private HashSet<BigInteger> eligibleConnectedCandidatesForTopic(
      String topic, HashSet<BigInteger> excludedPeers) {
    HashSet<BigInteger> candidates = new HashSet<>();
    List<BigInteger> deferredMatureReserveCandidates = new ArrayList<>();
    int reserveConnectedCount = reserveConnectedCount(topic);
    for (BigInteger peerId : connectedPeersForTopic(topic)) {
      if (isPeerInMeshOrFanout(topic, peerId)) continue;
      if (isMeshCandidateIneligible(topic, peerId, excludedPeers)) {
        continue;
      }
      CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
      if (isReserveConnectedPeer(topic, peerId, meta)) {
        if (!isReserveConnectedMature(meta)) {
          continue;
        }
        if (isReserveConnectedMeshPromotable(topic, peerId, meta, reserveConnectedCount, false)) {
          candidates.add(peerId);
        } else {
          deferredMatureReserveCandidates.add(peerId);
        }
      } else {
        candidates.add(peerId);
      }
    }
    if (candidates.isEmpty()) {
      candidates.addAll(deferredMatureReserveCandidates);
    }
    return candidates;
  }

  private HashSet<BigInteger> eligibleKnownDisconnectedCandidatesForTopic(
      String topic, HashSet<BigInteger> excludedPeers) {
    HashSet<BigInteger> candidates = new HashSet<>();
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      if (!topic.equals(key.topic)) continue;
      if (!candidateEntryActive(key.topic, key.peerId, entry.getValue())) continue;
      if (isConnectedLocalPeer(topic, key.peerId)) continue;
      if (isPeerInMeshOrFanout(topic, key.peerId)) continue;
      if (!isMeshCandidateIneligible(topic, key.peerId, excludedPeers)) {
        candidates.add(key.peerId);
      }
    }
    return candidates;
  }

  private HashSet<BigInteger> eligibleConnectableDisconnectedCandidatesForTopic(
      String topic, HashSet<BigInteger> excludedPeers) {
    HashSet<BigInteger> candidates = new HashSet<>();
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!topic.equals(key.topic)) continue;
      if (!isLocallyConnectable(key.topic, key.peerId, meta)) continue;
      if (isConnectedLocalPeer(topic, key.peerId)) continue;
      if (isPeerInMeshOrFanout(topic, key.peerId)) continue;
      if (!isMeshCandidateIneligible(topic, key.peerId, excludedPeers)) {
        candidates.add(key.peerId);
      }
    }
    return candidates;
  }

  private HashSet<BigInteger> eligibleConnectableCandidatesForTopic(
      String topic, HashSet<BigInteger> excludedPeers) {
    HashSet<BigInteger> candidates = eligibleConnectedCandidatesForTopic(topic, excludedPeers);
    candidates.addAll(eligibleConnectableDisconnectedCandidatesForTopic(topic, excludedPeers));
    return candidates;
  }

  private Comparator<BigInteger> localCandidatePreferenceComparator(
      String topic, boolean descendingScore) {
    Comparator<BigInteger> byConnectivity =
        Comparator.comparingInt((BigInteger peerId) -> isConnectedLocalPeer(topic, peerId) ? 1 : 0)
            .reversed();
    if (!isScoringEnabled()) {
      return byConnectivity;
    }
    Comparator<BigInteger> byScore =
        Comparator.comparingDouble((BigInteger peerId) -> scoreOf(topic, peerId));
    if (descendingScore) {
      byScore = byScore.reversed();
    }
    return byConnectivity.thenComparing(byScore);
  }

  private Comparator<BigInteger> localConnectTargetPreferenceComparator(String topic) {
    Comparator<BigInteger> byConnectSourceRank =
        Comparator.comparingInt(
            (BigInteger peerId) -> {
              CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
              return localConnectPreferenceRank(meta);
            });
    if (!isScoringEnabled()) {
      return byConnectSourceRank;
    }
    Comparator<BigInteger> byScoreDescending =
        Comparator.comparingDouble((BigInteger peerId) -> scoreOf(topic, peerId)).reversed();
    return byConnectSourceRank.thenComparing(byScoreDescending);
  }

  private HashSet<BigInteger> candidatePoolForTopic(String topic) {
    maybeBootstrapTopicKnowledge(topic);
    HashSet<BigInteger> candidates = new HashSet<>();
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      if (!key.topic.equals(topic)) {
        continue;
      }
      if (candidateEntryActive(key.topic, key.peerId, entry.getValue())) {
        candidates.add(key.peerId);
      }
    }
    return candidates;
  }

  private Comparator<BigInteger> pxAdvertisementPreferenceComparator(String topic) {
    Comparator<BigInteger> byConnected =
        Comparator.comparingInt((BigInteger peerId) -> isConnectedLocalPeer(topic, peerId) ? 1 : 0)
            .reversed();
    Comparator<BigInteger> byFreshness =
        Comparator.comparingLong(
                (BigInteger peerId) -> {
                  CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
                  return pxAdvertisementRecency(meta);
                })
            .reversed();
    Comparator<BigInteger> bySourceRank =
        Comparator.comparingInt(
            (BigInteger peerId) -> {
              CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, peerId));
              return localConnectPreferenceRank(meta);
            });
    if (!isScoringEnabled()) {
      return byConnected.thenComparing(byFreshness).thenComparing(bySourceRank);
    }
    Comparator<BigInteger> byScoreDescending =
        Comparator.comparingDouble((BigInteger peerId) -> scoreOf(topic, peerId)).reversed();
    return byConnected
        .thenComparing(byFreshness)
        .thenComparing(bySourceRank)
        .thenComparing(byScoreDescending);
  }

  private HashSet<BigInteger> selectFanoutPeers(
      String topic, int count, HashSet<BigInteger> excludedPeers) {
    HashSet<BigInteger> selected = new HashSet<>();
    if (count <= 0) return selected;

    List<BigInteger> candidates = new ArrayList<>();
    for (BigInteger peerId : connectedPeersForTopic(topic)) {
      if (isMeshCandidateIneligible(topic, peerId, excludedPeers, true)) {
        continue;
      }
      candidates.add(peerId);
    }

    if (candidates.isEmpty()) {
      return selected;
    }

    if (isScoringEnabled()) {
      candidates.sort(localCandidatePreferenceComparator(topic, true));
    } else {
      Collections.shuffle(candidates, CommonState.r);
      candidates.sort(localCandidatePreferenceComparator(topic, true));
    }

    for (BigInteger peerId : candidates) {
      selected.add(peerId);
      if (selected.size() >= count) {
        break;
      }
    }
    observeConnectedNonMeshUsedForFanout(selected);
    return selected;
  }

  private List<BigInteger> selectLocalGossipRecipients(String topic, int count) {
    List<BigInteger> recipients = new ArrayList<>();
    if (count <= 0) return recipients;

    HashSet<BigInteger> excluded = new HashSet<>();
    HashSet<BigInteger> topicMesh = mesh.get(topic);
    if (topicMesh != null) {
      excluded.addAll(topicMesh);
    }
    HashSet<BigInteger> topicFanout = fanout.get(topic);
    if (topicFanout != null) {
      excluded.addAll(topicFanout);
    }
    if (this.node != null) {
      excluded.add(this.node.getId());
    }

    List<BigInteger> candidates = new ArrayList<>();
    for (BigInteger peerId : connectedPeersForTopic(topic)) {
      if (excluded.contains(peerId)) continue;
      if (isScoringEnabled() && scoreOf(topic, peerId) < GossipCommonConfig.gossipScoreThreshold) {
        continue;
      }
      candidates.add(peerId);
    }

    if (candidates.isEmpty()) {
      return recipients;
    }

    if (isScoringEnabled()) {
      candidates.sort(localCandidatePreferenceComparator(topic, true));
    } else {
      Collections.shuffle(candidates, CommonState.r);
      candidates.sort(localCandidatePreferenceComparator(topic, true));
    }

    if (candidates.size() > count) {
      List<BigInteger> selected = new ArrayList<>(candidates.subList(0, count));
      observeConnectedNonMeshUsedForGossip(selected);
      return selected;
    }
    observeConnectedNonMeshUsedForGossip(candidates);
    return candidates;
  }

  private HashSet<BigInteger> getOrCreatePxPeerHints(String topic) {
    HashSet<BigInteger> candidates = pxPeerHints.get(topic);
    if (candidates == null) {
      candidates = new HashSet<>();
      pxPeerHints.put(topic, candidates);
    }
    return candidates;
  }

  private int remainingLocalPxAdmissionBudget(String topic) {
    if (topic == null) return 0;
    int used = localPxAdmissionUsedByTopic.getOrDefault(topic, 0);
    return Math.max(0, GossipCommonConfig.localPxAdmissionBudgetPerTopicPerHeartbeat - used);
  }

  private int activePxOnlyKnownCount(String topic) {
    if (topic == null) return 0;
    int count = 0;
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!topic.equals(key.topic)) continue;
      if (!candidateEntryActive(key.topic, key.peerId, meta)) continue;
      if (activeConnectionState(meta) != LocalConnectionState.DISCONNECTED) continue;
      if (isPeerInMeshOrFanout(key.topic, key.peerId)) continue;
      if (effectiveCandidateSourceMask(key.topic, key.peerId, meta)
          == CandidateSource.PX_HINT.mask) {
        count++;
      }
    }
    return count;
  }

  private boolean ensureLocalPxOnlyKnownCapacity(String topic, BigInteger incomingPeerId) {
    if (topic == null) return false;
    if (GossipCommonConfig.localPxKnownOnlyMaxPerTopic < 0) return false;
    if (activePxOnlyKnownCount(topic) < GossipCommonConfig.localPxKnownOnlyMaxPerTopic) {
      return true;
    }

    TopicPeerKey removableKey = null;
    long oldestPxHintHeartbeat = Long.MAX_VALUE;
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!topic.equals(key.topic)) continue;
      if (incomingPeerId != null && incomingPeerId.equals(key.peerId)) continue;
      if (!candidateEntryActive(key.topic, key.peerId, meta)) continue;
      if (activeConnectionState(meta) != LocalConnectionState.DISCONNECTED) continue;
      if (isPeerInMeshOrFanout(key.topic, key.peerId)) continue;
      if (effectiveCandidateSourceMask(key.topic, key.peerId, meta) != CandidateSource.PX_HINT.mask)
        continue;
      if (removableKey == null || meta.lastPxHintHeartbeat < oldestPxHintHeartbeat) {
        removableKey = key;
        oldestPxHintHeartbeat = meta.lastPxHintHeartbeat;
      }
    }

    if (removableKey == null) {
      return false;
    }

    if (topicCandidates.remove(removableKey) != null) {
      HashSet<BigInteger> pxHints = pxPeerHints.get(topic);
      if (pxHints != null) {
        pxHints.remove(removableKey.peerId);
      }
      lastPxAdvertisedHeartbeatByTopicPeer.remove(removableKey);
      candidatePoolCappedRemoved++;
      return true;
    }
    return false;
  }

  private List<BigInteger> selectPrunePxPeers(String topic, BigInteger prunedPeerId) {
    if (GossipCommonConfig.prunePeers <= 0) return Collections.emptyList();

    HashSet<BigInteger> excludedPeers = new HashSet<>();
    if (prunedPeerId != null) {
      excludedPeers.add(prunedPeerId);
    }

    List<BigInteger> candidates = new ArrayList<>();
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!topic.equals(key.topic)) continue;
      if (!candidateEntryActive(key.topic, key.peerId, meta)) continue;
      BigInteger peerId = key.peerId;
      if (!isMeshCandidateIneligible(topic, peerId, excludedPeers, true)) {
        candidates.add(peerId);
      }
    }

    if (candidates.isEmpty()) {
      candidatePoolEmptySelectionCount++;
      return candidates;
    }

    candidates.sort(pxAdvertisementPreferenceComparator(topic));
    List<BigInteger> selected = new ArrayList<>();
    for (BigInteger peerId : candidates) {
      if (wasRecentlyPxAdvertised(topic, peerId)) {
        pxPeersSkippedRecentlyAdvertised++;
        continue;
      }
      selected.add(peerId);
      markPxAdvertised(topic, peerId);
      if (selected.size() >= GossipCommonConfig.prunePeers) {
        break;
      }
    }
    pxPeersSelectedAfterNoveltyFiltering += selected.size();
    return selected;
  }

  private boolean isPxSuppressedByDefense(String topic, BigInteger requesterId) {
    // Defense 1: per-requester PX backoff
    if (GossipCommonConfig.pxRequesterBackoffEnabled) {
      Long backoffEnd = pxRequesterBackoffEnd.get(new TopicPeerKey(topic, requesterId));
      if (backoffEnd != null && heartbeatCounter < backoffEnd) return true;
    }
    // Defense 2: global rate limit per topic
    if (GossipCommonConfig.pxGlobalRateLimitEnabled) {
      Long windowStart = pxRateLimitWindowStart.get(topic);
      if (windowStart == null
          || heartbeatCounter
              >= windowStart + GossipCommonConfig.pxGlobalRateLimitWindowHeartbeats) {
        pxRateLimitWindowStart.put(topic, heartbeatCounter);
        pxRateLimitBudgetUsed.put(topic, 0);
      }
      int used = pxRateLimitBudgetUsed.getOrDefault(topic, 0);
      if (used >= GossipCommonConfig.pxGlobalRateLimitBudget) return true;
    }
    // Defense 3: flood detection suppression
    if (GossipCommonConfig.pxFloodDetectionEnabled) {
      Long suppressedUntil = pxFloodSuppressedUntil.get(topic);
      if (suppressedUntil != null && heartbeatCounter <= suppressedUntil) return true;
    }
    return false;
  }

  private void recordPxGiven(String topic, BigInteger requesterId, int count) {
    if (GossipCommonConfig.pxRequesterBackoffEnabled) {
      pxRequesterBackoffEnd.put(
          new TopicPeerKey(topic, requesterId),
          heartbeatCounter + GossipCommonConfig.pxRequesterBackoffHeartbeats);
    }
    if (GossipCommonConfig.pxGlobalRateLimitEnabled) {
      int used = pxRateLimitBudgetUsed.getOrDefault(topic, 0);
      pxRateLimitBudgetUsed.put(topic, used + count);
    }
  }

  private void recordInboundGraftForFloodDetection(String topic) {
    ArrayList<Long> timestamps =
        inboundGraftTimestamps.computeIfAbsent(topic, k -> new ArrayList<>());
    timestamps.add(heartbeatCounter);
    long windowStart = heartbeatCounter - GossipCommonConfig.pxFloodDetectionWindowHeartbeats + 1;
    timestamps.removeIf(t -> t < windowStart);
    if (timestamps.size() >= GossipCommonConfig.pxFloodDetectionGraftThreshold) {
      pxFloodSuppressedUntil.put(
          topic, heartbeatCounter + GossipCommonConfig.pxFloodDetectionWindowHeartbeats);
    }
  }

  private void maybeAcceptPxPeers(String topic, BigInteger senderId, List<BigInteger> pxPeers) {
    if (pxPeers == null || pxPeers.isEmpty()) return;
    pxCandidatesSeen += pxPeers.size();
    observePxSenderTrust(topic, senderId);
    if (!isScoringEnabled()) {
      pxCandidatesRejectedSenderThreshold += pxPeers.size();
      pxCandidatesRejected += pxPeers.size();
      localPxAdmissionRejectionsTotal += pxPeers.size();
      return;
    }
    if (scoreOf(topic, senderId) < GossipCommonConfig.acceptPXThreshold) {
      pxCandidatesRejectedSenderThreshold += pxPeers.size();
      pxCandidatesRejected += pxPeers.size();
      localPxAdmissionRejectionsTotal += pxPeers.size();
      return;
    }
    pxCandidatesPassedSenderThreshold += pxPeers.size();

    HashSet<BigInteger> topicMesh = mesh.get(topic);
    HashSet<BigInteger> acceptedCandidates = getOrCreatePxPeerHints(topic);
    int accepted = 0;
    int rejected = 0;
    for (BigInteger candidate : pxPeers) {
      localPxAdmissionAttempts++;
      if (candidate == null
          || (this.node != null && candidate.equals(this.node.getId()))
          || candidate.equals(senderId)) {
        pxCandidatesRejectedExpiredOrStale++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
        continue;
      }
      if (!isDiscoverablePeer(candidate)) {
        pxCandidatesRejectedExpiredOrStale++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
        continue;
      }

      CandidateMeta meta = topicCandidates.get(new TopicPeerKey(topic, candidate));
      if (topicMesh != null && topicMesh.contains(candidate)) {
        pxCandidatesRejectedInMesh++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
        continue;
      }
      if (meta != null && activeConnectionState(meta) != LocalConnectionState.DISCONNECTED) {
        pxCandidatesRejectedAlreadyConnected++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
        continue;
      }
      if (meta != null && candidateEntryActive(topic, candidate, meta)) {
        pxCandidatesRejectedAlreadyKnown++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
        continue;
      }
      if (isInPruneBackoff(topic, candidate)) {
        pxCandidatesRejectedPruneBackoff++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
        continue;
      }
      if (isScoringEnabled() && scoreOf(topic, candidate) < GossipCommonConfig.meshScoreThreshold) {
        pxCandidatesRejectedScore++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
        continue;
      }
      if (remainingLocalPxAdmissionBudget(topic) <= 0
          || !ensureLocalPxOnlyKnownCapacity(topic, candidate)) {
        pxCandidatesRejectedCapacity++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
        continue;
      }

      if (acceptedCandidates.add(candidate)) {
        localPxAdmissionUsedByTopic.merge(topic, 1, Integer::sum);
        recordCandidateSeen(topic, candidate, CandidateSource.PX_HINT);
        pxCandidatesAdmittedToKnown++;
        localPxAdmissionSuccesses++;
        accepted++;
      } else {
        pxCandidatesRejectedAlreadyKnown++;
        localPxAdmissionRejectionsTotal++;
        rejected++;
      }
    }

    pxCandidatesAccepted += accepted;
    pxAcceptedFromHighScoreSenders += accepted;
    pxCandidatesRejected += rejected;
  }

  private void maybeOpportunisticGraft(String topic) {
    if (!isScoringEnabled()) return;
    if (GossipCommonConfig.opportunisticGraftTicks <= 0) return;
    if (heartbeatCounter % GossipCommonConfig.opportunisticGraftTicks != 0) return;

    HashSet<BigInteger> topicMesh = mesh.get(topic);
    if (topicMesh == null || topicMesh.isEmpty()) return;
    opportunisticGraftEvaluationCount++;

    double medianScore = medianMeshScore(topic, topicMesh);
    if (medianScore >= GossipCommonConfig.opportunisticGraftThreshold) return;

    int desiredGraftCount =
        Math.max(GossipCommonConfig.opportunisticGraftPeers, outboundDeficit(topic, topicMesh));
    HashSet<BigInteger> graftCandidates =
        selectOpportunisticGraftPeers(topic, desiredGraftCount, topicMesh, medianScore);
    if (graftCandidates.isEmpty()) return;

    opportunisticGraftTriggerCount++;
    opportunisticGraftPeerCount += graftCandidates.size();

    for (BigInteger peerId : graftCandidates) {
      topicMesh.add(peerId);
      sendGraftMessage(peerId, topic);
    }
  }

  private double medianMeshScore(String topic, HashSet<BigInteger> topicMesh) {
    if (topicMesh.isEmpty()) return 0.0;

    List<Double> meshScores = new ArrayList<>();
    for (BigInteger peerId : topicMesh) {
      meshScores.add(scoreOf(topic, peerId));
    }

    Collections.sort(meshScores);
    int size = meshScores.size();
    int mid = size / 2;
    if (size % 2 == 1) {
      return meshScores.get(mid);
    }
    return (meshScores.get(mid - 1) + meshScores.get(mid)) / 2.0;
  }

  private HashSet<BigInteger> selectMeshPeers(
      String topic, int count, HashSet<BigInteger> excludedPeers) {
    HashSet<BigInteger> selected = new HashSet<>();
    if (count <= 0) return selected;

    HashSet<BigInteger> topicPeers = connectedPeersForTopic(topic);
    if (topicPeers == null || topicPeers.isEmpty()) return selected;

    List<BigInteger> candidates = new ArrayList<>();
    for (BigInteger peerId : topicPeers) {
      if (!isMeshCandidateIneligible(topic, peerId, excludedPeers, true)) {
        candidates.add(peerId);
      }
    }

    if (candidates.isEmpty()) {
      candidatePoolEmptySelectionCount++;
    }
    Collections.shuffle(candidates, CommonState.r);
    candidates.sort(localCandidatePreferenceComparator(topic, true));
    for (BigInteger peerId : candidates) {
      selected.add(peerId);
      if (selected.size() >= count) break;
    }
    return selected;
  }

  private HashSet<BigInteger> selectOpportunisticGraftPeers(
      String topic, int count, HashSet<BigInteger> excludedPeers, double minScoreExclusive) {
    HashSet<BigInteger> selected = new HashSet<>();
    if (count <= 0) return selected;

    HashSet<BigInteger> topicPeers = connectedPeersForTopic(topic);
    if (topicPeers == null || topicPeers.isEmpty()) return selected;

    List<BigInteger> candidates = new ArrayList<>();
    for (BigInteger peerId : topicPeers) {
      if (isMeshCandidateIneligible(topic, peerId, excludedPeers, true)) {
        continue;
      }
      if (scoreOf(topic, peerId) <= minScoreExclusive) {
        continue;
      }
      candidates.add(peerId);
    }

    if (candidates.isEmpty()) {
      candidatePoolEmptySelectionCount++;
    }
    candidates.sort(localCandidatePreferenceComparator(topic, true));

    for (BigInteger peerId : candidates) {
      selected.add(peerId);
      if (selected.size() >= count) break;
    }

    return selected;
  }

  private HashSet<BigInteger> selectMeshSurvivors(String topic, HashSet<BigInteger> currentMesh) {
    int targetSize = Math.min(GossipCommonConfig.D, currentMesh.size());
    if (targetSize <= 0) return new HashSet<>();

    List<BigInteger> candidates = new ArrayList<>(currentMesh);
    LinkedHashSet<BigInteger> survivors = new LinkedHashSet<>();

    List<BigInteger> outboundCandidates = new ArrayList<>();
    for (BigInteger peerId : candidates) {
      if (isOutboundMeshPeer(topic, peerId)) {
        outboundCandidates.add(peerId);
      }
    }

    int keepOutbound =
        Math.min(Math.min(GossipCommonConfig.D_out, targetSize), outboundCandidates.size());

    if (!isScoringEnabled()) {
      Collections.shuffle(outboundCandidates, CommonState.r);
      for (int i = 0; i < keepOutbound; i++) {
        survivors.add(outboundCandidates.get(i));
      }

      List<BigInteger> remainder = new ArrayList<>(candidates);
      remainder.removeAll(survivors);
      Collections.shuffle(remainder, CommonState.r);
      for (BigInteger peerId : remainder) {
        if (survivors.size() >= targetSize) break;
        survivors.add(peerId);
      }

      return new HashSet<>(survivors);
    }

    Comparator<BigInteger> scoreDescending =
        Comparator.comparingDouble((BigInteger peerId) -> scoreOf(topic, peerId)).reversed();
    outboundCandidates.sort(scoreDescending);
    for (int i = 0; i < keepOutbound; i++) {
      survivors.add(outboundCandidates.get(i));
    }

    candidates.sort(scoreDescending);
    int keepByScore = Math.min(Math.min(GossipCommonConfig.D_score, targetSize), candidates.size());
    for (int i = 0; i < keepByScore && survivors.size() < targetSize; i++) {
      survivors.add(candidates.get(i));
    }

    List<BigInteger> remainder = new ArrayList<>();
    for (BigInteger peerId : candidates) {
      if (!survivors.contains(peerId)) {
        remainder.add(peerId);
      }
    }
    Collections.shuffle(remainder, CommonState.r);
    for (BigInteger peerId : remainder) {
      if (survivors.size() >= targetSize) break;
      survivors.add(peerId);
    }

    return new HashSet<>(survivors);
  }

  public void setDegradedPeer(boolean degradedPeer) {
    this.degradedPeer = degradedPeer;
  }

  public boolean isDegradedPeer() {
    return degradedPeer;
  }

  public long getOpportunisticGraftEvaluationCount() {
    return opportunisticGraftEvaluationCount;
  }

  public long getOpportunisticGraftTriggerCount() {
    return opportunisticGraftTriggerCount;
  }

  public long getOpportunisticGraftPeerCount() {
    return opportunisticGraftPeerCount;
  }

  public long getPxPruneMessagesSent() {
    return pxPruneMessagesSent;
  }

  public long getPxCandidatesAdvertised() {
    return pxCandidatesAdvertised;
  }

  public long getPxCandidatesSeen() {
    return pxCandidatesSeen;
  }

  public long getPxCandidatesRejectedSenderThreshold() {
    return pxCandidatesRejectedSenderThreshold;
  }

  public long getPxCandidatesRejectedAlreadyKnown() {
    return pxCandidatesRejectedAlreadyKnown;
  }

  public long getPxCandidatesRejectedAlreadyConnected() {
    return pxCandidatesRejectedAlreadyConnected;
  }

  public long getPxCandidatesRejectedInMesh() {
    return pxCandidatesRejectedInMesh;
  }

  public long getPxCandidatesRejectedPruneBackoff() {
    return pxCandidatesRejectedPruneBackoff;
  }

  public long getPxCandidatesRejectedScore() {
    return pxCandidatesRejectedScore;
  }

  public long getPxCandidatesRejectedExpiredOrStale() {
    return pxCandidatesRejectedExpiredOrStale;
  }

  public long getPxCandidatesRejectedCapacity() {
    return pxCandidatesRejectedCapacity;
  }

  public long getPxCandidatesPassedSenderThreshold() {
    return pxCandidatesPassedSenderThreshold;
  }

  public long getPxCandidatesAdmittedToKnown() {
    return pxCandidatesAdmittedToKnown;
  }

  public long getPxCandidatesAdmittedAndLaterConnected() {
    return pxCandidatesAdmittedAndLaterConnected;
  }

  public long getPxPeersSkippedRecentlyAdvertised() {
    return pxPeersSkippedRecentlyAdvertised;
  }

  public long getPxPeersSelectedAfterNoveltyFiltering() {
    return pxPeersSelectedAfterNoveltyFiltering;
  }

  public long getPxCandidatesAccepted() {
    return pxCandidatesAccepted;
  }

  public long getPxCandidatesRejected() {
    return pxCandidatesRejected;
  }

  public long getPxAcceptedFromHighScoreSenders() {
    return pxAcceptedFromHighScoreSenders;
  }

  public long getPxSendersSeen() {
    return pxSendersSeen;
  }

  public long getPxSendersAboveAcceptThreshold() {
    return pxSendersAboveAcceptThreshold;
  }

  public long getPxSendersBelowAcceptThreshold() {
    return pxSendersBelowAcceptThreshold;
  }

  public double getPxSendersScoreSum() {
    return pxSendersScoreSum;
  }

  public long getPxSendersScoreCount() {
    return pxSendersScoreCount;
  }

  public double getPxSendersScoreMin() {
    return pxSendersScoreCount > 0 ? pxSendersScoreMin : 0.0;
  }

  public double getPxSendersScoreMax() {
    return pxSendersScoreCount > 0 ? pxSendersScoreMax : 0.0;
  }

  public long getPxSendersScoreLtThresholdMinusOne() {
    return pxSendersScoreLtThresholdMinusOne;
  }

  public long getPxSendersScoreThresholdMinusOneToMinusHalf() {
    return pxSendersScoreThresholdMinusOneToMinusHalf;
  }

  public long getPxSendersScoreThresholdMinusHalfToThreshold() {
    return pxSendersScoreThresholdMinusHalfToThreshold;
  }

  public long getPxSendersScoreGeThreshold() {
    return pxSendersScoreGeThreshold;
  }

  public long getCurrentCandidatePoolEntryCount() {
    long count = 0;
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      if (candidateEntryActive(key.topic, key.peerId, entry.getValue())) {
        count++;
      }
    }
    return count;
  }

  private long getCurrentPotentialPrunePxEntryCount(boolean capPerTopic) {
    if (GossipCommonConfig.prunePeers <= 0) return 0;

    Map<String, Long> eligibleEntriesByTopic = new HashMap<>();
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!candidateEntryActive(key.topic, key.peerId, meta)) continue;
      if (isMeshCandidateIneligible(key.topic, key.peerId, null, false)) continue;
      if (wasRecentlyPxAdvertised(key.topic, key.peerId)) continue;
      eligibleEntriesByTopic.put(
          key.topic, eligibleEntriesByTopic.getOrDefault(key.topic, 0L) + 1L);
    }

    long count = 0;
    for (long topicEntryCount : eligibleEntriesByTopic.values()) {
      count +=
          capPerTopic ? Math.min(topicEntryCount, GossipCommonConfig.prunePeers) : topicEntryCount;
    }
    return count;
  }

  /**
   * Counts active topic-peer entries that could be considered by PX right now without sending PX. A
   * real PRUNE excludes the peer being pruned, so its concrete list may be shorter by one entry.
   */
  public long getCurrentPotentialPrunePxCandidateEntryCount() {
    return getCurrentPotentialPrunePxEntryCount(false);
  }

  /**
   * Counts how many entries the current per-topic PX lists could carry after the PRUNE cap. The
   * count reuses the PX novelty filter but does not mark any peer as advertised.
   */
  public long getCurrentPotentialPrunePxAdvertisableEntryCount() {
    return getCurrentPotentialPrunePxEntryCount(true);
  }

  public long getCurrentPotentialPrunePxTopicCount() {
    if (GossipCommonConfig.prunePeers <= 0) return 0;

    HashSet<String> topics = new HashSet<>();
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!candidateEntryActive(key.topic, key.peerId, meta)) continue;
      if (isMeshCandidateIneligible(key.topic, key.peerId, null, false)) continue;
      if (wasRecentlyPxAdvertised(key.topic, key.peerId)) continue;
      topics.add(key.topic);
    }
    return topics.size();
  }

  private long getCurrentCandidatePoolEntryCountForSource(CandidateSource source) {
    if (source == null) return 0;
    long count = 0;
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      TopicPeerKey key = entry.getKey();
      CandidateMeta meta = entry.getValue();
      if (!candidateEntryActive(key.topic, key.peerId, meta)) continue;
      if ((effectiveCandidateSourceMask(key.topic, key.peerId, meta) & source.mask) != 0) {
        count++;
      }
    }
    return count;
  }

  public long getCurrentBootstrapKnownEntryCount() {
    return getCurrentCandidatePoolEntryCountForSource(CandidateSource.REGISTRY_BOOTSTRAP);
  }

  public long getCurrentAmbientKnownEntryCount() {
    return getCurrentCandidatePoolEntryCountForSource(CandidateSource.AMBIENT_DISCOVERY);
  }

  public long getCurrentPxKnownEntryCount() {
    return getCurrentCandidatePoolEntryCountForSource(CandidateSource.PX_HINT);
  }

  public long getCurrentObservedKnownEntryCount() {
    return getCurrentCandidatePoolEntryCountForSource(CandidateSource.KNOWN);
  }

  public long getCurrentConnectedPeerEntryCount() {
    long count = 0;
    for (Map.Entry<TopicPeerKey, CandidateMeta> entry : topicCandidates.entrySet()) {
      if (activeConnectionState(entry.getValue()) != LocalConnectionState.DISCONNECTED) {
        count++;
      }
    }
    return count;
  }

  public long getCurrentMeshPeerEntryCount() {
    long count = 0;
    for (HashSet<BigInteger> topicMesh : mesh.values()) {
      count += topicMesh.size();
    }
    return count;
  }

  public long getCandidatePoolActiveSum() {
    return candidatePoolActiveSum;
  }

  public long getCandidatePoolKnownActiveSum() {
    return candidatePoolKnownActiveSum;
  }

  public long getCandidatePoolPxActiveSum() {
    return candidatePoolPxActiveSum;
  }

  public long getCandidatePoolMeshRepairActiveSum() {
    return candidatePoolMeshRepairActiveSum;
  }

  public long getCandidatePoolRegistryActiveSum() {
    return candidatePoolRegistryActiveSum;
  }

  public long getCandidatePoolObservationCount() {
    return candidatePoolObservationCount;
  }

  public long getCandidatePoolExpiredRemoved() {
    return candidatePoolExpiredRemoved;
  }

  public long getCandidatePoolPxExpiredRemoved() {
    return candidatePoolPxExpiredRemoved;
  }

  public long getCandidatePoolCappedRemoved() {
    return candidatePoolCappedRemoved;
  }

  public long getCandidatePoolBackoffFiltered() {
    return candidatePoolBackoffFiltered;
  }

  public long getCandidatePoolScoreFiltered() {
    return candidatePoolScoreFiltered;
  }

  public long getCandidatePoolEmptySelectionCount() {
    return candidatePoolEmptySelectionCount;
  }

  public long getLocalTopologyObservationCount() {
    return localTopologyObservationCount;
  }

  public long getLocalKnownPeerSum() {
    return localKnownPeerSum;
  }

  public long getLocalConnectablePeerSum() {
    return localConnectablePeerSum;
  }

  public long getLocalConnectedPeerSum() {
    return localConnectedPeerSum;
  }

  public long getLocalConnectedNonMeshPeerSum() {
    return localConnectedNonMeshPeerSum;
  }

  public long getLocalReserveConnectedPeerSum() {
    return localReserveConnectedPeerSum;
  }

  public long getLocalReserveConnectedImmatureSum() {
    return localReserveConnectedImmatureSum;
  }

  public long getLocalReserveConnectedMatureSum() {
    return localReserveConnectedMatureSum;
  }

  public double getLocalMeshFractionOfConnectedSum() {
    return localMeshFractionOfConnectedSum;
  }

  public long getLocalConnectedHoldActiveSum() {
    return localConnectedHoldActiveSum;
  }

  public long getLocalConnectedHoldForReserveSum() {
    return localConnectedHoldForReserveSum;
  }

  public long getLocalKnownOnlyPeerSum() {
    return localKnownOnlyPeerSum;
  }

  public long getLocalKnownNonConnectablePeerSum() {
    return localKnownNonConnectablePeerSum;
  }

  public long getLocalConnectableButDisconnectedPeerSum() {
    return localConnectableButDisconnectedPeerSum;
  }

  public long getLocalKnownFromPxSum() {
    return localKnownFromPxSum;
  }

  public long getLocalKnownFromDirectSum() {
    return localKnownFromDirectSum;
  }

  public long getLocalPxKnownFreshSum() {
    return localPxKnownFreshSum;
  }

  public long getLocalPxKnownStaleSum() {
    return localPxKnownStaleSum;
  }

  public long getLocalMeshPeerSum() {
    return localMeshPeerSum;
  }

  public long getLocalEligibleCandidateSum() {
    return localEligibleCandidateSum;
  }

  public long getLocalEligibleConnectableCandidateSum() {
    return localEligibleConnectableCandidateSum;
  }

  public long getLocalEligibleConnectedCandidateSum() {
    return localEligibleConnectedCandidateSum;
  }

  public long getLocalEligibleKnownDisconnectedCandidateSum() {
    return localEligibleKnownDisconnectedCandidateSum;
  }

  public long getLocalConnectableFromDirectSum() {
    return localConnectableFromDirectSum;
  }

  public long getLocalConnectableFromMeshSum() {
    return localConnectableFromMeshSum;
  }

  public long getLocalConnectableFromConnectedSum() {
    return localConnectableFromConnectedSum;
  }

  public long getLocalConnectableFromRegistrySum() {
    return localConnectableFromRegistrySum;
  }

  public long getLocalConnectableFromPxSum() {
    return localConnectableFromPxSum;
  }

  public long getLocalKnownNonConnectableFromPxSum() {
    return localKnownNonConnectableFromPxSum;
  }

  public long getLocalKnownNonConnectableFromRegistrySum() {
    return localKnownNonConnectableFromRegistrySum;
  }

  public long getLocalConnectedDirectFreshSum() {
    return localConnectedDirectFreshSum;
  }

  public long getLocalConnectedPxFreshSum() {
    return localConnectedPxFreshSum;
  }

  public long getLocalConnectedMeshFreshSum() {
    return localConnectedMeshFreshSum;
  }

  public long getLocalConnectedFanoutFreshSum() {
    return localConnectedFanoutFreshSum;
  }

  public long getLocalConnectedReserveFreshSum() {
    return localConnectedReserveFreshSum;
  }

  public long getLocalConnectedReserveRegistryOnlySum() {
    return localConnectedReserveRegistryOnlySum;
  }

  public long getLocalConnectedTargetSum() {
    return localConnectedTargetSum;
  }

  public long getLocalConnectedSlackTargetSum() {
    return localConnectedSlackTargetSum;
  }

  public long getLocalConnectedReserveGapSum() {
    return localConnectedReserveGapSum;
  }

  public long getLocalReserveGapSum() {
    return localReserveGapSum;
  }

  public long getLocalConnectedNonMeshTargetGapSum() {
    return localConnectedNonMeshTargetGapSum;
  }

  public long getLocalConnectedNonMeshRetainedSum() {
    return localConnectedNonMeshRetainedSum;
  }

  public long getLocalKnownRetainedAfterDisconnectSum() {
    return localKnownRetainedAfterDisconnectSum;
  }

  public long getLocalConnectedPxOnlySum() {
    return localConnectedPxOnlySum;
  }

  public long getLocalKnownPxOnlySum() {
    return localKnownPxOnlySum;
  }

  public long getLocalPxRetainedKnownOnlySum() {
    return localPxRetainedKnownOnlySum;
  }

  public long getLocalOutboundConnectedPeerSum() {
    return localOutboundConnectedPeerSum;
  }

  public long getLocalInboundConnectedPeerSum() {
    return localInboundConnectedPeerSum;
  }

  public long getLocalDisconnectedKnownPeerSum() {
    return localDisconnectedKnownPeerSum;
  }

  public long getLocalRegistryBootstrapAdded() {
    return localRegistryBootstrapAdded;
  }

  public long getLocalCatalogExpiredRegistryRemoved() {
    return localCatalogExpiredRegistryRemoved;
  }

  public long getLocalCatalogExpiredKnownRemoved() {
    return localCatalogExpiredKnownRemoved;
  }

  public long getLocalCatalogExpiredMeshRepairRemoved() {
    return localCatalogExpiredMeshRepairRemoved;
  }

  public long getLocalCatalogExpiredConnectedRemoved() {
    return localCatalogExpiredConnectedRemoved;
  }

  public long getLocalConnectAttempts() {
    return localConnectAttempts;
  }

  public long getLocalConnectSuccesses() {
    return localConnectSuccesses;
  }

  public long getLocalConnectFailures() {
    return localConnectFailures;
  }

  public long getLocalConnectAttemptsFromDirect() {
    return localConnectAttemptsFromDirect;
  }

  public long getLocalConnectAttemptsFromPx() {
    return localConnectAttemptsFromPx;
  }

  public long getLocalConnectAttemptsFromMeshRepair() {
    return localConnectAttemptsFromMeshRepair;
  }

  public long getLocalConnectAttemptsForReserve() {
    return localConnectAttemptsForReserve;
  }

  public long getLocalConnectSuccessesFromDirect() {
    return localConnectSuccessesFromDirect;
  }

  public long getLocalConnectSuccessesFromPx() {
    return localConnectSuccessesFromPx;
  }

  public long getLocalConnectSuccessesFromMeshRepair() {
    return localConnectSuccessesFromMeshRepair;
  }

  public long getLocalConnectSuccessesForReserve() {
    return localConnectSuccessesForReserve;
  }

  public long getLocalConnectFailuresForReserve() {
    return localConnectFailuresForReserve;
  }

  public long getLocalReserveConnectedPromotedToMesh() {
    return localReserveConnectedPromotedToMesh;
  }

  public long getLocalReserveConnectedConsumedByMesh() {
    return localReserveConnectedConsumedByMesh;
  }

  public long getLocalReserveConnectedConsumedBeforeMaturity() {
    return localReserveConnectedConsumedBeforeMaturity;
  }

  public long getLocalReserveConnectedDisconnectedBeforePromotion() {
    return localReserveConnectedDisconnectedBeforePromotion;
  }

  public long getLocalReserveConnectedDisconnectedBeforeMaturity() {
    return localReserveConnectedDisconnectedBeforeMaturity;
  }

  public long getLocalReserveConnectedDisconnectedAfterMaturity() {
    return localReserveConnectedDisconnectedAfterMaturity;
  }

  public long getLocalMeshSelectionSkippedReservedPeer() {
    return localMeshSelectionSkippedReservedPeer;
  }

  public long getLocalPxPromotedToConnected() {
    return localPxPromotedToConnected;
  }

  public long getLocalPxAdmissionAttempts() {
    return localPxAdmissionAttempts;
  }

  public long getLocalPxAdmissionSuccesses() {
    return localPxAdmissionSuccesses;
  }

  public long getLocalPxAdmissionRejectionsTotal() {
    return localPxAdmissionRejectionsTotal;
  }

  public long getLocalConnectRetryBackoffActiveSum() {
    return localConnectRetryBackoffActiveSum;
  }

  public long getLocalConnectionTransitionOutbound() {
    return localConnectionTransitionOutbound;
  }

  public long getLocalConnectionTransitionInbound() {
    return localConnectionTransitionInbound;
  }

  public long getLocalConnectionTransitionDisconnected() {
    return localConnectionTransitionDisconnected;
  }

  public long getLocalConnectionDisconnectedByReceivedPrune() {
    return localConnectionDisconnectedByReceivedPrune;
  }

  public long getLocalConnectionDisconnectedByLocalPrune() {
    return localConnectionDisconnectedByLocalPrune;
  }

  public long getLocalConnectionDisconnectedByScore() {
    return localConnectionDisconnectedByScore;
  }

  public long getLocalConnectionDisconnectedByCapacity() {
    return localConnectionDisconnectedByCapacity;
  }

  public long getLocalConnectionDisconnectedByIdle() {
    return localConnectionDisconnectedByIdle;
  }

  public long getLocalConnectionDisconnectedByExpiry() {
    return localConnectionDisconnectedByExpiry;
  }

  public long getLocalConnectionDisconnectedByOther() {
    return localConnectionDisconnectedByOther;
  }

  public long getLocalConnectionPreservedAfterReceivedPrune() {
    return localConnectionPreservedAfterReceivedPrune;
  }

  public long getLocalConnectionPreservedAfterLocalOversubscriptionPrune() {
    return localConnectionPreservedAfterLocalOversubscriptionPrune;
  }

  public long getLocalConnectionPreservedAfterLocalLowScorePrune() {
    return localConnectionPreservedAfterLocalLowScorePrune;
  }

  public long getLocalConnectedNonMeshSurvivedAfterPrune() {
    return localConnectedNonMeshSurvivedAfterPrune;
  }

  public long getLocalMeshRemovedButStillConnectedCount() {
    return localMeshRemovedButStillConnectedCount;
  }

  public long getLocalConnectionLifetimeSum() {
    return localConnectionLifetimeSum;
  }

  public long getLocalConnectionLifetimeCount() {
    return localConnectionLifetimeCount;
  }

  public long getLocalConnectedNonMeshLifetimeSum() {
    return localConnectedNonMeshLifetimeSum;
  }

  public long getLocalConnectedNonMeshLifetimeCount() {
    return localConnectedNonMeshLifetimeCount;
  }

  public long getLocalReserveConnectedLifetimeSum() {
    return localReserveConnectedLifetimeSum;
  }

  public long getLocalReserveConnectedLifetimeCount() {
    return localReserveConnectedLifetimeCount;
  }

  public long getLocalReserveConnectedLifetimeBeforePromotionSum() {
    return localReserveConnectedLifetimeBeforePromotionSum;
  }

  public long getLocalReserveConnectedLifetimeBeforePromotionCount() {
    return localReserveConnectedLifetimeBeforePromotionCount;
  }

  public long getLocalConnectionReconnectSamePeerWithin5Heartbeats() {
    return localConnectionReconnectSamePeerWithin5Heartbeats;
  }

  public long getLocalConnectionReconnectSamePeerWithin10Heartbeats() {
    return localConnectionReconnectSamePeerWithin10Heartbeats;
  }

  public long getLocalConnectionIdleDisconnectsBeforeMinLifetime() {
    return localConnectionIdleDisconnectsBeforeMinLifetime;
  }

  public long getLocalConnectionIdleDisconnectsAfterMinLifetime() {
    return localConnectionIdleDisconnectsAfterMinLifetime;
  }

  public long getLocalConnectionDisconnectionBlockedByHold() {
    return localConnectionDisconnectionBlockedByHold;
  }

  public long getLocalConnectionHoldExpiredDisconnects() {
    return localConnectionHoldExpiredDisconnects;
  }

  public long getLocalConnectionConsecutiveIdleThresholdHits() {
    return localConnectionConsecutiveIdleThresholdHits;
  }

  public long getLocalConnectionRetryDelaySum() {
    return localConnectionRetryDelaySum;
  }

  public long getLocalConnectionRetryDelayCount() {
    return localConnectionRetryDelayCount;
  }

  public long getLocalConnectionRetryDelayMax() {
    return localConnectionRetryDelayMax;
  }

  public long getLocalConnectionFlappingPeerCount() {
    return localConnectionFlappingPeers == null ? 0 : localConnectionFlappingPeers.size();
  }

  public long getLocalConnectedStaleDropCount() {
    return localConnectedStaleDropCount;
  }

  public long getLocalConnectedNonMeshUsedForGossip() {
    return localConnectedNonMeshUsedForGossip;
  }

  public long getLocalConnectedNonMeshUsedForFanout() {
    return localConnectedNonMeshUsedForFanout;
  }

  public long getLocalConnectedNonMeshPromotedToMesh() {
    return localConnectedNonMeshPromotedToMesh;
  }

  public long getLocalConnectedNonMeshDisconnectedByCapacity() {
    return localConnectedNonMeshDisconnectedByCapacity;
  }

  public long getLocalConnectedNonMeshDisconnectedByScore() {
    return localConnectedNonMeshDisconnectedByScore;
  }

  public long getLocalConnectedNonMeshDisconnectedByExpiry() {
    return localConnectedNonMeshDisconnectedByExpiry;
  }

  public long getRepairFilterSelectionCalls() {
    return repairFilterSelectionCalls;
  }

  public long getRepairFilterCandidatePoolRawTotal() {
    return repairFilterCandidatePoolRawTotal;
  }

  public long getRepairFilterCandidatePoolEmptyCount() {
    return repairFilterCandidatePoolEmptyCount;
  }

  public long getRepairFilterRejectedByBackoff() {
    return repairFilterRejectedByBackoff;
  }

  public long getRepairFilterRejectedByScore() {
    return repairFilterRejectedByScore;
  }

  public long getRepairFilterRejectedByAlreadyInMesh() {
    return repairFilterRejectedByAlreadyInMesh;
  }

  public long getRepairFilterRejectedByExcluded() {
    return repairFilterRejectedByExcluded;
  }

  public long getRepairFilterNoCandidateAfterFiltering() {
    return repairFilterNoCandidateAfterFiltering;
  }

  public long getPruneBackoffSetFromReceivedPrune() {
    return pruneBackoffSetFromReceivedPrune;
  }

  public long getPruneBackoffSetFromOversubscriptionPrune() {
    return pruneBackoffSetFromOversubscriptionPrune;
  }

  public long getPruneBackoffSetFromLowScorePrune() {
    return pruneBackoffSetFromLowScorePrune;
  }

  public long getPruneBackoffSetFromGraftRejection() {
    return pruneBackoffSetFromGraftRejection;
  }

  public long getRepairRejectedByBackoffFromReceivedPrune() {
    return repairRejectedByBackoffFromReceivedPrune;
  }

  public long getRepairRejectedByBackoffFromOversubscriptionPrune() {
    return repairRejectedByBackoffFromOversubscriptionPrune;
  }

  public long getRepairRejectedByBackoffFromLowScorePrune() {
    return repairRejectedByBackoffFromLowScorePrune;
  }

  public long getRepairRejectedByBackoffFromGraftRejection() {
    return repairRejectedByBackoffFromGraftRejection;
  }

  public long getRepairCandidatesBlockedByBackoffPositiveScore() {
    return repairCandidatesBlockedByBackoffPositiveScore;
  }

  public long getRepairCandidatesBlockedByBackoffNegativeScore() {
    return repairCandidatesBlockedByBackoffNegativeScore;
  }

  public long getRepairRelaxedReceivedPruneConsidered() {
    return repairRelaxedReceivedPruneConsidered;
  }

  public long getRepairRelaxedReceivedPruneSelected() {
    return repairRelaxedReceivedPruneSelected;
  }

  public long getRepairRelaxedReceivedPruneSuccesses() {
    return repairRelaxedReceivedPruneSuccesses;
  }

  public long getRepairRelaxedReceivedPrunePositiveScoreSelected() {
    return repairRelaxedReceivedPrunePositiveScoreSelected;
  }

  public long getRepairRelaxedReceivedPruneNegativeScoreSelected() {
    return repairRelaxedReceivedPruneNegativeScoreSelected;
  }

  public long getRepairRelaxedReceivedPruneBlockedByBudget() {
    return repairRelaxedReceivedPruneBlockedByBudget;
  }

  public long getRepairRelaxedReceivedPruneBlockedNotPostWarmup() {
    return repairRelaxedReceivedPruneBlockedNotPostWarmup;
  }

  public long getRepairRelaxedReceivedPruneBlockedNoDOutDeficit() {
    return repairRelaxedReceivedPruneBlockedNoDOutDeficit;
  }

  public long getRepairRelaxedReceivedPruneGrafted() {
    return repairRelaxedReceivedPruneGrafted;
  }

  public long getRepairRelaxedReceivedPruneGraftAccepted() {
    return repairRelaxedReceivedPruneGraftAccepted;
  }

  public long getRepairRelaxedReceivedPruneGraftRejected() {
    return repairRelaxedReceivedPruneGraftRejected;
  }

  public long getRepairRelaxedReceivedPrunePruned() {
    return repairRelaxedReceivedPrunePruned;
  }

  public long getRepairRelaxedReceivedPruneBackoffReset() {
    return repairRelaxedReceivedPruneBackoffReset;
  }

  public long getRepairRelaxedReceivedPruneSurvived1Heartbeat() {
    return repairRelaxedReceivedPruneSurvived1Heartbeat;
  }

  public long getRepairRelaxedReceivedPruneSurvived5Heartbeats() {
    return repairRelaxedReceivedPruneSurvived5Heartbeats;
  }

  public long getRepairRelaxedReceivedPruneSurvived10Heartbeats() {
    return repairRelaxedReceivedPruneSurvived10Heartbeats;
  }

  public long getRepairRelaxedReceivedPruneLocalGraceActive() {
    return repairRelaxedReceivedPruneLocalGraceActive;
  }

  public long getRepairRelaxedReceivedPruneLocalPrunePreventedLowScore() {
    return repairRelaxedReceivedPruneLocalPrunePreventedLowScore;
  }

  public long getRepairRelaxedReceivedPruneLocalPrunePreventedOversubscription() {
    return repairRelaxedReceivedPruneLocalPrunePreventedOversubscription;
  }

  public long getRepairRelaxedReceivedPruneRemovedByReceivedPrune() {
    return repairRelaxedReceivedPruneRemovedByReceivedPrune;
  }

  public long getRepairRelaxedReceivedPruneRemovedByLocalLowScore() {
    return repairRelaxedReceivedPruneRemovedByLocalLowScore;
  }

  public long getRepairRelaxedReceivedPruneRemovedByLocalOversubscription() {
    return repairRelaxedReceivedPruneRemovedByLocalOversubscription;
  }

  public long getRepairRelaxedReceivedPruneRemovedByOther() {
    return repairRelaxedReceivedPruneRemovedByOther;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity() {
    return repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionAtCapacity;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore() {
    return repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionScore;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff() {
    return repairRelaxedReceivedPruneReceivedPruneReasonGraftRejectionBackoff;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneReasonLowScore() {
    return repairRelaxedReceivedPruneReceivedPruneReasonLowScore;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneReasonOversubscription() {
    return repairRelaxedReceivedPruneReceivedPruneReasonOversubscription;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe() {
    return repairRelaxedReceivedPruneReceivedPruneReasonLeaveOrUnsubscribe;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneReasonOther() {
    return repairRelaxedReceivedPruneReceivedPruneReasonOther;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneAfterGraft() {
    return repairRelaxedReceivedPruneReceivedPruneAfterGraft;
  }

  public long getRepairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat() {
    return repairRelaxedReceivedPruneReceivedPruneBeforeSurvived1Heartbeat;
  }

  public long getRepairRelaxedReceivedPruneSelectedPruneAgeSum() {
    return repairRelaxedReceivedPruneSelectedPruneAgeSum;
  }

  public long getRepairRelaxedReceivedPruneSelectedPruneAgeCount() {
    return repairRelaxedReceivedPruneSelectedPruneAgeCount;
  }

  public long getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum() {
    return repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeSum;
  }

  public long getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount() {
    return repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeCount;
  }

  public long getRepairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff() {
    return repairRelaxedReceivedPruneSelectedPruneAgeLtQuarterBackoff;
  }

  public long getRepairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff() {
    return repairRelaxedReceivedPruneSelectedPruneAgeQuarterToHalfBackoff;
  }

  public long getRepairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff() {
    return repairRelaxedReceivedPruneSelectedPruneAgeHalfToThreeQuartersBackoff;
  }

  public long getRepairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff() {
    return repairRelaxedReceivedPruneSelectedPruneAgeGeThreeQuartersBackoff;
  }

  public long getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff() {
    return repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeLtQuarterBackoff;
  }

  public long getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff() {
    return repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeQuarterToHalfBackoff;
  }

  public long
      getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff() {
    return repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeHalfToThreeQuartersBackoff;
  }

  public long getRepairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff() {
    return repairRelaxedReceivedPruneRemoteBackoffRejectionPruneAgeGeThreeQuartersBackoff;
  }

  public long getRepairRelaxReceivedPrunePrefilterCandidatesSeen() {
    return repairRelaxReceivedPrunePrefilterCandidatesSeen;
  }

  public long getRepairRelaxReceivedPrunePrefilterAgeKnown() {
    return repairRelaxReceivedPrunePrefilterAgeKnown;
  }

  public long getRepairRelaxReceivedPrunePrefilterAgeUnknown() {
    return repairRelaxReceivedPrunePrefilterAgeUnknown;
  }

  public long getRepairRelaxReceivedPrunePrefilterPassScore() {
    return repairRelaxReceivedPrunePrefilterPassScore;
  }

  public long getRepairRelaxReceivedPrunePrefilterBlockedByAge() {
    return repairRelaxReceivedPrunePrefilterBlockedByAge;
  }

  public long getRepairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff() {
    return repairRelaxReceivedPrunePrefilterAgeLtQuarterBackoff;
  }

  public long getRepairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff() {
    return repairRelaxReceivedPrunePrefilterAgeQuarterToHalfBackoff;
  }

  public long getRepairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff() {
    return repairRelaxReceivedPrunePrefilterAgeHalfToThreeQuartersBackoff;
  }

  public long getRepairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff() {
    return repairRelaxReceivedPrunePrefilterAgeGeThreeQuartersBackoff;
  }

  public long getReceiverRelaxGraftBackoffConsidered() {
    return receiverRelaxGraftBackoffConsidered;
  }

  public long getReceiverRelaxGraftBackoffSelected() {
    return receiverRelaxGraftBackoffSelected;
  }

  public long getReceiverRelaxGraftBackoffAccepted() {
    return receiverRelaxGraftBackoffAccepted;
  }

  public long getReceiverRelaxGraftBackoffBlockedByBudget() {
    return receiverRelaxGraftBackoffBlockedByBudget;
  }

  public long getReceiverRelaxGraftBackoffBlockedByScore() {
    return receiverRelaxGraftBackoffBlockedByScore;
  }

  public long getReceiverRelaxGraftBackoffBlockedByAge() {
    return receiverRelaxGraftBackoffBlockedByAge;
  }

  public long getReceiverRelaxGraftBackoffBlockedNotDOut() {
    return receiverRelaxGraftBackoffBlockedNotDOut;
  }

  public long getReceiverRelaxGraftBackoffBlockedOtherReason() {
    return receiverRelaxGraftBackoffBlockedOtherReason;
  }

  public long getReceiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff() {
    return receiverRelaxGraftBackoffAcceptedAgeLtQuarterBackoff;
  }

  public long getReceiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff() {
    return receiverRelaxGraftBackoffAcceptedAgeQuarterToHalfBackoff;
  }

  public long getReceiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff() {
    return receiverRelaxGraftBackoffAcceptedAgeHalfToThreeQuartersBackoff;
  }

  public long getReceiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff() {
    return receiverRelaxGraftBackoffAcceptedAgeGeThreeQuartersBackoff;
  }

  public long getReceiverRelaxGraftBackoffPrefilterCandidatesEncountered() {
    return receiverRelaxGraftBackoffPrefilterCandidatesEncountered;
  }

  public long getReceiverRelaxGraftBackoffPrefilterAgeKnown() {
    return receiverRelaxGraftBackoffPrefilterAgeKnown;
  }

  public long getReceiverRelaxGraftBackoffPrefilterAgeUnknown() {
    return receiverRelaxGraftBackoffPrefilterAgeUnknown;
  }

  public long getReceiverRelaxGraftBackoffPrefilterPassScore() {
    return receiverRelaxGraftBackoffPrefilterPassScore;
  }

  public long getReceiverRelaxGraftBackoffPrefilterBlockedByAge() {
    return receiverRelaxGraftBackoffPrefilterBlockedByAge;
  }

  public long getReceiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff() {
    return receiverRelaxGraftBackoffPrefilterAgeLtQuarterBackoff;
  }

  public long getReceiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff() {
    return receiverRelaxGraftBackoffPrefilterAgeQuarterToHalfBackoff;
  }

  public long getReceiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff() {
    return receiverRelaxGraftBackoffPrefilterAgeHalfToThreeQuartersBackoff;
  }

  public long getReceiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff() {
    return receiverRelaxGraftBackoffPrefilterAgeGeThreeQuartersBackoff;
  }

  public double getReceiverRelaxGraftBackoffScoreSum() {
    return receiverRelaxGraftBackoffScoreSum;
  }

  public long getReceiverRelaxGraftBackoffScoreCount() {
    return receiverRelaxGraftBackoffScoreCount;
  }

  public double getReceiverRelaxGraftBackoffScoreMin() {
    return receiverRelaxGraftBackoffScoreCount > 0 ? receiverRelaxGraftBackoffScoreMin : 0.0;
  }

  public double getReceiverRelaxGraftBackoffScoreMax() {
    return receiverRelaxGraftBackoffScoreCount > 0 ? receiverRelaxGraftBackoffScoreMax : 0.0;
  }

  public long getReceiverRelaxGraftBackoffScoreLtThresholdMinusOne() {
    return receiverRelaxGraftBackoffScoreLtThresholdMinusOne;
  }

  public long getReceiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf() {
    return receiverRelaxGraftBackoffScoreThresholdMinusOneToMinusHalf;
  }

  public long getReceiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold() {
    return receiverRelaxGraftBackoffScoreThresholdMinusHalfToThreshold;
  }

  public long getReceiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf() {
    return receiverRelaxGraftBackoffScoreThresholdToThresholdPlusHalf;
  }

  public long getReceiverRelaxGraftBackoffScoreGeThresholdPlusHalf() {
    return receiverRelaxGraftBackoffScoreGeThresholdPlusHalf;
  }

  public long getPxSenderTrustDecompositionCount() {
    return pxSenderTrustDecomposition.count;
  }

  public double getPxSenderTrustDecompositionTotalScoreSum() {
    return pxSenderTrustDecomposition.totalScoreSum;
  }

  public double getPxSenderTrustDecompositionThresholdSum() {
    return pxSenderTrustDecomposition.meshThresholdSum;
  }

  public double getPxSenderTrustDecompositionScoreMinusThresholdSum() {
    return pxSenderTrustDecomposition.scoreMinusMeshThresholdSum;
  }

  public double getPxSenderTrustDecompositionTimeInMeshContributionSum() {
    return pxSenderTrustDecomposition.timeInMeshContributionSum;
  }

  public double getPxSenderTrustDecompositionFirstDeliveriesContributionSum() {
    return pxSenderTrustDecomposition.firstDeliveriesContributionSum;
  }

  public double getPxSenderTrustDecompositionLocalInvalidDeliveriesPenaltySum() {
    return pxSenderTrustDecomposition.localInvalidDeliveriesPenaltySum;
  }

  public double getPxSenderTrustDecompositionLocalBrokenPromisesPenaltySum() {
    return pxSenderTrustDecomposition.localBrokenPromisesPenaltySum;
  }

  public double getPxSenderTrustDecompositionGlobalInvalidDeliveriesPenaltySum() {
    return pxSenderTrustDecomposition.globalInvalidDeliveriesPenaltySum;
  }

  public double getPxSenderTrustDecompositionGlobalBrokenPromisesPenaltySum() {
    return pxSenderTrustDecomposition.globalBrokenPromisesPenaltySum;
  }

  public long getReceiverGraftScoreRejectionDecompositionCount() {
    return receiverGraftScoreRejectionDecomposition.count;
  }

  public double getReceiverGraftScoreRejectionDecompositionTotalScoreSum() {
    return receiverGraftScoreRejectionDecomposition.totalScoreSum;
  }

  public double getReceiverGraftScoreRejectionDecompositionMeshThresholdSum() {
    return receiverGraftScoreRejectionDecomposition.meshThresholdSum;
  }

  public double getReceiverGraftScoreRejectionDecompositionScoreMinusMeshThresholdSum() {
    return receiverGraftScoreRejectionDecomposition.scoreMinusMeshThresholdSum;
  }

  public double getReceiverGraftScoreRejectionDecompositionTimeInMeshContributionSum() {
    return receiverGraftScoreRejectionDecomposition.timeInMeshContributionSum;
  }

  public double getReceiverGraftScoreRejectionDecompositionFirstDeliveriesContributionSum() {
    return receiverGraftScoreRejectionDecomposition.firstDeliveriesContributionSum;
  }

  public double getReceiverGraftScoreRejectionDecompositionLocalInvalidDeliveriesPenaltySum() {
    return receiverGraftScoreRejectionDecomposition.localInvalidDeliveriesPenaltySum;
  }

  public double getReceiverGraftScoreRejectionDecompositionLocalBrokenPromisesPenaltySum() {
    return receiverGraftScoreRejectionDecomposition.localBrokenPromisesPenaltySum;
  }

  public double getReceiverGraftScoreRejectionDecompositionGlobalInvalidDeliveriesPenaltySum() {
    return receiverGraftScoreRejectionDecomposition.globalInvalidDeliveriesPenaltySum;
  }

  public double getReceiverGraftScoreRejectionDecompositionGlobalBrokenPromisesPenaltySum() {
    return receiverGraftScoreRejectionDecomposition.globalBrokenPromisesPenaltySum;
  }

  public long getRemotePruneOutcomeDecompositionCount() {
    return remotePruneOutcomeDecomposition.count;
  }

  public double getRemotePruneOutcomeDecompositionTotalScoreSum() {
    return remotePruneOutcomeDecomposition.totalScoreSum;
  }

  public double getRemotePruneOutcomeDecompositionMeshThresholdSum() {
    return remotePruneOutcomeDecomposition.meshThresholdSum;
  }

  public double getRemotePruneOutcomeDecompositionScoreMinusMeshThresholdSum() {
    return remotePruneOutcomeDecomposition.scoreMinusMeshThresholdSum;
  }

  public double getRemotePruneOutcomeDecompositionTimeInMeshContributionSum() {
    return remotePruneOutcomeDecomposition.timeInMeshContributionSum;
  }

  public double getRemotePruneOutcomeDecompositionFirstDeliveriesContributionSum() {
    return remotePruneOutcomeDecomposition.firstDeliveriesContributionSum;
  }

  public double getRemotePruneOutcomeDecompositionLocalInvalidDeliveriesPenaltySum() {
    return remotePruneOutcomeDecomposition.localInvalidDeliveriesPenaltySum;
  }

  public double getRemotePruneOutcomeDecompositionLocalBrokenPromisesPenaltySum() {
    return remotePruneOutcomeDecomposition.localBrokenPromisesPenaltySum;
  }

  public double getRemotePruneOutcomeDecompositionGlobalInvalidDeliveriesPenaltySum() {
    return remotePruneOutcomeDecomposition.globalInvalidDeliveriesPenaltySum;
  }

  public double getRemotePruneOutcomeDecompositionGlobalBrokenPromisesPenaltySum() {
    return remotePruneOutcomeDecomposition.globalBrokenPromisesPenaltySum;
  }

  public long getDOutGraftsAcceptedAtCapacity() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutGraftsAcceptedAtCapacity;
  }

  public long getDOutGraftsRejectedAtCapacity() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutGraftsRejectedAtCapacity;
  }

  public long getCurrentOutboundMeshPeerCount() {
    if (!isDOutTrackingEnabled()) return 0;
    long count = 0;
    for (Map.Entry<String, HashSet<BigInteger>> entry : mesh.entrySet()) {
      count += countOutboundMeshPeers(entry.getKey(), entry.getValue());
    }
    return count;
  }

  public long getObservedOutboundMeshPeerSum() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutOutboundMeshPeerSum;
  }

  public long getObservedOutboundMeshPeerCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutOutboundMeshPeerObservationCount;
  }

  public long getObservedOutboundMeshPeerMin() {
    if (!isDOutTrackingEnabled()) return 0;
    if (dOutOutboundMeshPeerObservationCount == 0) {
      return 0;
    }
    return dOutOutboundMeshPeerMinObserved;
  }

  public long getDOutTarget() {
    return dOutTarget();
  }

  public long getDOutHeartbeatDeficitSum() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutHeartbeatDeficitSum;
  }

  public long getDOutHeartbeatDeficitNonzeroCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutHeartbeatDeficitNonzeroCount;
  }

  public long getDOutRepairAttemptsForDeficit() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutRepairAttemptsForDeficit;
  }

  public long getDOutRepairSuccesses() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutRepairSuccesses;
  }

  public long getDOutSteadyStateTopupAttempts() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutSteadyStateTopupAttempts;
  }

  public long getDOutSteadyStateTopupSuccesses() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutSteadyStateTopupSuccesses;
  }

  public long getDOutSteadyStateReplacements() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutSteadyStateReplacements;
  }

  public long getDOutSteadyStatePrunes() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutSteadyStatePrunes;
  }

  public long getDOutSteadyStateGrafts() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutSteadyStateGrafts;
  }

  public long getDOutHeartbeatObservationCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutHeartbeatObservationCount;
  }

  public long getDOutZeroOutboundObservationCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutZeroOutboundObservationCount;
  }

  public long getDOutDeficitObservationCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutDeficitObservationCount;
  }

  public long getDOutPostWarmupObservationCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutPostWarmupObservationCount;
  }

  public long getDOutPostWarmupZeroOutboundCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutPostWarmupZeroOutboundCount;
  }

  public long getDOutPostWarmupDeficitCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutPostWarmupDeficitCount;
  }

  public long getDOutPostWarmupOutboundMeshPeerMin() {
    if (!isDOutTrackingEnabled()) return 0;
    if (dOutPostWarmupObservationCount == 0) {
      return 0;
    }
    return dOutPostWarmupOutboundMeshPeerMinObserved;
  }

  public long getDOutMaxConsecutiveDeficitHeartbeats() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutMaxConsecutiveDeficitHeartbeats;
  }

  public long getDOutMaxConsecutiveZeroOutboundHeartbeats() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutMaxConsecutiveZeroOutboundHeartbeats;
  }

  public long getDOutEscalationAttempts() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutEscalationAttempts;
  }

  public long getDOutEscalationSuccesses() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutEscalationSuccesses;
  }

  public long getDOutZeroOutboundEmergencyAttempts() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutZeroOutboundEmergencyAttempts;
  }

  public long getDOutZeroOutboundEmergencySuccesses() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutZeroOutboundEmergencySuccesses;
  }

  public long getDOutInplacePromotionAttempts() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutInplacePromotionAttempts;
  }

  public long getDOutInplacePromotionSuccesses() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutInplacePromotionSuccesses;
  }

  public long getDOutInplaceZeroOutboundPromotions() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutInplaceZeroOutboundPromotions;
  }

  public long getDOutInplaceDeficitPromotions() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutInplaceDeficitPromotions;
  }

  public long getDOutForcedReseedAttempts() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedAttempts;
  }

  public long getDOutForcedReseedSuccesses() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedSuccesses;
  }

  public long getDOutForcedReseedGrafts() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedGrafts;
  }

  public long getDOutForcedReseedPrunes() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedPrunes;
  }

  public long getDOutForcedReseedSkippedNoCandidate() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedSkippedNoCandidate;
  }

  public long getDOutForcedReseedSkippedNoPrunableInbound() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedSkippedNoPrunableInbound;
  }

  public long getDOutForcedReseedEligibleCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedEligibleCount;
  }

  public long getDOutForcedReseedBlockedNotPostWarmup() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedBlockedNotPostWarmup;
  }

  public long getDOutForcedReseedBlockedConsecutiveZeroBelowThreshold() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedBlockedConsecutiveZeroBelowThreshold;
  }

  public long getDOutForcedReseedBlockedOutboundBeforePositive() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedBlockedOutboundBeforePositive;
  }

  public long getDOutForcedReseedBlockedDeficitBeforeZero() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedBlockedDeficitBeforeZero;
  }

  public long getDOutForcedReseedBlockedAfterInplaceNoRemainingDeficit() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedBlockedAfterInplaceNoRemainingDeficit;
  }

  public long getDOutForcedReseedBlockedAfterInplaceNotPersistent() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedBlockedAfterInplaceNotPersistent;
  }

  public long getDOutForcedReseedReachedCallsiteCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedReachedCallsiteCount;
  }

  public long getDOutForcedReseedZeroOutboundEmergencyCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedZeroOutboundEmergencyCount;
  }

  public long getDOutForcedReseedInplaceSuccessDuringZeroOutboundCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedInplaceSuccessDuringZeroOutboundCount;
  }

  public long getDOutForcedReseedAfterInplaceRemainingDeficitCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedAfterInplaceRemainingDeficitCount;
  }

  public long getDOutForcedReseedReturnedBeforeCallsiteCount() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedReturnedBeforeCallsiteCount;
  }

  public long getDOutForcedReseedEligibleByDeficit() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedEligibleByDeficit;
  }

  public long getDOutForcedReseedReachedCallsiteAfterInplace() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedReachedCallsiteAfterInplace;
  }

  public long getDOutForcedReseedEligibleByZeroRemainingDeficit() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedEligibleByZeroRemainingDeficit;
  }

  public long getDOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutForcedReseedReachedCallsiteAfterZeroRemainingDeficit;
  }

  public long getDOutMaxConsecutiveZeroRemainingDeficitHeartbeats() {
    if (!isDOutTrackingEnabled()) return 0;
    return dOutMaxConsecutiveZeroRemainingDeficitHeartbeats;
  }

  private boolean isPeerDegraded(BigInteger peerId) {
    Node peerNode = nodeIdtoNode(peerId, gossipid);
    if (peerNode == null) return false;
    Object protocol = peerNode.getProtocol(gossipid);
    if (!(protocol instanceof GossipSubProtocol)) return false;
    return ((GossipSubProtocol) protocol).isDegradedPeer();
  }

  public List<Map<String, Object>> getScoreSummaryRows() {
    List<Map<String, Object>> rows = new ArrayList<>();
    if (node == null) return rows;

    for (Map.Entry<TopicPeerKey, PeerScoreState> entry : topicPeerScores.entrySet()) {
      TopicPeerKey key = entry.getKey();
      PeerScoreState state = entry.getValue();
      Map<String, Object> row = new HashMap<>();
      row.put("node_id", node.getId().toString());
      row.put("topic", key.topic);
      row.put("peerId", key.peerId.toString());
      row.put("score_final", scoreOf(key.topic, key.peerId));
      row.put("meshHeartbeats", state.meshHeartbeats);
      row.put("firstDeliveries", state.firstDeliveries);
      row.put("brokenPromises", state.brokenPromises);
      row.put("degradedPeer", degradedPeer);
      row.put("peerIsDegraded", isPeerDegraded(key.peerId));
      rows.add(row);
    }

    return rows;
  }

  public static void resetExperimentState() {
    peers = new PeerTable();
  }

  private boolean isScoringEnabled() {
    return GossipCommonConfig.scoreEnabled;
  }

  /**
   * Set the callback function for Kademlia events.
   *
   * @param callback The callback function to set.
   */
  public void setEventsCallback(GossipEvent callback) {
    this.callback = callback;
  }

  private boolean shouldSuppressNode() {
    BigInteger nodeId = this.node == null ? null : this.node.getId();
    return PassiveCoalitionRegistry.shouldSuppressNode(nodeId)
        || PassiveEgoMeshRegistry.shouldSuppressNode(nodeId);
  }

  private boolean shouldSuppressApplicationCallback() {
    BigInteger nodeId = this.node == null ? null : this.node.getId();
    return PassiveCoalitionRegistry.shouldSuppressApplicationCallback(nodeId)
        || PassiveEgoMeshRegistry.shouldSuppressApplicationCallback(nodeId);
  }

  private boolean isDiscoverablePeer(BigInteger peerId) {
    return PassiveCoalitionRegistry.isDiscoverablePeer(peerId)
        && PassiveEgoMeshRegistry.isDiscoverablePeer(peerId);
  }
}
