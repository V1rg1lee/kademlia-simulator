package peersim.kademlia.gossipsub.inference;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import peersim.kademlia.gossipsub.inference.passive.PassiveEgoMeshAttack;

/** Shared CSV exporter for topology inference event timelines. */
public final class InferenceEventCsvWriter {

  public interface ObserverRoleResolver {
    String resolve(PassiveEgoMeshAttack.InferenceEvent event);
  }

  public interface AttackPhaseResolver {
    String resolve(PassiveEgoMeshAttack.InferenceEvent event);
  }

  private static final Comparator<PassiveEgoMeshAttack.InferenceEvent> EVENT_COMPARATOR =
      Comparator.comparingLong((PassiveEgoMeshAttack.InferenceEvent event) -> event.heartbeatIndex)
          .thenComparingInt(event -> event.observerIndex)
          .thenComparing(event -> event.observerId)
          .thenComparing(event -> event.topic)
          .thenComparing(event -> event.peerId)
          .thenComparing(event -> event.action)
          .thenComparing(event -> event.cause);

  private InferenceEventCsvWriter() {}

  public static void write(
      Path path,
      Iterable<PassiveEgoMeshAttack> attacks,
      ObserverRoleResolver observerRoleResolver,
      AttackPhaseResolver attackPhaseResolver)
      throws IOException {
    ArrayList<PassiveEgoMeshAttack.InferenceEvent> events = new ArrayList<>();
    for (PassiveEgoMeshAttack attack : attacks) {
      if (attack != null) {
        events.addAll(attack.snapshotInferenceEvents());
      }
    }
    events.sort(EVENT_COMPARATOR);

    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      writer.write(
          "heartbeat_index,observer_index,observer_id,observer_role,attack_phase,topic,peer_id,"
              + "action,cause,inferred_neighbor_present");
      writer.newLine();
      for (PassiveEgoMeshAttack.InferenceEvent event : events) {
        String observerRole =
            observerRoleResolver == null ? "" : observerRoleResolver.resolve(event);
        String attackPhase = attackPhaseResolver == null ? "" : attackPhaseResolver.resolve(event);
        writeRow(writer, event, observerRole, attackPhase);
      }
    }
  }

  private static void writeRow(
      BufferedWriter writer,
      PassiveEgoMeshAttack.InferenceEvent event,
      String observerRole,
      String attackPhase)
      throws IOException {
    List<String> fields =
        List.of(
            String.valueOf(event.heartbeatIndex),
            String.valueOf(event.observerIndex),
            event.observerId,
            observerRole == null ? "" : observerRole,
            attackPhase == null ? "" : attackPhase,
            event.topic,
            event.peerId,
            event.action,
            event.cause,
            String.valueOf(event.inferredNeighborPresent));
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) writer.write(',');
      writer.write(csv(fields.get(i)));
    }
    writer.newLine();
  }

  private static String csv(String value) {
    String safe = value == null ? "" : value;
    if (!safe.contains(",") && !safe.contains("\"") && !safe.contains("\n")) {
      return safe;
    }
    return "\"" + safe.replace("\"", "\"\"") + "\"";
  }
}
