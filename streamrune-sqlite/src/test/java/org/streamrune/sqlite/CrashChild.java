package org.streamrune.sqlite;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractEvent;
import org.streamrune.test.EventStoreContract.ContractState;

/**
 * The program a crash test runs in a second JVM: it writes to the database file and dies at a point
 * of one write with {@link Runtime#halt(int)}, exit code {@link SqliteCrashPoints#HALTED}, as a
 * killed process dies: no rollback, no close, no shutdown hook.
 *
 * <ul>
 *   <li>{@code append <db> <point>}: appends one event to stream {@link #A}, then three more, and
 *       dies at {@code point} of the second append.
 *   <li>{@code snapshot <db> <point>}: appends two events, saves a snapshot {@code at-1}, then dies
 *       at {@code point} of saving {@code at-2}.
 *   <li>{@code purge <db> <point>}: snapshots streams {@code k1}, {@code k2} and {@code k3}, then
 *       dies at {@code point} of the purge of all three.
 * </ul>
 *
 * A write that completes prints {@link #NOT_KILLED} and exits normally, which fails the test.
 */
public final class CrashChild {

  static final String NOT_KILLED = "NOT_KILLED";
  static final StreamId A = StreamId.of(AggregateType.of("order"), AggregateId.of("A"));
  static final List<StreamId> PURGED = List.of(stream("k1"), stream("k2"), stream("k3"));

  private static final Map<String, Point> POINTS =
      Map.of(
          "OFFSET_RESERVATION", Point.OFFSET_RESERVATION,
          "SECOND_EVENT", Point.SECOND_EVENT,
          "FIRST_SNAPSHOT_ROW", Point.FIRST_SNAPSHOT_ROW,
          "SECOND_SNAPSHOT_ROW", Point.SECOND_SNAPSHOT_ROW,
          "AFTER_COMMIT", Point.AFTER_COMMIT);

  private CrashChild() {}

  public static void main(String[] args) {
    String mode = args[0];
    Path db = Path.of(args[1]);
    Point point = POINTS.get(args[2]);
    SqliteCrashPoints crashPoints = new SqliteCrashPoints(Kill.HALT_PROCESS);
    SqliteDatabase database =
        SqliteDatabase.open(crashPoints.connections(TestDatabases.dataSource(db)));
    EventStore store =
        new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
            .create();
    switch (mode) {
      case "append" -> {
        store.append(A, events(1), Version.initial());
        crashPoints.arm(point);
        store.append(A, events(3), new Version(1));
      }
      case "snapshot" -> {
        store.append(A, events(2), Version.initial());
        store.saveSnapshot(A, new Version(1), new ContractState("at-1"));
        crashPoints.arm(point);
        store.saveSnapshot(A, new Version(2), new ContractState("at-2"));
      }
      case "purge" -> {
        for (StreamId stream : PURGED) {
          store.saveSnapshot(stream, new Version(1), new ContractState("v1"));
        }
        crashPoints.arm(point);
        new SqliteSnapshotStorePurger(database, subject -> PURGED).purge(SubjectId.of("crashed"));
      }
      default -> throw new IllegalArgumentException("unknown mode " + mode);
    }
    System.out.println(NOT_KILLED);
    System.out.flush();
  }

  static StreamId stream(String id) {
    return StreamId.of(AggregateType.of("order"), AggregateId.of(id));
  }

  private static List<EventEnvelope> events(int count) {
    List<EventEnvelope> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(
          new EventEnvelope(
              GlobalOffset.initial(),
              A,
              Version.initial(),
              EventStoreContract.EVENT_TYPE,
              new ContractEvent("e" + i),
              new EventMetadata(
                  IdGenerator.generateEventId(),
                  IdGenerator.generateCommandId(),
                  null,
                  CorrelationId.of(UUID.randomUUID().toString()),
                  null,
                  null,
                  java.time.Instant.now())));
    }
    return events;
  }
}
