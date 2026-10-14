package org.streamrune.e2e;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.sql.DataSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.GlobalOffset;

/**
 * The invariant the forward fence keeps, read from the database two ways.
 *
 * <ul>
 *   <li>{@link #lateForwardCommands}: from {@code command_inbox}. A forward command of a saga (key
 *       {@code saga:<id>:<offset>:<index>}) whose last stored event lies after the first stored
 *       event of one of the saga's compensation commands (key {@code
 *       saga:<id>:episode:<version>:comp:<index>}). The saga ids of these tests contain no {@code
 *       :}, and every command of them stores an event.
 *   <li>{@link #sagasWithAForwardEventAfterACompensationEvent}: from {@code event_stream}, read
 *       through {@link EventStore#readGlobalStream}. The events a saga's commands stored carry the
 *       saga in {@code metadata.sagaOrigin}; no {@code Reserved} or {@code Shipped} may follow the
 *       saga's first {@code Released} or {@code Refunded}.
 * </ul>
 */
final class SagaForwardFenceInvariant {

  /** Must return no row: a forward command stored after its saga's first compensation command. */
  static final String QUERY =
      """
      SELECT f.idempotency_key, c.idempotency_key
        FROM command_inbox f
        JOIN command_inbox c
          ON split_part(c.idempotency_key, ':', 2) = split_part(f.idempotency_key, ':', 2)
       WHERE c.idempotency_key LIKE 'saga:%:episode:%:comp:%'
         AND f.idempotency_key LIKE 'saga:%'
         AND f.idempotency_key NOT LIKE 'saga:%:episode:%'
         AND (SELECT max(o) FROM unnest(f.global_offsets) o)
           > (SELECT min(o) FROM unnest(c.global_offsets) o)
       ORDER BY f.idempotency_key, c.idempotency_key
      """;

  /** One row of {@link #QUERY}. */
  record LateForwardCommand(String forwardKey, String compensationKey) {}

  private SagaForwardFenceInvariant() {}

  static List<LateForwardCommand> lateForwardCommands(DataSource dataSource) throws SQLException {
    List<LateForwardCommand> late = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(QUERY);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        late.add(new LateForwardCommand(rs.getString(1), rs.getString(2)));
      }
    }
    return late;
  }

  /** The distinct forward keys among {@code late}. */
  static List<String> forwardKeysOf(List<LateForwardCommand> late) {
    return List.copyOf(new TreeSet<>(late.stream().map(LateForwardCommand::forwardKey).toList()));
  }

  /**
   * The names of the events each saga's commands stored, by saga id, in global order: every event
   * whose metadata names the saga that dispatched its command.
   */
  static Map<String, List<String>> storedBySaga(EventStore store) {
    Map<String, List<String>> bySaga = new LinkedHashMap<>();
    GlobalOffset after = GlobalOffset.initial();
    while (true) {
      List<EventEnvelope> page = store.readGlobalStream(after, 1_000);
      if (page.isEmpty()) {
        return bySaga;
      }
      for (EventEnvelope event : page) {
        SagaOrigin origin = event.metadata().sagaOrigin();
        if (origin != null) {
          bySaga
              .computeIfAbsent(origin.sagaId().value(), id -> new ArrayList<>())
              .add(event.event().getClass().getSimpleName());
        }
      }
      after = page.getLast().globalOffset();
    }
  }

  /** Whether a forward event follows the first compensation event in {@code stored}. */
  static boolean aForwardEventAfterTheFirstCompensationEvent(List<String> stored) {
    int first = -1;
    for (int i = 0; i < stored.size(); i++) {
      if (SagaForwardFenceNodes.COMPENSATION_EVENTS.contains(stored.get(i))) {
        first = i;
        break;
      }
    }
    return first >= 0
        && stored.subList(first, stored.size()).stream()
            .anyMatch(SagaForwardFenceNodes.FORWARD_EVENTS::contains);
  }

  /** The sagas, with their stored events, that broke the invariant in {@code event_stream}. */
  static Map<String, List<String>> sagasWithAForwardEventAfterACompensationEvent(EventStore store) {
    Map<String, List<String>> broken = new TreeMap<>();
    storedBySaga(store)
        .forEach(
            (saga, stored) -> {
              if (aForwardEventAfterTheFirstCompensationEvent(stored)) {
                broken.put(saga, stored);
              }
            });
    return broken;
  }

  /**
   * The sagas whose compensation was stored more than once: a compensation event stored twice, or
   * compensation keys of more than one episode. A saga opens one episode at most, and each of its
   * compensation commands is executed once, whichever node runs it.
   */
  static Map<String, String> sagasWithACompensationStoredTwice(
      EventStore store, DataSource dataSource) throws SQLException {
    Map<String, String> twice = new TreeMap<>();
    storedBySaga(store)
        .forEach(
            (saga, stored) -> {
              for (String compensation : SagaForwardFenceNodes.COMPENSATION_EVENTS) {
                long times = stored.stream().filter(compensation::equals).count();
                if (times > 1) {
                  twice.put(saga, compensation + " stored " + times + " times: " + stored);
                }
              }
            });
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT split_part(idempotency_key, ':', 2),"
                    + " count(DISTINCT split_part(idempotency_key, ':', 4))"
                    + " FROM command_inbox WHERE idempotency_key LIKE 'saga:%:episode:%:comp:%'"
                    + " GROUP BY 1 HAVING count(DISTINCT split_part(idempotency_key, ':', 4)) > 1");
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        twice.put(rs.getString(1), "compensation keys of " + rs.getLong(2) + " episodes");
      }
    }
    return twice;
  }

  /** The compensation keys of the saga {@code sagaId}, in key order. */
  static List<String> compensationKeysOf(DataSource dataSource, String sagaId) throws SQLException {
    List<String> keys = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT idempotency_key FROM command_inbox WHERE idempotency_key LIKE ?"
                    + " ORDER BY idempotency_key")) {
      ps.setString(1, "saga:" + sagaId + ":episode:%:comp:%");
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          keys.add(rs.getString(1));
        }
      }
    }
    return keys;
  }
}
