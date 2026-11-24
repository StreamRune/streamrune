package org.streamrune.sqlite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;

/**
 * The {@link SnapshotPurger} for {@link SqliteEventStore}: purges the snapshots of a subject's
 * streams from {@code snapshot_store}, as {@code PostgresSnapshotStorePurger} does on Postgres.
 *
 * <p><b>What a purge writes.</b> For every stream the mapping names, the stream's row loses its
 * state ({@code state_type} and {@code state_payload} become {@code NULL}, {@code version} {@code
 * 0}) and its {@code purge_generation} goes up by one; a stream with no row gets one in that form.
 * The row stays so that a command in flight cannot put the snapshot back: a snapshot save writes
 * only while the row still has the generation its load saw (see {@link
 * SqliteEventStore#saveSnapshot(StreamId, org.streamrune.core.types.Version,
 * org.streamrune.core.AggregateState, int, long)}), and SQLite's write lock serializes the purge
 * and the save. Do not delete these rows by hand while commands may be in flight.
 *
 * <p><b>Why the application constructs it.</b> {@code snapshot_store} is keyed by the stream, not
 * by the subject: which streams carry a subject's data is the application's domain knowledge, so it
 * supplies {@code streamsForSubject}. Register this purger whenever the table may hold snapshots,
 * also after a switch to {@code SnapshotPolicy.never()}, which leaves earlier rows in place.
 *
 * <pre>{@code
 * ForgetSubjectService.builder()
 *     .cryptoEngine(engine)
 *     .purgers(List.of(
 *         new CustomerReadModelPurger(jdbc),
 *         new SqliteSnapshotStorePurger(database, subjectId -> customerAggregateStreams(subjectId))))
 *     .build();
 * }</pre>
 *
 * <p><b>One statement, one transaction.</b> Every stream of the subject is purged by one upsert,
 * which SQLite runs as one transaction of its own, whatever the number of streams (they are passed
 * as one JSON array, so SQLite's limit on bound parameters does not apply): a failure or a crash
 * leaves all of them purged or none, and a re-run of the forget converges either way. Idempotent
 * per {@link SubjectDataPurger}'s contract: purging a stream again raises its generation once more;
 * an empty or {@code null} stream collection writes nothing. {@code created_at} is stamped with the
 * purger's {@link Clock}.
 */
public final class SqliteSnapshotStorePurger implements SnapshotPurger {

  private static final Logger log = LoggerFactory.getLogger(SqliteSnapshotStorePurger.class);

  private static final ObjectMapper JSON = new ObjectMapper();

  // One statement for every stream of the subject: the JSON array of [aggregate_type,
  // aggregate_id] pairs is unnested by json_each, each pair served by the primary key. DISTINCT,
  // because an upsert that met one row twice would raise its generation twice. The WHERE true is
  // SQLite's rule for an upsert whose rows come from a SELECT. Parameters: created_at, the array.
  // Package-private: the tests hold this statement open in a transaction of their own.
  static final String PURGE_SNAPSHOTS =
      "INSERT INTO snapshot_store (aggregate_type, aggregate_id, version, state_type,"
          + " state_payload, created_at, purge_generation)"
          + " SELECT DISTINCT k.value ->> 0, k.value ->> 1, 0, NULL, NULL, ?, 1"
          + " FROM json_each(?) AS k WHERE true"
          + " ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET"
          + " version = 0, state_type = NULL, state_payload = NULL,"
          + " created_at = excluded.created_at,"
          + " purge_generation = snapshot_store.purge_generation + 1";

  private final SqliteDatabase database;
  private final Function<SubjectId, ? extends Collection<StreamId>> streamsForSubject;
  private final Clock clock;

  /**
   * Creates the purger, stamping {@code created_at} with {@link Clock#systemUTC()}.
   *
   * @param database the database owning {@code snapshot_store} (the one the event store was built
   *     on)
   * @param streamsForSubject maps a subject id to every stream whose snapshot may carry that
   *     subject's data; {@code null} or an empty result is "nothing to purge"
   * @throws NullPointerException if an argument is {@code null}
   */
  public SqliteSnapshotStorePurger(
      SqliteDatabase database,
      Function<SubjectId, ? extends Collection<StreamId>> streamsForSubject) {
    this(database, streamsForSubject, Clock.systemUTC());
  }

  /**
   * Creates the purger.
   *
   * @param database the database owning {@code snapshot_store}
   * @param streamsForSubject maps a subject id to every stream whose snapshot may carry that
   *     subject's data; {@code null} or an empty result is "nothing to purge"
   * @param clock stamps {@code created_at} of the purged rows
   * @throws NullPointerException if an argument is {@code null}
   */
  public SqliteSnapshotStorePurger(
      SqliteDatabase database,
      Function<SubjectId, ? extends Collection<StreamId>> streamsForSubject,
      Clock clock) {
    this.database = Objects.requireNonNull(database, "database is required");
    this.streamsForSubject =
        Objects.requireNonNull(streamsForSubject, "streamsForSubject is required");
    this.clock = Objects.requireNonNull(clock, "clock is required");
  }

  @Override
  public String name() {
    return "snapshot_store";
  }

  @Override
  public void purge(SubjectId subjectId) {
    Objects.requireNonNull(subjectId, "subjectId is required");
    Collection<StreamId> streams = streamsForSubject.apply(subjectId);
    if (streams == null || streams.isEmpty()) {
      return;
    }
    String streamsJson = streamsJson(streams);
    long createdAt = SqliteTimestamps.toMicros(clock.instant());
    // One statement in autocommit: SQLite runs it as one transaction of its own, which takes the
    // write lock when the statement starts (waiting at most busy_timeout) and purges every stream
    // or none. It queues with the other writers of the process first, on the file's write mutex.
    try (var conn = database.dataSource().getConnection()) {
      int purged =
          SqliteTransactions.writeStatement(
              conn,
              c -> {
                try (var ps = c.prepareStatement(PURGE_SNAPSHOTS)) {
                  ps.setLong(1, createdAt);
                  ps.setString(2, streamsJson);
                  return ps.executeUpdate();
                }
              });
      log.info(
          "GDPR forget: purged the snapshots of {} stream(s) in snapshot_store for subject-hash={}",
          purged,
          subjectId.redacted());
    } catch (SQLException e) {
      EventStoreException lockFailure =
          SqliteEventStore.lockFailure(e, "purge of snapshot_store rows for subject");
      if (lockFailure != null) {
        throw lockFailure;
      }
      throw new EventStoreException("Failed to purge snapshot_store rows for subject", e);
    }
  }

  /** {@code streams} as the JSON array of {@code [aggregate_type, aggregate_id]} pairs. */
  static String streamsJson(Collection<StreamId> streams) {
    List<List<String>> pairs = new ArrayList<>(streams.size());
    for (StreamId stream : streams) {
      pairs.add(List.of(stream.aggregateType().value(), stream.aggregateId().value()));
    }
    try {
      return JSON.writeValueAsString(pairs);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("A list of strings always serializes", e);
    }
  }
}
