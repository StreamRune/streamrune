package org.streamrune.postgres;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;

/**
 * The {@link SnapshotPurger} for {@code PostgresEventStore}: purges the snapshots of a subject's
 * streams from the framework-owned {@code snapshot_store} table.
 *
 * <p><b>What a purge writes.</b> For every stream the mapping names, the stream's row loses its
 * state ({@code state_type} and {@code state_payload} become {@code NULL}) and its {@code
 * purge_generation} goes up by one; a stream with no row gets one in that form. The row stays so
 * that a command in flight cannot put the snapshot back: a command loads the aggregate, decides,
 * appends, and only then saves a snapshot, and a forget can run between the load and the save. The
 * save is a conditional upsert that writes only while the row still has the generation the load saw
 * (see {@code PostgresEventStore#saveSnapshot(StreamId, Version, AggregateState, int, long)}), so a
 * state loaded before the purge, which still holds whatever it copied from the subject's
 * {@code @Encrypted} fields in the clear, is never stored after it. The purge and the save upsert
 * one primary key, so the database serializes them on every replica, with any {@code
 * AggregateLocker}. The first command that loads after the purge sees the new generation and
 * snapshots again, from events whose {@code @Encrypted} fields read {@code [REDACTED]} on a replica
 * whose decrypt cache does not hold the subject's plaintext (a peer's cache keeps it until the
 * forget signal evicts it; see the GDPR guide). A loaded row without a state is no snapshot: the
 * load replays the stream's events.
 *
 * <p><b>Rows stay.</b> A purge leaves one row per stream the mapping names, with {@code version}
 * {@code 0} and no state, including a stream that never had a snapshot or does not exist. Do not
 * delete these rows by hand while commands may be in flight: the deleted row's generation is lost,
 * and a command that loaded the stream before the purge can store its snapshot again.
 *
 * <p><b>Why this exists.</b> {@link org.streamrune.runtime.gdpr.ForgetSubjectService#forget} does
 * two things: crypto-shred the subject's key, then run every REGISTERED {@link SubjectDataPurger}.
 * Without this purger among them, {@code forget()} leaves every non-{@code @Encrypted} field of the
 * aggregate state (a name the state copied in the clear, a computed age, an email domain, anything
 * the aggregate's {@code evolve()} copied out of an {@code @Encrypted} field into a plain one)
 * sitting in the last snapshot taken before the forget, readable indefinitely: a post-forget {@code
 * load()} tombstones only the {@code @Encrypted} fields still present in the stored JSON (the
 * snapshot-load crypto-mapping guard has nothing to catch on a plain field), and no new snapshot
 * gets taken without new commands against that aggregate to overwrite it. The forget service's
 * builder therefore refuses a service without a {@link SnapshotPurger} unless the application
 * states {@code withoutSnapshotPurge()}.
 *
 * <p><b>Why the application constructs it.</b> {@code snapshot_store} is keyed by the stream —
 * {@code (aggregate_type, aggregate_id)} — not by {@code subject_id}: unlike the read models {@link
 * SubjectDataPurger}'s own javadoc describes ({@code DELETE ... WHERE subject_id = ?}), there is no
 * indexed, subject-keyed column to purge against, and no framework-level way to derive one: the
 * relationship between a stream and the subject(s) whose PII it carries is an APPLICATION domain
 * decision (a {@code Customer} aggregate's id may equal the subject id directly; an {@code Order}
 * aggregate's does not, even though its state carries an {@code @Encrypted} field tied to the
 * customer subject). The application therefore supplies {@code streamsForSubject}: the same kind of
 * subject-id-to-affected-rows mapping every other {@link SubjectDataPurger} implements internally
 * against its own read model, here handed in explicitly because {@code snapshot_store} has no read
 * model of its own for this purger to query.
 *
 * <p><b>Do not skip this purger because of {@code SnapshotPolicy.never()}.</b> {@code never()}
 * stops NEW rows from being written (and, per {@code PostgresEventStore#loadIgnoringSnapshot},
 * stops any row from being READ), but it does not delete rows a stream already accumulated under an
 * earlier {@code EveryNEvents} policy — those rows are never read again and never overwritten, so
 * they persist indefinitely and still carry whatever derived plaintext this class exists to purge.
 * Register this purger whenever the table may hold rows from before a switch to {@code never()},
 * not only while actively snapshotting.
 *
 * <p>Register alongside the application's own purgers:
 *
 * <pre>{@code
 * ForgetSubjectService.builder()
 *     .cryptoEngine(engine)
 *     .purgers(List.of(
 *         new CustomerReadModelPurger(jdbc),
 *         new PostgresSnapshotStorePurger(dataSource, subjectId -> customerAggregateStreams(subjectId))))
 *     .build();
 * }</pre>
 *
 * <p>Idempotent per {@link SubjectDataPurger}'s contract: purging a stream again raises its
 * generation once more and leaves it without a state, and an empty or {@code null} stream
 * collection writes nothing. One statement purges every stream of the subject, so a failure or a
 * crash leaves either all of them purged or none; a re-run of the forget converges either way.
 */
public final class PostgresSnapshotStorePurger implements SnapshotPurger {

  private static final Logger log = LoggerFactory.getLogger(PostgresSnapshotStorePurger.class);

  // One statement for every stream of the subject: the two arrays are zipped by unnest into
  // (aggregate_type, aggregate_id) pairs, each served by the primary key. DISTINCT, because an
  // upsert may not touch one row twice; ORDER BY, so two purges of overlapping streams lock their
  // rows in one order.
  // Package-private: the tests hold this statement open in a transaction of their own.
  static final String PURGE_SNAPSHOTS =
      "INSERT INTO snapshot_store AS s (aggregate_type, aggregate_id, version, state_type,"
          + " state_payload, purge_generation)"
          + " SELECT DISTINCT k.aggregate_type, k.aggregate_id, 0, NULL, NULL::jsonb, 1"
          + " FROM unnest(?::varchar[], ?::varchar[]) AS k(aggregate_type, aggregate_id)"
          + " ORDER BY k.aggregate_type, k.aggregate_id"
          + " ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET"
          + " version = 0, state_type = NULL, state_payload = NULL, created_at = NOW(),"
          + " purge_generation = s.purge_generation + 1";

  private final DataSource dataSource;
  private final Function<SubjectId, ? extends Collection<StreamId>> streamsForSubject;

  /**
   * Creates the purger.
   *
   * @param dataSource the JDBC data source owning {@code snapshot_store} (the same one {@link
   *     PostgresEventStoreFactory} was configured with)
   * @param streamsForSubject maps a subject id to every stream whose snapshot may carry that
   *     subject's data; an application-supplied lookup (e.g. a read model query, or a fixed
   *     derivation like {@code StreamId.of(CUSTOMER, new AggregateId(subjectId.value()))} when a
   *     subject is one aggregate of a known type). {@code null} or an empty result is treated as
   *     "nothing to purge."
   */
  public PostgresSnapshotStorePurger(
      DataSource dataSource,
      Function<SubjectId, ? extends Collection<StreamId>> streamsForSubject) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource is required");
    this.streamsForSubject =
        Objects.requireNonNull(streamsForSubject, "streamsForSubject is required");
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
    String[] types = new String[streams.size()];
    String[] ids = new String[streams.size()];
    int i = 0;
    for (StreamId stream : streams) {
      types[i] = stream.aggregateType().value();
      ids[i] = stream.aggregateId().value();
      i++;
    }
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(PURGE_SNAPSHOTS)) {
      ps.setArray(1, conn.createArrayOf("varchar", types));
      ps.setArray(2, conn.createArrayOf("varchar", ids));
      int purged =
          PostgresTransactions.commitIfManual(conn, "snapshot purge", c -> ps.executeUpdate());
      log.info(
          "GDPR forget: purged the snapshots of {} stream(s) in snapshot_store for subject-hash={}",
          purged,
          subjectId.redacted());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to purge snapshot_store rows for subject", e);
    }
  }
}
