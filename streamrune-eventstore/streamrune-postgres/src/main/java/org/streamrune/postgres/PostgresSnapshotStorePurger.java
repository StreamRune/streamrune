package org.streamrune.postgres;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;

/**
 * Opt-in {@link SubjectDataPurger} that deletes a subject's rows from the framework-owned {@code
 * snapshot_store} table.
 *
 * <p><b>Why this exists.</b> {@link org.streamrune.runtime.gdpr.ForgetSubjectService#forget} does
 * two things: crypto-shred the subject's key, then run every REGISTERED {@link SubjectDataPurger}.
 * The framework's own {@code snapshot_store} is never among them unless the application registers
 * this class explicitly — {@code forget()} therefore leaves non-{@code @Encrypted} DERIVED
 * plaintext (a computed age, an email domain, anything the aggregate's {@code evolve()} copied out
 * of an {@code @Encrypted} field into a plain one) sitting in the last snapshot taken before the
 * forget, readable indefinitely: a post-forget {@code load()} tombstones only the
 * {@code @Encrypted} fields still present in the stored JSON (the snapshot-load crypto-mapping
 * guard has nothing to catch on a plain field), and no new snapshot gets taken without new commands
 * against that aggregate to overwrite it.
 *
 * <p><b>Why this is opt-in, not automatic.</b> {@code snapshot_store} is keyed by the stream —
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
 * <p>Idempotent per {@link SubjectDataPurger}'s contract: deleting an already-absent row, or an
 * empty/{@code null} stream collection, is a safe no-op.
 */
public final class PostgresSnapshotStorePurger implements SubjectDataPurger {

  private static final Logger log = LoggerFactory.getLogger(PostgresSnapshotStorePurger.class);

  // One statement for every stream of the subject: the two arrays are zipped by unnest into
  // (aggregate_type, aggregate_id) pairs, each served by the primary key.
  private static final String DELETE_SNAPSHOTS =
      "DELETE FROM snapshot_store s"
          + " USING unnest(?::varchar[], ?::varchar[]) AS k(aggregate_type, aggregate_id)"
          + " WHERE s.aggregate_type = k.aggregate_type AND s.aggregate_id = k.aggregate_id";

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
        var ps = conn.prepareStatement(DELETE_SNAPSHOTS)) {
      ps.setArray(1, conn.createArrayOf("varchar", types));
      ps.setArray(2, conn.createArrayOf("varchar", ids));
      int deleted =
          PostgresTransactions.commitIfManual(conn, "snapshot purge", c -> ps.executeUpdate());
      log.info(
          "GDPR forget: purged {} snapshot_store row(s) for subject-hash={}",
          deleted,
          subjectId.redacted());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to purge snapshot_store rows for subject", e);
    }
  }
}
