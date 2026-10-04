package org.streamrune.postgres;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Support for the projection crash tests: the backend killer (a "crash" between the read-model
 * write and the checkpoint is {@code pg_terminate_backend} of the processor's transaction
 * connection, observed from a second connection), per-role data sources told apart by {@code
 * application_name}, catalogue probes, a counter read model and the projections over it.
 */
final class CrashItSupport {

  private CrashItSupport() {}

  /** One unpooled connection per {@code getConnection()}, tagged with {@code applicationName}. */
  static PGSimpleDataSource dataSource(PostgreSQLContainer<?> pg, String applicationName) {
    var ds = new PGSimpleDataSource();
    ds.setUrl(pg.getJdbcUrl());
    ds.setUser(pg.getUsername());
    ds.setPassword(pg.getPassword());
    ds.setApplicationName(applicationName);
    return ds;
  }

  /** Like {@link #dataSource} with {@code search_path} set to {@code schema}. */
  static PGSimpleDataSource dataSourceForSchema(
      PostgreSQLContainer<?> pg, String applicationName, String schema) {
    var ds = dataSource(pg, applicationName);
    ds.setCurrentSchema(schema);
    return ds;
  }

  /**
   * Terminates the one backend of {@code applicationName} that is idle in an open transaction (the
   * processor's connection between the updater's last statement and the checkpoint save), waiting
   * up to five seconds for it to exit. Exactly one backend must match: zero means the session was
   * not idle-in-transaction at that instant, more than one means another connection of that role
   * was left open.
   */
  static void terminateIdleInTransaction(DataSource killer, String applicationName) {
    try (var c = killer.getConnection();
        var ps =
            c.prepareStatement(
                "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity"
                    + " WHERE application_name = ? AND state LIKE 'idle in transaction%'")) {
      ps.setString(1, applicationName);
      int terminated = 0;
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          if (!rs.getBoolean(1)) {
            throw new IllegalStateException(
                "pg_terminate_backend did not confirm the exit of the "
                    + applicationName
                    + " backend");
          }
          terminated++;
        }
      }
      if (terminated != 1) {
        throw new IllegalStateException(
            "expected exactly one idle-in-transaction backend for "
                + applicationName
                + ", terminated "
                + terminated);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Whether {@code schema.table} exists, by {@code to_regclass}. */
  static boolean tableExists(DataSource ds, String schema, String table) {
    try (var c = ds.getConnection();
        var ps = c.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
      ps.setString(1, schema + "." + table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The row count of {@code schema.table}; {@code 0} when the table does not exist. */
  static long rowCount(DataSource ds, String schema, String table) {
    if (!tableExists(ds, schema, table)) {
      return 0L;
    }
    try (var c = ds.getConnection();
        var st = c.createStatement();
        var rs = st.executeQuery("SELECT count(*) FROM " + schema + "." + table)) {
      rs.next();
      return rs.getLong(1);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Appends {@code n} {@code Ping} events on stream {@code s} (versions 1..n) in one append. */
  static void appendPings(InMemoryEventStore store, int n) {
    var events = new ArrayList<EventEnvelope>();
    for (int i = 1; i <= n; i++) {
      events.add(
          EventStoreFixture.event(
              TestStreams.stream("s"), new Version(i), new EventType("Ping"), new Ping(i)));
    }
    store.append(TestStreams.stream("s"), events, new Version(0));
  }

  /** One envelope per global offset in {@code [fromOffset, toOffset]}, for a processor call. */
  static List<EventEnvelope> batch(long fromOffset, long toOffset) {
    var events = new ArrayList<EventEnvelope>();
    for (long o = fromOffset; o <= toOffset; o++) {
      events.add(
          new EventEnvelope(
              GlobalOffset.of(o),
              TestStreams.stream("s"),
              new Version(o),
              new EventType("Ping"),
              new Ping((int) o),
              new EventMetadata(
                  IdGenerator.generateEventId(),
                  IdGenerator.generateCommandId(),
                  null,
                  null,
                  CorrelationId.of("crash-it"),
                  null,
                  null,
                  Instant.now())));
    }
    return events;
  }

  record Ping(int n) implements DomainEvent {}

  /** A counter read model: how many events the projection has applied to {@code id}. */
  record CountView(@JsonProperty("id") String id, @JsonProperty("applied") int applied) {
    @JsonCreator
    CountView {}
  }

  /**
   * A counter: adds the batch size to the stored count. Double-applies on redelivery, by design —
   * it pins what an at-least-once redelivery does to a non-idempotent read model.
   */
  static final class CountingProjection extends BaseProjection {
    CountingProjection(ProjectionRepository repository, String projectionName) {
      super(repository, projectionName);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      int applied = findById("row", CountView.class).map(CountView::applied).orElse(0);
      save("row", new CountView("row", applied + batch.size()));
    }
  }

  /** Fails the first {@code saveOffset} (store outage), then forwards. Counts every attempt. */
  static final class FailingOnceOffsetStore implements OffsetStore {
    private final OffsetStore delegate;
    final AtomicInteger saveAttempts = new AtomicInteger();
    private volatile boolean failed;

    FailingOnceOffsetStore(OffsetStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return delegate.getLastOffset(projectionName);
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      saveAttempts.incrementAndGet();
      if (!failed) {
        failed = true;
        throw new IllegalStateException("offset store down");
      }
      delegate.saveOffset(projectionName, offset);
    }

    @Override
    public void reset(ProjectionName projectionName) {
      delegate.reset(projectionName);
    }
  }
}
