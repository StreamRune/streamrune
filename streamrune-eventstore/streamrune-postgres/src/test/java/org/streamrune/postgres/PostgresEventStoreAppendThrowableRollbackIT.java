package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Predicate;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A non-{@link java.sql.SQLException} {@link Throwable} escaping {@code append} / {@code
 * appendWithKey} <em>mid-transaction</em> (after the event rows are inserted, before {@code
 * conn.commit()}) must roll the transaction back — NOT let the {@code finally}'s {@code
 * setAutoCommit(true)} silently COMMIT the partial transaction.
 *
 * <p>The fault is injected by a {@link Connection} proxy whose {@code prepareStatement} throws a
 * plain {@link RuntimeException} for the {@code event_audit_log} INSERT — which runs last, after
 * the event rows and the outbox rows have already been inserted in the same transaction. Neither
 * inner {@code catch} (SQLException / EventStoreException) matches it, so before the fix the {@code
 * finally} restored autoCommit on a connection with an open transaction and pgjdbc COMMITted the
 * partial write: durable domain events + outbox claim WITHOUT their audit rows (and, for the
 * variant, the outbox rows without the events / the inbox claim without either).
 */
@Testcontainers
class PostgresEventStoreAppendThrowableRollbackIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_esint1_it");

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;

  record MappedEvent(String value) implements DomainEvent {}

  static final OutboxEventMapper TEST_MAPPER =
      envelope ->
          List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(envelope.metadata().eventId().value()),
                  "{\"k\":1}",
                  "MappedEvent",
                  envelope.streamId()));

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return MappedEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of(MappedEvent.class);
          }
        };

    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  // ── (a) append rolls back a partial transaction on a mid-tx non-SQL Throwable ────────

  @Test
  void append_nonSqlThrowableMidTransaction_rollsBackEverything() throws Exception {
    DataSource faulting =
        faultInjectingDataSource(dataSource, sql -> sql.contains("event_audit_log"));
    var outboxStore = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    var store =
        PostgresEventStore.builder()
            .dataSource(faulting)
            .typeRegistry(typeRegistry)
            .outboxStore(outboxStore)
            .outboxEventMapper(TEST_MAPPER)
            .eventAuditStore(new PostgresEventAuditStore(faulting))
            .build();

    var streamId = TestStreams.stream("esint1-append-" + System.nanoTime());
    var env = envelope(streamId, new MappedEvent("boom"));

    assertThatThrownBy(() -> store.append(streamId, List.of(env), Version.initial()))
        .isInstanceOf(AssertionError.class);

    assertThat(eventRows(streamId))
        .as("event_stream must be rolled back, not partially committed")
        .isZero();
    assertThat(outboxRows(streamId))
        .as("outbox_events must be rolled back, not partially committed")
        .isZero();
  }

  // ── (b) appendWithKey rolls back events + outbox + the inbox claim ───────────────────

  @Test
  void appendWithKey_nonSqlThrowableMidTransaction_rollsBackEverythingIncludingInboxClaim()
      throws Exception {
    DataSource faulting =
        faultInjectingDataSource(dataSource, sql -> sql.contains("event_audit_log"));
    var outboxStore = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    var store =
        PostgresEventStore.builder()
            .dataSource(faulting)
            .typeRegistry(typeRegistry)
            .outboxStore(outboxStore)
            .outboxEventMapper(TEST_MAPPER)
            .eventAuditStore(new PostgresEventAuditStore(faulting))
            .commandInbox(new PostgresCommandInbox(faulting))
            .build();

    var streamId = TestStreams.stream("esint1-keyed-" + System.nanoTime());
    var key = IdempotencyKey.of("esint1-key-" + System.nanoTime());
    var env = envelope(streamId, new MappedEvent("boom"));

    assertThatThrownBy(
            () -> store.appendWithKey(streamId, List.of(env), Version.initial(), key, "TestCmd"))
        .isInstanceOf(AssertionError.class);

    assertThat(eventRows(streamId))
        .as("event_stream must be rolled back on the keyed path too")
        .isZero();
    assertThat(outboxRows(streamId))
        .as("outbox_events must be rolled back on the keyed path too")
        .isZero();
    assertThat(inboxRows(key))
        .as("command_inbox claim must be rolled back — a committed claim would wedge replay")
        .isZero();
  }

  // ── A failed rollback must not be committed by the autoCommit restore or the pool ─────────

  /**
   * On append: the audit INSERT fails and the rollback itself fails with a {@code SQLException} on
   * a connection that still works. The finally used to log that as benign and call {@code
   * setAutoCommit(true)}, which COMMITTED the events and outbox rows without their audit rows. The
   * connection is now aborted instead, nothing commits, and a rerun appends once.
   */
  @Test
  void append_aFailedRollbackOnALiveConnection_commitsNothing_andARerunConverges()
      throws Exception {
    DataSource faulting =
        faultInjectingDataSource(
            dataSource,
            sql -> sql.contains("event_audit_log"),
            new java.sql.SQLException("injected rollback failure"),
            false);
    var streamId = TestStreams.stream("c2-append-live-" + System.nanoTime());
    var env = envelope(streamId, new MappedEvent("boom"));

    assertThatThrownBy(() -> store(faulting).append(streamId, List.of(env), Version.initial()))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("event_audit_log");

    assertThat(eventRows(streamId)).as("the aborted append must commit no event").isZero();
    assertThat(outboxRows(streamId)).as("nor its outbox rows").isZero();

    store(dataSource).append(streamId, List.of(env), Version.initial());
    assertThat(eventRows(streamId)).isEqualTo(1);
    assertThat(outboxRows(streamId)).isEqualTo(1);
  }

  /**
   * The Agroal variant: an {@link Error} from the rollback used to skip the autoCommit restore and
   * propagate, and the pool's reset on return ({@code setAutoCommit(true)}, no rollback first)
   * COMMITTED the partial append. The Error still reaches the caller, after the abort.
   */
  @Test
  void append_aRollbackThatThrowsAnError_onAPoolThatResetsAutoCommitOnReturn_commitsNothing()
      throws Exception {
    DataSource faulting =
        faultInjectingDataSource(
            dataSource,
            sql -> sql.contains("event_audit_log"),
            new AssertionError("injected Error in the rollback"),
            true);
    var streamId = TestStreams.stream("c2-append-agroal-" + System.nanoTime());
    var env = envelope(streamId, new MappedEvent("boom"));

    assertThatThrownBy(() -> store(faulting).append(streamId, List.of(env), Version.initial()))
        .isInstanceOf(AssertionError.class)
        .hasMessage("injected Error in the rollback");

    assertThat(eventRows(streamId)).as("the pool's reset must not commit the append").isZero();
    assertThat(outboxRows(streamId)).isZero();
  }

  /**
   * On the keyed append, with a {@code RuntimeException} from the rollback under the Agroal return:
   * it used to escape the finally (only a {@code SQLException} was caught) and leave the pool's
   * reset to commit the events and the inbox claim. A committed claim would also have turned the
   * rerun into a fabricated "already applied". Now nothing commits, and the rerun appends.
   */
  @Test
  void appendWithKey_aFailedRollback_onAPoolThatResetsAutoCommitOnReturn_commitsNothing()
      throws Exception {
    DataSource faulting =
        faultInjectingDataSource(
            dataSource,
            sql -> sql.contains("event_audit_log"),
            new IllegalStateException("injected rollback failure"),
            true);
    var streamId = TestStreams.stream("c2-keyed-agroal-" + System.nanoTime());
    var key = IdempotencyKey.of("c2-key-" + System.nanoTime());
    var env = envelope(streamId, new MappedEvent("boom"));

    assertThatThrownBy(
            () ->
                store(faulting)
                    .appendWithKey(streamId, List.of(env), Version.initial(), key, "TestCmd"))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("event_audit_log");

    assertThat(eventRows(streamId)).isZero();
    assertThat(outboxRows(streamId)).isZero();
    assertThat(inboxRows(key)).as("the inbox claim must not commit").isZero();

    var rerun =
        store(dataSource).appendWithKey(streamId, List.of(env), Version.initial(), key, "TestCmd");
    assertThat(rerun.alreadyApplied()).isFalse();
    assertThat(eventRows(streamId)).isEqualTo(1);
    assertThat(inboxRows(key)).isEqualTo(1);
  }

  private static PostgresEventStore store(DataSource ds) {
    return PostgresEventStore.builder()
        .dataSource(ds)
        .typeRegistry(typeRegistry)
        .outboxStore(new PostgresOutboxStore(dataSource, Duration.ofMinutes(1)))
        .outboxEventMapper(TEST_MAPPER)
        .eventAuditStore(new PostgresEventAuditStore(ds))
        .commandInbox(new PostgresCommandInbox(ds))
        .build();
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  private static EventEnvelope envelope(StreamId streamId, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(1),
        new EventType("MappedEvent"),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("esint1-it"),
            null,
            null,
            Instant.now()));
  }

  private static int eventRows(StreamId streamId) throws Exception {
    return countBy(
        "SELECT COUNT(*) FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?",
        streamId.aggregateType().value(),
        streamId.aggregateId().value());
  }

  private static int outboxRows(StreamId streamId) throws Exception {
    return countBy(
        "SELECT COUNT(*) FROM outbox_events WHERE aggregate_type = ? AND aggregate_id = ?",
        streamId.aggregateType().value(),
        streamId.aggregateId().value());
  }

  private static int inboxRows(IdempotencyKey key) throws Exception {
    return countBy("SELECT COUNT(*) FROM command_inbox WHERE idempotency_key = ?", key.value());
  }

  private static int countBy(String sql, String... params) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setString(i + 1, params[i]);
      }
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  /**
   * Wraps {@code real} so every connection it hands out throws a plain {@link RuntimeException}
   * from {@code prepareStatement} when the SQL matches {@code failOnSql} — modelling an
   * OOM/AssertionError / RuntimeException escaping mid-transaction (here, at the audit INSERT). All
   * other calls delegate, so {@code commit}/{@code rollback}/{@code setAutoCommit} run against the
   * real connection.
   */
  private static DataSource faultInjectingDataSource(DataSource real, Predicate<String> failOnSql) {
    return faultInjectingDataSource(real, failOnSql, null, false);
  }

  /**
   * As {@link #faultInjectingDataSource(DataSource, Predicate)}; in addition, with a {@code
   * rollbackFailure}, {@code rollback()} throws it without rolling back, so the transaction stays
   * open. With {@code agroalReturn}, {@code close()} returns the connection the way Agroal 3.0 (the
   * Quarkus pool) does: a changed autoCommit is reset with {@code setAutoCommit(true)} and no
   * rollback first (a {@code SQLException} there is only a pool warning), then the real connection
   * is closed.
   */
  private static DataSource faultInjectingDataSource(
      DataSource real,
      Predicate<String> failOnSql,
      Throwable rollbackFailure,
      boolean agroalReturn) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if ("getConnection".equals(method.getName())) {
                Connection realConn = (Connection) invoke(method, real, args);
                return wrapConnection(realConn, failOnSql, rollbackFailure, agroalReturn);
              }
              return invoke(method, real, args);
            });
  }

  private static Connection wrapConnection(
      Connection real,
      Predicate<String> failOnSql,
      Throwable rollbackFailure,
      boolean agroalReturn) {
    boolean[] autoCommitDirty = {false};
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (rollbackFailure != null
                  && "rollback".equals(method.getName())
                  && (args == null || args.length == 0)) {
                throw rollbackFailure;
              }
              if (agroalReturn
                  && "setAutoCommit".equals(method.getName())
                  && real.getAutoCommit() != (Boolean) args[0]) {
                autoCommitDirty[0] = true;
              }
              if (agroalReturn && "close".equals(method.getName()) && autoCommitDirty[0]) {
                autoCommitDirty[0] = false;
                try {
                  real.setAutoCommit(true);
                } catch (java.sql.SQLException _) {
                  // Agroal logs a warning; a fatal SQLState makes it destroy the connection.
                }
              }
              if ("prepareStatement".equals(method.getName())
                  && args != null
                  && args.length > 0
                  && args[0] instanceof String sql
                  && failOnSql.test(sql)) {
                // An Error (models OutOfMemoryError/AssertionError escaping mid-transaction):
                // slips past the store's catch(Exception) wrapping AND append's catch(SQLException
                // | EventStoreException), so only the finally runs — exactly the window.
                throw new AssertionError("injected mid-transaction fault at: " + sql);
              }
              return invoke(method, real, args);
            });
  }

  private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
