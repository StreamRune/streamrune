package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.saga.SagaStore.CompensatingSaga;
import org.streamrune.core.saga.SagaStore.RowFacts;
import org.streamrune.core.saga.SagaTypeCollisionException;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.test.EventStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Characterization of the PostgreSQL saga store: the statements, connections, transactions,
 * statement timeouts and the saga's exclusive fence lock of every read and write, the exact
 * exception type, message, cause and suppressed exceptions of every refusal and failure, the
 * boundary between row decoding (a storage failure, with the connection open) and state
 * deserialization (a state failure, after the connection is returned), the create-conflict race,
 * the episode stamp rule, and the clock the timestamps come from. The values are what the store
 * does on a real database today, written down as they come out; these tests stay unchanged while
 * the store's decisions and read path move into shared helpers.
 *
 * <p>Every store borrows from a {@link RecordingDataSource}. Over a pool that hands out {@code
 * autoCommit=true} connections a read, an insert and a write that cannot change the forward fence's
 * verdict are one statement; an update carries the driver's millisecond row-write timeout. A write
 * that can take the saga out of its forward phase (landing {@code COMPENSATING} or a terminal
 * status, and every delete) runs in its own transaction that first pins READ COMMITTED, bounds its
 * idle time and takes the saga's exclusive advisory fence lock in one statement. Over a pool that
 * hands out {@code autoCommit=false} connections every single-statement write sets its idle bound
 * and commits. The store takes no clock: every timestamp is the database's {@code NOW()}.
 */
@Testcontainers
class PostgresSagaStoreCharacterizationTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_saga_store_characterization");

  private static final SagaType TYPE = SagaType.of("order-fulfillment");
  private static final SagaType OTHER = SagaType.of("refund");
  private static final SagaId S = SagaId.of("s");
  private static final SagaId N = SagaId.of("n");

  /** The instant {@code fixed_clock.now()} answers; see {@link #onTheFixedClock}. */
  private static final Instant FIXED = Instant.parse("2026-11-27T09:00:00.654321Z");

  // ── today's statements, as literals ─────────────────────────────────────────────────────────

  private static final String INSERT_NEW =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, genesis_applied,"
          + " dead_letter_pending, episode_version, episode_claimed_at, episode_cause_event_id)"
          + " VALUES (?, ?, ?, ?::jsonb, 1, TRUE, ?"
          + ", CASE WHEN ? THEN 1 END, CASE WHEN ? THEN NOW() END,"
          + " CASE WHEN ? THEN CAST(? AS TEXT) END)"
          + " ON CONFLICT (saga_id) DO NOTHING";
  private static final String INSERT_GENESIS_PENDING =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, genesis_applied,"
          + " dead_letter_pending, episode_version, episode_claimed_at, episode_cause_event_id)"
          + " VALUES (?, ?, ?, ?::jsonb, 1, FALSE, ?"
          + ", CASE WHEN ? THEN 1 END, CASE WHEN ? THEN NOW() END,"
          + " CASE WHEN ? THEN CAST(? AS TEXT) END)"
          + " ON CONFLICT (saga_id) DO NOTHING";
  private static final String INSERT_FAULTED =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, genesis_applied,"
          + " dead_letter_pending) VALUES (?, ?, 'FAULTED', ?::jsonb, 1, FALSE, TRUE)"
          + " ON CONFLICT (saga_id) DO NOTHING";
  private static final String SELECT_TYPE = "SELECT saga_type FROM saga_state WHERE saga_id = ?";
  private static final String EPISODE_STAMP_SET =
      "episode_version = CASE WHEN ? AND saga_state.episode_version IS NULL"
          + " THEN saga_state.version + 1 ELSE saga_state.episode_version END, "
          + "episode_claimed_at = CASE WHEN ? AND saga_state.episode_version IS NULL"
          + " THEN NOW() ELSE saga_state.episode_claimed_at END, "
          + "episode_cause_event_id = CASE WHEN ? AND saga_state.episode_version IS NULL"
          + " THEN CAST(? AS TEXT) ELSE saga_state.episode_cause_event_id END ";
  private static final String UPDATE_CAS =
      "UPDATE saga_state SET status = ?, state_json = ?::jsonb, "
          + "version = saga_state.version + 1, updated_at = NOW(), "
          + "pre_fault_status = NULL, "
          + EPISODE_STAMP_SET
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";
  private static final String APPLY_EVENT =
      "UPDATE saga_state SET status = ?, state_json = ?::jsonb, "
          + "version = saga_state.version + 1, updated_at = NOW(), "
          + "pre_fault_status = NULL, "
          + "genesis_applied = saga_state.genesis_applied OR ?, "
          + "last_applied_offset = GREATEST(COALESCE(saga_state.last_applied_offset, -1), ?), "
          + "last_replayed_offset = CASE WHEN ? THEN ? ELSE saga_state.last_replayed_offset END, "
          + EPISODE_STAMP_SET
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";
  private static final String MARK_FAULTED =
      "WITH prior AS ("
          + "SELECT saga_id, status AS prior_status FROM saga_state "
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')"
          + " FOR UPDATE) "
          + "UPDATE saga_state s SET "
          + "pre_fault_status = CASE WHEN prior.prior_status = 'FAULTED' THEN s.pre_fault_status"
          + " WHEN NOT s.genesis_applied AND prior.prior_status <> 'COMPENSATING' THEN NULL"
          + " ELSE prior.prior_status END, "
          + "status = 'FAULTED', version = s.version + 1, updated_at = NOW() "
          + "FROM prior WHERE s.saga_id = prior.saga_id "
          + "RETURNING prior.prior_status <> 'FAULTED' AS transitioned";
  private static final String SET_DEAD_LETTER_PENDING =
      "UPDATE saga_state SET dead_letter_pending = ? WHERE saga_id = ? AND saga_type = ?";
  private static final String DELETE_BY_TYPE =
      "DELETE FROM saga_state WHERE saga_id = ? AND saga_type = ?";
  private static final String SELECT_BY_TYPE =
      "SELECT state_json, status, version, updated_at, episode_version, episode_claimed_at, "
          + "episode_cause_event_id, genesis_applied, pre_fault_status, dead_letter_pending, "
          + "last_applied_offset, last_replayed_offset"
          + " FROM saga_state WHERE saga_id = ? AND saga_type = ?";
  private static final String SELECT_ROW_FACTS =
      "SELECT status, dead_letter_pending, last_applied_offset FROM saga_state"
          + " WHERE saga_id = ? AND saga_type = ?";
  private static final String SELECT_TIMED_OUT =
      "WITH forward_timeouts AS ("
          + "SELECT saga_id, created_at FROM saga_state "
          + "WHERE saga_type = ? AND status IN ('STARTED', 'RUNNING') AND created_at < ? "
          + "ORDER BY created_at ASC LIMIT ? FOR UPDATE SKIP LOCKED), "
          + "compensation_repicks AS ("
          + "SELECT saga_id, updated_at FROM saga_state "
          + "WHERE saga_type = ? AND status = 'COMPENSATING' AND updated_at < ? "
          + "ORDER BY updated_at ASC LIMIT ? FOR UPDATE SKIP LOCKED) "
          + "SELECT saga_id FROM ("
          + "SELECT saga_id, 0 AS population, "
          + "row_number() OVER (ORDER BY created_at ASC) AS slot FROM forward_timeouts "
          + "UNION ALL "
          + "SELECT saga_id, 1 AS population, "
          + "row_number() OVER (ORDER BY updated_at ASC) AS slot FROM compensation_repicks) batch "
          + "ORDER BY slot ASC, population ASC LIMIT ?";
  private static final String SELECT_BY_STATUS =
      "SELECT saga_id FROM saga_state WHERE saga_type = ? AND status = ? "
          + "ORDER BY updated_at DESC LIMIT ?";
  private static final String COUNT_BY_STATUS =
      "SELECT COUNT(*) FROM saga_state WHERE saga_type = ? AND status = ?";
  private static final String SELECT_COMPENSATING =
      "SELECT saga_id, updated_at, episode_claimed_at FROM saga_state "
          + "WHERE saga_type = ? AND status = 'COMPENSATING' AND updated_at < ? "
          + "ORDER BY updated_at ASC LIMIT ? "
          + "FOR UPDATE SKIP LOCKED";

  /** The fence lock's class key, {@code 0x53524746}. */
  private static final long FENCE_CLASS = 1397901126L;

  /** The fence lock's object key of saga {@code s} of {@link #TYPE}. */
  private static final long FENCE_S = -1736360927L;

  /** The idle bound a single-statement write sets on an {@code autoCommit=false} connection. */
  private static final String IDLE_BOUND = "SET LOCAL idle_in_transaction_session_timeout = 5000";

  /** The one statement that opens a fence-changing write's transaction, for saga {@code s}. */
  private static final String LOCK_S = lock(5000, FENCE_S);

  private static String lock(long idleMillis, long objectKey) {
    return "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; "
        + "SET LOCAL idle_in_transaction_session_timeout = "
        + idleMillis
        + "; SELECT pg_advisory_xact_lock("
        + FENCE_CLASS
        + ", "
        + objectKey
        + ")";
  }

  /** Called by {@link ProbeState}'s creator, i.e. while the store deserializes a state. */
  @FunctionalInterface
  interface Probe {
    void deserializing();
  }

  /**
   * A saga state whose JSON creator reports to the injected {@link Probe} and refuses the step
   * {@code "poison"}: valid JSON that does not bind.
   */
  static final class ProbeState implements SagaState {
    @JsonProperty("status")
    private final SagaStatus status;

    @JsonProperty("step")
    private final String step;

    ProbeState(SagaStatus status, String step) {
      this.status = status;
      this.step = step;
    }

    @JsonCreator
    static ProbeState of(
        @JsonProperty("status") SagaStatus status,
        @JsonProperty("step") String step,
        @JacksonInject("probe") Probe probe) {
      probe.deserializing();
      if ("poison".equals(step)) {
        throw new IllegalStateException("poison state");
      }
      return new ProbeState(status, step);
    }

    @Override
    public SagaStatus status() {
      return status;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof ProbeState other
          && status == other.status
          && Objects.equals(step, other.step);
    }

    @Override
    public int hashCode() {
      return Objects.hash(status, step);
    }

    @Override
    public String toString() {
      return "ProbeState[" + status + ", " + step + "]";
    }
  }

  /** A saga state whose JSON serialization always fails: its one property throws. */
  static final class Unserializable implements SagaState {
    @Override
    public SagaStatus status() {
      return SagaStatus.RUNNING;
    }

    public String getBoom() {
      throw new IllegalStateException("cannot serialize");
    }
  }

  /** A pool that hands out {@code autoCommit=false} connections, as a configured pool may. */
  static class ManualCommitDataSource extends PGSimpleDataSource {
    @Override
    public Connection getConnection() throws SQLException {
      Connection connection = super.getConnection();
      connection.setAutoCommit(false);
      return connection;
    }
  }

  /** An autoCommit=false pool whose connections' {@code rollback()} throws once armed. */
  static final class RollbackFailingDataSource extends ManualCommitDataSource {
    volatile boolean failRollback;

    @Override
    public Connection getConnection() throws SQLException {
      Connection real = super.getConnection();
      return (Connection)
          Proxy.newProxyInstance(
              RollbackFailingDataSource.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (failRollback
                    && method.getName().equals("rollback")
                    && (args == null || args.length == 0)) {
                  throw new SQLException("injected rollback failure");
                }
                try {
                  return method.invoke(real, args);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    }
  }

  private static final ProbeState RUNNING = new ProbeState(SagaStatus.RUNNING, "running");
  private static final ProbeState COMPENSATING =
      new ProbeState(SagaStatus.COMPENSATING, "compensating");
  private static final ProbeState COMPLETED = new ProbeState(SagaStatus.COMPLETED, "completed");

  static PGSimpleDataSource dataSource;
  static ManualCommitDataSource manualDataSource;

  /** The recorder's open connections at each state deserialization, in order. */
  private final List<Integer> openAtDeserialize = new CopyOnWriteArrayList<>();

  private RecordingDataSource recorder;
  private PostgresSagaStore store;

  @BeforeAll
  static void initSchema() throws SQLException {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    manualDataSource = new ManualCommitDataSource();
    manualDataSource.setUrl(PG.getJdbcUrl());
    manualDataSource.setUser(PG.getUsername());
    manualDataSource.setPassword(PG.getPassword());
    EventTypeRegistry registry = EventStoreContract.Setup.defaults().registry();
    new PostgresEventStoreFactory(dataSource, registry).create();
    // A now() that answers one fixed instant, found before pg_catalog's only by a session whose
    // search_path names its schema first (see onTheFixedClock).
    execute("CREATE SCHEMA fixed_clock");
    execute(
        "CREATE FUNCTION fixed_clock.now() RETURNS timestamptz LANGUAGE sql IMMUTABLE AS"
            + " $$ SELECT TIMESTAMPTZ '2026-11-27 09:00:00.654321+00' $$");
  }

  /** Every test starts from an empty table and a fresh recorder over an autoCommit=true pool. */
  @BeforeEach
  void emptyTable() throws SQLException {
    execute("DELETE FROM saga_state");
    recorder = new RecordingDataSource(dataSource);
    store = new PostgresSagaStore(recorder, mapper());
  }

  /** {@code createObjectMapper(null)}, with the probe {@link ProbeState}'s creator reports to. */
  private ObjectMapper mapper() {
    ObjectMapper mapper = PostgresSagaStore.createObjectMapper(null);
    Probe probe = () -> openAtDeserialize.add(recorder.openConnections());
    mapper.setInjectableValues(new InjectableValues.Std().addValue("probe", probe));
    return mapper;
  }

  /** Switches the test to a recorder over the autoCommit=false pool. */
  private void useManualCommitPool() {
    recorder = new RecordingDataSource(manualDataSource);
    store = new PostgresSagaStore(recorder, mapper());
  }

  // ── Golden transcripts ──────────────────────────────────────────────────────────────────────

  @Test
  void transcript_create() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + INSERT_NEW, "#1 close");

    // The conflict: the stored type is read after the insert, on the same connection.
    recorder.clear();
    Throwable conflict =
        catchThrowable(() -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false));

    assertThat(conflict).isExactlyInstanceOf(OptimisticLockException.class);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + INSERT_NEW, "#1 prepare " + SELECT_TYPE, "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void transcript_load() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.clear();

    assertThat(store.load(S, TYPE, ProbeState.class)).isPresent();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_BY_TYPE, "#1 close");

    recorder.clear();
    assertThat(store.load(SagaId.of("absent"), TYPE, ProbeState.class)).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_BY_TYPE, "#1 close");
  }

  @Test
  void transcript_loadRowFacts() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.clear();

    assertThat(store.loadRowFacts(S, TYPE)).isPresent();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_ROW_FACTS, "#1 close");
  }

  @Test
  void transcript_findCompensating() throws SQLException {
    store.create(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, false);
    recorder.clear();

    assertThat(store.findCompensating(TYPE, databaseNow().plusSeconds(1), 10)).hasSize(1);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_COMPENSATING, "#1 close");
  }

  @Test
  void transcript_otherReads() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    Instant later = databaseNow().plusSeconds(1);

    recorder.clear();
    assertThat(store.findTimedOut(TYPE, later, 10)).containsExactly(S);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_TIMED_OUT, "#1 close");

    recorder.clear();
    assertThat(store.findByStatus(TYPE, SagaStatus.RUNNING, 10)).containsExactly(S);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_BY_STATUS, "#1 close");

    recorder.clear();
    assertThat(store.countByStatus(TYPE, SagaStatus.RUNNING)).isEqualTo(1L);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + COUNT_BY_STATUS, "#1 close");
  }

  /**
   * Every write over an autoCommit=true pool: inserts are one unbounded statement, an update that
   * cannot change the fence's verdict is one bounded statement, and a fence-changing one (landing
   * COMPENSATING or terminal, or a delete) runs in a transaction behind the exclusive fence lock.
   */
  @Test
  void transcript_writes() {
    store.createGenesisPending(S, TYPE, RUNNING, SagaStatus.RUNNING, false, new EventId("e-1"));
    assertThat(recorder.transcript()).containsExactly(single(INSERT_GENESIS_PENDING, false));

    recorder.clear();
    store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1);
    assertThat(recorder.transcript()).containsExactly(single(UPDATE_CAS, true));

    recorder.clear();
    store.applyEvent(
        S, TYPE, RUNNING, SagaStatus.RUNNING, 2, AppliedEvent.liveStart(GlobalOffset.of(5)));
    assertThat(recorder.transcript()).containsExactly(single(APPLY_EVENT, true));

    recorder.clear();
    store.applyEvent(
        S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, 3, AppliedEvent.live(GlobalOffset.of(6)));
    assertThat(recorder.transcript()).containsExactly(fenced(LOCK_S, APPLY_EVENT));

    recorder.clear();
    assertThat(store.markFaulted(S, TYPE, 4)).isTrue();
    assertThat(recorder.transcript()).containsExactly(single(MARK_FAULTED, true));

    recorder.clear();
    store.claimCompensating(S, TYPE, COMPENSATING, 5, new EventId("e-2"));
    assertThat(recorder.transcript()).containsExactly(fenced(LOCK_S, UPDATE_CAS));

    recorder.clear();
    store.update(S, TYPE, COMPLETED, SagaStatus.COMPLETED, 6);
    assertThat(recorder.transcript()).containsExactly(fenced(LOCK_S, UPDATE_CAS));

    recorder.clear();
    store.setDeadLetterPending(S, TYPE, true);
    assertThat(recorder.transcript()).containsExactly(single(SET_DEAD_LETTER_PENDING, true));

    recorder.clear();
    store.delete(S, TYPE);
    assertThat(recorder.transcript()).containsExactly(fenced(LOCK_S, DELETE_BY_TYPE));

    // A delete of an absent row still takes the fence lock.
    recorder.clear();
    store.delete(S, TYPE);
    assertThat(recorder.transcript()).containsExactly(fenced(LOCK_S, DELETE_BY_TYPE));

    recorder.clear();
    store.createFaulted(S, TYPE, RUNNING);
    assertThat(recorder.transcript()).containsExactly(single(INSERT_FAULTED, false));
    assertThat(recorder.openConnections()).isZero();
  }

  /**
   * Every write over an autoCommit=false pool: a single-statement write sets its idle bound right
   * before its statement and commits; a fence-changing write is the same transaction as on an
   * autoCommit=true pool and restores autoCommit=false. A create conflict commits the insert and
   * then reads the stored type in a transaction that is never committed or rolled back.
   */
  @Test
  void transcript_writesOnAManualCommitPool() {
    useManualCommitPool();

    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    assertThat(recorder.transcript()).containsExactly(manualSingle(INSERT_NEW, false));

    recorder.clear();
    Throwable conflict =
        catchThrowable(() -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false));
    assertThat(conflict)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Saga s already exists");
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + INSERT_NEW,
            "#1 execute " + IDLE_BOUND,
            "#1 commit",
            "#1 prepare " + SELECT_TYPE,
            "#1 close");

    recorder.clear();
    store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1);
    assertThat(recorder.transcript()).containsExactly(manualSingle(UPDATE_CAS, true));

    recorder.clear();
    store.applyEvent(
        S, TYPE, RUNNING, SagaStatus.RUNNING, 2, AppliedEvent.live(GlobalOffset.of(5)));
    assertThat(recorder.transcript()).containsExactly(manualSingle(APPLY_EVENT, true));

    recorder.clear();
    assertThat(store.markFaulted(S, TYPE, 3)).isTrue();
    assertThat(recorder.transcript()).containsExactly(manualSingle(MARK_FAULTED, true));

    recorder.clear();
    store.setDeadLetterPending(S, TYPE, true);
    assertThat(recorder.transcript()).containsExactly(manualSingle(SET_DEAD_LETTER_PENDING, true));

    recorder.clear();
    store.claimCompensating(S, TYPE, COMPENSATING, 4, null);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 queryTimeout=driver",
            "#1 execute " + LOCK_S,
            "#1 prepare " + UPDATE_CAS,
            "#1 queryTimeout=driver",
            "#1 commit",
            "#1 autoCommit=false",
            "#1 close");

    recorder.clear();
    store.delete(S, TYPE);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 queryTimeout=driver",
            "#1 execute " + LOCK_S,
            "#1 prepare " + DELETE_BY_TYPE,
            "#1 queryTimeout=driver",
            "#1 commit",
            "#1 autoCommit=false",
            "#1 close");

    recorder.clear();
    store.createGenesisPending(S, TYPE, RUNNING, SagaStatus.RUNNING, false, null);
    assertThat(recorder.transcript()).containsExactly(manualSingle(INSERT_GENESIS_PENDING, false));

    recorder.clear();
    store.createFaulted(N, TYPE, RUNNING);
    assertThat(recorder.transcript()).containsExactly(manualSingle(INSERT_FAULTED, false));
    assertThat(recorder.openConnections()).isZero();
  }

  /**
   * The row-write bound is the store's: another bound changes the number in the lock statement and
   * in the idle bound; a disabled bound sets no query timeout and leaves the idle bound out.
   */
  @Test
  void transcript_rowWriteBound() {
    store = new PostgresSagaStore(recorder, mapper(), Duration.ofMillis(1500));
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.clear();
    store.claimCompensating(S, TYPE, COMPENSATING, 1, null);
    assertThat(recorder.transcript()).containsExactly(fenced(lock(1500, FENCE_S), UPDATE_CAS));

    useManualCommitPool();
    store = new PostgresSagaStore(recorder, mapper(), Duration.ofMillis(1500));
    store.setDeadLetterPending(S, TYPE, true);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + SET_DEAD_LETTER_PENDING,
            "#1 queryTimeout=driver",
            "#1 execute SET LOCAL idle_in_transaction_session_timeout = 1500",
            "#1 commit",
            "#1 close");

    // Disabled: no timeout, no idle bound.
    store = new PostgresSagaStore(recorder, mapper(), Duration.ZERO);
    recorder.clear();
    store.setDeadLetterPending(S, TYPE, false);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + SET_DEAD_LETTER_PENDING, "#1 commit", "#1 close");

    recorder = new RecordingDataSource(dataSource);
    store = new PostgresSagaStore(recorder, mapper(), Duration.ZERO);
    store.update(S, TYPE, COMPLETED, SagaStatus.COMPLETED, 2);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 execute SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"
                + " SELECT pg_advisory_xact_lock("
                + FENCE_CLASS
                + ", "
                + FENCE_S
                + ")",
            "#1 prepare " + UPDATE_CAS,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
  }

  /** One autocommit statement on connection #1, with the row-write timeout when bounded. */
  private static String[] single(String sql, boolean bounded) {
    List<String> events = new ArrayList<>();
    events.add("open #1");
    events.add("#1 prepare " + sql);
    if (bounded) {
      events.add("#1 queryTimeout=driver");
    }
    events.add("#1 close");
    return events.toArray(String[]::new);
  }

  /** One single-statement write on an autoCommit=false connection #1. */
  private static String[] manualSingle(String sql, boolean bounded) {
    List<String> events = new ArrayList<>();
    events.add("open #1");
    events.add("#1 prepare " + sql);
    if (bounded) {
      events.add("#1 queryTimeout=driver");
    }
    events.add("#1 execute " + IDLE_BOUND);
    events.add("#1 commit");
    events.add("#1 close");
    return events.toArray(String[]::new);
  }

  /** One fence-changing write's committed transaction on an autoCommit=true connection #1. */
  private static String[] fenced(String lock, String sql) {
    return new String[] {
      "open #1",
      "#1 autoCommit=false",
      "#1 queryTimeout=driver",
      "#1 execute " + lock,
      "#1 prepare " + sql,
      "#1 queryTimeout=driver",
      "#1 commit",
      "#1 autoCommit=true",
      "#1 close"
    };
  }

  // ── The fence lock and the row-write bound, on the database ─────────────────────────────────

  /**
   * The lock a fence-changing write takes is the saga's exclusive advisory lock under the fence's
   * class key: while a transaction holds the saga's shared lock (as a forward append does), the
   * write waits for it until its row-write bound cancels it, and rolls back. A write that cannot
   * change the verdict, and the same write for another saga, do not wait.
   */
  @Test
  @Timeout(60)
  void fenceLock_isTheSagasExclusiveAdvisoryLock() throws SQLException {
    store = new PostgresSagaStore(recorder, mapper(), Duration.ofMillis(300));
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    store.create(N, TYPE, RUNNING, SagaStatus.RUNNING, false);

    try (Connection holder = dataSource.getConnection()) {
      holder.setAutoCommit(false);
      try (Statement st = holder.createStatement()) {
        st.execute("SELECT pg_advisory_xact_lock_shared(" + FENCE_CLASS + ", " + FENCE_S + ")");
      }
      try {
        recorder.clear();
        Throwable claim =
            catchThrowable(() -> store.claimCompensating(S, TYPE, COMPENSATING, 1, null));

        assertThat(claim)
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessage("Failed to claim compensating saga: s");
        assertCanceled(claim);
        assertThat(recorder.transcript())
            .containsExactly(
                "open #1",
                "#1 autoCommit=false",
                "#1 queryTimeout=driver",
                "#1 execute " + lock(300, FENCE_S),
                "#1 rollback",
                "#1 autoCommit=true",
                "#1 close");

        Throwable delete = catchThrowable(() -> store.delete(S, TYPE));
        assertThat(delete)
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessage("Failed to delete saga state: s");
        assertCanceled(delete);

        // Not fence-changing: no lock, no wait.
        store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1);
        assertThat(store.markFaulted(S, TYPE, 2)).isTrue();
        // Another saga's lock.
        store.claimCompensating(N, TYPE, COMPENSATING, 1, null);
      } finally {
        holder.rollback();
      }
    }
    assertThat(store.load(S, TYPE, ProbeState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(store.load(N, TYPE, ProbeState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(recorder.openConnections()).isZero();
  }

  /**
   * An update waits for the row's lock at most the row-write bound, whether or not it is
   * fence-changing; an insert carries no bound of its own.
   */
  @Test
  @Timeout(60)
  void rowWriteBound_cancelsAWriteWaitingForTheRow() throws SQLException {
    store = new PostgresSagaStore(recorder, mapper(), Duration.ofMillis(300));
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    try (Connection holder = dataSource.getConnection()) {
      holder.setAutoCommit(false);
      try (Statement st = holder.createStatement()) {
        st.execute("SELECT 1 FROM saga_state WHERE saga_id = 's' FOR UPDATE");
      }
      try {
        recorder.clear();
        Throwable update =
            catchThrowable(() -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1));
        assertThat(update)
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessage("Failed to update saga state: s");
        assertCanceled(update);
        assertThat(recorder.transcript()).containsExactly(single(UPDATE_CAS, true));

        assertCanceled(
            catchThrowable(() -> store.update(S, TYPE, COMPLETED, SagaStatus.COMPLETED, 1)));
        assertCanceled(
            catchThrowable(
                () ->
                    store.applyEvent(
                        S,
                        TYPE,
                        RUNNING,
                        SagaStatus.RUNNING,
                        1,
                        AppliedEvent.live(GlobalOffset.of(1)))));
        assertCanceled(catchThrowable(() -> store.markFaulted(S, TYPE, 1)));
        assertCanceled(catchThrowable(() -> store.setDeadLetterPending(S, TYPE, true)));
      } finally {
        holder.rollback();
      }
    }
    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.deadLetterPending()).isFalse();
  }

  private static void assertCanceled(Throwable thrown) {
    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class);
    assertThat(thrown.getCause()).isExactlyInstanceOf(PSQLException.class);
    assertThat(((PSQLException) thrown.getCause()).getSQLState()).isEqualTo("57014");
    // The first line is the server's; the context lines after it name the tuple or lock waited
    // for, and the driver translates their label.
    assertThat(thrown.getCause().getMessage().lines().findFirst())
        .contains("ERROR: canceling statement due to user request");
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  // ── The create-conflict race ────────────────────────────────────────────────────────────────

  /**
   * The row that made the insert do nothing is deleted on the insert's own connection right before
   * the stored type is read: the create fails as "already exists", never succeeds silently, and
   * writes nothing.
   */
  @Test
  void createRace_conflictingRowVanishes_isOptimisticLock() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    List<String> transcriptAtHook = new ArrayList<>();
    List<Integer> deletedAtHook = new ArrayList<>();
    recorder.beforePrepare(
        SELECT_TYPE,
        sameConnection -> {
          transcriptAtHook.addAll(recorder.transcript());
          try (PreparedStatement ps =
              sameConnection.prepareStatement("DELETE FROM saga_state WHERE saga_id = ?")) {
            ps.setString(1, S.value());
            deletedAtHook.add(ps.executeUpdate());
          }
        });
    recorder.clear();

    Throwable thrown =
        catchThrowable(
            () ->
                store.create(
                    S,
                    TYPE,
                    new ProbeState(SagaStatus.STARTED, "second"),
                    SagaStatus.STARTED,
                    false));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Saga s already exists")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(transcriptAtHook).containsExactly("open #1", "#1 prepare " + INSERT_NEW);
    assertThat(deletedAtHook).containsExactly(1);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + INSERT_NEW, "#1 prepare " + SELECT_TYPE, "#1 close");
    assertThat(recorder.openConnections()).isZero();
    assertThat(store.load(S, TYPE, ProbeState.class)).isEmpty();
    assertThat(longValue("SELECT count(*) FROM saga_state")).isZero();
  }

  /**
   * On an autoCommit=false pool the conflict read runs in a transaction the store never commits, so
   * a delete made in it is not kept: the refusal is the same and the first row survives.
   */
  @Test
  void createRace_onAManualCommitPool_isOptimisticLock() throws SQLException {
    useManualCommitPool();
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.beforePrepare(
        SELECT_TYPE,
        sameConnection -> {
          try (Statement st = sameConnection.createStatement()) {
            st.executeUpdate("DELETE FROM saga_state WHERE saga_id = 's'");
          }
        });

    Throwable thrown =
        catchThrowable(() -> store.create(S, TYPE, RUNNING, SagaStatus.STARTED, false));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Saga s already exists")
        .hasNoCause();
    assertThat(store.load(S, TYPE, ProbeState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void createConflict_otherType_isTypeCollision() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.clear();

    Throwable thrown =
        catchThrowable(() -> store.create(S, OTHER, RUNNING, SagaStatus.RUNNING, false));

    assertThat(thrown)
        .isExactlyInstanceOf(SagaTypeCollisionException.class)
        .hasMessage(collisionMessage("order-fulfillment", "refund"))
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    SagaTypeCollisionException collision = (SagaTypeCollisionException) thrown;
    assertThat(collision.sagaId()).isEqualTo(S);
    assertThat(collision.existingType()).isEqualTo(TYPE);
    assertThat(collision.attemptedType()).isEqualTo(OTHER);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + INSERT_NEW, "#1 prepare " + SELECT_TYPE, "#1 close");

    // The same refusal from the other two inserts; nothing changed.
    assertThat(
            catchThrowable(
                () ->
                    store.createGenesisPending(
                        S, OTHER, RUNNING, SagaStatus.RUNNING, false, new EventId("e-1"))))
        .isExactlyInstanceOf(SagaTypeCollisionException.class)
        .hasMessage(collisionMessage("order-fulfillment", "refund"))
        .hasNoCause();
    assertThat(catchThrowable(() -> store.createFaulted(S, OTHER, RUNNING)))
        .isExactlyInstanceOf(SagaTypeCollisionException.class)
        .hasMessage(collisionMessage("order-fulfillment", "refund"))
        .hasNoCause();
    assertThat(store.load(S, TYPE, ProbeState.class).orElseThrow().state()).isEqualTo(RUNNING);
    assertThat(store.load(S, OTHER, ProbeState.class)).isEmpty();
  }

  @Test
  void createConflict_sameType_isOptimisticLock() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.clear();

    Throwable thrown =
        catchThrowable(() -> store.create(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, true));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Saga s already exists")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(
            catchThrowable(
                () ->
                    store.createGenesisPending(
                        S, TYPE, RUNNING, SagaStatus.RUNNING, false, new EventId("e-1"))))
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Saga s already exists")
        .hasNoCause();
    assertThat(catchThrowable(() -> store.createFaulted(S, TYPE, RUNNING)))
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Saga s already exists")
        .hasNoCause();
    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.state()).isEqualTo(RUNNING);
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.deadLetterPending()).isFalse();
    assertThat(recorder.openConnections()).isZero();
  }

  private static String collisionMessage(String existing, String attempted) {
    return "Saga id collision: 's' is already owned by saga type '"
        + existing
        + "', but saga type '"
        + attempted
        + "' tried to start/claim the same id. Two distinct saga types must not derive the same"
        + " SagaId — give each type a distinct id prefix (e.g. \"fulfillment-\" + orderId vs"
        + " \"refund-\" + orderId).";
  }

  // ── Stale views ─────────────────────────────────────────────────────────────────────────────

  @Test
  void staleView_message() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    Throwable thrown = catchThrowable(() -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 3));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Stale view of saga s (expectedVersion=3)")
        .hasNoCause();
  }

  /**
   * Every compare-and-swap that touches no row is a stale view, thrown after its connection is
   * returned; a fence-changing one commits its (empty) transaction first.
   */
  @Test
  void staleView_ofEveryWrite() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    String stale = "Stale view of saga s (expectedVersion=7)";

    assertStale(
        () -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 7),
        stale,
        single(UPDATE_CAS, true));
    assertStale(
        () -> store.update(S, TYPE, COMPLETED, SagaStatus.COMPLETED, 7),
        stale,
        fenced(LOCK_S, UPDATE_CAS));
    assertStale(
        () -> store.claimCompensating(S, TYPE, COMPENSATING, 7, new EventId("e-1")),
        stale,
        fenced(LOCK_S, UPDATE_CAS));
    assertStale(
        () ->
            store.applyEvent(
                S, TYPE, RUNNING, SagaStatus.RUNNING, 7, AppliedEvent.live(GlobalOffset.of(3))),
        stale,
        single(APPLY_EVENT, true));
    assertStale(
        () ->
            store.applyEvent(
                S,
                TYPE,
                COMPENSATING,
                SagaStatus.COMPENSATING,
                7,
                AppliedEvent.live(GlobalOffset.of(3))),
        stale,
        fenced(LOCK_S, APPLY_EVENT));
    assertStale(() -> store.markFaulted(S, TYPE, 7), stale, single(MARK_FAULTED, true));
    // A row of another type is a stale view to every compare-and-swap.
    assertStale(
        () -> store.update(S, OTHER, RUNNING, SagaStatus.RUNNING, 1),
        "Stale view of saga s (expectedVersion=1)",
        single(UPDATE_CAS, true));

    // None of them changed the row.
    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
  }

  private void assertStale(ThrowingCallable call, String message, String... transcript) {
    recorder.clear();
    Throwable thrown = catchThrowable(call);
    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage(message)
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly(transcript);
    assertThat(recorder.openConnections()).isZero();
  }

  /** On an autoCommit=false pool markFaulted's stale view is thrown inside, and rolled back. */
  @Test
  void staleView_markFaultedOnAManualCommitPool_rollsBack() {
    useManualCommitPool();
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.clear();

    Throwable thrown = catchThrowable(() -> store.markFaulted(S, TYPE, 7));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Stale view of saga s (expectedVersion=7)")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + MARK_FAULTED,
            "#1 queryTimeout=driver",
            "#1 execute " + IDLE_BOUND,
            "#1 rollback",
            "#1 close");
  }

  @Test
  void staleView_terminalRow() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    store.update(S, TYPE, COMPLETED, SagaStatus.COMPLETED, 1);

    Throwable thrown = catchThrowable(() -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 2));
    Throwable fault = catchThrowable(() -> store.markFaulted(S, TYPE, 2));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Stale view of saga s (expectedVersion=2)");
    assertThat(fault)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Stale view of saga s (expectedVersion=2)");
  }

  @Test
  void sagaIdsInMessagesAreSanitized() {
    SagaId unsafe = SagaId.of("a\nb\u001bc");
    store.create(unsafe, TYPE, RUNNING, SagaStatus.RUNNING, false);

    Throwable stale =
        catchThrowable(() -> store.update(unsafe, TYPE, RUNNING, SagaStatus.RUNNING, 9));
    Throwable exists =
        catchThrowable(() -> store.create(unsafe, TYPE, RUNNING, SagaStatus.RUNNING, false));
    recorder.beforePrepare(
        SELECT_BY_TYPE,
        c -> {
          throw new SQLException("injected");
        });
    Throwable load = catchThrowable(() -> store.load(unsafe, TYPE, ProbeState.class));

    assertThat(stale).hasMessage("Stale view of saga abc (expectedVersion=9)");
    assertThat(exists).hasMessage("Saga abc already exists");
    assertThat(load).hasMessage("Failed to load saga state: abc");
  }

  // ── Row decoding versus state deserialization ───────────────────────────────────────────────

  /**
   * A stored status no {@link SagaStatus} names fails while the row is decoded, with the connection
   * open: a storage failure, and the state is never deserialized.
   */
  @Test
  void corruptStatus_isEventStoreException() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    execute("UPDATE saga_state SET status = 'BOGUS' WHERE saga_id = 's'");
    recorder.clear();
    openAtDeserialize.clear();

    Throwable load = catchThrowable(() -> store.load(S, TYPE, ProbeState.class));

    assertThat(load)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to load saga state: s");
    assertUnknownStatusCause(load, "BOGUS");
    assertThat(openAtDeserialize).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_BY_TYPE, "#1 close");

    recorder.clear();
    Throwable facts = catchThrowable(() -> store.loadRowFacts(S, TYPE));
    assertThat(facts)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to load saga row facts: s");
    assertUnknownStatusCause(facts, "BOGUS");
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_ROW_FACTS, "#1 close");

    // A row with both a corrupt status and corrupt JSON fails as the status, before the JSON.
    execute("UPDATE saga_state SET state_json = '{\"status\":\"RUNNING\",\"step\":\"poison\"}'");
    Throwable both = catchThrowable(() -> store.load(S, TYPE, ProbeState.class));
    assertThat(both)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to load saga state: s");
    assertUnknownStatusCause(both, "BOGUS");
    assertThat(openAtDeserialize).isEmpty();
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void corruptPreFaultStatus_isEventStoreException() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    store.markFaulted(S, TYPE, 1);
    execute("UPDATE saga_state SET pre_fault_status = 'WEIRD' WHERE saga_id = 's'");
    openAtDeserialize.clear();

    Throwable load = catchThrowable(() -> store.load(S, TYPE, ProbeState.class));

    assertThat(load)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to load saga state: s");
    assertUnknownStatusCause(load, "WEIRD");
    assertThat(openAtDeserialize).isEmpty();
    // loadRowFacts does not read pre_fault_status.
    assertThat(store.loadRowFacts(S, TYPE)).contains(new RowFacts(SagaStatus.FAULTED, false, null));
  }

  private static void assertUnknownStatusCause(Throwable thrown, String stored) {
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("No enum constant org.streamrune.core.saga.SagaStatus." + stored)
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  /**
   * Valid JSON that does not bind fails while the state is deserialized, after the connection was
   * returned: a {@link SagaStateSerializationException}, not a storage failure.
   */
  @Test
  void corruptJson_isSagaStateSerializationException() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    execute("UPDATE saga_state SET state_json = '{\"status\":\"RUNNING\",\"step\":\"poison\"}'");
    recorder.clear();
    openAtDeserialize.clear();

    Throwable load = catchThrowable(() -> store.load(S, TYPE, ProbeState.class));

    assertThat(load)
        .isExactlyInstanceOf(SagaStateSerializationException.class)
        .hasMessage("Failed to deserialize saga state: s");
    assertThat(((SagaStateSerializationException) load).sagaId()).isEqualTo(S);
    assertThat(load.getSuppressed()).isEmpty();
    assertThat(load.getCause()).isExactlyInstanceOf(ValueInstantiationException.class);
    assertThat(load.getCause().getCause())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("poison state");
    // The creator ran once, with no connection open: the row was read and the connection closed.
    assertThat(openAtDeserialize).containsExactly(0);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_BY_TYPE, "#1 close");

    // loadRowFacts never reads the state.
    openAtDeserialize.clear();
    assertThat(store.loadRowFacts(S, TYPE)).contains(new RowFacts(SagaStatus.RUNNING, false, null));
    assertThat(openAtDeserialize).isEmpty();

    // An unknown property fails as the same exception type, also after the connection closed.
    execute(
        "UPDATE saga_state SET state_json = '{\"status\":\"RUNNING\",\"step\":\"x\",\"extra\":1}'");
    Throwable unknown = catchThrowable(() -> store.load(S, TYPE, ProbeState.class));
    assertThat(unknown)
        .isExactlyInstanceOf(SagaStateSerializationException.class)
        .hasMessage("Failed to deserialize saga state: s");
    assertThat(unknown.getCause()).isExactlyInstanceOf(UnrecognizedPropertyException.class);
    assertThat(openAtDeserialize).containsExactly(0);
  }

  @Test
  void loadDeserializesWithNoConnectionOpen() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    openAtDeserialize.clear();

    assertThat(store.load(S, TYPE, ProbeState.class).orElseThrow().state()).isEqualTo(RUNNING);
    assertThat(openAtDeserialize).containsExactly(0);
  }

  /**
   * findCompensating decodes the saga id and the two timestamps, never the status or the state: a
   * corrupt saga id is a storage failure, a corrupt status drops the row from the scan, corrupt
   * JSON is not read.
   */
  @Test
  void findCompensating_corruptRows() throws SQLException {
    store.create(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, false);
    Instant before = databaseNow().plusSeconds(1);
    Instant written = instantValue("SELECT updated_at FROM saga_state");

    execute("UPDATE saga_state SET state_json = '{\"status\":\"RUNNING\",\"step\":\"poison\"}'");
    openAtDeserialize.clear();
    assertThat(store.findCompensating(TYPE, before, 10))
        .containsExactly(new CompensatingSaga(S, written, written));
    assertThat(openAtDeserialize).isEmpty();

    execute("UPDATE saga_state SET saga_id = ' ' WHERE saga_id = 's'");
    recorder.clear();
    Throwable thrown = catchThrowable(() -> store.findCompensating(TYPE, before, 10));
    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find compensating sagas");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("SagaId value must not be null or blank");
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_COMPENSATING, "#1 close");

    execute("UPDATE saga_state SET saga_id = 's', status = 'BOGUS'");
    assertThat(store.findCompensating(TYPE, before, 10)).isEmpty();
  }

  // ── Argument refusals and the negative limit ────────────────────────────────────────────────

  /**
   * A negative limit is not refused by the store: the statement runs and the database refuses it,
   * which surfaces as the read's storage failure.
   */
  @Test
  void negativeLimit_isASqlError() throws SQLException {
    Instant now = databaseNow();

    assertNegativeLimit(
        () -> store.findCompensating(TYPE, now, -1),
        "Failed to find compensating sagas",
        SELECT_COMPENSATING);
    assertNegativeLimit(
        () -> store.findTimedOut(TYPE, now, -1),
        "Failed to find timed-out sagas",
        SELECT_TIMED_OUT);
    assertNegativeLimit(
        () -> store.findByStatus(TYPE, SagaStatus.RUNNING, -1),
        "Failed to find sagas by status",
        SELECT_BY_STATUS);
  }

  private void assertNegativeLimit(ThrowingCallable call, String message, String sql) {
    recorder.clear();
    Throwable thrown = catchThrowable(call);
    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(PSQLException.class)
        .hasMessage("ERROR: LIMIT must not be negative");
    assertThat(((PSQLException) thrown.getCause()).getSQLState()).isEqualTo("2201W");
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly("open #1", "#1 prepare " + sql, "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void argumentRefusals() {
    assertRefused(() -> new PostgresSagaStore(null, mapper()), "dataSource is required");
    assertRefused(() -> new PostgresSagaStore(recorder, null), "objectMapper is required");
    assertRefused(
        () -> new PostgresSagaStore(recorder, mapper(), Duration.ofSeconds(-1)),
        "statementTimeout must be a non-negative duration (Duration.ZERO disables it)");
    assertRefused(
        () -> store.create(null, TYPE, RUNNING, SagaStatus.RUNNING, false),
        "sagaId must not be null");
    assertRefused(
        () -> store.create(S, null, RUNNING, SagaStatus.RUNNING, false),
        "sagaType must not be null");
    assertRefused(
        () -> store.create(S, TYPE, null, SagaStatus.RUNNING, false), "state must not be null");
    assertRefused(() -> store.create(S, TYPE, RUNNING, null, false), "status must not be null");
    assertRefused(
        () -> store.createGenesisPending(S, TYPE, RUNNING, null, false, null),
        "status must not be null");
    assertRefused(() -> store.createFaulted(S, TYPE, null), "state must not be null");
    assertRefused(() -> store.update(S, TYPE, RUNNING, null, 1), "status must not be null");
    assertRefused(() -> store.claimCompensating(S, TYPE, null, 1, null), "state must not be null");
    assertRefused(
        () -> store.applyEvent(S, TYPE, RUNNING, SagaStatus.RUNNING, 1, null),
        "applied must not be null");
    assertRefused(() -> store.markFaulted(null, TYPE, 1), "sagaId must not be null");
    assertRefused(() -> store.setDeadLetterPending(S, null, true), "sagaType must not be null");
    assertRefused(() -> store.delete(null, TYPE), "sagaId must not be null");
    assertRefused(() -> store.load(S, TYPE, null), "stateType must not be null");
    assertRefused(() -> store.loadRowFacts(S, null), "sagaType must not be null");
    assertRefused(() -> store.findTimedOut(TYPE, null, 1), "cutoff must not be null");
    assertRefused(() -> store.findCompensating(TYPE, null, 1), "updatedBefore must not be null");
    assertRefused(() -> store.findByStatus(TYPE, null, 1), "status must not be null");
    assertRefused(() -> store.countByStatus(null, SagaStatus.RUNNING), "sagaType must not be null");

    Throwable metrics = catchThrowable(() -> PostgresSagaStore.createObjectMapper(null, null));
    assertThat(metrics)
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("metrics is required");
  }

  private void assertRefused(ThrowingCallable call, String message) {
    recorder.clear();
    Throwable thrown = catchThrowable(call);
    assertThat(thrown)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message)
        .hasNoCause();
    assertThat(recorder.connectionsOpened()).isZero();
  }

  /** The state is converted before a connection is borrowed; a failure is a state failure. */
  @Test
  void serializationFailure_isSagaStateSerializationExceptionBeforeAnyConnection() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    Unserializable bad = new Unserializable();

    for (ThrowingCallable call :
        List.<ThrowingCallable>of(
            () -> store.create(N, TYPE, bad, SagaStatus.RUNNING, false),
            () -> store.createGenesisPending(N, TYPE, bad, SagaStatus.RUNNING, false, null),
            () -> store.createFaulted(N, TYPE, bad),
            () -> store.update(N, TYPE, bad, SagaStatus.RUNNING, 1),
            () -> store.claimCompensating(N, TYPE, bad, 1, null),
            () ->
                store.applyEvent(
                    N, TYPE, bad, SagaStatus.RUNNING, 1, AppliedEvent.live(GlobalOffset.of(1))))) {
      recorder.clear();
      Throwable thrown = catchThrowable(call);
      assertThat(thrown)
          .isExactlyInstanceOf(SagaStateSerializationException.class)
          .hasMessage("Failed to serialize saga state: n");
      assertThat(((SagaStateSerializationException) thrown).sagaId()).isEqualTo(N);
      assertThat(thrown.getCause()).isExactlyInstanceOf(JsonMappingException.class);
      assertThat(recorder.connectionsOpened()).isZero();
    }
  }

  // ── Storage failures ────────────────────────────────────────────────────────────────────────

  /**
   * A statement of a write that fails surfaces as an {@link EventStoreException} naming the write,
   * whose cause is the driver's exception itself; a fence-changing write rolls its transaction back
   * and restores autoCommit.
   */
  @Test
  void writeFailureMessages() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    assertWriteFails(
        INSERT_NEW,
        () -> store.create(N, TYPE, RUNNING, SagaStatus.RUNNING, false),
        "Failed to create saga state: n",
        "open #1",
        "#1 close");
    assertWriteFails(
        INSERT_GENESIS_PENDING,
        () -> store.createGenesisPending(N, TYPE, RUNNING, SagaStatus.RUNNING, false, null),
        "Failed to create saga state: n",
        "open #1",
        "#1 close");
    assertWriteFails(
        INSERT_FAULTED,
        () -> store.createFaulted(N, TYPE, RUNNING),
        "Failed to create saga state: n",
        "open #1",
        "#1 close");
    assertWriteFails(
        SELECT_TYPE,
        () -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false),
        "Failed to create saga state: s",
        "open #1",
        "#1 prepare " + INSERT_NEW,
        "#1 close");
    assertWriteFails(
        UPDATE_CAS,
        () -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1),
        "Failed to update saga state: s",
        "open #1",
        "#1 close");
    assertWriteFails(
        UPDATE_CAS,
        () -> store.update(S, TYPE, COMPLETED, SagaStatus.COMPLETED, 1),
        "Failed to update saga state: s",
        fencedRolledBack());
    assertWriteFails(
        UPDATE_CAS,
        () -> store.claimCompensating(S, TYPE, COMPENSATING, 1, null),
        "Failed to claim compensating saga: s",
        fencedRolledBack());
    assertWriteFails(
        APPLY_EVENT,
        () ->
            store.applyEvent(
                S, TYPE, RUNNING, SagaStatus.RUNNING, 1, AppliedEvent.live(GlobalOffset.of(1))),
        "Failed to apply event to saga state: s",
        "open #1",
        "#1 close");
    assertWriteFails(
        APPLY_EVENT,
        () ->
            store.applyEvent(
                S,
                TYPE,
                COMPENSATING,
                SagaStatus.COMPENSATING,
                1,
                AppliedEvent.live(GlobalOffset.of(1))),
        "Failed to apply event to saga state: s",
        fencedRolledBack());
    assertWriteFails(
        MARK_FAULTED,
        () -> store.markFaulted(S, TYPE, 1),
        "Failed to mark saga FAULTED: s",
        "open #1",
        "#1 close");
    assertWriteFails(
        SET_DEAD_LETTER_PENDING,
        () -> store.setDeadLetterPending(S, TYPE, true),
        "Failed to set saga dead-letter shield: s",
        "open #1",
        "#1 close");
    assertWriteFails(
        DELETE_BY_TYPE,
        () -> store.delete(S, TYPE),
        "Failed to delete saga state: s",
        fencedRolledBack());

    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.deadLetterPending()).isFalse();
    assertThat(store.load(N, TYPE, ProbeState.class)).isEmpty();
  }

  private static String[] fencedRolledBack() {
    return new String[] {
      "open #1",
      "#1 autoCommit=false",
      "#1 queryTimeout=driver",
      "#1 execute " + LOCK_S,
      "#1 rollback",
      "#1 autoCommit=true",
      "#1 close"
    };
  }

  private void assertWriteFails(
      String sql, ThrowingCallable call, String message, String... transcript) {
    SQLException injected = new SQLException("injected");
    recorder.beforePrepare(
        sql,
        c -> {
          throw injected;
        });
    recorder.clear();

    Throwable thrown = catchThrowable(call);

    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isSameAs(injected);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly(transcript);
    assertThat(recorder.openConnections()).isZero();
  }

  /** On an autoCommit=false pool a single-statement write whose statement fails rolls back. */
  @Test
  void writeFailureOnAManualCommitPool_rollsBack() {
    useManualCommitPool();
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.beforePrepare(
        SET_DEAD_LETTER_PENDING, PostgresSagaStoreCharacterizationTest::noTables);
    recorder.clear();

    Throwable thrown = catchThrowable(() -> store.setDeadLetterPending(S, TYPE, true));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to set saga dead-letter shield: s");
    assertNoSuchTable(thrown);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + SET_DEAD_LETTER_PENDING,
            "#1 queryTimeout=driver",
            "#1 execute " + IDLE_BOUND,
            "#1 rollback",
            "#1 close");
    assertThat(store.loadRowFacts(S, TYPE)).contains(new RowFacts(SagaStatus.RUNNING, false, null));
  }

  /** A session setting, made on the real connection, that makes every table lookup fail. */
  private static void noTables(Connection sameConnection) throws SQLException {
    try (Statement st = sameConnection.createStatement()) {
      st.execute("SET search_path TO no_such_schema");
    }
  }

  private static void assertNoSuchTable(Throwable thrown) {
    assertThat(thrown.getCause()).isExactlyInstanceOf(PSQLException.class);
    assertThat(((PSQLException) thrown.getCause()).getSQLState()).isEqualTo("42P01");
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  @Test
  void readFailureMessages() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    Instant now = databaseNow();

    assertReadFails(
        SELECT_BY_TYPE,
        () -> store.load(S, TYPE, ProbeState.class),
        "Failed to load saga state: s");
    assertReadFails(
        SELECT_ROW_FACTS, () -> store.loadRowFacts(S, TYPE), "Failed to load saga row facts: s");
    assertReadFails(
        SELECT_TIMED_OUT, () -> store.findTimedOut(TYPE, now, 1), "Failed to find timed-out sagas");
    assertReadFails(
        SELECT_BY_STATUS,
        () -> store.findByStatus(TYPE, SagaStatus.RUNNING, 1),
        "Failed to find sagas by status");
    assertReadFails(
        COUNT_BY_STATUS,
        () -> store.countByStatus(TYPE, SagaStatus.RUNNING),
        "Failed to count sagas by status");
    assertReadFails(
        SELECT_COMPENSATING,
        () -> store.findCompensating(TYPE, now, 1),
        "Failed to find compensating sagas");
  }

  private void assertReadFails(String sql, ThrowingCallable call, String message) {
    SQLException injected = new SQLException("injected");
    recorder.beforePrepare(
        sql,
        c -> {
          throw injected;
        });
    recorder.clear();

    Throwable thrown = catchThrowable(call);

    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isSameAs(injected);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly("open #1", "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  /**
   * A transaction whose rollback fails is aborted, and the shared transaction helper logs one WARN
   * naming the write; the store itself logs nothing.
   */
  @Test
  void rollbackFailure_logsTheWriteAndAbortsTheConnection() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    try (LogCapture storeLog = LogCapture.attach(PostgresSagaStore.class);
        LogCapture txLog = LogCapture.attach(PostgresTransactions.class)) {

      assertRollbackFails(
          UPDATE_CAS,
          () -> store.claimCompensating(S, TYPE, COMPENSATING, 1, null),
          "Failed to claim compensating saga: s");
      assertRollbackFails(
          APPLY_EVENT,
          () ->
              store.applyEvent(
                  S,
                  TYPE,
                  COMPLETED,
                  SagaStatus.COMPLETED,
                  1,
                  AppliedEvent.live(GlobalOffset.of(1))),
          "Failed to apply event to saga state: s");
      assertRollbackFails(
          DELETE_BY_TYPE, () -> store.delete(S, TYPE), "Failed to delete saga state: s");

      // The rollback fails on the closed connection, and so does the abort after it.
      assertThat(txLog.events(Level.WARN).stream().map(ILoggingEvent::getFormattedMessage))
          .containsExactly(
              rollbackWarning("saga update"),
              ABORT_WARNING,
              rollbackWarning("saga apply event"),
              ABORT_WARNING,
              rollbackWarning("saga delete"),
              ABORT_WARNING);
      assertThat(storeLog.events(Level.TRACE)).isEmpty();
      assertThat(storeLog.events(Level.DEBUG)).isEmpty();
      assertThat(storeLog.events(Level.INFO)).isEmpty();
      assertThat(storeLog.events(Level.WARN)).isEmpty();
      assertThat(storeLog.events(Level.ERROR)).isEmpty();
    }
  }

  private void assertRollbackFails(String sql, ThrowingCallable call, String message) {
    recorder.beforePrepare(sql, Connection::close);
    recorder.clear();

    Throwable thrown = catchThrowable(call);

    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(PSQLException.class)
        .hasMessage("This connection has been closed.");
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 queryTimeout=driver",
            "#1 execute " + LOCK_S,
            "#1 prepare " + sql,
            "#1 rollback",
            "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  private static final String ABORT_WARNING =
      "Failed to abort a connection whose rollback failed; its pool may commit the open"
          + " transaction: org.postgresql.util.PSQLException: This connection has been closed.";

  private static String rollbackWarning(String transaction) {
    return "Rollback of an uncommitted "
        + transaction
        + " transaction failed; aborting the connection so that neither an autoCommit restore nor"
        + " the pool's reset can commit it: org.postgresql.util.PSQLException: This connection has"
        + " been closed.";
  }

  /**
   * The single-statement writes on an autoCommit=false pool name their own transactions in the WARN
   * of a failed rollback.
   */
  @Test
  void rollbackFailureOnAManualCommitPool_logsTheWrite() {
    RollbackFailingDataSource failing = new RollbackFailingDataSource();
    failing.setUrl(PG.getJdbcUrl());
    failing.setUser(PG.getUsername());
    failing.setPassword(PG.getPassword());
    recorder = new RecordingDataSource(failing);
    store = new PostgresSagaStore(recorder, mapper());
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    failing.failRollback = true;

    try (LogCapture txLog = LogCapture.attach(PostgresTransactions.class)) {
      recorder.beforePrepare(INSERT_NEW, PostgresSagaStoreCharacterizationTest::noTables);
      recorder.clear();
      Throwable create =
          catchThrowable(() -> store.create(N, TYPE, RUNNING, SagaStatus.RUNNING, false));
      assertThat(create)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to create saga state: n");
      assertNoSuchTable(create);
      assertThat(recorder.transcript())
          .containsExactly(
              "open #1",
              "#1 prepare " + INSERT_NEW,
              "#1 execute " + IDLE_BOUND,
              "#1 rollback",
              "#1 close");

      recorder.beforePrepare(UPDATE_CAS, PostgresSagaStoreCharacterizationTest::noTables);
      assertNoSuchTable(
          catchThrowable(() -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1)));
      recorder.beforePrepare(APPLY_EVENT, PostgresSagaStoreCharacterizationTest::noTables);
      assertNoSuchTable(
          catchThrowable(
              () ->
                  store.applyEvent(
                      S,
                      TYPE,
                      RUNNING,
                      SagaStatus.RUNNING,
                      1,
                      AppliedEvent.live(GlobalOffset.of(1)))));
      recorder.beforePrepare(MARK_FAULTED, PostgresSagaStoreCharacterizationTest::noTables);
      assertNoSuchTable(catchThrowable(() -> store.markFaulted(S, TYPE, 1)));
      recorder.beforePrepare(
          SET_DEAD_LETTER_PENDING, PostgresSagaStoreCharacterizationTest::noTables);
      assertNoSuchTable(catchThrowable(() -> store.setDeadLetterPending(S, TYPE, true)));

      assertThat(txLog.events(Level.WARN).stream().map(ILoggingEvent::getFormattedMessage))
          .containsExactly(
              injectedRollbackWarning("saga create"),
              injectedRollbackWarning("saga update"),
              injectedRollbackWarning("saga apply event"),
              injectedRollbackWarning("saga mark faulted"),
              injectedRollbackWarning("saga dead-letter shield"));
    }
    assertThat(recorder.openConnections()).isZero();
  }

  private static String injectedRollbackWarning(String transaction) {
    return "Rollback of an uncommitted "
        + transaction
        + " transaction failed; aborting the connection so that neither an autoCommit restore nor"
        + " the pool's reset can commit it: java.sql.SQLException: injected rollback failure";
  }

  /**
   * A fence-changing write whose connection fails to close after its commit surfaces as a storage
   * failure, though the row was written; a stale view in that position is masked by it. An "already
   * exists" refusal on an autoCommit=false pool keeps its type and carries the close failure as
   * suppressed.
   */
  @Test
  void closeFailureAfterCommit() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.failNextCloseAfterCommit();
    recorder.clear();

    Throwable thrown =
        catchThrowable(() -> store.claimCompensating(S, TYPE, COMPENSATING, 1, null));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to claim compensating saga: s");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly(fenced(LOCK_S, UPDATE_CAS));
    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.version()).isEqualTo(2L);
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);

    // A stale view whose close fails after its commit is that storage failure, not the stale view.
    recorder.failNextCloseAfterCommit();
    recorder.clear();
    Throwable stale =
        catchThrowable(() -> store.update(S, TYPE, COMPLETED, SagaStatus.COMPLETED, 5));
    assertThat(stale)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to update saga state: s");
    assertThat(stale.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(stale.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly(fenced(LOCK_S, UPDATE_CAS));

    // Single statements on an autoCommit=true pool never commit, so the arm stays for the next.
    recorder.failNextCloseAfterCommit();
    store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 2);
    Throwable delete = catchThrowable(() -> store.delete(S, TYPE));
    assertThat(delete)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to delete saga state: s");
    assertThat(delete.getCause()).hasMessage("injected close failure");
    assertThat(store.load(S, TYPE, ProbeState.class)).isEmpty();
  }

  @Test
  void closeFailureAfterCommit_onAManualCommitPool() {
    useManualCommitPool();
    recorder.failNextCloseAfterCommit();

    Throwable created =
        catchThrowable(() -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false));

    assertThat(created)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to create saga state: s");
    assertThat(created.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(store.load(S, TYPE, ProbeState.class).orElseThrow().version()).isEqualTo(1L);

    recorder.failNextCloseAfterCommit();
    Throwable exists =
        catchThrowable(() -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false));

    assertThat(exists)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Saga s already exists")
        .hasNoCause();
    assertThat(exists.getSuppressed()).hasSize(1);
    assertThat(exists.getSuppressed()[0])
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
  }

  // ── Row assembly, the episode rule and timestamps ───────────────────────────────────────────

  @Test
  void loadAssemblesEveryColumn() throws SQLException {
    store.createGenesisPending(
        S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, true, new EventId("e-1"));
    store.applyEvent(
        S,
        TYPE,
        COMPENSATING,
        SagaStatus.COMPENSATING,
        1,
        new AppliedEvent(GlobalOffset.of(9), true, false, new EventId("e-2")));
    store.applyEvent(
        S,
        TYPE,
        COMPENSATING,
        SagaStatus.COMPENSATING,
        2,
        AppliedEvent.replayed(GlobalOffset.of(7)));
    assertThat(store.markFaulted(S, TYPE, 3)).isTrue();
    Instant updatedAt = instantValue("SELECT updated_at FROM saga_state WHERE saga_id = 's'");
    Instant claimedAt =
        instantValue("SELECT episode_claimed_at FROM saga_state WHERE saga_id = 's'");

    // A live offset is a running maximum, a replayed one is set exactly: 9 and 7.
    assertThat(store.load(S, TYPE, ProbeState.class))
        .contains(
            new LoadedSaga<>(
                COMPENSATING,
                SagaStatus.FAULTED,
                4L,
                updatedAt,
                1L,
                claimedAt,
                new EventId("e-1"),
                true,
                SagaStatus.COMPENSATING,
                true,
                9L,
                7L));
    assertThat(store.loadRowFacts(S, TYPE)).contains(new RowFacts(SagaStatus.FAULTED, true, 9L));

    // Genesis pending and shielded: the two flags apart.
    SagaId pending = SagaId.of("pending");
    store.createGenesisPending(pending, TYPE, RUNNING, SagaStatus.RUNNING, true, new EventId("e"));
    assertThat(store.load(pending, TYPE, ProbeState.class))
        .contains(
            new LoadedSaga<>(
                RUNNING,
                SagaStatus.RUNNING,
                1L,
                instantValue("SELECT updated_at FROM saga_state WHERE saga_id = 'pending'"),
                null,
                null,
                null,
                false,
                null,
                true,
                null,
                null));
    assertThat(store.loadRowFacts(pending, TYPE))
        .contains(new RowFacts(SagaStatus.RUNNING, true, null));

    SagaId plain = SagaId.of("plain");
    store.create(plain, TYPE, RUNNING, SagaStatus.RUNNING, false);
    assertThat(store.load(plain, TYPE, ProbeState.class))
        .contains(
            new LoadedSaga<>(
                RUNNING,
                SagaStatus.RUNNING,
                1L,
                instantValue("SELECT updated_at FROM saga_state WHERE saga_id = 'plain'"),
                null,
                null,
                null,
                true,
                null,
                false,
                null,
                null));
    assertThat(store.loadRowFacts(plain, TYPE))
        .contains(new RowFacts(SagaStatus.RUNNING, false, null));
    assertThat(store.loadRowFacts(SagaId.of("absent"), TYPE)).isEmpty();
    assertThat(store.load(plain, OTHER, ProbeState.class)).isEmpty();
  }

  /**
   * The episode rule on the update path: a write that lands COMPENSATING on an unstamped row stamps
   * {@code episode_version = version + 1} with its cause (none for update, the given one for a
   * claim, the event's id for applyEvent); a write landing anything else leaves the row unstamped;
   * a stamped row is never re-stamped, and fault and terminal writes keep the stamp.
   */
  @Test
  void episodeRule_updatePath() throws SQLException {
    // update landing COMPENSATING: stamped at the version it lands at, no cause.
    SagaId u = SagaId.of("u");
    store.create(u, TYPE, RUNNING, SagaStatus.RUNNING, false);
    store.update(u, TYPE, RUNNING, SagaStatus.RUNNING, 1);
    assertThat(episode(u)).containsExactly(null, null, null);
    store.update(u, TYPE, COMPENSATING, SagaStatus.COMPENSATING, 2);
    List<Object> stamped = episode(u);
    assertThat(stamped.get(0)).isEqualTo(3L);
    assertThat(stamped.get(1)).isNotNull();
    assertThat(stamped.get(2)).isNull();
    // A later claim, a fault and a terminal write keep it.
    store.claimCompensating(u, TYPE, COMPENSATING, 3, new EventId("e-late"));
    assertThat(episode(u)).isEqualTo(stamped);
    store.markFaulted(u, TYPE, 4);
    assertThat(episode(u)).isEqualTo(stamped);
    assertThat(store.load(u, TYPE, ProbeState.class).orElseThrow().preFaultStatus())
        .isEqualTo(SagaStatus.COMPENSATING);
    store.update(u, TYPE, COMPLETED, SagaStatus.COMPLETED, 5);
    assertThat(episode(u)).isEqualTo(stamped);

    // claimCompensating: stamped with the given cause.
    SagaId c = SagaId.of("c");
    store.create(c, TYPE, RUNNING, SagaStatus.RUNNING, false);
    store.applyEvent(
        c,
        TYPE,
        RUNNING,
        SagaStatus.RUNNING,
        1,
        AppliedEvent.live(GlobalOffset.of(4)).withEventId(new EventId("e-4")));
    assertThat(episode(c)).containsExactly(null, null, null);
    store.claimCompensating(c, TYPE, COMPENSATING, 2, new EventId("e-9"));
    assertThat(episode(c).get(0)).isEqualTo(3L);
    assertThat(episode(c).get(2)).isEqualTo("e-9");

    // applyEvent landing COMPENSATING: stamped with the applied event's id.
    SagaId a = SagaId.of("a");
    store.create(a, TYPE, RUNNING, SagaStatus.RUNNING, false);
    store.applyEvent(
        a,
        TYPE,
        COMPENSATING,
        SagaStatus.COMPENSATING,
        1,
        AppliedEvent.live(GlobalOffset.of(5)).withEventId(new EventId("e-5")));
    assertThat(episode(a).get(0)).isEqualTo(2L);
    assertThat(episode(a).get(2)).isEqualTo("e-5");
    LoadedSaga<ProbeState> loaded = store.load(a, TYPE, ProbeState.class).orElseThrow();
    assertThat(loaded.episodeVersion()).isEqualTo(2L);
    assertThat(loaded.episodeCauseEventId()).isEqualTo(new EventId("e-5"));

    // The insert path: born stamped at 1 only when born COMPENSATING; the cause only then.
    SagaId born = SagaId.of("born");
    store.createGenesisPending(
        born, TYPE, COMPENSATING, SagaStatus.COMPENSATING, false, new EventId("e-b"));
    assertThat(episode(born).get(0)).isEqualTo(1L);
    assertThat(episode(born).get(2)).isEqualTo("e-b");
    SagaId forward = SagaId.of("forward");
    store.createGenesisPending(
        forward, TYPE, RUNNING, SagaStatus.RUNNING, false, new EventId("e-f"));
    assertThat(episode(forward)).containsExactly(null, null, null);
    SagaId faulted = SagaId.of("faulted");
    store.createFaulted(faulted, TYPE, COMPENSATING);
    assertThat(episode(faulted)).containsExactly(null, null, null);
  }

  /** {@code episode_version}, {@code episode_claimed_at} and {@code episode_cause_event_id}. */
  private static List<Object> episode(SagaId sagaId) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement(
                "SELECT episode_version, episode_claimed_at, episode_cause_event_id"
                    + " FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, sagaId.value());
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        List<Object> values = new ArrayList<>();
        long version = rs.getLong(1);
        values.add(rs.wasNull() ? null : version);
        var claimed = rs.getTimestamp(2);
        values.add(claimed == null ? null : claimed.toInstant());
        values.add(rs.getString(3));
        return values;
      }
    }
  }

  /**
   * Every timestamp is the database's: a statement's {@code NOW()} (shown here by a session whose
   * {@code now()} answers a fixed instant) or a column default. The store takes no clock, so the
   * JVM's has no part in them.
   */
  @Test
  void timestampsComeFromTheDatabaseClock() throws SQLException {
    Instant before = databaseNow();
    onTheFixedClock(INSERT_NEW);
    store.create(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, false);
    Instant after = databaseNow();

    // The statement's NOW() is the session's; the column defaults are the server clock's.
    assertThat(instantValue("SELECT episode_claimed_at FROM saga_state")).isEqualTo(FIXED);
    Instant created = instantValue("SELECT created_at FROM saga_state");
    assertThat(created).isBetween(before, after);
    assertThat(instantValue("SELECT updated_at FROM saga_state")).isEqualTo(created);

    onTheFixedClock(UPDATE_CAS);
    store.update(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, 1);
    assertThat(instantValue("SELECT updated_at FROM saga_state")).isEqualTo(FIXED);
    assertThat(instantValue("SELECT created_at FROM saga_state")).isEqualTo(created);
    assertThat(store.load(S, TYPE, ProbeState.class).orElseThrow().updatedAt()).isEqualTo(FIXED);
    assertThat(store.findCompensating(TYPE, FIXED.plusNanos(1_000), 10))
        .containsExactly(new CompensatingSaga(S, FIXED, FIXED));
    assertThat(store.findCompensating(TYPE, FIXED, 10)).isEmpty();

    // A stamping update and markFaulted read the same clock.
    SagaId other = SagaId.of("other");
    store.create(other, TYPE, RUNNING, SagaStatus.RUNNING, false);
    onTheFixedClock(UPDATE_CAS);
    store.claimCompensating(other, TYPE, COMPENSATING, 1, null);
    assertThat(episode(other).get(1)).isEqualTo(FIXED);
    execute("UPDATE saga_state SET updated_at = created_at WHERE saga_id = 'other'");
    onTheFixedClock(MARK_FAULTED);
    store.markFaulted(other, TYPE, 2);
    assertThat(instantValue("SELECT updated_at FROM saga_state WHERE saga_id = 'other'"))
        .isEqualTo(FIXED);
    execute("UPDATE saga_state SET updated_at = created_at WHERE saga_id = 'other'");
    onTheFixedClock(APPLY_EVENT);
    store.applyEvent(
        other, TYPE, RUNNING, SagaStatus.RUNNING, 3, AppliedEvent.live(GlobalOffset.of(1)));
    assertThat(instantValue("SELECT updated_at FROM saga_state WHERE saga_id = 'other'"))
        .isEqualTo(FIXED);
  }

  /**
   * Makes the session of the store's next statement starting with {@code sql} resolve {@code now()}
   * to {@code fixed_clock.now()}, which answers {@link #FIXED}.
   */
  private void onTheFixedClock(String sql) {
    recorder.beforePrepare(
        sql,
        sameConnection -> {
          try (Statement st = sameConnection.createStatement()) {
            st.execute("SET search_path TO fixed_clock, pg_catalog, public");
          }
        });
  }

  // ── raw SQL on an unrecorded connection ─────────────────────────────────────────────────────

  private static void execute(String statement) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement()) {
      st.execute(statement);
    }
  }

  private static long longValue(String query) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      assertThat(rs.next()).isTrue();
      return rs.getLong(1);
    }
  }

  private static Instant instantValue(String query) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      assertThat(rs.next()).isTrue();
      return rs.getTimestamp(1).toInstant();
    }
  }

  /** The database server's clock right now. */
  private static Instant databaseNow() throws SQLException {
    return instantValue("SELECT clock_timestamp()");
  }
}
