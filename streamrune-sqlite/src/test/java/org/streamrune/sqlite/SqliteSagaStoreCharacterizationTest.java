package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStoreException;
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

/**
 * Characterization of the SQLite saga store: the statements and connections of every read and
 * write, the exact exception type, message and cause of every refusal and failure, where each
 * failure surfaces relative to the write transaction, the boundary between row decoding (a storage
 * failure) and state deserialization (a state failure, run after the connection is returned), the
 * create-conflict race, and the clock the timestamps come from. The values are what the store does
 * on a real database file today, written down as they come out; these tests stay unchanged while
 * the store's decisions and read path move into shared helpers.
 *
 * <p>Every store borrows from a {@link RecordingDataSource}: reads run in autocommit on one
 * connection, writes in one connection's {@code BEGIN IMMEDIATE} transaction committed with a
 * {@code COMMIT} statement. A domain refusal (stale view, type collision, "already exists") is
 * thrown after that transaction committed, so it keeps its own type.
 */
class SqliteSagaStoreCharacterizationTest {

  private static final SagaType TYPE = SagaType.of("order-fulfillment");
  private static final SagaType OTHER = SagaType.of("refund");
  private static final SagaId S = SagaId.of("s");
  private static final Instant NOW = Instant.parse("2026-11-27T09:00:00.654321Z");
  private static final long NOW_MICROS = 1_795_770_000_654_321L;
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

  // ── today's statements, as literals ─────────────────────────────────────────────────────────

  private static final String INSERT_NEW =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, created_at,"
          + " updated_at, genesis_applied, dead_letter_pending, episode_version,"
          + " episode_claimed_at, episode_cause_event_id)"
          + " VALUES (?, ?, ?, ?, 1, ?, ?, 1, ?, ?, ?, ?)"
          + " ON CONFLICT (saga_id) DO NOTHING";
  private static final String INSERT_GENESIS_PENDING =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, created_at,"
          + " updated_at, genesis_applied, dead_letter_pending, episode_version,"
          + " episode_claimed_at, episode_cause_event_id)"
          + " VALUES (?, ?, ?, ?, 1, ?, ?, 0, ?, ?, ?, ?)"
          + " ON CONFLICT (saga_id) DO NOTHING";
  private static final String INSERT_FAULTED =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, created_at,"
          + " updated_at, genesis_applied, dead_letter_pending)"
          + " VALUES (?, ?, 'FAULTED', ?, 1, ?, ?, 0, 1)"
          + " ON CONFLICT (saga_id) DO NOTHING";
  private static final String SELECT_TYPE = "SELECT saga_type FROM saga_state WHERE saga_id = ?";
  private static final String UPDATE_CAS =
      "UPDATE saga_state SET status = ?, state_json = ?, version = version + 1, updated_at = ?,"
          + " pre_fault_status = NULL,"
          + " episode_version = CASE WHEN ? AND episode_version IS NULL"
          + " THEN version + 1 ELSE episode_version END,"
          + " episode_claimed_at = CASE WHEN ? AND episode_version IS NULL"
          + " THEN ? ELSE episode_claimed_at END,"
          + " episode_cause_event_id = CASE WHEN ? AND episode_version IS NULL"
          + " THEN ? ELSE episode_cause_event_id END"
          + " WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";
  private static final String APPLY_EVENT =
      "UPDATE saga_state SET status = ?, state_json = ?, version = version + 1, updated_at = ?,"
          + " pre_fault_status = NULL,"
          + " genesis_applied = (genesis_applied OR ?),"
          + " last_applied_offset = max(coalesce(last_applied_offset, -1), ?),"
          + " last_replayed_offset = CASE WHEN ? THEN ? ELSE last_replayed_offset END,"
          + " episode_version = CASE WHEN ? AND episode_version IS NULL"
          + " THEN version + 1 ELSE episode_version END,"
          + " episode_claimed_at = CASE WHEN ? AND episode_version IS NULL"
          + " THEN ? ELSE episode_claimed_at END,"
          + " episode_cause_event_id = CASE WHEN ? AND episode_version IS NULL"
          + " THEN ? ELSE episode_cause_event_id END"
          + " WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";
  private static final String MARK_FAULTED_PRIOR =
      "SELECT status FROM saga_state WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";
  private static final String MARK_FAULTED =
      "UPDATE saga_state SET"
          + " pre_fault_status = CASE WHEN status = 'FAULTED' THEN pre_fault_status"
          + " WHEN genesis_applied = 0 AND status <> 'COMPENSATING' THEN NULL"
          + " ELSE status END,"
          + " status = 'FAULTED', version = version + 1, updated_at = ?"
          + " WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";
  private static final String SET_DEAD_LETTER_PENDING =
      "UPDATE saga_state SET dead_letter_pending = ? WHERE saga_id = ? AND saga_type = ?";
  private static final String DELETE_BY_TYPE =
      "DELETE FROM saga_state WHERE saga_id = ? AND saga_type = ?";
  private static final String SELECT_BY_TYPE =
      "SELECT state_json, status, version, updated_at, episode_version, episode_claimed_at,"
          + " episode_cause_event_id, genesis_applied, pre_fault_status, dead_letter_pending,"
          + " last_applied_offset, last_replayed_offset"
          + " FROM saga_state WHERE saga_id = ? AND saga_type = ?";
  private static final String SELECT_ROW_FACTS =
      "SELECT status, dead_letter_pending, last_applied_offset FROM saga_state"
          + " WHERE saga_id = ? AND saga_type = ?";
  private static final String SELECT_TIMED_OUT =
      "WITH forward_timeouts AS ("
          + "SELECT saga_id, created_at FROM saga_state "
          + "WHERE saga_type = ? AND status IN ('STARTED', 'RUNNING') AND created_at < ? "
          + "ORDER BY created_at ASC LIMIT ?), "
          + "compensation_repicks AS ("
          + "SELECT saga_id, updated_at FROM saga_state "
          + "WHERE saga_type = ? AND status = 'COMPENSATING' AND updated_at < ? "
          + "ORDER BY updated_at ASC LIMIT ?) "
          + "SELECT saga_id FROM ("
          + "SELECT saga_id, 0 AS population, "
          + "row_number() OVER (ORDER BY created_at ASC) AS slot FROM forward_timeouts "
          + "UNION ALL "
          + "SELECT saga_id, 1 AS population, "
          + "row_number() OVER (ORDER BY updated_at ASC) AS slot FROM compensation_repicks) "
          + "ORDER BY slot ASC, population ASC LIMIT ?";
  private static final String SELECT_BY_STATUS =
      "SELECT saga_id FROM saga_state WHERE saga_type = ? AND status = ?"
          + " ORDER BY updated_at DESC LIMIT ?";
  private static final String COUNT_BY_STATUS =
      "SELECT count(*) FROM saga_state WHERE saga_type = ? AND status = ?";
  private static final String SELECT_COMPENSATING =
      "SELECT saga_id, updated_at, episode_claimed_at FROM saga_state"
          + " WHERE saga_type = ? AND status = 'COMPENSATING' AND updated_at < ?"
          + " ORDER BY updated_at ASC LIMIT ?";

  private static final String BUSY_ADVICE =
      " stored nothing. Most likely an application transaction on the same database file holds"
          + " the write lock: StreamRune's stores never join an application's transaction (for"
          + " example a Spring @Transactional method on the same DataSource), so call them outside"
          + " it. Otherwise another connection of this process held the lock that long: a schema"
          + " migration or a stalled write. Retry later, or raise busy_timeout.";

  /** What a write transaction that committed and then threw looks like. */
  private static final List<String> COMMITTED_SINGLE_STATEMENT =
      List.of("open #1", "#1 execute BEGIN IMMEDIATE", "#1 execute COMMIT", "#1 close");

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

  private static final ProbeState RUNNING = new ProbeState(SagaStatus.RUNNING, "running");
  private static final ProbeState COMPENSATING =
      new ProbeState(SagaStatus.COMPENSATING, "compensating");

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();

  /** The recorder's open connections at each state deserialization, in order. */
  private final List<Integer> openAtDeserialize = new CopyOnWriteArrayList<>();

  private Path file;
  private RecordingDataSource recorder;
  private SqliteDatabase database;
  private SqliteSagaStore store;

  @BeforeEach
  void openDatabase() {
    use("sagas.db", null);
  }

  @AfterEach
  void closeDatabases() throws Exception {
    for (AutoCloseable resource : resources.reversed()) {
      resource.close();
    }
  }

  /** Points the test at a fresh, migrated database file, through a new recorder and store. */
  private void use(String name, String override) {
    file = dir.resolve(name);
    recorder =
        new RecordingDataSource(
            override == null
                ? TestDatabases.dataSource(file)
                : TestDatabases.dataSource(file, override));
    database = SqliteDatabase.open(recorder);
    resources.add(database);
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
    store = store(CLOCK);
    recorder.clear();
  }

  /** {@code createObjectMapper(null)}, with the probe {@link ProbeState}'s creator reports to. */
  private ObjectMapper mapper() {
    ObjectMapper mapper = SqliteSagaStore.createObjectMapper(null);
    Probe probe = () -> openAtDeserialize.add(recorder.openConnections());
    mapper.setInjectableValues(new InjectableValues.Std().addValue("probe", probe));
    return mapper;
  }

  private SqliteSagaStore store(Clock clock) {
    return new SqliteSagaStore(database, mapper(), clock);
  }

  // ── Golden transcripts ──────────────────────────────────────────────────────────────────────

  @Test
  void transcript_create() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare " + INSERT_NEW,
            "#1 execute COMMIT",
            "#1 close");

    // The conflict: the stored type is read after the insert, on the same connection, in the same
    // transaction, which then commits; the refusal is thrown after it.
    recorder.clear();
    Throwable conflict =
        catchThrowable(() -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false));

    assertThat(conflict).isExactlyInstanceOf(OptimisticLockException.class);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare " + INSERT_NEW,
            "#1 prepare " + SELECT_TYPE,
            "#1 execute COMMIT",
            "#1 close");
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
  void transcript_findCompensating() {
    store.create(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, false);
    recorder.clear();

    assertThat(store.findCompensating(TYPE, NOW.plusSeconds(1), 10)).hasSize(1);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SELECT_COMPENSATING, "#1 close");
  }

  @Test
  void transcript_otherReads() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    recorder.clear();
    assertThat(store.findTimedOut(TYPE, NOW.plusSeconds(1), 10)).containsExactly(S);
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

  @Test
  void transcript_writes() {
    recorder.clear();
    store.createGenesisPending(S, TYPE, RUNNING, SagaStatus.RUNNING, false, new EventId("e-1"));
    assertThat(recorder.transcript()).containsExactly(write(INSERT_GENESIS_PENDING));

    recorder.clear();
    store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1);
    assertThat(recorder.transcript()).containsExactly(write(UPDATE_CAS));

    recorder.clear();
    store.applyEvent(
        S, TYPE, RUNNING, SagaStatus.RUNNING, 2, AppliedEvent.liveStart(GlobalOffset.of(5)));
    assertThat(recorder.transcript()).containsExactly(write(APPLY_EVENT));

    recorder.clear();
    store.claimCompensating(S, TYPE, COMPENSATING, 3, new EventId("e-2"));
    assertThat(recorder.transcript()).containsExactly(write(UPDATE_CAS));

    recorder.clear();
    assertThat(store.markFaulted(S, TYPE, 4)).isTrue();
    assertThat(recorder.transcript()).containsExactly(write(MARK_FAULTED_PRIOR, MARK_FAULTED));

    recorder.clear();
    store.setDeadLetterPending(S, TYPE, true);
    assertThat(recorder.transcript()).containsExactly(write(SET_DEAD_LETTER_PENDING));

    recorder.clear();
    store.delete(S, TYPE);
    assertThat(recorder.transcript()).containsExactly(write(DELETE_BY_TYPE));

    recorder.clear();
    store.createFaulted(S, TYPE, RUNNING);
    assertThat(recorder.transcript()).containsExactly(write(INSERT_FAULTED));
    assertThat(recorder.openConnections()).isZero();
  }

  /** One committed write transaction on connection #1 preparing {@code statements} in order. */
  private static String[] write(String... statements) {
    List<String> events = new ArrayList<>();
    events.add("open #1");
    events.add("#1 execute BEGIN IMMEDIATE");
    for (String statement : statements) {
      events.add("#1 prepare " + statement);
    }
    events.add("#1 execute COMMIT");
    events.add("#1 close");
    return events.toArray(String[]::new);
  }

  // ── The create-conflict race ────────────────────────────────────────────────────────────────

  /**
   * The row that made the insert do nothing is deleted on the insert's own connection, inside its
   * write transaction, right before the stored type is read: the create still fails as "already
   * exists", never succeeds silently. The transaction commits (with the delete), so afterwards
   * there is no row.
   */
  @Test
  void createRace_conflictingRowVanishes_isOptimisticLock() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    List<String> transcriptAtHook = new ArrayList<>();
    List<Integer> deletedAtHook = new ArrayList<>();
    List<Long> rowsSeenByAnotherConnection = new ArrayList<>();
    recorder.beforePrepare(
        SELECT_TYPE,
        sameConnection -> {
          transcriptAtHook.addAll(recorder.transcript());
          try (PreparedStatement ps =
              sameConnection.prepareStatement("DELETE FROM saga_state WHERE saga_id = ?")) {
            ps.setString(1, S.value());
            deletedAtHook.add(ps.executeUpdate());
          }
          // The delete is not committed yet: it is part of the insert's open transaction.
          rowsSeenByAnotherConnection.add(longValue("SELECT count(*) FROM saga_state"));
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
    assertThat(transcriptAtHook)
        .containsExactly("open #1", "#1 execute BEGIN IMMEDIATE", "#1 prepare " + INSERT_NEW);
    assertThat(deletedAtHook).containsExactly(1);
    assertThat(rowsSeenByAnotherConnection).containsExactly(1L);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare " + INSERT_NEW,
            "#1 prepare " + SELECT_TYPE,
            "#1 execute COMMIT",
            "#1 close");
    assertThat(recorder.openConnections()).isZero();
    assertThat(store.load(S, TYPE, ProbeState.class)).isEmpty();
    assertThat(longValue("SELECT count(*) FROM saga_state")).isZero();
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
    SagaTypeCollisionException collision = (SagaTypeCollisionException) thrown;
    assertThat(collision.sagaId()).isEqualTo(S);
    assertThat(collision.existingType()).isEqualTo(TYPE);
    assertThat(collision.attemptedType()).isEqualTo(OTHER);
    // Nothing changed: the row is the first create's.
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
    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.state()).isEqualTo(RUNNING);
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.deadLetterPending()).isFalse();
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

  // ── Where each domain refusal surfaces ──────────────────────────────────────────────────────

  /**
   * Stale views, type collisions and the "already exists" refusal of every write are thrown after
   * the write transaction committed (outside {@code SqliteTransactions.write}), with their own
   * exact type, never wrapped in an {@link EventStoreException}.
   */
  @Test
  void domainExceptionsSurfaceOutsideTheWrite() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    String stale = "Stale view of saga s (expectedVersion=7)";

    assertRefusedAfterCommit(
        () -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 7),
        OptimisticLockException.class,
        stale,
        UPDATE_CAS);
    assertRefusedAfterCommit(
        () -> store.claimCompensating(S, TYPE, COMPENSATING, 7, new EventId("e-1")),
        OptimisticLockException.class,
        stale,
        UPDATE_CAS);
    assertRefusedAfterCommit(
        () ->
            store.applyEvent(
                S, TYPE, RUNNING, SagaStatus.RUNNING, 7, AppliedEvent.live(GlobalOffset.of(3))),
        OptimisticLockException.class,
        stale,
        APPLY_EVENT);
    assertRefusedAfterCommit(
        () -> store.markFaulted(S, TYPE, 7),
        OptimisticLockException.class,
        stale,
        MARK_FAULTED_PRIOR);
    // A row of another type is a stale view to every compare-and-swap.
    assertRefusedAfterCommit(
        () -> store.update(S, OTHER, RUNNING, SagaStatus.RUNNING, 1),
        OptimisticLockException.class,
        "Stale view of saga s (expectedVersion=1)",
        UPDATE_CAS);

    String exists = "Saga s already exists";
    assertRefusedAfterCommit(
        () -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false),
        OptimisticLockException.class,
        exists,
        INSERT_NEW,
        SELECT_TYPE);
    assertRefusedAfterCommit(
        () ->
            store.createGenesisPending(
                S, TYPE, RUNNING, SagaStatus.RUNNING, false, new EventId("e-1")),
        OptimisticLockException.class,
        exists,
        INSERT_GENESIS_PENDING,
        SELECT_TYPE);
    assertRefusedAfterCommit(
        () -> store.createFaulted(S, TYPE, RUNNING),
        OptimisticLockException.class,
        exists,
        INSERT_FAULTED,
        SELECT_TYPE);

    String collision = collisionMessage("order-fulfillment", "refund");
    assertRefusedAfterCommit(
        () -> store.create(S, OTHER, RUNNING, SagaStatus.RUNNING, false),
        SagaTypeCollisionException.class,
        collision,
        INSERT_NEW,
        SELECT_TYPE);
    assertRefusedAfterCommit(
        () ->
            store.createGenesisPending(
                S, OTHER, RUNNING, SagaStatus.RUNNING, false, new EventId("e-1")),
        SagaTypeCollisionException.class,
        collision,
        INSERT_GENESIS_PENDING,
        SELECT_TYPE);
    assertRefusedAfterCommit(
        () -> store.createFaulted(S, OTHER, RUNNING),
        SagaTypeCollisionException.class,
        collision,
        INSERT_FAULTED,
        SELECT_TYPE);

    // None of them changed the row.
    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
  }

  private void assertRefusedAfterCommit(
      ThrowingCallable call,
      Class<? extends Throwable> type,
      String message,
      String... statements) {
    recorder.clear();
    Throwable thrown = catchThrowable(call);
    assertThat(thrown).isExactlyInstanceOf(type).hasMessage(message).hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly(write(statements));
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void staleView_message() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    Throwable thrown = catchThrowable(() -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 3));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Stale view of saga s (expectedVersion=3)")
        .hasNoCause();
  }

  @Test
  void staleView_terminalRow() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    store.update(S, TYPE, RUNNING, SagaStatus.COMPLETED, 1);

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

    assertThat(stale).hasMessage("Stale view of saga abc (expectedVersion=9)");
    assertThat(exists).hasMessage("Saga abc already exists");
  }

  // ── Row decoding versus state deserialization ───────────────────────────────────────────────

  /**
   * A stored status no {@link SagaStatus} names fails while the row is decoded, with the connection
   * open: a storage failure, and the state is never deserialized.
   */
  @Test
  void corruptStatus_isEventStoreException() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    sql("UPDATE saga_state SET status = 'BOGUS' WHERE saga_id = 's'");
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

    Throwable facts = catchThrowable(() -> store.loadRowFacts(S, TYPE));
    assertThat(facts)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to load saga row facts: s");
    assertUnknownStatusCause(facts, "BOGUS");

    // A row with both a corrupt status and corrupt JSON fails as the status, before the JSON.
    sql("UPDATE saga_state SET state_json = '{\"status\":\"RUNNING\",\"step\":\"poison\"}'");
    Throwable both = catchThrowable(() -> store.load(S, TYPE, ProbeState.class));
    assertThat(both)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to load saga state: s");
    assertThat(openAtDeserialize).isEmpty();
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void corruptPreFaultStatus_isEventStoreException() throws SQLException {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    store.markFaulted(S, TYPE, 1);
    sql("UPDATE saga_state SET pre_fault_status = 'WEIRD' WHERE saga_id = 's'");
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
    sql("UPDATE saga_state SET state_json = '{\"status\":\"RUNNING\",\"step\":\"poison\"}'");
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

    // An unknown property after the creator properties fails as the same exception type; the
    // creator has already run by then, again with no connection open.
    sql("UPDATE saga_state SET state_json = '{\"status\":\"RUNNING\",\"step\":\"x\",\"extra\":1}'");
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
   * findCompensating decodes the saga id and timestamps, never the status or the state: a corrupt
   * saga id is a storage failure, a corrupt status drops the row from the scan, corrupt JSON is not
   * read.
   */
  @Test
  void findCompensating_corruptRows() throws SQLException {
    store.create(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, false);
    Instant before = NOW.plusSeconds(1);

    sql("UPDATE saga_state SET state_json = '{\"status\":\"RUNNING\",\"step\":\"poison\"}'");
    openAtDeserialize.clear();
    assertThat(store.findCompensating(TYPE, before, 10))
        .containsExactly(new CompensatingSaga(S, NOW, NOW));
    assertThat(openAtDeserialize).isEmpty();

    sql("UPDATE saga_state SET saga_id = ' ' WHERE saga_id = 's'");
    Throwable thrown = catchThrowable(() -> store.findCompensating(TYPE, before, 10));
    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find compensating sagas");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("SagaId value must not be null or blank");
    assertThat(recorder.openConnections()).isZero();

    sql("UPDATE saga_state SET saga_id = 's', status = 'BOGUS'");
    assertThat(store.findCompensating(TYPE, before, 10)).isEmpty();
  }

  // ── Argument refusals ───────────────────────────────────────────────────────────────────────

  @Test
  void negativeLimit_isIllegalArgument() {
    assertRefused(
        () -> store.findCompensating(TYPE, NOW, -1), "limit must not be negative, got -1");
    assertRefused(() -> store.findTimedOut(TYPE, NOW, -1), "limit must not be negative, got -1");
    assertRefused(
        () -> store.findByStatus(TYPE, SagaStatus.RUNNING, -1),
        "limit must not be negative, got -1");
  }

  @Test
  void argumentRefusals() {
    assertRefused(() -> new SqliteSagaStore(null, mapper(), CLOCK), "database is required");
    assertRefused(() -> new SqliteSagaStore(database, null, CLOCK), "objectMapper is required");
    assertRefused(() -> new SqliteSagaStore(database, mapper(), null), "clock is required");
    assertRefused(
        () -> store.create(null, TYPE, RUNNING, SagaStatus.RUNNING, false),
        "sagaId must not be null");
    assertRefused(
        () -> store.create(S, null, RUNNING, SagaStatus.RUNNING, false),
        "sagaType must not be null");
    assertRefused(
        () -> store.create(S, TYPE, null, SagaStatus.RUNNING, false), "state must not be null");
    assertRefused(() -> store.create(S, TYPE, RUNNING, null, false), "status must not be null");
    assertRefused(() -> store.createFaulted(S, TYPE, null), "state must not be null");
    assertRefused(() -> store.update(S, TYPE, RUNNING, null, 1), "status must not be null");
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

    Throwable metrics = catchThrowable(() -> SqliteSagaStore.createObjectMapper(null, null));
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
            () -> store.create(SagaId.of("n"), TYPE, bad, SagaStatus.RUNNING, false),
            () ->
                store.createGenesisPending(
                    SagaId.of("n"), TYPE, bad, SagaStatus.RUNNING, false, null),
            () -> store.createFaulted(SagaId.of("n"), TYPE, bad),
            () -> store.update(SagaId.of("n"), TYPE, bad, SagaStatus.RUNNING, 1),
            () -> store.claimCompensating(SagaId.of("n"), TYPE, bad, 1, null),
            () ->
                store.applyEvent(
                    SagaId.of("n"),
                    TYPE,
                    bad,
                    SagaStatus.RUNNING,
                    1,
                    AppliedEvent.live(GlobalOffset.of(1))))) {
      recorder.clear();
      Throwable thrown = catchThrowable(call);
      assertThat(thrown)
          .isExactlyInstanceOf(SagaStateSerializationException.class)
          .hasMessage("Failed to serialize saga state: n");
      assertThat(((SagaStateSerializationException) thrown).sagaId()).isEqualTo(SagaId.of("n"));
      assertThat(thrown.getCause()).isExactlyInstanceOf(JsonMappingException.class);
      assertThat(recorder.connectionsOpened()).isZero();
    }
  }

  // ── Storage failures ────────────────────────────────────────────────────────────────────────

  /**
   * A statement of a write that fails rolls the transaction back and surfaces as an {@link
   * EventStoreException} naming the write, whose cause is the driver's exception itself.
   */
  @Test
  void writeFailureMessages() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    assertWriteFails(
        "INSERT INTO saga_state",
        () -> store.create(SagaId.of("n"), TYPE, RUNNING, SagaStatus.RUNNING, false),
        "Failed to create saga state: n");
    assertWriteFails(
        "INSERT INTO saga_state",
        () ->
            store.createGenesisPending(
                SagaId.of("n"), TYPE, RUNNING, SagaStatus.RUNNING, false, null),
        "Failed to create saga state: n");
    assertWriteFails(
        "INSERT INTO saga_state",
        () -> store.createFaulted(SagaId.of("n"), TYPE, RUNNING),
        "Failed to create saga state: n");
    assertWriteFails(
        "UPDATE saga_state SET status",
        () -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1),
        "Failed to update saga state: s");
    assertWriteFails(
        "UPDATE saga_state SET status",
        () -> store.claimCompensating(S, TYPE, COMPENSATING, 1, null),
        "Failed to claim compensating saga: s");
    assertWriteFails(
        "UPDATE saga_state SET status",
        () ->
            store.applyEvent(
                S, TYPE, RUNNING, SagaStatus.RUNNING, 1, AppliedEvent.live(GlobalOffset.of(1))),
        "Failed to apply event to saga state: s");
    assertWriteFails(
        "SELECT status FROM saga_state",
        () -> store.markFaulted(S, TYPE, 1),
        "Failed to mark saga FAULTED: s");
    assertWriteFails(
        "UPDATE saga_state SET dead_letter_pending",
        () -> store.setDeadLetterPending(S, TYPE, true),
        "Failed to set saga dead-letter shield: s");
    assertWriteFails(
        "DELETE FROM saga_state", () -> store.delete(S, TYPE), "Failed to delete saga state: s");

    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.deadLetterPending()).isFalse();
    assertThat(store.load(SagaId.of("n"), TYPE, ProbeState.class)).isEmpty();
  }

  private void assertWriteFails(String prefix, ThrowingCallable call, String message) {
    SQLException injected = new SQLException("injected");
    recorder.beforePrepare(
        prefix,
        c -> {
          throw injected;
        });
    recorder.clear();

    Throwable thrown = catchThrowable(call);

    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isSameAs(injected);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 execute BEGIN IMMEDIATE", "#1 execute ROLLBACK", "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void readFailureMessages() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);

    assertReadFails(
        SELECT_BY_TYPE,
        () -> store.load(S, TYPE, ProbeState.class),
        "Failed to load saga state: s");
    assertReadFails(
        SELECT_ROW_FACTS, () -> store.loadRowFacts(S, TYPE), "Failed to load saga row facts: s");
    assertReadFails(
        SELECT_TIMED_OUT, () -> store.findTimedOut(TYPE, NOW, 1), "Failed to find timed-out sagas");
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
        () -> store.findCompensating(TYPE, NOW, 1),
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
   * markFaulted's update must change the row its first statement read; when it does not, the
   * transaction (and whatever else it did) rolls back and the fault is a storage failure.
   */
  @Test
  void markFaulted_rowChangedBetweenItsStatements() {
    store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false);
    recorder.beforePrepare(
        "UPDATE saga_state SET pre_fault_status",
        sameConnection -> {
          try (Statement st = sameConnection.createStatement()) {
            st.executeUpdate("UPDATE saga_state SET version = version + 10 WHERE saga_id = 's'");
          }
        });
    recorder.clear();

    Throwable thrown = catchThrowable(() -> store.markFaulted(S, TYPE, 1));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to mark saga FAULTED: s");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("the row read in this transaction was not updated")
        .hasNoCause();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare " + MARK_FAULTED_PRIOR,
            "#1 prepare " + MARK_FAULTED,
            "#1 execute ROLLBACK",
            "#1 close");
    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
  }

  /**
   * A write whose connection fails to close after its COMMIT surfaces as a storage failure, though
   * the row was written.
   */
  @Test
  void closeFailureAfterCommit_isAStorageFailureOfAWrittenRow() {
    recorder.failNextCloseAfterCommit();

    Throwable thrown =
        catchThrowable(() -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to create saga state: s");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(recorder.transcript()).containsExactly(write(INSERT_NEW));
    assertThat(store.load(S, TYPE, ProbeState.class).orElseThrow().version()).isEqualTo(1L);

    // A stale view whose close fails after its COMMIT is that storage failure, not the stale view.
    recorder.failNextCloseAfterCommit();
    Throwable stale = catchThrowable(() -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 5));
    assertThat(stale)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to update saga state: s");
    assertThat(stale.getCause()).hasMessage("injected close failure");
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void lockFailureMessages() throws SQLException {
    use("busy.db", "busy_timeout=200");

    try (Connection holder = TestDatabases.dataSource(file).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      try {
        assertBusy(
            () -> store.create(S, TYPE, RUNNING, SagaStatus.RUNNING, false),
            "saga create of saga s");
        assertBusy(() -> store.createFaulted(S, TYPE, RUNNING), "saga create of saga s");
        assertBusy(
            () -> store.update(S, TYPE, RUNNING, SagaStatus.RUNNING, 1), "saga update of saga s");
        assertBusy(
            () ->
                store.applyEvent(
                    S, TYPE, RUNNING, SagaStatus.RUNNING, 1, AppliedEvent.live(GlobalOffset.of(1))),
            "saga apply event of saga s");
        assertBusy(() -> store.markFaulted(S, TYPE, 1), "saga mark faulted of saga s");
        assertBusy(
            () -> store.setDeadLetterPending(S, TYPE, true), "saga dead-letter shield of saga s");
        assertBusy(() -> store.delete(S, TYPE), "saga delete of saga s");
      } finally {
        st.execute("ROLLBACK");
      }
    }
  }

  private void assertBusy(ThrowingCallable call, String what) {
    recorder.clear();
    Throwable thrown = catchThrowable(call);
    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "SQLite's write lock was not granted within the connection's busy_timeout, so the "
                + what
                + BUSY_ADVICE);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(thrown.getCause()).isExactlyInstanceOf(SQLiteException.class);
    assertThat(((SQLiteException) thrown.getCause()).getResultCode())
        .isEqualTo(SQLiteErrorCode.SQLITE_BUSY);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 execute BEGIN IMMEDIATE", "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  // ── Row assembly and timestamps ─────────────────────────────────────────────────────────────

  @Test
  void loadAssemblesEveryColumn() {
    store.createGenesisPending(
        S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, true, new EventId("e-1"));
    store.applyEvent(
        S,
        TYPE,
        COMPENSATING,
        SagaStatus.COMPENSATING,
        1,
        new AppliedEvent(GlobalOffset.of(7), true, true, new EventId("e-2")));
    assertThat(store.markFaulted(S, TYPE, 2)).isTrue();

    assertThat(store.load(S, TYPE, ProbeState.class))
        .contains(
            new LoadedSaga<>(
                COMPENSATING,
                SagaStatus.FAULTED,
                3L,
                NOW,
                1L,
                NOW,
                new EventId("e-1"),
                true,
                SagaStatus.COMPENSATING,
                true,
                7L,
                7L));
    assertThat(store.loadRowFacts(S, TYPE)).contains(new RowFacts(SagaStatus.FAULTED, true, 7L));

    store.create(SagaId.of("plain"), TYPE, RUNNING, SagaStatus.RUNNING, false);
    assertThat(store.load(SagaId.of("plain"), TYPE, ProbeState.class))
        .contains(
            new LoadedSaga<>(
                RUNNING,
                SagaStatus.RUNNING,
                1L,
                NOW,
                null,
                null,
                null,
                true,
                null,
                false,
                null,
                null));
    assertThat(store.loadRowFacts(SagaId.of("plain"), TYPE))
        .contains(new RowFacts(SagaStatus.RUNNING, false, null));
    assertThat(store.loadRowFacts(SagaId.of("absent"), TYPE)).isEmpty();
  }

  /** Every timestamp is the store's JVM clock in epoch microseconds; there is no database clock. */
  @Test
  void timestampsComeFromTheGivenClock() throws SQLException {
    store.create(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, false);

    assertThat(longValue("SELECT created_at FROM saga_state")).isEqualTo(NOW_MICROS);
    assertThat(longValue("SELECT updated_at FROM saga_state")).isEqualTo(NOW_MICROS);
    assertThat(longValue("SELECT episode_claimed_at FROM saga_state")).isEqualTo(NOW_MICROS);
    LoadedSaga<ProbeState> row = store.load(S, TYPE, ProbeState.class).orElseThrow();
    assertThat(row.updatedAt()).isEqualTo(NOW);
    assertThat(row.episodeClaimedAt()).isEqualTo(NOW);

    // A store on another clock moves updated_at only; created_at and the stamp stay.
    Instant later = NOW.plus(Duration.ofHours(1));
    store(Clock.fixed(later, ZoneOffset.UTC))
        .update(S, TYPE, COMPENSATING, SagaStatus.COMPENSATING, 1);

    assertThat(longValue("SELECT created_at FROM saga_state")).isEqualTo(NOW_MICROS);
    assertThat(longValue("SELECT updated_at FROM saga_state"))
        .isEqualTo(NOW_MICROS + 3_600_000_000L);
    assertThat(longValue("SELECT episode_claimed_at FROM saga_state")).isEqualTo(NOW_MICROS);
    assertThat(store.load(S, TYPE, ProbeState.class).orElseThrow().updatedAt()).isEqualTo(later);
    assertThat(store.findCompensating(TYPE, later.plusNanos(1_000), 10))
        .containsExactly(new CompensatingSaga(S, later, NOW));
    assertThat(store.findCompensating(TYPE, later, 10)).isEmpty();
  }

  // ── raw SQL on an unrecorded connection ─────────────────────────────────────────────────────

  private void sql(String statement) throws SQLException {
    try (Connection c = TestDatabases.dataSource(file).getConnection();
        Statement st = c.createStatement()) {
      st.executeUpdate(statement);
    }
  }

  private long longValue(String query) throws SQLException {
    try (Connection c = TestDatabases.dataSource(file).getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      assertThat(rs.next()).isTrue();
      return rs.getLong(1);
    }
  }
}
