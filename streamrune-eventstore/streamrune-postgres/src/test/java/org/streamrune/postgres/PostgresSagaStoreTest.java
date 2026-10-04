package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaTypeCollisionException;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresSagaStoreTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  // Jackson-annotated saga state for integration tests
  static class TestSagaState implements SagaState {
    @JsonProperty("status")
    private final SagaStatus status;

    @JsonProperty("orderId")
    private final String orderId;

    @JsonCreator
    TestSagaState(
        @JsonProperty("status") SagaStatus status, @JsonProperty("orderId") String orderId) {
      this.status = status;
      this.orderId = orderId;
    }

    @Override
    public SagaStatus status() {
      return status;
    }

    public String orderId() {
      return orderId;
    }
  }

  // Minimal test saga whose status() is always RUNNING — for divergence test
  record TestSaga(SagaStatus s) implements SagaState {
    @JsonCreator
    TestSaga(@JsonProperty("s") SagaStatus s) {
      this.s = s;
    }

    @Override
    public SagaStatus status() {
      return s;
    }
  }

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  PostgresSagaStore sagaStore;

  SagaId casId = SagaId.of("saga-cas-1");
  SagaType casType = SagaType.of("TestSaga");
  TestSagaState casState = new TestSagaState(SagaStatus.RUNNING, "order-cas");

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());

    // Apply the shipped event-store baseline (creates saga_state)
    EventTypeRegistry typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            // No @Encrypted-bearing types are exercised through this registry in this test; report
            // none explicitly rather than inheriting the throwing default.
            return java.util.List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    sagaStore = new PostgresSagaStore(dataSource, objectMapper);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_state");
    }
  }

  // --- CAS tests (7 cases, mirrors InMemorySagaStoreTest) ---

  @Test
  void create_thenLoad_returnsVersion1() {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING);
    assertThat(sagaStore.load(casId, casType, TestSagaState.class).orElseThrow().version())
        .isEqualTo(1L);
  }

  @Test
  void create_whenAlreadyExists_throwsOptimisticLock() {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING);
    assertThatThrownBy(() -> sagaStore.create(casId, casType, casState, SagaStatus.RUNNING))
        .isInstanceOf(OptimisticLockException.class);
  }

  @Test
  void update_withCurrentVersion_succeeds_andIncrements() {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING); // v1
    sagaStore.update(casId, casType, casState, SagaStatus.RUNNING, 1L); // → v2
    assertThat(sagaStore.load(casId, casType, TestSagaState.class).orElseThrow().version())
        .isEqualTo(2L);
  }

  @Test
  void update_withStaleVersion_throwsOptimisticLock() {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING); // v1
    sagaStore.update(casId, casType, casState, SagaStatus.RUNNING, 1L); // v2
    assertThatThrownBy(
            () -> sagaStore.update(casId, casType, casState, SagaStatus.RUNNING, 1L)) // stale
        .isInstanceOf(OptimisticLockException.class);
  }

  @Test
  void update_onTerminalRow_throwsOptimisticLock_regardlessOfVersion() {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING);
    sagaStore.update(casId, casType, casState, SagaStatus.COMPENSATED, 1L); // terminal, v2
    assertThatThrownBy(() -> sagaStore.update(casId, casType, casState, SagaStatus.RUNNING, 2L))
        .isInstanceOf(OptimisticLockException.class);
  }

  @Test
  void update_onFaultedRow_succeeds() { // FAULTED writable (Slice-2 replay)
    sagaStore.create(casId, casType, casState, SagaStatus.FAULTED);
    sagaStore.update(casId, casType, casState, SagaStatus.RUNNING, 1L);
    assertThat(sagaStore.load(casId, casType, TestSagaState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void update_onMissingRow_throwsOptimisticLock() {
    assertThatThrownBy(() -> sagaStore.update(casId, casType, casState, SagaStatus.RUNNING, 1L))
        .isInstanceOf(OptimisticLockException.class);
  }

  // --- Durable compensation-episode identity (episode_version) round-trip ---

  @Test
  void create_leavesEpisodeVersionNull() {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING);
    assertThat(sagaStore.load(casId, casType, TestSagaState.class).orElseThrow().episodeVersion())
        .isNull();
  }

  @Test
  void claimCompensating_stampsEpisodeVersionToTheNewRowVersion() {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING); // v1
    sagaStore.claimCompensating(casId, casType, casState, 1L); // → v2, episode_version = 2
    var loaded = sagaStore.load(casId, casType, TestSagaState.class).orElseThrow();
    assertThat(loaded.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(loaded.version()).isEqualTo(2L);
    assertThat(loaded.episodeVersion()).isEqualTo(2L);
  }

  @Test
  void faultThenUnfaultCycle_bumpsVersionButLeavesEpisodeVersionUntouched() {
    // The core invariant: markFaulted (COMPENSATING -> FAULTED) then unfault
    // (FAULTED -> COMPENSATING) both bump the row version, but neither disturbs episode_version, so
    // a
    // replayed resume re-derives the ORIGINAL episode key.
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING); // v1
    sagaStore.claimCompensating(
        casId, casType, casState, 1L); // v2, episode_version = 2 (episode E)

    sagaStore.update(casId, casType, casState, SagaStatus.FAULTED, 2L); // markFaulted → v3
    sagaStore.update(casId, casType, casState, SagaStatus.COMPENSATING, 3L); // unfault → v4

    var loaded = sagaStore.load(casId, casType, TestSagaState.class).orElseThrow();
    assertThat(loaded.version()).as("row version shifted by the fault/unfault cycle").isEqualTo(4L);
    assertThat(loaded.episodeVersion())
        .as("episode identity survives the fault->replay cycle unchanged")
        .isEqualTo(2L);
  }

  // --- Durable episode claim instant (episode_claimed_at) round-trip ---

  @Test
  void create_leavesEpisodeClaimedAtNull() {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING);
    assertThat(sagaStore.load(casId, casType, TestSagaState.class).orElseThrow().episodeClaimedAt())
        .isNull();
  }

  @Test
  void claimCompensating_stampsEpisodeClaimedAt() {
    Instant before = Instant.now().minusSeconds(60);
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING); // v1
    sagaStore.claimCompensating(casId, casType, casState, 1L); // → v2, claim instant stamped
    var loaded = sagaStore.load(casId, casType, TestSagaState.class).orElseThrow();
    assertThat(loaded.episodeClaimedAt()).isNotNull().isAfter(before);
  }

  @Test
  void faultThenUnfaultCycle_leavesEpisodeClaimedAtUntouched() {
    // The core invariant: markFaulted then unfault bump the row version but neither
    // disturbs episode_claimed_at, so the stale-compensation replay guard still measures key age
    // from the ORIGINAL claim — a fault->replay cycle cannot reset the clock.
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING); // v1
    sagaStore.claimCompensating(casId, casType, casState, 1L); // v2, claim instant stamped
    Instant claimedAt =
        sagaStore.load(casId, casType, TestSagaState.class).orElseThrow().episodeClaimedAt();
    assertThat(claimedAt).isNotNull();

    sagaStore.update(casId, casType, casState, SagaStatus.FAULTED, 2L); // markFaulted → v3
    sagaStore.update(casId, casType, casState, SagaStatus.COMPENSATING, 3L); // unfault → v4

    var loaded = sagaStore.load(casId, casType, TestSagaState.class).orElseThrow();
    assertThat(loaded.episodeClaimedAt())
        .as("claim instant survives the fault->replay cycle unchanged")
        .isEqualTo(claimedAt);
  }

  // --- The schema itself refuses an unstamped compensation episode ---

  /**
   * The {@code saga_state_episode_stamped} CHECK constraint is the second line behind the store's
   * statements: a row that is COMPENSATING, or FAULTED out of COMPENSATING, without both halves of
   * the stamp cannot exist, however it is written — so the runtime's refusal of such a row ({@code
   * SagaUnstampedCompensationEpisodeException}) is unreachable through this store and guards only a
   * third-party one.
   */
  @Test
  void theSchemaRefusesAnUnstampedCompensationEpisode() throws Exception {
    sagaStore.create(casId, casType, casState, SagaStatus.RUNNING); // v1, unstamped
    sagaStore.claimCompensating(casId, casType, casState, 1L); // v2, stamped

    assertCheckViolation(
        "UPDATE saga_state SET episode_version = NULL, episode_claimed_at = NULL WHERE saga_id = ?",
        "a COMPENSATING row cannot lose its stamp");
    assertCheckViolation(
        "UPDATE saga_state SET episode_claimed_at = NULL WHERE saga_id = ?",
        "the two halves are written together or not at all");
    sagaStore.markFaulted(casId, casType, 2L); // v3: FAULTED, pre_fault_status = COMPENSATING
    assertCheckViolation(
        "UPDATE saga_state SET episode_version = NULL, episode_claimed_at = NULL WHERE saga_id = ?",
        "a row FAULTED out of COMPENSATING keeps its stamp too");

    var other = SagaId.of("unstamped-by-hand");
    sagaStore.create(other, casType, casState, SagaStatus.RUNNING);
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE saga_state SET status = 'COMPENSATING' WHERE saga_id = ?")) {
      ps.setString(1, other.value());
      assertThatThrownBy(ps::executeUpdate)
          .as("a RUNNING row cannot be flipped to COMPENSATING without a stamp")
          .isInstanceOf(java.sql.SQLException.class)
          .hasMessageContaining("saga_state_episode_stamped");
    }
    assertThat(sagaStore.load(other, casType, TestSagaState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.RUNNING);
  }

  private void assertCheckViolation(String sql, String because) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(sql)) {
      ps.setString(1, casId.value());
      assertThatThrownBy(ps::executeUpdate)
          .as(because)
          .isInstanceOf(java.sql.SQLException.class)
          .hasMessageContaining("saga_state_episode_stamped");
    }
  }

  // --- existing tests (save → create / update) ---

  @Test
  void create_then_load_returns_state() {
    var id = SagaId.of("saga-order-1");
    var state = new TestSagaState(SagaStatus.RUNNING, "order-1");

    sagaStore.create(id, SagaType.of("TestSaga"), state, SagaStatus.RUNNING);

    var loaded = sagaStore.load(id, casType, TestSagaState.class);
    assertTrue(loaded.isPresent());
    assertEquals(SagaStatus.RUNNING, loaded.get().status());
    assertEquals("order-1", loaded.get().state().orderId());
  }

  @Test
  void create_then_update_overwrites() {
    var id = SagaId.of("saga-order-2");
    sagaStore.create(
        id,
        SagaType.of("TestSaga"),
        new TestSagaState(SagaStatus.STARTED, "order-2"),
        SagaStatus.STARTED);
    sagaStore.update(
        id,
        SagaType.of("TestSaga"),
        new TestSagaState(SagaStatus.RUNNING, "order-2"),
        SagaStatus.RUNNING,
        1L);

    var loaded = sagaStore.load(id, casType, TestSagaState.class);
    assertTrue(loaded.isPresent());
    assertEquals(SagaStatus.RUNNING, loaded.get().status());
  }

  @Test
  void update_increments_version_on_each_call() throws Exception {
    var id = SagaId.of("saga-version");
    sagaStore.create(
        id,
        SagaType.of("TestSaga"),
        new TestSagaState(SagaStatus.STARTED, "order-v"),
        SagaStatus.STARTED);
    sagaStore.update(
        id,
        SagaType.of("TestSaga"),
        new TestSagaState(SagaStatus.RUNNING, "order-v"),
        SagaStatus.RUNNING,
        1L);

    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT version FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(2, rs.getLong("version"));
      }
    }
  }

  @Test
  void update_rejects_overwrite_of_terminal_saga() {
    var id = SagaId.of("saga-terminal");
    sagaStore.create(
        id,
        SagaType.of("TestSaga"),
        new TestSagaState(SagaStatus.RUNNING, "order-t"),
        SagaStatus.RUNNING);
    sagaStore.update(
        id,
        SagaType.of("TestSaga"),
        new TestSagaState(SagaStatus.COMPLETED, "order-t"),
        SagaStatus.COMPLETED,
        1L);

    // A stale writer (e.g. a second instance holding old state) must not undo the terminal state.
    var ex =
        assertThrows(
            OptimisticLockException.class,
            () ->
                sagaStore.update(
                    id,
                    SagaType.of("TestSaga"),
                    new TestSagaState(SagaStatus.RUNNING, "order-t"),
                    SagaStatus.RUNNING,
                    2L));
    assertTrue(ex.getMessage().contains("saga-terminal"));

    var loaded = sagaStore.load(id, casType, TestSagaState.class);
    assertTrue(loaded.isPresent());
    assertEquals(SagaStatus.COMPLETED, loaded.get().status());
  }

  @Test
  void load_nonexistent_returns_empty() {
    assertTrue(sagaStore.load(SagaId.of("not-found"), casType, TestSagaState.class).isEmpty());
  }

  @Test
  void delete_removes_saga() {
    var id = SagaId.of("saga-order-3");
    sagaStore.create(
        id,
        SagaType.of("TestSaga"),
        new TestSagaState(SagaStatus.RUNNING, "order-3"),
        SagaStatus.RUNNING);
    sagaStore.delete(id, SagaType.of("TestSaga"));
    assertTrue(sagaStore.load(id, casType, TestSagaState.class).isEmpty());
  }

  @Test
  void delete_nonexistent_is_noop() {
    assertDoesNotThrow(() -> sagaStore.delete(SagaId.of("never-saved"), SagaType.of("TestSaga")));
  }

  @Test
  void constructor_rejects_null_datasource() {
    assertThrows(
        IllegalArgumentException.class, () -> new PostgresSagaStore(null, new ObjectMapper()));
  }

  @Test
  void constructor_rejects_null_object_mapper() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresSagaStore(dataSource, null));
  }

  @Test
  void findTimedOut_excludes_faulted_sagas() {
    var running = SagaId.of("saga-running-to");
    var faulted = SagaId.of("saga-faulted-to");
    var type = SagaType.of("TimeoutSaga");
    sagaStore.create(
        running, type, new TestSagaState(SagaStatus.RUNNING, "order-r"), SagaStatus.RUNNING);
    sagaStore.create(
        faulted, type, new TestSagaState(SagaStatus.RUNNING, "order-f"), SagaStatus.FAULTED);

    List<SagaId> timedOut = sagaStore.findTimedOut(type, Instant.now().plusSeconds(60), 10);

    // FAULTED is halted — excluded from timeout sweeps; RUNNING is included
    assertThat(timedOut).containsExactly(running);
  }

  @Test
  void findTimedOut_returnsExactlyTheNonHaltedStatuses() {
    // Positive-set pin: seed one saga per ALL 7 SagaStatus values (same type, old updatedAt) and
    // assert findTimedOut returns exactly the STARTED/RUNNING/COMPENSATING ids. Today's other
    // findTimedOut tests only assert exclusions; this pins the positive IN-list predicate against
    // the full enum partition (see SagaStatusTest#activeStatuses_areExactlyTheNonHaltedSet, and the
    // idx_saga_state_created_timeout partial-index predicate, which must match the forward
    // STARTED/RUNNING set exactly).
    var type = SagaType.of("AllStatusesTimeoutSaga");

    var started = SagaId.of("saga-started-allstatus");
    var running = SagaId.of("saga-running-allstatus");
    var compensating = SagaId.of("saga-compensating-allstatus");
    var completed = SagaId.of("saga-completed-allstatus");
    var compensated = SagaId.of("saga-compensated-allstatus");
    var failed = SagaId.of("saga-failed-allstatus");
    var faulted = SagaId.of("saga-faulted-allstatus");

    sagaStore.create(
        started, type, new TestSagaState(SagaStatus.STARTED, "o1"), SagaStatus.STARTED);
    sagaStore.create(
        running, type, new TestSagaState(SagaStatus.RUNNING, "o2"), SagaStatus.RUNNING);
    sagaStore.create(
        compensating,
        type,
        new TestSagaState(SagaStatus.COMPENSATING, "o3"),
        SagaStatus.COMPENSATING);
    sagaStore.create(
        completed, type, new TestSagaState(SagaStatus.COMPLETED, "o4"), SagaStatus.COMPLETED);
    sagaStore.create(
        compensated, type, new TestSagaState(SagaStatus.COMPENSATED, "o5"), SagaStatus.COMPENSATED);
    sagaStore.create(failed, type, new TestSagaState(SagaStatus.FAILED, "o6"), SagaStatus.FAILED);
    sagaStore.create(
        faulted, type, new TestSagaState(SagaStatus.FAULTED, "o7"), SagaStatus.FAULTED);

    List<SagaId> timedOut = sagaStore.findTimedOut(type, Instant.now().plusSeconds(60), 10);

    assertThat(timedOut).containsExactlyInAnyOrder(started, running, compensating);
  }

  @Test
  void load_returns_persisted_status_even_when_it_differs_from_state_status() {
    var sagaId = SagaId.of("saga-status-divergence");
    var sagaType = SagaType.of("TestSaga");
    // state.status() == RUNNING, but the framework persists COMPENSATED
    SagaState running = new TestSaga(SagaStatus.RUNNING);
    sagaStore.create(sagaId, sagaType, running, SagaStatus.COMPENSATED); // framework override

    var loaded = sagaStore.load(sagaId, casType, TestSaga.class);
    assertThat(loaded).isPresent();
    assertThat(loaded.get().status()).isEqualTo(SagaStatus.COMPENSATED); // persisted wins
  }

  // --- findByStatus ---

  @Test
  void findByStatus_filtersTypeAndStatus_newestFirst_limited() throws Exception {
    var type = SagaType.of("StatusOrderSaga");
    var otherType = SagaType.of("StatusPaymentSaga");

    var oldest = SagaId.of("status-faulted-oldest");
    var middle = SagaId.of("status-faulted-middle");
    var newest = SagaId.of("status-faulted-newest");
    var running = SagaId.of("status-running-same-type");
    var otherFaulted = SagaId.of("status-faulted-other-type");

    sagaStore.create(oldest, type, new TestSagaState(SagaStatus.FAULTED, "o"), SagaStatus.FAULTED);
    sagaStore.create(middle, type, new TestSagaState(SagaStatus.FAULTED, "m"), SagaStatus.FAULTED);
    sagaStore.create(newest, type, new TestSagaState(SagaStatus.FAULTED, "n"), SagaStatus.FAULTED);
    sagaStore.create(running, type, new TestSagaState(SagaStatus.RUNNING, "r"), SagaStatus.RUNNING);
    sagaStore.create(
        otherFaulted, otherType, new TestSagaState(SagaStatus.FAULTED, "x"), SagaStatus.FAULTED);

    // Stagger updated_at explicitly — Postgres has no injectable clock, so we set the
    // timestamps directly to make the newest-first ordering deterministic.
    Instant base = Instant.now().truncatedTo(ChronoUnit.MICROS);
    setUpdatedAt(oldest, base.minusSeconds(120));
    setUpdatedAt(middle, base.minusSeconds(60));
    setUpdatedAt(newest, base);

    List<SagaId> result = sagaStore.findByStatus(type, SagaStatus.FAULTED, 2);

    assertThat(result).containsExactly(newest, middle);
  }

  @Test
  void findByStatus_empty_whenNoMatch() {
    var type = SagaType.of("StatusEmptySaga");
    sagaStore.create(
        SagaId.of("status-running-only"),
        type,
        new TestSagaState(SagaStatus.RUNNING, "r"),
        SagaStatus.RUNNING);

    assertThat(sagaStore.findByStatus(type, SagaStatus.FAULTED, 10)).isEmpty();
  }

  // --- findCompensating ---

  @Test
  void findCompensating_returnsOnlyCompensatingRows_withUpdatedAt() {
    var type = SagaType.of("CompSweepSaga");
    var compensating = SagaId.of("comp-sweep-compensating");
    sagaStore.create(
        compensating,
        type,
        new TestSagaState(SagaStatus.COMPENSATING, "o1"),
        SagaStatus.COMPENSATING);
    sagaStore.create(
        SagaId.of("comp-sweep-running"),
        type,
        new TestSagaState(SagaStatus.RUNNING, "o2"),
        SagaStatus.RUNNING);
    sagaStore.create(
        SagaId.of("comp-sweep-faulted"),
        type,
        new TestSagaState(SagaStatus.FAULTED, "o3"),
        SagaStatus.FAULTED);
    sagaStore.create(
        SagaId.of("comp-sweep-compensated"),
        type,
        new TestSagaState(SagaStatus.COMPENSATED, "o4"),
        SagaStatus.COMPENSATED);

    var result = sagaStore.findCompensating(type, Instant.now().plusSeconds(60), 10);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).sagaId()).isEqualTo(compensating);
    assertThat(result.get(0).updatedAt()).isNotNull();
  }

  @Test
  void findCompensating_appliesAgeCutoff_excludesRowsNewerThanCutoff() {
    var type = SagaType.of("CompCutoffSaga");
    sagaStore.create(
        SagaId.of("comp-cutoff-fresh"),
        type,
        new TestSagaState(SagaStatus.COMPENSATING, "o1"),
        SagaStatus.COMPENSATING);

    // A just-created COMPENSATING saga (updated_at = NOW) must NOT be returned for a cutoff in the
    // past — this is the guard against re-driving an episode the event path just claimed.
    assertThat(sagaStore.findCompensating(type, Instant.now().minusSeconds(60), 10)).isEmpty();
    // With a cutoff in the future it IS due.
    assertThat(sagaStore.findCompensating(type, Instant.now().plusSeconds(60), 10)).hasSize(1);
  }

  @Test
  void findCompensating_ordersOldestFirst_respectsLimit() throws Exception {
    var type = SagaType.of("CompOrderSaga");
    var oldest = SagaId.of("comp-order-oldest");
    var middle = SagaId.of("comp-order-middle");
    var newest = SagaId.of("comp-order-newest");
    for (var id : List.of(oldest, middle, newest)) {
      sagaStore.create(
          id,
          type,
          new TestSagaState(SagaStatus.COMPENSATING, id.value()),
          SagaStatus.COMPENSATING);
    }
    Instant base = Instant.now().minusSeconds(3600);
    setUpdatedAt(oldest, base);
    setUpdatedAt(middle, base.plusSeconds(10));
    setUpdatedAt(newest, base.plusSeconds(20));

    var result = sagaStore.findCompensating(type, Instant.now(), 2);

    assertThat(result)
        .extracting(org.streamrune.core.saga.SagaStore.CompensatingSaga::sagaId)
        .containsExactly(oldest, middle);
  }

  // --- Absolute (start-instant) timeout for progress sagas ---

  @Test
  void findTimedOut_multiStepRunningSaga_timesOutOnAbsoluteStartInstant_notInactivity()
      throws Exception {
    // A multi-step saga that keeps receiving events: started 2h ago (created_at), but a fresh
    // correlated event bumped updated_at only 5 min ago. With a 1h timeout (cutoff = now-1h) the
    // documented "time in a non-terminal state" contract (SagaDecider#timeout) is ABSOLUTE from the
    // saga's start, so this saga has blown its 1h SLA and MUST be selected — even though it was
    // active 5 min ago. Fails under the pre-fix inactivity (updated_at) reading.
    var type = SagaType.of("MultiStepAbsoluteSaga");
    var saga = SagaId.of("multistep-running-absolute");
    sagaStore.create(saga, type, new TestSagaState(SagaStatus.RUNNING, "o"), SagaStatus.RUNNING);
    Instant now = Instant.now();
    setCreatedAndUpdatedAt(saga, now.minus(2, ChronoUnit.HOURS), now.minus(5, ChronoUnit.MINUTES));

    List<SagaId> timedOut = sagaStore.findTimedOut(type, now.minus(1, ChronoUnit.HOURS), 10);

    assertThat(timedOut).contains(saga);
  }

  @Test
  void findTimedOut_compensatingSaga_staysInactivityBased_forCrashResume() throws Exception {
    // COMPENSATING re-pick keeps the inactivity (updated_at) semantic: a freshly-touched
    // COMPENSATING episode (claimed 5 min ago) must NOT be re-selected even though it was created
    // long ago — this is what spaces out crash-resume re-drives. But once its updated_at ages past
    // the cutoff it IS re-picked.
    var type = SagaType.of("CompensatingInactivitySaga");
    var saga = SagaId.of("compensating-inactivity");
    sagaStore.create(
        saga, type, new TestSagaState(SagaStatus.COMPENSATING, "o"), SagaStatus.COMPENSATING);
    Instant now = Instant.now();
    setCreatedAndUpdatedAt(saga, now.minus(2, ChronoUnit.HOURS), now.minus(5, ChronoUnit.MINUTES));

    // Freshly-claimed (updated 5 min ago) → NOT re-picked at a 1h cutoff.
    assertThat(sagaStore.findTimedOut(type, now.minus(1, ChronoUnit.HOURS), 10))
        .doesNotContain(saga);

    // Aged claim (updated 2h ago) → re-picked (crash-resume).
    setUpdatedAt(saga, now.minus(2, ChronoUnit.HOURS));
    assertThat(sagaStore.findTimedOut(type, now.minus(1, ChronoUnit.HOURS), 10)).contains(saga);
  }

  // --- Cross-type SagaId collision -------------------------------------------------

  /** Two distinct saga-state types with the SAME Jackson shape but different class simple names. */
  record FulfillmentState(SagaStatus status, String orderId) implements SagaState {
    @JsonCreator
    FulfillmentState(
        @JsonProperty("status") SagaStatus status, @JsonProperty("orderId") String orderId) {
      this.status = status;
      this.orderId = orderId;
    }

    @Override
    public SagaStatus status() {
      return status;
    }
  }

  record RefundState(SagaStatus status, String orderId) implements SagaState {
    @JsonCreator
    RefundState(
        @JsonProperty("status") SagaStatus status, @JsonProperty("orderId") String orderId) {
      this.status = status;
      this.orderId = orderId;
    }

    @Override
    public SagaStatus status() {
      return status;
    }
  }

  @Test
  void create_whenSameIdOwnedByDifferentType_throwsLoudCollision_notSilentOptimisticLock() {
    var sagaId = SagaId.of("order-collision-1");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    var refundType = SagaType.fromClass(RefundState.class);
    sagaStore.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "o1"),
        SagaStatus.RUNNING);

    // A DIFFERENT saga type claiming the same id must fail LOUDLY (naming both types), not be
    // swallowed as a benign OptimisticLockException the SagaRunner treats as a spurious dedup.
    assertThatThrownBy(
            () ->
                sagaStore.create(
                    sagaId,
                    refundType,
                    new RefundState(SagaStatus.RUNNING, "o1"),
                    SagaStatus.RUNNING))
        .isInstanceOf(SagaTypeCollisionException.class)
        .hasMessageContaining(fulfillmentType.value())
        .hasMessageContaining(refundType.value());
  }

  @Test
  void typeScopedLoad_ofAForeignType_returnsEmpty_soTheSecondTypeIsNotServedTheFirstTypesState() {
    var sagaId = SagaId.of("order-collision-2");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    var refundType = SagaType.fromClass(RefundState.class);
    sagaStore.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "o2"),
        SagaStatus.RUNNING);

    // Type-scoped load for the FOREIGN (refund) type must NOT return the fulfillment row —
    // otherwise
    // the refund saga silently reads (and could CAS-overwrite) the fulfillment saga's state.
    assertThat(sagaStore.load(sagaId, refundType, RefundState.class)).isEmpty();
    // The owning type still reads its own row.
    assertThat(sagaStore.load(sagaId, fulfillmentType, FulfillmentState.class)).isPresent();
  }

  @Test
  void update_ofAForeignType_isRejected_soCrossTypeStateCannotBeOverwritten() {
    var sagaId = SagaId.of("order-collision-3");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    var refundType = SagaType.fromClass(RefundState.class);
    sagaStore.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "o3"),
        SagaStatus.RUNNING);

    // A CAS update stamped with the wrong saga_type must not overwrite the owning type's row.
    assertThatThrownBy(
            () ->
                sagaStore.update(
                    sagaId,
                    refundType,
                    new RefundState(SagaStatus.COMPENSATED, "o3"),
                    SagaStatus.COMPENSATED,
                    1L))
        .isInstanceOf(OptimisticLockException.class);
    // The fulfillment row is untouched (still RUNNING at version 1).
    var owner = sagaStore.load(sagaId, fulfillmentType, FulfillmentState.class).orElseThrow();
    assertThat(owner.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(owner.version()).isEqualTo(1L);
  }

  @Test
  void typeScopedDelete_ofAForeignType_isNoOp_soCrossTypeStateCannotBeDeleted() {
    // A delete stamped with the wrong saga_type must not remove the owning type's row.
    var sagaId = SagaId.of("order-collision-delete");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    var refundType = SagaType.fromClass(RefundState.class);
    sagaStore.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "od"),
        SagaStatus.RUNNING);

    sagaStore.delete(sagaId, refundType);
    assertThat(sagaStore.load(sagaId, fulfillmentType, FulfillmentState.class)).isPresent();

    // The owning type's delete removes it.
    sagaStore.delete(sagaId, fulfillmentType);
    assertThat(sagaStore.load(sagaId, fulfillmentType, FulfillmentState.class)).isEmpty();
  }

  @Test
  void create_sameType_concurrentDedup_stillThrowsPlainOptimisticLock() {
    // A genuine same-type concurrent-create race must stay a benign OptimisticLockException (the
    // SagaRunner's start-event dedup), NOT a loud collision.
    var sagaId = SagaId.of("order-collision-4");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    sagaStore.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "o4"),
        SagaStatus.RUNNING);
    assertThatThrownBy(
            () ->
                sagaStore.create(
                    sagaId,
                    fulfillmentType,
                    new FulfillmentState(SagaStatus.RUNNING, "o4"),
                    SagaStatus.RUNNING))
        .isInstanceOf(OptimisticLockException.class)
        .isNotInstanceOf(SagaTypeCollisionException.class);
  }

  // --- every SagaId rendered into an exception message goes through LogSanitizer -----------

  /**
   * A saga id carrying a forged log line: {@link SagaId} bounds only the length (its constructor
   * also runs on the read path), so a correlated-event value can put {@code \n} and a bidi override
   * into it, and these exceptions are logged by the subscription that retries or quarantines them.
   */
  static final SagaId HOSTILE = SagaId.of("saga-1\nINFO forged entry \u202Eevil");

  private static void assertLogSafe(Throwable thrown) {
    assertThat(thrown.getMessage())
        .contains(LogSanitizer.sanitizeForLog(HOSTILE.value()))
        .doesNotContain("\n")
        .doesNotContain("\u202E");
  }

  private static DataSource unreachableDataSource() {
    return (DataSource)
        Proxy.newProxyInstance(
            PostgresSagaStoreTest.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                throw new SQLException("connection refused");
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  @Test
  void storageFailureMessages_renderTheSagaIdLogSafe() {
    var store = new PostgresSagaStore(unreachableDataSource(), objectMapper);
    List<org.junit.jupiter.api.function.Executable> calls =
        List.of(
            () -> store.create(HOSTILE, casType, casState, SagaStatus.RUNNING),
            () -> store.createGenesisPending(HOSTILE, casType, casState, SagaStatus.RUNNING, false),
            () -> store.createFaulted(HOSTILE, casType, casState),
            () -> store.update(HOSTILE, casType, casState, SagaStatus.RUNNING, 1L),
            () ->
                store.applyEvent(
                    HOSTILE,
                    casType,
                    casState,
                    SagaStatus.RUNNING,
                    1L,
                    SagaStore.AppliedEvent.live(GlobalOffset.of(1L))),
            () -> store.markFaulted(HOSTILE, casType, 1L),
            () -> store.setDeadLetterPending(HOSTILE, casType, true),
            () -> store.claimCompensating(HOSTILE, casType, casState, 1L),
            () -> store.load(HOSTILE, casType, TestSagaState.class),
            () -> store.delete(HOSTILE, casType));
    for (var call : calls) {
      assertLogSafe(assertThrows(EventStoreException.class, call));
    }
  }

  /**
   * A {@code null} saga id is a caller bug, refused with {@link IllegalArgumentException} before
   * any I/O, like {@code InMemorySagaStore} ({@code SagaStoreContract} pins it for both stores
   * against a live database). This store's pool refuses every connection and its mapper refuses
   * every write, so a check placed after the serialization or the connection borrow surfaces as a
   * {@link SagaStateSerializationException} or an {@link EventStoreException} instead — the latter
   * retried by the saga runtime as an infrastructure failure forever.
   */
  @Test
  void aNullSagaId_isRejectedBeforeSerializingOrBorrowingAConnection() {
    var store = new PostgresSagaStore(unreachableDataSource(), refusingMapper());
    List<org.junit.jupiter.api.function.Executable> calls =
        List.of(
            () -> store.create(null, casType, casState, SagaStatus.RUNNING),
            () -> store.create(null, casType, casState, SagaStatus.RUNNING, true),
            () -> store.createGenesisPending(null, casType, casState, SagaStatus.RUNNING, false),
            () -> store.createFaulted(null, casType, casState),
            () -> store.update(null, casType, casState, SagaStatus.RUNNING, 1L),
            () ->
                store.applyEvent(
                    null,
                    casType,
                    casState,
                    SagaStatus.RUNNING,
                    1L,
                    SagaStore.AppliedEvent.live(GlobalOffset.of(1L))),
            () -> store.markFaulted(null, casType, 1L),
            () -> store.setDeadLetterPending(null, casType, true),
            () -> store.claimCompensating(null, casType, casState, 1L),
            () -> store.load(null, casType, TestSagaState.class),
            () -> store.delete(null, casType));
    for (var call : calls) {
      assertThat(assertThrows(IllegalArgumentException.class, call))
          .hasMessage("sagaId must not be null");
    }
  }

  /**
   * Follow-up: every OTHER null argument is the same caller bug as a null id, refused with {@link
   * IllegalArgumentException} naming it before any I/O, like {@code InMemorySagaStore} ({@code
   * SagaStoreContract} pins it for both stores against a live database). A null status used to NPE
   * at {@code status.name()} inside the storage catch and surface as an {@link EventStoreException}
   * (retried forever by the saga runtime); a null state was serialized to the string {@code "null"}
   * and stored as a JSON {@code null}; a null cutoff, age bound or status filter failed at the JDBC
   * bind. This store's pool refuses every connection and its mapper refuses every write, so a check
   * placed after the serialization or the connection borrow surfaces as a {@link
   * SagaStateSerializationException} or an {@link EventStoreException} instead of the {@link
   * IllegalArgumentException} asserted here.
   */
  @Test
  void everyOtherNullArgument_isRejectedBeforeSerializingOrBorrowingAConnection() {
    var store = new PostgresSagaStore(unreachableDataSource(), refusingMapper());
    var applied = SagaStore.AppliedEvent.live(GlobalOffset.of(1L));
    Instant cutoff = Instant.now();
    record Call(String argument, org.junit.jupiter.api.function.Executable call) {}
    List<Call> calls =
        List.of(
            new Call("state", () -> store.create(casId, casType, null, SagaStatus.RUNNING)),
            new Call("state", () -> store.create(casId, casType, null, SagaStatus.RUNNING, true)),
            new Call(
                "state",
                () -> store.createGenesisPending(casId, casType, null, SagaStatus.RUNNING, true)),
            new Call("state", () -> store.createFaulted(casId, casType, null)),
            new Call("state", () -> store.update(casId, casType, null, SagaStatus.RUNNING, 1L)),
            new Call(
                "state",
                () -> store.applyEvent(casId, casType, null, SagaStatus.RUNNING, 1L, applied)),
            new Call("state", () -> store.claimCompensating(casId, casType, null, 1L)),
            new Call("status", () -> store.create(casId, casType, casState, null)),
            new Call("status", () -> store.create(casId, casType, casState, null, true)),
            new Call(
                "status", () -> store.createGenesisPending(casId, casType, casState, null, true)),
            new Call("status", () -> store.update(casId, casType, casState, null, 1L)),
            new Call("status", () -> store.applyEvent(casId, casType, casState, null, 1L, applied)),
            new Call(
                "applied",
                () -> store.applyEvent(casId, casType, casState, SagaStatus.RUNNING, 1L, null)),
            new Call("stateType", () -> store.load(casId, casType, null)),
            new Call("cutoff", () -> store.findTimedOut(casType, null, 10)),
            new Call("status", () -> store.findByStatus(casType, null, 10)),
            new Call("status", () -> store.countByStatus(casType, null)),
            new Call("updatedBefore", () -> store.findCompensating(casType, null, 10)));
    for (var c : calls) {
      assertThat(assertThrows(IllegalArgumentException.class, c.call()))
          .hasMessage(c.argument() + " must not be null");
    }
    // The still-valid arguments of the same calls reach the (refused) connection: the guards above
    // reject on the null argument alone, not on the fixture.
    assertThrows(
        EventStoreException.class,
        () -> store.findTimedOut(casType, cutoff, 10),
        "a non-null cutoff is not refused by the guard");
  }

  /** A mapper whose every write fails, as a deterministic state-conversion failure. */
  private static ObjectMapper refusingMapper() {
    return new ObjectMapper() {
      @Override
      public String writeValueAsString(Object value) throws JsonProcessingException {
        throw new JsonMappingException(null, "cannot serialize");
      }
    };
  }

  @Test
  void stateConversionFailureMessages_renderTheSagaIdLogSafe() {
    var store = new PostgresSagaStore(dataSource, refusingMapper());
    assertLogSafe(
        assertThrows(
            SagaStateSerializationException.class,
            () -> store.create(HOSTILE, casType, casState, SagaStatus.RUNNING)));

    sagaStore.create(HOSTILE, casType, casState, SagaStatus.RUNNING);
    // TestSaga has no "status"/"orderId" properties: the stored JSON cannot be mapped onto it.
    assertLogSafe(
        assertThrows(
            SagaStateSerializationException.class,
            () -> sagaStore.load(HOSTILE, casType, TestSaga.class)));
  }

  @Test
  void conflictMessages_renderTheSagaIdLogSafe() {
    sagaStore.create(HOSTILE, casType, casState, SagaStatus.RUNNING); // version 1
    assertLogSafe(
        assertThrows(
            OptimisticLockException.class,
            () -> sagaStore.create(HOSTILE, casType, casState, SagaStatus.RUNNING)));
    assertLogSafe(
        assertThrows(
            SagaTypeCollisionException.class,
            () ->
                sagaStore.create(HOSTILE, SagaType.of("OtherSaga"), casState, SagaStatus.RUNNING)));
    assertLogSafe(
        assertThrows(
            OptimisticLockException.class,
            () -> sagaStore.update(HOSTILE, casType, casState, SagaStatus.RUNNING, 9L)));
    assertLogSafe(
        assertThrows(
            OptimisticLockException.class,
            () ->
                sagaStore.applyEvent(
                    HOSTILE,
                    casType,
                    casState,
                    SagaStatus.RUNNING,
                    9L,
                    SagaStore.AppliedEvent.live(GlobalOffset.of(1L)))));
    assertLogSafe(
        assertThrows(
            OptimisticLockException.class, () -> sagaStore.markFaulted(HOSTILE, casType, 9L)));
    assertLogSafe(
        assertThrows(
            OptimisticLockException.class,
            () -> sagaStore.claimCompensating(HOSTILE, casType, casState, 9L)));
  }

  private void setUpdatedAt(SagaId sagaId, Instant updatedAt) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("UPDATE saga_state SET updated_at = ? WHERE saga_id = ?")) {
      ps.setTimestamp(1, java.sql.Timestamp.from(updatedAt));
      ps.setString(2, sagaId.value());
      ps.executeUpdate();
    }
  }

  private void setCreatedAndUpdatedAt(SagaId sagaId, Instant createdAt, Instant updatedAt)
      throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE saga_state SET created_at = ?, updated_at = ? WHERE saga_id = ?")) {
      ps.setTimestamp(1, java.sql.Timestamp.from(createdAt));
      ps.setTimestamp(2, java.sql.Timestamp.from(updatedAt));
      ps.setString(3, sagaId.value());
      ps.executeUpdate();
    }
  }
}
