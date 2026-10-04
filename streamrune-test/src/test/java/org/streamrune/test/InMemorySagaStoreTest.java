package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaTypeCollisionException;
import org.streamrune.core.types.SagaType;

class InMemorySagaStoreTest {

  record TestState(SagaStatus status, String data) implements SagaState {
    @Override
    public SagaStatus status() {
      return status;
    }
  }

  record TimedState(SagaId sagaId, SagaStatus status) implements SagaState {}

  InMemorySagaStore store;
  SagaId casId = SagaId.of("saga-1");
  SagaType casType = SagaType.of("TestSaga");
  TestState casState = new TestState(SagaStatus.RUNNING, "data");

  @BeforeEach
  void setUp() {
    store = new InMemorySagaStore();
  }

  // --- create + load ---

  @Test
  void create_thenLoad_returnsVersion1() {
    store.create(casId, casType, casState, SagaStatus.RUNNING);
    assertEquals(1L, store.load(casId, casType, TestState.class).orElseThrow().version());
  }

  @Test
  void create_whenAlreadyExists_throwsOptimisticLock() {
    store.create(casId, casType, casState, SagaStatus.RUNNING);
    assertThrows(
        OptimisticLockException.class,
        () -> store.create(casId, casType, casState, SagaStatus.RUNNING));
  }

  // --- update CAS ---

  @Test
  void update_withCurrentVersion_succeeds_andIncrements() {
    store.create(casId, casType, casState, SagaStatus.RUNNING); // v1
    store.update(casId, casType, casState, SagaStatus.RUNNING, 1L); // → v2
    assertEquals(2L, store.load(casId, casType, TestState.class).orElseThrow().version());
  }

  @Test
  void update_withStaleVersion_throwsOptimisticLock() {
    store.create(casId, casType, casState, SagaStatus.RUNNING); // v1
    store.update(casId, casType, casState, SagaStatus.RUNNING, 1L); // v2
    assertThrows(
        OptimisticLockException.class,
        () -> store.update(casId, casType, casState, SagaStatus.RUNNING, 1L)); // stale
  }

  @Test
  void update_onTerminalRow_throwsOptimisticLock_regardlessOfVersion() {
    store.create(casId, casType, casState, SagaStatus.RUNNING);
    store.update(casId, casType, casState, SagaStatus.COMPENSATED, 1L); // terminal, v2
    assertThrows(
        OptimisticLockException.class,
        () ->
            store.update(
                casId, casType, casState, SagaStatus.RUNNING, 2L)); // terminal-guard parity
  }

  @Test
  void update_onFaultedRow_succeeds() { // FAULTED writable (Slice-2 replay)
    store.create(casId, casType, casState, SagaStatus.FAULTED);
    store.update(casId, casType, casState, SagaStatus.RUNNING, 1L);
    assertEquals(
        SagaStatus.RUNNING, store.load(casId, casType, TestState.class).orElseThrow().status());
  }

  @Test
  void update_onMissingRow_throwsOptimisticLock() {
    assertThrows(
        OptimisticLockException.class,
        () -> store.update(casId, casType, casState, SagaStatus.RUNNING, 1L));
  }

  // --- existing tests (save → create / update) ---

  @Test
  void create_then_load_returns_state() {
    var state = new TestState(SagaStatus.RUNNING, "some-data");

    store.create(casId, SagaType.of("TestSaga"), state, SagaStatus.RUNNING);

    Optional<LoadedSaga<TestState>> loaded = store.load(casId, casType, TestState.class);
    assertTrue(loaded.isPresent());
    assertEquals(SagaStatus.RUNNING, loaded.get().status());
    assertEquals("some-data", loaded.get().state().data());
  }

  @Test
  void update_overwrites() {
    store.create(
        casId,
        SagaType.of("TestSaga"),
        new TestState(SagaStatus.STARTED, "v1"),
        SagaStatus.STARTED);
    store.update(
        casId,
        SagaType.of("TestSaga"),
        new TestState(SagaStatus.RUNNING, "v2"),
        SagaStatus.RUNNING,
        1L);

    Optional<LoadedSaga<TestState>> loaded = store.load(casId, casType, TestState.class);
    assertTrue(loaded.isPresent());
    assertEquals(SagaStatus.RUNNING, loaded.get().status());
    assertEquals("v2", loaded.get().state().data());
  }

  @Test
  void load_nonexistent_returns_empty() {
    Optional<LoadedSaga<TestState>> loaded =
        store.load(SagaId.of("not-found"), casType, TestState.class);
    assertTrue(loaded.isEmpty());
  }

  @Test
  void delete_removes_saga() {
    store.create(
        casId, SagaType.of("TestSaga"), new TestState(SagaStatus.RUNNING, "d"), SagaStatus.RUNNING);
    store.delete(casId, SagaType.of("TestSaga"));
    assertTrue(store.load(casId, casType, TestState.class).isEmpty());
  }

  @Test
  void delete_nonexistent_is_noop() {
    assertDoesNotThrow(() -> store.delete(SagaId.of("never-saved"), SagaType.of("TestSaga")));
  }

  @Test
  void findTimedOutReturnsNonTerminalSagasUpdatedBeforeCutoff() {
    var s = new InMemorySagaStore();
    s.create(
        SagaId.of("s1"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("s1"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);
    s.create(
        SagaId.of("s2"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("s2"), SagaStatus.COMPLETED),
        SagaStatus.COMPLETED);
    s.create(
        SagaId.of("s3"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("s3"), SagaStatus.STARTED),
        SagaStatus.STARTED);
    s.create(
        SagaId.of("s4"),
        SagaType.of("PaymentSaga"),
        new TimedState(SagaId.of("s4"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    // All saved "just now" — cutoff in future catches them all
    List<SagaId> timedOut =
        s.findTimedOut(SagaType.of("OrderSaga"), Instant.now().plusSeconds(60), 10);

    // s2 is COMPLETED (terminal) → excluded; s4 is different type → excluded
    assertEquals(2, timedOut.size());
    assertTrue(timedOut.contains(SagaId.of("s1")));
    assertTrue(timedOut.contains(SagaId.of("s3")));
  }

  @Test
  void findTimedOutRespectsLimit() {
    var s = new InMemorySagaStore();
    s.create(
        SagaId.of("s1"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("s1"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);
    s.create(
        SagaId.of("s2"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("s2"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);
    s.create(
        SagaId.of("s3"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("s3"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    List<SagaId> timedOut =
        s.findTimedOut(SagaType.of("OrderSaga"), Instant.now().plusSeconds(60), 2);
    assertEquals(2, timedOut.size());
  }

  @Test
  void findTimedOutExcludesAllTerminalStatuses() {
    var s = new InMemorySagaStore();
    s.create(
        SagaId.of("s1"),
        SagaType.of("Saga"),
        new TimedState(SagaId.of("s1"), SagaStatus.COMPLETED),
        SagaStatus.COMPLETED);
    s.create(
        SagaId.of("s2"),
        SagaType.of("Saga"),
        new TimedState(SagaId.of("s2"), SagaStatus.COMPENSATED),
        SagaStatus.COMPENSATED);
    s.create(
        SagaId.of("s3"),
        SagaType.of("Saga"),
        new TimedState(SagaId.of("s3"), SagaStatus.FAILED),
        SagaStatus.FAILED);

    List<SagaId> timedOut = s.findTimedOut(SagaType.of("Saga"), Instant.now().plusSeconds(60), 10);
    assertTrue(timedOut.isEmpty());
  }

  @Test
  void create_rejects_null_sagaId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            store.create(
                null,
                SagaType.of("TestSaga"),
                new TestState(SagaStatus.RUNNING, "data"),
                SagaStatus.RUNNING));
  }

  @Test
  void create_rejects_null_state() {
    assertThrows(
        IllegalArgumentException.class,
        () -> store.create(SagaId.of("saga-1"), SagaType.of("TestSaga"), null, SagaStatus.RUNNING));
  }

  @Test
  void create_rejects_null_status() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            store.create(
                SagaId.of("saga-1"),
                SagaType.of("TestSaga"),
                new TestState(SagaStatus.RUNNING, "data"),
                null));
  }

  @Test
  void constructor_rejects_null_clock() {
    assertThrows(NullPointerException.class, () -> new InMemorySagaStore(null));
  }

  @Test
  void saga_timeouts_are_testable_with_a_mutable_clock_without_sleeping() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = new InMemorySagaStore(clock);
    s.create(
        SagaId.of("stale"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("stale"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);
    clock.advance(Duration.ofMinutes(31));
    s.create(
        SagaId.of("fresh"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("fresh"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    // Runner-style cutoff: now minus a 30-minute timeout. Only the saga last
    // updated before the cutoff has timed out.
    Instant cutoff = clock.instant().minus(Duration.ofMinutes(30));
    assertEquals(List.of(SagaId.of("stale")), s.findTimedOut(SagaType.of("OrderSaga"), cutoff, 10));
  }

  @Test
  void update_doesNotResetTheAbsoluteTimeout_ofARunningSaga() {
    // Resaving a RUNNING saga (a progress event) advances updated_at but MUST NOT
    // reset
    // its absolute start-instant deadline — otherwise a multi-step saga that keeps receiving events
    // never times out (a past bug). Started at T0, resaved at T0+31m; at that same instant a
    // 30m timeout (cutoff T0+1m) already deems it timed out, because created_at (T0) is the
    // reference for STARTED/RUNNING.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = new InMemorySagaStore(clock);
    var id = SagaId.of("active");
    s.create(
        id, SagaType.of("OrderSaga"), new TimedState(id, SagaStatus.RUNNING), SagaStatus.RUNNING);
    clock.advance(Duration.ofMinutes(31));
    s.update(
        id,
        SagaType.of("OrderSaga"),
        new TimedState(id, SagaStatus.RUNNING),
        SagaStatus.RUNNING,
        1L); // progress → updatedAt now, but the absolute deadline is unchanged

    Instant cutoff = clock.instant().minus(Duration.ofMinutes(30));
    assertEquals(List.of(id), s.findTimedOut(SagaType.of("OrderSaga"), cutoff, 10));
  }

  @Test
  void compensatingSaga_reSelectsOnInactivity_afterAResaveSpacesItOut() {
    // The COMPENSATING crash-resume re-pick keeps the inactivity (updated_at) semantic: a freshly
    // claimed/resaved COMPENSATING episode is NOT re-selected until it ages past the cutoff, so the
    // sweeper/timeout runner space out re-drives instead of tight-looping.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = new InMemorySagaStore(clock);
    var id = SagaId.of("compensating");
    s.create(
        id,
        SagaType.of("OrderSaga"),
        new TimedState(id, SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING);
    clock.advance(Duration.ofMinutes(31));
    s.update(
        id,
        SagaType.of("OrderSaga"),
        new TimedState(id, SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING,
        1L); // a claim/resave bumps updated_at to T0+31m

    // Freshly resaved (updated_at T0+31m) → NOT due at a 30m cutoff (T0+1m).
    Instant cutoff = clock.instant().minus(Duration.ofMinutes(30));
    assertTrue(s.findTimedOut(SagaType.of("OrderSaga"), cutoff, 10).isEmpty());

    // Once it ages (advance so updated_at is older than the cutoff) it IS re-picked.
    clock.advance(Duration.ofMinutes(31));
    Instant laterCutoff = clock.instant().minus(Duration.ofMinutes(30));
    assertEquals(List.of(id), s.findTimedOut(SagaType.of("OrderSaga"), laterCutoff, 10));
  }

  @Test
  void findTimedOut_runningSaga_absoluteFromStart_survivesActivity() {
    // A multi-step RUNNING saga that keeps receiving events must still time out on its
    // ABSOLUTE start-instant deadline (SagaDecider#timeout = "time in a non-terminal state"), not
    // be
    // kept perpetually fresh by activity. Started at T0, an event bumps updated_at at T0+45m; at
    // T0+90m with a 60m timeout (cutoff T0+30m) it has blown its SLA and MUST be selected. Fails
    // under the pre-fix inactivity (updated_at) reading (updated_at T0+45m is after cutoff T0+30m).
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = new InMemorySagaStore(clock);
    var id = SagaId.of("multistep");
    s.create(
        id, SagaType.of("OrderSaga"), new TimedState(id, SagaStatus.RUNNING), SagaStatus.RUNNING);
    clock.advance(Duration.ofMinutes(45));
    s.update(
        id,
        SagaType.of("OrderSaga"),
        new TimedState(id, SagaStatus.RUNNING),
        SagaStatus.RUNNING,
        1L); // a correlated event bumps updated_at to T0+45m
    clock.advance(Duration.ofMinutes(45)); // now = T0+90m

    Instant cutoff = clock.instant().minus(Duration.ofMinutes(60));
    assertEquals(List.of(id), s.findTimedOut(SagaType.of("OrderSaga"), cutoff, 10));
  }

  @Test
  void findTimedOutExcludesFaultedSagas() {
    var s = new InMemorySagaStore();
    s.create(
        SagaId.of("running"),
        SagaType.of("Saga"),
        new TimedState(SagaId.of("running"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);
    s.create(
        SagaId.of("faulted"),
        SagaType.of("Saga"),
        new TimedState(SagaId.of("faulted"), SagaStatus.RUNNING),
        SagaStatus.FAULTED);

    List<SagaId> timedOut = s.findTimedOut(SagaType.of("Saga"), Instant.now().plusSeconds(60), 10);

    // FAULTED is halted (not timed-out-eligible) — only the RUNNING saga returned
    assertEquals(List.of(SagaId.of("running")), timedOut);
  }

  @Test
  void load_returns_persisted_status_even_when_it_differs_from_state_status() {
    var id = SagaId.of("saga-status-divergence");
    var type = SagaType.of("TestSaga");
    // State says RUNNING; we persist COMPENSATED (framework override)
    var runningState = new TestState(SagaStatus.RUNNING, "data");
    store.create(id, type, runningState, SagaStatus.COMPENSATED);

    var loaded = store.load(id, type, TestState.class);
    assertTrue(loaded.isPresent());
    // state.status() is still RUNNING
    assertEquals(SagaStatus.RUNNING, loaded.get().state().status());
    // but persisted (authoritative) status is COMPENSATED
    assertEquals(SagaStatus.COMPENSATED, loaded.get().status());
  }

  // --- findByStatus ---

  @Test
  void findByStatus_filtersTypeAndStatus_newestFirst_limited() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = new InMemorySagaStore(clock);
    var type = SagaType.of("OrderSaga");
    var otherType = SagaType.of("PaymentSaga");

    var oldest = SagaId.of("faulted-oldest");
    s.create(oldest, type, new TimedState(oldest, SagaStatus.FAULTED), SagaStatus.FAULTED);
    clock.advance(Duration.ofMinutes(1));

    var middle = SagaId.of("faulted-middle");
    s.create(middle, type, new TimedState(middle, SagaStatus.FAULTED), SagaStatus.FAULTED);
    clock.advance(Duration.ofMinutes(1));

    var newest = SagaId.of("faulted-newest");
    s.create(newest, type, new TimedState(newest, SagaStatus.FAULTED), SagaStatus.FAULTED);
    clock.advance(Duration.ofMinutes(1));

    var running = SagaId.of("running-same-type");
    s.create(running, type, new TimedState(running, SagaStatus.RUNNING), SagaStatus.RUNNING);

    var otherFaulted = SagaId.of("faulted-other-type");
    s.create(
        otherFaulted,
        otherType,
        new TimedState(otherFaulted, SagaStatus.FAULTED),
        SagaStatus.FAULTED);

    List<SagaId> result = s.findByStatus(type, SagaStatus.FAULTED, 2);

    assertEquals(List.of(newest, middle), result);
  }

  @Test
  void findByStatus_empty_whenNoMatch() {
    var s = new InMemorySagaStore();
    s.create(
        SagaId.of("running-1"),
        SagaType.of("OrderSaga"),
        new TimedState(SagaId.of("running-1"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    assertTrue(s.findByStatus(SagaType.of("OrderSaga"), SagaStatus.FAULTED, 10).isEmpty());
  }

  // --- Cross-type SagaId collision -------------------------------------------------

  /** Two distinct saga-state types that could derive the same SagaId from one business key. */
  record FulfillmentState(SagaStatus status, String orderId) implements SagaState {}

  record RefundState(SagaStatus status, String orderId) implements SagaState {}

  @Test
  void create_whenSameIdOwnedByDifferentType_throwsLoudCollision_notSilentOptimisticLock() {
    var sagaId = SagaId.of("order-collision-1");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    var refundType = SagaType.fromClass(RefundState.class);
    store.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "o1"),
        SagaStatus.RUNNING);

    assertThrows(
        SagaTypeCollisionException.class,
        () ->
            store.create(
                sagaId, refundType, new RefundState(SagaStatus.RUNNING, "o1"), SagaStatus.RUNNING));
  }

  @Test
  void typeScopedLoad_ofAForeignType_returnsEmpty() {
    var sagaId = SagaId.of("order-collision-2");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    var refundType = SagaType.fromClass(RefundState.class);
    store.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "o2"),
        SagaStatus.RUNNING);

    assertTrue(store.load(sagaId, refundType, RefundState.class).isEmpty());
    assertTrue(store.load(sagaId, fulfillmentType, FulfillmentState.class).isPresent());
  }

  @Test
  void typeScopedDelete_ofAForeignType_isNoOp_ownerTypeRemoves() {
    var sagaId = SagaId.of("order-collision-4");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    var refundType = SagaType.fromClass(RefundState.class);
    store.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "o4"),
        SagaStatus.RUNNING);

    // A foreign type's delete must NOT remove another type's row.
    store.delete(sagaId, refundType);
    assertTrue(store.load(sagaId, fulfillmentType, FulfillmentState.class).isPresent());

    // The owning type's delete removes it.
    store.delete(sagaId, fulfillmentType);
    assertTrue(store.load(sagaId, fulfillmentType, FulfillmentState.class).isEmpty());
  }

  @Test
  void update_ofAForeignType_isRejected() {
    var sagaId = SagaId.of("order-collision-3");
    var fulfillmentType = SagaType.fromClass(FulfillmentState.class);
    var refundType = SagaType.fromClass(RefundState.class);
    store.create(
        sagaId,
        fulfillmentType,
        new FulfillmentState(SagaStatus.RUNNING, "o3"),
        SagaStatus.RUNNING);

    assertThrows(
        OptimisticLockException.class,
        () ->
            store.update(
                sagaId,
                refundType,
                new RefundState(SagaStatus.COMPENSATED, "o3"),
                SagaStatus.COMPENSATED,
                1L));
    var owner = store.load(sagaId, fulfillmentType, FulfillmentState.class).orElseThrow();
    assertEquals(SagaStatus.RUNNING, owner.status());
    assertEquals(1L, owner.version());
  }
}
