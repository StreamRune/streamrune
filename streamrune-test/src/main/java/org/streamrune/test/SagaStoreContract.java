package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

/**
 * The recorded-recovery-state contract every {@link SagaStore} must honour. Run by {@code
 * InMemorySagaStoreContractTest} and {@code PostgresSagaStoreContractTest}: a store that passes
 * here can drive {@code SagaStepExecutor}, {@code SagaRunner} and {@code SagaDeadLetterReplayer};
 * the two shipped stores can no longer diverge unnoticed.
 */
public abstract class SagaStoreContract {

  /** A fresh, empty store for each test. */
  protected abstract SagaStore newStore();

  public record ContractState(SagaStatus status, String marker) implements SagaState {
    @JsonCreator
    public ContractState(
        @JsonProperty("status") SagaStatus status, @JsonProperty("marker") String marker) {
      this.status = status;
      this.marker = marker;
    }
  }

  protected static final SagaType TYPE = SagaType.fromClass(ContractState.class);
  protected static final SagaId ID = SagaId.of("contract-saga-1");
  protected static final ContractState INITIAL = new ContractState(SagaStatus.STARTED, "initial");
  protected static final ContractState EVOLVED = new ContractState(SagaStatus.RUNNING, "evolved");

  protected SagaStore store;

  @BeforeEach
  void freshStore() {
    store = newStore();
  }

  protected LoadedSaga<ContractState> row() {
    return store.load(ID, TYPE, ContractState.class).orElseThrow();
  }

  @Test
  public void create_isACompleteRow_genesisApplied_noFault_noShield_noOffsets() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    var row = row();
    assertEquals(1L, row.version());
    assertTrue(row.genesisApplied());
    assertNull(row.preFaultStatus());
    assertFalse(row.deadLetterPending());
    assertNull(row.lastAppliedOffset());
    assertNull(row.lastReplayedOffset());
    assertEquals(SagaStatus.RUNNING, row.effectiveStatus());
  }

  @Test
  public void createGenesisPending_isVersion1_withGenesisNotApplied() {
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, false);
    var row = row();
    assertEquals(1L, row.version());
    assertFalse(row.genesisApplied());
    assertEquals(SagaStatus.RUNNING, row.status());
    assertEquals("evolved", row.state().marker());
    assertThrows(
        OptimisticLockException.class,
        () -> store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, false));
  }

  /**
   * A saga may already own dead-letter entries before its row exists (the row-less residual); the
   * genesis-path creates carry the shield so the row is born shielded in the same statement — no
   * crash point between "row exists" and "row shielded".
   */
  @Test
  public void createGenesisPending_withTheShield_isBornShielded_inOneStatement() {
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, true);
    var row = row();
    assertTrue(row.deadLetterPending(), "born shielded: the saga already owns entries");
    assertFalse(row.genesisApplied());
    assertNull(row.preFaultStatus());
    assertEquals(1L, row.version());
  }

  @Test
  public void create_withTheShield_isBornShielded_andComplete() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.COMPLETED, true);
    var row = row();
    assertTrue(row.deadLetterPending(), "the DONE-at-genesis create carries the shield too");
    assertTrue(row.genesisApplied());
    assertEquals(SagaStatus.COMPLETED, row.status());
    assertEquals(1L, row.version());
  }

  @Test
  public void createFaulted_isFaultedAtVersion1_genesisPending_shielded_noPreFault() {
    store.createFaulted(ID, TYPE, INITIAL);
    var row = row();
    assertEquals(SagaStatus.FAULTED, row.status());
    assertEquals(1L, row.version());
    assertEquals("initial", row.state().marker(), "createFaulted persists initialState verbatim");
    assertFalse(row.genesisApplied());
    assertNull(row.preFaultStatus());
    assertTrue(row.deadLetterPending());
    assertNull(row.effectiveStatus());
    assertThrows(OptimisticLockException.class, () -> store.createFaulted(ID, TYPE, INITIAL));
  }

  @Test
  public void applyEvent_startPath_setsGenesisApplied_andLastAppliedOffset() {
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, false);
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.RUNNING, 1L, AppliedEvent.liveStart(GlobalOffset.of(10L)));
    var row = row();
    assertEquals(2L, row.version());
    assertTrue(row.genesisApplied());
    assertEquals(10L, row.lastAppliedOffset());
    assertNull(row.lastReplayedOffset());
  }

  @Test
  public void applyEvent_liveOffsetIsAMaximum_replayOffsetIsExact() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.RUNNING, 1L, AppliedEvent.live(GlobalOffset.of(10L)));
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.RUNNING, 2L, AppliedEvent.replayed(GlobalOffset.of(7L)));
    var row = row();
    assertEquals(3L, row.version());
    assertEquals(
        10L, row.lastAppliedOffset(), "a replay of an older entry never lowers the live maximum");
    assertEquals(
        7L, row.lastReplayedOffset(), "the replay column records exactly the last replay feed");
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.RUNNING, 3L, AppliedEvent.live(GlobalOffset.of(12L)));
    assertEquals(7L, row().lastReplayedOffset(), "a live apply leaves the replay column alone");
    assertEquals(12L, row().lastAppliedOffset());
  }

  @Test
  public void applyEvent_clearsTheFault_andRecordsTheNewStatus() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    assertTrue(store.markFaulted(ID, TYPE, 1L));
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.RUNNING, 2L, AppliedEvent.replayed(GlobalOffset.of(3L)));
    var row = row();
    assertEquals(SagaStatus.RUNNING, row.status());
    assertNull(row.preFaultStatus());
    assertEquals(3L, row.version());
  }

  @Test
  public void update_clearsPreFault_keepsGenesisAndOffsets() {
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, false);
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.RUNNING, 1L, AppliedEvent.liveStart(GlobalOffset.of(5L)));
    assertTrue(store.markFaulted(ID, TYPE, 2L));
    store.update(ID, TYPE, EVOLVED, SagaStatus.COMPENSATED, 3L);
    var row = row();
    assertEquals(SagaStatus.COMPENSATED, row.status());
    assertNull(row.preFaultStatus());
    assertTrue(row.genesisApplied());
    assertEquals(5L, row.lastAppliedOffset());
  }

  @Test
  public void claimCompensating_clearsPreFault_stampsEpisode_keepsGenesisFlag() {
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, false);
    assertTrue(store.markFaulted(ID, TYPE, 1L));
    store.claimCompensating(ID, TYPE, EVOLVED, 2L);
    var row = row();
    assertEquals(SagaStatus.COMPENSATING, row.status());
    assertNull(row.preFaultStatus());
    assertFalse(row.genesisApplied(), "a compensation claim never applies the genesis");
    assertEquals(3L, row.episodeVersion());
    assertNotNull(row.episodeClaimedAt());
  }

  @Test
  public void markFaulted_recordsThePreFaultStatus_bumpsVersion_returnsTrue() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    store.claimCompensating(ID, TYPE, EVOLVED, 1L);
    assertTrue(store.markFaulted(ID, TYPE, 2L));
    var row = row();
    assertEquals(SagaStatus.FAULTED, row.status());
    assertEquals(SagaStatus.COMPENSATING, row.preFaultStatus());
    assertEquals(SagaStatus.COMPENSATING, row.effectiveStatus());
    assertEquals(3L, row.version());
    assertEquals("evolved", row.state().marker(), "markFaulted writes no state");
  }

  @Test
  public void markFaulted_onAGenesisPendingRow_recordsNoPreFaultStatus() {
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, false);
    assertTrue(store.markFaulted(ID, TYPE, 1L));
    var row = row();
    assertEquals(SagaStatus.FAULTED, row.status());
    assertNull(row.preFaultStatus(), "a genesis-pending fault has nothing to resume");
    assertNull(row.effectiveStatus());
    assertFalse(row.genesisApplied());
  }

  @Test
  public void markFaulted_onACompensatingGenesisPendingRow_recordsCompensating() {
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, false);
    store.claimCompensating(ID, TYPE, EVOLVED, 1L); // deterministic genesis failure / timeout claim
    assertTrue(store.markFaulted(ID, TYPE, 2L));
    var row = row();
    assertEquals(
        SagaStatus.COMPENSATING,
        row.preFaultStatus(),
        "a claimed episode resumes, whatever the genesis flag says");
    assertEquals(SagaStatus.COMPENSATING, row.effectiveStatus());
    assertFalse(row.genesisApplied());
    assertFalse(store.markFaulted(ID, TYPE, 3L), "a still-poison refresh keeps COMPENSATING");
    assertEquals(SagaStatus.COMPENSATING, row().preFaultStatus());
  }

  @Test
  public void markFaulted_onAFaultedRow_keepsPreFault_bumpsVersion_returnsFalse() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    assertTrue(store.markFaulted(ID, TYPE, 1L));
    assertFalse(store.markFaulted(ID, TYPE, 2L), "a still-poison refresh is not a transition");
    var row = row();
    assertEquals(SagaStatus.RUNNING, row.preFaultStatus());
    assertEquals(3L, row.version());
  }

  @Test
  public void markFaulted_staleVersionOrTerminalRow_throwsOptimisticLock() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    assertThrows(OptimisticLockException.class, () -> store.markFaulted(ID, TYPE, 5L));
    store.update(ID, TYPE, EVOLVED, SagaStatus.COMPLETED, 1L);
    assertThrows(OptimisticLockException.class, () -> store.markFaulted(ID, TYPE, 2L));
    assertThrows(
        OptimisticLockException.class,
        () -> store.markFaulted(SagaId.of("no-such-saga"), TYPE, 1L));
  }

  @Test
  public void casWrites_staleVersion_terminalRow_absentRow_orForeignType_throwOptimisticLock() {
    // Every CAS write (update, applyEvent, claimCompensating) shares the same four refusal
    // arms as markFaulted: a store's applyEvent SQL, in particular, could omit
    // the "status NOT IN (terminal)" or "saga_type = ?" predicate and still pass unless the
    // contract pins each arm for each method directly.
    var otherType = SagaType.of("OtherType");
    var missing = SagaId.of("no-such-saga");
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // version 1

    // (a) stale version — row exists, right type, wrong version.
    assertThrows(
        OptimisticLockException.class,
        () -> store.update(ID, TYPE, EVOLVED, SagaStatus.RUNNING, 99L));
    assertThrows(
        OptimisticLockException.class,
        () ->
            store.applyEvent(
                ID,
                TYPE,
                EVOLVED,
                SagaStatus.RUNNING,
                99L,
                AppliedEvent.live(GlobalOffset.of(1L))));
    assertThrows(
        OptimisticLockException.class, () -> store.claimCompensating(ID, TYPE, EVOLVED, 99L));

    // (d) foreign type — row exists, right version, wrong type: isolates the saga_type guard
    // from the version guard (the same predicate update() already guards).
    assertThrows(
        OptimisticLockException.class,
        () -> store.update(ID, otherType, EVOLVED, SagaStatus.RUNNING, 1L));
    assertThrows(
        OptimisticLockException.class,
        () ->
            store.applyEvent(
                ID,
                otherType,
                EVOLVED,
                SagaStatus.RUNNING,
                1L,
                AppliedEvent.live(GlobalOffset.of(1L))));
    assertThrows(
        OptimisticLockException.class, () -> store.claimCompensating(ID, otherType, EVOLVED, 1L));
    assertThrows(OptimisticLockException.class, () -> store.markFaulted(ID, otherType, 1L));

    // (c) absent saga id.
    assertThrows(
        OptimisticLockException.class,
        () -> store.update(missing, TYPE, EVOLVED, SagaStatus.RUNNING, 1L));
    assertThrows(
        OptimisticLockException.class,
        () ->
            store.applyEvent(
                missing,
                TYPE,
                EVOLVED,
                SagaStatus.RUNNING,
                1L,
                AppliedEvent.live(GlobalOffset.of(1L))));
    assertThrows(
        OptimisticLockException.class, () -> store.claimCompensating(missing, TYPE, EVOLVED, 1L));

    // (b) terminal row — advance the SAME row to a terminal status, then every CAS write must
    // refuse it even with the correct (post-transition) version and type.
    store.update(ID, TYPE, EVOLVED, SagaStatus.COMPLETED, 1L); // → version 2, terminal
    assertThrows(
        OptimisticLockException.class,
        () -> store.update(ID, TYPE, EVOLVED, SagaStatus.RUNNING, 2L));
    assertThrows(
        OptimisticLockException.class,
        () ->
            store.applyEvent(
                ID, TYPE, EVOLVED, SagaStatus.RUNNING, 2L, AppliedEvent.live(GlobalOffset.of(1L))));
    assertThrows(
        OptimisticLockException.class, () -> store.claimCompensating(ID, TYPE, EVOLVED, 2L));
  }

  @Test
  public void setDeadLetterPending_isAFlagWrite_noVersionBump_absentRowIsANoop() {
    store.setDeadLetterPending(ID, TYPE, true); // absent: no-op, no exception
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    store.setDeadLetterPending(ID, TYPE, true);
    var shielded = row();
    assertTrue(shielded.deadLetterPending());
    assertEquals(1L, shielded.version());
    store.setDeadLetterPending(ID, TYPE, false);
    assertFalse(row().deadLetterPending());
    store.setDeadLetterPending(ID, SagaType.of("OtherType"), true);
    assertFalse(row().deadLetterPending(), "type-scoped: a foreign type never flips the flag");
  }

  @Test
  public void load_isTypeScoped_forTheRecordedFacts() {
    store.createFaulted(ID, TYPE, INITIAL);
    assertTrue(store.load(ID, SagaType.of("OtherType"), ContractState.class).isEmpty());
    assertTrue(store.load(ID, TYPE, ContractState.class).isPresent());
  }

  /**
   * The load is type-scoped with no untyped mode: a {@code null} saga type is rejected, never read
   * as "any type" (which would return a colliding foreign type's row).
   */
  @Test
  public void load_rejectsANullSagaType() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    assertThrows(IllegalArgumentException.class, () -> store.load(ID, null, ContractState.class));
  }

  /**
   * Every typed operation is type-scoped like {@link #load_rejectsANullSagaType()}: a {@code null}
   * saga type is a caller bug, rejected with {@link IllegalArgumentException} before any I/O. It is
   * never read as "any type" (the store is keyed by {@code saga_id} alone, so an untyped delete
   * would remove a colliding foreign type's row), and never surfaced as a storage failure, which
   * the saga runtime treats as infrastructure and retries forever.
   */
  @Test
  public void delete_rejectsANullSagaType_andRemovesNothing() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    assertThrows(IllegalArgumentException.class, () -> store.delete(ID, null));
    assertEquals(1L, row().version(), "the owning type's row survives a rejected delete");
  }

  /** The three creates insert nothing for a {@code null} type: the id stays free for its owner. */
  @Test
  public void creates_rejectANullSagaType_andInsertNothing() {
    assertThrows(
        IllegalArgumentException.class,
        () -> store.create(ID, null, EVOLVED, SagaStatus.RUNNING, false));
    assertThrows(
        IllegalArgumentException.class, () -> store.create(ID, null, EVOLVED, SagaStatus.RUNNING));
    assertThrows(
        IllegalArgumentException.class,
        () -> store.createGenesisPending(ID, null, EVOLVED, SagaStatus.RUNNING, true));
    assertThrows(IllegalArgumentException.class, () -> store.createFaulted(ID, null, INITIAL));
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    var row = row();
    assertEquals(1L, row.version(), "no untyped row was left behind to collide with");
    assertTrue(row.genesisApplied());
    assertFalse(row.deadLetterPending());
  }

  /** The four CAS writes leave the row exactly as it was for a {@code null} type. */
  @Test
  public void casWrites_rejectANullSagaType_andWriteNothing() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // version 1
    assertThrows(
        IllegalArgumentException.class,
        () -> store.update(ID, null, INITIAL, SagaStatus.COMPLETED, 1L));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            store.applyEvent(
                ID, null, INITIAL, SagaStatus.RUNNING, 1L, AppliedEvent.live(GlobalOffset.of(4L))));
    assertThrows(
        IllegalArgumentException.class, () -> store.claimCompensating(ID, null, INITIAL, 1L));
    assertThrows(IllegalArgumentException.class, () -> store.markFaulted(ID, null, 1L));
    var row = row();
    assertEquals(1L, row.version());
    assertEquals(SagaStatus.RUNNING, row.status());
    assertEquals("evolved", row.state().marker());
    assertNull(row.preFaultStatus());
    assertNull(row.lastAppliedOffset());
    assertNull(row.episodeVersion());
  }

  /** The shield flag write is type-scoped too: a {@code null} type flips nothing. */
  @Test
  public void setDeadLetterPending_rejectsANullSagaType_andWritesNothing() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    assertThrows(IllegalArgumentException.class, () -> store.setDeadLetterPending(ID, null, true));
    assertFalse(row().deadLetterPending());
  }

  /**
   * A {@code null} saga id is a caller bug like a {@code null} type: every operation that takes a
   * {@link SagaId} rejects it with {@link IllegalArgumentException} before any I/O. Left to the
   * storage layer it surfaced as a storage failure (a JDBC bind of {@code sagaId.value()}), which
   * the saga runtime treats as infrastructure and retries forever. Nothing is written: the existing
   * row is untouched and no row is created.
   */
  @Test
  public void everySagaIdOperation_rejectsANullSagaId_andWritesNothing() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // version 1
    assertThrows(
        IllegalArgumentException.class,
        () -> store.create(null, TYPE, INITIAL, SagaStatus.STARTED, true),
        "create");
    assertThrows(
        IllegalArgumentException.class,
        () -> store.create(null, TYPE, INITIAL, SagaStatus.STARTED),
        "create (4-arg)");
    assertThrows(
        IllegalArgumentException.class,
        () -> store.createGenesisPending(null, TYPE, INITIAL, SagaStatus.STARTED, true),
        "createGenesisPending");
    assertThrows(
        IllegalArgumentException.class,
        () -> store.createFaulted(null, TYPE, INITIAL),
        "createFaulted");
    assertThrows(
        IllegalArgumentException.class,
        () -> store.update(null, TYPE, INITIAL, SagaStatus.COMPLETED, 1L),
        "update");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            store.applyEvent(
                null,
                TYPE,
                INITIAL,
                SagaStatus.RUNNING,
                1L,
                AppliedEvent.live(GlobalOffset.of(4L))),
        "applyEvent");
    assertThrows(
        IllegalArgumentException.class,
        () -> store.claimCompensating(null, TYPE, INITIAL, 1L),
        "claimCompensating");
    assertThrows(
        IllegalArgumentException.class, () -> store.markFaulted(null, TYPE, 1L), "markFaulted");
    assertThrows(
        IllegalArgumentException.class,
        () -> store.setDeadLetterPending(null, TYPE, true),
        "setDeadLetterPending");
    assertThrows(
        IllegalArgumentException.class, () -> store.load(null, TYPE, ContractState.class), "load");
    assertThrows(IllegalArgumentException.class, () -> store.delete(null, TYPE), "delete");

    var row = row();
    assertEquals(1L, row.version(), "the existing row survives every rejected call");
    assertEquals(SagaStatus.RUNNING, row.status());
    assertEquals("evolved", row.state().marker());
    assertNull(row.preFaultStatus());
    assertFalse(row.deadLetterPending());
    assertNull(row.lastAppliedOffset());
    assertNull(row.episodeVersion());
    assertEquals(List.of(ID), store.findByStatus(TYPE, SagaStatus.RUNNING, 10));
    assertTrue(store.findByStatus(TYPE, SagaStatus.STARTED, 10).isEmpty(), "no row was created");
    assertTrue(store.findByStatus(TYPE, SagaStatus.FAULTED, 10).isEmpty(), "no row was created");
  }

  /** The sweeper and triage queries have no untyped form either. */
  @Test
  public void queries_rejectANullSagaType() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    Instant later = Instant.now().plusSeconds(3600);
    assertThrows(IllegalArgumentException.class, () -> store.findTimedOut(null, later, 10));
    assertThrows(
        IllegalArgumentException.class, () -> store.findByStatus(null, SagaStatus.RUNNING, 10));
    assertThrows(
        IllegalArgumentException.class, () -> store.countByStatus(null, SagaStatus.RUNNING));
    assertThrows(IllegalArgumentException.class, () -> store.findCompensating(null, later, 10));
  }

  // ===== Null-argument parity. The id and the type are not the only arguments a
  // caller can get wrong. EVERY null argument of EVERY operation is the same caller bug, refused
  // with IllegalArgumentException (naming the argument) before any I/O: never a
  // NullPointerException
  // (one store), never a storage failure the saga runtime retries as infrastructure forever (a null
  // status NPE'd at status.name() inside PostgresSagaStore's storage catch), never a silently
  // stored
  // JSON null (a null state). The two shipped stores diverged on exactly these; this pins them.

  private static final SagaId FRESH_ID = SagaId.of("contract-saga-fresh");

  /** The call must be refused as a caller bug that names the offending argument. */
  private static void assertRejectsNull(String argument, Executable call) {
    var thrown = assertThrows(IllegalArgumentException.class, call, argument);
    assertNotNull(thrown.getMessage(), argument + ": the refusal names the argument");
    assertTrue(
        thrown.getMessage().contains(argument),
        argument + ": the refusal names the argument, was: " + thrown.getMessage());
  }

  /** The seeded row is exactly as {@code create(ID, TYPE, EVOLVED, RUNNING)} left it. */
  private void assertSeededRowUntouched() {
    var row = row();
    assertEquals(1L, row.version(), "the existing row survives every rejected call");
    assertEquals(SagaStatus.RUNNING, row.status());
    assertEquals("evolved", row.state().marker());
    assertTrue(row.genesisApplied());
    assertNull(row.preFaultStatus());
    assertFalse(row.deadLetterPending());
    assertNull(row.lastAppliedOffset());
    assertNull(row.lastReplayedOffset());
    assertNull(row.episodeVersion());
    assertTrue(store.load(FRESH_ID, TYPE, ContractState.class).isEmpty(), "no row was created");
  }

  /**
   * A {@code null} saga state is refused by every operation that takes one, and nothing is written:
   * the creates leave their id free, the CAS writes leave the row at its version (a store that
   * stored it would have bumped the version and persisted a JSON {@code null}).
   */
  @Test
  public void everyStateOperation_rejectsANullState_andWritesNothing() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // version 1
    assertRejectsNull("state", () -> store.create(FRESH_ID, TYPE, null, SagaStatus.RUNNING, true));
    assertRejectsNull("state", () -> store.create(FRESH_ID, TYPE, null, SagaStatus.RUNNING));
    assertRejectsNull(
        "state", () -> store.createGenesisPending(FRESH_ID, TYPE, null, SagaStatus.RUNNING, true));
    assertRejectsNull("state", () -> store.createFaulted(FRESH_ID, TYPE, null));
    assertRejectsNull("state", () -> store.update(ID, TYPE, null, SagaStatus.COMPLETED, 1L));
    assertRejectsNull(
        "state",
        () ->
            store.applyEvent(
                ID, TYPE, null, SagaStatus.RUNNING, 1L, AppliedEvent.live(GlobalOffset.of(4L))));
    assertRejectsNull("state", () -> store.claimCompensating(ID, TYPE, null, 1L));
    assertSeededRowUntouched();
  }

  /**
   * A {@code null} lifecycle status is refused by every operation that takes one — as the same
   * {@link IllegalArgumentException}, not the {@link NullPointerException} one store raised nor the
   * storage failure ({@code status.name()} inside the JDBC catch) the other one did.
   */
  @Test
  public void everyStatusOperation_rejectsANullStatus_andWritesNothing() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // version 1
    assertRejectsNull("status", () -> store.create(FRESH_ID, TYPE, INITIAL, null, true));
    assertRejectsNull("status", () -> store.create(FRESH_ID, TYPE, INITIAL, null));
    assertRejectsNull(
        "status", () -> store.createGenesisPending(FRESH_ID, TYPE, INITIAL, null, true));
    assertRejectsNull("status", () -> store.update(ID, TYPE, INITIAL, null, 1L));
    assertRejectsNull(
        "status",
        () ->
            store.applyEvent(ID, TYPE, INITIAL, null, 1L, AppliedEvent.live(GlobalOffset.of(4L))));
    assertSeededRowUntouched();
  }

  /**
   * A {@code null} {@link AppliedEvent} is refused by {@code applyEvent} as the same {@link
   * IllegalArgumentException} in both stores (one raised a {@link NullPointerException}).
   */
  @Test
  public void applyEvent_rejectsANullAppliedEvent_andWritesNothing() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // version 1
    assertRejectsNull(
        "applied", () -> store.applyEvent(ID, TYPE, INITIAL, SagaStatus.RUNNING, 1L, null));
    assertSeededRowUntouched();
  }

  /** The load's state class is an argument too: refused, never read as "any state". */
  @Test
  public void load_rejectsANullStateType() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    assertRejectsNull("stateType", () -> store.load(ID, TYPE, null));
    assertSeededRowUntouched();
  }

  /**
   * The queries' other arguments: a {@code null} cutoff, status or age bound is a caller bug, not
   * "no filter" and not a failure surfaced only when a row happens to be compared against it — with
   * a row of the type present for the filters to meet.
   */
  @Test
  public void queries_rejectANullStatusOrInstant() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING);
    store.create(FRESH_ID, TYPE, EVOLVED, SagaStatus.COMPENSATING);
    assertRejectsNull("cutoff", () -> store.findTimedOut(TYPE, null, 10));
    assertRejectsNull("status", () -> store.findByStatus(TYPE, null, 10));
    assertRejectsNull("status", () -> store.countByStatus(TYPE, null));
    assertRejectsNull("updatedBefore", () -> store.findCompensating(TYPE, null, 10));
  }

  /** A query on an empty store must refuse a {@code null} too — before it has a row to compare. */
  @Test
  public void queries_rejectANullStatusOrInstant_evenWhenNoRowExists() {
    assertRejectsNull("cutoff", () -> store.findTimedOut(TYPE, null, 10));
    assertRejectsNull("status", () -> store.findByStatus(TYPE, null, 10));
    assertRejectsNull("status", () -> store.countByStatus(TYPE, null));
    assertRejectsNull("updatedBefore", () -> store.findCompensating(TYPE, null, 10));
  }

  // ===== The episode stamp rule. Every write that lands a
  // row at COMPENSATING stamps episode_version (the version the row lands at) and
  // episode_claimed_at (the write's instant) in the same statement, unless the row already carries
  // a stamp; no write ever re-stamps. The runtime derives the compensation idempotency keys and the
  // key-age anchors from these two columns with NO fallback (see SagaStore.claimCompensating).

  @Test
  public void create_withCompensating_isBornStamped() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.COMPENSATING);
    var row = row();
    assertEquals(SagaStatus.COMPENSATING, row.status());
    assertEquals(1L, row.episodeVersion(), "episode_version = the version the row is born at");
    assertNotNull(row.episodeClaimedAt(), "episode_claimed_at = the insert's instant");
  }

  @Test
  public void createGenesisPending_withCompensating_isBornStamped_inTheInsert() {
    // The start event's evolve yielded COMPENSATING: the executor's create-first write carries the
    // stamp, so there is no crash point between "row is COMPENSATING" and "episode stamped".
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.COMPENSATING, false);
    var row = row();
    assertEquals(SagaStatus.COMPENSATING, row.status());
    assertFalse(row.genesisApplied());
    assertEquals(1L, row.version());
    assertEquals(1L, row.episodeVersion());
    assertNotNull(row.episodeClaimedAt());
  }

  @Test
  public void applyEvent_landingCompensating_stampsTheEpisode_atTheVersionItLandsAt() {
    store.createGenesisPending(ID, TYPE, EVOLVED, SagaStatus.RUNNING, false); // v1, no episode
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.RUNNING, 1L, AppliedEvent.liveStart(GlobalOffset.of(10L)));
    assertNull(row().episodeVersion(), "a forward write leaves a RUNNING row unstamped");
    assertNull(row().episodeClaimedAt());
    // A correlated event's evolve yielded COMPENSATING: the event CAS stamps it in the same write.
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.COMPENSATING, 2L, AppliedEvent.live(GlobalOffset.of(11L)));
    var row = row();
    assertEquals(SagaStatus.COMPENSATING, row.status());
    assertEquals(3L, row.version());
    assertEquals(3L, row.episodeVersion(), "stamped with the version the row lands at");
    assertNotNull(row.episodeClaimedAt());
    assertEquals(11L, row.lastAppliedOffset(), "the applied-event facts ride in the same write");
    assertTrue(row.genesisApplied());
  }

  @Test
  public void update_landingCompensating_stampsTheEpisode() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // v1
    store.update(ID, TYPE, EVOLVED, SagaStatus.COMPENSATING, 1L); // v2
    var row = row();
    assertEquals(2L, row.episodeVersion());
    assertNotNull(row.episodeClaimedAt());
  }

  @Test
  public void aStampedEpisode_isNeverReStamped_byAnyWrite() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // v1
    store.claimCompensating(ID, TYPE, EVOLVED, 1L); // v2: the episode is entered here
    var claimed = row();
    assertEquals(2L, claimed.episodeVersion());
    Instant claimedAt = claimed.episodeClaimedAt();
    assertNotNull(claimedAt);
    assertTrue(store.markFaulted(ID, TYPE, 2L)); // v3: a fault keeps the stamp
    var faulted = row();
    assertEquals(SagaStatus.COMPENSATING, faulted.effectiveStatus());
    assertEquals(2L, faulted.episodeVersion(), "a fault out of COMPENSATING keeps the stamp");
    assertEquals(claimedAt, faulted.episodeClaimedAt());
    store.update(ID, TYPE, EVOLVED, SagaStatus.COMPENSATING, 3L); // v4: un-fault, already stamped
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.COMPENSATING, 4L, AppliedEvent.replayed(GlobalOffset.of(7L)));
    store.claimCompensating(ID, TYPE, EVOLVED, 5L); // v6: a re-claim of a stamped row keeps it
    var row = row();
    assertEquals(6L, row.version());
    assertEquals(
        2L,
        row.episodeVersion(),
        "the episode identity is written once, by the write that entered the episode");
    assertEquals(claimedAt, row.episodeClaimedAt());
    store.update(ID, TYPE, EVOLVED, SagaStatus.COMPENSATED, 6L); // terminal: the stamp stays
    assertEquals(2L, row().episodeVersion());
    assertEquals(claimedAt, row().episodeClaimedAt());
  }

  @Test
  public void writesThatLandAnyOtherStatus_neverStamp() {
    store.create(ID, TYPE, EVOLVED, SagaStatus.RUNNING); // v1
    store.applyEvent(
        ID, TYPE, EVOLVED, SagaStatus.RUNNING, 1L, AppliedEvent.live(GlobalOffset.of(1L))); // v2
    assertTrue(store.markFaulted(ID, TYPE, 2L)); // v3: a forward fault
    store.update(ID, TYPE, EVOLVED, SagaStatus.RUNNING, 3L); // v4
    store.update(ID, TYPE, EVOLVED, SagaStatus.COMPLETED, 4L); // v5
    var row = row();
    assertEquals(5L, row.version());
    assertNull(row.episodeVersion(), "no write entered a compensation episode");
    assertNull(row.episodeClaimedAt());
    store.createFaulted(SagaId.of("faulted-at-genesis"), TYPE, INITIAL);
    var faulted =
        store.load(SagaId.of("faulted-at-genesis"), TYPE, ContractState.class).orElseThrow();
    assertNull(faulted.episodeVersion(), "the evolve-poison create enters no episode");
    assertNull(faulted.episodeClaimedAt());
  }

  // ===== findTimedOut serves two populations — forward timeouts (STARTED/RUNNING past their
  // absolute deadline) and compensation re-picks (COMPENSATING rows idle past the cutoff) — each in
  // its own deadline order, and interleaves them, so a backlog in one can never fill the whole
  // batch while the other waits.

  /** Far enough ahead that every row written by a test is past it, whatever the store's clock. */
  private static Instant everythingTimedOut() {
    return Instant.now().plusSeconds(3600);
  }

  /** Lets the store's clock move on, so consecutive writes carry strictly increasing instants. */
  private static void tick() {
    try {
      Thread.sleep(5);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  @Test
  public void findTimedOut_aCompensatingBacklogLargerThanTheBatch_cannotStarveAForwardTimeout() {
    // Episodes whose re-drives keep failing transiently write nothing, so their last-write instant
    // stays the oldest in the table on every poll.
    for (int i = 0; i < 4; i++) {
      store.create(SagaId.of("stuck-episode-" + i), TYPE, EVOLVED, SagaStatus.COMPENSATING);
    }
    tick();
    SagaId overdue = SagaId.of("overdue-forward");
    store.create(overdue, TYPE, EVOLVED, SagaStatus.RUNNING);

    List<SagaId> batch = store.findTimedOut(TYPE, everythingTimedOut(), 4);

    assertTrue(
        batch.contains(overdue),
        "a saga past its deadline must be selected even while the compensating backlog alone"
            + " could fill the batch: "
            + batch);
    assertEquals(overdue, batch.get(0), "the forward timeout takes the first slot");
    assertEquals(4, batch.size(), "slots the forward population leaves unused go to the backlog");
  }

  @Test
  public void findTimedOut_aForwardBacklogLargerThanTheBatch_cannotStarveACompensationRepick() {
    for (int i = 0; i < 4; i++) {
      store.create(SagaId.of("overdue-forward-" + i), TYPE, EVOLVED, SagaStatus.RUNNING);
    }
    tick();
    SagaId episode = SagaId.of("idle-episode");
    store.create(episode, TYPE, EVOLVED, SagaStatus.COMPENSATING);

    List<SagaId> batch = store.findTimedOut(TYPE, everythingTimedOut(), 4);

    assertTrue(
        batch.contains(episode),
        "an idle compensation episode must be re-picked even while the forward backlog alone"
            + " could fill the batch: "
            + batch);
    assertEquals(4, batch.size(), "slots the compensation population leaves unused go forward");
  }

  @Test
  public void findTimedOut_bothPopulationsSaturated_interleavesThemByRank_forwardFirst() {
    List<SagaId> episodes = List.of(SagaId.of("c0"), SagaId.of("c1"), SagaId.of("c2"));
    List<SagaId> forward = List.of(SagaId.of("f0"), SagaId.of("f1"), SagaId.of("f2"));
    for (SagaId id : episodes) {
      store.create(id, TYPE, EVOLVED, SagaStatus.COMPENSATING);
      tick();
    }
    for (SagaId id : forward) {
      store.create(id, TYPE, EVOLVED, SagaStatus.RUNNING);
      tick();
    }

    assertEquals(
        List.of(forward.get(0), episodes.get(0), forward.get(1), episodes.get(1)),
        store.findTimedOut(TYPE, everythingTimedOut(), 4),
        "each population gets half the batch, oldest first, forward first at each rank");
    assertEquals(
        List.of(forward.get(0), episodes.get(0), forward.get(1), episodes.get(1), forward.get(2)),
        store.findTimedOut(TYPE, everythingTimedOut(), 5),
        "an odd slot goes to the forward population");
  }

  @Test
  public void findTimedOut_forwardTimeoutsAreServedInStartOrder_notLastWriteOrder() {
    SagaId startedFirst = SagaId.of("started-first");
    SagaId startedSecond = SagaId.of("started-second");
    store.create(startedFirst, TYPE, EVOLVED, SagaStatus.RUNNING);
    tick();
    store.create(startedSecond, TYPE, EVOLVED, SagaStatus.RUNNING);
    tick();
    // Forward progress refreshes the last-write instant; the deadline still runs from the start.
    store.update(startedFirst, TYPE, EVOLVED, SagaStatus.RUNNING, 1L);

    assertEquals(
        List.of(startedFirst),
        store.findTimedOut(TYPE, everythingTimedOut(), 1),
        "the saga whose absolute deadline passed first is served first");
  }

  @Test
  public void findTimedOut_compensationRepicksAreServedInLastWriteOrder() {
    SagaId claimedFirst = SagaId.of("claimed-first");
    SagaId claimedSecond = SagaId.of("claimed-second");
    store.create(claimedFirst, TYPE, EVOLVED, SagaStatus.RUNNING);
    store.claimCompensating(claimedFirst, TYPE, EVOLVED, 1L);
    tick();
    store.create(claimedSecond, TYPE, EVOLVED, SagaStatus.RUNNING);
    store.claimCompensating(claimedSecond, TYPE, EVOLVED, 1L);
    tick();
    // A write to the first episode makes it the most recently active one.
    store.update(claimedFirst, TYPE, EVOLVED, SagaStatus.COMPENSATING, 2L);

    assertEquals(
        List.of(claimedSecond, claimedFirst),
        store.findTimedOut(TYPE, everythingTimedOut(), 2),
        "the episode idle longest is re-picked first");
  }
}
