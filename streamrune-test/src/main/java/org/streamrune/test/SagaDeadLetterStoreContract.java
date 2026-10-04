package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.test.SagaStoreContract.ContractState;

/**
 * The dead-letter contract both shipped stores honour: stored facts win, the shield is the
 * retention rule.
 */
public abstract class SagaDeadLetterStoreContract {

  /** A fresh saga store (rows the dead-letter store consults for FAULTED / shield). */
  protected abstract SagaStore newSagaStore();

  /** A fresh, empty dead-letter store consulting {@code sagaStore}. */
  protected abstract SagaDeadLetterStore newStore(SagaStore sagaStore);

  protected static final SagaType TYPE = SagaStoreContract.TYPE;
  protected static final SagaType OTHER = SagaType.of("Other");
  protected static final SagaId A = SagaId.of("dl-saga-a");
  protected static final Instant T0 = Instant.parse("2026-09-10T00:00:00Z");

  protected SagaStore sagaStore;
  protected SagaDeadLetterStore store;

  @BeforeEach
  void fresh() {
    sagaStore = newSagaStore();
    store = newStore(sagaStore);
  }

  protected static SagaDeadLetterEntry entry(SagaId sagaId, long offset, Instant faultedAt) {
    return new SagaDeadLetterEntry(
        sagaId,
        TYPE,
        GlobalOffset.of(offset),
        EventType.of("Evt"),
        "java.lang.RuntimeException",
        "boom",
        faultedAt);
  }

  protected void row(SagaId id, SagaStatus status) {
    sagaStore.create(id, TYPE, new ContractState(status, "m"), status);
  }

  @Test
  public void publish_named_refreshesErrorDetails_keepsFirstFaultedAtAnchorAndTarget() {
    store.publish(entry(A, 10L, T0));
    store.establishFirstReplayAnchor(A, TYPE, GlobalOffset.of(10L), T0.plusSeconds(5));
    store.publish(
        new SagaDeadLetterEntry(
            A,
            TYPE,
            GlobalOffset.of(10L),
            EventType.of("Evt"),
            "x.Other",
            "again",
            T0.plusSeconds(60)));
    SagaDeadLetterEntry e = store.findBySaga(A).get(0);
    assertEquals(1, store.findBySaga(A).size());
    assertEquals("x.Other", e.errorType());
    assertEquals(T0.plusSeconds(60), e.faultedAt());
    assertEquals(T0, e.firstFaultedAt());
    assertEquals(T0.plusSeconds(5), e.firstReplayStartedAt());
  }

  @Test
  public void publish_nullSaga_keepsFirstFaultedAtAnchorAndResolvedTarget_acrossRefreshes() {
    store.publish(entry(null, 20L, T0));
    store.establishFirstReplayAnchor(null, TYPE, GlobalOffset.of(20L), T0.plusSeconds(1));
    store.setResolvedTarget(TYPE, GlobalOffset.of(20L), A);
    store.publish(entry(null, 20L, T0.plusSeconds(90)));
    SagaDeadLetterEntry e = store.findNullSagaEntry(TYPE, GlobalOffset.of(20L)).orElseThrow();
    assertEquals(T0.plusSeconds(90), e.faultedAt());
    assertEquals(T0, e.firstFaultedAt());
    assertEquals(T0.plusSeconds(1), e.firstReplayStartedAt());
    assertEquals(A, e.targetSagaId());
  }

  @Test
  public void establishFirstReplayAnchor_storedValueWins() {
    store.publish(entry(A, 10L, T0));
    store.establishFirstReplayAnchor(A, TYPE, GlobalOffset.of(10L), T0.plusSeconds(1));
    store.establishFirstReplayAnchor(A, TYPE, GlobalOffset.of(10L), T0.plusSeconds(99));
    assertEquals(T0.plusSeconds(1), store.findBySaga(A).get(0).firstReplayStartedAt());
  }

  @Test
  public void findNullSagaEntriesByResolvedTarget_newestPerOffset_ascendingByOffset() {
    store.publish(entry(null, 31L, T0));
    store.publish(entry(null, 30L, T0));
    store.setResolvedTarget(TYPE, GlobalOffset.of(31L), A);
    store.setResolvedTarget(TYPE, GlobalOffset.of(30L), A);
    store.publish(entry(null, 30L, T0.plusSeconds(10))); // refresh keeps the target
    List<SagaDeadLetterEntry> folded = store.findNullSagaEntriesByResolvedTarget(TYPE, A);
    assertEquals(2, folded.size());
    assertEquals(30L, folded.get(0).eventOffset().value());
    assertEquals(T0.plusSeconds(10), folded.get(0).faultedAt());
    assertEquals(31L, folded.get(1).eventOffset().value());
  }

  @Test
  public void discard_typed_removesOnlyTheMatchingType() {
    store.publish(entry(A, 10L, T0));
    assertFalse(store.discard(A, SagaType.of("Other"), GlobalOffset.of(10L)));
    assertTrue(store.discard(A, TYPE, GlobalOffset.of(10L)));
    assertTrue(store.findBySaga(A).isEmpty());
  }

  /**
   * The discard is type-scoped with no untyped mode: a {@code null} saga type is rejected, never
   * read as "any type" — which would delete a colliding foreign type's only quarantine record.
   * Nothing is removed, named or null-saga.
   */
  @Test
  public void discard_rejectsANullSagaType() {
    store.publish(entry(A, 10L, T0));
    store.publish(entry(null, 11L, T0));
    assertThrows(
        IllegalArgumentException.class, () -> store.discard(A, null, GlobalOffset.of(10L)));
    assertThrows(
        IllegalArgumentException.class, () -> store.discard(null, null, GlobalOffset.of(11L)));
    assertEquals(1, store.findBySaga(A).size());
    assertTrue(store.findNullSagaEntry(TYPE, GlobalOffset.of(11L)).isPresent());
  }

  /**
   * The null-saga lookup is type-scoped with no untyped mode: a {@code null} saga type is rejected,
   * never read as "any type" — which would hand one type's replayer another type's entry at the
   * same offset.
   */
  @Test
  public void findNullSagaEntry_rejectsANullSagaType() {
    store.publish(entry(null, 11L, T0));
    store.publish(foreignEntry(null, 11L));
    assertThrows(
        IllegalArgumentException.class, () -> store.findNullSagaEntry(null, GlobalOffset.of(11L)));
  }

  /**
   * The resolved-target fold is type-scoped with no untyped mode: a {@code null} saga type is
   * rejected, never read as "any type" — which would fold a foreign type's resolved entry into this
   * type's drain.
   */
  @Test
  public void findNullSagaEntriesByResolvedTarget_rejectsANullSagaType() {
    store.publish(entry(null, 30L, T0));
    store.setResolvedTarget(TYPE, GlobalOffset.of(30L), A);
    store.publish(foreignEntry(null, 31L));
    store.setResolvedTarget(OTHER, GlobalOffset.of(31L), A);
    assertThrows(
        IllegalArgumentException.class, () -> store.findNullSagaEntriesByResolvedTarget(null, A));
  }

  /**
   * The anchor write is type-scoped with no untyped mode: a {@code null} saga type is rejected
   * before anything is written — read as "any type" it would stamp a colliding foreign type's entry
   * (named, or null-saga at the same offset) with an anchor its own replayer never set, and the
   * key-age guard would then measure that entry against a replay that never happened.
   */
  @Test
  public void establishFirstReplayAnchor_rejectsANullSagaType_andWritesNothing() {
    store.publish(foreignEntry(A, 10L));
    store.publish(entry(null, 11L, T0));
    store.publish(foreignEntry(null, 11L));
    assertThrows(
        IllegalArgumentException.class,
        () -> store.establishFirstReplayAnchor(A, null, GlobalOffset.of(10L), T0.plusSeconds(1)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            store.establishFirstReplayAnchor(null, null, GlobalOffset.of(11L), T0.plusSeconds(1)));
    assertNull(store.findBySaga(A).get(0).firstReplayStartedAt());
    assertNull(
        store.findNullSagaEntry(TYPE, GlobalOffset.of(11L)).orElseThrow().firstReplayStartedAt());
    assertNull(
        store.findNullSagaEntry(OTHER, GlobalOffset.of(11L)).orElseThrow().firstReplayStartedAt());
  }

  /**
   * The resolved-target write is type-scoped with no untyped mode: a {@code null} saga type is
   * rejected before anything is written — read as "any type" it would stamp a foreign type's
   * null-saga entry at the same offset with this type's target, which protects it from retention
   * and folds it into a drain it does not belong to.
   */
  @Test
  public void setResolvedTarget_rejectsANullSagaType_andWritesNothing() {
    store.publish(entry(null, 40L, T0));
    store.publish(foreignEntry(null, 40L));
    assertThrows(
        IllegalArgumentException.class,
        () -> store.setResolvedTarget(null, GlobalOffset.of(40L), A));
    assertNull(store.findNullSagaEntry(TYPE, GlobalOffset.of(40L)).orElseThrow().targetSagaId());
    assertNull(store.findNullSagaEntry(OTHER, GlobalOffset.of(40L)).orElseThrow().targetSagaId());
  }

  /**
   * The conditional shield clear has no untyped mode either: a {@code null} saga id or type is
   * rejected with {@code IllegalArgumentException} before anything is read or written, and the
   * shield stays set.
   */
  @Test
  public void clearShieldIfDrained_rejectsANullSagaIdOrType_andLeavesTheShield() {
    row(A, SagaStatus.RUNNING);
    sagaStore.setDeadLetterPending(A, TYPE, true);
    assertThrows(IllegalArgumentException.class, () -> store.clearShieldIfDrained(A, null));
    assertThrows(IllegalArgumentException.class, () -> store.clearShieldIfDrained(null, TYPE));
    assertTrue(sagaStore.load(A, TYPE, ContractState.class).orElseThrow().deadLetterPending());
  }

  /** An entry of {@link #OTHER}: a foreign type colliding with {@link #TYPE}'s id or offset. */
  private static SagaDeadLetterEntry foreignEntry(SagaId sagaId, long offset) {
    return new SagaDeadLetterEntry(
        sagaId,
        OTHER,
        GlobalOffset.of(offset),
        EventType.of("Evt"),
        "java.lang.RuntimeException",
        "boom",
        T0);
  }

  @Test
  public void deleteOlderThan_neverPrunesEntriesOfAFaultedOrShieldedOwner() {
    row(A, SagaStatus.RUNNING);
    sagaStore.setDeadLetterPending(A, TYPE, true);
    store.publish(entry(A, 10L, T0));
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    sagaStore.setDeadLetterPending(A, TYPE, false);
    sagaStore.markFaulted(A, TYPE, 1L);
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  public void deleteOlderThan_prunesEntriesOfAHealthyUnshieldedOwner_andUnroutableNullEntries() {
    row(A, SagaStatus.RUNNING);
    store.publish(entry(A, 10L, T0));
    store.publish(entry(null, 11L, T0)); // no target: unroutable garbage
    assertEquals(0, store.deleteOlderThan(T0.minusSeconds(1)));
    assertEquals(2, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  /** The retention guard sees a shield the row was BORN with, not only one set afterwards. */
  @Test
  public void deleteOlderThan_neverPrunesEntriesOfAnOwnerBornShielded() {
    store.publish(entry(A, 10L, T0)); // row-less residual first
    sagaStore.createGenesisPending(
        A, TYPE, new ContractState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING, true);
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    assertEquals(1L, store.countFaultedBacklog());
  }

  @Test
  public void deleteOlderThan_neverPrunesANamedEntryWhoseSagaHasNoRow() {
    store.publish(entry(A, 10L, T0)); // the row-less residual: initialState threw
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  public void deleteOlderThan_neverPrunesAResolvedNullEntry_whateverItsTargetLooksLike() {
    store.publish(entry(null, 40L, T0));
    store.setResolvedTarget(TYPE, GlobalOffset.of(40L), A); // target row absent: still resolved
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    row(A, SagaStatus.RUNNING); // healthy, unshielded target: still resolved
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    store.discard(null, TYPE, GlobalOffset.of(40L));
    store.publish(entry(null, 41L, T0)); // unresolved: garbage after the cutoff
    assertEquals(1, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  /**
   * The conditional clear — clears only where no named entry of this type and no null-saga entry
   * resolved to the saga exist at the store; a foreign type's entry, another saga's entry, an
   * already-clear flag and an absent row all leave it alone; returns whether it cleared.
   */
  @Test
  public void clearShieldIfDrained_clearsOnlyWhenNoNamedAndNoResolvedEntryExists() {
    row(A, SagaStatus.RUNNING);
    sagaStore.setDeadLetterPending(A, TYPE, true);
    store.publish(entry(A, 10L, T0));
    assertFalse(store.clearShieldIfDrained(A, TYPE), "a named entry of this type exists");
    assertTrue(shield(A));
    store.discard(A, TYPE, GlobalOffset.of(10L));
    store.publish(entry(null, 11L, T0));
    store.setResolvedTarget(TYPE, GlobalOffset.of(11L), A);
    assertFalse(store.clearShieldIfDrained(A, TYPE), "a null-saga entry resolved to A exists");
    assertTrue(shield(A));
    store.discard(null, TYPE, GlobalOffset.of(11L));
    store.publish(
        new SagaDeadLetterEntry(
            A,
            SagaType.of("Other"),
            GlobalOffset.of(12L),
            EventType.of("Evt"),
            "java.lang.RuntimeException",
            "boom",
            T0)); // a foreign type's entry under the same id: not this type's backlog
    store.publish(entry(SagaId.of("dl-saga-b"), 13L, T0)); // another saga's entry: irrelevant
    assertTrue(store.clearShieldIfDrained(A, TYPE), "backlog empty for this type: cleared");
    assertFalse(shield(A));
    assertFalse(store.clearShieldIfDrained(A, TYPE), "already clear: nothing to do");
    assertFalse(store.clearShieldIfDrained(SagaId.of("dl-absent"), TYPE), "absent row: no-op");
  }

  private boolean shield(SagaId id) {
    return sagaStore.load(id, TYPE, ContractState.class).orElseThrow().deadLetterPending();
  }

  /** The entry and the owner's shield land in one call; an absent owner is a no-op. */
  @Test
  public void publishShielded_publishesTheEntryAndShieldsTheOwner_inOneCall() {
    row(A, SagaStatus.RUNNING);
    assertFalse(shield(A));
    store.publishShielded(entry(A, 10L, T0));
    assertEquals(1, store.findBySaga(A).size());
    assertTrue(shield(A), "the owner row is shielded by the publish itself");
    store.publishShielded(entry(A, 10L, T0.plusSeconds(5))); // idempotent refresh
    assertEquals(1, store.findBySaga(A).size());
    assertEquals(T0, store.findBySaga(A).get(0).firstFaultedAt());
    assertTrue(shield(A));
  }

  @Test
  public void publishShielded_absentOwnerRow_recordsTheEntry_andDoesNotThrow() {
    var rowless = SagaId.of("dl-rowless-shielded");
    store.publishShielded(entry(rowless, 11L, T0));
    assertEquals(1, store.findBySaga(rowless).size());
    assertTrue(sagaStore.load(rowless, TYPE, ContractState.class).isEmpty());
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))), "row-less: never pruned");
  }

  @Test
  public void countFaultedBacklog_countsShieldedOrFaultedOwners_rowlessNamed_andResolvedNull() {
    row(A, SagaStatus.RUNNING);
    store.publish(entry(A, 10L, T0)); // healthy, unshielded: not counted
    assertEquals(0L, store.countFaultedBacklog());
    sagaStore.setDeadLetterPending(A, TYPE, true);
    assertEquals(1L, store.countFaultedBacklog());
    store.publish(entry(SagaId.of("dl-rowless"), 12L, T0)); // row-less named: counted
    store.publish(entry(null, 13L, T0)); // unresolved: not counted
    assertEquals(2L, store.countFaultedBacklog());
    store.setResolvedTarget(TYPE, GlobalOffset.of(13L), A); // resolved: counted
    assertEquals(3L, store.countFaultedBacklog());
  }

  @Test
  public void entry_convenienceConstructors_defaultFirstFaultedAtToFaultedAt() {
    SagaDeadLetterEntry e = entry(A, 10L, T0);
    assertEquals(T0, e.firstFaultedAt());
    assertNull(e.firstReplayStartedAt());
    assertNull(e.targetSagaId());
  }
}
