package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

class InMemorySagaDeadLetterStoreTest {

  InMemorySagaStore sagaStore;
  InMemorySagaDeadLetterStore store;

  @BeforeEach
  void setUp() {
    sagaStore = new InMemorySagaStore();
    store = new InMemorySagaDeadLetterStore(sagaStore);
  }

  /** A terminal owner row: its entries are unshielded, so retention may prune them. */
  private void completedOwnerRow(SagaId sagaId, SagaType sagaType) {
    sagaStore.create(sagaId, sagaType, new GuardState(SagaStatus.COMPLETED), SagaStatus.COMPLETED);
  }

  private SagaDeadLetterStore.SagaDeadLetterEntry entry(
      SagaId sagaId, SagaType sagaType, long offset, Instant faultedAt) {
    return new SagaDeadLetterStore.SagaDeadLetterEntry(
        sagaId,
        sagaType,
        GlobalOffset.of(offset),
        EventType.of("OrderCreated"),
        "java.lang.RuntimeException",
        "msg",
        faultedAt);
  }

  @Test
  void publish_then_findBySaga_and_findAll() {
    var sid = SagaId.of("s1");
    var type = SagaType.of("OrderSaga");
    store.publish(entry(sid, type, 1, Instant.EPOCH));
    store.publish(entry(sid, type, 2, Instant.EPOCH));

    List<SagaDeadLetterStore.SagaDeadLetterEntry> bySaga = store.findBySaga(sid);
    assertEquals(2, bySaga.size());

    List<SagaDeadLetterStore.SagaDeadLetterEntry> all = store.findAll(10);
    assertEquals(2, all.size());
  }

  @Test
  void publish_isIdempotent_onSameSagaAndOffset() {
    var sid = SagaId.of("s1");
    var off = GlobalOffset.of(7);
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sid,
            SagaType.of("T"),
            off,
            EventType.of("E"),
            "java.lang.RuntimeException",
            "boom",
            Instant.EPOCH));
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sid,
            SagaType.of("T"),
            off,
            EventType.of("E"),
            "java.lang.RuntimeException",
            "boom-again",
            Instant.EPOCH));
    assertEquals(1, store.findBySaga(sid).size()); // upsert, not duplicate
  }

  @Test
  void findBySaga_filters_by_saga_id() {
    var s1 = SagaId.of("saga-1");
    var s2 = SagaId.of("saga-2");
    store.publish(entry(s1, SagaType.of("T"), 10, Instant.EPOCH));
    store.publish(entry(s2, SagaType.of("T"), 20, Instant.EPOCH));

    assertEquals(1, store.findBySaga(s1).size());
    assertEquals(1, store.findBySaga(s2).size());
    assertTrue(store.findBySaga(SagaId.of("unknown")).isEmpty());
  }

  @Test
  void findAll_respects_limit() {
    var sid = SagaId.of("s");
    store.publish(entry(sid, SagaType.of("T"), 1, Instant.EPOCH));
    store.publish(entry(sid, SagaType.of("T"), 2, Instant.EPOCH));
    store.publish(entry(sid, SagaType.of("T"), 3, Instant.EPOCH));

    assertEquals(2, store.findAll(2).size());
  }

  @Test
  void findAll_orders_by_faultedAt_descending() {
    var sid = SagaId.of("s");
    var t1 = SagaType.of("T");
    store.publish(entry(sid, t1, 1, Instant.ofEpochSecond(100)));
    store.publish(entry(sid, t1, 2, Instant.ofEpochSecond(200)));

    List<SagaDeadLetterStore.SagaDeadLetterEntry> all = store.findAll(10);
    // most recent first
    assertEquals(Instant.ofEpochSecond(200), all.get(0).faultedAt());
    assertEquals(Instant.ofEpochSecond(100), all.get(1).faultedAt());
  }

  @Test
  void publish_with_null_sagaId_keyed_by_offset() {
    // null sagaId = routing failure; keyed by offset alone
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("T"),
            GlobalOffset.of(5),
            EventType.of("UnknownEvent"),
            "java.lang.IllegalArgumentException",
            "no saga found",
            Instant.EPOCH));

    assertEquals(1, store.findAll(10).size());
    assertTrue(store.findBySaga(SagaId.of("any")).isEmpty());
  }

  @Test
  void publish_rejects_null_entry() {
    assertThrows(IllegalArgumentException.class, () -> store.publish(null));
  }

  @Test
  void discard_removesExactEntry_returnsTrue() {
    var sid = SagaId.of("s1");
    var type = SagaType.of("T");
    store.publish(entry(sid, type, 7, Instant.EPOCH));
    store.publish(entry(sid, type, 9, Instant.EPOCH));

    assertTrue(store.discard(sid, type, GlobalOffset.of(7)));

    List<SagaDeadLetterStore.SagaDeadLetterEntry> remaining = store.findBySaga(sid);
    assertEquals(1, remaining.size());
    assertEquals(GlobalOffset.of(9), remaining.get(0).eventOffset());
  }

  @Test
  void discard_missingEntry_returnsFalse() {
    var sid = SagaId.of("s1");
    store.publish(entry(sid, SagaType.of("T"), 7, Instant.EPOCH));

    assertFalse(store.discard(sid, SagaType.of("T"), GlobalOffset.of(999)));
  }

  @Test
  void discard_nullSagaEntry_matchesByOffsetWithNullSagaId() {
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("T"),
            GlobalOffset.of(7),
            EventType.of("UnknownEvent"),
            "java.lang.IllegalArgumentException",
            "no saga found",
            Instant.EPOCH));

    assertTrue(store.discard(null, SagaType.of("T"), GlobalOffset.of(7)));
    assertTrue(store.all().isEmpty());

    // null must NOT match a non-null saga row at the same offset
    var s1 = SagaId.of("s1");
    store.publish(entry(s1, SagaType.of("T"), 7, Instant.EPOCH));
    assertFalse(store.discard(null, SagaType.of("T"), GlobalOffset.of(7)));
  }

  @Test
  void deleteOlderThan_removesStrictlyOlder_returnsCount() {
    var sid = SagaId.of("s1");
    var type = SagaType.of("T");
    Instant now = Instant.parse("2026-01-03T00:00:00Z");
    Instant tMinus1d = now.minusSeconds(86400);
    Instant tMinus2d = now.minusSeconds(2 * 86400);
    completedOwnerRow(sid, type);

    store.publish(entry(sid, type, 1, tMinus2d));
    store.publish(entry(sid, type, 2, tMinus1d));
    store.publish(entry(sid, type, 3, now));

    int removed = store.deleteOlderThan(tMinus1d);

    assertEquals(1, removed); // only strictly-before tMinus1d removed
    List<SagaDeadLetterStore.SagaDeadLetterEntry> remaining = store.findBySaga(sid);
    assertEquals(2, remaining.size());
    assertTrue(remaining.stream().noneMatch(e -> e.faultedAt().equals(tMinus2d)));
  }

  // --- Immutable firstFaultedAt across re-publish (parity with the Postgres upsert) ---

  @Test
  void publish_upsert_refreshesFaultedAt_butNeverResetsFirstFaultedAt() {
    var sid = SagaId.of("s-first");
    var type = SagaType.of("T");
    Instant day0 = Instant.parse("2026-01-01T00:00:00Z");
    Instant day5 = Instant.parse("2026-01-06T00:00:00Z");

    store.publish(entry(sid, type, 1, day0));
    // A failed replay re-quarantines the same key with a fresh faultedAt (day 5).
    store.publish(entry(sid, type, 1, day5));

    SagaDeadLetterStore.SagaDeadLetterEntry e = store.findBySaga(sid).get(0);
    assertEquals(day5, e.faultedAt(), "faultedAt refreshes on re-quarantine");
    assertEquals(day0, e.firstFaultedAt(), "firstFaultedAt is immutable across re-quarantines");
  }

  @Test
  void deleteOlderThan_prunesOnFirstFaultedAt_reQuarantineCannotExtendRetentionLife() {
    var sid = SagaId.of("s-retention-first");
    var type = SagaType.of("T");
    Instant now = Instant.parse("2026-01-10T00:00:00Z");
    Instant tMinus8d = now.minusSeconds(8 * 86400);
    completedOwnerRow(sid, type);

    // First faulted 8 days ago; a failed replay re-quarantined it TODAY (fresh faultedAt).
    store.publish(entry(sid, type, 1, tMinus8d));
    store.publish(entry(sid, type, 1, now));

    int removed = store.deleteOlderThan(now.minusSeconds(7 * 86400));

    assertEquals(
        1,
        removed,
        "retention is bounded by the FIRST fault — a re-quarantine must not extend the entry's"
            + " life");
    assertTrue(store.findBySaga(sid).isEmpty());
  }

  @Test
  void publish_sevenArgEntry_defaultsFirstFaultedAtToFaultedAt() {
    var sid = SagaId.of("s-first-default");
    Instant faultedAt = Instant.parse("2026-01-02T00:00:00Z");
    store.publish(entry(sid, SagaType.of("T"), 1, faultedAt));

    assertEquals(faultedAt, store.findBySaga(sid).get(0).firstFaultedAt());
  }

  @Test
  void publish_explicitNullFirstFaultedAt_normalizesToFaultedAt() {
    // An 8-arg entry with an explicit null firstFaultedAt (third-party input) must normalize to
    // faultedAt on first insert — every stored entry carries its first-fault anchor.
    var sid = SagaId.of("s-first-null");
    Instant faultedAt = Instant.parse("2026-01-02T00:00:00Z");
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sid,
            SagaType.of("T"),
            GlobalOffset.of(1),
            EventType.of("E"),
            "java.lang.RuntimeException",
            "boom",
            faultedAt,
            null));

    assertEquals(faultedAt, store.findBySaga(sid).get(0).firstFaultedAt());
  }

  // --- FAULTED-owner retention guard (parity with the Postgres NOT EXISTS) ---

  /** Minimal saga state for the retention-guard wiring. */
  record GuardState(org.streamrune.core.saga.SagaStatus status)
      implements org.streamrune.core.saga.SagaState {}

  @Test
  void deleteOlderThan_withWiredSagaStore_retainsFaultedOwnersEntry() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    Instant now = Instant.parse("2026-01-10T00:00:00Z");
    Instant old = now.minusSeconds(8 * 86400);

    sagaStore.create(
        SagaId.of("s-faulted"),
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);
    guarded.publish(entry(SagaId.of("s-faulted"), SagaType.of("T"), 1, old));
    guarded.publish(entry(SagaId.of("s-rowless"), SagaType.of("T"), 2, old));
    sagaStore.create(
        SagaId.of("s-recovered"),
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.COMPLETED),
        org.streamrune.core.saga.SagaStatus.COMPLETED);
    guarded.publish(entry(SagaId.of("s-recovered"), SagaType.of("T"), 3, old));

    int removed = guarded.deleteOlderThan(now.minusSeconds(7 * 86400));

    // A NAMED entry whose saga has NO row is the sole record of a saga FAULTED without
    // a row — shielded like a FAULTED owner's. Only the terminalized saga's entry is
    // prunable.
    assertEquals(1, removed, "only the recovered saga's entry is prunable");
    assertEquals(1, guarded.findBySaga(SagaId.of("s-faulted")).size());
    assertEquals(1, guarded.findBySaga(SagaId.of("s-rowless")).size());
    assertTrue(guarded.findBySaga(SagaId.of("s-recovered")).isEmpty());
    assertEquals(2, guarded.countFaultedBacklog(), "the gauge sees the row-less FAULTED saga");
  }

  @Test
  void deleteOlderThan_withWiredSagaStore_rowlessTargetRecordAndResolvedNullEntries_allShielded() {
    // A RESOLVED null-saga entry is operator-attended by construction — shielded
    // and counted whatever its target looks like (row-less but owning a record, or never started);
    // the row-less target's own named record is shielded as the row-less residual.
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    Instant now = Instant.parse("2026-01-10T00:00:00Z");
    Instant old = now.minusSeconds(8 * 86400);
    var type = SagaType.of("T");

    guarded.publish(entry(SagaId.of("t-rowless"), type, 1, old)); // the target's own record
    guarded.publish(entry(null, type, 2, old));
    guarded.setResolvedTarget(type, GlobalOffset.of(2), SagaId.of("t-rowless"));
    guarded.publish(entry(null, type, 3, old));
    guarded.setResolvedTarget(type, GlobalOffset.of(3), SagaId.of("never-started"));

    assertEquals(3, guarded.countFaultedBacklog());
    assertEquals(0, guarded.deleteOlderThan(now.minusSeconds(7 * 86400)));
    assertEquals(3, guarded.all().size(), "all three survive");
  }

  @Test
  void deleteOlderThan_withWiredSagaStore_foreignTypeRowIsIrrelevant_ownTypeRowlessShields() {
    // Type-scoping discipline in the guard: a foreign type's row never protects — or releases —
    // this
    // type's entry. For its OWN type the saga has no row while owning a record, so it is
    // FAULTED-without-a-row and the entry is shielded, whatever TypeB's colliding row says (the
    // store is keyed by saga id alone, so TypeA can never have a row of its own here).
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    Instant now = Instant.parse("2026-01-10T00:00:00Z");

    sagaStore.create(
        SagaId.of("s-cross"),
        SagaType.of("TypeB"),
        new GuardState(org.streamrune.core.saga.SagaStatus.COMPLETED),
        org.streamrune.core.saga.SagaStatus.COMPLETED);
    guarded.publish(
        entry(SagaId.of("s-cross"), SagaType.of("TypeA"), 1, now.minusSeconds(8 * 86400)));

    assertEquals(0, guarded.deleteOlderThan(now.minusSeconds(7 * 86400)));
    assertEquals(1, guarded.findBySaga(SagaId.of("s-cross")).size());
    assertEquals(1, guarded.countFaultedBacklog());
  }

  // --- Observability: FAULTED backlog count = the retention guard's complement ---

  @Test
  void countFaultedBacklog_countsOnlyEntriesOwnedByACurrentlyFaultedSaga() {
    // Exactly the entries deleteOlderThan protects: same type scoping, same treatment of
    // absent rows (a named entry with no own-type row is a row-less FAULTED saga's
    // record and IS counted) — but age-independent ("is this saga stranded right now?").
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    Instant now = Instant.parse("2026-01-10T00:00:00Z");

    sagaStore.create(
        SagaId.of("s-faulted"),
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);
    sagaStore.create(
        SagaId.of("s-recovered"),
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.COMPENSATED),
        org.streamrune.core.saga.SagaStatus.COMPENSATED);
    sagaStore.create(
        SagaId.of("s-cross"),
        SagaType.of("TypeB"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);

    guarded.publish(entry(SagaId.of("s-faulted"), SagaType.of("T"), 1, now)); // counted
    guarded.publish(entry(SagaId.of("s-faulted"), SagaType.of("T"), 2, now)); // counted (2nd entry)
    guarded.publish(entry(SagaId.of("s-recovered"), SagaType.of("T"), 3, now)); // not FAULTED
    guarded.publish(entry(SagaId.of("s-rowless"), SagaType.of("T"), 4, now)); // no saga row
    guarded.publish(entry(SagaId.of("s-cross"), SagaType.of("TypeA"), 5, now)); // foreign type

    assertEquals(4, guarded.countFaultedBacklog());
  }

  @Test
  void countFaultedBacklog_dropsToZeroOnceTheSagaIsUnFaulted() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    var sid = SagaId.of("s-recovers");

    sagaStore.create(
        sid,
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);
    guarded.publish(entry(sid, SagaType.of("T"), 1, Instant.parse("2026-01-10T00:00:00Z")));
    assertEquals(1, guarded.countFaultedBacklog());

    sagaStore.update(
        sid,
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.RUNNING),
        org.streamrune.core.saga.SagaStatus.RUNNING,
        1L); // create() lands the row at version 1

    assertEquals(0, guarded.countFaultedBacklog(), "a replayed/un-faulted saga leaves the backlog");
  }

  // --- Null-saga entries protected through their resolved target ---

  @Test
  void setResolvedTarget_stampsNullSagaEntriesTypeScoped_neverNamedRows() {
    store.publish(entry(null, SagaType.of("TypeA"), 91, Instant.EPOCH));
    store.publish(entry(null, SagaType.of("TypeB"), 91, Instant.EPOCH));
    store.publish(entry(SagaId.of("named-91"), SagaType.of("TypeA"), 91, Instant.EPOCH));

    store.setResolvedTarget(SagaType.of("TypeA"), GlobalOffset.of(91), SagaId.of("target-s"));

    for (var e : store.findAll(10)) {
      if (e.sagaId() == null && SagaType.of("TypeA").equals(e.sagaType())) {
        assertEquals(SagaId.of("target-s"), e.targetSagaId(), "own null-saga entry stamped");
      } else {
        assertNull(e.targetSagaId(), "foreign-type and named entries are never stamped");
      }
    }
  }

  @Test
  void deleteOlderThan_retainsAResolvedNullSagaEntry_faultedTargetOrRecovered() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    Instant now = Instant.parse("2026-01-31T00:00:00Z");
    sagaStore.create(
        SagaId.of("target-s"),
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);
    guarded.publish(entry(null, SagaType.of("T"), 92, now.minusSeconds(8L * 86400)));
    guarded.setResolvedTarget(SagaType.of("T"), GlobalOffset.of(92), SagaId.of("target-s"));

    assertEquals(
        0,
        guarded.deleteOlderThan(now.minusSeconds(7L * 86400)),
        "the sole record of a never-consumed event owned by a FAULTED target must survive");
    assertEquals(1, guarded.findAll(10).size());

    // A recovered target does NOT release a RESOLVED entry — it is
    // operator-attended by construction and stays until replayed or discarded.
    sagaStore.update(
        SagaId.of("target-s"),
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.RUNNING),
        org.streamrune.core.saga.SagaStatus.RUNNING,
        1L);
    assertEquals(0, guarded.deleteOlderThan(now.minusSeconds(7L * 86400)));
  }

  @Test
  void deleteOlderThan_unresolvedNullSagaEntry_staysPrunable() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    Instant now = Instant.parse("2026-01-31T00:00:00Z");
    guarded.publish(entry(null, SagaType.of("T"), 93, now.minusSeconds(8L * 86400)));

    assertEquals(1, guarded.deleteOlderThan(now.minusSeconds(7L * 86400)));
  }

  @Test
  void publish_reQuarantine_keepsResolvedTarget() {
    // Stored-wins conflict semantics, like firstFaultedAt/firstReplayStartedAt: a re-poison must
    // not strip the retention protection an earlier TARGET_PENDING resolution established.
    store.publish(entry(null, SagaType.of("T"), 94, Instant.EPOCH));
    store.setResolvedTarget(SagaType.of("T"), GlobalOffset.of(94), SagaId.of("kept-target"));

    store.publish(entry(null, SagaType.of("T"), 94, Instant.ofEpochSecond(60))); // re-quarantine

    var e = store.findAll(10).get(0);
    assertEquals(SagaId.of("kept-target"), e.targetSagaId(), "stored resolved target wins");
    assertEquals(Instant.ofEpochSecond(60), e.faultedAt(), "faultedAt still refreshes");
  }

  @Test
  void countFaultedBacklog_countsTargetFaultedNullSagaEntries() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    sagaStore.create(
        SagaId.of("target-bk"),
        SagaType.of("T"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);
    guarded.publish(entry(null, SagaType.of("T"), 95, Instant.parse("2026-01-10T00:00:00Z")));
    assertEquals(0, guarded.countFaultedBacklog(), "unresolved null-saga entry not counted");

    guarded.setResolvedTarget(SagaType.of("T"), GlobalOffset.of(95), SagaId.of("target-bk"));

    assertEquals(1, guarded.countFaultedBacklog(), "target-FAULTED null-saga entries are visible");
  }

  // --- Retention follows the shield (also pinned by the shared contract suite) ---

  private static final Instant T0 = Instant.parse("2026-09-10T00:00:00Z");

  @Test
  void deleteOlderThan_neverPrunesEntriesOfAFaultedOrShieldedOwner() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    var a = SagaId.of("dl-saga-a");
    var type = SagaType.of("T");
    sagaStore.create(a, type, new GuardState(SagaStatus.RUNNING), SagaStatus.RUNNING);
    sagaStore.setDeadLetterPending(a, type, true);
    guarded.publish(entry(a, type, 10, T0));
    assertEquals(0, guarded.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    sagaStore.setDeadLetterPending(a, type, false);
    sagaStore.markFaulted(a, type, 1L);
    assertEquals(0, guarded.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  void deleteOlderThan_prunesEntriesOfAHealthyUnshieldedOwner_andUnroutableNullEntries() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    var a = SagaId.of("dl-saga-a");
    var type = SagaType.of("T");
    sagaStore.create(a, type, new GuardState(SagaStatus.RUNNING), SagaStatus.RUNNING);
    guarded.publish(entry(a, type, 10, T0));
    guarded.publish(entry(null, type, 11, T0)); // no target: unroutable garbage
    assertEquals(0, guarded.deleteOlderThan(T0.minusSeconds(1)));
    assertEquals(2, guarded.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  void deleteOlderThan_neverPrunesANamedEntryWhoseSagaHasNoRow() {
    var guarded = new InMemorySagaDeadLetterStore(new InMemorySagaStore());
    // the row-less residual: initialState threw
    guarded.publish(entry(SagaId.of("dl-saga-a"), SagaType.of("T"), 10, T0));
    assertEquals(0, guarded.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  void deleteOlderThan_neverPrunesAResolvedNullEntry_whateverItsTargetLooksLike() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    var a = SagaId.of("dl-saga-a");
    var type = SagaType.of("T");
    guarded.publish(entry(null, type, 40, T0));
    guarded.setResolvedTarget(type, GlobalOffset.of(40), a); // target row absent: still resolved
    assertEquals(0, guarded.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    // healthy, unshielded target: still resolved
    sagaStore.create(a, type, new GuardState(SagaStatus.RUNNING), SagaStatus.RUNNING);
    assertEquals(0, guarded.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    guarded.discard(null, type, GlobalOffset.of(40));
    guarded.publish(entry(null, type, 41, T0)); // unresolved: garbage after the cutoff
    assertEquals(1, guarded.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  void countFaultedBacklog_countsShieldedOrFaultedOwners_rowlessNamed_andResolvedNull() {
    var sagaStore = new InMemorySagaStore();
    var guarded = new InMemorySagaDeadLetterStore(sagaStore);
    var a = SagaId.of("dl-saga-a");
    var type = SagaType.of("T");
    sagaStore.create(a, type, new GuardState(SagaStatus.RUNNING), SagaStatus.RUNNING);
    guarded.publish(entry(a, type, 10, T0)); // healthy, unshielded: not counted
    assertEquals(0L, guarded.countFaultedBacklog());
    sagaStore.setDeadLetterPending(a, type, true);
    assertEquals(1L, guarded.countFaultedBacklog());
    guarded.publish(entry(SagaId.of("dl-rowless"), type, 12, T0)); // row-less named: counted
    guarded.publish(entry(null, type, 13, T0)); // unresolved: not counted
    assertEquals(2L, guarded.countFaultedBacklog());
    guarded.setResolvedTarget(type, GlobalOffset.of(13), a); // resolved: counted
    assertEquals(3L, guarded.countFaultedBacklog());
  }

  // --- Type-scoped discard (parity with the Postgres store) ---

  @Test
  void discard_typeScoped_neverDeletesForeignTypeRow() {
    var sid = SagaId.of("s-typed");
    store.publish(entry(sid, SagaType.of("TypeB"), 1, Instant.EPOCH));

    assertFalse(
        store.discard(sid, SagaType.of("TypeA"), GlobalOffset.of(1)),
        "a foreign type must never delete another type's quarantine record");
    assertEquals(1, store.findBySaga(sid).size());

    assertTrue(store.discard(sid, SagaType.of("TypeB"), GlobalOffset.of(1)));
    assertTrue(store.findBySaga(sid).isEmpty());
  }

  // --- Null-saga entries carry the quarantining type; per-type isolation ---

  @Test
  void publish_nullSagaEntries_twoTypesSameOffset_coexist() {
    // Two saga types quarantining the SAME offset as null-saga entries hold two independent
    // delivery obligations (Postgres NULL-distinct semantics keep both rows). The in-memory
    // store must not collapse them onto one offset-keyed slot — B's publish silently destroying
    // A's only record.
    store.publish(entry(null, SagaType.of("TypeA"), 42, Instant.EPOCH));
    store.publish(entry(null, SagaType.of("TypeB"), 42, Instant.EPOCH));

    assertEquals(2, store.findAll(10).size(), "each type keeps its own null-saga record");
  }

  @Test
  void publish_nullSagaSameTypeSameOffset_isStillAnUpsert() {
    // Same-type re-quarantine of a null-saga event stays an idempotent upsert (the in-memory
    // store's documented improvement over Postgres's accepted duplicate accumulation).
    store.publish(entry(null, SagaType.of("TypeA"), 43, Instant.EPOCH));
    store.publish(entry(null, SagaType.of("TypeA"), 43, Instant.ofEpochSecond(60)));

    assertEquals(1, store.findAll(10).size());
    assertEquals(Instant.ofEpochSecond(60), store.findAll(10).get(0).faultedAt());
  }

  @Test
  void discard_typeScoped_nullSagaEntry_removesOnlyOwnTypesRow() {
    // With types stamped at quarantine, one type's replay-then-discard of a null-saga
    // entry must never remove another type's record at the same offset.
    store.publish(entry(null, SagaType.of("TypeA"), 44, Instant.EPOCH));
    store.publish(entry(null, SagaType.of("TypeB"), 44, Instant.EPOCH));

    assertTrue(store.discard(null, SagaType.of("TypeA"), GlobalOffset.of(44)));

    var remaining = store.findAll(10);
    assertEquals(1, remaining.size(), "B's null-saga record must survive A's typed discard");
    assertEquals(SagaType.of("TypeB"), remaining.get(0).sagaType());
  }

  @Test
  void publish_reQuarantine_keepsFirstReplayAnchor() {
    // The STORED anchor survives a re-quarantine and a publish input can never forge one on an
    // existing entry (the same immutable-on-conflict rule as firstFaultedAt).
    var sid = SagaId.of("s-first-anchor-keep");
    var type = SagaType.of("T");
    store.publish(entry(sid, type, 1, Instant.EPOCH));
    Instant first = Instant.parse("2026-01-05T00:00:00Z");
    store.establishFirstReplayAnchor(sid, type, GlobalOffset.of(1), first);

    store.publish(entry(sid, type, 1, Instant.parse("2026-01-06T00:00:00Z"))); // re-quarantine

    assertEquals(
        first,
        store.findBySaga(sid).get(0).firstReplayStartedAt(),
        "the re-quarantine upsert must keep the stored anchor");
  }

  // --- Establish-only anchor write, called before every replay attempt ---

  @Test
  void establishFirstReplayAnchor_setsAnchorWhenAbsent() {
    var sid = SagaId.of("s-est");
    var type = SagaType.of("T");
    store.publish(entry(sid, type, 1, Instant.EPOCH));

    var anchor = Instant.ofEpochSecond(500);
    store.establishFirstReplayAnchor(sid, type, GlobalOffset.of(1), anchor);

    var e = store.findBySaga(sid).get(0);
    assertEquals(anchor, e.firstReplayStartedAt());
  }

  @Test
  void establishFirstReplayAnchor_keepsAnExistingAnchor_andIsTypeScoped() {
    var sid = SagaId.of("s-est2");
    var type = SagaType.of("T");
    store.publish(entry(sid, type, 1, Instant.EPOCH));
    store.establishFirstReplayAnchor(sid, type, GlobalOffset.of(1), Instant.ofEpochSecond(100));

    store.establishFirstReplayAnchor(sid, type, GlobalOffset.of(1), Instant.ofEpochSecond(900));
    assertEquals(
        Instant.ofEpochSecond(100),
        store.findBySaga(sid).get(0).firstReplayStartedAt(),
        "establish-only: the stored anchor always wins");

    var sid2 = SagaId.of("s-est3");
    store.publish(entry(sid2, SagaType.of("TypeA"), 2, Instant.EPOCH));
    store.establishFirstReplayAnchor(
        sid2, SagaType.of("TypeB"), GlobalOffset.of(2), Instant.ofEpochSecond(100));
    assertNull(
        store.findBySaga(sid2).get(0).firstReplayStartedAt(),
        "a foreign type's write must never touch this type's entry");
  }

  // --- The replayAll fold lookup ---

  @Test
  void findNullSagaEntriesByResolvedTarget_returnsResolvedNullSagaEntries_offsetAscending() {
    var type = SagaType.of("T");
    var target = SagaId.of("fold-target");
    store.publish(entry(null, type, 30, Instant.EPOCH));
    store.publish(entry(null, type, 10, Instant.EPOCH));
    store.publish(entry(null, type, 20, Instant.EPOCH)); // never resolved
    store.publish(entry(target, type, 40, Instant.EPOCH)); // named: never returned
    store.publish(entry(null, SagaType.of("Other"), 50, Instant.EPOCH)); // foreign type
    store.setResolvedTarget(type, GlobalOffset.of(30), target);
    store.setResolvedTarget(type, GlobalOffset.of(10), target);
    store.setResolvedTarget(SagaType.of("Other"), GlobalOffset.of(50), target);

    var folded = store.findNullSagaEntriesByResolvedTarget(type, target);

    assertEquals(List.of(10L, 30L), folded.stream().map(e -> e.eventOffset().value()).toList());
    assertTrue(folded.stream().allMatch(e -> e.sagaId() == null));
    assertEquals(
        List.of(50L),
        store.findNullSagaEntriesByResolvedTarget(SagaType.of("Other"), target).stream()
            .map(e -> e.eventOffset().value())
            .toList(),
        "the foreign type sees only its own resolved entry");
  }
}
