package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Testcontainers-backed tests for {@link PostgresSagaDeadLetterStore}. Exercises publish /
 * findBySaga / findAll against a real PostgreSQL instance provisioned by the event-store baseline.
 */
@Testcontainers
class PostgresSagaDeadLetterStoreTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_saga_dlq_test");

  static PGSimpleDataSource dataSource;
  PostgresSagaDeadLetterStore store;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // Apply the shipped event-store baseline (creates saga_dead_letters) via the factory
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
    store = new PostgresSagaDeadLetterStore(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_dead_letters");
      stmt.execute("DELETE FROM saga_state");
    }
  }

  private void insertSagaRow(String sagaId, String sagaType, String status) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version)"
                    + " VALUES (?, ?, ?, '{}'::jsonb, 1)")) {
      ps.setString(1, sagaId);
      ps.setString(2, sagaType);
      ps.setString(3, status);
      ps.executeUpdate();
    }
  }

  private SagaDeadLetterStore.SagaDeadLetterEntry entry(
      SagaId sagaId, SagaType sagaType, long offset, String errorMessage, Instant faultedAt) {
    return new SagaDeadLetterStore.SagaDeadLetterEntry(
        sagaId,
        sagaType,
        GlobalOffset.of(offset),
        EventType.of("OrderCreated"),
        "java.lang.RuntimeException",
        errorMessage,
        faultedAt);
  }

  @Test
  void publish_then_findBySaga_roundtrip() {
    var sid = SagaId.of("saga-order-1");
    var type = SagaType.of("OrderFulfillmentSaga");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

    store.publish(entry(sid, type, 1L, "boom", now));

    var results = store.findBySaga(sid);
    assertEquals(1, results.size());
    var e = results.get(0);
    assertEquals(sid, e.sagaId());
    assertEquals(type, e.sagaType());
    assertEquals(GlobalOffset.of(1L), e.eventOffset());
    assertEquals(EventType.of("OrderCreated"), e.eventType());
    assertEquals("java.lang.RuntimeException", e.errorType());
    assertEquals("boom", e.errorMessage());
    assertThat(e.faultedAt())
        .isCloseTo(now, org.assertj.core.api.Assertions.within(1, ChronoUnit.SECONDS));
  }

  @Test
  void publish_then_findAll_includes_entry() {
    var sid = SagaId.of("saga-order-2");
    store.publish(entry(sid, SagaType.of("T"), 10L, "err", Instant.now()));

    var all = store.findAll(10);
    assertEquals(1, all.size());
    assertEquals(sid, all.get(0).sagaId());
  }

  @Test
  void publish_isIdempotent_onSameSagaAndOffset() {
    var sid = SagaId.of("saga-idempotent");
    var off = GlobalOffset.of(42L);
    var type = SagaType.of("T");

    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sid,
            type,
            off,
            EventType.of("E"),
            "java.lang.RuntimeException",
            "first",
            Instant.now()));
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sid,
            type,
            off,
            EventType.of("E"),
            "java.lang.RuntimeException",
            "second",
            Instant.now()));

    assertEquals(1, store.findBySaga(sid).size()); // upsert, not duplicate
  }

  @Test
  void findBySaga_filters_by_saga_id() {
    var s1 = SagaId.of("saga-filter-1");
    var s2 = SagaId.of("saga-filter-2");
    store.publish(entry(s1, SagaType.of("T"), 100L, "err", Instant.now()));
    store.publish(entry(s2, SagaType.of("T"), 200L, "err", Instant.now()));

    assertEquals(1, store.findBySaga(s1).size());
    assertEquals(1, store.findBySaga(s2).size());
    assertTrue(store.findBySaga(SagaId.of("unknown")).isEmpty());
  }

  @Test
  void findAll_respects_limit() {
    var sid = SagaId.of("saga-limit");
    store.publish(entry(sid, SagaType.of("T"), 1L, "a", Instant.now()));
    store.publish(entry(sid, SagaType.of("T"), 2L, "b", Instant.now()));
    store.publish(entry(sid, SagaType.of("T"), 3L, "c", Instant.now()));

    assertEquals(2, store.findAll(2).size());
  }

  @Test
  void publish_null_sagaId_entry_roundtrips() {
    // null sagaId = routing failure; keyed by offset alone
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("T"),
            GlobalOffset.of(999L),
            EventType.of("UnknownEvent"),
            "java.lang.IllegalArgumentException",
            "no saga found",
            Instant.now()));

    var all = store.findAll(10);
    assertEquals(1, all.size());
    assertNull(all.get(0).sagaId());
    assertEquals(SagaType.of("T"), all.get(0).sagaType());
  }

  @Test
  void constructor_rejects_null_dataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresSagaDeadLetterStore(null));
  }

  @Test
  void publish_rejects_null_entry() {
    assertThrows(IllegalArgumentException.class, () -> store.publish(null));
  }

  /**
   * GDPR contract: {@code saga_dead_letters} carries only metadata (offset + type, not payload).
   * The event payload is never duplicated here — it remains solely in the crypto-governed {@code
   * event_stream} table, reachable via the quarantined entry's {@code eventOffset}. This is
   * verified two ways: (1) the table schema has no {@code event_payload} column, so a plaintext PII
   * copy is not even representable; (2) a publish/round-trip carries offset + type but no payload
   * accessor exists on {@link SagaDeadLetterStore.SagaDeadLetterEntry} (the record carries only
   * metadata — none of its components is a payload). {@code pre_fault_status} lives on {@code
   * saga_state}, not here.
   */
  @Test
  void saga_dead_letters_table_has_no_event_payload_column() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT column_name FROM information_schema.columns "
                    + "WHERE table_name = 'saga_dead_letters'")) {
      var columns = new java.util.ArrayList<String>();
      while (rs.next()) {
        columns.add(rs.getString("column_name"));
      }
      assertThat(columns)
          .as("saga_dead_letters columns")
          .containsExactlyInAnyOrder(
              "saga_id",
              "saga_type",
              "event_offset",
              "event_type",
              "error_type",
              "error_message",
              "faulted_at",
              "first_faulted_at",
              // The immutable first-replay-attempt anchor — a timestamp, not
              // payload.
              "first_replay_started_at",
              "target_saga_id")
          .doesNotContain("event_payload");
    }
  }

  @Test
  void quarantine_roundtrip_carries_offset_and_type_but_no_payload() {
    // The offending event's payload lives only in event_stream, addressable via eventOffset —
    // this store never sees or persists it. A round-trip proves offset/type/error metadata
    // survive; there is no payload field to assert absent on the entry (compile-time reality:
    // SagaDeadLetterEntry has 7 components, none of them a payload).
    var sid = SagaId.of("saga-gdpr-1");
    var type = SagaType.of("OrderFulfillmentSaga");
    var offset = GlobalOffset.of(4242L);

    store.publish(entry(sid, type, 4242L, "boom", Instant.now()));

    var e = store.findBySaga(sid).get(0);
    assertEquals(offset, e.eventOffset());
    assertEquals(EventType.of("OrderCreated"), e.eventType());
    // To inspect the actual event data for this entry, an operator reads event_stream at
    // e.eventOffset() — post-shred, that read correctly comes back redacted/unreadable.
  }

  @Test
  void discard_removesExactEntry_returnsTrue() {
    var sid = SagaId.of("saga-discard-1");
    var type = SagaType.of("T");
    store.publish(entry(sid, type, 7L, "boom", Instant.now()));
    store.publish(entry(sid, type, 9L, "boom", Instant.now()));

    assertTrue(store.discard(sid, type, GlobalOffset.of(7L)));

    var remaining = store.findBySaga(sid);
    assertEquals(1, remaining.size());
    assertEquals(GlobalOffset.of(9L), remaining.get(0).eventOffset());
  }

  @Test
  void discard_missingEntry_returnsFalse() {
    var sid = SagaId.of("saga-discard-2");
    store.publish(entry(sid, SagaType.of("T"), 7L, "boom", Instant.now()));

    assertFalse(store.discard(sid, SagaType.of("T"), GlobalOffset.of(999L)));
  }

  @Test
  void discard_nullSagaEntry_matchesByOffsetWithNullSagaId() {
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("T"),
            GlobalOffset.of(1234L),
            EventType.of("UnknownEvent"),
            "java.lang.IllegalArgumentException",
            "no saga found",
            Instant.now()));

    assertTrue(store.discard(null, SagaType.of("T"), GlobalOffset.of(1234L)));
    assertTrue(store.findAll(10).isEmpty());

    // null must NOT match a non-null saga row at the same offset
    var s1 = SagaId.of("saga-discard-3");
    store.publish(entry(s1, SagaType.of("T"), 1234L, "boom", Instant.now()));
    assertFalse(store.discard(null, SagaType.of("T"), GlobalOffset.of(1234L)));
  }

  /**
   * Pins the accepted production divergence documented on {@link PostgresSagaDeadLetterStore}: SQL
   * treats two {@code NULL} values as distinct, so re-publishing a null-saga entry at the same
   * offset does NOT upsert like the non-null case — it inserts a second row. This is a deliberate,
   * accepted edge case (rare routing failures before a saga could be identified), not a bug to fix.
   * {@code discard(null, type, offset)} uses {@code IS NOT DISTINCT FROM}, so it removes both rows
   * at once; the retention sweeper ({@code deleteOlderThan}) independently clears any stray
   * duplicates that accumulate across repeated failed replays. The accumulation itself stays
   * accepted — but each fresh row is born carrying the set's immutable evidence, pinned separately
   * in {@code publish_nullSagaRequarantine_newRowCarriesAnchorAndFirstFault}.
   */
  @Test
  void publish_nullSagaEntry_sameOffsetTwice_insertsTwoRows_discardRemovesBoth() {
    var offset = GlobalOffset.of(5555L);

    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("T"),
            offset,
            EventType.of("UnknownEvent"),
            "java.lang.IllegalArgumentException",
            "no saga found (attempt 1)",
            Instant.now()));
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("T"),
            offset,
            EventType.of("UnknownEvent"),
            "java.lang.IllegalArgumentException",
            "no saga found (attempt 2)",
            Instant.now()));

    // SQL NULL-distinct semantics: two null-saga rows at the same offset are independent
    // entries, not an upsert — divergence from InMemorySagaDeadLetterStore, accepted.
    var all = store.findAll(10);
    assertEquals(2, all.size());
    assertTrue(all.stream().allMatch(e -> e.sagaId() == null));
    assertTrue(all.stream().allMatch(e -> e.eventOffset().equals(offset)));

    // discard(null, type, offset) matches via IS NOT DISTINCT FROM — clears both stray rows at
    // once.
    assertTrue(store.discard(null, SagaType.of("T"), offset));
    assertTrue(store.findAll(10).isEmpty());
  }

  @Test
  void deleteOlderThan_removesStrictlyOlder_returnsCount() throws Exception {
    var sid = SagaId.of("saga-retention");
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Instant tMinus1d = now.minus(1, java.time.temporal.ChronoUnit.DAYS);
    Instant tMinus2d = now.minus(2, java.time.temporal.ChronoUnit.DAYS);

    // A named entry with NO saga row is a row-less FAULTED saga's record and is
    // shielded; ordinary prunable data belongs to a saga that recovered or terminalized.
    insertSagaRow("saga-retention", "T", "COMPLETED");
    store.publish(entry(sid, type, 1L, "a", tMinus2d));
    store.publish(entry(sid, type, 2L, "b", tMinus1d));
    store.publish(entry(sid, type, 3L, "c", now));

    int removed = store.deleteOlderThan(tMinus1d);

    assertEquals(1, removed); // only strictly-before tMinus1d removed
    var remaining = store.findBySaga(sid);
    assertEquals(2, remaining.size());
    assertTrue(remaining.stream().noneMatch(e -> e.faultedAt().equals(tMinus2d)));
  }

  // --- Immutable first_faulted_at across re-quarantine upserts ---

  @Test
  void publish_upsert_refreshesFaultedAt_butNeverResetsFirstFaultedAt() {
    var sid = SagaId.of("saga-first-fault");
    var type = SagaType.of("T");
    Instant day0 = Instant.now().truncatedTo(ChronoUnit.MICROS).minus(5, ChronoUnit.DAYS);
    Instant day5 = day0.plus(5, ChronoUnit.DAYS);

    store.publish(entry(sid, type, 1L, "first fault", day0));
    // A failed replay re-quarantines the same key with a fresh faultedAt (day 5).
    store.publish(entry(sid, type, 1L, "still poison", day5));

    var e = store.findBySaga(sid).get(0);
    assertThat(e.faultedAt())
        .as("faulted_at refreshes on re-quarantine (still-poison detection)")
        .isCloseTo(day5, org.assertj.core.api.Assertions.within(1, ChronoUnit.SECONDS));
    assertThat(e.firstFaultedAt())
        .as("first_faulted_at is immutable — a re-quarantine cannot reset the entry's age")
        .isCloseTo(day0, org.assertj.core.api.Assertions.within(1, ChronoUnit.SECONDS));
  }

  @Test
  void deleteOlderThan_prunesOnFirstFaultedAt_reQuarantineCannotExtendRetentionLife()
      throws Exception {
    var sid = SagaId.of("saga-retention-first");
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Instant tMinus8d = now.minus(8, ChronoUnit.DAYS);

    insertSagaRow("saga-retention-first", "T", "COMPLETED"); // Prunable only with a row
    // First faulted 8 days ago; a failed replay re-quarantined it TODAY (fresh faulted_at).
    store.publish(entry(sid, type, 1L, "first fault", tMinus8d));
    store.publish(entry(sid, type, 1L, "still poison", now));

    int removed = store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS));

    assertEquals(
        1,
        removed,
        "the entry's retention life is bounded by its FIRST fault — the fresh faulted_at from the"
            + " re-quarantine must not extend it");
    assertTrue(store.findBySaga(sid).isEmpty());
  }

  @Test
  void publish_sevenArgEntry_defaultsFirstFaultedAtToFaultedAt() {
    var sid = SagaId.of("saga-first-default");
    Instant faultedAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.publish(entry(sid, SagaType.of("T"), 1L, "boom", faultedAt));

    assertThat(store.findBySaga(sid).get(0).firstFaultedAt())
        .isCloseTo(faultedAt, org.assertj.core.api.Assertions.within(1, ChronoUnit.SECONDS));
  }

  // --- Retention must never prune the only replay handle of a FAULTED saga ---

  @Test
  void deleteOlderThan_retainsEntryWhoseSagaIsStillFaulted() throws Exception {
    // Repro: the quarantine entry is the SOLE framework handle for un-faulting a
    // FAULTED saga (replay needs it for pre_fault_status, the offset, and the un-fault step). A
    // fix that ships after the retention window (default 7d, capped <= inbox retention by the
    // boot validator, so it cannot simply be raised) must still find the entry — pruning it
    // strands the saga FAULTED forever, recoverable only by manual saga_state surgery, with
    // replay() reporting ENTRY_NOT_FOUND. Entries whose saga recovered or terminalized — and
    // unresolved null-saga entries — stay prunable. A NAMED entry whose saga has NO row
    // is the sole record of a saga FAULTED without a row and is shielded too.
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Instant old = now.minus(8, ChronoUnit.DAYS);

    insertSagaRow("saga-faulted-x", "T", "FAULTED");
    store.publish(entry(SagaId.of("saga-faulted-x"), type, 1L, "poison", old));
    store.publish(entry(SagaId.of("saga-rowless-y"), type, 2L, "stale", old));
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            type,
            GlobalOffset.of(3L),
            EventType.of("E"),
            "java.lang.RuntimeException",
            "routing failure",
            old));

    int removed = store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS));

    assertEquals(1, removed, "only the unresolved null-saga entry is prunable");
    assertEquals(
        1,
        store.findBySaga(SagaId.of("saga-faulted-x")).size(),
        "the FAULTED saga's only replay handle must survive retention");
    assertEquals(
        1,
        store.findBySaga(SagaId.of("saga-rowless-y")).size(),
        "The row-less FAULTED saga's only record must survive retention too");
    assertEquals(2, store.findAll(10).size());
  }

  // --- The row-less FAULTED population is shielded and counted ---

  @Test
  void deleteOlderThan_retainsNamedEntryOfARowlessSaga_andCountsIt() throws Exception {
    // A saga FAULTED without a row (markFaulted skipped the create — initialState threw) owns
    // exactly this entry; pre-fix it aged out at retention and the backlog gauge read 0.
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.publish(
        entry(
            SagaId.of("saga-rowless-faulted"),
            type,
            1L,
            "initialState threw",
            now.minus(8, ChronoUnit.DAYS)));

    assertEquals(0, store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS)));
    assertEquals(1, store.findBySaga(SagaId.of("saga-rowless-faulted")).size());
    assertEquals(1, store.countFaultedBacklog(), "the gauge sees the row-less FAULTED saga");
  }

  @Test
  void deleteOlderThan_retainsNullSagaEntryWhoseResolvedTargetIsRowlessAndOwnsARecord()
      throws Exception {
    // The keep signal, applied by retention: the null-saga entry's resolved target has
    // no row but owns its own genesis record, so it is FAULTED without a row and still owes the
    // event a delivery. Both entries are shielded and both are counted.
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Instant old = now.minus(8, ChronoUnit.DAYS);
    store.publish(entry(SagaId.of("target-rowless"), type, 1L, "genesis poison", old));
    store.publish(entry(null, type, 2L, "routing failure", old));
    store.setResolvedTarget(type, GlobalOffset.of(2L), SagaId.of("target-rowless"));

    assertEquals(0, store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS)));
    assertEquals(2, store.findAll(10).size());
    assertEquals(2, store.countFaultedBacklog());
  }

  @Test
  void deleteOlderThan_neverPrunesAResolvedNullSagaEntryWhoseTargetNeverStarted_andCountsIt()
      throws Exception {
    // A RESOLVED null-saga entry is operator-attended by construction — shielded
    // and counted even when its target never started (no row, no record); only the replayer's
    // discard or a successful replay removes it.
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.publish(entry(null, type, 3L, "routing failure", now.minus(8, ChronoUnit.DAYS)));
    store.setResolvedTarget(type, GlobalOffset.of(3L), SagaId.of("never-started"));

    assertEquals(1, store.countFaultedBacklog());
    assertEquals(0, store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS)));
    assertEquals(1, store.findAll(10).size());
  }

  @Test
  void deleteOlderThan_prunesOnceSagaLeavesFaulted() throws Exception {
    // The guard protects exactly the FAULTED state: once the saga recovered/terminalized the
    // entry is stale metadata again and retention applies.
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    insertSagaRow("saga-recovered-z", "T", "COMPENSATED");
    store.publish(
        entry(SagaId.of("saga-recovered-z"), type, 1L, "old", now.minus(8, ChronoUnit.DAYS)));

    int removed = store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS));

    assertEquals(1, removed);
    assertTrue(store.findBySaga(SagaId.of("saga-recovered-z")).isEmpty());
  }

  @Test
  void deleteOlderThan_foreignTypeRow_isIrrelevant_entryIsJudgedByItsOwnTypesRow()
      throws Exception {
    // Discipline in the guard: a row owned by a DIFFERENT saga type never protects — or
    // releases — this type's entry; for its own type the saga effectively has no row. The guard
    // then applies the row-less rule to THIS type: TypeA owns a record and has no TypeA row, so
    // TypeA's saga is FAULTED-without-a-row and its entry is shielded — whatever status TypeB's
    // colliding row carries (saga_state is keyed by saga_id alone, so TypeA can never have a row
    // of its own here). The foreign row's status is deliberately the most "releasing" one.
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Instant old = now.minus(8, ChronoUnit.DAYS);
    insertSagaRow("saga-cross-type", "TypeB", "COMPLETED");
    store.publish(entry(SagaId.of("saga-cross-type"), SagaType.of("TypeA"), 1L, "old", old));

    int removed = store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS));

    assertEquals(0, removed, "TypeA's entry is judged by TypeA's (absent) row, not TypeB's");
    assertEquals(1, store.findBySaga(SagaId.of("saga-cross-type")).size());
    assertEquals(1, store.countFaultedBacklog(), "and the gauge counts it under TypeA");
  }

  // --- Null-saga entries protected through their resolved target ---

  private SagaDeadLetterStore.SagaDeadLetterEntry nullSagaEntry(
      SagaType sagaType, long offset, Instant faultedAt) {
    return new SagaDeadLetterStore.SagaDeadLetterEntry(
        null,
        sagaType,
        GlobalOffset.of(offset),
        EventType.of("PaymentCompleted"),
        "java.lang.RuntimeException",
        "correlate poison",
        faultedAt);
  }

  @Test
  void setResolvedTarget_stampsNullSagaEntriesTypeScoped_neverNamedRows() throws Exception {
    var offset = GlobalOffset.of(9100L);
    store.publish(nullSagaEntry(SagaType.of("TypeA"), 9100L, Instant.now()));
    store.publish(nullSagaEntry(SagaType.of("TypeB"), 9100L, Instant.now()));
    store.publish(
        entry(SagaId.of("saga-named-9100"), SagaType.of("TypeA"), 9100L, "x", Instant.now()));

    store.setResolvedTarget(SagaType.of("TypeA"), offset, SagaId.of("target-s"));

    var all = store.findAll(10);
    var typeA =
        all.stream().filter(e -> SagaType.of("TypeA").equals(e.sagaType()) && e.sagaId() == null);
    assertTrue(
        typeA.allMatch(e -> SagaId.of("target-s").equals(e.targetSagaId())),
        "own type's null-saga entry is stamped and round-trips");
    assertTrue(
        all.stream()
            .filter(e -> SagaType.of("TypeB").equals(e.sagaType()))
            .allMatch(e -> e.targetSagaId() == null),
        "a foreign type's null-saga entry is never stamped");
    assertTrue(
        all.stream().filter(e -> e.sagaId() != null).allMatch(e -> e.targetSagaId() == null),
        "a named entry is never stamped — its association IS its saga_id");
  }

  @Test
  void deleteOlderThan_retainsNullSagaEntryWhoseResolvedTargetIsFaulted() throws Exception {
    // The audited pruning: a null-saga entry names no saga_state row, so the
    // FAULTED-owner guard can never protect it — but after TARGET_PENDING persisted the resolved
    // target, the same type-scoped FAULTED check applies to that saga. The entry is the sole
    // record of a never-consumed event the FAULTED target still owes a delivery.
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Instant old = now.minus(8, ChronoUnit.DAYS);
    insertSagaRow("target-faulted-s", "T", "FAULTED");
    store.publish(nullSagaEntry(SagaType.of("T"), 9200L, old));
    store.setResolvedTarget(
        SagaType.of("T"), GlobalOffset.of(9200L), SagaId.of("target-faulted-s"));

    int removed = store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS));

    assertEquals(
        0,
        removed,
        "a null-saga entry whose resolved target is still FAULTED is the sole record of a"
            + " never-consumed event — the sweep must not prune it");
    assertEquals(1, store.findAll(10).size());
  }

  @Test
  void deleteOlderThan_keepsAResolvedNullSagaEntryAfterItsTargetRecovers() throws Exception {
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Instant old = now.minus(8, ChronoUnit.DAYS);
    insertSagaRow("target-recovered-s", "T", "RUNNING");
    store.publish(nullSagaEntry(SagaType.of("T"), 9300L, old));
    store.setResolvedTarget(
        SagaType.of("T"), GlobalOffset.of(9300L), SagaId.of("target-recovered-s"));

    assertEquals(
        0,
        store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS)),
        "a RESOLVED null-saga entry is operator-attended by construction — a"
            + " recovered target does not release it");
  }

  @Test
  void deleteOlderThan_unresolvedNullSagaEntry_staysPrunable() throws Exception {
    // A null-saga entry never replayed carries no resolution: retention eventually wins over
    // unattended records (the FAULTED target has been on the faulted_backlog gauge the whole
    // time via its own named entry).
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.publish(nullSagaEntry(SagaType.of("T"), 9400L, now.minus(8, ChronoUnit.DAYS)));

    assertEquals(1, store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS)));
  }

  /**
   * A concurrent change to a candidate inside the retention DELETE's window shortens a chunk
   * although prunable rows remain. In production that is a discard or a second instance's sweeper
   * (the replayer anchors only entries that are already protected). Here an anchor UPDATE on an
   * unresolved null-saga entry, a state the replayer never anchors, stands in for any such change
   * deterministically: the DELETE matches its candidates by {@code ctid}, the UPDATE moves the row
   * to a new ctid, and READ COMMITTED's EvalPlanQual re-check skips it, so the chunk deletes fewer
   * than its LIMIT. Under the former {@code batch == DELETE_BATCH_SIZE} termination the sweep
   * stopped there and left the updated entry plus every prunable entry past the first chunk behind.
   */
  @Test
  void deleteOlderThan_keepsDrainingWhenAConcurrentAnchorWriteShortensAChunk() throws Exception {
    var type = SagaType.of("H3DrainT");
    Instant old = Instant.now().truncatedTo(ChronoUnit.MICROS).minus(30, ChronoUnit.DAYS);
    int prunable = 1500;
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO saga_dead_letters (saga_id, saga_type, event_offset, event_type,"
                    + " error_type, error_message, faulted_at, first_faulted_at) VALUES"
                    + " (NULL, ?, ?, 'PoisonEvent', 'java.lang.RuntimeException', 'boom', ?, ?)")) {
      for (int i = 0; i < prunable; i++) {
        ps.setString(1, type.value());
        ps.setLong(2, 70_000L + i);
        ps.setTimestamp(3, java.sql.Timestamp.from(old.plusMillis(i)));
        ps.setTimestamp(4, java.sql.Timestamp.from(old.plusMillis(i)));
        ps.addBatch();
      }
      ps.executeBatch();
    }

    int removed =
        RowLockRaces.sweepWhileAWriterHoldsARow(
            dataSource,
            "UPDATE saga_dead_letters SET first_replay_started_at ="
                + " COALESCE(first_replay_started_at, NOW())"
                + " WHERE saga_id IS NULL AND event_offset = ?::bigint AND saga_type = ?",
            () -> store.deleteOlderThan(Instant.now().minus(7, ChronoUnit.DAYS)),
            "70000",
            type.value());

    assertEquals(prunable, removed, "the concurrently anchored entry is still prunable");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT count(*) FROM saga_dead_letters");
        var rs = ps.executeQuery()) {
      rs.next();
      assertEquals(0L, rs.getLong(1), "no prunable entry may outlive the sweep");
    }
  }

  @Test
  void deleteOlderThan_resolvedNullSagaEntry_isShieldedWhateverTheTargetsTypeOrStatus()
      throws Exception {
    // The resolution itself is the shield — the target row's type and status are
    // not consulted (the replayer only ever resolves through the entry's own type's routers).
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    insertSagaRow("target-cross-s", "TypeB", "FAULTED");
    store.publish(nullSagaEntry(SagaType.of("TypeA"), 9500L, now.minus(8, ChronoUnit.DAYS)));
    store.setResolvedTarget(
        SagaType.of("TypeA"), GlobalOffset.of(9500L), SagaId.of("target-cross-s"));

    assertEquals(0, store.deleteOlderThan(now.minus(7, ChronoUnit.DAYS)));
  }

  @Test
  void publish_reQuarantine_keepsResolvedTarget() throws Exception {
    // A re-poison of the same null-saga entry must not strip the retention protection an earlier
    // TARGET_PENDING resolution established (stored value wins on conflict, like firstFaultedAt).
    // NB: null-saga re-publish INSERTS a second row on Postgres (NULL-distinct) rather than
    // conflicting — so this pins the NAMED-row conflict path plus the null-saga stamp surviving
    // alongside the new row.
    var sid = SagaId.of("saga-target-keep");
    store.publish(entry(sid, SagaType.of("T"), 9600L, "boom", Instant.now()));
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "UPDATE saga_dead_letters SET target_saga_id = 'kept-target'"
              + " WHERE saga_id = 'saga-target-keep'");
    }

    store.publish(entry(sid, SagaType.of("T"), 9600L, "boom again", Instant.now()));

    assertEquals(
        SagaId.of("kept-target"),
        store.findBySaga(sid).get(0).targetSagaId(),
        "the upsert conflict must keep the STORED resolved target");
  }

  @Test
  void countFaultedBacklog_countsTargetFaultedNullSagaEntries() throws Exception {
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    insertSagaRow("target-bk-s", "T", "FAULTED");
    store.publish(nullSagaEntry(SagaType.of("T"), 9700L, now));
    assertEquals(0, store.countFaultedBacklog(), "unresolved null-saga entry is not counted");

    store.setResolvedTarget(SagaType.of("T"), GlobalOffset.of(9700L), SagaId.of("target-bk-s"));

    assertEquals(
        1,
        store.countFaultedBacklog(),
        "a null-saga entry whose resolved target is FAULTED is stranded work and must be visible");
  }

  // --- FAULTED backlog = the retention guard's complement ---

  @Test
  void countFaultedBacklog_countsExactlyTheEntriesTheRetentionGuardProtects() throws Exception {
    // The sweep now PROTECTS these entries, so they accumulate silently — SAGA_FAULTED is a
    // counter fired once at fault time, not a standing backlog. This count is the NOT-EXISTS
    // guard's complement, with identical scoping (own-type only; a named entry with no
    // own-type row is a row-less FAULTED saga's record and IS counted) but WITHOUT any age
    // filter: the question is "how many sagas are stranded right now?".
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

    insertSagaRow("saga-bk-faulted", "T", "FAULTED");
    insertSagaRow("saga-bk-recovered", "T", "COMPENSATED");
    insertSagaRow("saga-bk-cross", "TypeB", "FAULTED");

    store.publish(entry(SagaId.of("saga-bk-faulted"), type, 1L, "poison", now)); // counted
    store.publish(entry(SagaId.of("saga-bk-faulted"), type, 2L, "poison2", now)); // counted
    store.publish(entry(SagaId.of("saga-bk-recovered"), type, 3L, "old", now)); // not FAULTED
    store.publish(
        entry(SagaId.of("saga-bk-rowless"), type, 4L, "stale", now)); // counted: row-less FAULTED
    store.publish(
        entry(SagaId.of("saga-bk-cross"), SagaType.of("TypeA"), 5L, "x", now)); // counted: no
    // TypeA row (the TypeB row is a different saga type's)
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            type,
            GlobalOffset.of(6L),
            EventType.of("E"),
            "java.lang.RuntimeException",
            "routing failure",
            now)); // null-saga, unresolved: names no row and no target — not counted

    assertEquals(4, store.countFaultedBacklog());
  }

  @Test
  void countFaultedBacklog_dropsToZeroOnceTheSagaLeavesFaulted() throws Exception {
    var type = SagaType.of("T");
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    insertSagaRow("saga-bk-replayed", "T", "FAULTED");
    store.publish(entry(SagaId.of("saga-bk-replayed"), type, 1L, "poison", now));
    assertEquals(1, store.countFaultedBacklog());

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("UPDATE saga_state SET status = 'RUNNING' WHERE saga_id = 'saga-bk-replayed'");
    }

    assertEquals(
        0, store.countFaultedBacklog(), "a replayed/un-faulted saga leaves the stranded backlog");
  }

  // --- Retention follows the shield ---

  /** Minimal saga state for creating shield/FAULTED rows through {@link PostgresSagaStore}. */
  record ShieldState(SagaStatus status, String marker) implements SagaState {}

  private static final Instant T0 = Instant.parse("2026-01-10T00:00:00Z");

  private static PostgresSagaStore sagaRows() {
    return new PostgresSagaStore(
        dataSource, new ObjectMapper().registerModule(new JavaTimeModule()));
  }

  @Test
  void deleteOlderThan_neverPrunesEntriesOfAFaultedOrShieldedOwner() {
    var rows = sagaRows();
    var a = SagaId.of("dl-saga-a");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    rows.setDeadLetterPending(a, type, true);
    store.publish(entry(a, type, 10L, "boom", T0));
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    rows.setDeadLetterPending(a, type, false);
    rows.markFaulted(a, type, 1L);
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  void deleteOlderThan_prunesEntriesOfAHealthyUnshieldedOwner_andUnroutableNullEntries() {
    var rows = sagaRows();
    var a = SagaId.of("dl-saga-a");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    store.publish(entry(a, type, 10L, "boom", T0));
    store.publish(nullSagaEntry(type, 11L, T0)); // no target: unroutable garbage
    assertEquals(0, store.deleteOlderThan(T0.minusSeconds(1)));
    assertEquals(2, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  void deleteOlderThan_neverPrunesANamedEntryWhoseSagaHasNoRow() {
    // the row-less residual: initialState threw
    store.publish(entry(SagaId.of("dl-saga-a"), SagaType.of("T"), 10L, "boom", T0));
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  void deleteOlderThan_neverPrunesAResolvedNullEntry_whateverItsTargetLooksLike() {
    var rows = sagaRows();
    var a = SagaId.of("dl-saga-a");
    var type = SagaType.of("T");
    store.publish(nullSagaEntry(type, 40L, T0));
    store.setResolvedTarget(type, GlobalOffset.of(40L), a); // target row absent: still resolved
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    // healthy, unshielded target: still resolved
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
    store.discard(null, type, GlobalOffset.of(40L));
    store.publish(nullSagaEntry(type, 41L, T0)); // unresolved: garbage after the cutoff
    assertEquals(1, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  @Test
  void countFaultedBacklog_countsShieldedOrFaultedOwners_rowlessNamed_andResolvedNull() {
    var rows = sagaRows();
    var a = SagaId.of("dl-saga-a");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    store.publish(entry(a, type, 10L, "boom", T0)); // healthy, unshielded: not counted
    assertEquals(0L, store.countFaultedBacklog());
    rows.setDeadLetterPending(a, type, true);
    assertEquals(1L, store.countFaultedBacklog());
    store.publish(entry(SagaId.of("dl-rowless"), type, 12L, "x", T0)); // row-less named: counted
    store.publish(nullSagaEntry(type, 13L, T0)); // unresolved: not counted
    assertEquals(2L, store.countFaultedBacklog());
    store.setResolvedTarget(type, GlobalOffset.of(13L), a); // resolved: counted
    assertEquals(3L, store.countFaultedBacklog());
  }

  // --- Type-scoped discard ---

  @Test
  void discard_typeScoped_neverDeletesForeignTypeRow() {
    var sid = SagaId.of("saga-typed-discard");
    store.publish(entry(sid, SagaType.of("TypeB"), 1L, "boom", Instant.now()));

    assertFalse(
        store.discard(sid, SagaType.of("TypeA"), GlobalOffset.of(1L)),
        "a foreign type must never delete another type's quarantine record");
    assertEquals(1, store.findBySaga(sid).size());

    assertTrue(store.discard(sid, SagaType.of("TypeB"), GlobalOffset.of(1L)));
    assertTrue(store.findBySaga(sid).isEmpty());
  }

  @Test
  void discard_typeScoped_nullSagaEntry_matchesOnlyNullSagaRows() {
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("T"),
            GlobalOffset.of(3L),
            EventType.of("E"),
            "java.lang.RuntimeException",
            "routing failure",
            Instant.now()));
    store.publish(entry(SagaId.of("saga-named-3"), SagaType.of("T"), 3L, "boom", Instant.now()));

    assertTrue(store.discard(null, SagaType.of("T"), GlobalOffset.of(3L)));
    assertEquals(
        1, store.findBySaga(SagaId.of("saga-named-3")).size(), "the named row must survive");
  }

  @Test
  void discard_typeScoped_nullSagaEntries_twoTypesSameOffset_removesOnlyOwnRow() {
    // Two saga types poisoning on the SAME event at the same offset produce two
    // null-saga rows (NULL-distinct unique semantics insert both). Now that SagaRunner stamps the
    // quarantining type on every null-saga entry, one type's replay-then-discard must remove only
    // its OWN row — never the other type's sole record of the never-consumed event.
    var offset = GlobalOffset.of(4242L);
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("TypeA"),
            offset,
            EventType.of("E"),
            "java.lang.RuntimeException",
            "A's correlate poison",
            Instant.now()));
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            null,
            SagaType.of("TypeB"),
            offset,
            EventType.of("E"),
            "java.lang.RuntimeException",
            "B's correlate poison",
            Instant.now()));
    assertEquals(2, store.findAll(10).size(), "NULL-distinct semantics keep both typed rows");

    assertTrue(store.discard(null, SagaType.of("TypeA"), offset));

    var remaining = store.findAll(10);
    assertEquals(1, remaining.size(), "B's null-saga record must survive A's typed discard");
    assertEquals(SagaType.of("TypeB"), remaining.get(0).sagaType());
  }

  // --- The replayAll fold lookup ---

  @Test
  void findNullSagaEntriesByResolvedTarget_returnsResolvedNullSagaEntries_offsetAscending() {
    var type = SagaType.of("T");
    var target = SagaId.of("saga-fold-target");
    store.publish(entry(null, type, 30L, "boom", Instant.now()));
    store.publish(entry(null, type, 10L, "boom", Instant.now()));
    store.publish(entry(null, type, 20L, "boom", Instant.now())); // never resolved
    store.publish(entry(target, type, 40L, "boom", Instant.now())); // named: never returned
    store.setResolvedTarget(type, GlobalOffset.of(30L), target);
    store.setResolvedTarget(type, GlobalOffset.of(10L), target);
    store.setResolvedTarget(type, GlobalOffset.of(20L), SagaId.of("someone-else"));

    var folded = store.findNullSagaEntriesByResolvedTarget(type, target);

    assertThat(folded).extracting(e -> e.eventOffset().value()).containsExactly(10L, 30L);
    assertThat(folded).allMatch(e -> e.sagaId() == null);
  }

  @Test
  void findNullSagaEntriesByResolvedTarget_newestRowPerOffset_andTypeScoped() {
    var type = SagaType.of("T");
    var target = SagaId.of("saga-fold-target2");
    // Two duplicate null-saga rows at one offset (NULL-distinct accumulation); the newest wins.
    Instant older = Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MICROS);
    Instant newer = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.publish(entry(null, type, 10L, "first", older));
    store.publish(entry(null, type, 10L, "second", newer));
    store.setResolvedTarget(type, GlobalOffset.of(10L), target);
    // A foreign type's resolved entry at another offset must never be folded into this type.
    store.publish(entry(null, SagaType.of("Other"), 20L, "boom", Instant.now()));
    store.setResolvedTarget(SagaType.of("Other"), GlobalOffset.of(20L), target);

    var folded = store.findNullSagaEntriesByResolvedTarget(type, target);

    assertThat(folded).hasSize(1);
    assertThat(folded.get(0).eventOffset().value()).isEqualTo(10L);
    assertThat(folded.get(0).errorMessage()).as("newest row of the set").isEqualTo("second");
    assertThat(store.findNullSagaEntriesByResolvedTarget(SagaType.of("Other"), target))
        .as("the foreign type sees only its own resolved entry")
        .extracting(e -> e.eventOffset().value())
        .containsExactly(20L);
  }

  // --- Establish-only anchor write, called before every replay attempt ---

  @Test
  void establishFirstReplayAnchor_setsAnchorWhenAbsent() {
    var sid = SagaId.of("saga-est-anchor");
    var type = SagaType.of("T");
    store.publish(entry(sid, type, 1L, "boom", Instant.now()));

    Instant anchor = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.establishFirstReplayAnchor(sid, type, GlobalOffset.of(1L), anchor);

    var e = store.findBySaga(sid).get(0);
    assertThat(e.firstReplayStartedAt())
        .as("the anchor is established")
        .isCloseTo(anchor, org.assertj.core.api.Assertions.within(1, ChronoUnit.SECONDS));
  }

  @Test
  void establishFirstReplayAnchor_keepsAnExistingAnchor() {
    var sid = SagaId.of("saga-est-anchor2");
    var type = SagaType.of("T");
    store.publish(entry(sid, type, 1L, "boom", Instant.now()));
    Instant first = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.establishFirstReplayAnchor(sid, type, GlobalOffset.of(1L), first);

    store.establishFirstReplayAnchor(
        sid, type, GlobalOffset.of(1L), first.plus(3, ChronoUnit.DAYS));

    assertThat(store.findBySaga(sid).get(0).firstReplayStartedAt())
        .as("establish-only: the stored anchor always wins (stamp-once)")
        .isCloseTo(first, org.assertj.core.api.Assertions.within(1, ChronoUnit.SECONDS));
  }

  @Test
  void establishFirstReplayAnchor_isTypeScoped() {
    var sid = SagaId.of("saga-est-anchor3");
    store.publish(entry(sid, SagaType.of("TypeA"), 1L, "boom", Instant.now()));

    store.establishFirstReplayAnchor(sid, SagaType.of("TypeB"), GlobalOffset.of(1L), Instant.now());

    assertNull(
        store.findBySaga(sid).get(0).firstReplayStartedAt(),
        "a foreign type's write must never touch this type's entry");
  }

  @Test
  void publish_reQuarantine_keepsFirstReplayAnchor_andConflictNeverForgesOne() {
    // The STORED anchor survives a re-quarantine, and an incoming publish input can
    // never SET one on an existing row — establishFirstReplayAnchor is the sole writer for
    // existing rows.
    var sid = SagaId.of("saga-first-anchor-keep");
    var type = SagaType.of("T");
    store.publish(entry(sid, type, 1L, "boom", Instant.now()));
    Instant first = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.establishFirstReplayAnchor(sid, type, GlobalOffset.of(1L), first);

    store.publish(entry(sid, type, 1L, "still poison", Instant.now())); // re-quarantine

    assertThat(store.findBySaga(sid).get(0).firstReplayStartedAt())
        .as("the re-quarantine upsert must keep the stored anchor")
        .isCloseTo(first, org.assertj.core.api.Assertions.within(1, ChronoUnit.SECONDS));

    var sid2 = SagaId.of("saga-first-anchor-no-forge");
    store.publish(entry(sid2, type, 2L, "boom", Instant.now()));
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sid2,
            type,
            GlobalOffset.of(2L),
            EventType.of("E"),
            "java.lang.RuntimeException",
            "boom again",
            Instant.now(),
            null,
            Instant.now(), // incoming anchor must be ignored on conflict
            null));
    assertNull(
        store.findBySaga(sid2).get(0).firstReplayStartedAt(),
        "an upsert conflict must keep the STORED anchor (null here) — publish never stamps");
  }

  // --- The first-fault anchor and the saga type are never NULL in storage ---

  @Test
  void firstFaultedAtAndSagaTypeAreNotNullColumns() throws Exception {
    // Every writer binds both (publish normalizes a null first-fault candidate to faultedAt, and
    // SagaDeadLetterEntry requires its saga type), so the baseline declares them NOT NULL: the
    // retention sweep prunes on first_faulted_at directly and every type-scoped statement matches
    // saga_type by equality.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT column_name, is_nullable FROM information_schema.columns"
                    + " WHERE table_schema = current_schema() AND table_name = 'saga_dead_letters'"
                    + " AND column_name IN ('first_faulted_at', 'saga_type')")) {
      var nullability = new java.util.HashMap<String, String>();
      while (rs.next()) {
        nullability.put(rs.getString("column_name"), rs.getString("is_nullable"));
      }
      assertEquals(java.util.Map.of("first_faulted_at", "NO", "saga_type", "NO"), nullability);
    }
  }

  @Test
  void publish_explicitNullFirstFaultedAt_normalizesToFaultedAt() {
    // An 8-arg entry with an explicit null firstFaultedAt must normalize to faultedAt on insert —
    // first_faulted_at is NOT NULL, so publish must never bind SQL NULL.
    var sid = SagaId.of("saga-first-null");
    Instant faultedAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
    store.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sid,
            SagaType.of("T"),
            GlobalOffset.of(1L),
            EventType.of("E"),
            "java.lang.RuntimeException",
            "boom",
            faultedAt,
            null));

    assertThat(store.findBySaga(sid).get(0).firstFaultedAt())
        .isCloseTo(faultedAt, org.assertj.core.api.Assertions.within(1, ChronoUnit.SECONDS));
  }

  // --- Null-saga re-quarantine carries the set's evidence ---

  private SagaDeadLetterStore.SagaDeadLetterEntry nullSagaEntry(
      SagaType sagaType, long offset, String errorMessage, Instant faultedAt) {
    return new SagaDeadLetterStore.SagaDeadLetterEntry(
        null,
        sagaType,
        GlobalOffset.of(offset),
        EventType.of("PaymentCompleted"),
        "java.lang.RuntimeException",
        errorMessage,
        faultedAt);
  }

  /**
   * A null-saga re-quarantine INSERTs a fresh row (NULL-distinct semantics), but the new row must
   * be BORN carrying the duplicate set's immutable evidence — the oldest {@code
   * first_replay_started_at} anchor and the oldest {@code first_faulted_at}. The replayer's key-age
   * guard reads the NEWEST row of the set, so a born-NULL anchor on the newest row would launder a
   * prior attempt's evidence and let a plain replay past the inbox window re-drive pruned forward
   * keys (duplicate CapturePayment). The mutable columns stay per-row: {@code faulted_at} fresh
   * (load-bearing for the still-poison detection).
   */
  @Test
  void publish_nullSagaRequarantine_newRowCarriesAnchorAndFirstFault() {
    var type = SagaType.of("CarryT");
    var offset = GlobalOffset.of(9500L);
    Instant t0 = Instant.now().truncatedTo(ChronoUnit.MICROS).minus(10, ChronoUnit.DAYS);
    Instant t1 = t0.plus(2, ChronoUnit.DAYS);
    Instant t2 = t0.plus(4, ChronoUnit.DAYS);

    // Row R1: original quarantine at t0; a replay attempt then establishes the immutable anchor
    // (t1).
    store.publish(nullSagaEntry(type, 9500L, "first poison", t0));
    store.establishFirstReplayAnchor(null, type, offset, t1);

    // The attempt's feed re-poisons: the re-quarantine INSERTs R2.
    store.publish(nullSagaEntry(type, 9500L, "second poison", t2));

    var rows = store.findAll(10);
    assertEquals(2, rows.size());
    var r2 =
        rows.stream()
            .filter(e -> "second poison".equals(e.errorMessage()))
            .findFirst()
            .orElseThrow();
    assertEquals(
        t1, r2.firstReplayStartedAt(), "the new row must carry the set's oldest anchor (t1)");
    assertEquals(
        t0,
        r2.firstFaultedAt(),
        "the new row must carry the set's oldest first fault — a re-quarantine never extends the"
            + " entry's retention life");
    assertEquals(t2, r2.faultedAt(), "faulted_at stays fresh (still-poison detection)");
  }

  @Test
  void publish_nullSagaCarryForward_takesTheOLDESTAnchor_notTheNewest() {
    // Direction pin: the carry must be MIN, never MAX. Every other carry test builds a
    // set with at most ONE anchored row, so a MAX implementation would pass them all — while the
    // direction is load-bearing in a reachable state. A MAX carry hands the replayer's key-age
    // guard the YOUNGER anchor and grants a re-drive against forward keys aging from the OLDER
    // attempt: the audited duplicate execution, through a plain non-forced replay.
    //
    // Reachable mixed population: R1 anchored t1; R2 inserted by raw SQL with the NULL anchor the
    // snapshot race documented on NULL_SAGA_INSERT leaves; a later attempt's establish-only
    // COALESCE then establishes t2 on R2 ONLY, because it establishes a MISSING anchor and keeps an
    // existing one. The set then carries anchors {t1, t2}, and the next re-quarantine must inherit
    // t1.
    var type = SagaType.of("MinMaxT");
    var offset = GlobalOffset.of(9520L);
    Instant t0 = Instant.now().truncatedTo(ChronoUnit.MICROS).minus(20, ChronoUnit.DAYS);
    Instant t1 = t0.plus(1, ChronoUnit.DAYS);
    Instant t2 = t0.plus(9, ChronoUnit.DAYS);
    Instant t3 = t0.plus(12, ChronoUnit.DAYS);

    store.publish(nullSagaEntry(type, 9520L, "first poison", t0));
    store.establishFirstReplayAnchor(null, type, offset, t1); // row R1: anchor t1
    insertLaunderedNullSagaRow(type, 9520L, "raced duplicate", t0.plus(2, ChronoUnit.DAYS));
    store.establishFirstReplayAnchor(null, type, offset, t2); // R1 keeps t1; R2 (anchor-less) -> t2

    var anchorsBefore =
        store.findAll(10).stream()
            .map(SagaDeadLetterStore.SagaDeadLetterEntry::firstReplayStartedAt)
            .collect(java.util.stream.Collectors.toSet());
    assertEquals(
        java.util.Set.of(t1, t2), anchorsBefore, "the set must carry two DISTINCT anchors here");

    store.publish(nullSagaEntry(type, 9520L, "third poison", t3));

    var r3 =
        store.findAll(10).stream()
            .filter(e -> "third poison".equals(e.errorMessage()))
            .findFirst()
            .orElseThrow();
    assertEquals(
        t1,
        r3.firstReplayStartedAt(),
        "the carry must take the OLDEST anchor (t1) — MAX would hand the guard t2 and license a"
            + " re-drive against keys aging from t1");
    assertEquals(t0, r3.firstFaultedAt(), "and the OLDEST first fault, for the same reason");
  }

  /**
   * Writes a null-saga row with a NULL anchor, as the snapshot race documented on {@code
   * NULL_SAGA_INSERT} leaves it (a re-quarantine whose statement snapshot precedes a concurrent
   * {@code establishFirstReplayAnchor} commit). The row's own {@code first_faulted_at} and NULL
   * target differ from a raced row, which carries the set's first fault and may inherit its target,
   * but they do not affect the anchor MIN under test. Bypasses {@code publish} deliberately: a
   * sequential publish never produces a NULL anchor in an anchored set.
   */
  private static void insertLaunderedNullSagaRow(
      SagaType type, long offset, String errorMessage, Instant faultedAt) {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO saga_dead_letters (saga_id, saga_type, event_offset, event_type,"
                    + " error_type, error_message, faulted_at, first_faulted_at,"
                    + " first_replay_started_at, target_saga_id) VALUES"
                    + " (NULL, ?, ?, 'PoisonEvent', 'java.lang.RuntimeException', ?, ?, ?,"
                    + " NULL, NULL)")) {
      ps.setString(1, type.value());
      ps.setLong(2, offset);
      ps.setString(3, errorMessage);
      ps.setTimestamp(4, java.sql.Timestamp.from(faultedAt));
      ps.setTimestamp(5, java.sql.Timestamp.from(faultedAt));
      ps.executeUpdate();
    } catch (java.sql.SQLException e) {
      throw new IllegalStateException("failed to seed the evidence-bare null-saga row", e);
    }
  }

  @Test
  void publish_nullSagaRequarantine_neverAttempted_staysUnanchored_keepsFirstFault() {
    // A duplicate set NO replay attempt ever touched must stay anchor-NULL — a first-EVER replay
    // is never refused, and the carry-forward must not manufacture evidence from nothing — while
    // still inheriting the oldest first fault (retention life never extends on re-quarantine).
    var type = SagaType.of("FreshT");
    Instant t0 = Instant.now().truncatedTo(ChronoUnit.MICROS).minus(3, ChronoUnit.DAYS);
    Instant t2 = t0.plus(1, ChronoUnit.DAYS);

    store.publish(nullSagaEntry(type, 9510L, "first poison", t0));
    store.publish(nullSagaEntry(type, 9510L, "second poison", t2));

    var rows = store.findAll(10);
    assertEquals(2, rows.size());
    var r2 =
        rows.stream()
            .filter(e -> "second poison".equals(e.errorMessage()))
            .findFirst()
            .orElseThrow();
    assertNull(r2.firstReplayStartedAt(), "no attempt ever started — the set stays first-EVER");
    assertEquals(t0, r2.firstFaultedAt(), "the oldest first fault still carries");
  }

  @Test
  void publish_nullSagaCarryForward_isTypeScoped() {
    // The carry SELECT mirrors establishFirstReplayAnchor/setResolvedTarget's type scoping: a
    // foreign type's null-saga row at the same offset is a DIFFERENT logical entry
    // and its evidence must not leak.
    Instant t0 = Instant.now().truncatedTo(ChronoUnit.MICROS).minus(5, ChronoUnit.DAYS);
    Instant anchor = t0.plus(1, ChronoUnit.DAYS);
    Instant t2 = t0.plus(2, ChronoUnit.DAYS);

    // Foreign type: TypeA's anchored row must NOT feed TypeB's new row.
    store.publish(nullSagaEntry(SagaType.of("TypeA"), 9520L, "A poison", t0));
    store.establishFirstReplayAnchor(null, SagaType.of("TypeA"), GlobalOffset.of(9520L), anchor);
    store.publish(nullSagaEntry(SagaType.of("TypeB"), 9520L, "B poison", t2));
    var typeB =
        store.findAll(10).stream()
            .filter(e -> "B poison".equals(e.errorMessage()))
            .findFirst()
            .orElseThrow();
    assertNull(typeB.firstReplayStartedAt(), "a foreign type's anchor never carries");
    assertEquals(t2, typeB.firstFaultedAt(), "a foreign type's first fault never carries");
  }

  // --- The conditional shield clear ---

  /**
   * A {@link DataSource} whose connections run {@code beforeConditionalUpdate} right before the
   * store prepares the conditional UPDATE — i.e. AFTER {@code SELECT … FOR UPDATE} took the saga
   * row lock and BEFORE the second statement's snapshot. This is the seam a forwarding-store hook
   * cannot reach (the race is inside one store call), and it is the exact point the single-
   * statement design got wrong.
   */
  private static DataSource hookedBeforeConditionalUpdate(Runnable beforeConditionalUpdate) {
    return hookedBeforeStatement(
        "UPDATE saga_state SET dead_letter_pending = FALSE", beforeConditionalUpdate);
  }

  /** A {@link DataSource} whose connections run {@code hook} right before preparing a statement. */
  private static DataSource hookedBeforeStatement(String sqlPrefix, Runnable hook) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              Object result;
              try {
                result = method.invoke(dataSource, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (!"getConnection".equals(method.getName())) {
                return result;
              }
              Connection real = (Connection) result;
              return Proxy.newProxyInstance(
                  Connection.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  (p, m, a) -> {
                    if ("prepareStatement".equals(m.getName())
                        && a != null
                        && a.length > 0
                        && a[0] instanceof String sql
                        && sql.startsWith(sqlPrefix)) {
                      hook.run();
                    }
                    try {
                      return m.invoke(real, a);
                    } catch (InvocationTargetException e) {
                      throw e.getCause();
                    }
                  });
            });
  }

  /**
   * Polls {@code pg_stat_activity} until a backend waits on a lock while running {@code prefix}.
   */
  private static void awaitBlockedOnRowLock(String prefix) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity"
                    + " WHERE wait_event_type = 'Lock' AND query LIKE ?")) {
      ps.setString(1, prefix + "%");
      while (System.nanoTime() < deadline) {
        try (var rs = ps.executeQuery()) {
          rs.next();
          if (rs.getLong(1) > 0) {
            return;
          }
        }
        Thread.sleep(20);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    throw new AssertionError("the hold's shield write never queued behind the drain's row lock");
  }

  /**
   * Schedule 1 — the drain's clear C takes the row lock FIRST. The runner's hold (T: set TRUE → P:
   * publish → R: re-assert TRUE) queues behind it at T, so C's fresh-snapshot UPDATE sees no entry
   * and clears (correct on the facts at that instant); then T, P and R run and R restores the
   * shield. End state: shield TRUE, E2 present, retention prunes nothing. Two real connections.
   */
  @Test
  void clearShieldIfDrained_holdBlockedBehindTheRowLock_thenRestoresTheShield_interleaving()
      throws Exception {
    var rows = sagaRows();
    var a = SagaId.of("dl-race-a");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    rows.setDeadLetterPending(a, type, true); // the drain discarded the last entry: shield still on
    var hold = new AtomicReference<CompletableFuture<Void>>();
    var drain =
        new PostgresSagaDeadLetterStore(
            hookedBeforeConditionalUpdate(
                () -> {
                  // the runner decides to hold a live event NOW: T blocks behind C's row lock
                  hold.set(
                      CompletableFuture.runAsync(
                          () -> {
                            rows.setDeadLetterPending(a, type, true); // T
                            store.publish(entry(a, type, 20L, "held", T0)); // P
                            rows.setDeadLetterPending(a, type, true); // R
                          }));
                  awaitBlockedOnRowLock("UPDATE saga_state SET dead_letter_pending = $1");
                }));

    boolean cleared = drain.clearShieldIfDrained(a, type);

    assertTrue(cleared, "C saw no entry under its lock and cleared — correct on the facts then");
    hold.get().get(20, TimeUnit.SECONDS);
    assertTrue(
        rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending(),
        "R re-asserted the shield after C's clear");
    assertEquals(1, store.findBySaga(a).size(), "E2 is recorded");
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))), "E2 is not prunable");
  }

  /**
   * Schedule 2 — T committed BEFORE C's lock; P commits while C holds the lock (a publish is never
   * blocked by the saga-row lock). Because the conditional UPDATE is a SECOND statement with a
   * snapshot taken after the lock, it SEES E2 and does not clear. A single {@code UPDATE … WHERE
   * NOT EXISTS} whose snapshot predated P would have cleared here (EvalPlanQual re-checks the
   * updated row with the statement's own snapshot). End state: shield TRUE, E2 present.
   */
  @Test
  void
      clearShieldIfDrained_publishBetweenTheLockAndTheConditionalUpdate_isSeen_noClear_interleaving() {
    var rows = sagaRows();
    var a = SagaId.of("dl-race-b");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    rows.setDeadLetterPending(a, type, true); // T already committed
    var published = new AtomicBoolean();
    var drain =
        new PostgresSagaDeadLetterStore(
            hookedBeforeConditionalUpdate(
                () -> {
                  store.publish(entry(a, type, 21L, "held", T0)); // P, on its own connection
                  published.set(true);
                }));

    boolean cleared = drain.clearShieldIfDrained(a, type);

    assertTrue(published.get(), "the hook ran between the lock and the conditional UPDATE");
    assertFalse(cleared, "the second statement's fresh snapshot saw E2: no clear");
    rows.setDeadLetterPending(a, type, true); // R (a no-op here)
    assertTrue(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());
    assertEquals(1, store.findBySaga(a).size());
    assertEquals(0, store.deleteOlderThan(T0.plus(Duration.ofDays(30))));
  }

  /**
   * The entry and the owner's shield are ONE transaction. A failure after the entry's upsert (here:
   * at the shield UPDATE) rolls the entry back too — never "entry recorded, shield not", the state
   * that let a redelivery apply an event whose entry a later drain fed again.
   */
  @Test
  void publishShielded_isOneTransaction_aFailureAfterTheEntryInsertRollsTheEntryBack() {
    var rows = sagaRows();
    var a = SagaId.of("dl-atomic-a");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    var failing =
        new PostgresSagaDeadLetterStore(
            hookedBeforeStatement(
                "UPDATE saga_state SET dead_letter_pending = TRUE",
                () -> {
                  throw new IllegalStateException("simulated failure after the entry insert");
                }));

    assertThrows(
        IllegalStateException.class,
        () -> failing.publishShielded(entry(a, type, 30L, "held", T0)));

    assertTrue(store.findBySaga(a).isEmpty(), "the entry was rolled back with the failed shield");
    assertFalse(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());

    store.publishShielded(entry(a, type, 30L, "held", T0)); // the plain store: both land
    assertEquals(1, store.findBySaga(a).size());
    assertTrue(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());
  }

  /** The clear never runs its UPDATE on an absent row: no lock, no write, {@code false}. */
  @Test
  void clearShieldIfDrained_absentRow_isFalse_withoutTheConditionalUpdate() {
    var reached = new AtomicBoolean();
    var drain =
        new PostgresSagaDeadLetterStore(hookedBeforeConditionalUpdate(() -> reached.set(true)));
    assertFalse(drain.clearShieldIfDrained(SagaId.of("dl-absent"), SagaType.of("T")));
    assertFalse(reached.get(), "an absent row ends the call before the conditional UPDATE");
  }

  /**
   * The storage-failure message of {@code findBySaga} renders the saga id log-safe, like every
   * other saga-store message: the id is usually derived from correlated event data, and the
   * subscription that retries the failure logs the message ({@code SagaRunner}'s own-entry lookup).
   */
  @Test
  void findBySaga_storageFailureMessage_rendersTheSagaIdLogSafe() {
    var hostile = SagaId.of("saga-1\nINFO forged entry \u202Eevil");
    var unreachable =
        new PostgresSagaDeadLetterStore(
            (DataSource)
                Proxy.newProxyInstance(
                    PostgresSagaDeadLetterStoreTest.class.getClassLoader(),
                    new Class<?>[] {DataSource.class},
                    (proxy, method, args) -> {
                      if (method.getName().equals("getConnection")) {
                        throw new java.sql.SQLException("connection refused");
                      }
                      throw new UnsupportedOperationException(method.getName());
                    }));

    var thrown =
        assertThrows(
            org.streamrune.core.EventStoreException.class, () -> unreachable.findBySaga(hostile));

    assertThat(thrown.getMessage())
        .contains(org.streamrune.core.types.LogSanitizer.sanitizeForLog(hostile.value()))
        .doesNotContain("\n")
        .doesNotContain("\u202E");
  }

  // --- an Error inside publishShielded / clearShieldIfDrained commits nothing ---

  /**
   * A {@link DataSource} whose connections throw an {@link AssertionError} (standing in for an
   * {@code OutOfMemoryError} or a {@code LinkageError}: an {@link Error}, not an {@link Exception})
   * when the store prepares a statement starting with {@code errorBeforeSql} (null: never) or when
   * it commits ({@code errorAtCommit}); the commit is then never sent. With a {@code
   * rollbackFailure}, {@code rollback()} throws it without rolling back, so the transaction stays
   * open. Every other call reaches the real pgjdbc connection, whose {@code setAutoCommit(true)}
   * COMMITS an open transaction — the mechanism these tests pin.
   *
   * <p>With {@code agroalReturn}, {@code close()} returns the connection the way Agroal 3.0 (the
   * Quarkus pool) does: a changed autoCommit is reset with {@code setAutoCommit(true)} and NO
   * rollback first ({@code ConnectionHandler.resetConnection}); a {@code SQLException} there only
   * becomes a pool warning. Only then is the real connection closed here (Agroal would keep it).
   * Agroal's wrapper {@code abort()} never reaches the physical connection (it also never returns
   * it to the pool, leaking it with the transaction open); here it is a no-op, so a store relying
   * on it fails on the reset's commit instead of hanging on the leaked locks.
   */
  private static DataSource errorInjecting(
      String errorBeforeSql,
      boolean errorAtCommit,
      Throwable rollbackFailure,
      boolean agroalReturn) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              Object result;
              try {
                result = method.invoke(dataSource, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (!"getConnection".equals(method.getName())) {
                return result;
              }
              Connection real = (Connection) result;
              boolean[] autoCommitDirty = {false};
              return Proxy.newProxyInstance(
                  Connection.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  (p, m, a) -> {
                    if (errorBeforeSql != null
                        && "prepareStatement".equals(m.getName())
                        && a != null
                        && a.length > 0
                        && a[0] instanceof String sql
                        && sql.startsWith(errorBeforeSql)) {
                      throw new AssertionError("injected Error before: " + errorBeforeSql);
                    }
                    if (errorAtCommit && "commit".equals(m.getName())) {
                      throw new AssertionError("injected Error before the commit");
                    }
                    if (rollbackFailure != null && "rollback".equals(m.getName())) {
                      throw rollbackFailure;
                    }
                    if (agroalReturn && "abort".equals(m.getName())) {
                      return null;
                    }
                    if (agroalReturn
                        && "setAutoCommit".equals(m.getName())
                        && real.getAutoCommit() != (Boolean) a[0]) {
                      autoCommitDirty[0] = true;
                    }
                    if (agroalReturn && "close".equals(m.getName()) && autoCommitDirty[0]) {
                      autoCommitDirty[0] = false;
                      try {
                        real.setAutoCommit(true);
                      } catch (java.sql.SQLException _) {
                        // Agroal fires a warning; a fatal SQLState makes it destroy the connection.
                      }
                    }
                    try {
                      return m.invoke(real, a);
                    } catch (InvocationTargetException e) {
                      throw e.getCause();
                    }
                  });
            });
  }

  /**
   * Schedule: the runner's hold T sets the shield, a drain's clear C then commits (no entry yet, so
   * it clears), and the hold's publish P fails with an {@link Error} after the entry's upsert,
   * before the shield write. The catch used to take only {@code SQLException | RuntimeException},
   * so the {@code finally}'s autoCommit restore COMMITTED the entry alone: entry present, shield
   * FALSE — the entry-without-shield state the SPI forbids, in which the redelivery is applied live
   * and the next drain evolves the entry again. Now nothing commits, and a rerun of the quarantine
   * converges to entry plus shield.
   */
  @Test
  void publishShielded_anErrorAfterTheEntryUpsert_commitsNothing_andARerunConverges_cpH2E() {
    var rows = sagaRows();
    var a = SagaId.of("dl-cph2e-a");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    rows.setDeadLetterPending(a, type, true); // T
    assertTrue(store.clearShieldIfDrained(a, type), "C: no entry yet, so the clear commits"); // C
    var failing =
        new PostgresSagaDeadLetterStore(
            errorInjecting("UPDATE saga_state SET dead_letter_pending = TRUE", false, null, false));

    assertThrows(
        AssertionError.class, () -> failing.publishShielded(entry(a, type, 40L, "held", T0))); // P

    assertTrue(
        store.findBySaga(a).isEmpty(),
        "the entry must not commit without its shield (no entry-with-shield-FALSE row)");
    assertFalse(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());

    store.publishShielded(entry(a, type, 40L, "held", T0)); // the rerun of the quarantine
    assertEquals(1, store.findBySaga(a).size());
    assertTrue(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());
  }

  /**
   * The rollback-fails variant: when the rollback itself throws, the store aborts the connection
   * instead of restoring autoCommit (which would commit the open transaction); the server discards
   * it. The Error in flight is the one the caller sees.
   */
  @Test
  void publishShielded_anErrorWhoseRollbackAlsoFails_commitsNothing_andARerunConverges() {
    var rows = sagaRows();
    var a = SagaId.of("dl-cph2e-b");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    var failing =
        new PostgresSagaDeadLetterStore(
            errorInjecting(
                "UPDATE saga_state SET dead_letter_pending = TRUE",
                false,
                new java.sql.SQLException("injected rollback failure", "08006"),
                false));

    assertThrows(
        AssertionError.class, () -> failing.publishShielded(entry(a, type, 41L, "held", T0)));

    assertTrue(store.findBySaga(a).isEmpty(), "the aborted transaction committed nothing");
    assertFalse(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());

    store.publishShielded(entry(a, type, 41L, "held", T0));
    assertEquals(1, store.findBySaga(a).size());
    assertTrue(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());
  }

  /**
   * The same double fault on a pool that does not roll back on return. Agroal (Quarkus) resets the
   * changed autoCommit with {@code setAutoCommit(true)} and no rollback, which COMMITTED the entry
   * without its shield — the entry-without-shield state the SPI forbids — when the store left
   * autoCommit off for {@code close()}. The store now aborts the physical connection, so the reset
   * fails on a closed connection and nothing commits.
   */
  @Test
  void publishShielded_aFailedRollback_onAPoolThatResetsAutoCommitOnReturn_commitsNothing() {
    var rows = sagaRows();
    var a = SagaId.of("dl-cph2e-c");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    var failing =
        new PostgresSagaDeadLetterStore(
            errorInjecting(
                "UPDATE saga_state SET dead_letter_pending = TRUE",
                false,
                new java.sql.SQLException("injected rollback failure"),
                true));

    assertThrows(
        AssertionError.class, () -> failing.publishShielded(entry(a, type, 42L, "held", T0)));

    assertTrue(store.findBySaga(a).isEmpty(), "the pool's reset must not commit the entry");
    assertFalse(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());

    store.publishShielded(entry(a, type, 42L, "held", T0));
    assertEquals(1, store.findBySaga(a).size());
    assertTrue(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());
  }

  /**
   * The abort is best effort: when the connection cannot be unwrapped (or aborted) either, the
   * caller still sees the original {@link Error}, and raw pgjdbc's {@code close()} discards the
   * open transaction.
   */
  @Test
  void publishShielded_aFailedAbortAfterAFailedRollback_keepsTheErrorInFlight() {
    var rows = sagaRows();
    var a = SagaId.of("dl-cph2e-e");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    var failing =
        new PostgresSagaDeadLetterStore(
            unwrapRefusing(
                errorInjecting(
                    "UPDATE saga_state SET dead_letter_pending = TRUE",
                    false,
                    new java.sql.SQLException("injected rollback failure"),
                    false)));

    var thrown =
        assertThrows(
            AssertionError.class, () -> failing.publishShielded(entry(a, type, 44L, "held", T0)));

    assertEquals(
        "injected Error before: UPDATE saga_state SET dead_letter_pending = TRUE",
        thrown.getMessage());
    assertTrue(store.findBySaga(a).isEmpty(), "close() discarded the open transaction");
    store.publishShielded(entry(a, type, 44L, "held", T0));
    assertEquals(1, store.findBySaga(a).size());
    assertTrue(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());
  }

  /** {@code inner}'s connections, whose {@code unwrap} throws: the store cannot abort them. */
  private static DataSource unwrapRefusing(DataSource inner) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              Object result;
              try {
                result = method.invoke(inner, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (!"getConnection".equals(method.getName())) {
                return result;
              }
              Connection connection = (Connection) result;
              return Proxy.newProxyInstance(
                  Connection.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  (p, m, a) -> {
                    if ("unwrap".equals(m.getName())) {
                      throw new java.sql.SQLException("injected unwrap failure");
                    }
                    try {
                      return m.invoke(connection, a);
                    } catch (InvocationTargetException e) {
                      throw e.getCause();
                    }
                  });
            });
  }

  /**
   * An {@link Error} from the rollback itself (a second {@code OutOfMemoryError}): it reaches the
   * caller, but only after the connection is aborted, so the pool's reset commits nothing.
   */
  @Test
  void
      publishShielded_aRollbackThatThrowsAnError_onAPoolThatResetsAutoCommitOnReturn_commitsNothing() {
    var rows = sagaRows();
    var a = SagaId.of("dl-cph2e-d");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    var failing =
        new PostgresSagaDeadLetterStore(
            errorInjecting(
                "UPDATE saga_state SET dead_letter_pending = TRUE",
                false,
                new AssertionError("injected Error in the rollback"),
                true));

    var thrown =
        assertThrows(
            AssertionError.class, () -> failing.publishShielded(entry(a, type, 43L, "held", T0)));

    assertEquals("injected Error in the rollback", thrown.getMessage());
    assertTrue(store.findBySaga(a).isEmpty(), "the pool's reset must not commit the entry");
    assertFalse(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());

    store.publishShielded(entry(a, type, 43L, "held", T0));
    assertEquals(1, store.findBySaga(a).size());
    assertTrue(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());
  }

  /**
   * The same rule applied to the conditional clear: an {@link Error} after the conditional UPDATE
   * and before the commit used to be committed by the autoCommit restore — a clear reported as
   * failed but durable. Harmless on its own, but the store's writes now share one rule: an
   * uncommitted exit commits nothing. The shield stays on (retention keeps protecting), and a rerun
   * of the clear converges.
   */
  @Test
  void clearShieldIfDrained_anErrorBeforeTheCommit_commitsNothing_andARerunClears() {
    var rows = sagaRows();
    var a = SagaId.of("dl-clear-e");
    var type = SagaType.of("T");
    rows.create(a, type, new ShieldState(SagaStatus.RUNNING, "m"), SagaStatus.RUNNING);
    rows.setDeadLetterPending(a, type, true);
    var failing = new PostgresSagaDeadLetterStore(errorInjecting(null, true, null, false));

    assertThrows(AssertionError.class, () -> failing.clearShieldIfDrained(a, type));

    assertTrue(
        rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending(),
        "the failed clear must not commit");

    assertTrue(store.clearShieldIfDrained(a, type), "the rerun clears the drained saga");
    assertFalse(rows.load(a, type, ShieldState.class).orElseThrow().deadLetterPending());
  }
}
