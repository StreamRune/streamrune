package org.streamrune.test;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

/**
 * In-memory {@link SagaDeadLetterStore} for unit tests. Thread-safe via {@link ConcurrentHashMap};
 * not suitable for production use.
 *
 * <p>{@link #publish} is an idempotent upsert: a second call with the same {@code (sagaId,
 * eventOffset)} pair replaces the existing entry rather than inserting a duplicate — except the
 * immutable {@code firstFaultedAt}, the immutable {@code firstReplayStartedAt} anchor and the
 * resolved {@code targetSagaId}, which keep their stored values on a conflict so a re-quarantine
 * can neither reset the first-fault anchor, forge a fresh first-attempt instant, nor strip an
 * earlier resolution's retention protection. When {@code sagaId} is {@code null}, the key
 * degenerates to {@code (sagaType, eventOffset)} — <b>one entry per quarantining type per
 * offset</b>. PostgreSQL's NULL-distinct unique semantics keep every type's null-saga row at an
 * offset as an independent entry; collapsing them onto the offset alone would let one type's
 * publish silently destroy another type's only record of a never-consumed event. (Same-type
 * re-publish stays an upsert here — a deliberate improvement over the Postgres store's accepted
 * duplicate accumulation. The evidence the reader observes is nevertheless identical: this upsert
 * preserves the stored immutable columns on a conflict, and the Postgres store carries the set's
 * oldest {@code firstReplayStartedAt}/{@code firstFaultedAt} and newest {@code targetSagaId} onto
 * every fresh duplicate, so in both stores the entry the replayer reads bears the set's true
 * first-attempt, first-fault and resolved-target facts.) Null-saga {@link #discard} therefore
 * operates on all matching null-saga entries at the offset, mirroring the Postgres {@code IS NOT
 * DISTINCT FROM} statements.
 *
 * <p><b>The store is bound to the saga rows it shields</b> (the SPI's wiring rule): it is
 * constructed with the {@link SagaStore} whose rows its runner writes. A test whose runner uses a
 * decorating saga store passes the undecorated store it wraps — the Postgres dead-letter store
 * likewise reaches {@code saga_state} directly on the shared database, not through a decorator.
 *
 * <p><b>Retention follows the shield:</b> {@link #deleteOlderThan} never prunes an entry whose
 * owner row is {@code FAULTED} or shielded ({@code dead_letter_pending}), a named entry whose saga
 * has no row (the row-less residual), or a null-saga entry that has been RESOLVED to a target
 * ({@code targetSagaId} stamped — operator-attended by construction); everything else whose {@code
 * firstFaultedAt} is older than the cutoff is garbage, and {@link #countFaultedBacklog} counts
 * exactly the protected set (the {@code streamrune.saga.faulted_backlog} gauge). There is no
 * in-flight replay marker: {@code firstReplayStartedAt} is the only replay-related fact the entry
 * keeps, and the row staying {@code FAULTED} for an attempt's whole duration (the executor's atomic
 * create-first/CAS writes) is what the owner shield above already covers.
 *
 * <p><b>The conditional shield clear:</b> {@link #clearShieldIfDrained} is the ONLY writer of
 * {@code dead_letter_pending = false} in the framework; it runs under the same monitor {@link
 * #publish}, {@link #publishShielded} and {@link #setResolvedTarget} take (see {@code
 * entriesMonitor}), which is what makes it race-free against the runner's hold in this store.
 * {@link #publishShielded} additionally writes the owner's shield inside that monitor, so the entry
 * and its shield are one indivisible step here exactly as they are one transaction in the Postgres
 * store.
 */
public final class InMemorySagaDeadLetterStore implements SagaDeadLetterStore {

  /**
   * Composite map key: sagaId value (empty string when null) + a null-saga type discriminator +
   * event offset. For a NAMED entry the discriminator is always empty — the Postgres unique
   * constraint is {@code (saga_id, event_offset)}, type not included, so a cross-type publish at
   * the same named key upserts the one row. For a NULL-saga entry the discriminator is the saga
   * type value (every entry carries its type), so each quarantining type keeps its own row at an
   * offset (mirrors Postgres NULL-distinct inserts, bounded to one row per type).
   */
  private record Key(String sagaIdValue, String nullSagaTypeDiscriminator, long offset) {}

  private static Key keyOf(SagaDeadLetterEntry entry) {
    if (entry.sagaId() != null) {
      return new Key(entry.sagaId().value(), "", entry.eventOffset().value());
    }
    return new Key("", entry.sagaType().value(), entry.eventOffset().value());
  }

  private final ConcurrentHashMap<Key, SagaDeadLetterEntry> store = new ConcurrentHashMap<>();
  private final SagaStore sagaStore;

  /**
   * The entries monitor. {@link #clearShieldIfDrained} evaluates "no named entry, no resolved
   * null-saga entry" AND writes the saga store's flag while holding it; {@link #publish}, {@link
   * #publishShielded} and {@link #setResolvedTarget} — the writes that make an entry count for the
   * shield — take the SAME monitor. <b>Invariant:</b> that shared monitor is this store's "one
   * transaction", and it is the only reason the in-memory hold (T: set TRUE → P: {@code
   * publishShielded}) is race-free against the clear. Two things it buys, both load-bearing: P
   * cannot land between the clear's check and its write, and P's own two writes — the entry upsert
   * and the shield — cannot be separated by a clear, so no observer ever sees a held entry on an
   * unshielded row. A refactor that moves {@code publish}, {@code publishShielded} or {@code
   * setResolvedTarget} off this monitor, or that splits {@code publishShielded}'s two writes across
   * two monitor-held sections, silently re-opens the race (a held entry left unshielded, prunable,
   * and re-evolved by a later drain after its redelivery is applied live).
   */
  private final Object entriesMonitor = new Object();

  /**
   * Creates a store bound to the saga rows it shields: {@link #publishShielded} sets the owner
   * row's {@code dead_letter_pending} shield in {@code sagaStore}, {@link #clearShieldIfDrained}
   * clears it, and {@link #deleteOlderThan} never prunes an entry whose owner row is {@code
   * FAULTED} or shielded, mirroring the Postgres store's join.
   *
   * @param sagaStore the saga store whose rows this store shields — the one the runner writes (the
   *     undecorated store, when the runner's is a test decorator); only the row's status and shield
   *     flag are read, and only the shield flag is written
   */
  public InMemorySagaDeadLetterStore(SagaStore sagaStore) {
    this.sagaStore = Objects.requireNonNull(sagaStore, "sagaStore is required");
  }

  @Override
  public void publish(SagaDeadLetterEntry entry) {
    if (entry == null) throw new IllegalArgumentException("entry must not be null");
    synchronized (entriesMonitor) { // see entriesMonitor: serializes P against the clear
      upsert(entry);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Under {@link #entriesMonitor}: the upsert and the saga store's flag write happen with no
   * clear able to interleave, which is this store's "one transaction".
   */
  @Override
  public void publishShielded(SagaDeadLetterEntry entry) {
    if (entry == null) throw new IllegalArgumentException("entry must not be null");
    if (entry.sagaId() == null) throw new IllegalArgumentException("sagaId is required");
    synchronized (entriesMonitor) {
      upsert(entry);
      sagaStore.setDeadLetterPending(entry.sagaId(), entry.sagaType(), true);
    }
  }

  /**
   * Mirrors PostgresSagaDeadLetterStore's UPSERT: a re-publish refreshes every column EXCEPT the
   * immutable first-fault instant — the stored firstFaultedAt always wins on a conflict, so a
   * failed-replay re-quarantine can never reset it. On a first insert a null input normalizes to
   * faultedAt. The stored firstReplayStartedAt likewise wins on a conflict — the immutable
   * first-attempt anchor (mirroring first_faulted_at) must survive a re-quarantine; only
   * establishFirstReplayAnchor's FIRST stamp establishes it. The stored targetSagaId wins when
   * present — a re-poison must not strip the retention protection an earlier TARGET_PENDING
   * resolution established; setResolvedTarget is its sole writer for existing entries, falling back
   * to the incoming value only when nothing is stored yet. Callers hold {@link #entriesMonitor}.
   */
  private void upsert(SagaDeadLetterEntry entry) {
    store.merge(
        keyOf(entry),
        withFirstFaultedAt(
            entry, entry.firstFaultedAt() == null ? entry.faultedAt() : entry.firstFaultedAt()),
        (existing, incoming) ->
            withTargetSagaId(
                withFirstReplayStartedAt(
                    withFirstFaultedAt(incoming, existing.firstFaultedAt()),
                    existing.firstReplayStartedAt()),
                existing.targetSagaId() != null
                    ? existing.targetSagaId()
                    : incoming.targetSagaId()));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Establish-only (the stored anchor always wins). Keyed and type-scoped exactly like {@link
   * #setResolvedTarget}; mirrors the Postgres {@code COALESCE} statement. A {@code null sagaType}
   * is rejected before anything is written.
   */
  @Override
  public void establishFirstReplayAnchor(
      SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset, Instant anchorIfAbsent) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    Objects.requireNonNull(eventOffset, "eventOffset is required");
    Objects.requireNonNull(anchorIfAbsent, "anchorIfAbsent is required");
    if (sagaId != null) {
      store.computeIfPresent(
          new Key(sagaId.value(), "", eventOffset.value()),
          (key, existing) ->
              typeMatches(existing, sagaType) && existing.firstReplayStartedAt() == null
                  ? withFirstReplayStartedAt(existing, anchorIfAbsent)
                  : existing);
      return;
    }
    store.replaceAll(
        (key, existing) ->
            existing.sagaId() == null
                    && existing.eventOffset().equals(eventOffset)
                    && typeMatches(existing, sagaType)
                    && existing.firstReplayStartedAt() == null
                ? withFirstReplayStartedAt(existing, anchorIfAbsent)
                : existing);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Stamps every type-matching null-saga entry at the offset, mirroring the Postgres {@code
   * SET_RESOLVED_TARGET} statement; named entries are never touched. A {@code null sagaType} is
   * rejected before anything is written.
   */
  @Override
  public void setResolvedTarget(SagaType sagaType, GlobalOffset eventOffset, SagaId targetSagaId) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    Objects.requireNonNull(eventOffset, "eventOffset is required");
    Objects.requireNonNull(targetSagaId, "targetSagaId is required");
    synchronized (entriesMonitor) { // a resolution makes the entry count for the target's shield
      store.replaceAll(
          (key, existing) ->
              existing.sagaId() == null
                      && existing.eventOffset().equals(eventOffset)
                      && typeMatches(existing, sagaType)
                  ? withTargetSagaId(existing, targetSagaId)
                  : existing);
    }
  }

  /**
   * Whether the entry's stored type is {@code sagaType}. There is no unscoped form: every caller
   * has already rejected a {@code null} type, so {@code sagaType} is never {@code null} here.
   */
  private static boolean typeMatches(SagaDeadLetterEntry entry, SagaType sagaType) {
    return entry.sagaType().equals(sagaType);
  }

  private static SagaDeadLetterEntry withFirstFaultedAt(
      SagaDeadLetterEntry entry, Instant firstFaultedAt) {
    return new SagaDeadLetterEntry(
        entry.sagaId(),
        entry.sagaType(),
        entry.eventOffset(),
        entry.eventType(),
        entry.errorType(),
        entry.errorMessage(),
        entry.faultedAt(),
        firstFaultedAt,
        entry.firstReplayStartedAt(),
        entry.targetSagaId());
  }

  private static SagaDeadLetterEntry withFirstReplayStartedAt(
      SagaDeadLetterEntry entry, Instant firstReplayStartedAt) {
    return new SagaDeadLetterEntry(
        entry.sagaId(),
        entry.sagaType(),
        entry.eventOffset(),
        entry.eventType(),
        entry.errorType(),
        entry.errorMessage(),
        entry.faultedAt(),
        entry.firstFaultedAt(),
        firstReplayStartedAt,
        entry.targetSagaId());
  }

  private static SagaDeadLetterEntry withTargetSagaId(
      SagaDeadLetterEntry entry, SagaId targetSagaId) {
    return new SagaDeadLetterEntry(
        entry.sagaId(),
        entry.sagaType(),
        entry.eventOffset(),
        entry.eventType(),
        entry.errorType(),
        entry.errorMessage(),
        entry.faultedAt(),
        entry.firstFaultedAt(),
        entry.firstReplayStartedAt(),
        targetSagaId);
  }

  @Override
  public List<SagaDeadLetterEntry> findAll(int limit) {
    return store.values().stream()
        .sorted(Comparator.comparing(SagaDeadLetterEntry::faultedAt).reversed())
        .limit(limit)
        .toList();
  }

  @Override
  public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
    Objects.requireNonNull(sagaId, "sagaId is required");
    return store.values().stream()
        .filter(e -> e.sagaId() != null && e.sagaId().equals(sagaId))
        .sorted(Comparator.comparing(SagaDeadLetterEntry::faultedAt).reversed())
        .toList();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Type-scoped (a {@code null sagaType} is rejected), one entry per offset, ordered by event
   * offset ascending — mirroring the Postgres {@code DISTINCT ON} statement. This store keeps one
   * null-saga entry per quarantining type per offset, so a type-scoped query meets at most one
   * entry per offset and the Postgres newest-of-duplicates rule has nothing to choose between here.
   */
  @Override
  public List<SagaDeadLetterEntry> findNullSagaEntriesByResolvedTarget(
      SagaType sagaType, SagaId targetSagaId) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    Objects.requireNonNull(targetSagaId, "targetSagaId is required");
    java.util.Map<Long, SagaDeadLetterEntry> byOffset = new java.util.TreeMap<>();
    for (SagaDeadLetterEntry e : store.values()) {
      if (e.sagaId() != null
          || !targetSagaId.equals(e.targetSagaId())
          || !typeMatches(e, sagaType)) {
        continue;
      }
      byOffset.put(e.eventOffset().value(), e);
    }
    return List.copyOf(byOffset.values());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Type-scoped: only an entry whose stored type equals {@code sagaType} is removed. A {@code
   * null sagaType} is rejected: there is no untyped discard. For a null-saga discard, all
   * type-matching null-saga entries at the offset are removed (a foreign type's row at the same
   * offset is never touched).
   */
  @Override
  public boolean discard(SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    Objects.requireNonNull(eventOffset, "eventOffset is required");
    if (sagaId != null) {
      Key key = new Key(sagaId.value(), "", eventOffset.value());
      SagaDeadLetterEntry existing = store.get(key);
      if (existing == null || !typeMatches(existing, sagaType)) {
        return false;
      }
      return store.remove(key, existing);
    }
    return store
        .values()
        .removeIf(
            e ->
                e.sagaId() == null
                    && e.eventOffset().equals(eventOffset)
                    && typeMatches(e, sagaType));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Under {@link #entriesMonitor} (see its invariant): both predicates are evaluated and the
   * saga store's flag is written while no {@code publish}/{@code setResolvedTarget} can interleave.
   */
  @Override
  public boolean clearShieldIfDrained(SagaId sagaId, SagaType sagaType) {
    if (sagaId == null) throw new IllegalArgumentException("sagaId is required");
    if (sagaType == null) throw new IllegalArgumentException("sagaType is required");
    synchronized (entriesMonitor) {
      boolean entryExists =
          store.values().stream().anyMatch(e -> countsForShieldOf(e, sagaId, sagaType));
      if (entryExists) {
        return false;
      }
      var row = sagaStore.load(sagaId, sagaType, SagaState.class);
      if (row.isEmpty() || !row.get().deadLetterPending()) {
        return false;
      }
      sagaStore.setDeadLetterPending(sagaId, sagaType, false);
      return true;
    }
  }

  /** A named entry of the type for the saga, or a null-saga entry of the type resolved to it. */
  private static boolean countsForShieldOf(SagaDeadLetterEntry e, SagaId sagaId, SagaType type) {
    if (!typeMatches(e, type)) {
      return false;
    }
    return sagaId.equals(e.sagaId()) || (e.sagaId() == null && sagaId.equals(e.targetSagaId()));
  }

  @Override
  public int deleteOlderThan(Instant cutoff) {
    Objects.requireNonNull(cutoff, "cutoff is required");
    int before = store.size();
    // Retention follows the shield: an entry older than the cutoff (measured
    // on the immutable first-fault instant, which every stored entry carries) is garbage UNLESS
    // its owner row is FAULTED or shielded, it is a named entry whose saga has no row (the
    // row-less residual), or it is a null-saga entry RESOLVED to a target (operator-attended by
    // construction). Mirrors the Postgres SHIELDED_OWNER / ROWLESS_NAMED / RESOLVED_NULL
    // predicates; no replay marker is consulted.
    store
        .values()
        .removeIf(
            e -> e.firstFaultedAt().isBefore(cutoff) && !ownerShielded(e) && !targetShielded(e));
    return before - store.size();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Exactly the set {@link #deleteOlderThan} protects, evaluated with the SAME predicates so the
   * gauge and the retention decision cannot disagree.
   */
  @Override
  public long countFaultedBacklog() {
    return store.values().stream().filter(e -> ownerShielded(e) || targetShielded(e)).count();
  }

  private static boolean shielded(LoadedSaga<?> row) {
    return row.status() == SagaStatus.FAULTED || row.deadLetterPending();
  }

  /**
   * A named entry is shielded while its type-scoped owner row is {@code FAULTED} or carries the
   * {@code dead_letter_pending} shield — or has NO row at all (the row-less residual: the entry is
   * the saga's only record and is never pruned). Null-saga entries are never shielded this way.
   * Only the row's status and shield flag are read.
   */
  private boolean ownerShielded(SagaDeadLetterEntry e) {
    if (e.sagaId() == null) {
      return false;
    }
    var row = sagaStore.load(e.sagaId(), e.sagaType(), SagaState.class);
    return row.map(InMemorySagaDeadLetterStore::shielded)
        .orElse(true); // row-less named: never pruned
  }

  /**
   * A resolved null-saga entry is operator-attended by construction: never pruned, always counted.
   */
  private static boolean targetShielded(SagaDeadLetterEntry e) {
    return e.sagaId() == null && e.targetSagaId() != null;
  }

  /** Returns all stored entries. For test assertions. */
  public List<SagaDeadLetterEntry> all() {
    return List.copyOf(store.values());
  }
}
