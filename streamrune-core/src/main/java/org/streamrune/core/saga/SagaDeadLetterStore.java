package org.streamrune.core.saga;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

/**
 * Persistence abstraction for poison saga events. Parallels the command {@code DeadLetterQueue} for
 * the saga-runner side: events that could not be processed by a saga (routing failure,
 * extract/correlate errors, decider exceptions) are quarantined here so they do not block the
 * subscription and can be inspected. Replay is supported via the manual, operator-triggered {@code
 * org.streamrune.runtime.SagaDeadLetterReplayer} (invoked after the poison cause is fixed); there
 * is deliberately no automatic polling retry — poison failures are deterministic.
 *
 * <p><b>Metadata only — GDPR / crypto-shredding stays authoritative in {@code event_stream}.</b> A
 * quarantined entry stores only identifying metadata (saga id/type, event offset/type, error
 * details, fault time). The offending event's payload is deliberately never copied here: {@code
 * event_stream} is the framework's single crypto-governed, shreddable source of truth for event
 * payloads, and duplicating an already-decrypted payload into a second table would silently defeat
 * per-subject key deletion. To inspect a quarantined event, read the event store at {@code
 * eventOffset} — see {@link SagaDeadLetterEntry}.
 *
 * <p>{@link #publish} is an idempotent upsert keyed by {@code (sagaId, eventOffset)} so
 * re-quarantine after an infra retry does not produce duplicate rows. When {@code sagaId} is {@code
 * null} (e.g. {@code extractSagaId} or {@code correlate} failed before a saga could be identified),
 * the key degenerates to {@code eventOffset} alone.
 *
 * <p><b>A dead-letter store must READ AND WRITE the saga rows it shields</b> (normative for
 * implementations and for how a store is WIRED). Three of its operations are not entry-local at
 * all: {@link #publishShielded} sets {@code saga_state.dead_letter_pending} with the entry, {@link
 * #clearShieldIfDrained} is the framework's ONLY writer of that flag back to {@code false}, and
 * {@link #deleteOlderThan} / {@link #countFaultedBacklog} must never prune (and must count) an
 * entry whose owner row is {@code FAULTED} or shielded. A store that cannot reach the saga rows
 * cannot honour any of them: it will either drop the shield silently or prune a held entry. That is
 * why {@link #publishShielded}, {@link #clearShieldIfDrained} and {@link #deleteOlderThan} are
 * abstract. {@link #countFaultedBacklog} is an optional default: a store that cannot see saga rows
 * leaves it throwing, and the gauge is then absent. It must never return {@code 0}.
 *
 * <p><b>The wiring consequence:</b> the dead-letter store and the {@code SagaStore} a {@code
 * SagaRunner} is built with must see the SAME saga rows. The framework's defaults do — all three
 * integrations pair {@code PostgresSagaStore} with {@code PostgresSagaDeadLetterStore} on one
 * {@code DataSource}. A test that pairs a runner's saga store with a dead-letter store that does
 * not see the SAME saga rows (a store over different rows; a test decorator's undecorated backing
 * store sees the same rows and is fine) is a false witness: its holds, drains and retention sweeps
 * answer about rows that are not the ones under test. Pair {@code InMemorySagaStore} with {@code
 * new InMemorySagaDeadLetterStore(sagaStore)} (its only constructor takes the saga store), or a
 * Postgres saga store with {@code new PostgresSagaDeadLetterStore(dataSource)} / {@code new
 * InMemorySagaDeadLetterStore(postgresSagaStore)}.
 *
 * <p>Implementations: {@code InMemorySagaDeadLetterStore} (in streamrune-test) and {@code
 * PostgresSagaDeadLetterStore} (in streamrune-postgres).
 */
public interface SagaDeadLetterStore {

  /**
   * Quarantines a poison saga event. Idempotent: a second call with the same {@code (sagaId,
   * eventOffset)} pair updates the existing row rather than inserting a duplicate.
   *
   * <p><b>Conflict semantics (normative for implementations):</b> the re-quarantine refreshes the
   * mutable columns ({@code faultedAt}, error details, ...) but preserves the STORED {@code
   * firstFaultedAt} (the first-fault anchor is immutable), the STORED {@code firstReplayStartedAt}
   * (the immutable first-attempt anchor, established only by {@link #establishFirstReplayAnchor}),
   * and the STORED {@code targetSagaId} (a re-poison must not strip the retention protection an
   * earlier {@code TARGET_PENDING} resolution established — {@link #setResolvedTarget} is its sole
   * writer for existing entries).
   *
   * <p><b>Null-saga duplicate accumulation (normative for implementations whose unique key cannot
   * upsert a {@code null} sagaId):</b> where NULL-distinct unique semantics make a null-saga
   * re-quarantine INSERT a fresh row instead of conflicting (the PostgreSQL store), the new row
   * MUST be born carrying the duplicate set's immutable evidence — the oldest stored {@code
   * firstReplayStartedAt}, the oldest stored {@code firstFaultedAt}, and the newest stored {@code
   * targetSagaId} among type-matching null-saga rows at the offset. Readers surface one row per
   * entry (newest-faulted first), so an evidence-bare newest row would launder a prior replay
   * attempt's anchor out of the replayer's key-age guard — re-opening the pruned-forward-key
   * re-execution the guard exists to block — a fresh first fault would grant the set a new
   * retention life per re-quarantine, breaking the {@code SagaRetentionValidator} interlock
   * premise, and a target-bare newest row would let the retention sweep prune a
   * TARGET_PENDING-resolved set out from under an operator mid-reconciliation. The mutable columns
   * (a fresh {@code faultedAt}, which decides the row readers surface, and error details) stay
   * per-row.
   *
   * @param entry the dead-letter entry to publish
   * @throws IllegalArgumentException if entry is null
   */
  void publish(SagaDeadLetterEntry entry);

  /**
   * {@link #publish} for a NAMED saga PLUS the owner's shield, as ONE durable write: the entry is
   * upserted and {@code saga_state.dead_letter_pending} is set TRUE for {@code (entry.sagaId(),
   * entry.sagaType())} in the same transaction, so no crash point exists between "the entry is
   * recorded" and "the row is shielded". An absent owner row (the row-less residual) records the
   * entry and leaves the shield write a no-op, exactly like {@code SagaStore.setDeadLetterPending}.
   *
   * <p><b>Why one write.</b> The runner holds an event as T ({@code setDeadLetterPending(true)}) →
   * P ({@code publish}). A concurrent drain's {@link #clearShieldIfDrained} may legitimately land
   * between T and P (it sees no entry yet and clears). A separate re-assert after P closed the race
   * but opened a crash window: a process death between P and the re-assert left the entry on an
   * UNSHIELDED row, the redelivery was then APPLIED live (the shield WAS the hold's condition) and
   * the entry survived as the record of an already-applied event that the next drain evolved again.
   * With the entry and the shield in one transaction the window is gone: after any crash either
   * neither exists (the redelivery re-decides on the row) or both do (the redelivery is held behind
   * its own entry).
   *
   * <p>Lock order (PostgreSQL): the dead-letter row first (the upsert), then the saga row (the
   * shield UPDATE). {@link #clearShieldIfDrained} takes the saga row first and only READS the
   * dead-letter table (MVCC, no row lock), and {@link #deleteOlderThan} locks dead-letter rows and
   * only reads {@code saga_state}, so no transaction ever holds a dead-letter row lock while
   * waiting on a saga row held by one waiting the other way round — no cycle.
   *
   * <p>A store that cannot write the saga row cannot promise the shield invariant for a hold.
   *
   * @param entry the entry to publish; {@code sagaId} is required
   * @throws IllegalArgumentException if {@code entry} or its saga id is null
   */
  void publishShielded(SagaDeadLetterEntry entry);

  /**
   * Returns the most recently faulted entries across all sagas, up to {@code limit}.
   *
   * @param limit maximum number of entries to return
   * @return entries ordered by {@code faultedAt} descending
   */
  List<SagaDeadLetterEntry> findAll(int limit);

  /**
   * Returns all dead-letter entries for the given saga.
   *
   * @param sagaId the saga to query
   * @return all entries for this saga, ordered by {@code faultedAt} descending
   */
  List<SagaDeadLetterEntry> findBySaga(SagaId sagaId);

  /**
   * Returns the NEWEST-faulted <b>null-saga</b> entry at {@code eventOffset} that {@code sagaType}
   * owns, or empty. A null-saga entry names no saga of its own, so {@link #findBySaga} (a {@code
   * saga_id = ?} lookup) can never return it — this is the targeted counterpart for that
   * population.
   *
   * <p><b>Why this exists.</b> The only way to resolve a null-saga entry used to be {@code
   * findAll(Integer.MAX_VALUE)} filtered in Java. The null-saga population is precisely the one
   * that grows without bound: PostgreSQL's {@code UNIQUE(saga_id, event_offset)} treats NULLs as
   * distinct, so every re-quarantine INSERTs a fresh row, and a broken {@code correlate} on a 1000
   * event/s stream produces tens of millions of rows inside one retention window. Scanning that
   * table — twice per replay attempt, once to find the entry and again for the still-poison check —
   * materialises every row as a {@link SagaDeadLetterEntry} on the heap and kills the JVM on
   * exactly the recovery path the operator reaches for once the router is fixed.
   *
   * <p><b>Type scoping matches the replayer's own rule</b>: only an entry of {@code sagaType}
   * matches. There is no untyped lookup: a {@code null sagaType} is rejected, never read as "any
   * type" (which would hand one type's replayer a colliding foreign type's entry at the same
   * offset). Newest-first ordering matches {@link #findAll}, so a re-quarantined set resolves to
   * the same row either way.
   *
   * <p>The default implementation is a correct but unindexed scan of {@link #findAll};
   * implementations that can index the offset SHOULD override it.
   *
   * @param sagaType the replayer's saga type (required)
   * @param eventOffset the quarantined event's global offset
   * @return the newest type-matching null-saga entry at that offset, or empty
   * @throws IllegalArgumentException if {@code sagaType} is {@code null}
   */
  default Optional<SagaDeadLetterEntry> findNullSagaEntry(
      SagaType sagaType, GlobalOffset eventOffset) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    return findAll(Integer.MAX_VALUE).stream()
        .filter(e -> e.sagaId() == null && e.eventOffset().equals(eventOffset))
        .filter(e -> sagaType.equals(e.sagaType()))
        .findFirst();
  }

  /**
   * Returns the <b>null-saga</b> entries whose durably RESOLVED target ({@link
   * SagaDeadLetterEntry#targetSagaId()}, stamped by {@link #setResolvedTarget} on a {@code
   * TARGET_PENDING} replay) is {@code targetSagaId} — one entry per offset (the newest-faulted of
   * each duplicate set, matching {@link #findNullSagaEntry}), ordered by event offset ascending.
   *
   * <p><b>Why this exists.</b> {@code SagaDeadLetterReplayer.replayAll(sagaId)} drains a saga's
   * quarantined events in offset order via {@link #findBySaga} — which structurally cannot see a
   * NULL-saga entry (it has no {@code saga_id}) even when that entry is an EARLIER event of the
   * very saga being drained (its router was broken when the event arrived; a later replay resolved
   * and stamped the target). Without this lookup the drain forward-dispatched later events from a
   * state that never consumed the earlier one — the exact corruption class the drain's own
   * stop-gate exists to prevent, laundered through the documented remedy itself. The drain now
   * folds these entries in at their causal position.
   *
   * <p>Type scoping matches the replayer's ownership rule: only entries of {@code sagaType} match.
   * There is no untyped lookup: a {@code null sagaType} is rejected, never read as "any type"
   * (which would fold a colliding foreign type's resolved entry into this type's drain). Entries
   * never replayed carry no resolution and are — correctly — invisible here: resolving them
   * requires the routers, which only the replayer can run.
   *
   * <p>Default implementation rejects a {@code null sagaType} like every implementation, then
   * throws {@link UnsupportedOperationException}: a store that does not support this lookup must
   * fail loudly rather than silently return nothing — an empty answer here IS the drain corruption
   * described above. {@code SagaDeadLetterReplayer.replayAll} catches the throw and degrades
   * EXPLICITLY, with a WARNING naming the exposure, to the named-only drain. The framework's {@code
   * PostgresSagaDeadLetterStore} and {@code InMemorySagaDeadLetterStore} override it.
   *
   * @param sagaType the replayer's saga type (required)
   * @param targetSagaId the resolved target saga (required)
   * @return the newest type-matching null-saga entry per offset whose resolved target is {@code
   *     targetSagaId}, ordered by event offset ascending
   * @throws IllegalArgumentException if {@code sagaType} is {@code null}
   * @throws UnsupportedOperationException if this store has not overridden the method
   */
  default List<SagaDeadLetterEntry> findNullSagaEntriesByResolvedTarget(
      SagaType sagaType, SagaId targetSagaId) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    throw new UnsupportedOperationException(
        "findNullSagaEntriesByResolvedTarget is not implemented by this SagaDeadLetterStore; the"
            + " replayAll drain needs it to fold a saga's earlier NULL-SAGA entries (events whose"
            + " router was broken on arrival, later resolved to this saga by a TARGET_PENDING"
            + " replay) into the drain at their causal position. Silently"
            + " returning nothing makes the drain feed later events over a never-consumed earlier"
            + " one, corrupting the saga's state through its own documented remedy. Override it"
            + " (see PostgresSagaDeadLetterStore / InMemorySagaDeadLetterStore); replayAll"
            + " degrades to the named-only drain with a WARNING when this throws.");
  }

  /**
   * Removes a single quarantined entry, identified by its idempotency key {@code (sagaId,
   * eventOffset)}, and only when its stored saga type equals {@code sagaType}. Once a quarantined
   * event has been replayed or otherwise resolved, the entry is discarded so it no longer appears
   * in {@link #findAll} / {@link #findBySaga}.
   *
   * <p>{@code sagaId} is nullable and its null-ness is significant: SQL {@code IS NOT DISTINCT
   * FROM} semantics apply, so a {@code null sagaId} matches <b>only</b> entries whose stored {@code
   * sagaId} is also {@code null} — it never matches a row that has a non-null saga id, even if the
   * offset is identical.
   *
   * <p>Two saga types deriving the same {@code SagaId} — the exact collision {@code saga_state}'s
   * type-scoped CAS/delete paths were hardened against — must never let one type's replayer destroy
   * the other type's only quarantine record, so this must not drop the {@code saga_type} filter.
   * There is no untyped discard: a {@code null sagaType} is rejected, never read as "any type".
   * {@code SagaDeadLetterReplayer} discards through this method with its runner's type.
   *
   * <p>A store-level discard never touches the saga's {@code dead_letter_pending} shield. Operators
   * discard through {@code SagaDeadLetterReplayer.discard}, which also releases the shield once the
   * saga's backlog is empty ({@link #clearShieldIfDrained}).
   *
   * @param sagaId the saga id of the entry to remove, or {@code null} to match a null-saga entry
   * @param sagaType the saga type scoping the removal (required)
   * @param eventOffset the event offset of the entry to remove (required)
   * @return {@code true} if an entry was removed, {@code false} if no matching entry existed
   * @throws IllegalArgumentException if {@code sagaType} is {@code null}
   */
  boolean discard(SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset);

  /**
   * Establishes the immutable first-replay anchor ({@link
   * SagaDeadLetterEntry#firstReplayStartedAt()}) on the entry identified by {@code (sagaId,
   * eventOffset)} <em>if — and only if — none exists yet</em>.
   *
   * <p>{@code SagaDeadLetterReplayer} calls this immediately before the FIRST feed of every entry
   * it drains, named or null-saga alike. The executor's atomic create-first/CAS writes keep the
   * saga row {@code FAULTED} for the entry attempt's whole duration, so the ordinary owner shield
   * already covers the in-flight window and no in-flight marker exists; this durable anchor is the
   * entry's ONLY record that a replay was ever attempted, and it is what the {@code
   * STALE_REDRIVE_BLOCKED} key-age guard measures against: once {@code now - firstReplayStartedAt}
   * exceeds the inbox retention window, a re-driven forward command may MISS a pruned inbox key and
   * re-execute, so the drive is refused instead.
   *
   * <p>Idempotent and establish-only: the stored anchor always wins ({@code COALESCE} semantics),
   * so a rerun can never move an existing anchor forward.
   *
   * <p>{@code sagaId} nullness and {@code sagaType} scoping mirror {@link #setResolvedTarget}: a
   * {@code null sagaId} matches only null-saga entries (mirroring {@link #discard}: {@code IS NOT
   * DISTINCT FROM} semantics), and {@code sagaType} scopes the write — only entries of that type
   * match. There is no untyped write: a {@code null sagaType} is rejected before anything is
   * written, never read as "any type" (which would stamp a colliding foreign type's entry with an
   * anchor its own replayer never set).
   *
   * <p>Skipping the write reintroduces a duplicate dispatch: an aged entry with no anchor would
   * replay as though it were first-ever and could re-execute an already-committed forward command
   * past a pruned inbox key.
   *
   * @param sagaId the entry's saga id, or {@code null} to match only null-saga entries
   * @param sagaType the saga type scoping the write (required)
   * @param eventOffset the entry's event offset (required)
   * @param anchorIfAbsent the anchor instant to establish when none exists (required)
   * @throws IllegalArgumentException if {@code sagaType} is {@code null}
   */
  void establishFirstReplayAnchor(
      SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset, Instant anchorIfAbsent);

  /**
   * Durably records the resolved TARGET saga ({@link SagaDeadLetterEntry#targetSagaId()}) on the
   * <b>null-saga</b> entries at {@code eventOffset}.
   *
   * <p>{@code SagaDeadLetterReplayer} calls this whenever a replay of a null-saga entry classifies
   * {@code TARGET_PENDING}: the fixed routers have just resolved the event's target and the discard
   * was refused because that saga is {@code FAULTED} — the entry is the sole record of a
   * never-consumed event the target still owes a delivery. The retention sweep is SQL and cannot
   * run the routers itself; this persisted resolution is what lets {@link #deleteOlderThan} extend
   * its FAULTED-owner protection to null-saga entries (which name no {@code saga_state} row of
   * their own). Idempotent — re-stamped on every {@code TARGET_PENDING}, overwriting a stale
   * resolution after a routing change. There is deliberately no clear operation: a recovered target
   * stops matching the retention guard on its own, and a successful replay discards the entry, hint
   * and all.
   *
   * <p>{@code sagaType} scopes the write to the replayer's own entries (type-scoping discipline):
   * only entries of that type match; a foreign type's entry at the same offset is never touched.
   * There is no untyped write: a {@code null sagaType} is rejected before anything is written,
   * never read as "any type". Named entries are never touched either — their association IS their
   * {@code sagaId}.
   *
   * <p>Dropping the resolution lets the retention sweep prune a null-saga entry whose resolved
   * target is still FAULTED — the sole record of a never-consumed event that saga still owes a
   * delivery — with no error anywhere. A store that genuinely cannot persist the hint may implement
   * this as a no-op — a visible, deliberate choice, documented against the retention exposure it
   * accepts. The framework's {@code PostgresSagaDeadLetterStore} and {@code
   * InMemorySagaDeadLetterStore} persist it.
   *
   * @param sagaType the saga type scoping the write (required)
   * @param eventOffset the null-saga entries' event offset (required)
   * @param targetSagaId the resolved target saga (required — there is no clear operation)
   * @throws IllegalArgumentException if {@code sagaType} is {@code null}
   */
  void setResolvedTarget(SagaType sagaType, GlobalOffset eventOffset, SagaId targetSagaId);

  /**
   * The conditional shield clear: clears {@code saga_state.dead_letter_pending} for {@code (sagaId,
   * sagaType)} only where, AT THE STORE and serialized against the runner's hold, no named entry of
   * that type for the saga and no null-saga entry resolved to it exist. Returns {@code true} iff
   * the flag was cleared by this call ({@code false}: an entry exists, the flag was already clear,
   * or the row is absent).
   *
   * <p><b>Why this is a store method and not two calls.</b> The replayer's former
   * observe-then-write ({@code findBySaga} empty → {@code setDeadLetterPending(false)}) raced the
   * runner's hold ({@code setDeadLetterPending(true)} → {@code publish}): the drain observed an
   * empty backlog, the runner shielded and published, the drain wrote FALSE — a held entry left
   * unshielded, applied behind by the next live event and pruned by {@link #deleteOlderThan}. Nor
   * is a single conditional {@code UPDATE … WHERE NOT EXISTS} enough under READ COMMITTED: its
   * subqueries run on the statement snapshot and EvalPlanQual re-checks a concurrently updated row
   * with that SAME snapshot, so a clear whose snapshot predates the hold still writes FALSE behind
   * a completed hold. The contract is therefore: <em>serialize against the shield writes, then
   * decide on a fresh view</em> — PostgreSQL: one transaction, {@code SELECT … FOR UPDATE} on the
   * saga row (the hold's flag writes queue behind it) followed by the conditional UPDATE as a
   * SECOND statement (fresh snapshot after the lock); in-memory: the entries monitor that {@code
   * publish}, {@link #publishShielded} and {@code setResolvedTarget} also take. Together with
   * {@link #publishShielded} writing the entry and the shield as ONE durable step, every
   * interleaving of the hold's T (set TRUE) / P (entry + shield) with the clear C ends with the
   * shield TRUE while an entry exists, <em>and stays that way across a crash at any point</em> (see
   * {@code SagaRunner}'s single poison handler). C before or between T and P: C sees no entry and
   * clears, then P re-establishes both. C after P: C's fresh snapshot sees the entry and does not
   * clear. C concurrent with P: C holds the saga row, so P's shield UPDATE waits; C's read of the
   * dead-letter table cannot see P's uncommitted entry, so C clears, and P then sets the shield
   * back — never the reverse, because C never holds a dead-letter row lock and so can never
   * overtake a committed P.
   *
   * <p>A store that cannot answer must never clear — the shield invariant errs toward recording. It
   * throws {@link UnsupportedOperationException} instead, and {@code SagaDeadLetterReplayer} WARNs
   * and leaves the shield set. The framework's {@code PostgresSagaDeadLetterStore} and {@code
   * InMemorySagaDeadLetterStore} answer it.
   *
   * @param sagaId the saga whose shield may be cleared (required)
   * @param sagaType the replayer's saga type (required — the flag is per {@code (saga_id,
   *     saga_type)} row, and only entries of that type count)
   * @return {@code true} iff this call cleared the flag
   * @throws IllegalArgumentException if {@code sagaId} or {@code sagaType} is null (nothing is read
   *     or written)
   * @throws UnsupportedOperationException if this store cannot answer (it then never clears)
   */
  boolean clearShieldIfDrained(SagaId sagaId, SagaType sagaType);

  /**
   * Deletes all dead-letter entries FIRST faulted strictly before {@code cutoff}. Intended for GDPR
   * storage-limitation retention sweeps: this metadata (error details, event offset) should not be
   * retained indefinitely once it is stale.
   *
   * <p><b>The cutoff compares against {@link SagaDeadLetterEntry#firstFaultedAt()}, not the
   * refreshable {@code faultedAt}.</b> A failed replay re-quarantines the entry and the upsert
   * refreshes {@code faultedAt}; pruning on that column would let each failed replay extend the
   * entry's life by a full retention window — past what the boot validator (dead-letter retention
   * &le; inbox retention) reasons about, and past the point where the entry's compensation dedup
   * keys still exist. The immutable first-fault instant keeps retention monotonic.
   *
   * <p><b>Entries whose owning saga is still {@code FAULTED} must be excluded.</b> The quarantine
   * entry is the framework's <em>sole</em> handle for recovering its saga: the replayer drains a
   * saga's entries, re-reading each quarantined event at the entry's offset, and a {@code FAULTED}
   * row is cleared only by the successful CAS of such a replayed step — pruning it while the saga
   * is still {@code FAULTED} strands the saga permanently, recoverable only by manual {@code
   * saga_state} surgery. Implementations exclude such entries from the sweep, and likewise every
   * entry whose owner row carries the {@code dead_letter_pending} shield. Type-scoped: an entry is
   * judged only against its OWN type's row. A foreign type's row neither protects nor releases an
   * entry — an entry whose id carries only a foreign type's row is judged as row-less (see the
   * row-less case below). The entry becomes prunable again once its owner row is neither {@code
   * FAULTED} nor shielded. A store must be able to read {@code saga_state} to honour this (see the
   * class javadoc).
   *
   * <p><b>A saga that is FAULTED WITHOUT a row shields its entries too.</b> The runner's poison
   * handler ({@code SagaRunner.recordPoison}) deliberately skips the FAULTED-row create when {@code
   * initialState()} throws or the genesis cannot be converted, so such a saga names no row for the
   * guard above to join — yet its entries are the sole records of never-consumed events. The signal
   * is the one the runner and the replayer already use ({@code SagaRunner.hasOwnDeadLetterRecord}):
   * the quarantine always precedes the fault write, so a row-less saga that owns an own-type entry
   * is FAULTED-without-a-row, while a saga that genuinely never started owns none. Implementations
   * therefore also exclude (a) a NAMED entry whose type-scoped owner row is absent — the entry is
   * itself that record — and (b) a null-saga entry whose {@link SagaDeadLetterEntry#targetSagaId()
   * resolved target} has no type-scoped row but owns an own-type named entry. A row an operator
   * removes by hand no longer releases its entries — discard them explicitly.
   *
   * <p><b>A null-saga entry RESOLVED to a {@link SagaDeadLetterEntry#targetSagaId() target} is
   * never pruned while the resolution stands.</b> A null-saga entry names no {@code saga_state} row
   * of its own, so the owner guard above never protects it — the resolution itself is the shield,
   * UNCONDITIONALLY (not contingent on the target's current status): once the replayer has
   * persisted it (via {@link #setResolvedTarget}, stamped BEFORE any refusal, anchor or feed), the
   * entry is operator-attended by construction and stays protected until a successful replay
   * discards it, hint and all. A null-saga entry that was never replayed carries no resolution and
   * stays prunable — retention must eventually win over records nobody has attempted to recover
   * (the {@code faulted_backlog} gauge has been showing the stranded target the whole time).
   *
   * @param cutoff entries whose {@code firstFaultedAt} is strictly before this instant are removed;
   *     entries at exactly {@code cutoff} are retained, as is every entry the shield paragraphs
   *     above protect
   * @return the total number of rows removed
   */
  int deleteOlderThan(Instant cutoff);

  /**
   * The number of entries the {@link #deleteOlderThan} retention guard currently protects — its
   * <b>exact complement</b>, minus the age filters: entries whose owning saga row is {@code
   * FAULTED} or shielded ({@code dead_letter_pending}), a named entry whose saga has no row (the
   * row-less residual), and every null-saga entry RESOLVED to a target (unconditionally, whatever
   * the target's current status). Read-only.
   *
   * <p>Because the sweep now protects those entries rather than pruning them, the stranded
   * population grows silently: {@code SAGA_FAULTED} is a counter fired once at fault time, not a
   * standing depth, and every runner (saga, timeout, compensation-retry) skips {@code FAULTED}
   * sagas. This count is what makes the backlog observable — the {@code
   * SagaDeadLetterRetentionSweeper} samples it once per cycle and reports it as the {@code
   * streamrune.saga.faulted_backlog} gauge, so a DEAD-LETTERED halted business process awaiting an
   * operator replay shows up on the metrics endpoint instead of only in a support ticket.
   *
   * <p><b>This covers the dead-lettered SUBSET of halted sagas, not halted sagas generally</b> — an
   * operator alerting on this gauge alone is blind to the rest. Every AUTOMATIC quarantine the
   * framework performs writes {@code FAULTED} with no entry at all: {@code
   * SagaRunner.faultStuckCompensation} (the compensation sweeper's stale-key / give-up route),
   * {@code SagaTimeoutRunner.faultStaleEpisode} and {@code faultPastDwell}, and {@code
   * SagaStepExecutor.faultEpisode} (a {@code compensate()} that threw on a resume, {@code
   * catchPoison = true}). Those sagas are equally halted and frequently hold half-moved money, but
   * they name no row here and this count cannot see them. The companion per-type {@code
   * streamrune.saga.faulted_rows} gauge counts the saga rows themselves and is the one to alert on
   * for the FULL halted population; this one stays anchored to the entries, because its scoping
   * must keep matching the retention guard exactly.
   *
   * <p><b>Scoping matches the guard exactly</b> so the gauge and the retention decision can never
   * disagree. A NAMED entry is counted when its owner row is {@code FAULTED} or shielded ({@code
   * dead_letter_pending}), or when it has no owner row at all — type-scoped: an entry is judged
   * only against its OWN type's row. A null-saga entry is counted exactly when it has been RESOLVED
   * to a target, whatever that target's row says; an unresolved null-saga entry is never counted.
   * Age is irrelevant: the question is "how many recovery handles are stranded right now?", not
   * "how old are they?".
   *
   * <p>The default throws — override in stores that can see saga state. A store that cannot is
   * simply excluded from the gauge (the sweeper disables the sample after the first {@link
   * UnsupportedOperationException} and keeps pruning, mirroring {@code OutboxStore.countByStatus});
   * pruning and replay are unaffected. Implementations that cannot observe saga state must NOT
   * return {@code 0} — an empty backlog on a dashboard while sagas are stranded is worse than a
   * missing series.
   *
   * @return the number of dead-letter entries the {@link #deleteOlderThan} guard protects: named
   *     entries whose owner row is {@code FAULTED}, shielded or absent, plus every resolved
   *     null-saga entry
   */
  default long countFaultedBacklog() {
    throw new UnsupportedOperationException(
        "This SagaDeadLetterStore does not support countFaultedBacklog — override it to enable the"
            + " streamrune.saga.faulted_backlog gauge.");
  }

  /**
   * A quarantined poison saga event.
   *
   * <p><b>Metadata only — the event payload is deliberately NOT duplicated here.</b>
   * Crypto-shredding (GDPR "right to be forgotten" via per-subject key deletion) is authoritative
   * only for the {@code event_stream} table: that is the sole store whose payload column is
   * governed by {@code @Encrypted} fields and erased when a subject's key is shredded. Copying the
   * decrypted in-memory event into a second table would create an unshreddable, plaintext copy of
   * personal data, defeating the crypto-shred guarantee entirely. Instead, this entry carries only
   * {@code eventOffset}, which points back into {@code event_stream}: to inspect a quarantined
   * event, read the event store at that offset. Post-shred, that read correctly comes back
   * redacted/unreadable — that is the intended behavior, not a bug.
   *
   * @param sagaId the saga that faulted, or {@code null} when the saga could not be identified
   *     (e.g. {@code extractSagaId} / {@code correlate} threw before a saga ID was resolved)
   * @param sagaType the saga type of the QUARANTINING runner (required) — stamped by the
   *     framework's {@code SagaRunner} on every entry, <b>including null-saga entries</b> (the
   *     runner's type is well-defined even when the saga id is not). Every type-scoped operation
   *     matches an entry by this value, so one type's replay never sees, anchors, resolves or
   *     discards another type's entry at the same offset
   * @param eventOffset the global offset of the offending event (required; used as idempotency
   *     key). Also the sole way to locate the offending event's payload: look it up in the event
   *     store at this offset — it is never copied into this record.
   * @param eventType the type of the offending event (required)
   * @param errorType the fully-qualified class name of the exception (required)
   * @param errorMessage the exception message, may be {@code null}. May contain orchestrator
   *     exception text; orchestrator authors should avoid embedding event field values in exception
   *     messages so this column does not become a second, unshreddable copy of personal data. This
   *     cannot be enforced by the framework — it is a documented expectation of orchestrator
   *     authors.
   * @param faultedAt the instant the fault was <em>most recently</em> recorded (required). The
   *     idempotent re-quarantine upsert refreshes this on every re-fire, so it is useless as a
   *     staleness anchor. The stores order entries by it, newest first ({@link #findAll}, {@link
   *     #findBySaga}, and the one row surfaced for a null-saga duplicate set). The replayer does
   *     not read it: a still-poison feed is recognised from the feed's own outcome and the saga row
   *     afterwards, not from this instant.
   * @param firstFaultedAt the instant this {@code (sagaId, eventOffset)} pair was FIRST
   *     quarantined. Unlike {@code faultedAt}, this is <b>immutable across re-quarantines</b>: the
   *     store's publish upsert sets it on the first insert and never updates it, so a failed replay
   *     cannot reset it. It anchors the retention sweep ({@link #deleteOlderThan}). It does NOT
   *     anchor the stale-compensation replay guard, which reads the saga row's {@code
   *     episode_claimed_at} (every write that enters {@code COMPENSATING} stamps it). On a publish
   *     input it is merely the candidate value for a first insert (the stores keep the existing
   *     value on conflict, and store {@code faultedAt} for a {@code null} candidate); the
   *     seven-argument constructor defaults it to {@code faultedAt}. A store therefore never
   *     persists a {@code null} first-fault instant, and every entry read back from a store carries
   *     one.
   * @param firstReplayStartedAt the instant a replay attempt was FIRST attempted on this entry,
   *     established solely by {@link #establishFirstReplayAnchor} — <b>the only replay-related fact
   *     the entry keeps</b> (no in-flight marker is needed: the executor's atomic create-first/CAS
   *     writes keep the saga row {@code FAULTED} for an attempt's whole duration, so the ordinary
   *     owner shield — {@code FAULTED} or {@code dead_letter_pending} on the saga row — already
   *     covers the in-flight window; see {@link #deleteOlderThan}). {@code null} when no attempt
   *     has ever been made. <b>Immutable for the entry's life</b> — establish-only, the stored
   *     value always wins — the exact mirror of {@code firstFaultedAt} vs {@code faultedAt}. The
   *     re-quarantine {@link #publish} upsert likewise preserves the stored value on a conflict. It
   *     anchors the {@code SagaDeadLetterReplayer} key-age guard ({@code STALE_REDRIVE_BLOCKED}):
   *     {@link #establishFirstReplayAnchor} always commits before the feed that dispatches, so this
   *     is the earliest instant any of the entry's replay attempts can have written a forward
   *     command-inbox key — once {@code now - firstReplayStartedAt} exceeds the inbox retention
   *     window, a re-driven forward command may MISS the pruned inbox and re-execute, so the
   *     re-drive is refused instead. On a publish input this is normally {@code null}; stores
   *     persist it verbatim on a first insert and keep the STORED value on a conflict.
   * @param targetSagaId the durably resolved TARGET saga of a <b>null-saga</b> entry, or {@code
   *     null} when no replay has resolved one. Stamped by the {@code SagaDeadLetterReplayer} (via
   *     {@link #setResolvedTarget}) whenever a replay classifies {@code TARGET_PENDING} — the
   *     moment it has just resolved the event's target through the fixed routers, before any
   *     refusal, anchor or feed. A null-saga entry names no {@code saga_state} row of its own, so
   *     this column is its OWN retention shield: once resolved, the entry is protected
   *     UNCONDITIONALLY — operator-attended by construction — regardless of the target's current
   *     status, until a successful replay discards it. Idempotently re-stamped on every {@code
   *     TARGET_PENDING} (a routing change simply overwrites it); there is deliberately no clear
   *     operation. Always {@code null} for named entries. On a publish input this is normally
   *     {@code null}; stores persist it verbatim on a first insert and keep the STORED value on a
   *     conflict (a re-quarantine must not strip the retention protection the resolution
   *     established) — implementations whose unique key cannot upsert a null {@code sagaId}
   *     additionally carry the NEWEST stored value forward onto a fresh duplicate row so a
   *     refreshed null-saga entry keeps the target the drain folds on.
   */
  record SagaDeadLetterEntry(
      SagaId sagaId,
      SagaType sagaType,
      GlobalOffset eventOffset,
      EventType eventType,
      String errorType,
      String errorMessage,
      Instant faultedAt,
      Instant firstFaultedAt,
      Instant firstReplayStartedAt,
      SagaId targetSagaId) {

    public SagaDeadLetterEntry {
      Objects.requireNonNull(sagaType, "sagaType is required");
      Objects.requireNonNull(eventOffset, "eventOffset is required");
      Objects.requireNonNull(eventType, "eventType is required");
      Objects.requireNonNull(errorType, "errorType is required");
      Objects.requireNonNull(faultedAt, "faultedAt is required");
    }

    /**
     * Constructor without a first-replay-attempt anchor or resolved-target hint. {@code
     * firstFaultedAt} is explicit; {@code firstReplayStartedAt} and {@code targetSagaId} default to
     * {@code null} — correct for a publish input: only {@link #establishFirstReplayAnchor}
     * establishes the anchor and only {@link #setResolvedTarget} stamps the target, and on an
     * upsert conflict the stores keep the STORED values regardless.
     */
    public SagaDeadLetterEntry(
        SagaId sagaId,
        SagaType sagaType,
        GlobalOffset eventOffset,
        EventType eventType,
        String errorType,
        String errorMessage,
        Instant faultedAt,
        Instant firstFaultedAt) {
      this(
          sagaId,
          sagaType,
          eventOffset,
          eventType,
          errorType,
          errorMessage,
          faultedAt,
          firstFaultedAt,
          null,
          null);
    }

    /**
     * Constructor without an explicit first-fault instant (the common quarantine shape). {@code
     * firstFaultedAt} defaults to {@code faultedAt} — correct for a publish input, where this
     * quarantine attempt's own timestamp is the first-insert candidate (the stores preserve the
     * older stored value on an upsert conflict).
     */
    public SagaDeadLetterEntry(
        SagaId sagaId,
        SagaType sagaType,
        GlobalOffset eventOffset,
        EventType eventType,
        String errorType,
        String errorMessage,
        Instant faultedAt) {
      this(sagaId, sagaType, eventOffset, eventType, errorType, errorMessage, faultedAt, faultedAt);
    }
  }
}
