package org.streamrune.core.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Persistence contract for outbox entries.
 *
 * <p>Implementations must guarantee idempotent saves: a second {@link #save} with the same {@link
 * OutboxEntryId} must be a no-op (duplicate detection).
 */
public interface OutboxStore {

  /**
   * Persists a new outbox entry. If an entry with the same {@link OutboxEntryId} already exists,
   * this call is silently ignored.
   *
   * @throws OutboxOrderingViolationException if this store is {@link
   *     OutboxOrderingMode#STRICT_PER_AGGREGATE} and the entry's {@code streamId} is {@code null} —
   *     thrown before any write, unwrapped, so an enclosing append fails and rolls back
   */
  void save(OutboxEntry entry);

  /**
   * Atomically <em>claims</em> and returns up to {@code limit} deliverable entries, ordered by
   * insertion sequence (seq) ascending; createdAt is not monotonic within a transaction. Claiming
   * moves each returned entry out of {@code PENDING} (the JDBC implementation uses {@code FOR
   * UPDATE SKIP LOCKED} to mark them {@code IN_PROGRESS} with a claim owner/timestamp), so a second
   * concurrent or immediately-following {@code loadPending} does not return the same entries — this
   * is what lets multiple relay instances poll the same outbox without double-publishing. A claim
   * is released back to {@code PENDING} by {@link #markRetry} (a transient failure) or reclaimed
   * after a lease timeout if the claiming relay crashes before resolving it.
   *
   * <p>An entry is eligible if its {@code nextRetryAt} is {@code null} (never failed) or in the
   * past (backoff window expired). Entries with {@code nextRetryAt} in the future are skipped.
   *
   * <p><b>Per-aggregate ordering.</b> For a non-null {@code streamId}, the <em>head</em> of stream
   * {@code A} is its lowest-{@code seq} row whose status is {@code PENDING} or — on a {@link
   * OutboxOrderingMode#STRICT_PER_AGGREGATE} channel — {@code FAILED}. {@code A} is claimable iff
   * its head is {@code PENDING}, eligible ({@code nextRetryAt} null or past) and no row of {@code
   * A} is {@code IN_PROGRESS}. A claim takes only heads, so a batch holds at most one entry per
   * stream and delivery within a stream is serial in {@code seq} order. A backing-off head holds
   * the whole stream. Two aggregate types sharing an id value are two streams with two heads. In
   * strict mode a terminal {@code FAILED} head is still the head: nothing of {@code A} is delivered
   * until an operator replays it ({@link #resetFailedToPending}, same row, same {@code seq}, so it
   * is delivered before its successors) or skips it ({@link #skipFailed}). On an {@link
   * OutboxOrderingMode#AVAILABILITY_FIRST} channel a {@code FAILED} row is not a head: the next
   * {@code PENDING} row of {@code A} is claimed past it (a gap, and a later replay arrives after
   * already-delivered successors — consumers of such a channel must be idempotent and
   * order-tolerant). Entries with a {@code null} {@code streamId} are never heads and are claimed
   * freely in both modes.
   *
   * @param limit maximum number of entries to return; must be positive
   */
  List<OutboxEntry> loadPending(int limit);

  /**
   * The identity this store claims and marks entries under. {@link #loadPending} stamps this value
   * into each claimed entry's {@code claimed_by}, and every {@code mark*} method guards its write
   * on {@code claimed_by = }{@code claimedBy()} so a relay whose lease was stolen cannot overwrite
   * the outcome recorded by the relay that actually holds the claim. Each store instance (i.e. each
   * relay process) has its own stable identity for its lifetime.
   */
  String claimedBy();

  /**
   * The claim lease: how long a claimed ({@code IN_PROGRESS}) entry is honored before another relay
   * may reclaim it back to {@code PENDING} (the crash-recovery / stuck-claim path). The {@code
   * OutboxPoller} reads this to bound its per-batch publish cycle to strictly less than the lease:
   * if a live-but-slow relay kept publishing past the lease, a second relay would reclaim and
   * re-deliver its still-in-flight entries, producing a duplicate <em>and</em> out-of-order
   * same-aggregate delivery that the per-aggregate ordering contract otherwise forbids. Coupling
   * the publish deadline to the lease closes that window.
   *
   * <p>There is <b>no safe default</b>: this value must equal the store's own reclaim window and no
   * framework can guess it. The removed default of four minutes was a plausible-looking guess that
   * {@link org.streamrune.core.outbox.OutboxStore}'s poller (its lease-sizing guard) would then
   * validate as if it were real — a store whose actual reclaim window is shorter would pass the
   * guard and still reclaim a still-in-flight entry, producing the exact duplicate + same-aggregate
   * reorder the guard exists to refuse. The default therefore throws {@link
   * UnsupportedOperationException}; the poller reads this once in {@code build()} before any poll,
   * so an un-overriding store fails loudly at wiring rather than silently in production, and so
   * does one that returns {@code null} ({@code build()} throws {@link IllegalStateException} naming
   * the store class). A store that deliberately runs without deadline-bounding overrides this to
   * return a non-positive {@link Duration} — the documented opt-out (the poller then applies no
   * publish deadline and skips the guard) — a visible, deliberate choice. The framework's {@code
   * PostgresOutboxStore} (see {@code DEFAULT_CLAIM_LEASE} — four minutes, strictly exceeding
   * Kafka's full 180s in-flight horizon: {@code max.block.ms} 60s + {@code delivery.timeout.ms}
   * 120s) and {@code InMemoryOutboxStore} override it.
   *
   * @return the claim lease honored before an {@code IN_PROGRESS} entry is reclaimable; never
   *     {@code null} (a non-positive value opts out of deadline-bounding)
   * @throws UnsupportedOperationException if this store has not overridden the method
   */
  default Duration claimLease() {
    throw new UnsupportedOperationException(
        "claimLease is not implemented by this OutboxStore; it must return this store's own reclaim"
            + " window (the point at which a stuck IN_PROGRESS claim returns to PENDING)."
            + " OutboxPoller.build()'s lease-sizing guard bounds the publish cycle against it, so a"
            + " guessed default would pass the guard while the store reclaims a still-in-flight"
            + " entry, causing a duplicate and a same-aggregate reorder. Override it (see"
            + " PostgresOutboxStore / InMemoryOutboxStore), or return a non-positive Duration to opt"
            + " out of deadline-bounding explicitly.");
  }

  /**
   * The ordering mode this store enforces in {@link #loadPending} (which rows are heads, whether a
   * {@code FAILED} head blocks) and in {@link #save} (whether a {@code null} {@code streamId} is
   * rejected). There is <b>no safe default</b>: a store that does not say what it enforces cannot
   * be trusted to enforce it (the {@link #claimLease()} precedent). {@code OutboxPoller.build()}
   * reads this before the first poll, so an un-overriding store fails at wiring, and so does one
   * that returns {@code null} ({@code build()} throws {@link IllegalStateException} naming the
   * store class; an un-stubbed mock of this interface returns {@code null}). {@code
   * PostgresOutboxStore} and {@code InMemoryOutboxStore} override it; both default to {@link
   * OutboxOrderingMode#STRICT_PER_AGGREGATE}.
   *
   * @return the ordering mode this store enforces; never {@code null}
   * @throws UnsupportedOperationException if this store has not overridden the method
   */
  default OutboxOrderingMode orderingMode() {
    throw new UnsupportedOperationException(
        "orderingMode is not implemented by this OutboxStore; it must return the"
            + " OutboxOrderingMode this store enforces in loadPending and save"
            + " (STRICT_PER_AGGREGATE or AVAILABILITY_FIRST). OutboxPoller.build() reads it before"
            + " the first poll. Override it (see PostgresOutboxStore / InMemoryOutboxStore).");
  }

  /**
   * Marks an entry as {@code DELIVERED} and records the current timestamp as {@code processedAt}.
   * The write is a compare-and-set: it only transitions the entry when it is still {@code
   * IN_PROGRESS} <em>and</em> claimed by {@code claimedBy}.
   *
   * @param id the entry to mark delivered
   * @param claimedBy the identity that claimed the entry (see {@link #claimedBy()})
   * @return {@code true} if the entry transitioned; {@code false} means the lease was lost — the
   *     entry is no longer {@code IN_PROGRESS} under this {@code claimedBy} (another holder already
   *     recorded the outcome, expected under failover). Callers must treat {@code false} as a
   *     <b>benign no-op, not an error</b>.
   */
  boolean markDelivered(OutboxEntryId id, String claimedBy);

  /**
   * Marks multiple entries delivered under {@code claimedBy}. The default loops {@link
   * #markDelivered(OutboxEntryId, String)}; database-backed stores should override with a single
   * set-based UPDATE. Each entry is a compare-and-set on {@code IN_PROGRESS} + {@code claimedBy},
   * so an id whose lease was lost is simply skipped.
   *
   * @param ids the entries to mark delivered
   * @param claimedBy the identity that claimed the entries (see {@link #claimedBy()})
   * @return the number of entries that actually transitioned to {@code DELIVERED}; a value less
   *     than {@code ids.size()} means the remaining ids' leases were lost — a <b>benign no-op, not
   *     an error</b>.
   */
  default int markDeliveredAll(java.util.Collection<OutboxEntryId> ids, String claimedBy) {
    int transitioned = 0;
    for (OutboxEntryId id : ids) {
      if (markDelivered(id, claimedBy)) {
        transitioned++;
      }
    }
    return transitioned;
  }

  /**
   * Releases the claims on entries that were <b>never handed to the transport</b>, returning them
   * to {@code PENDING} <em>without</em> touching the retry ladder.
   *
   * <p><b>Only for provably un-attempted entries.</b> The {@code OutboxPoller} calls this for the
   * entries a deadline-truncated (or interrupt-truncated) publish cycle left in <em>neither</em>
   * bucket of {@link OutboxPublisher.BatchResult} — see {@link OutboxPublisher#publishBatch(List,
   * java.time.Instant)}, whose contract is that such an entry was never passed to {@code publish}.
   * Nothing exists at the transport for them, so re-arming cannot duplicate. Never call this for an
   * entry whose delivery outcome is merely <em>unknown</em>: that is the {@link
   * OutboxPublisher.FailureKind#IN_FLIGHT} case, where the claim must be held until the lease
   * expires so a second relay cannot re-publish into a delivery still resolving.
   *
   * <p>Holding those claims instead is safe but collapses throughput exactly when the broker is
   * degraded: a claimed entry keeps its aggregate un-claimable ({@code loadPending} admits at most
   * one in-flight entry per aggregate), and no path exists for a live relay to release its own
   * claim — {@code RECLAIM_EXPIRED} only fires past the lease and the claim statements only see
   * {@code PENDING} rows. A relay that claimed 100 aggregates and reached 6 before its deadline
   * would therefore idle behind its own 94 never-touched entries for a full lease.
   *
   * <p><b>Releasing and burning an attempt are separable.</b> This must NOT go through {@link
   * #markRetry} semantics of a failed attempt: {@code attempts} stays where it was, so a relay
   * whose deadline keeps truncating cannot walk an entry it never attempted up to terminal {@code
   * FAILED} (which would also block its aggregate on a {@code STRICT_PER_AGGREGATE} channel, or
   * un-gate it and reorder the stream on an {@code AVAILABILITY_FIRST} one). The entry becomes
   * immediately eligible again — no backoff is stamped for a delivery that never happened.
   *
   * <p>Each release is a compare-and-set on {@code IN_PROGRESS} + {@code claimedBy}, so an entry
   * whose lease was stolen (or already reclaimed) is skipped — a <b>benign no-op, not an error</b>.
   *
   * <p>The default loops {@link #markRetry} re-passing each entry's own {@code attempts} and {@code
   * lastError} with a {@link Duration#ZERO} backoff, which releases the claim without advancing the
   * ladder or discarding the diagnostic; database-backed stores should override with a single
   * set-based UPDATE that leaves {@code attempts}, {@code last_error} and {@code next_retry_at}
   * untouched (the entry was claimable a moment ago, so its {@code next_retry_at} is already past).
   *
   * @param entries the never-attempted entries whose claims are to be released, as loaded by {@link
   *     #loadPending}
   * @param claimedBy the identity that claimed the entries (see {@link #claimedBy()})
   * @return the number of entries that actually returned to {@code PENDING}; a value less than
   *     {@code entries.size()} means the remaining leases were lost
   */
  default int releaseClaims(java.util.Collection<OutboxEntry> entries, String claimedBy) {
    int released = 0;
    for (OutboxEntry entry : entries) {
      if (markRetry(entry.id(), entry.attempts(), entry.lastError(), Duration.ZERO, claimedBy)) {
        released++;
      }
    }
    return released;
  }

  /**
   * Deletes DELIVERED entries whose processed-at timestamp is older than {@code olderThan}
   * (retention). The default throws — override in stores that support retention.
   *
   * @return the number of rows deleted
   */
  default int deleteDelivered(java.time.Instant olderThan) {
    throw new UnsupportedOperationException(
        "This OutboxStore does not support retention — override deleteDelivered(Instant).");
  }

  /**
   * Returns up to {@code limit} entries currently in {@code status}, <b>oldest first</b> (by {@code
   * seq} ascending), for operator enumeration/paging. Read-only: it does <em>not</em> claim entries
   * ({@code claimed_by} is untouched), so it is safe to call while relays poll. Oldest-first means
   * an operator paging a large backlog starts at the entries that have been stuck longest.
   *
   * @param status the status to enumerate (e.g. {@link OutboxStatus#FAILED})
   * @param limit the maximum number of entries to return; must be positive
   * @return the matching entries, oldest ({@code seq}) first, capped at {@code limit}
   */
  List<OutboxEntry> findByStatus(OutboxStatus status, int limit);

  /**
   * Read-only lookup of one entry by id, for operator tooling ({@code OutboxFailedReplayer} reads
   * the stream for its log line and distinguishes NOT_FOUND from NOT_FAILED with it). Never claims.
   */
  Optional<OutboxEntry> findById(OutboxEntryId id);

  /**
   * Operator skip: {@code FAILED → SKIPPED}, writing {@code skipped_at} (store clock), {@code
   * skipped_by} and {@code skip_reason} together with the status in ONE statement. Guarded on
   * {@code status = 'FAILED'} only — a FAILED row has no live claim to compare against (the {@link
   * #resetFailedToPending} precedent). On a {@code STRICT_PER_AGGREGATE} channel a skipped head
   * releases its aggregate: the next {@code PENDING}/{@code FAILED} row becomes the head and is
   * claimable from the next poll. A skip is not a delivery.
   *
   * <p>Reached through {@code OutboxFailedReplayer.skip}/{@code skipFailed}, which validate and
   * sanitize {@code skippedBy} and {@code reason} before calling this; the store persists what it
   * is given.
   *
   * @return {@code true} if a {@code FAILED} entry became {@code SKIPPED}; {@code false} if the
   *     entry is absent or not {@code FAILED} (already replayed, already skipped, never failed)
   */
  boolean skipFailed(OutboxEntryId id, String skippedBy, String reason);

  /**
   * One read for the blockage gauges and the relay's first-cycle legacy-data check: the number of
   * distinct NON-NULL streams {@code (aggregate_type, aggregate_id)} with an unresolved {@code
   * FAILED} row ({@code count(DISTINCT (aggregate_type, aggregate_id)) FILTER (WHERE aggregate_id
   * IS NOT NULL)} — on a strict channel each one is blocked), the {@code processed_at} of the
   * oldest {@code FAILED} row of ANY stream ({@code null} when none — the age gauge is the alert
   * series, so it covers rows with no stream too), and the number of {@code FAILED} rows with no
   * stream (legacy availability-first data on a strict channel). The default throws — the poller
   * then logs one WARN and disables the blockage gauges.
   */
  default BlockageSample sampleBlockage() {
    throw new UnsupportedOperationException(
        "This OutboxStore does not support sampleBlockage — override it to enable the outbox"
            + " blockage gauges (streamrune.outbox.blocked_aggregates, blockage_age_seconds).");
  }

  /** See {@link #sampleBlockage()}. */
  record BlockageSample(long failedAggregates, Instant oldestFailedAt, long nullAggregateFailed) {}

  /**
   * Deletes {@code SKIPPED} entries whose {@code skipped_at} is strictly before {@code cutoff}
   * (SKIPPED retention). The default throws — override in stores that support retention. There is
   * no FAILED retention: the framework never deletes an unresolved {@code FAILED} row.
   *
   * @return the number of rows deleted
   */
  default int deleteSkipped(Instant cutoff) {
    throw new UnsupportedOperationException(
        "This OutboxStore does not support SKIPPED retention — override deleteSkipped(Instant).");
  }

  /**
   * Resets a single {@code FAILED} entry back to {@code PENDING} for redelivery, clearing its retry
   * and claim bookkeeping ({@code attempts=0}, {@code last_error=NULL}, {@code next_retry_at=NULL},
   * {@code claimed_at=NULL}, {@code claimed_by=NULL}). This is an <b>operator action on a terminal
   * row</b>, invoked by {@code OutboxFailedReplayer} after a broker fix — it deliberately
   * <em>bypasses</em> the {@code mark*} CAS (which guards on {@code IN_PROGRESS} + {@code
   * claimed_by}); a {@code FAILED} row has no live claim to compare against. Once reset, the entry
   * re-enters {@link #loadPending} as an eligible {@code PENDING} row ordered against that
   * aggregate's <em>remaining</em> pending work — the claim SQL, not this method, enforces
   * ordering.
   *
   * <p><b>Position-preserving.</b> Only the status and the retry/claim bookkeeping change; {@code
   * seq} never does. On a {@code STRICT_PER_AGGREGATE} channel the successors never moved while the
   * entry sat {@code FAILED}, so the replayed row is its aggregate's head again and is delivered
   * <em>before</em> every later row. On an {@code AVAILABILITY_FIRST} channel the successors may
   * already be {@code DELIVERED}; the replayed entry then arrives after them.
   *
   * @param id the entry to reset; must currently be {@code FAILED}
   * @return {@code true} if a {@code FAILED} entry was reset; {@code false} if no entry with that
   *     id exists or it was not {@code FAILED}
   */
  boolean resetFailedToPending(OutboxEntryId id);

  /**
   * Marks an entry as {@code FAILED} (terminal). Records {@code attempts} and {@code error} for
   * observability. Called by the OutboxPoller once all retries are exhausted. The write is a
   * compare-and-set: it only transitions the entry when it is still {@code IN_PROGRESS}
   * <em>and</em> claimed by {@code claimedBy}.
   *
   * @param id the entry to mark as failed
   * @param attempts total number of delivery attempts made
   * @param error the last error message, or {@code null} if unavailable
   * @param claimedBy the identity that claimed the entry (see {@link #claimedBy()})
   * @return {@code true} if the entry transitioned; {@code false} means the lease was lost — a
   *     <b>benign no-op, not an error</b> (see {@link #markDelivered(OutboxEntryId, String)}).
   */
  boolean markFailed(OutboxEntryId id, int attempts, String error, String claimedBy);

  /**
   * Records a failed delivery attempt without marking the entry as terminal. Releases the claim:
   * updates {@code attempts}, {@code last_error}, and {@code next_retry_at} and returns the entry
   * to {@code PENDING}. The write is a compare-and-set: it only transitions the entry when it is
   * still {@code IN_PROGRESS} <em>and</em> claimed by {@code claimedBy}.
   *
   * <p><b>The backoff is a duration, stamped by the store's own clock — never a caller-supplied
   * absolute instant.</b> The store sets {@code next_retry_at = }<i>storeNow</i>{@code + backoff}
   * using the SAME clock that {@link #loadPending} evaluates {@code next_retry_at} against (the
   * database {@code NOW()} for the Postgres store). Passing an instant computed on the relay JVM's
   * wall clock — as this once did — lets a relay whose clock lags the store write an already-past
   * {@code next_retry_at}, collapsing exponential backoff into a per-poll retry storm and
   * exhausting the retry ladder prematurely under a broker outage. Passing a {@link Duration} keeps
   * the backoff on one clock and immune to relay/store clock skew, mirroring the reclaim-lease
   * cutoff.
   *
   * @param id the entry to update
   * @param attempts total number of delivery attempts made so far
   * @param error the last error message, or {@code null} if unavailable
   * @param backoff how long from now (store clock) until this entry is next eligible for delivery;
   *     a zero or negative duration makes it immediately eligible
   * @param claimedBy the identity that claimed the entry (see {@link #claimedBy()})
   * @return {@code true} if the entry transitioned; {@code false} means the lease was lost — a
   *     <b>benign no-op, not an error</b> (see {@link #markDelivered(OutboxEntryId, String)}).
   */
  boolean markRetry(
      OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy);

  /**
   * Returns the number of entries currently in {@code status}. Read-only: it does not claim entries
   * or take any lease. Used for backlog observability — the {@code OutboxPoller} samples the {@code
   * PENDING} count each poll cycle and reports it as the {@code streamrune.outbox.pending} gauge,
   * so a stalled or lagging relay is visible before any entry exhausts its retry ladder.
   *
   * <p>The default throws — override in stores that support counting. A store that does not is
   * simply excluded from the backlog gauge (the poller degrades gracefully); delivery is
   * unaffected.
   *
   * @param status the status to count
   * @return the number of entries in {@code status}
   */
  default long countByStatus(OutboxStatus status) {
    throw new UnsupportedOperationException(
        "This OutboxStore does not support countByStatus — override it to enable the outbox backlog"
            + " gauge.");
  }

  /**
   * Permanently deletes a non-{@code FAILED} entry (tests, tooling). <b>Refuses a {@code FAILED}
   * row</b> with {@link IllegalStateException}: the only exits from {@code FAILED} are the two
   * audited operator operations, {@link #skipFailed} and {@link #resetFailedToPending} — a raw
   * delete of a blocking head would release its aggregate with no record. An absent id is a no-op.
   * No framework code calls this.
   *
   * @throws IllegalStateException if the entry is currently {@code FAILED}
   */
  void delete(OutboxEntryId id);
}
