package org.streamrune.test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;

/**
 * In-memory {@link OutboxStore} for unit tests.
 *
 * <p>Thread-safe via {@link ConcurrentHashMap}. Not suitable for production use.
 *
 * <p>Idempotent save: a second {@link #save} with the same {@link OutboxEntryId} is silently
 * ignored ({@code putIfAbsent} semantics).
 *
 * <p><b>Claim semantics.</b> This double honors the same competing-consumer contract as the
 * production {@code PostgresOutboxStore}, so unit tests built on it cannot pass while the real
 * store would behave differently. {@link #loadPending} <em>claims</em> the entries it returns:
 * eligible {@code PENDING} entries are moved to {@code IN_PROGRESS} (with a claim timestamp) and
 * become invisible to a subsequent {@code loadPending} until the claim is resolved or its lease
 * expires — just as {@code FOR UPDATE SKIP LOCKED} keeps two relay instances from receiving the
 * same entry. The claim is resolved by {@link #markDelivered}/{@link #markFailed} (terminal) or
 * {@link #markRetry} (releases the claim back to {@code PENDING}). A claim whose lease has expired
 * is reclaimed back to {@code PENDING} on the next {@code loadPending}, mirroring the production
 * lease-reclaim path for crashed relays.
 *
 * <p><b>Per-stream ordering.</b> Parity with the production claim SQL: at any instant, for any
 * non-null {@code streamId}, at most one entry is {@code IN_PROGRESS} — always that stream's
 * <em>head</em> (seq = insertion order, modeling the {@code outbox_events.seq} BIGSERIAL). A head
 * is claimable only while it is {@code PENDING}, eligible and nothing of its stream is in flight:
 * if it is still backing off ({@code nextRetryAt} in the future), the whole stream waits — a later
 * entry is never claimed past it. Two aggregate types sharing an id value are two streams with two
 * heads. Entries with a {@code null} {@code streamId} have no ordering constraint and are claimed
 * freely in both modes. A single batch therefore contains at most one entry per stream.
 *
 * <p><b>Ordering modes</b> ({@link OutboxOrderingMode}, a constructor argument; the zero-, one- and
 * two-argument constructors are strict, like {@code PostgresOutboxStore}):
 *
 * <ul>
 *   <li>{@link OutboxOrderingMode#STRICT_PER_AGGREGATE} (default): the head is the lowest-seq entry
 *       among {@code PENDING} and {@code FAILED}, so a terminal {@code FAILED} head <b>blocks</b>
 *       every later entry of its stream until an operator replays it ({@link #resetFailedToPending}
 *       — same entry, same seq, delivered before its successors) or skips it ({@link #skipFailed} —
 *       audited {@code SKIPPED}, releases the stream). {@link #save} rejects an entry with no
 *       {@code streamId} with {@link OutboxOrderingViolationException} before anything is written.
 *   <li>{@link OutboxOrderingMode#AVAILABILITY_FIRST} (opt-in): the head is the lowest-seq {@code
 *       PENDING} entry, so a {@code FAILED} entry does not gate ordering — a later entry of the
 *       same stream is claimed past it (gap) and a later replay arrives after already-delivered
 *       successors (reorder). Entries with no {@code streamId} are accepted.
 * </ul>
 *
 * <p>In both modes {@link #delete} refuses a {@code FAILED} entry and no retention sweep ever
 * removes one ({@link #deleteSkipped} prunes only {@code SKIPPED}). {@link #withOrderingMode}
 * builds a second relay over the SAME entries enforcing another mode — the
 * channel-restart-with-a-different-mode event. See {@link
 * org.streamrune.core.outbox.OutboxStore#loadPending}.
 *
 * <p><b>No reset-vs-claim race.</b> Every mutator (including {@link #loadPending} and {@link
 * #resetFailedToPending}) runs under one {@code synchronized (lock)}, so a reset can never
 * interleave with a claim's snapshot the way it can under PostgreSQL READ COMMITTED. The
 * ≤1-IN_PROGRESS gate is therefore always evaluated against the committed state and cannot be
 * bypassed — this store needs no equivalent of the production {@code
 * ux_outbox_one_inflight_per_stream} unique index.
 *
 * <p>Time is read from the injected {@link Clock} — pass a {@link MutableClock} to test retry
 * backoff ({@code nextRetryAt} gating in {@link #loadPending}) and claim-lease reclaim without
 * sleeping. The no-arg constructor uses {@link Clock#systemUTC()} and the {@linkplain
 * #DEFAULT_CLAIM_LEASE default lease}.
 *
 * <p>Use {@link #all()} in assertions to inspect entries regardless of status.
 */
public final class InMemoryOutboxStore implements OutboxStore {

  /**
   * Default claim lease: an {@code IN_PROGRESS} claim older than this is considered abandoned.
   * <b>Four minutes</b>, matching {@code PostgresOutboxStore.DEFAULT_CLAIM_LEASE} and the {@code
   * OutboxStore.claimLease()} SPI default (all raised from one minute so a publish cycle fits
   * inside the lease, then from three to cover the Kafka publisher's maximum block time). The lease
   * must strictly exceed every wired publisher's in-flight horizon (enforced by {@code
   * OutboxPoller.build()}); the largest shipped default horizon is Kafka's {@code max.block.ms}
   * (60s) + {@code delivery.timeout.ms} (120s) = 180s, so this double at the previous three-minute
   * default paired with a shipped {@code KafkaOutboxPublisher} would have tripped that guard at
   * construction (180s is not STRICTLY above 180s). Four minutes clears 180s with head-room for
   * relay-vs-store clock skew.
   *
   * <p>Keep this in lock-step with the two production defaults: a lagging test-kit default silently
   * makes the double boot-incompatible with the shipped Kafka publisher (caught in an earlier
   * review, re-checked here).
   */
  public static final Duration DEFAULT_CLAIM_LEASE = Duration.ofMinutes(4);

  // The map, counter and lock fields below (all but claimedBy) are the "table": a store built by
  // withOrderingMode shares them with the store it was derived from (one table, two relays).
  private final ConcurrentHashMap<OutboxEntryId, OutboxEntry> store;
  // Claim timestamps for IN_PROGRESS entries, keyed by entry id. Models claimed_at; an entry is
  // claimed iff it is IN_PROGRESS and has an entry here. Released/finished entries are removed.
  private final ConcurrentHashMap<OutboxEntryId, Instant> claimedAt;
  // The identity that currently holds each IN_PROGRESS claim, keyed by entry id (models
  // outbox_events.claimed_by). Stamped on claim in loadPending; every mark* is a compare-and-set
  // that transitions only while the entry is IN_PROGRESS AND this equals the passed identity.
  private final ConcurrentHashMap<OutboxEntryId, String> claimedByOf;
  // This store instance's stable claim identity (models a single relay process). loadPending stamps
  // it; the poller reads it via claimedBy() and threads it into every mark*.
  private final String claimedBy = "outbox-" + UUID.randomUUID();
  // Monotonic insertion sequence per entry, modeling the outbox_events.seq BIGSERIAL that claims
  // are ordered and per-stream heads are selected by. Assigned once on first save.
  private final ConcurrentHashMap<OutboxEntryId, Long> seqs;
  private final AtomicLong seqCounter;
  // Serializes loadPending's composite select-then-claim against concurrent status transitions,
  // mirroring the single-statement atomicity of the production claim SQL. Without it, a mark*
  // racing between the in-flight check and the head selection could let a claim leapfrog a head.
  private final Object lock;
  private final Clock clock;
  private final Duration claimLease;
  private final OutboxOrderingMode orderingMode;

  /**
   * Creates a store reading time from {@link Clock#systemUTC()} with the {@linkplain
   * #DEFAULT_CLAIM_LEASE default claim lease}.
   */
  public InMemoryOutboxStore() {
    this(Clock.systemUTC());
  }

  /**
   * Creates a store reading time from the given clock (e.g. a {@link MutableClock}) with the
   * {@linkplain #DEFAULT_CLAIM_LEASE default claim lease}.
   */
  public InMemoryOutboxStore(Clock clock) {
    this(clock, DEFAULT_CLAIM_LEASE);
  }

  /**
   * Creates a {@link OutboxOrderingMode#STRICT_PER_AGGREGATE} store reading time from the given
   * clock with an explicit claim lease. Strict by default, like {@code PostgresOutboxStore}.
   *
   * @param clock the clock governing {@code nextRetryAt} gating and lease arithmetic (required)
   * @param claimLease how long an {@code IN_PROGRESS} claim is honored before {@link #loadPending}
   *     reclaims it back to {@code PENDING}; must be positive
   */
  public InMemoryOutboxStore(Clock clock, Duration claimLease) {
    this(clock, claimLease, OutboxOrderingMode.STRICT_PER_AGGREGATE);
  }

  /**
   * Creates a store reading time from the given clock with an explicit claim lease and ordering
   * mode.
   *
   * @param clock the clock governing {@code nextRetryAt} gating and lease arithmetic (required)
   * @param claimLease how long an {@code IN_PROGRESS} claim is honored before {@link #loadPending}
   *     reclaims it back to {@code PENDING}; must be positive
   * @param orderingMode which ordering this channel enforces (see {@link OutboxOrderingMode}); the
   *     zero-, one- and two-argument constructors are {@code STRICT_PER_AGGREGATE}
   */
  public InMemoryOutboxStore(Clock clock, Duration claimLease, OutboxOrderingMode orderingMode) {
    this.clock = Objects.requireNonNull(clock, "clock is required");
    Objects.requireNonNull(claimLease, "claimLease is required");
    if (claimLease.isZero() || claimLease.isNegative()) {
      throw new IllegalArgumentException("claimLease must be positive");
    }
    this.claimLease = claimLease;
    this.orderingMode = Objects.requireNonNull(orderingMode, "orderingMode is required");
    this.store = new ConcurrentHashMap<>();
    this.claimedAt = new ConcurrentHashMap<>();
    this.claimedByOf = new ConcurrentHashMap<>();
    this.seqs = new ConcurrentHashMap<>();
    this.seqCounter = new AtomicLong();
    this.lock = new Object();
  }

  /** Shares every map, the lock and the seq counter of {@code shared}; a new claim identity. */
  private InMemoryOutboxStore(InMemoryOutboxStore shared, OutboxOrderingMode orderingMode) {
    this.clock = shared.clock;
    this.claimLease = shared.claimLease;
    this.orderingMode = Objects.requireNonNull(orderingMode, "orderingMode is required");
    this.store = shared.store;
    this.claimedAt = shared.claimedAt;
    this.claimedByOf = shared.claimedByOf;
    this.seqs = shared.seqs;
    this.seqCounter = shared.seqCounter;
    this.lock = shared.lock;
  }

  /**
   * A second relay over the SAME entries enforcing {@code mode} — what restarting a channel with a
   * different mode over one {@code outbox_events} table is. Its own {@link #claimedBy()}.
   *
   * @param mode the ordering mode the new relay enforces (required)
   * @return a store sharing this store's entries, seq counter and lock
   */
  public InMemoryOutboxStore withOrderingMode(OutboxOrderingMode mode) {
    return new InMemoryOutboxStore(this, mode);
  }

  @Override
  public OutboxOrderingMode orderingMode() {
    return orderingMode;
  }

  /**
   * Saves {@code entry} (idempotent per id). A {@link OutboxOrderingMode#STRICT_PER_AGGREGATE}
   * store rejects an entry with no {@code streamId} with {@link OutboxOrderingViolationException}
   * before anything is written.
   */
  @Override
  public void save(OutboxEntry entry) {
    Objects.requireNonNull(entry, "entry is required");
    if (orderingMode == OutboxOrderingMode.STRICT_PER_AGGREGATE && entry.streamId() == null) {
      throw new OutboxOrderingViolationException(entry.id(), entry.payloadType());
    }
    synchronized (lock) {
      if (store.putIfAbsent(entry.id(), entry) == null) {
        seqs.put(entry.id(), seqCounter.incrementAndGet());
      }
    }
  }

  /**
   * Claims up to {@code limit} eligible entries for this instance and returns them with status
   * {@code IN_PROGRESS}. Claimed entries are invisible to a concurrent (or immediate second) {@code
   * loadPending} until {@link #markRetry} releases them, {@link #markDelivered}/{@link #markFailed}
   * finishes them, or the claim lease expires. First reclaims any abandoned claims.
   *
   * <p>Per non-null stream, only its head is ever claimed, and only while the head is {@code
   * PENDING}, eligible, and nothing of that stream is in flight. The head is the lowest-seq {@code
   * PENDING} entry or — in {@link OutboxOrderingMode#STRICT_PER_AGGREGATE} mode — the lowest-seq
   * entry among {@code PENDING} and {@code FAILED}, so a {@code FAILED} head blocks its stream (see
   * the class javadoc). Entries with no {@code streamId} are claimed freely in both modes.
   */
  @Override
  public List<OutboxEntry> loadPending(int limit) {
    if (limit < 1) throw new IllegalArgumentException("limit must be >= 1");
    synchronized (lock) {
      var now = clock.instant();
      reclaimExpired(now);

      // Streams with an entry currently in flight: gated entirely (≤1 IN_PROGRESS each).
      Set<StreamId> inFlight = new HashSet<>();
      for (OutboxEntry e : store.values()) {
        if (e.status() == OutboxStatus.IN_PROGRESS && e.streamId() != null) {
          inFlight.add(e.streamId());
        }
      }

      // Candidates: every eligible PENDING entry with no stream, plus — per non-null stream —
      // its single lowest-seq HEAD. The head is the lowest-seq PENDING row, or (strict mode only)
      // the lowest-seq row among PENDING and FAILED: a FAILED head is selected so that it can
      // refuse the whole stream below. A head is claimable only when it is PENDING, eligible,
      // and nothing of its stream is in flight — so a backing-off head and (strict) a FAILED
      // head both hold the whole stream.
      List<OutboxEntry> candidates = new ArrayList<>();
      Map<StreamId, OutboxEntry> heads = new HashMap<>();
      for (OutboxEntry e : store.values()) {
        boolean headCandidate =
            e.status() == OutboxStatus.PENDING
                || (orderingMode == OutboxOrderingMode.STRICT_PER_AGGREGATE
                    && e.status() == OutboxStatus.FAILED);
        if (!headCandidate) {
          continue;
        }
        if (e.streamId() == null) {
          if (e.status() == OutboxStatus.PENDING && isEligible(e, now)) {
            candidates.add(e);
          }
        } else {
          heads.merge(e.streamId(), e, (a, b) -> seqOf(a) <= seqOf(b) ? a : b);
        }
      }
      for (OutboxEntry head : heads.values()) {
        if (head.status() == OutboxStatus.PENDING
            && !inFlight.contains(head.streamId())
            && isEligible(head, now)) {
          candidates.add(head);
        }
      }

      candidates.sort(Comparator.comparingLong(this::seqOf));
      List<OutboxEntry> result = new ArrayList<>();
      for (OutboxEntry e : candidates.subList(0, Math.min(limit, candidates.size()))) {
        OutboxEntry claimed = withStatus(e, OutboxStatus.IN_PROGRESS);
        store.put(e.id(), claimed);
        claimedAt.put(e.id(), now);
        claimedByOf.put(e.id(), claimedBy);
        result.add(claimed);
      }
      return result;
    }
  }

  private static boolean isEligible(OutboxEntry e, Instant now) {
    return e.nextRetryAt() == null || !e.nextRetryAt().isAfter(now);
  }

  private long seqOf(OutboxEntry e) {
    return seqs.get(e.id());
  }

  /**
   * Abandoned claims (a relay that crashed mid-delivery) return to PENDING once the lease lapses.
   */
  private void reclaimExpired(Instant now) {
    Instant cutoff = now.minus(claimLease);
    claimedAt.forEach(
        (id, since) -> {
          if (since.isBefore(cutoff)) {
            store.computeIfPresent(
                id,
                (k, e) ->
                    e.status() == OutboxStatus.IN_PROGRESS
                        ? withStatus(e, OutboxStatus.PENDING)
                        : e);
            claimedAt.remove(id);
            claimedByOf.remove(id);
          }
        });
  }

  @Override
  public String claimedBy() {
    return claimedBy;
  }

  /**
   * The configured claim lease — the same value {@link #loadPending} reclaims expired {@code
   * IN_PROGRESS} claims after. Overridden (parity with {@code PostgresOutboxStore}) so the {@code
   * OutboxPoller} bounds its per-batch publish deadline to this store's actual configured lease,
   * not the inherited SPI default.
   */
  @Override
  public Duration claimLease() {
    return claimLease;
  }

  /**
   * Returns true iff {@code id} is currently {@code IN_PROGRESS} and its claim is held by {@code
   * claimedBy}. Mirrors the Postgres {@code AND status = 'IN_PROGRESS' AND claimed_by = ?} guard: a
   * mark from a relay that did not win (or has lost) the claim is a no-op. Caller holds {@link
   * #lock}.
   */
  private boolean heldBy(OutboxEntryId id, String claimedBy) {
    OutboxEntry e = store.get(id);
    return e != null
        && e.status() == OutboxStatus.IN_PROGRESS
        && Objects.equals(claimedByOf.get(id), claimedBy);
  }

  @Override
  public boolean markDelivered(OutboxEntryId id, String claimedBy) {
    Objects.requireNonNull(id, "id is required");
    synchronized (lock) {
      if (!heldBy(id, claimedBy)) {
        return false; // lease lost — benign no-op
      }
      claimedAt.remove(id);
      claimedByOf.remove(id);
      store.computeIfPresent(
          id,
          (k, e) ->
              new OutboxEntry(
                  e.id(),
                  e.payload(),
                  e.payloadType(),
                  e.streamId(),
                  OutboxStatus.DELIVERED,
                  e.attempts() + 1,
                  null,
                  e.createdAt(),
                  clock.instant(),
                  null,
                  null));
      return true;
    }
  }

  @Override
  public boolean markFailed(OutboxEntryId id, int attempts, String error, String claimedBy) {
    Objects.requireNonNull(id, "id is required");
    synchronized (lock) {
      if (!heldBy(id, claimedBy)) {
        return false; // lease lost — benign no-op
      }
      claimedAt.remove(id);
      claimedByOf.remove(id);
      store.computeIfPresent(
          id,
          (k, e) ->
              new OutboxEntry(
                  e.id(),
                  e.payload(),
                  e.payloadType(),
                  e.streamId(),
                  OutboxStatus.FAILED,
                  attempts,
                  error,
                  e.createdAt(),
                  clock.instant(),
                  null,
                  null));
      return true;
    }
  }

  @Override
  public boolean markRetry(
      OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy) {
    Objects.requireNonNull(id, "id is required");
    Objects.requireNonNull(backoff, "backoff is required");
    // markRetry releases the claim: the entry returns to PENDING and becomes claimable again once
    // next_retry_at passes. The backoff is stamped from THIS store's clock (next_retry_at =
    // clock.now() + backoff), mirroring the Postgres store's DB-clock NOW() + interval — so the
    // same clock that loadPending gates on governs the backoff, immune to a relay clock skew.
    // The poller supplies a Duration, never a wall-clock instant.
    synchronized (lock) {
      if (!heldBy(id, claimedBy)) {
        return false; // lease lost — benign no-op
      }
      Instant nextRetryAt = clock.instant().plus(backoff);
      claimedAt.remove(id);
      claimedByOf.remove(id);
      store.computeIfPresent(
          id,
          (k, e) ->
              new OutboxEntry(
                  e.id(),
                  e.payload(),
                  e.payloadType(),
                  e.streamId(),
                  OutboxStatus.PENDING,
                  attempts,
                  error,
                  e.createdAt(),
                  e.processedAt(),
                  nextRetryAt,
                  null));
      return true;
    }
  }

  /**
   * Returns never-attempted entries to {@code PENDING}, touching only the claim columns. Overridden
   * rather than inherited so this double keeps <b>exact</b> parity with {@code
   * PostgresOutboxStore.releaseClaims}: the SPI default expresses the release through {@link
   * #markRetry}, which would also re-stamp {@code nextRetryAt} to "now" — harmless (the entry was
   * claimable a moment ago, so its old value is already past) but observably different, and a test
   * that pinned {@code nextRetryAt} would then pass here and fail against the real store.
   *
   * <p>Each release is the same {@code IN_PROGRESS} + {@code claimedBy} compare-and-set as every
   * {@code mark*}: an entry whose lease was lost is skipped, not stolen back.
   */
  @Override
  public int releaseClaims(java.util.Collection<OutboxEntry> entries, String claimedBy) {
    Objects.requireNonNull(entries, "entries is required");
    int released = 0;
    synchronized (lock) {
      for (OutboxEntry entry : entries) {
        OutboxEntryId id = entry.id();
        if (!heldBy(id, claimedBy)) {
          continue; // lease lost — benign no-op
        }
        claimedAt.remove(id);
        claimedByOf.remove(id);
        store.computeIfPresent(
            id,
            (k, e) ->
                new OutboxEntry(
                    e.id(),
                    e.payload(),
                    e.payloadType(),
                    e.streamId(),
                    OutboxStatus.PENDING,
                    e.attempts(),
                    e.lastError(),
                    e.createdAt(),
                    e.processedAt(),
                    e.nextRetryAt(),
                    null));
        released++;
      }
    }
    return released;
  }

  /**
   * Removes a non-{@code FAILED} entry; an absent id is a no-op. A {@code FAILED} entry is refused
   * with {@link IllegalStateException} — its only exits are the audited {@link #skipFailed} and
   * {@link #resetFailedToPending}, and a raw delete of a blocking head would release its stream
   * with no record.
   */
  @Override
  public void delete(OutboxEntryId id) {
    Objects.requireNonNull(id, "id is required");
    synchronized (lock) {
      OutboxEntry e = store.get(id);
      if (e != null && e.status() == OutboxStatus.FAILED) {
        throw new IllegalStateException(
            "outbox entry "
                + LogSanitizer.sanitizeForLog(id.value())
                + " is FAILED; resolve it with OutboxFailedReplayer.skip or .replay — delete would"
                + " release its stream with no record");
      }
      claimedAt.remove(id);
      claimedByOf.remove(id);
      seqs.remove(id);
      store.remove(id);
    }
  }

  /**
   * Returns up to {@code limit} entries in {@code status}, oldest ({@code seq}) first. Read-only:
   * does not claim entries. Mirrors the Postgres {@code ORDER BY seq ASC LIMIT ?} enumeration.
   */
  @Override
  public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
    Objects.requireNonNull(status, "status is required");
    if (limit < 1) throw new IllegalArgumentException("limit must be >= 1");
    synchronized (lock) {
      List<OutboxEntry> matching = new ArrayList<>();
      for (OutboxEntry e : store.values()) {
        if (e.status() == status) {
          matching.add(e);
        }
      }
      matching.sort(Comparator.comparingLong(this::seqOf));
      return new ArrayList<>(matching.subList(0, Math.min(limit, matching.size())));
    }
  }

  /**
   * Deletes {@code DELIVERED} entries whose {@code processedAt} is strictly before {@code
   * olderThan}. Mirrors the Postgres {@code deleteDelivered} retention delete. {@code
   * OutboxRetentionSweeper} calls {@code deleteDelivered} — without this override the default
   * {@link OutboxStore#deleteDelivered} throws {@code UnsupportedOperationException}, so a unit
   * test on this double could not exercise the delivered-retention path the real store supports.
   */
  @Override
  public int deleteDelivered(Instant olderThan) {
    Objects.requireNonNull(olderThan, "olderThan is required");
    synchronized (lock) {
      List<OutboxEntryId> toDelete = new ArrayList<>();
      for (OutboxEntry e : store.values()) {
        if (e.status() == OutboxStatus.DELIVERED
            && e.processedAt() != null
            && e.processedAt().isBefore(olderThan)) {
          toDelete.add(e.id());
        }
      }
      for (OutboxEntryId id : toDelete) {
        seqs.remove(id);
        store.remove(id);
      }
      return toDelete.size();
    }
  }

  /**
   * Resets a {@code FAILED} entry back to {@code PENDING}, clearing attempts/error/backoff. Unlike
   * the {@code mark*} CAS this is unconditional on status ({@code FAILED} rows carry no live
   * claim), mirroring the Postgres {@code WHERE entry_id = ? AND status = 'FAILED'} operator reset.
   */
  @Override
  public boolean resetFailedToPending(OutboxEntryId id) {
    Objects.requireNonNull(id, "id is required");
    synchronized (lock) {
      OutboxEntry e = store.get(id);
      if (e == null || e.status() != OutboxStatus.FAILED) {
        return false;
      }
      claimedAt.remove(id);
      claimedByOf.remove(id);
      store.put(
          id,
          new OutboxEntry(
              e.id(),
              e.payload(),
              e.payloadType(),
              e.streamId(),
              OutboxStatus.PENDING,
              0,
              null,
              e.createdAt(),
              e.processedAt(),
              null,
              null));
      return true;
    }
  }

  @Override
  public Optional<OutboxEntry> findById(OutboxEntryId id) {
    Objects.requireNonNull(id, "id is required");
    return Optional.ofNullable(store.get(id));
  }

  /** FAILED → SKIPPED with the audit record stamped from this store's clock, in one step. */
  @Override
  public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
    Objects.requireNonNull(id, "id is required");
    Objects.requireNonNull(skippedBy, "skippedBy is required");
    Objects.requireNonNull(reason, "reason is required");
    synchronized (lock) {
      OutboxEntry e = store.get(id);
      if (e == null || e.status() != OutboxStatus.FAILED) {
        return false;
      }
      store.put(
          id,
          new OutboxEntry(
              e.id(),
              e.payload(),
              e.payloadType(),
              e.streamId(),
              OutboxStatus.SKIPPED,
              e.attempts(),
              e.lastError(),
              e.createdAt(),
              e.processedAt(),
              null,
              new OutboxEntry.SkipRecord(clock.instant(), skippedBy, reason)));
      return true;
    }
  }

  /**
   * Distinct non-null streams with a {@code FAILED} entry, the oldest {@code FAILED} {@code
   * processedAt} of ANY stream, and the count of {@code FAILED} entries with no stream — one pass
   * under the lock, mirroring the Postgres single-statement sample.
   */
  @Override
  public BlockageSample sampleBlockage() {
    synchronized (lock) {
      Set<StreamId> streams = new HashSet<>();
      Instant oldest = null;
      long nullAggregateFailed = 0;
      for (OutboxEntry e : store.values()) {
        if (e.status() != OutboxStatus.FAILED) {
          continue;
        }
        if (e.streamId() == null) {
          nullAggregateFailed++;
        } else {
          streams.add(e.streamId());
        }
        if (e.processedAt() != null && (oldest == null || e.processedAt().isBefore(oldest))) {
          oldest = e.processedAt();
        }
      }
      return new BlockageSample(streams.size(), oldest, nullAggregateFailed);
    }
  }

  /**
   * Deletes {@code SKIPPED} entries whose {@code skippedAt} is strictly before {@code cutoff}.
   * {@code FAILED} is never a candidate.
   */
  @Override
  public int deleteSkipped(Instant cutoff) {
    Objects.requireNonNull(cutoff, "cutoff is required");
    synchronized (lock) {
      List<OutboxEntryId> toDelete = new ArrayList<>();
      for (OutboxEntry e : store.values()) {
        if (e.status() == OutboxStatus.SKIPPED && e.skip().skippedAt().isBefore(cutoff)) {
          toDelete.add(e.id());
        }
      }
      for (OutboxEntryId id : toDelete) {
        seqs.remove(id);
        store.remove(id);
      }
      return toDelete.size();
    }
  }

  /**
   * Counts entries currently in {@code status}. Read-only; does not claim entries. Mirrors the
   * Postgres {@code SELECT count(*) ... WHERE status = ?} used for the outbox backlog gauge.
   */
  @Override
  public long countByStatus(OutboxStatus status) {
    Objects.requireNonNull(status, "status is required");
    synchronized (lock) {
      return store.values().stream().filter(e -> e.status() == status).count();
    }
  }

  /** Returns all entries regardless of status. Use in test assertions. */
  public List<OutboxEntry> all() {
    return List.copyOf(store.values());
  }

  /** Returns a copy of the entry with only its status changed; all other fields are preserved. */
  private static OutboxEntry withStatus(OutboxEntry e, OutboxStatus status) {
    return new OutboxEntry(
        e.id(),
        e.payload(),
        e.payloadType(),
        e.streamId(),
        status,
        e.attempts(),
        e.lastError(),
        e.createdAt(),
        e.processedAt(),
        e.nextRetryAt(),
        null);
  }
}
