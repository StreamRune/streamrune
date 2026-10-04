package org.streamrune.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.LogSanitizer;

/**
 * Background poller that delivers {@link OutboxEntry outbox entries} to an {@link OutboxPublisher}.
 *
 * <p>Runs on a virtual thread ({@link Thread#ofVirtual()}). Call {@link #start()} to begin polling
 * and {@link #close()} to stop the thread.
 *
 * <p><b>Retry semantics:</b> On delivery failure, the entry's {@code attempts}, {@code last_error},
 * and {@code next_retry_at} are persisted via {@link OutboxStore#markRetry}. The backoff delay is
 * computed by the configured {@link RetryPolicy} (exponential backoff with optional jitter).
 * Entries whose {@code next_retry_at} is in the future are skipped by {@link
 * OutboxStore#loadPending}. When {@code attempts} reaches {@link RetryPolicy#maxAttempts()}, the
 * entry is marked {@code FAILED} (terminal).
 *
 * <p>The <b>terminal ladder</b> and the <b>retry cadence</b> are two separate mechanisms. A {@link
 * OutboxPublisher.FailureKind#TRANSPORT TRANSPORT} failure never advances the persisted ladder (an
 * outage must not terminal-{@code FAILED} the backlog), yet its retry cadence still escalates while
 * the failures last: the backoff is computed from the entry's attempts <i>plus</i> the stronger of
 * two relay-local streaks — the {@linkplain #transportStreak relay-wide
 * consecutive-all-transport-cycle streak} (a broker-wide outage) and the entry's own {@linkplain
 * #entryTransportStreaks consecutive-transport-failure rung} (a poisoned entry amid healthy
 * traffic) — clamped to {@link #MAX_RETRY_BACKOFF}.
 *
 * <p><b>Resilience:</b> a failure of the whole poll cycle (e.g. the outbox store being temporarily
 * unreachable) is logged and retried with capped exponential backoff — it never silently kills the
 * relay thread. Per-entry delivery failures are handled by the retry semantics above.
 *
 * <p><b>Stopping:</b> {@link #close()} interrupts the poll thread to cut an idle sleep or a publish
 * short, but never the store writes that record a batch's outcome: those run with the interrupt
 * held back (on a virtual thread an interrupted JDBC call fails at the socket), so deliveries the
 * transport confirmed before the stop are marked delivered, an entry whose publish was cut short is
 * held as in flight until its claim lease expires, and the entries the batch never reached are
 * released at once.
 *
 * <p><b>Testing:</b> {@link #processBatch()} is package-private to allow tests to invoke one
 * delivery cycle synchronously without starting a background thread.
 */
public final class OutboxPoller implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(OutboxPoller.class);

  /**
   * Default outbox retry policy: 10 attempts, 1s initial delay, 2x backoff, jitter — raised from 5
   * (~15-25s) so the per-ENTRY ladder survives a realistic broker restart even for a publisher
   * whose {@code classifyFailure} override bins some transport-ish failures as counting {@code
   * ENTRY}. (A publisher that does not override {@code classifyFailure} at all is non-counting for
   * every failure — the SPI default is {@code TRANSPORT} — so this ladder then governs nothing
   * until a real classification is supplied.) With the {@link #MAX_RETRY_BACKOFF 60s backoff cap}
   * the ladder spans several minutes before an entry is marked terminal {@code FAILED}. TRANSPORT
   * failures are additionally non-counting (see {@link #recordFailure}), so an outage never
   * terminal-fails the backlog regardless of this ladder — and every transport-failing entry is
   * retried on an <b>escalating</b> cadence (1s, 2s, 4s … 60s by these defaults, driven by {@link
   * #transportStreak} for a broker-wide outage and by {@link #entryTransportStreaks} for a
   * persistently failing single entry), not repeatedly at {@code initialDelay}.
   */
  public static final RetryPolicy DEFAULT_RETRY_POLICY =
      new RetryPolicy(10, Duration.ofSeconds(1), 2.0, true);

  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  /**
   * Cap on the per-entry retry backoff. {@link RetryPolicy#delayForAttempt} grows without an
   * explicit bound; clamping it here keeps a long outage from stamping {@code next_retry_at}
   * arbitrarily far out (which would delay recovery for many minutes after the broker returns) and
   * bounds the retry cadence of a non-counting TRANSPORT retry that never terminal-fails.
   *
   * <p>It is a ceiling, not a floor: the escalation that actually walks a sustained transport
   * failure <em>up to</em> this ceiling is the pair of transport streaks — the relay-wide {@link
   * #transportStreak} and the per-entry rungs in {@link #entryTransportStreaks}. A saturated outage
   * therefore costs at most this much extra recovery latency once the broker returns — the
   * deliberate trade-off for not hammering a down broker (and the reason the cap is a minute, not
   * an hour).
   */
  private static final Duration MAX_RETRY_BACKOFF = Duration.ofSeconds(60);

  /**
   * Clamp on both transport streaks — the relay-wide {@link #transportStreak} and each per-entry
   * rung in {@link #entryTransportStreaks}. Escalation only has to run until the backoff saturates
   * {@link #MAX_RETRY_BACKOFF}; 32 rungs overshoot that for every realistic initial
   * delay/multiplier, and clamping keeps a multi-day outage from overflowing the counter or feeding
   * {@link RetryPolicy#delayForAttempt} a pathological exponent.
   */
  private static final int MAX_TRANSPORT_STREAK = 32;

  /**
   * Size cap on {@link #entryTransportStreaks}. The memory bound it buys: at most this many map
   * nodes of (entry-id string, boxed streak) — on the order of ~150 bytes each for typical ids, so
   * worst case roughly 150 KB of relay-local soft state, regardless of how many distinct entries
   * fail transport over the relay's lifetime. Eviction is least-recently-FAILED (access-order LRU):
   * under a mass-transport scenario wide enough to overflow the cap, the evicted entries are the
   * ones not currently failing — and a genuinely failing entry that is evicted anyway merely
   * restarts its escalation from the {@link #transportStreak relay-wide streak} floor, which in
   * exactly that broker-wide scenario is itself escalated. Sized above any realistic poisoned-entry
   * population (poisoned entries are per-message defects, not fleets) while staying far below the
   * backlog sizes a broker-wide outage produces.
   */
  private static final int MAX_TRACKED_TRANSPORT_ENTRIES = 1024;

  /**
   * Upper bound, as a fraction of the store's claim lease, on the budget within which one publish
   * cycle must finish. The poller stops publishing once the deadline passes and leaves the
   * remaining claimed entries for the lease-reclaim path — so a live-but-slow relay can never keep
   * delivering entries past the point where a second relay reclaims and re-delivers them (a
   * duplicate + out-of-order same-aggregate delivery the ordering contract forbids). Strictly
   * {@code < 1} leaves margin for relay/store clock skew before the lease actually expires.
   *
   * <p>This fraction alone bounds only the common/zero-horizon case. Where a wired publisher may
   * leave work in flight past its own {@code publish} call returning, {@link #publishDeadlineFor}
   * further caps the budget at {@code lease - maxInFlightHorizon}, so the actual per-cycle deadline
   * is {@code min} of this fraction and that cap — never this fraction alone.
   */
  private static final double PUBLISH_DEADLINE_LEASE_FRACTION = 0.8;

  /**
   * Head-room added to a publisher's {@link OutboxPublisher#inFlightHorizon() in-flight horizon}
   * when the in-flight wiring guard reports the minimum safe claim lease. The hard guard only
   * requires {@code lease > horizon}; this margin is added to the horizon in the recommended-lease
   * hint so that a lease configured at the recommendation still clears the transport's horizon
   * after allowing for relay-vs-store clock skew (the horizon is measured on the relay/producer
   * clock, the reclaim cutoff on the store clock) and the scheduling latency between a claim being
   * stamped and its first {@link OutboxPublisher#publish}. It is deliberately NOT sized to cover
   * multi-entry batch accumulation — that head-room is now provided <em>structurally</em> by the
   * publish deadline, which {@link #publishDeadlineFor} caps at {@code lease - maxInFlightHorizon}:
   * any entry a batch is allowed to START before the deadline still has at least the publisher's
   * in-flight horizon of lease remaining, enough for its worst-case in-flight duration to resolve
   * before the lease could expire and a reclaim fire. Sizing {@code batchSize} so a batch confirms
   * well inside the deadline keeps throughput up (see docs/guide/advanced/outbox.md "Sizing the
   * claim lease").
   */
  private static final Duration CLAIM_LEASE_INFLIGHT_MARGIN = Duration.ofSeconds(30);

  /**
   * Upper bound {@link #close()} waits to join the poll thread before proceeding. The interrupted
   * thread exits promptly in practice; this only caps a pathologically stuck poll.
   */
  private static final long CLOSE_JOIN_TIMEOUT_MS = 30_000L;

  /** The outcome of a batch the relay stopped before handing anything off. */
  private static final OutboxPublisher.BatchResult NOTHING_ATTEMPTED =
      new OutboxPublisher.BatchResult(Set.of(), Map.of());

  private final long closeJoinTimeoutMs;
  private final OutboxStore store;
  private final OutboxPublisher publisher;
  // The maximum in-flight horizon across this poller's wired publisher(s), retained
  // from construction so publishDeadlineFor can cap the per-batch publish deadline against the SAME
  // horizon the build() guard enforces (lease must strictly exceed it), rather than a second,
  // independently-computed number. Normalized: a null/negative publisher horizon becomes ZERO.
  private final Duration maxInFlightHorizon;
  private final RetryPolicy retryPolicy;
  private final int batchSize;
  private final Duration pollInterval;
  private final StreamRuneMetrics metrics;
  private final ResilientPollLoop pollLoop;
  // The identity this relay claims and marks entries under. Read once from the store; every mark*
  // is guarded on claimed_by = this value, so a stolen lease cannot clobber another relay's
  // outcome.
  private final String claimedBy;
  // The running poll thread and the interrupt deferral it shares with close(); null when stopped.
  private volatile PollThread pollThread;
  private final AtomicBoolean started = new AtomicBoolean(false);
  // Set false the first time countByStatus throws UnsupportedOperationException (a custom store
  // that does not support counting), so the backlog gauge degrades gracefully without retrying
  // every cycle. Delivery is never affected.
  private volatile boolean backlogSamplingSupported = true;

  /** The mode the store enforces; read once at build() (fails loudly if the store has none). */
  private final OutboxOrderingMode orderingMode;

  /**
   * The store's claim lease, read once at build() so the lease the wiring guard validated is the
   * one every publish deadline is bounded by. Never {@code null}; a non-positive value is the
   * store's explicit opt-out of deadline-bounding.
   */
  private final Duration claimLease;

  // Set false the first time sampleBlockage throws UnsupportedOperationException. Unlike the
  // backlog degrade (DEBUG), this one is logged ONCE at WARN: blockage_age_seconds is the primary
  // alert series, so a store that cannot feed it must be visible.
  private volatile boolean blockageSamplingSupported = true;

  // Armed after the first SUCCESSFUL sampleBlockage read on a strict channel; a read that
  // throws leaves it unarmed so the next cycle retries ("once", not "once attempted").
  private volatile boolean legacyNullAggregateChecked;

  /**
   * Number of consecutive poll cycles whose publishes ALL failed with a broker-wide TRANSPORT
   * failure — the <b>relay-wide half</b> of the escalation input for the transport retry
   * <b>cadence</b> (the per-entry half is {@link #entryTransportStreaks}), deliberately separate
   * from the persisted per-entry ladder counter that is frozen for transport failures. Without it
   * the two guards shared one variable and the frozen counter pinned the backoff at {@code
   * initialDelay} for the entire outage (re-claiming and re-failing the whole backlog every poll
   * interval: log storm, sustained DB write load, lock-step reconnects).
   *
   * <p>Soft, <b>relay-local</b> state on purpose — no schema column: the streak describes this
   * relay's current view of the transport, and losing it on restart merely costs one fast retry
   * cycle before it re-escalates. Maintained in {@link #processBatch} (see there for the exact
   * evidence rules) and read by {@link #recordFailure}. An {@link AtomicInteger} rather than a
   * {@code volatile int}: normally only the single thread running a cycle mutates it, but when
   * {@link #close()} is invoked from within the poll thread itself (see {@link
   * #entryTransportStreaks}) a new poll thread can start its own cycle while the closing one is
   * still unwinding, and a plain read-modify-write would then lose the other thread's update (an
   * old {@code +1} overwriting a new reset). The atomic keeps each reset and each increment whole;
   * which of two racing threads lands last can still leave the streak one rung off, which this soft
   * pacing state tolerates.
   */
  private final AtomicInteger transportStreak = new AtomicInteger();

  /**
   * per-ENTRY consecutive-transport-failure streaks, the second (and per-entry) half of the cadence
   * escalation. The relay-wide {@link #transportStreak} resets whenever ANY delivery confirms in a
   * cycle, and the persisted attempt counter stays frozen for TRANSPORT — so with the relay-wide
   * streak alone, a single entry that keeps failing TRANSPORT amid healthy confirming traffic
   * computed {@code backoffAttempt = 1} every time and re-published at {@code initialDelay} (~1s)
   * FOREVER. Under the SPI's default-TRANSPORT classification that was the fate of every
   * unclassified custom-publisher failure. This map gives each entry its own streak: {@link
   * #recordFailure} reads the entry's rung for the backoff input (the STRONGER of the two streaks
   * wins) and advances it one rung per transport failure of that entry, clamped to {@link
   * #MAX_TRANSPORT_STREAK}; a confirmed delivery clears the confirmed entry's rung (see {@link
   * #processBatch}) and a terminal {@code FAILED} drops the entry's rung with the entry.
   *
   * <p>Bounded, access-order LRU — {@link #MAX_TRACKED_TRANSPORT_ENTRIES} caps it and documents the
   * memory bound. Like the relay-wide streak it is soft, <b>relay-local</b> state on purpose — no
   * schema column: losing a rung (restart, eviction) merely costs one fast retry cycle before the
   * entry re-escalates. Normally mutated only by the single thread running a cycle, with the
   * happens-before chain join-then-{@code started.set(false)} / {@code started.compareAndSet} in
   * {@link #start()} ordering one cycle's mutations before the next poll thread's — <b>except</b>
   * when {@link #close()} is invoked FROM WITHIN the poll thread itself (its self-join guard: a
   * thread cannot join itself), in which case {@code started} is reset immediately with no join, so
   * a new poll thread from a subsequent {@code start()} can begin its own cycle while the closing
   * thread is still unwinding the tail of its current one. That path has no happens-before edge
   * between the two threads' mutations, so the map is wrapped in {@link
   * Collections#synchronizedMap} to keep the underlying {@link LinkedHashMap} (including its
   * access-order relinking and {@code removeEldestEntry} eviction) from corrupting under genuinely
   * concurrent access; individual per-entry read-then-write sequences (e.g. in {@link
   * #recordFailure}) are still only atomic within one synchronized call, which is sufficient here
   * since the self-close race is rare and merely risks one entry's rung being off by a step, never
   * map corruption.
   */
  private final Map<OutboxEntryId, Integer> entryTransportStreaks =
      Collections.synchronizedMap(
          new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<OutboxEntryId, Integer> eldest) {
              return size() > MAX_TRACKED_TRANSPORT_ENTRIES;
            }
          });

  /**
   * Number of entries currently tracked in {@link #entryTransportStreaks}. Package-private test
   * seam pinning the memory bound and the confirm-clears-streak rule; production code never reads
   * it.
   */
  int trackedTransportStreakEntries() {
    return entryTransportStreaks.size();
  }

  /**
   * Whether the zero-horizon contract violation — an {@code IN_FLIGHT} classification from a
   * publisher whose {@link OutboxPublisher#inFlightHorizon()} is still the {@code ZERO} default —
   * has already been logged at ERROR by this relay. The violation is a property of the WIRING, not
   * of one entry, so the full explanation is logged once per relay: a post-hand-off outage
   * classifies the whole claimed batch {@code IN_FLIGHT} every cycle, and one ERROR per entry per
   * cycle would be the exact log storm that was removed. The {@code
   * streamrune.outbox.in_flight_horizon_violation} metric still fires per held entry, so the RATE
   * stays observable after the one-time log. {@code volatile} because a {@code close()}/{@code
   * start()} pair hands the poll body to a different thread. Normally mutated only by the thread
   * running a cycle; on the self-close path two threads may both set it, which is harmless for a
   * flag that is only ever set to {@code true}.
   */
  private volatile boolean zeroHorizonViolationLogged = false;

  private OutboxPoller(Builder builder) {
    this.closeJoinTimeoutMs = builder.closeJoinTimeoutMs;
    this.store = builder.store;
    this.publisher = builder.publisher;
    this.maxInFlightHorizon = normalizeHorizon(builder.publisher.inFlightHorizon());
    this.retryPolicy = builder.retryPolicy;
    this.batchSize = builder.batchSize;
    this.pollInterval = builder.pollInterval;
    this.metrics = builder.metrics;
    this.orderingMode = builder.orderingMode;
    this.claimLease = builder.claimLease;
    this.claimedBy = builder.store.claimedBy();
    Duration maxBackoff =
        pollInterval.compareTo(MAX_FAILURE_BACKOFF) > 0 ? pollInterval : MAX_FAILURE_BACKOFF;
    this.pollLoop =
        new ResilientPollLoop(
            "outbox-poller",
            () -> !Thread.currentThread().isInterrupted(),
            () -> pollInterval,
            pollInterval,
            maxBackoff);
  }

  /**
   * Starts the background virtual thread. start()/close() pairs are repeatable; may be restarted
   * after close().
   *
   * @throws IllegalStateException if already started
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("OutboxPoller already started — call start() exactly once");
    }
    LOG.info(
        "Outbox relay started: orderingMode={}, batchSize={}, pollInterval={}, claimLease={}",
        orderingMode,
        batchSize,
        pollInterval,
        claimLease);
    var interrupts = new InterruptDeferral();
    Thread t = Thread.ofVirtual().name("outbox-poller").unstarted(() -> loop(interrupts));
    pollThread = new PollThread(t, interrupts);
    t.start();
  }

  private void loop(InterruptDeferral interrupts) {
    // ResilientPollLoop logs cycle failures (e.g. EventStoreException from loadPending) and
    // retries with capped exponential backoff. The relay only stops on close()/interrupt.
    pollLoop.run(() -> processBatch(interrupts));
  }

  /** A started poll thread and the interrupt deferral {@link #close()} stops it through. */
  private record PollThread(Thread thread, InterruptDeferral interrupts) {}

  /**
   * Number of consecutive poll-cycle failures; {@code 0} when the last cycle succeeded. A non-zero
   * value means the relay is degraded — still running, retrying with backoff.
   */
  public int consecutiveFailures() {
    return pollLoop.consecutiveFailures();
  }

  /**
   * Processes one batch of pending entries synchronously. Package-private for testing; production
   * code uses {@link #start()}.
   */
  void processBatch() {
    processBatch(new InterruptDeferral());
  }

  private void processBatch(InterruptDeferral interrupts) {
    // The first-cycle legacy-data check runs before the gauges and independently
    // of them (a WARN must not depend on metrics wiring); it never aborts the cycle either.
    checkLegacyNullAggregateFailedOnce();
    // Sample the observability gauges at the top of the cycle, before loadPending claims anything,
    // so the backlog reflects the true PENDING count (undelivered events awaiting a broker) even on
    // an idle cycle. Decoupled from delivery: a backlog-count failure never aborts the cycle.
    sampleObservability();
    // Anchor the publish deadline BEFORE loadPending claims anything. loadPending stamps
    // claimed_at (the DB clock) as it runs; capturing the base here — rather than after it returns
    // — charges the claim-query latency (RECLAIM_EXPIRED + claim CTE on a large backlog, or a GC
    // pause) AGAINST the publish budget instead of silently extending the window past claimed_at +
    // fraction*lease and eroding the margin below the lease-reclaim horizon.
    Instant claimBase = Instant.now();
    Duration lease = claimLease;
    List<OutboxEntry> pending = store.loadPending(batchSize);
    if (pending.isEmpty()) {
      return;
    }
    // Bound this publish cycle to a deadline strictly below the claim lease. The publisher
    // stops attempting entries once the deadline passes; the never-attempted ones are released at
    // the end of the cycle — never left for a second relay racing this one past the lease.
    Instant publishDeadline = publishDeadlineFor(claimBase, lease);
    // A stop that arrived while the batch was being claimed: hand nothing off, release it all.
    var result =
        Thread.currentThread().isInterrupted()
            ? NOTHING_ATTEMPTED
            : publisher.publishBatch(pending, publishDeadline);
    // Record the outcome with close()'s interrupt held back: on a virtual thread every JDBC call
    // made with the flag set fails at the socket, which would lose the deliveries the transport
    // has already confirmed. A stop requested meanwhile is delivered once the outcome is stored.
    boolean stopping = interrupts.defer();
    try {
      recordOutcome(pending, result, claimBase, publishDeadline, lease, stopping);
    } finally {
      interrupts.resume();
    }
  }

  /**
   * Stores the outcome of one publish cycle: confirmed entries are marked delivered, failures go
   * through {@link #recordFailure}, and entries the cycle never attempted are released. Runs with
   * {@link #close()}'s interrupt deferred.
   *
   * @param stopping whether a stop was requested during the cycle (changes only the log wording for
   *     the released entries)
   */
  private void recordOutcome(
      List<OutboxEntry> pending,
      OutboxPublisher.BatchResult result,
      Instant claimBase,
      Instant publishDeadline,
      Duration lease,
      boolean stopping) {
    // FIX 4: pass an immutable snapshot to markDeliveredAll so any late mutation of the live set
    // from the publisher cannot affect the store call.
    if (!result.confirmed().isEmpty()) {
      var confirmed = List.copyOf(result.confirmed());
      int transitioned = store.markDeliveredAll(confirmed, claimedBy);
      if (transitioned < confirmed.size()) {
        // Lost lease for the missing ids: another relay recorded their outcome (expected under
        // failover). This is a benign no-op — log and continue, never throw.
        LOG.warn(
            "Outbox markDeliveredAll transitioned {} of {} confirmed entries; {} lost their lease"
                + " (claimed by another relay) and were skipped",
            transitioned,
            confirmed.size(),
            confirmed.size() - transitioned);
      }
    }
    // Entries the publish cycle never reached, collected so their claims
    // can be released together at the end of the cycle (see the releaseClaims call below).
    var unattemptedEntries = new ArrayList<OutboxEntry>();
    // Per-cycle transport tally: how many entries this cycle deferred on a TRANSPORT
    // failure, the longest backoff stamped for them, and one representative entry + cause
    // — the inputs to the consecutive-transport streak and to the single aggregated WARN
    // below.
    int transportFailures = 0;
    Duration maxTransportBackoff = null;
    String lastTransportError = null;
    OutboxEntryId lastTransportEntryId = null;
    // Count of resolved-but-non-transport, non-in-flight failures this cycle (an ENTRY
    // ladder rejection, a terminal FAILED, or a lost-lease no-op — everything recordFailure bins
    // as FailureOutcome.OTHER). Needed to tell "every RESOLVED entry failed transport" apart from
    // "every claimed entry failed transport" — the latter also counts unattempted
    // (deadline-truncated) entries as if they were transport evidence, which they are not.
    int entryFailures = 0;
    // The same per-cycle aggregation for IN_FLIGHT hand-offs. A stalled
    // backend that accepts the request and then drops the connection produces one IN_FLIGHT per
    // claimed entry, which used to be one WARN per entry per cycle — the exact log flood
    // removed on the transport path.
    int inFlightFailures = 0;
    String lastInFlightError = null;
    // Entries whose publish was cut short by an interrupt: held like IN_FLIGHT, logged apart.
    int interruptedPublishes = 0;
    for (OutboxEntry entry : pending) {
      if (result.confirmed().contains(entry.id())) {
        continue;
      }
      if (!result.failed().containsKey(entry.id())) {
        // The publish cycle hit its deadline (or a stop) before reaching this entry — it was NEVER
        // attempted. It must NOT go through recordFailure: that would burn a retry attempt (and
        // could eventually terminal-FAIL) an entry no broker ever saw. Nor may it simply stay
        // claimed. Every truncation path breaks BEFORE the hand-off (the deadline and interrupt
        // checks OutboxPublisher.publishSequentially makes before each publish;
        // RabbitMqOutboxPublisher's stopPublishing latch, set before basicPublish; the stop check
        // above), so nothing exists at the transport for it and there is nothing a release could
        // duplicate — the exact opposite of the IN_FLIGHT and interrupted cases below, where
        // holding is required. Held, it would keep its aggregate un-claimable for a FULL LEASE with
        // no path for this live relay to release its own claim.
        unattemptedEntries.add(entry);
        continue;
      }
      Exception failure = result.failed().get(entry.id());
      FailureOutcome outcome = recordFailure(entry, failure);
      if (outcome.interrupted()) {
        interruptedPublishes++;
      } else if (outcome.transportBackoff() != null) {
        transportFailures++;
        // A null-error failure is binned TRANSPORT, so transport no longer implies a
        // non-null exception — mirror recordFailure's placeholder message for the aggregation.
        lastTransportError = failure != null ? failure.getMessage() : "not confirmed by broker";
        lastTransportEntryId = entry.id(); // representative entry for the aggregated WARN
        if (maxTransportBackoff == null
            || outcome.transportBackoff().compareTo(maxTransportBackoff) > 0) {
          maxTransportBackoff = outcome.transportBackoff();
        }
      } else if (outcome.inFlight()) {
        inFlightFailures++;
        lastInFlightError = failure.getMessage(); // in-flight implies a non-null exception
      } else {
        // An ENTRY-ladder rejection, a terminal FAILED, or a lost-lease no-op — this entry
        // RESOLVED this cycle but not as transport evidence.
        entryFailures++;
      }
    }
    // Maintain the relay-wide consecutive-TRANSPORT streak that paces transport retries
    // (the per-entry rungs — the other half of the cadence input — are advanced inside
    // recordFailure, one rung per transport failure of that entry, and cleared on confirm below).
    // Strictly evidence-based, and only from cycles that actually resolved a publish: a confirmed
    // delivery PROVES the transport works (reset — the next failure re-probes fast), while a cycle
    // whose resolved publishes all failed broker-wide means the outage is still on (escalate one
    // rung). Every other shape carries no evidence and must leave the streak alone: an idle cycle
    // (returned above — during an outage MOST cycles are idle because the backlog sits inside its
    // backoff window, so resetting here would flatten the escalation back to initialDelay forever,
    // exactly the bug being fixed), a deadline-truncated cycle, and IN_FLIGHT-only or ENTRY-only
    // outcomes (different mechanisms — see recordFailure).
    if (!result.confirmed().isEmpty()) {
      transportStreak.set(0);
      // A confirm also proves the CONFIRMED entries' own delivery paths — clear their
      // per-entry streaks so the bounded map tracks only entries that are still failing. Entries
      // that KEEP failing transport keep their rungs; that is the point of the per-entry streak:
      // a poisoned head's escalation must survive healthy traffic confirming around it.
      for (OutboxEntryId id : result.confirmed()) {
        entryTransportStreaks.remove(id);
      }
    } else if (transportFailures > 0) {
      transportStreak.updateAndGet(streak -> Math.min(streak + 1, MAX_TRANSPORT_STREAK));
    }
    if (transportFailures > 0) {
      // ONE aggregated WARN per cycle instead of one identical line per entry: a
      // 100-entry backlog against a down broker used to emit 100 WARNs every poll interval. Names
      // the outage streak and the longest deferral stamped this cycle, so the escalating cadence
      // (and the fact that it is capped) is visible to an operator watching the logs.
      // With the SPI's default-TRANSPORT classification this WARN may describe ONE
      // poisoned entry rather than a broker-wide outage. key "broker-wide" on RESOLVED
      // outcomes only, mirroring the relay-streak's own evidence rule above (:402-411) — an
      // unattempted (deadline-truncated) entry was never handed to the transport and an IN_FLIGHT
      // hand-off resolved differently, so neither is evidence either way. Counting them in the
      // denominator (transportFailures == pending.size()) undercounted "broker-wide" during a real
      // blocking-connect outage that also truncated the batch, sending the WARN's wording the
      // WRONG way — "NOT broker-wide … possibly a single poisoned entry" while the broker was in
      // fact fully down. even restricted to resolved outcomes, the predicate is vacuously
      // true with exactly one resolved entry (a lone poisoned entry then logs "broker-wide … 1 of
      // 1" every cycle) — hedge it off in that case, and also when the representative entry's own
      // per-entry streak has already outpaced the relay-wide streak (proof that some earlier cycle
      // confirmed other traffic while this entry kept failing: the signature of a single poisoned
      // entry escalating on its own cadence, not a broker-wide outage). Always name a
      // representative entry so the operator can find a poisoned head. The entry id can carry
      // user-derived content (aggregate-seeded ids), so it passes through the LogSanitizer sink
      // guard like every identifier the framework renders into a log record.
      int resolvedEntries = pending.size() - unattemptedEntries.size();
      boolean everyResolvedEntryFailedTransport =
          result.confirmed().isEmpty()
              && inFlightFailures == 0
              && entryFailures == 0
              && interruptedPublishes == 0;
      // Non-null here: set from entry.id() (never null) in the same branch that counts
      // transportFailures.
      int representativeEntryStreak = entryTransportStreaks.getOrDefault(lastTransportEntryId, 0);
      int relayStreak = transportStreak.get();
      boolean brokerWide =
          everyResolvedEntryFailedTransport
              && resolvedEntries > 1
              && representativeEntryStreak <= relayStreak;
      LOG.warn(
          "Outbox delivery hit {} for {} of {} claimed entries (e.g. entry {}; consecutive"
              + " all-transport cycles: {}; NOT counted against the {}-attempt per-entry ladder, so"
              + " these failures never terminal-FAIL entries); retry deferred by up to {} —"
              + " escalating toward the {}s cap while the failures last: {}",
          brokerWide
              ? "a broker-wide TRANSPORT failure"
              : "TRANSPORT-classified failures (NOT broker-wide this cycle — other claimed entries"
                  + " confirmed or resolved differently, too few entries resolved this cycle to"
                  + " tell, or this entry's own per-entry cadence has outpaced the relay-wide"
                  + " streak; possibly a single poisoned entry escalating on its own per-entry"
                  + " cadence)",
          transportFailures,
          pending.size(),
          LogSanitizer.sanitizeForLog(lastTransportEntryId.value()),
          relayStreak,
          retryPolicy.maxAttempts(),
          maxTransportBackoff,
          MAX_RETRY_BACKOFF.toSeconds(),
          lastTransportError);
    }
    if (inFlightFailures > 0) {
      // ONE aggregated WARN per cycle, mirroring the transport WARN above:
      // a post-hand-off outage fails the WHOLE claimed batch the same way, so N identical per-entry
      // lines per cycle were pure noise. Names the recovery mechanism and the gauge to watch,
      // because the pending backlog gauge deliberately does NOT move for these entries.
      LOG.warn(
          "Outbox delivery is IN-FLIGHT/unconfirmed for {} of {} claimed entries (result unknown,"
              + " they may still be delivered); leaving them claimed for lease-reclaim rather than"
              + " releasing them (avoids a duplicate + same-aggregate reorder). They stay"
              + " IN_PROGRESS, so watch streamrune.outbox.in_flight — streamrune.outbox.pending"
              + " does NOT count them: {}",
          inFlightFailures,
          pending.size(),
          lastInFlightError);
    }
    if (interruptedPublishes > 0) {
      LOG.info(
          "Outbox relay interrupted while publishing {} of {} claimed entries; the transport may"
              + " still deliver them, so their claims are held until the {}s claim lease expires"
              + " and the next relay redelivers them",
          interruptedPublishes,
          pending.size(),
          lease.toSeconds());
    }
    if (!unattemptedEntries.isEmpty()) {
      releaseUnattempted(
          unattemptedEntries, result, pending.size(), claimBase, publishDeadline, lease, stopping);
    }
  }

  /**
   * Returns the entries a publish cycle never attempted to {@code PENDING}, after every delivered
   * and failed outcome of the cycle is stored.
   */
  private void releaseUnattempted(
      List<OutboxEntry> unattemptedEntries,
      OutboxPublisher.BatchResult result,
      int claimed,
      Instant claimBase,
      Instant publishDeadline,
      Duration lease,
      boolean stopping) {
    if (stopping) {
      LOG.info(
          "Outbox relay stopping after delivering {} of {} claimed entries; releasing the {}"
              + " never-attempted claims for the next relay",
          result.confirmed().size(),
          claimed,
          unattemptedEntries.size());
    } else {
      // Loud, actionable WARN: the batch could not be fully published within the lease-bounded
      // deadline. The deadline is now min(fraction*lease, lease - maxInFlightHorizon),
      // so with a Kafka-class in-flight horizon it can be tighter than the old
      // fraction alone — report the ACTUAL budget rather than a fixed percentage. Recurrence means
      // the lease is too small for batchSize × worst-case per-entry publish time — raise the lease
      // or lower batchSize (see docs/guide/advanced/outbox.md "Sizing the claim lease").
      long budgetSeconds =
          publishDeadline != null
              ? Math.max(0L, Duration.between(claimBase, publishDeadline).toSeconds())
              : 0L;
      LOG.warn(
          "Outbox publish cycle reached its ~{}s per-batch deadline (of the {}s claim lease, capped "
              + "to keep each wired publisher's in-flight horizon inside the lease) after delivering "
              + "{} of {} claimed entries; releasing the {} never-attempted claims so the next cycle "
              + "can retry them immediately. If this recurs, raise the claim lease or lower "
              + "batchSize (see outbox.md \"Sizing the claim lease\").",
          budgetSeconds,
          lease.toSeconds(),
          result.confirmed().size(),
          claimed,
          unattemptedEntries.size());
    }
    // Released LAST, after every delivered/failed outcome is durable, so an entry can never be
    // back in PENDING while this cycle's accounting for it is still pending. Logged first so the
    // diagnostic lands even if the release itself throws — in which case the rows simply stay
    // IN_PROGRESS and lease-reclaim recovers them. releaseClaims is a per-entry CAS on IN_PROGRESS
    // + claimedBy, so entries this relay already lost (lease stolen while the cycle ran) are
    // skipped, never stolen back.
    int released = store.releaseClaims(List.copyOf(unattemptedEntries), claimedBy);
    if (released < unattemptedEntries.size()) {
      LOG.warn(
          "Outbox releaseClaims returned {} of {} never-attempted entries to PENDING; {} lost"
              + " their lease (claimed by another relay) and were skipped",
          released,
          unattemptedEntries.size(),
          unattemptedEntries.size() - released);
    }
  }

  /**
   * The instant by which this publish cycle must stop attempting entries, measured from {@code
   * base} — the instant captured just before {@code loadPending} claimed the batch, so claim-query
   * latency is charged against the budget rather than added on top of it. The budget is the smaller
   * of {@link #PUBLISH_DEADLINE_LEASE_FRACTION} of the lease and {@code lease -
   * maxInFlightHorizon}: the fraction bounds the common case, and the {@code lease -
   * maxInFlightHorizon} cap guarantees any entry allowed to START before the deadline still has at
   * least the publisher's in-flight horizon of lease left for its worst-case in-flight duration to
   * resolve before the lease could expire and a second relay reclaim it — closing the batch-tail
   * duplicate/reorder residual. The cap reuses the SAME {@link #maxInFlightHorizon} the {@code
   * build()} guard enforces ({@code lease > horizon}), so the two mechanisms uphold one invariant.
   * With a zero horizon it reduces to the fraction of the lease (the unchanged common/no-Kafka
   * case). Returns {@code null} (no deadline) when the store reports a non-positive lease, opting
   * that store out of deadline-bounding.
   *
   * <p>The {@code build()} guard already ensures {@code lease > maxInFlightHorizon}, so the capped
   * budget is non-negative; a razor-thin lease (one barely above the horizon, against the guard's
   * recommended margin) merely yields a near-zero budget that defers the whole batch to the next
   * cycle — a fail-safe stall with no duplicate risk, surfaced by the deadline WARN.
   */
  private Instant publishDeadlineFor(Instant base, Duration lease) {
    if (lease.isZero() || lease.isNegative()) {
      return null;
    }
    long fractionBudgetMillis = (long) (lease.toMillis() * PUBLISH_DEADLINE_LEASE_FRACTION);
    long horizonCappedBudgetMillis = lease.toMillis() - maxInFlightHorizon.toMillis();
    long budgetMillis = Math.min(fractionBudgetMillis, horizonCappedBudgetMillis);
    return base.plusMillis(budgetMillis);
  }

  /**
   * Normalizes a publisher's {@link OutboxPublisher#inFlightHorizon() in-flight horizon} for the
   * publish-deadline cap: a {@code null} or negative horizon (a publisher that leaves nothing in
   * flight, or a misbehaving one) becomes {@link Duration#ZERO}, so the cap degrades to the
   * uncapped {@link #PUBLISH_DEADLINE_LEASE_FRACTION} budget. Mirrors the {@code build()} guard,
   * which likewise treats a null/zero/negative horizon as no constraint.
   */
  private static Duration normalizeHorizon(Duration horizon) {
    return (horizon == null || horizon.isNegative()) ? Duration.ZERO : horizon;
  }

  /**
   * Reports the outbox observability gauges each poll cycle: the relay degradation gauge ({@code
   * streamrune.outbox.relay.consecutive_failures}) so a stalled-but-alive relay is visible on the
   * metrics endpoint (not only in the logs), the PENDING backlog gauge ({@code
   * streamrune.outbox.pending}), and the IN_PROGRESS backlog gauge ({@code
   * streamrune.outbox.in_flight}). Skipped entirely when no metrics are wired.
   *
   * <p>The in-flight gauge is not a nice-to-have — it is the ONLY backlog signal for a sustained
   * post-hand-off delivery outage. An {@link OutboxPublisher.FailureKind#IN_FLIGHT} failure
   * deliberately holds the claim (see {@link #recordFailure}) instead of releasing the entry back
   * to PENDING, so the whole backlog parks in IN_PROGRESS while {@code streamrune.outbox.pending}
   * reads ~0, {@code streamrune.outbox.delivery_failed} never fires (IN_FLIGHT burns no attempt)
   * and the transport streak stays at zero (IN_FLIGHT is not transport evidence). It is a SEPARATE
   * series rather than being counted into the pending gauge on purpose: folding the two would
   * silently redefine every existing alert written against {@code streamrune.outbox.pending}.
   *
   * <p>Both backlog samples run in their OWN try so an outbox-store outage does not abort the
   * delivery cycle: a transient count failure is logged and swallowed (the gauges hold their last
   * value), a store that does not support {@link OutboxStore#countByStatus} disables the samples
   * permanently, and the outage still surfaces distinctly through the degradation gauge
   * (loadPending failing below drives consecutiveFailures up) rather than as a silently-stale gauge
   * or a killed cycle. They share one try/flag because they share one store capability — a store
   * that can count one status can count the other.
   *
   * <p>The two blockage gauges ({@code streamrune.outbox.blocked_aggregates}, {@code
   * streamrune.outbox.blockage_age_seconds}) are sampled in a second try behind their own flag,
   * {@link #blockageSamplingSupported}, from one {@link OutboxStore#sampleBlockage()} read. The two
   * flags degrade at different levels on purpose: a store without {@code countByStatus} loses a
   * convenience backlog view (DEBUG, {@link #backlogSamplingSupported}), whereas a store without
   * {@code sampleBlockage} loses the <em>primary alert series</em> for a blocked strict aggregate
   * (or a stuck FAILED row on an availability-first channel), so that degrade is logged ONCE at
   * WARN, naming the consequence and the fix. A transient failure of either read is a per-cycle
   * WARN and the gauges hold their last value.
   */
  private void sampleObservability() {
    if (metrics == StreamRuneMetrics.NOOP) {
      return;
    }
    // Guarded — a byte-identical twin of the DLQ retry runner's sampleObservability, whose
    // own degradation-gauge call was guarded the same way. This one runs FIRST in
    // every processBatch(), before loadPending claims anything, so an unguarded throw here aborted
    // the ENTIRE outbox poll cycle — no claim, no delivery, no retry — every single cycle a
    // throwing metrics backend was wired, indistinguishable from a dead relay except for the
    // gauge that never got the chance to report it.
    recordMetric(
        "recordOutboxRelayDegradation",
        () -> metrics.recordOutboxRelayDegradation(consecutiveFailures()));
    if (backlogSamplingSupported) {
      try {
        metrics.recordOutboxBacklog(store.countByStatus(OutboxStatus.PENDING));
        metrics.recordOutboxInFlight(store.countByStatus(OutboxStatus.IN_PROGRESS));
      } catch (UnsupportedOperationException _) {
        backlogSamplingSupported = false;
        LOG.debug(
            "OutboxStore {} does not support countByStatus; outbox backlog gauges disabled",
            store.getClass().getSimpleName());
      } catch (RuntimeException e) {
        LOG.warn("Failed to sample outbox backlog this cycle: {}", e.getMessage());
      }
    }
    if (!blockageSamplingSupported) {
      return;
    }
    try {
      OutboxStore.BlockageSample s = store.sampleBlockage();
      metrics.recordOutboxBlockedAggregates(s.failedAggregates());
      long age =
          s.oldestFailedAt() == null
              ? 0
              : Math.max(0, Duration.between(s.oldestFailedAt(), Instant.now()).toSeconds());
      metrics.recordOutboxBlockageAge(age);
    } catch (UnsupportedOperationException _) {
      blockageSamplingSupported = false;
      LOG.warn(
          "OutboxStore {} does not support sampleBlockage; outbox blockage gauges disabled —"
              + " blockage alerts will NOT fire for this {} channel; implement sampleBlockage()",
          store.getClass().getSimpleName(),
          orderingMode);
    } catch (RuntimeException e) {
      LOG.warn("Failed to sample outbox blockage this cycle: {}", e.getMessage());
    }
  }

  /**
   * A strict channel that holds FAILED rows with no aggregate_id (legacy availability-first data)
   * WARNs once and keeps running — those rows are not heads, never block, and stay resolvable. Runs
   * before sampleObservability and independently of the metrics NOOP guard: a WARN must not depend
   * on metrics wiring.
   */
  private void checkLegacyNullAggregateFailedOnce() {
    if (legacyNullAggregateChecked || orderingMode != OutboxOrderingMode.STRICT_PER_AGGREGATE) {
      return;
    }
    try {
      OutboxStore.BlockageSample sample = store.sampleBlockage();
      legacyNullAggregateChecked = true;
      if (sample.nullAggregateFailed() > 0) {
        LOG.warn(
            "Strict outbox channel holds {} FAILED entries with no aggregate_id (written under"
                + " AVAILABILITY_FIRST): they are not heads, never block, and remain resolvable"
                + " through findByStatus(FAILED) / OutboxFailedReplayer.replay / .skip",
            sample.nullAggregateFailed());
      }
    } catch (UnsupportedOperationException _) {
      // sampleObservability's WARN-once names the missing sampleBlockage(); nothing to check with.
      legacyNullAggregateChecked = true;
      LOG.debug(
          "OutboxStore {} does not support sampleBlockage; legacy null-aggregate check skipped",
          store.getClass().getSimpleName());
    } catch (RuntimeException e) {
      LOG.warn(
          "Could not check for legacy null-aggregate FAILED outbox entries this cycle; retrying"
              + " next cycle: {}",
          e.getMessage());
    }
  }

  /**
   * Runs one metrics call, logging and swallowing any {@link RuntimeException} from the backend
   * (mirroring {@code DeadLetterRetryRunner.recordMetric}). {@link StreamRuneMetrics} is a
   * pluggable sink — a Micrometer implementation registers meters lazily and can throw against a
   * closed or misconfigured registry — and a failure there must never abort a delivery cycle or a
   * failure-bookkeeping path.
   */
  private void recordMetric(String metricName, Runnable call) {
    try {
      call.run();
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for {}", metricName, e);
    }
  }

  /**
   * Returns whether {@link #start()} has been called and {@link #close()} has not. Combined with
   * {@link #isAlive()} this distinguishes a relay that was never started (or was cleanly closed)
   * from one whose poll thread <em>died</em> — {@code isStarted() && !isAlive()} is the dead-relay
   * signal the health check reports DOWN.
   */
  public boolean isStarted() {
    return started.get();
  }

  /**
   * Returns whether the relay's poll thread is currently alive. A started relay whose thread is no
   * longer alive died unexpectedly (e.g. an {@link Error} escaped the poll loop) — see {@link
   * #isStarted()}.
   */
  public boolean isAlive() {
    PollThread running = pollThread;
    return running != null && running.thread().isAlive();
  }

  /**
   * Records a per-entry delivery failure. A broker-wide {@link
   * OutboxPublisher.FailureKind#TRANSPORT TRANSPORT} failure is <b>non-counting</b>: it is retried
   * forever with capped backoff ({@link OutboxStore#markRetry}) and never marked terminal, so a
   * routine broker restart cannot terminal-{@code FAILED} the backlog (nor silently un-gate a
   * {@code FAILED} head's successors). A per-{@link OutboxPublisher.FailureKind#ENTRY ENTRY}
   * rejection increments the attempt counter and is marked terminal ({@link
   * OutboxStore#markFailed}) once {@link RetryPolicy#maxAttempts()} is reached; otherwise it is
   * retried with capped backoff. A {@code null} error is <b>unclassifiable</b> — there is no
   * exception to hand to {@link OutboxPublisher#classifyFailure} — so it is binned with the
   * unclassifiable default: non-counting {@code TRANSPORT}. It is deliberately NOT binned {@code
   * IN_FLIGHT}: holding the claim on a shape the wiring never declared would trip the zero-horizon
   * violation guard by design, and nothing proves a hand-off happened. The old {@code ENTRY}
   * binning walked the counting ladder to terminal {@code FAILED} for exactly the nack/timeout
   * shapes now classified as non-counting — reproducing the mass-FAIL the TRANSPORT default
   * removed, from any custom {@code publishBatch} that followed this method's own javadoc.
   *
   * <p><b>Cadence vs ladder.</b> Only the TRANSPORT path adds a streak — the stronger of the
   * relay-wide {@link #transportStreak} and the entry's own {@link #entryTransportStreaks rung} —
   * to the attempt number handed to {@link RetryPolicy#delayForAttempt}, so a sustained outage AND
   * a persistently transport-failing single entry both escalate their retry cadence (1s, 2s, 4s …
   * up to {@link #MAX_RETRY_BACKOFF}) while the persisted counter stays frozen. Escalating
   * <em>through</em> {@code delayForAttempt} keeps the policy's jitter on every rung, which is what
   * de-correlates several relays' retry instants. An ENTRY rejection is untouched by both streaks:
   * it keeps its own counted-ladder backoff. An {@link OutboxPublisher.FailureKind#IN_FLIGHT
   * IN_FLIGHT} hand-off is not released for retry at all, so it has no cadence to escalate — its
   * redelivery is paced by the claim lease.
   *
   * <p><b>Interrupted publishes.</b> A failure whose cause chain holds an {@link
   * InterruptedException} is a publish that was cut short after it started (a stop arrived while it
   * waited on the transport). It is held like an {@code IN_FLIGHT} hand-off — claim kept, no
   * attempt counted, nothing recorded — before {@link OutboxPublisher#classifyFailure} is
   * consulted: a publisher's classification describes broker outcomes, and for an interruption the
   * outcome is unknown whatever the publisher would answer. It is the relay's own decision, so it
   * is not reported as a zero-horizon contract violation.
   *
   * @param entry the entry that was not delivered
   * @param error the exception that caused the failure. A custom {@code publishBatch} MUST report a
   *     broker nack or a timeout as a TYPED exception describing it (so {@code classifyFailure} can
   *     bin the failure — see the shipped publishers' three-way splits); {@code null} is tolerated
   *     as "failed with no local exception" and is binned as unclassifiable — non-counting {@code
   *     TRANSPORT}, never the counting ladder and never {@code IN_FLIGHT}
   * @return what this failure was, for the caller's per-cycle aggregation: {@link
   *     FailureOutcome#transportBackoff()} is the TRANSPORT retry backoff stamped for this entry
   *     (null for every other kind) and feeds the consecutive-transport streak and the aggregated
   *     transport WARN — a transport failure is outage evidence even if {@code markRetry} then lost
   *     its lease, and that case logs its own WARN; {@link FailureOutcome#inFlight()} feeds the
   *     aggregated IN_FLIGHT WARN; {@link FailureOutcome#interrupted()} feeds the
   *     interrupted-publish log line.
   */
  private FailureOutcome recordFailure(OutboxEntry entry, Exception error) {
    if (error != null && isInterruption(error)) {
      // The publish started and was then interrupted: the transport may still deliver it (a Kafka
      // record already in the producer buffer, an HTTP request already written, a RabbitMQ message
      // awaiting its confirm). Releasing it would let a peer relay re-publish it while that copy
      // is still on its way — a duplicate and a same-aggregate reorder. Hold the claim; only
      // lease-reclaim redelivers it.
      LOG.debug(
          "Outbox entry {} publish was interrupted after it started; leaving it claimed for"
              + " lease-reclaim: {}",
          LogSanitizer.sanitizeForLog(entry.id().value()),
          error.getMessage());
      return FailureOutcome.STOP_INTERRUPTED;
    }
    // A failure whose delivery outcome is genuinely UNKNOWN — the message was
    // handed off to the broker but not confirmed (a send/confirm/request timeout; it may still be
    // delivered) — must NOT be markRetry'd. markRetry releases the claim (status -> PENDING,
    // claimed_by cleared), so a second HA relay reclaims and RE-PUBLISHES while our publish is
    // still in-flight: a duplicate AND a same-aggregate reorder. Leave the entry IN_PROGRESS
    // (claimed) so ONLY the lease-reclaim path redelivers it, after the lease (which must exceed
    // the transport's max in-flight horizon) expires and the in-flight publish has resolved.
    if (error != null
        && publisher.classifyFailure(error) == OutboxPublisher.FailureKind.IN_FLIGHT) {
      if (maxInFlightHorizon.isZero()) {
        // Zero-horizon violation: classifyFailure just said "the hand-off may still
        // resolve at the transport" while inFlightHorizon() still reports the ZERO default —
        // "nothing is ever left in flight". The two defaults contradict each other, and the
        // contradiction is dangerous: the build() lease guard SKIPS a zero horizon, so the claim
        // lease was never validated against the transport's real in-flight window. A build-time
        // refusal is impossible in general (the classification is behavioral — a lambda cannot
        // override the default, and an anonymous class may override only classifyFailure), so it
        // is caught here, at the first classified IN_FLIGHT. Fail SAFE: hold the claim for the
        // full lease exactly as for a declared horizon (the code path below is unchanged — never
        // release early), and report LOUDLY so the operator learns the lease guard is not
        // protecting this wiring.
        reportZeroHorizonInFlight(entry, error);
      }
      // No per-entry WARN — a post-hand-off outage fails the whole claimed
      // batch, so processBatch logs ONE aggregated WARN per cycle from this outcome, exactly as the
      // TRANSPORT path already does. DEBUG keeps the per-entry detail available when needed.
      LOG.debug(
          "Outbox entry {} delivery is IN-FLIGHT/unconfirmed (result unknown, may still be"
              + " delivered); leaving it claimed for lease-reclaim rather than releasing it (avoids a"
              + " duplicate + same-aggregate reorder): {}",
          LogSanitizer.sanitizeForLog(entry.id().value()),
          error.getMessage());
      return FailureOutcome.IN_FLIGHT;
    }
    // The message is persisted (outbox_events.last_error), so it is sanitized at this sink.
    String message =
        LogSanitizer.sanitizeFreeText(
            error != null ? error.getMessage() : "not confirmed by broker");
    // A null error is unclassifiable — classifyFailure's contract takes a never-null
    // failure, so it must not even be called. Bin it with the unclassifiable default: non-counting
    // TRANSPORT (see the method javadoc for why not ENTRY and why not IN_FLIGHT).
    boolean transport =
        error == null || publisher.classifyFailure(error) == OutboxPublisher.FailureKind.TRANSPORT;
    int attempts = entry.attempts() + 1;
    // TRANSPORT never terminal-fails, however many attempts it accrues — that is the guard.
    boolean terminal = !transport && attempts >= retryPolicy.maxAttempts();
    boolean transitioned;
    Duration transportBackoff = null;
    if (terminal) {
      transitioned = store.markFailed(entry.id(), attempts, message, claimedBy);
    } else {
      // A TRANSPORT (non-counting) failure must NOT persist the incremented counter.
      // Persisting attempts+1 during a broker outage lets outage-accrued attempts retroactively
      // terminal-FAIL the entry on its FIRST later ENTRY-classified rejection — before its real
      // per-entry ladder is exhausted (permanent gap + silent same-aggregate reorder once a FAILED
      // head stops gating). Keep the persisted counter flat for transport; only an ENTRY rejection
      // advances the ladder. The backoff is still stamped so the entry keeps backing off.
      int persistedAttempts = transport ? entry.attempts() : attempts;
      // The backoff INPUT is the attempt number plus — for transport only —
      // the STRONGER of this relay's two transport streaks: the relay-wide
      // consecutive-all-transport-cycle streak (broker-wide outages, and the floor for entries the
      // bounded per-entry map evicted) and the entry's OWN consecutive-transport-failure rung (a
      // poisoned head amid healthy traffic — confirms reset the relay-wide streak every cycle, so
      // without the per-entry rung such an entry re-published at initialDelay forever). The
      // persisted counter above stays frozen for transport, so without a streak term
      // the input never grew. The relay-wide streak escalates one rung per all-transport cycle and
      // resets on the first confirmed publish (see processBatch); the per-entry rung advances one
      // rung per transport failure of that entry, below, and is cleared when the entry confirms.
      // ENTRY rejections keep their own counted ladder untouched. Composes with a real counted
      // history: an entry that already burned ENTRY attempts starts its transport escalation from
      // that rung, and the cap bounds the sum.
      int backoffAttempt = attempts;
      if (transport) {
        int entryStreak = entryTransportStreaks.getOrDefault(entry.id(), 0);
        backoffAttempt = attempts + Math.max(transportStreak.get(), entryStreak);
        // Advance the entry's rung on the failure itself — transport evidence for THIS entry even
        // if markRetry below then loses its lease (matching the relay-wide streak's evidence
        // rule: a transport failure is outage evidence regardless of the lease outcome).
        entryTransportStreaks.put(entry.id(), Math.min(entryStreak + 1, MAX_TRANSPORT_STREAK));
      }
      // Pass the backoff as a Duration; the store DB-stamps next_retry_at = storeNow + backoff on
      // the same clock the claim gate reads, so a skewed relay wall clock cannot collapse the
      // backoff. Cap it so a long outage cannot push next_retry_at arbitrarily far
      // out. Never compute an absolute instant off Instant.now() here.
      Duration backoff = cappedBackoff(retryPolicy.delayForAttempt(backoffAttempt));
      transitioned = store.markRetry(entry.id(), persistedAttempts, message, backoff, claimedBy);
      if (transport) {
        // No per-entry WARN: transport failures are broker-wide, so the whole claimed batch fails
        // together and N identical lines per cycle were pure noise. processBatch logs one
        // aggregated WARN for the cycle from this returned backoff.
        transportBackoff = backoff;
      } else if (transitioned) {
        LOG.warn(
            "Outbox delivery failed for entry {} (attempt {}/{}), retrying in {}: {}",
            LogSanitizer.sanitizeForLog(entry.id().value()),
            attempts,
            retryPolicy.maxAttempts(),
            backoff,
            message);
      }
    }
    if (terminal && transitioned) {
      // Terminal FAILED: exhausted the retry ladder. Surface it loudly and emit the metric so a
      // broker outage that outlasts backoff is not silent (recovery is via OutboxFailedReplayer).
      // The line names the consequence the channel's mode implies: on a strict channel the
      // stream is BLOCKED until this row is replayed or skipped; on an availability-first
      // channel its later entries continue and the stream carries a gap until replay.
      String stream =
          entry.streamId() == null
              ? "<none>"
              : LogSanitizer.sanitizeForLog(entry.streamId().value());
      if (orderingMode == OutboxOrderingMode.STRICT_PER_AGGREGATE && entry.streamId() != null) {
        LOG.error(
            "Outbox entry {} of stream {} FAILED after {} attempts; stream BLOCKED — its later"
                + " entries will not be delivered until this entry is replayed or skipped"
                + " (OutboxFailedReplayer): {}",
            LogSanitizer.sanitizeForLog(entry.id().value()),
            stream,
            attempts,
            message);
      } else {
        LOG.error(
            "Outbox entry {} of stream {} FAILED after {} attempts; later entries of stream {}"
                + " continue — this stream now has a gap until replay: {}",
            LogSanitizer.sanitizeForLog(entry.id().value()),
            stream,
            attempts,
            stream,
            message);
      }
      // Guarded — was bare; a throw here propagated out of recordFailure() (past the
      // already-committed store.markFailed) and out of processBatch()'s per-entry loop, silently
      // skipping every remaining entry in the batch this cycle.
      recordMetric("recordOutboxDeliveryFailed", metrics::recordOutboxDeliveryFailed);
      // Map hygiene: the entry left circulation — drop its per-entry transport rung. Only
      // a mixed transport-then-ENTRY history can reach here with a rung (pure TRANSPORT never
      // terminals), and the LRU eviction would reclaim it eventually anyway.
      entryTransportStreaks.remove(entry.id());
    }
    if (!transitioned) {
      // Lost lease: the entry is no longer IN_PROGRESS under this relay's claimedBy — another relay
      // already recorded its outcome (expected under failover). Benign no-op, never an error.
      LOG.warn(
          "Outbox entry {} failure not recorded: lease lost (claimed by another relay)",
          LogSanitizer.sanitizeForLog(entry.id().value()));
    }
    return transportBackoff == null ? FailureOutcome.OTHER : new FailureOutcome(transportBackoff);
  }

  /**
   * Reports one zero-horizon contract violation (see {@link #recordFailure}): the metric fires per
   * held entry so a persistent violation stays visible as a rate, while the ERROR — naming the
   * publisher class, the fail-safe behavior, and the fix — is logged once per relay (see {@link
   * #zeroHorizonViolationLogged}).
   */
  private void reportZeroHorizonInFlight(OutboxEntry entry, Exception error) {
    // Guarded — was bare; a throw here propagated out of recordFailure() and out of
    // processBatch()'s per-entry loop, aborting the CURRENT entry's failure recording (skipping
    // the LOG.debug below and the FailureOutcome.IN_FLIGHT return) and every remaining entry in
    // the batch this cycle — a metrics hiccup silently stalling delivery for entries that never
    // even reached the fail-safe branch this method exists to report.
    recordMetric(
        "recordOutboxInFlightHorizonViolation", metrics::recordOutboxInFlightHorizonViolation);
    if (!zeroHorizonViolationLogged) {
      zeroHorizonViolationLogged = true;
      LOG.error(
          "OutboxPublisher contract violation (zero in-flight horizon): {} classified a"
              + " failure IN_FLIGHT (the hand-off may still resolve at the transport) for entry {},"
              + " but its inFlightHorizon() reports Duration.ZERO ('nothing is ever left in"
              + " flight'), so OutboxPoller.build() never validated the claim lease against the"
              + " transport's real in-flight window — the lease may be too short to stop a second"
              + " relay from re-publishing a still-in-flight entry (a duplicate + a same-aggregate"
              + " reorder). Failing SAFE: the claim is held for the full lease, exactly as for a"
              + " declared horizon. FIX: override inFlightHorizon() with an honest upper bound on"
              + " how long a publish can keep resolving after publish() returns/throws (see the"
              + " OutboxPublisher javadoc); the build() guard will then size-check the lease."
              + " Further occurrences are counted on streamrune.outbox.in_flight_horizon_violation"
              + " without repeating this log. Failure was: {}",
          publisher.getClass().getName(),
          LogSanitizer.sanitizeForLog(entry.id().value()),
          error.getMessage());
    }
  }

  /**
   * What {@link #recordFailure} did with one entry, for {@link #processBatch}'s per-cycle
   * aggregation. At most one of the three shapes carries information: a non-null {@code
   * transportBackoff} (the deferral stamped for a broker-wide TRANSPORT retry), {@code inFlight}
   * (the hand-off whose outcome is unknown and whose claim is deliberately held) or {@code
   * interrupted} (a publish cut short by an interrupt, held the same way). Everything else — an
   * ENTRY rejection, a terminal FAILED, a lost lease — is {@link #OTHER} and is reported per-entry
   * at the point it happens.
   */
  private record FailureOutcome(Duration transportBackoff, boolean inFlight, boolean interrupted) {

    private static final FailureOutcome OTHER = new FailureOutcome(null, false, false);
    private static final FailureOutcome IN_FLIGHT = new FailureOutcome(null, true, false);
    private static final FailureOutcome STOP_INTERRUPTED = new FailureOutcome(null, false, true);

    private FailureOutcome(Duration transportBackoff) {
      this(transportBackoff, false, false);
    }
  }

  /**
   * Whether {@code failure} is, or was caused by, an {@link InterruptedException}. The cause walk
   * is bounded (depth 50) and stops at a self-referencing cause, so a cyclic chain terminates.
   */
  private static boolean isInterruption(Throwable failure) {
    Throwable t = failure;
    for (int depth = 0; t != null && depth < 50; depth++) {
      if (t instanceof InterruptedException) {
        return true;
      }
      Throwable cause = t.getCause();
      if (cause == t) {
        return false;
      }
      t = cause;
    }
    return false;
  }

  /** Clamps a retry backoff to {@link #MAX_RETRY_BACKOFF}. */
  private static Duration cappedBackoff(Duration backoff) {
    return backoff.compareTo(MAX_RETRY_BACKOFF) > 0 ? MAX_RETRY_BACKOFF : backoff;
  }

  /**
   * Stops the background thread and resets the started guard so start() may be called again.
   *
   * <p>The poll thread is interrupted, which cuts an idle sleep or a publish waiting on the
   * transport short. While the thread is storing a batch's outcome the interrupt is held back until
   * the outcome is stored (so this call can wait for those store writes), and the thread stops
   * right after. Waits up to 30s for the thread to end; a thread still running after that is left
   * alone and the relay stays not restartable (logged).
   */
  @Override
  public void close() {
    PollThread running = pollThread;
    pollThread = null;
    boolean stuck = false;
    if (running != null) {
      Thread t = running.thread();
      running.interrupts().interrupt(t);
      // Join the poll thread before resetting `started`, so a stale poll thread cannot
      // still be running when a subsequent start() spawns a new one (a leaked duplicate relay).
      // Self-join guard: a thread cannot join itself (close() invoked from within the poll thread).
      if (t != Thread.currentThread()) {
        try {
          t.join(closeJoinTimeoutMs);
        } catch (InterruptedException _) {
          Thread.currentThread().interrupt();
        }
        // The join timed out and the poll thread is still alive (a black-holed poll
        // body). Do NOT reset `started` — refuse to become restartable, because a restart would run
        // a SECOND poll thread alongside the stuck one (silent duplicate relay). Leave it started
        // so start() throws until the stuck thread is dealt with, and log loudly so it is
        // observable.
        stuck = t.isAlive();
      }
    }
    if (stuck) {
      LOG.warn(
          "OutboxPoller poll thread did not stop within {}ms; leaving the runner not-restartable so"
              + " a restart cannot spawn a duplicate relay. Investigate the stuck poll.",
          closeJoinTimeoutMs);
    } else {
      started.set(false);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link OutboxPoller}. */
  public static final class Builder {
    private OutboxStore store;
    private OutboxPublisher publisher;
    private RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;
    private int batchSize = 100;
    private Duration pollInterval = Duration.ofSeconds(1);
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private long closeJoinTimeoutMs = CLOSE_JOIN_TIMEOUT_MS;
    // Read from the store in build(): the poller logs and reasons about the mode, so a store that
    // does not state one (the throwing default, or null) fails at wiring, like claimLease.
    private OutboxOrderingMode orderingMode;
    // Read once from the store in build(), alongside the mode: the lease the guard validates is the
    // lease the poller bounds every publish deadline by.
    private Duration claimLease;

    public Builder outboxStore(OutboxStore store) {
      this.store = store;
      return this;
    }

    /**
     * Sets the metrics collector. Optional; defaults to {@link StreamRuneMetrics#NOOP}. Used to
     * emit {@link StreamRuneMetrics#recordOutboxDeliveryFailed()} when an entry reaches terminal
     * {@code FAILED}.
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
      return this;
    }

    public Builder publisher(OutboxPublisher publisher) {
      this.publisher = publisher;
      return this;
    }

    public Builder retryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder pollInterval(Duration pollInterval) {
      this.pollInterval = pollInterval;
      return this;
    }

    /**
     * Overrides the {@link #close()} join timeout. Package-private test seam: production uses the
     * 30s default; the stuck-poll test shrinks it so the join provably times out without a 30s
     * wait.
     */
    Builder closeJoinTimeoutMs(long closeJoinTimeoutMs) {
      this.closeJoinTimeoutMs = closeJoinTimeoutMs;
      return this;
    }

    public OutboxPoller build() {
      if (store == null) throw new IllegalStateException("outboxStore is required");
      if (publisher == null) throw new IllegalStateException("publisher is required");
      if (retryPolicy == null) throw new IllegalStateException("retryPolicy is required");
      if (batchSize < 1) throw new IllegalStateException("batchSize must be >= 1");
      if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
        throw new IllegalStateException(
            "pollInterval must be non-null and positive (Duration.ZERO causes a tight spin loop)");
      }
      orderingMode = store.orderingMode();
      if (orderingMode == null) {
        // Every mode-dependent branch tests for STRICT_PER_AGGREGATE, so a null mode would run the
        // relay availability-first without anyone having chosen it (an un-stubbed mock returns
        // exactly this). Refuse it at wiring, like a store that does not state a mode at all.
        throw new IllegalStateException(
            "OutboxStore "
                + store.getClass().getName()
                + " returned null from orderingMode(); it must return the OutboxOrderingMode it"
                + " enforces in loadPending and save (STRICT_PER_AGGREGATE or AVAILABILITY_FIRST).");
      }
      claimLease = store.claimLease();
      if (claimLease == null) {
        // A null lease would read as the opt-out below (no wiring guard, no publish deadline)
        // without anyone having chosen it. The deliberate opt-out is a non-positive Duration, so
        // refuse null at wiring, like a store that does not state a lease at all.
        throw new IllegalStateException(
            "OutboxStore "
                + store.getClass().getName()
                + " returned null from claimLease(); it must return the reclaim window after which a"
                + " claimed entry returns to PENDING, or a non-positive Duration to opt out of"
                + " deadline-bounding explicitly.");
      }
      validateLeaseExceedsInFlightHorizon();
      return new OutboxPoller(this);
    }

    /**
     * Refuse to build when the store's claim lease does not STRICTLY EXCEED the publisher's {@link
     * OutboxPublisher#inFlightHorizon() in-flight horizon}.
     *
     * <p>The claim lease is the point at which another relay may reclaim a still-{@code
     * IN_PROGRESS} entry (see {@link OutboxStore#claimLease()}) and re-publish it. The in-flight
     * horizon is the longest a single {@link OutboxPublisher#publish} attempt can still be
     * resolving at the transport, measured from the moment the attempt STARTS — e.g. Kafka's {@code
     * send()} first blocks up to {@code max.block.ms} on metadata/accumulator space and only then
     * enqueues the record, which stays buffered and retried internally until {@code
     * delivery.timeout.ms}, even after {@code send().get(sendTimeout)} throws a {@link
     * OutboxPublisher.FailureKind#IN_FLIGHT} {@code TimeoutException} — so its horizon is the SUM
     * of the two (180s at Kafka's defaults). If the lease did not exceed the horizon, a reclaim
     * could fire while that original publish is still being delivered, producing a duplicate AND a
     * same-aggregate reorder — the one thing the ordering contract forbids. Fail loudly at wiring
     * rather than run the unsafe combination ("Zero Magic, Full Control"), matching the framework's
     * other refuse-to- boot config validators.
     *
     * <p>A store that returns a non-positive lease has opted out of deadline-bounding (see {@link
     * OutboxStore#claimLease()}) — it has no reclaim window to protect — so it is skipped, as is a
     * publisher whose horizon is {@link Duration#ZERO} (nothing left in flight).
     */
    private void validateLeaseExceedsInFlightHorizon() {
      Duration lease = claimLease;
      if (lease.isZero() || lease.isNegative()) {
        return; // store opted out of deadline-bounding: no reclaim window to protect.
      }
      Duration horizon = publisher.inFlightHorizon();
      if (horizon == null || horizon.isZero() || horizon.isNegative()) {
        // Publisher declares that nothing is left in flight once publish() returns. This guard
        // must take the declaration at face value (an IN_FLIGHT classification is behavioral and
        // unobservable at build time), so a publisher that LIES here — classifying failures
        // IN_FLIGHT while keeping the ZERO horizon default — is caught at delivery time instead:
        // recordFailure reports the contract violation loudly and fail-safes the claim for the
        // full lease.
        return;
      }
      if (lease.compareTo(horizon) <= 0) {
        Duration recommended = horizon.plus(CLAIM_LEASE_INFLIGHT_MARGIN);
        throw new IllegalStateException(
            "Outbox claim lease ("
                + lease.toMillis()
                + "ms) does not exceed the in-flight horizon ("
                + horizon.toMillis()
                + "ms) of publisher "
                + publisher.getClass().getName()
                + ". A lease at or below the horizon lets a second relay reclaim and"
                + " re-publish an entry whose delivery is still in flight, causing a duplicate and a"
                + " same-aggregate reorder. Configure the outbox claim lease to at least "
                + recommended.toMillis()
                + "ms (in-flight horizon + "
                + CLAIM_LEASE_INFLIGHT_MARGIN.toMillis()
                + "ms margin), or lower the publisher's transport timeouts so the horizon itself"
                + " shrinks below the lease (Kafka's horizon is max.block.ms + delivery.timeout.ms,"
                + " and max.block.ms is usually the cheapest lever; RabbitMQ's is"
                + " publishBlockBudget + confirmTimeout, HTTP's is the request timeout).");
      }
    }
  }
}
