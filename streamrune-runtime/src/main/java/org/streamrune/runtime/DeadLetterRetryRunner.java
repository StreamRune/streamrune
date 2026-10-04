package org.streamrune.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.SealedHierarchy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneContext.RequestContext;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.crypto.CryptoConfigValidator;
import org.streamrune.crypto.CryptoShreddingModule;

/**
 * Virtual-thread-based poller that retries dead letter queue entries by re-dispatching them through
 * a {@link CommandBus}. Entries that succeed are discarded; entries that fail have their DLQ
 * attempt count incremented.
 *
 * <p>Each entry is retried on its own schedule derived from the {@link DeadLetterRetryPolicy}: the
 * first retry is due {@code delayForAttempt(1)} after the entry was published, each subsequent
 * retry {@code delayForAttempt(dlqAttempts + 1)} after the previous attempt. Entries whose backoff
 * window has not elapsed are skipped by the poll cycle, so a recovering downstream is not hammered
 * by every entry on every poll in lockstep.
 *
 * <p><b>Replay context.</b> The DLQ entry persists the originating request context (correlation,
 * user, trace), and the runner rebinds it on {@link org.streamrune.core.StreamRuneContext#CURRENT}
 * around {@link CommandBus#execute} so the replay runs as the original request: events keep their
 * original correlation, and interceptors that read the current user — e.g. fail-closed
 * authorization — re-evaluate the replay as the original user, so {@code @RequireRole} commands
 * that originally passed pass again instead of burning their retries. Entries for commands that ran
 * with no context bound carry a null correlation id and replay unbound. Both hold whichever path
 * triggers the replay: the scheduled poll runs on the runner's own thread, and the on-demand {@link
 * #retry(CommandId)} runs the replay on a fresh virtual thread and waits for it, so the context of
 * the caller — an operator's admin request — never becomes the replay's identity.
 *
 * <p><b>Idempotent replay.</b> The entry's claim (a {@code FOR UPDATE SKIP LOCKED}-style read) is
 * released before the command executes, so two poller instances — or two poll cycles on the same
 * instance — can both pick up the same entry and both attempt to execute it. Each replay is
 * therefore executed under an {@link org.streamrune.core.types.IdempotencyKey}: the ORIGINAL
 * caller-supplied key persisted on the entry when the command ran via {@code execute(command,
 * key)}, or — for commands that ran unkeyed — a deterministic key derived from the entry's command
 * id ({@code "dlq-replay:" + commandId}). Preferring the original key is essential: it is the
 * single dedup identity that reconciles a cross-instance duplicate replay AND the client's own
 * at-least-once redelivery of the same command against the same {@link
 * org.streamrune.core.CommandInbox} row, so a keyed command that failed on transient infra and was
 * dead-lettered still produces its events exactly once (replaying under a synthetic key instead
 * would let the client's redelivery miss the inbox and re-execute the handler). In both cases a
 * duplicate replay short-circuits at the {@link org.streamrune.core.CommandInbox} instead of
 * re-running the handler — but only when the bus actually supports it. The runner checks {@link
 * CommandBus#supportsIdempotentExecution()} up front: {@code true} means an inbox is configured and
 * the replay runs keyed; {@code false} means it runs the unkeyed {@link
 * CommandBus#execute(Command)}, logging a one-time WARN — replays still work, just without
 * cross-instance de-duplication. The capability is checked before the call rather than inferred
 * from a caught exception: exception-type detection is fragile (implementations that lack an inbox
 * may throw different exception types) and, worse, could mistake an unrelated failure from inside a
 * keyed execution — e.g. a decider that itself throws {@link UnsupportedOperationException} — for
 * "keyed execution is unsupported," triggering a spurious unkeyed double-execute of the command.
 *
 * <p>An unresolvable command type (no class registered for the entry's {@code commandType}) is
 * treated as a failed attempt like any other failure — it bumps {@code dlqAttempts} and ages toward
 * discard-per-policy — so it can never permanently occupy the oldest-first, batch-bounded retry
 * window and starve resolvable entries behind it. Nothing is executed for it.
 *
 * <p>A failure of the whole poll cycle (e.g. the dead letter queue being temporarily unreachable)
 * is logged and retried with capped exponential backoff — it never silently kills the retry thread.
 * {@link #isRunning()} reflects the actual liveness of the polling thread.
 *
 * <p><b>Single-active-consumer.</b> When a {@link SubscriptionLeadership} is configured (via {@link
 * Builder#leadership}), only the instance holding leadership for the fixed consumer name {@code
 * "dead-letter-retry"} polls and retries: {@link #processBatch()} calls {@link
 * SubscriptionLeadership#tryAcquire} first and returns without reading a single entry when it is
 * not the leader. This closes the multi-replica double-execute hole that the {@link
 * org.streamrune.core.CommandInbox}-keyed replay only covers when an inbox is present: {@code FOR
 * UPDATE SKIP LOCKED} protects only the read transaction, and the runner executes <em>after</em>
 * that transaction commits, so without an inbox two replicas' staggered polls would both re-execute
 * the same entry and emit duplicate domain events. The leadership gate makes exactly one replica
 * poll. The default is {@link SubscriptionLeadership#NOOP} (always leader), so single-instance
 * behavior is byte-identical to a runner with no leadership at all. On {@link #close()} the runner
 * {@linkplain SubscriptionLeadership#resign resigns} its name so a standby replica can take over
 * immediately (NOOP resign is a no-op). The on-demand {@link #retry(CommandId)} admin path is NOT
 * gated — an operator "Retry" click is an explicit override and must work on any replica.
 *
 * <p><b>GDPR: forgotten-subject entries.</b> When the configured {@link ObjectMapper} carries
 * {@link CryptoShreddingModule} (see {@link #createObjectMapper(CryptoEngine)}), deserializing an
 * entry whose {@code @Encrypted} field belongs to a subject who was forgotten (crypto-shredded)
 * after the entry was queued does not throw — it silently substitutes {@link
 * CryptoShreddingModule#REDACTED} for that field. Replaying such a command would execute business
 * logic against the literal string {@code "[REDACTED]"} instead of the real (now-erased) PII,
 * silently corrupting domain state. {@link #processEntry} therefore checks the deserialized command
 * for the marker before executing it — see {@link #isRedacted(Command)} — and, when detected,
 * treats the entry as a failed attempt (WARN-logged) without ever calling {@link
 * CommandBus#execute}. The check walks the whole value graph: record components, collection, map,
 * optional and array elements, and the instance fields of a plain class (a command that is not a
 * record, or a plain class on the way to a nested record), inherited fields included. A record
 * component or field the check cannot read (its module does not open the package to this one, or a
 * record accessor throws) fails closed the same way when it is, or can lead to, an
 * {@code @Encrypted} field: the check cannot rule out the marker there.
 *
 * <p>Use the {@link #builder()} to configure and create instances.
 */
public final class DeadLetterRetryRunner implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DeadLetterRetryRunner.class);
  private static final int DEFAULT_BATCH_SIZE = 10;
  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  /**
   * Upper bound {@link #close()} waits to join the poll thread before proceeding. The interrupted
   * thread exits promptly in practice; this only caps a pathologically stuck poll.
   */
  private static final long CLOSE_JOIN_TIMEOUT_MS = 30_000L;

  /**
   * How many times {@code batchSize} the poll over-fetches retry-eligible rows so a page of
   * not-yet-due entries at the head cannot starve due entries behind them. The store returns rows
   * least-recently-active first (most-likely-due first), so due entries concentrate at the front of
   * this window and the poll still processes at most {@code batchSize} of them per cycle.
   */
  private static final int RETRYABLE_OVERFETCH_FACTOR = 10;

  /**
   * The single-active-consumer name this runner coordinates on. There is exactly one dead-letter
   * retry consumer per deployment, so the name is a fixed constant rather than configurable.
   */
  static final String CONSUMER_NAME = "dead-letter-retry";

  private final DeadLetterQueue deadLetterQueue;
  private final CommandBus commandBus;
  private final ObjectMapper objectMapper;
  private final DeadLetterRetryPolicy policy;
  private final int batchSize;
  private final Map<String, Class<?>> commandTypeRegistry;
  private final Clock clock;
  private final SubscriptionLeadership leadership;
  private final StreamRuneMetrics metrics;
  private final ResilientPollLoop pollLoop;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean keyedReplayUnsupportedWarned = new AtomicBoolean(false);
  private volatile boolean running;
  private volatile Thread pollThread;
  // Set false the first time countPending() throws UnsupportedOperationException (a store that does
  // not support counting), so the depth gauge degrades gracefully without retrying every cycle.
  private volatile boolean backlogSamplingSupported = true;

  private DeadLetterRetryRunner(
      DeadLetterQueue deadLetterQueue,
      CommandBus commandBus,
      ObjectMapper objectMapper,
      DeadLetterRetryPolicy policy,
      Duration pollInterval,
      int batchSize,
      Map<String, Class<?>> commandTypeRegistry,
      Clock clock,
      SubscriptionLeadership leadership,
      StreamRuneMetrics metrics) {
    this.deadLetterQueue = deadLetterQueue;
    this.commandBus = commandBus;
    this.objectMapper = objectMapper;
    this.policy = policy;
    this.batchSize = batchSize;
    this.commandTypeRegistry = Map.copyOf(commandTypeRegistry);
    this.clock = clock;
    this.leadership = leadership != null ? leadership : SubscriptionLeadership.NOOP;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
    Duration maxBackoff =
        pollInterval.compareTo(MAX_FAILURE_BACKOFF) > 0 ? pollInterval : MAX_FAILURE_BACKOFF;
    this.pollLoop =
        new ResilientPollLoop(
            "dead-letter-retry", () -> running, () -> pollInterval, pollInterval, maxBackoff);
  }

  /**
   * Starts the polling loop on a virtual thread. Not idempotent — call exactly once. A second call
   * would spawn a concurrent loop re-executing the same DLQ commands.
   *
   * @throws IllegalStateException if already started
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "DeadLetterRetryRunner already started — call start() exactly once");
    }
    running = true;
    pollThread = Thread.ofVirtual().name("streamrune-dlq-retry").start(this::runPollLoop);
  }

  @Override
  public void close() {
    running = false;
    Thread t = pollThread;
    pollThread = null;
    boolean stuck = false;
    if (t != null) {
      t.interrupt();
      // Join the poll thread before resetting `started`, so its
      // finally { running = false } cannot run AFTER a subsequent start() set running = true —
      // which would silently kill the restarted runner. Self-join guard: a thread cannot join
      // itself (close() invoked from within the poll thread, e.g. a shutdown triggered inside the
      // poll callback).
      if (t != Thread.currentThread()) {
        try {
          t.join(CLOSE_JOIN_TIMEOUT_MS);
        } catch (InterruptedException _) {
          Thread.currentThread().interrupt();
        }
        // The join timed out and the poll thread is still alive. Do NOT reset `started`
        // — a restart would run a SECOND poll thread whose stale finally { running = false } then
        // silently kills the restarted runner. Stay not-restartable and log loudly instead.
        stuck = t.isAlive();
      }
    }
    // Resign leadership so a healthy standby replica can take over immediately instead of waiting
    // for process exit / context shutdown to release the advisory lock — mirrors the projection
    // runner's resign-on-exit. NOOP.resign is a no-op, so single-instance behavior is unchanged.
    // The SubscriptionLeadership contract requires resign to be no-throw for transient
    // failures, but close() must complete its lifecycle bookkeeping (the stuck warning and the
    // restartability reset below) even against a contract-breaking implementation — a resign throw
    // used to escape close() into the shutting-down lifecycle adapter and leave the runner
    // permanently not-restartable.
    try {
      leadership.resign(CONSUMER_NAME);
    } catch (RuntimeException resignFailure) {
      LOG.warn(
          "DeadLetterRetryRunner could not resign leadership on close; the lease will expire"
              + " naturally at its TTL",
          resignFailure);
    }
    if (stuck) {
      LOG.warn(
          "DeadLetterRetryRunner poll thread did not stop within {}ms; leaving the runner"
              + " not-restartable so a restart cannot duplicate or silently kill it. Investigate the"
              + " stuck poll.",
          CLOSE_JOIN_TIMEOUT_MS);
    } else {
      // Reset the started guard so start() can be called again on the same instance after
      // close() — matches the restartable lifecycle contract (actuator restart, context refresh).
      started.set(false);
    }
  }

  /** Returns whether the polling thread is currently active. */
  public boolean isRunning() {
    return running;
  }

  /**
   * Returns whether {@link #start()} has been called and {@link #close()} has not (the runner is
   * expected to be running). Combined with {@link #isRunning()} this distinguishes a runner that
   * was never started (or was cleanly closed) from one whose poll thread <em>died</em> — {@code
   * isStarted() && !isRunning()} is the dead-relay signal the health check reports DOWN.
   */
  public boolean isStarted() {
    return started.get();
  }

  /**
   * Package-private for testing — the single-active-consumer coordinator this runner gates its poll
   * on. Defaults to {@link SubscriptionLeadership#NOOP} (always leader) when none is configured.
   */
  SubscriptionLeadership leadership() {
    return leadership;
  }

  /**
   * Number of consecutive poll-cycle failures; {@code 0} when the last cycle succeeded. A non-zero
   * value means the runner is degraded — still running, retrying with backoff.
   */
  public int consecutiveFailures() {
    return pollLoop.consecutiveFailures();
  }

  private void runPollLoop() {
    try {
      // ResilientPollLoop logs cycle failures and retries with capped exponential backoff; it
      // exits only on close()/interrupt — a transient DLQ error never silently kills the runner.
      pollLoop.run(this::processBatch);
    } finally {
      // Keep isRunning() truthful even if the thread exits for any reason other than close().
      running = false;
    }
  }

  /**
   * Retries a single dead-letter entry on demand — e.g. an operator clicking "Retry" in an admin UI
   * — bypassing the per-entry backoff window that the scheduled poll honours. The command is
   * re-dispatched through the bus under {@code DLQ_REPLAY} and the entry's original request
   * context, then discarded on success or have its attempt count bumped on failure, exactly like
   * {@link #processBatch()}. This is a genuine re-execution, not a discard.
   *
   * <p><b>The replay never runs as the caller.</b> It executes on a fresh virtual thread, which
   * this method waits for, so it sees exactly what a scheduled replay sees on the poll thread: the
   * context recorded on the entry, or no context at all when the entry recorded none. Nothing the
   * calling thread has bound reaches it — not the operator's {@link StreamRuneContext#CURRENT}
   * request context (user, correlation, trace, baggage, captured authority), not a saga-dispatch
   * marker, not an inheritable thread-local such as a security context propagated to child threads
   * — so the replayed events and audit row never name the operator as the actor of a command they
   * did not issue. An interrupt of the caller does not abandon the replay: the method still waits
   * for its outcome and returns with the interrupt status set.
   *
   * @param commandId the command id of the entry to retry
   * @return {@code true} if a matching entry was found and re-dispatched (regardless of whether the
   *     command itself then succeeded — that outcome is reflected in the entry's state and logs);
   *     {@code false} if no entry matched the id
   * @throws RuntimeException the failure to record the replay's outcome on the entry (a dead letter
   *     store error), rethrown on the calling thread
   */
  public boolean retry(CommandId commandId) {
    Optional<DeadLetterQueue.DeadLetterEntry> entry = deadLetterQueue.find(commandId);
    if (entry.isEmpty()) {
      return false;
    }
    processEntryOffTheCallersThread(entry.get());
    return true;
  }

  /**
   * Runs {@link #processEntry} on a fresh virtual thread and waits for it. {@link ScopedValue}
   * bindings never cross into a new thread, and the thread is built without inheriting inheritable
   * thread-locals; it keeps the caller's context class loader so application classes resolve as
   * they do for the caller. The wait ignores interrupts, because returning early would report
   * nothing about a replay that is still running; the interrupt status is restored afterwards. A
   * failure escaping {@code processEntry} (the dead letter store refusing the attempt update or the
   * discard) is rethrown here, as it was when the replay ran on the caller's thread.
   *
   * <p>No durable state is added: the entry is the only record, and {@code processEntry} writes it
   * exactly as on the poll path. A crash at any point — before the thread starts, during the
   * command, between the command's commit and the discard — leaves the entry in place, and the
   * rerun replays it under the same idempotency key, which the command inbox collapses into a no-op
   * when the first run had committed.
   */
  private void processEntryOffTheCallersThread(DeadLetterQueue.DeadLetterEntry entry) {
    Throwable[] failure = new Throwable[1];
    Thread replay =
        Thread.ofVirtual()
            .name("streamrune-dlq-retry-on-demand")
            .inheritInheritableThreadLocals(false)
            .unstarted(
                () -> {
                  try {
                    processEntry(entry);
                  } catch (RuntimeException | Error e) {
                    failure[0] = e;
                  }
                });
    replay.setContextClassLoader(Thread.currentThread().getContextClassLoader());
    replay.start();
    boolean interrupted = false;
    while (true) {
      try {
        replay.join();
        break;
      } catch (InterruptedException _) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (failure[0] instanceof RuntimeException e) {
      throw e;
    }
    if (failure[0] instanceof Error e) {
      throw e;
    }
  }

  /** Package-private for testing — processes one batch of retryable entries. */
  void processBatch() {
    // Sample observability signals every cycle, on EVERY replica and BEFORE the
    // leadership gate — the DLQ depth is a global read-only count and the degradation gauge is this
    // replica's own liveness, so both must update regardless of who leads.
    sampleObservability();
    // Single-active-consumer gate: only the leader for this name polls and retries. A non-leader
    // returns before reading a single entry — no readRetryable side effects, no execute — so two
    // replicas' staggered polls can never both re-execute the same DLQ command (which, without a
    // CommandInbox, would emit duplicate domain events). With SubscriptionLeadership.NOOP (the
    // single-instance default) tryAcquire always returns a lease, so this is a no-op guard. Its
    // writes are command-inbox idempotent, so no fencing epoch is threaded — the lease gate alone
    // suffices.
    if (leadership.tryAcquire(CONSUMER_NAME).isEmpty()) {
      return;
    }
    // readRetryable filters only by attempts (dlq_attempts < maxRetries) and is
    // batch-bounded, while due-ness (per-entry exponential backoff) is evaluated here in Java. If
    // we read exactly `batchSize` rows, a page full of not-yet-due entries at the head — e.g. the
    // most-attempted, longest-backoff entries under a backlog — would fill the page and starve
    // genuinely-due entries behind them, poll after poll. Over-fetch a wider window (the store
    // orders it least-recently-active first, i.e. most-likely-due first) and process up to
    // `batchSize` entries that are actually DUE, skipping the not-due ones instead of letting them
    // consume the batch budget. This bounds the read while keeping the per-cycle retry throughput
    // at batchSize.
    int fetchLimit = overfetchLimit(batchSize);
    var entries = deadLetterQueue.readRetryable(policy.maxRetries(), fetchLimit);
    Instant now = clock.instant();
    int processed = 0;
    for (var entry : entries) {
      if (processed >= batchSize) {
        break;
      }
      // The gate above checks leadership ONCE per batch and discards the acquired
      // lease. A batch that outlives this replica's lease TTL (e.g. paused by GC between the
      // acquire and here) must not keep processing entries oblivious to a fresh leader having
      // already taken over — re-check before EACH entry (SubscriptionLeadership#current is a
      // local check, not a re-acquisition round trip per its contract) and stop the moment this
      // replica is no longer the leader, leaving the rest of the batch for whoever is.
      if (leadership.current(CONSUMER_NAME).isEmpty()) {
        LOG.warn(
            "Lost leadership for '{}' mid-batch; stopping before processing the remaining entries"
                + " this cycle",
            CONSUMER_NAME);
        break;
      }
      if (isDue(entry, now)) {
        processEntry(entry);
        processed++;
      }
    }
  }

  /**
   * Reports the command DLQ depth gauge ({@code streamrune.dlq.pending}) and this runner's
   * degradation gauge ({@code streamrune.dlq.retry.consecutive_failures}) each poll cycle — the
   * command-path analog of {@code OutboxPoller.sampleBacklog}. Skipped entirely when no metrics are
   * wired, and the depth sample is disabled after the first {@link UnsupportedOperationException}
   * from a store that does not support {@link DeadLetterQueue#countPending()}. A transient count
   * failure is logged and swallowed so it never kills the retry cycle; the degradation gauge (from
   * the resilient poll loop) still reflects it.
   */
  private void sampleObservability() {
    if (metrics == StreamRuneMetrics.NOOP) {
      return;
    }
    // Guarded like its sibling below — a throwing backend must not abort the cycle.
    recordMetric(
        "recordDlqRetryDegradation",
        () -> metrics.recordDlqRetryDegradation(consecutiveFailures()));
    if (!backlogSamplingSupported) {
      return;
    }
    try {
      metrics.recordDlqBacklog(deadLetterQueue.countPending());
    } catch (UnsupportedOperationException _) {
      backlogSamplingSupported = false;
      LOG.debug(
          "DeadLetterQueue {} does not support countPending(); command DLQ depth gauge disabled",
          deadLetterQueue.getClass().getSimpleName());
    } catch (RuntimeException e) {
      // A transient count failure must not abort the retry cycle — the gauge simply holds its last
      // value this cycle; the degradation gauge above still surfaces sustained trouble.
      LOG.warn(
          "Failed to sample command DLQ depth this cycle: {}",
          LogSanitizer.sanitizeFreeText(e.getMessage()));
    }
  }

  /**
   * Runs one metrics call, logging and swallowing any {@link RuntimeException} from the backend.
   * {@link StreamRuneMetrics} is a pluggable sink — a Micrometer implementation registers meters
   * lazily and can throw against a closed or misconfigured registry — and a failure there must
   * never abort a poll cycle or a terminal state transition. Same guard the projection runners,
   * {@code PollingEventSubscription} and {@code SagaTimeoutRunner} apply.
   */
  private void recordMetric(String metricName, Runnable call) {
    try {
      call.run();
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for {}", metricName, e);
    }
  }

  /**
   * The number of retry-eligible rows to over-fetch so a page of not-yet-due entries at the head
   * cannot hide due entries behind them. {@code batchSize * RETRYABLE_OVERFETCH_ FACTOR},
   * saturating at {@link Integer#MAX_VALUE} rather than overflowing.
   */
  private static int overfetchLimit(int batchSize) {
    long limit = (long) batchSize * RETRYABLE_OVERFETCH_FACTOR;
    return (int) Math.min(limit, Integer.MAX_VALUE);
  }

  /**
   * Returns whether the entry's per-entry backoff window has elapsed. The first retry is due {@code
   * delayForAttempt(1)} after the entry was published; each subsequent retry is due {@code
   * delayForAttempt(dlqAttempts + 1)} after the previous attempt. The policy's delay carries
   * jitter, so entries that failed together drift apart instead of retrying in lockstep.
   */
  private boolean isDue(DeadLetterQueue.DeadLetterEntry entry, Instant now) {
    Instant lastActivity =
        entry.lastAttemptAt() != null ? entry.lastAttemptAt() : entry.publishedAt();
    Duration backoff = policy.delayForAttempt(entry.dlqAttempts() + 1);
    return !now.isBefore(lastActivity.plus(backoff));
  }

  private void processEntry(DeadLetterQueue.DeadLetterEntry entry) {
    Class<?> commandClass = resolveCommandClass(entry.commandType());
    if (commandClass == null) {
      LOG.warn(
          "Unknown command type '{}' for DLQ entry {}, skipping",
          entry.commandType(),
          LogSanitizer.sanitizeForLog(entry.commandId().value()));
      // Still a failed attempt: without this, an unresolvable command type never ages toward
      // discard-per-policy, and since readRetryable() is oldest-first and bounded by batchSize,
      // enough unresolvable entries would permanently occupy the batch (head-of-queue
      // starvation). Nothing is executed for an unresolvable type either way.
      recordFailedAttempt(
          entry, new IllegalStateException("Unknown command type: " + entry.commandType()));
      return;
    }
    Command command;
    try {
      command = (Command) objectMapper.readValue(entry.commandPayload(), commandClass);
    } catch (Exception e) {
      recordFailedAttempt(entry, e);
      return;
    }
    if (command == null) {
      // The bus records a command it could not serialize as metadata only (a JSON null payload).
      // There is nothing to replay; age the entry like any other unreplayable one so it cannot
      // hold a slot of the oldest-first batch forever.
      LOG.warn(
          "DLQ entry {} ({}) was recorded metadata only — its payload could not be serialized when"
              + " it was dead-lettered — and cannot be replayed",
          LogSanitizer.sanitizeForLog(entry.commandId().value()),
          entry.commandType());
      recordFailedAttempt(
          entry, new IllegalStateException("No payload recorded (metadata-only entry)"));
      return;
    }
    boolean redacted;
    try {
      redacted = isRedacted(command);
    } catch (UnverifiableRedactionException e) {
      // A record component or field that is, or can lead to, an @Encrypted
      // field could not be read, so the scan cannot rule out a crypto-shredded "[REDACTED]" value.
      // Treating that as "not redacted" is the GDPR fail-open this check exists to prevent, so the
      // command must not reach the bus. Still a failed attempt: the entry ages toward exhaustion
      // per policy like any other entry that cannot be replayed. No new write: this is the existing
      // failed-attempt path. Convergence does not rest on the scan being deterministic (a user
      // record accessor need not be): on every run, the command is dispatched only if the scan
      // completes without finding the marker, and a crash before updateAttempts leaves the row
      // unchanged for a rerun that applies the same rule.
      LOG.warn(
          "DLQ entry {} ({}) cannot be checked for crypto-shredded PII — {}. Not replaying it.",
          LogSanitizer.sanitizeForLog(entry.commandId().value()),
          entry.commandType(),
          LogSanitizer.sanitizeFreeText(e.getMessage()));
      recordFailedAttempt(entry, e);
      return;
    }
    if (redacted) {
      // The subject was forgotten (crypto-shredded) while this entry sat in the DLQ:
      // CryptoShreddingModule decrypted the @Encrypted field to CryptoShreddingModule.REDACTED
      // instead of throwing (replay of past data must keep working). Executing the command now
      // would run business logic against the literal string "[REDACTED]" in place of the erased
      // PII, silently corrupting domain state — so it must never reach the command bus. Still a
      // failed attempt (ages toward discard-per-policy like any other permanently unresolvable
      // entry), never a silent, forever-retried ghost.
      LOG.warn(
          "DLQ entry {} ({}) carries an @Encrypted field redacted by crypto-shredding — the"
              + " subject was forgotten while this command was queued for retry. Discarding the"
              + " replay attempt instead of executing a command with redacted PII.",
          LogSanitizer.sanitizeForLog(entry.commandId().value()),
          entry.commandType());
      recordFailedAttempt(
          entry,
          new IllegalStateException(
              "Command carries a crypto-shredded (forgotten-subject) field and cannot be"
                  + " faithfully replayed: "
                  + entry.commandType()));
      return;
    }
    // Holds the replay's CommandResult so processEntry can inspect the outcome (VETOED vs
    // NONE/IDEMPOTENT_REPLAY) after the scoped-value carrier.run() returns.
    CommandBus.CommandResult[] replayResult = new CommandBus.CommandResult[1];
    try {
      // Replay under the ORIGINAL client key when the entry carries one (a command that ran via
      // execute(command, IdempotencyKey)). The original key is the single
      // dedup identity that reconciles BOTH a cross-instance duplicate DLQ replay AND the client's
      // own at-least-once redelivery of the same command — both short-circuit at the same
      // CommandInbox row, so the events are produced exactly once. Replaying under a synthetic key
      // instead would let the client's redelivery (under the original key) miss the inbox and
      // re-execute the handler, duplicating the committed domain events.
      //
      // Fall back to a deterministic key derived from the commandId only when the command ran
      // unkeyed (no original key persisted): the FOR UPDATE SKIP LOCKED claim on a DLQ entry is
      // released before execute() runs, so two instances (or two poll cycles) can both pick up the
      // same entry; presenting this key still lets the CommandInbox collapse a cross-instance
      // duplicate replay into a no-op hit instead of a second real execution.
      IdempotencyKey key =
          entry.idempotencyKey() != null
              ? entry.idempotencyKey()
              // Derived from the STORED command id, so built through the decode door (the
              // canonical constructor), not the ingress factory that refuses control characters.
              : new IdempotencyKey("dlq-replay:" + entry.commandId());
      // DLQ_REPLAY tells the bus this execution IS a DLQ retry — a failure must not be
      // re-published as a fresh DLQ entry (retry storm); attempts are tracked on this entry.
      ScopedValue.Carrier carrier =
          ScopedValue.where(VirtualThreadCommandBus.DLQ_REPLAY, Boolean.TRUE);
      // Rebind the original request context persisted on the entry. Without it the replay runs
      // anonymous: events lose correlation and fail-closed authorization rejects @RequireRole
      // commands that originally passed. A context is bound only when the entry carries a
      // correlation id (RequestContext requires one); an entry for a command that ran without a
      // bound context replays unbound. That holds for retry() too: it runs this method on a fresh
      // thread, so the caller's own binding is never there to inherit.
      RequestContext replayContext = replayContextFrom(entry);
      if (replayContext != null) {
        carrier = carrier.where(StreamRuneContext.CURRENT, replayContext);
      }
      // Capture the replay's CommandResult so a VETO (a rejection — see below) is not misread as a
      // successful retry. run() cannot return a value out of the scoped-value scope, so a
      // one-element holder carries it out.
      carrier.run(() -> replayResult[0] = executeReplay(command, key));
    } catch (Exception e) {
      if (hasAdmissionRefusalCause(e)) {
        // A CommandBusClosedException OR a
        // CommandBusOverloadedException means the replay was REFUSED ADMISSION — nothing was
        // attempted, nothing was decided, no infrastructure was touched (a pure "later"
        // signal; both causes are the identical shape). Counting it as a failed
        // attempt would let shutdown/overload windows age entries toward the terminal transition —
        // under discardAfterMaxRetries a bouncing or saturated node could permanently DROP a
        // command via refusals alone. Skip the count AND the terminal transition; the entry stays
        // untouched for the next run (this node restarted or drained, or another node's runner).
        LOG.info(
            "Skipping DLQ replay of {} ({}) — the command bus refused admission (shutting down, or"
                + " its in-flight async budget is exhausted); the entry stays untouched for the"
                + " next run.",
            LogSanitizer.sanitizeForLog(entry.commandId().value()),
            entry.commandType());
        return;
      }
      recordFailedAttempt(entry, e);
      return;
    }
    // executeCounted RETURNS (never throws) a VETOED CommandResult when a
    // user CommandInterceptor's before() returns false (maintenance mode / tenant-suspended /
    // kill-switch / rate-limit — a supported feature). A veto is a REJECTION: the command never ran
    // and produced no events. Discarding the entry here (discardAfterSuccess) would silently drop a
    // legitimate business operation and log "DLQ retry succeeded". Treat it as a failed attempt so
    // the entry is retained and ages toward discard-per-policy exactly like any other failure. A
    // NONE (the command actually executed) or IDEMPOTENT_REPLAY (a success whose events were
    // already persisted by the original keyed execution) result is a genuine success and still
    // falls through to discardAfterSuccess.
    CommandBus.CommandResult result = replayResult[0];
    if (result != null && result.vetoed()) {
      LOG.warn(
          "DLQ replay of {} ({}) was VETOED by interceptor '{}' — the command did not run and"
              + " produced no events. Keeping the entry and bumping its attempt count instead of"
              + " discarding it as a successful retry.",
          LogSanitizer.sanitizeForLog(entry.commandId().value()),
          entry.commandType(),
          result.shortCircuitedBy());
      recordFailedAttempt(
          entry,
          new IllegalStateException(
              "DLQ replay vetoed by interceptor '"
                  + result.shortCircuitedBy()
                  + "' — command not executed"));
      return;
    }
    discardAfterSuccess(entry);
  }

  /**
   * Returns whether any {@code @Encrypted} record component reachable from {@code command} holds
   * {@link CryptoShreddingModule#REDACTED} — the tombstone {@link CryptoShreddingModule}
   * substitutes for a field whose subject's key has been deleted (GDPR forget), instead of
   * throwing. The command may be a record or a plain class: {@link #containsRedactedValue} walks
   * either.
   *
   * <p>This is the simplest robust generic signal available at this layer: {@link
   * CryptoShreddingModule} deliberately never throws on a forgotten-subject decrypt (event/command
   * replay must keep working), so no exception distinguishes "redacted" from "decrypted fine" — the
   * marker value itself is the only signal. Scanning the deserialized command's own
   * {@code @Encrypted} fields (rather than re-serializing and inspecting the JSON) avoids a second
   * round trip through the crypto engine — re-encrypting the marker string would itself throw for a
   * terminally-erased subject on every production {@code CryptoEngine} implementation, which would
   * make re-serialization unusable as a detection mechanism. A command type with no
   * {@code @Encrypted} components (or built with a crypto-blind mapper) can never match, so this is
   * a no-op for the common case.
   *
   * @throws UnverifiableRedactionException when a record component or field that is, or can lead
   *     to, an {@code @Encrypted} field cannot be read; the caller must then not dispatch the
   *     command
   */
  private static boolean isRedacted(Command command) throws UnverifiableRedactionException {
    return containsRedactedValue(
        command, java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
  }

  /**
   * Walks one value of the deserialized command's VALUE graph: a record through its components
   * ({@link #containsRedactedRecord}), a collection / map / map entry / optional / atomic reference
   * / array through its elements, and any other application object through its instance fields
   * ({@link #containsRedactedFields}). {@link CryptoShreddingModule} redacts {@code @Encrypted}
   * fields at ANY depth — its per-bean deserializer modifier fires on every record Jackson builds,
   * a record held in a plain class's field included — exactly as {@code CryptoConfigValidator.scan}
   * mirrors on the TYPE graph. Stopping at the first value that is not a record (a command that is
   * a plain class, or a plain class between the command and a nested record) would miss a {@code
   * [REDACTED]} field and replay a command with erased PII in place. The identity {@code visited}
   * set guards against cyclic and shared references, which a plain class can form through a Jackson
   * back-reference.
   *
   * <p>{@code @Encrypted} targets record components only, so a plain class never holds the marker
   * itself; it can only lead to a record that does. A JDK/platform value, an enum constant and a
   * primitive array hold no record Jackson built from the payload, so the walk stops there and
   * never reads JDK internals.
   */
  private static boolean containsRedactedValue(Object value, java.util.Set<Object> visited)
      throws UnverifiableRedactionException {
    if (value == null) {
      return false;
    }
    if (value.getClass().isRecord()) {
      return containsRedactedRecord(value, visited);
    }
    if (value instanceof java.util.Collection<?> collection) {
      return anyContainsRedacted(collection, visited);
    }
    if (value instanceof java.util.Map<?, ?> map) {
      return anyContainsRedacted(map.values(), visited);
    }
    if (value instanceof java.util.Optional<?> optional) {
      return optional.isPresent() && containsRedactedValue(optional.get(), visited);
    }
    if (value instanceof java.util.concurrent.atomic.AtomicReference<?> reference) {
      // Jackson builds an AtomicReference's content with the bean deserializer, so a record
      // inside one is redacted like any other; the platform-type leaf rule below would skip it.
      return containsRedactedValue(reference.get(), visited);
    }
    if (value instanceof java.util.Map.Entry<?, ?> entry) {
      // The same for a Map.Entry's value (an AbstractMap.SimpleEntry at run time). Its key, like a
      // map's, comes from a key deserializer, never the bean deserializer, so it is never redacted.
      return containsRedactedValue(entry.getValue(), visited);
    }
    if (value instanceof Object[] array) {
      return anyContainsRedacted(java.util.Arrays.asList(array), visited);
    }
    if (value instanceof Enum<?> || isPlatformType(value.getClass())) {
      return false;
    }
    return containsRedactedFields(value, visited);
  }

  private static boolean anyContainsRedacted(Iterable<?> values, java.util.Set<Object> visited)
      throws UnverifiableRedactionException {
    for (Object element : values) {
      if (containsRedactedValue(element, visited)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks a record's own {@code @Encrypted} components for the marker and walks every other
   * component.
   *
   * <p><b>A component that cannot be read.</b> Jackson builds a command record that is not public,
   * or whose package is not exported to this module, by overriding access, so reading its
   * components back needs the same override: {@link #readComponent} grants it first. A component
   * can still be unreadable — its module does not open the package to this one, or its accessor
   * throws. Such a component goes to {@link #failClosedIfItCanHoldPii}.
   */
  private static boolean containsRedactedRecord(Object obj, java.util.Set<Object> visited)
      throws UnverifiableRedactionException {
    if (!visited.add(obj)) {
      return false;
    }
    for (var component : obj.getClass().getRecordComponents()) {
      boolean encrypted =
          component.getAnnotation(org.streamrune.core.crypto.Encrypted.class) != null;
      Object value;
      try {
        value = readComponent(obj, component);
      } catch (ReflectiveOperationException | RuntimeException e) {
        failClosedIfItCanHoldPii(
            obj.getClass(),
            "component",
            component.getName(),
            component.getGenericType(),
            encrypted,
            e);
        continue;
      }
      if (encrypted) {
        if (CryptoShreddingModule.REDACTED.equals(value)) {
          return true;
        }
      } else if (containsRedactedValue(value, visited)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Reads one record component, granting this module access to the accessor first when it lacks it.
   * {@code trySetAccessible} succeeds wherever the record's module opens its package to this one,
   * which is always the case on the class path; where it does not, {@code invoke} throws {@link
   * IllegalAccessException} and the caller decides whether that is fatal to the scan.
   */
  private static Object readComponent(Object owner, RecordComponent component)
      throws ReflectiveOperationException {
    Method accessor = component.getAccessor();
    if (!accessor.canAccess(owner)) {
      accessor.trySetAccessible();
    }
    return accessor.invoke(owner);
  }

  /**
   * Walks every instance field of a plain (non-record) object, the fields it inherits included:
   * Jackson fills a private field declared on a superclass through its setter just as it fills the
   * class's own, so the walk climbs the superclass chain until it reaches a JDK/platform class.
   * Static and synthetic fields are not part of the value. A field is read directly rather than
   * through a getter, so no application code runs during the scan.
   *
   * <p><b>A field that cannot be read.</b> Access is granted first, as for a record component; a
   * field stays unreadable only where its module does not open the package to this one. Such a
   * field goes to {@link #failClosedIfItCanHoldPii}, judged by its declared type.
   */
  private static boolean containsRedactedFields(Object obj, java.util.Set<Object> visited)
      throws UnverifiableRedactionException {
    if (!visited.add(obj)) {
      return false;
    }
    for (Class<?> type = obj.getClass();
        type != null && !isPlatformType(type);
        type = type.getSuperclass()) {
      for (Field field : type.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
          continue;
        }
        Object value;
        try {
          value = readField(obj, field);
        } catch (ReflectiveOperationException | RuntimeException e) {
          failClosedIfItCanHoldPii(
              type, "field", field.getName(), field.getGenericType(), false, e);
          continue;
        }
        if (containsRedactedValue(value, visited)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Reads one instance field, granting this module access first when it lacks it. As with {@link
   * #readComponent}, {@code trySetAccessible} succeeds wherever the declaring module opens the
   * package to this one; where it does not, {@code get} throws {@link IllegalAccessException}.
   */
  private static Object readField(Object owner, Field field) throws IllegalAccessException {
    if (!field.canAccess(owner)) {
      field.trySetAccessible();
    }
    return field.get(owner);
  }

  /**
   * The fail-closed rule for a record component or field the scan could not read: an
   * {@code @Encrypted} component, or a member whose declared type reaches an {@code @Encrypted}
   * field ({@link CryptoConfigValidator#referencesEncrypted}), may hold {@code [REDACTED]}, so the
   * scan fails closed with {@link UnverifiableRedactionException} rather than guessing "not
   * redacted". Any other unreadable member can never hold the marker and the scan skips it; normal
   * execution surfaces whatever made it unreadable. A reachability walk that itself fails (a
   * malformed {@code @Encrypted} declaration on the way) cannot show the member is free of PII, so
   * it counts as reaching one.
   */
  private static void failClosedIfItCanHoldPii(
      Class<?> owner,
      String kind,
      String name,
      Type declaredType,
      boolean encrypted,
      Exception cause)
      throws UnverifiableRedactionException {
    if (encrypted || mayLeadToEncrypted(declaredType)) {
      throw new UnverifiableRedactionException(owner, kind, name, encrypted, cause);
    }
  }

  private static boolean mayLeadToEncrypted(Type declaredType) {
    try {
      return CryptoConfigValidator.referencesEncrypted(declaredType);
    } catch (RuntimeException _) {
      return true;
    }
  }

  /**
   * Whether {@code type} is a JDK/platform class (loaded by the bootstrap loader, or in a {@code
   * java.}, {@code javax.}, {@code jakarta.}, {@code jdk.}, {@code sun.} or {@code com.sun.}
   * package) — the same boundary {@link CryptoConfigValidator#referencesEncrypted} stops at. Such a
   * class never declares a field holding a record Jackson built from the payload, other than the
   * containers {@link #containsRedactedValue} opens itself.
   */
  private static boolean isPlatformType(Class<?> type) {
    if (type.getClassLoader() == null) {
      return true;
    }
    String name = type.getPackageName();
    return name.startsWith("java.")
        || name.startsWith("javax.")
        || name.startsWith("jakarta.")
        || name.startsWith("jdk.")
        || name.startsWith("sun.")
        || name.startsWith("com.sun.");
  }

  /**
   * The redaction scan could not read a record component or field that is, or can lead to, an
   * {@code @Encrypted} field, so it cannot rule out a crypto-shredded value. {@link #processEntry}
   * answers it by not dispatching the command and recording a failed attempt. The message names the
   * declaring type, the member and the failure's type; it never carries a value.
   */
  private static final class UnverifiableRedactionException extends Exception {

    UnverifiableRedactionException(
        Class<?> owner, String kind, String name, boolean encrypted, Throwable cause) {
      super(
          "cannot read "
              + (encrypted ? "@Encrypted " : "")
              + kind
              + " "
              + owner.getName()
              + "."
              + name
              + (encrypted ? "" : ", whose type can hold an @Encrypted field,")
              + " to check it for crypto-shredded PII ("
              + failureName(cause)
              + ")",
          cause);
    }

    /** The accessor's own exception when it threw, else the reflective failure itself. */
    private static String failureName(Throwable cause) {
      Throwable reason =
          cause instanceof InvocationTargetException ite && ite.getCause() != null
              ? ite.getCause()
              : cause;
      return reason.getClass().getSimpleName();
    }
  }

  /**
   * Executes a replay via the keyed overload when the bus reports {@link
   * CommandBus#supportsIdempotentExecution()}, so a cross-instance duplicate replay short-circuits
   * at the command inbox instead of re-running the handler. Otherwise executes the unkeyed {@link
   * CommandBus#execute(Command)} directly — no keyed call is attempted first, so a bus without an
   * inbox never throws and never has a chance to be double-executed. The unkeyed path is logged at
   * WARN exactly once per runner instance to avoid log spam on every subsequent replay.
   *
   * @return the bus's {@link CommandBus.CommandResult}. The caller MUST inspect {@link
   *     CommandBus.CommandResult#vetoed()}: a VETOED result is a rejection (the command never ran),
   *     NOT a successful retry, so it must route to {@code recordFailedAttempt} — see the veto
   *     handling in {@link #processEntry}.
   */
  private CommandBus.CommandResult executeReplay(Command command, IdempotencyKey key) {
    if (commandBus.supportsIdempotentExecution()) {
      return commandBus.execute(command, key);
    } else {
      if (keyedReplayUnsupportedWarned.compareAndSet(false, true)) {
        LOG.warn(
            "CommandBus does not support idempotency keys; DLQ replay runs WITHOUT cross-instance"
                + " de-duplication. This warning is logged once.");
      }
      return commandBus.execute(command);
    }
  }

  /**
   * Builds the request context to rebind for a replay from the context fields persisted on the
   * entry, or returns {@code null} when no correlation id was persisted (a {@link RequestContext}
   * requires one). The context's timestamp reuses the entry's {@code firstAttemptAt} — the
   * request-creation instant analog — so the replay carries the original request's clock, not the
   * replay's; the bus stamps event timestamps from its own clock regardless.
   *
   * <p><b>This is a RECONSTRUCTION, not an ingress.</b> Every value here was read back out of
   * {@code dead_letter_queue}; none of it was read off a socket. It therefore uses the plain {@link
   * RequestContext} constructor and NOT {@code RequestContext.fromRequest}, whose {@code
   * IdConstraints} bound belongs only where an unauthenticated header first enters the system.
   * Applying that bound here would reject persisted ids that never crossed that door (a command
   * that ran under a context built with the plain constructor, or under a saga-derived correlation
   * id), and because the failure is deterministic it would burn the entry's entire retry ladder and
   * permanently discard an already-authorized business command — the loss class the authorization
   * interceptor's off-request rule exists to prevent, through a different door. The ids are rebound
   * VERBATIM: truncating or scrubbing one would make the replay carry a different correlation id
   * from the events the original attempt already produced.
   */
  private static RequestContext replayContextFrom(DeadLetterQueue.DeadLetterEntry entry) {
    if (entry.correlationId() == null) {
      return null;
    }
    // The five-argument constructor leaves
    // RequestContext.authority() null, and that is deliberate — the DLQ persists identity
    // (user/correlation/trace) but NEVER a resolved role set. Persisting one would let a role
    // grant outlive the process that resolved it, be replayed from a database row hours after the
    // user's roles changed, and be forged by anything that can write the table. A replay whose
    // resolver cannot answer off-request is instead completed under the RECORDED DLQ-replay system
    // principal by AnnotationAuthorizationInterceptor; a resolver that IS a pure function of the
    // user id is still consulted here and can still deny a revoked user.
    return new RequestContext(
        entry.traceId(), entry.userId(), entry.correlationId(), entry.firstAttemptAt(), null);
  }

  /**
   * Resolves a DLQ entry's {@code commandType} — the fully-qualified class name the bus persists —
   * to a registered command class.
   *
   * <p>{@link Builder#registerCommand(Class)} is the only way into the registry and keys every
   * class by its {@code getName()}, so the persisted type and the key are produced by the same
   * function and the lookup is one exact, collision-free map access. No other match is attempted: a
   * type that names no registered class is unresolvable, and the caller records a failed attempt
   * without executing anything.
   *
   * @return the resolved class, or {@code null} when the type matches nothing registered
   */
  private Class<?> resolveCommandClass(String commandType) {
    return commandTypeRegistry.get(commandType);
  }

  /**
   * Renders a persisted {@code commandType} (the fully-qualified class name) as the SIMPLE class
   * name for metric tagging, so {@code streamrune.dlq.exhausted}'s command-type tag matches {@code
   * streamrune.dlq.published}'s — which {@code VirtualThreadCommandBus} records with {@link
   * Class#getSimpleName()} and which {@code MicrometerStreamRuneMetrics} tags by {@code
   * command.type} (before that the Micrometer implementation discarded the published side's type,
   * so this parity existed only at the SPI and the join could not be written). Strips everything up
   * to the last {@code '.'} or {@code '$'}, mirroring {@code getSimpleName()} for top-level and
   * nested classes; a value that carries no separator is returned unchanged.
   */
  private static String simpleTypeName(String commandType) {
    if (commandType == null) {
      return null;
    }
    int cut = Math.max(commandType.lastIndexOf('.'), commandType.lastIndexOf('$'));
    return cut >= 0 ? commandType.substring(cut + 1) : commandType;
  }

  /**
   * True when {@code t} or any exception in its (bounded) cause chain is a {@link
   * CommandBusClosedException} (the bus's admission refusal during shutdown) or a {@link
   * CommandBusOverloadedException} (the bus's {@code executeAsync} in-flight admission budget
   * exhausted) — the same ADMISSION-refusal shape for two different causes. The chain is walked
   * because the refusal may surface wrapped by a dispatch adapter; bounded so a
   * self-referential/cyclic chain terminates, matching the family's other cause walks.
   */
  private static boolean hasAdmissionRefusalCause(Throwable t) {
    Throwable c = t;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof CommandBusClosedException || c instanceof CommandBusOverloadedException) {
        return true;
      }
    }
    return false;
  }

  private void recordFailedAttempt(DeadLetterQueue.DeadLetterEntry entry, Exception e) {
    int newAttempts = entry.dlqAttempts() + 1;
    deadLetterQueue.updateAttempts(entry.commandId(), newAttempts, clock.instant());
    if (newAttempts >= policy.maxRetries()) {
      if (entry.dlqAttempts() >= policy.maxRetries()) {
        // An operator retry of an entry that had already exhausted its retries (retained under
        // the default policy): the loss was signalled when the entry crossed maxRetries, so a
        // failed click is not another lost command and must not page as one.
        if (policy.discardAfterMaxRetries()) {
          deadLetterQueue.discard(entry.commandId());
        }
        LOG.warn(
            "Operator retry of DLQ entry {} ({}) failed (attempt {}); the entry had already"
                + " exhausted its {} retries and is {}. Last error: {}",
            LogSanitizer.sanitizeForLog(entry.commandId().value()),
            entry.commandType(),
            newAttempts,
            policy.maxRetries(),
            policy.discardAfterMaxRetries() ? "now discarded" : "kept for inspection",
            LogSanitizer.sanitizeFreeText(e.getMessage()));
        return;
      }
      // Retry-exhaustion — TERMINAL. The command has failed every DLQ retry and is permanently
      // dropped/abandoned: a lost business operation whose domain events were never produced. Emit
      // the alert-worthy metric and an ERROR log REGARDLESS of the discard policy — under the
      // default discardAfterMaxRetries=false the entry is otherwise silently aged out
      // (readRetryable filters dlq_attempts < maxRetries) with no metric, no health signal, and no
      // terminal log, so a permanently-lost command was invisible to every dashboard. Mirrors the
      // outbox terminal FAILED path (recordOutboxDeliveryFailed + ERROR). readRetryable's filter
      // means this attempt is the last poll to touch the entry, so the counter fires exactly once
      // as it crosses maxRetries.
      // Tag the exhausted counter with the SIMPLE command name so it correlates with
      // streamrune.dlq.published, which VirtualThreadCommandBus records with Class#getSimpleName()
      // and which MicrometerStreamRuneMetrics tags by command.type (until recently the
      // published side was a bare, untagged family and the join this comment promised could not be
      // written). The FQN is persisted only so DLQ REPLAY resolves the class — it is not a
      // metric-grouping key, and letting exhausted drift to the FQN silently breaks
      // published↔exhausted dashboards.
      // Guarded — this runs AFTER updateAttempts has crossed maxRetries, and
      // readRetryable never re-reads the entry, so a throwing backend must not be allowed to
      // skip the discard and the terminal ERROR below (the only remaining signal of the loss).
      recordMetric(
          "recordDeadLetterExhausted",
          () -> metrics.recordDeadLetterExhausted(simpleTypeName(entry.commandType())));
      if (policy.discardAfterMaxRetries()) {
        deadLetterQueue.discard(entry.commandId());
        LOG.error(
            "DLQ entry {} ({}) exhausted all {} retries and was DISCARDED — the command is"
                + " permanently dropped; its domain events were never produced. Last error: {}",
            LogSanitizer.sanitizeForLog(entry.commandId().value()),
            entry.commandType(),
            policy.maxRetries(),
            LogSanitizer.sanitizeFreeText(e.getMessage()));
      } else {
        LOG.error(
            "DLQ entry {} ({}) exhausted all {} retries — the command is permanently abandoned"
                + " (retained for inspection, no longer retried); its domain events were never"
                + " produced. Last error: {}",
            LogSanitizer.sanitizeForLog(entry.commandId().value()),
            entry.commandType(),
            policy.maxRetries(),
            LogSanitizer.sanitizeFreeText(e.getMessage()));
      }
    } else {
      LOG.warn(
          "DLQ retry failed for {} (attempt {}/{}): {}",
          LogSanitizer.sanitizeForLog(entry.commandId().value()),
          newAttempts,
          policy.maxRetries(),
          LogSanitizer.sanitizeFreeText(e.getMessage()));
    }
  }

  /**
   * Discards a successfully re-executed entry. A failed discard is NOT a failed command attempt —
   * the command already ran. The entry stays queued, so a later poll re-executes the command
   * (at-least-once); the attempt counter is still bumped (best effort) so a persistently failing
   * discard cannot re-execute it forever.
   */
  private void discardAfterSuccess(DeadLetterQueue.DeadLetterEntry entry) {
    try {
      deadLetterQueue.discard(entry.commandId());
      LOG.info(
          "DLQ retry succeeded for {} ({})",
          LogSanitizer.sanitizeForLog(entry.commandId().value()),
          entry.commandType());
    } catch (Exception discardFailure) {
      LOG.error(
          "DLQ retry succeeded for {} ({}) but discarding the entry failed — the command may be"
              + " executed again on a later poll",
          LogSanitizer.sanitizeForLog(entry.commandId().value()),
          entry.commandType(),
          discardFailure);
      try {
        deadLetterQueue.updateAttempts(entry.commandId(), entry.dlqAttempts() + 1, clock.instant());
      } catch (Exception updateFailure) {
        LOG.error(
            "Could not record the failed discard for {} — the entry stays on its previous retry"
                + " schedule",
            LogSanitizer.sanitizeForLog(entry.commandId().value()),
            updateFailure);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds the {@link ObjectMapper} that both the command bus's dead-letter publish path (see
   * {@code VirtualThreadCommandBus.Builder#objectMapper}) and this runner's replay/deserialize path
   * must share, so DLQ payloads round-trip: {@link JavaTimeModule} always registered, plus {@link
   * CryptoShreddingModule} when {@code cryptoEngine} is non-null so {@code @Encrypted} command
   * fields are persisted as ciphertext instead of plaintext PII, and fall within GDPR forget scope.
   * Mirrors {@code PostgresSagaStore.createObjectMapper} and {@code PostgresEventStoreFactory}'s
   * equivalent event-store wiring.
   *
   * <p>Intended for framework auto-configs (Spring/Quarkus/Micronaut) wiring up the default
   * dead-letter pipeline, and for tests exercising the crypto-aware path.
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} to build a crypto-blind
   *     mapper (DLQ payloads are then persisted with {@code @Encrypted} fields as PLAINTEXT)
   * @return a new {@link ObjectMapper}, independent from any other mapper instance
   */
  public static ObjectMapper createObjectMapper(CryptoEngine cryptoEngine) {
    return createObjectMapper(cryptoEngine, StreamRuneMetrics.NOOP);
  }

  /**
   * Same as {@link #createObjectMapper(CryptoEngine)} but threads a {@link StreamRuneMetrics}
   * collector into {@link CryptoShreddingModule} so the crypto-redaction signals ({@code
   * streamrune.crypto.subject_redacted} / systemic-failure) fire when an {@code @Encrypted} DLQ
   * command field decrypts to {@code [REDACTED]} because a key is gone. Without this the DLQ
   * replay/deserialize path builds the mapper with NOOP metrics and the alerting is silent.
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} for a crypto-blind mapper
   * @param metrics the crypto-redaction metrics collector (null → {@link StreamRuneMetrics#NOOP})
   * @return a new {@link ObjectMapper}, independent from any other mapper instance
   */
  public static ObjectMapper createObjectMapper(
      CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    if (cryptoEngine != null) {
      mapper.registerModule(
          new CryptoShreddingModule(
              cryptoEngine, metrics == null ? StreamRuneMetrics.NOOP : metrics));
    }
    return mapper;
  }

  public static final class Builder {

    private DeadLetterQueue deadLetterQueue;
    private CommandBus commandBus;
    private ObjectMapper objectMapper;
    private DeadLetterRetryPolicy policy = DeadLetterRetryPolicy.DEFAULT;
    private Duration pollInterval = Duration.ofMinutes(1);
    private int batchSize = DEFAULT_BATCH_SIZE;
    private final Map<String, Class<?>> commandTypeRegistry = new HashMap<>();
    private Clock clock = Clock.systemUTC();
    private SubscriptionLeadership leadership = SubscriptionLeadership.NOOP;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;

    public Builder deadLetterQueue(DeadLetterQueue deadLetterQueue) {
      this.deadLetterQueue = deadLetterQueue;
      return this;
    }

    public Builder commandBus(CommandBus commandBus) {
      this.commandBus = commandBus;
      return this;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public Builder policy(DeadLetterRetryPolicy policy) {
      this.policy = policy;
      return this;
    }

    public Builder pollInterval(Duration pollInterval) {
      this.pollInterval = pollInterval;
      return this;
    }

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Registers a command class the runner may deserialize a DLQ entry into.
     *
     * <p>The class is keyed by its fully-qualified name ({@link Class#getName()}) — exactly what
     * the command bus persists as the entry's {@code commandType} — so an entry resolves by one
     * exact lookup and two commands sharing a simple name across bounded contexts never collide.
     * There is deliberately no way to choose the key: a hand-picked key (e.g. the simple name)
     * could never match a persisted row. Registering the same class twice is a no-op.
     *
     * <p><b>Sealed hierarchies.</b> The bus persists the CONCRETE class of the command it ran,
     * while a decider is usually registered by its sealed command root ({@code
     * OrderCommand.class}). So when {@code commandClass} is sealed, every permitted subclass is
     * registered too, recursively through nested sealed levels: registering the root makes every
     * command of the hierarchy resolvable. The set stays closed — only classes the root itself
     * permits are added, never a class named by a row. A non-sealed supertype cannot be expanded,
     * so its concrete command classes must be registered one by one.
     *
     * <p><b>Native image.</b> The expansion reads {@link Class#getPermittedSubclasses()}, which a
     * GraalVM native image answers only for a sealed type registered for reflection; for any other
     * it reports the type sealed with NO permitted subclasses. Registering the root alone would
     * leave every concrete command of the hierarchy unresolvable at replay time, silently, so such
     * a root — at any nested level — is refused instead (see {@link SealedHierarchy}).
     *
     * @throws IllegalArgumentException if {@code commandClass} is null
     * @throws IllegalStateException if {@code commandClass}, or a sealed level below it, is sealed
     *     but its permitted subclasses cannot be read
     */
    public Builder registerCommand(Class<?> commandClass) {
      if (commandClass == null) {
        throw new IllegalArgumentException("commandClass is required");
      }
      List<Class<?>> permitted =
          SealedHierarchy.permittedSubclasses(commandClass, "dead-letter command registration");
      this.commandTypeRegistry.put(commandClass.getName(), commandClass);
      for (Class<?> subclass : permitted) {
        registerCommand(subclass);
      }
      return this;
    }

    /**
     * Sets the clock the runner reads "now" from for due-window evaluation and the timestamps it
     * stamps on retry attempts. Defaults to {@link Clock#systemUTC()}; inject a fixed or stepping
     * clock to test backoff boundaries deterministically.
     */
    public Builder clock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Sets the single-active-consumer leadership coordinator. Optional; defaults to {@link
     * SubscriptionLeadership#NOOP} (always leader), which makes multi-instance and single-instance
     * behavior identical. When a real leadership is injected, only the leader for the fixed
     * dead-letter-retry consumer name polls and retries; non-leaders skip the poll entirely. The
     * on-demand {@link DeadLetterRetryRunner#retry(CommandId)} admin path is never gated.
     */
    public Builder leadership(SubscriptionLeadership leadership) {
      this.leadership = leadership != null ? leadership : SubscriptionLeadership.NOOP;
      return this;
    }

    /**
     * Sets the metrics collector. Optional; defaults to {@link StreamRuneMetrics#NOOP}. When wired
     * (e.g. by the framework auto-configs), the runner emits {@code streamrune.dlq.exhausted}
     * (tagged by command type) plus an ERROR log each time a dead-letter entry exhausts every retry
     * and is permanently dropped/abandoned — the terminal signal for a permanently-lost command.
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
      return this;
    }

    public DeadLetterRetryRunner build() {
      if (deadLetterQueue == null) {
        throw new IllegalArgumentException("deadLetterQueue is required");
      }
      if (commandBus == null) {
        throw new IllegalArgumentException("commandBus is required");
      }
      // Mirrors ProjectionFencingPolicy.requireFencingCapable — real (multi-replica)
      // leadership paired with a bus that cannot dedup a keyed replay turns the leadership gate
      // into theatre. The gate makes exactly one replica POLL, but that is not the same as exactly
      // one replica EXECUTING a given entry: a stale leader (resumed after a GC/VM pause, past its
      // lease TTL, still working through a batch it already fetched) racing a fresh one has
      // nothing to stop it re-executing the same entry, because executeReplay's fallback — the
      // unkeyed CommandBus#execute(Command) — is exactly the path taken when
      // supportsIdempotentExecution() is false: no inbox, no cross-instance dedup, no protection
      // at all. That is a configuration error, not a degraded mode, so it is rejected here, before
      // any traffic — the inbox-less bus remains a fully supported shape for a single instance
      // (SubscriptionLeadership.NOOP), just not paired with real leadership.
      if (leadership != SubscriptionLeadership.NOOP && !commandBus.supportsIdempotentExecution()) {
        throw new IllegalArgumentException(
            "DeadLetterRetryRunner was configured with single-active-consumer leadership ("
                + leadership.getClass().getName()
                + ") but its CommandBus does not support idempotent execution"
                + " (supportsIdempotentExecution() == false, i.e. no CommandInbox is configured)."
                + " The leadership gate only makes ONE replica POLL per cycle — it does not fence a"
                + " stale leader (one that resumed after a GC/VM pause, past its lease TTL) out of"
                + " a batch it already fetched, and without an inbox-backed idempotency key a stale"
                + " leader's replay and a fresh leader's replay of the same entry both execute,"
                + " emitting duplicate domain events. Wire a CommandBus with CommandInbox support"
                + " (all three integrations auto-configure one whenever a DataSource is present),"
                + " or — if exactly one instance runs this runner — leave leadership unconfigured"
                + " so it defaults to SubscriptionLeadership.NOOP (unfenced, single-instance).");
      }
      if (objectMapper == null) {
        throw new IllegalArgumentException("objectMapper is required");
      }
      if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
        throw new IllegalArgumentException(
            "pollInterval must be non-null and positive (Duration.ZERO causes a tight spin loop)");
      }
      if (clock == null) {
        throw new IllegalArgumentException("clock is required");
      }
      if (batchSize < 1) {
        throw new IllegalArgumentException(
            "batchSize must be at least 1 (0 would retry nothing, forever), was " + batchSize);
      }
      return new DeadLetterRetryRunner(
          deadLetterQueue,
          commandBus,
          objectMapper,
          policy,
          pollInterval,
          batchSize,
          commandTypeRegistry,
          clock,
          leadership,
          metrics);
    }
  }
}
