package org.streamrune.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.AggregateState;
import org.streamrune.core.AsyncCommandBus;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.CommandFailureClassification;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.Decider;
import org.streamrune.core.DeciderRegistrations;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.LockException;
import org.streamrune.core.NoDeciderException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

/**
 * Command bus implementation that processes each command on a virtual thread with locking, retry on
 * optimistic lock conflicts, and optional snapshotting.
 */
public final class VirtualThreadCommandBus implements CommandBus, AsyncCommandBus, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(VirtualThreadCommandBus.class);

  private static final String RECORD_INBOX_REPLAY_HIT = "recordInboxReplayHit";

  private static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofSeconds(5);

  /**
   * Baggage key naming the id of the message (typically an event) that caused the current command.
   * Reaction dispatchers (sagas, subscriptions) put the triggering message id under this key in
   * {@link StreamRuneContext} baggage; the bus then writes it into {@link
   * EventMetadata#causationId()} of every event the command produces, keeping causation chains
   * traversable. When absent, produced events have a {@code null} causationId (chain root). The
   * command's own id is always recorded separately in {@link EventMetadata#commandId()}.
   */
  public static final String CAUSATION_ID_BAGGAGE_KEY = "streamrune.causation-id";

  /**
   * Marker for command executions that replay an existing dead-letter-queue entry. DLQ retry
   * infrastructure (e.g. {@link DeadLetterRetryRunner}) binds this to {@code true} around {@code
   * execute(...)}: while bound, a failing execution is NOT published to the DLQ again — the retry
   * infrastructure already tracks attempts on the original entry, and republishing would create a
   * new entry on every retry cycle (unbounded entry multiplication).
   */
  public static final ScopedValue<Boolean> DLQ_REPLAY = ScopedValue.newInstance();

  /**
   * Default {@link Builder#dlqEligible(Predicate)}: excludes permanent rejections — {@link
   * DomainException} (including {@code ValidationException}/{@code AuthorizationException}), {@link
   * IllegalArgumentException}, and a {@link SubjectForgottenException} anywhere in the cause chain
   * — from the dead letter queue. A business rejection is a correct, final answer from the domain,
   * and replaying it from the DLQ would blindly re-append a command the domain already, correctly
   * rejected. A command targeting a crypto-shredded (GDPR-erased) subject is permanent for a second
   * reason too: dead-lettering it would persist the subject's identifier (potentially PII) in
   * {@code dead_letter_queue.error_message}, so the erasure flow itself would re-materialize the
   * data it erased.
   *
   * <p>This is {@link CommandFailureClassification#DLQ_ELIGIBLE} — derived from the same {@code
   * isPermanentRejection} definition {@link
   * CircuitBreakerCommandInterceptor#INFRASTRUCTURE_FAILURES} derives from, not a second copy that
   * claims to mirror it (the two copies had already drifted on exactly the forgotten-subject
   * clause). The two predicates are no longer IDENTICAL, by design — a deterministic per-aggregate
   * stored-data failure ({@link CommandFailureClassification#isDeterministicPerAggregate}) IS
   * dead-lettered here (the entry, marked by its {@code errorType}, is the durable operator signal)
   * while it is excluded from the breaker's threshold. See the {@link CommandFailureClassification}
   * matrix.
   */
  public static final Predicate<Throwable> DEFAULT_DLQ_ELIGIBLE =
      CommandFailureClassification.DLQ_ELIGIBLE;

  /**
   * Payload persisted for a dead-letter entry whose command could not be serialized (e.g. the
   * crypto module's fail-fast {@code CryptoOperationException}). The literal JSON {@code null} is a
   * valid JSON value, so it survives {@code PostgresDeadLetterQueue}'s {@code ?::jsonb} cast (no
   * silent drop) and carries no {@code @Encrypted} PII — unlike the former {@code
   * command.toString()} fallback. The command type and error metadata are still recorded on the
   * entry.
   */
  private static final String METADATA_ONLY_PAYLOAD = "null";

  /**
   * Per thread, the streams whose lock this thread holds via {@link #acquireLockTimed}. {@code
   * postCommitAfterPhase} runs inside the stream lock's try-with-resources, so a nested same-thread
   * dispatch from inside {@code after()} (e.g. an INLINE process-manager projection dispatching a
   * follow-up command) against the SAME stream must re-enter rather than re-acquire. {@link
   * LocalStripedLocker}'s {@link java.util.concurrent.locks.ReentrantLock} would re-enter for free,
   * but {@code PgAdvisoryLocker} checks out a NEW pool connection and takes a session-scoped
   * advisory lock on every {@link AggregateLocker#acquireLock} call, so the identical pattern
   * self-blocks there until {@code lock_timeout} instead. Tracking reentrancy here —
   * locker-agnostic, {@code PgAdvisoryLocker} itself is unchanged — makes every locker reentrant on
   * the same thread for the same stream uniformly. Two aggregate types sharing an id value are two
   * streams: a nested dispatch into the other type takes its own lock.
   */
  private final ThreadLocal<Set<StreamId>> heldStreamLocks = ThreadLocal.withInitial(HashSet::new);

  /** No-op lock handle returned by {@link #acquireLockTimed} for a reentrant acquisition. */
  private static final AutoCloseable NOOP_LOCK = () -> {};

  private final EventStore eventStore;
  private final AggregateLocker locker;
  private final RetryPolicy retryPolicy;
  private final SnapshotPolicy snapshotPolicy;
  private final Duration lockTimeout;
  private final Map<
          Class<?>, DeciderRegistration<?, ? extends AggregateState, ? extends DomainEvent>>
      registry;
  private final ObjectMapper objectMapper;
  private final DeadLetterQueue deadLetterQueue;
  private final StreamRuneMetrics metrics;
  private final List<CommandInterceptor> interceptors;
  private final UnaryOperator<Runnable> asyncTaskWrapper;
  private final Clock clock;
  private final CommandInbox commandInbox;
  private final Predicate<Throwable> dlqEligible;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicInteger inFlightCommands = new AtomicInteger(0);
  // Admission control for executeAsync ONLY — the synchronous execute
  // path is bounded by the caller's own threads and is deliberately not gated by this. A fair
  // Semaphore (FIFO on contention) so a submission burst is rejected in arrival order rather than
  // an arbitrary one once the budget is exhausted.
  private final Semaphore asyncAdmission;
  private final int maxInFlightAsyncCommands;

  private VirtualThreadCommandBus(
      EventStore eventStore,
      AggregateLocker locker,
      RetryPolicy retryPolicy,
      SnapshotPolicy snapshotPolicy,
      Duration lockTimeout,
      Map<Class<?>, DeciderRegistration<?, ? extends AggregateState, ? extends DomainEvent>>
          registry,
      ObjectMapper objectMapper,
      DeadLetterQueue deadLetterQueue,
      StreamRuneMetrics metrics,
      List<CommandInterceptor> interceptors,
      UnaryOperator<Runnable> asyncTaskWrapper,
      Clock clock,
      CommandInbox commandInbox,
      Predicate<Throwable> dlqEligible,
      int maxInFlightAsyncCommands) {
    this.eventStore = eventStore;
    this.locker = locker;
    this.retryPolicy = retryPolicy;
    this.snapshotPolicy = snapshotPolicy;
    this.lockTimeout = lockTimeout;
    // Insertion order is load-bearing: findRegistration() scans linearly and the first
    // registered assignable type must win deterministically. Map.copyOf would discard it.
    this.registry = Collections.unmodifiableMap(new LinkedHashMap<>(registry));
    this.objectMapper = objectMapper;
    this.deadLetterQueue = deadLetterQueue;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
    this.interceptors = List.copyOf(interceptors);
    this.asyncTaskWrapper = asyncTaskWrapper;
    this.clock = clock != null ? clock : Clock.systemUTC();
    this.commandInbox = commandInbox;
    this.dlqEligible = dlqEligible != null ? dlqEligible : DEFAULT_DLQ_ELIGIBLE;
    this.asyncAdmission = new Semaphore(maxInFlightAsyncCommands, true);
    this.maxInFlightAsyncCommands = maxInFlightAsyncCommands;
    // A live gauge so the metrics endpoint always reads the CURRENT in-flight count on every
    // scrape, independent of whether anything happens to push a value in between (mirrors
    // registerSubscriptionLagGauge's rationale). inFlightCommands already counts both the
    // synchronous and async paths together — the same counter close() drains against.
    this.metrics.registerInFlightCommandsGauge(inFlightCommands::get);
  }

  /** Creates a new builder for configuring the command bus. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public <C extends Command> CommandResult execute(C command) {
    // Count first, then check the closed flag. The reverse order is a check-then-act race: a
    // thread could pass the check, get preempted before incrementing, and run the command after
    // close() observed zero in-flight commands and returned.
    inFlightCommands.incrementAndGet();
    if (closed.get()) {
      inFlightCommands.decrementAndGet();
      throw new CommandBusClosedException("Command bus is closed, cannot accept new commands");
    }
    try {
      return executeCounted(command, null);
    } finally {
      inFlightCommands.decrementAndGet();
    }
  }

  /**
   * Reports whether a {@link CommandInbox} was configured on this bus. When {@code false}, {@link
   * #execute(Command, IdempotencyKey)} throws {@link IllegalStateException} rather than executing —
   * callers that need to choose between keyed and unkeyed execution (e.g. {@link
   * DeadLetterRetryRunner}) must check this up front instead of catching that exception.
   */
  @Override
  public boolean supportsIdempotentExecution() {
    return commandInbox != null;
  }

  @Override
  public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
    Objects.requireNonNull(key, "idempotencyKey is required");
    if (commandInbox == null) {
      throw new IllegalStateException(
          "execute(command, key) requires a CommandInbox; none was configured on this bus");
    }
    // The inbox pre-check is NOT done here: it must run inside executeCounted(), after the
    // interceptors' before() chain, so an idempotent replay is still authorized/validated/audited
    // like any other execution. See executeCounted() for the check itself.
    inFlightCommands.incrementAndGet();
    if (closed.get()) {
      inFlightCommands.decrementAndGet();
      throw new CommandBusClosedException("Command bus is closed, cannot accept new commands");
    }
    try {
      return executeCounted(command, key);
    } finally {
      inFlightCommands.decrementAndGet();
    }
  }

  /**
   * Executes a command that is already counted as in-flight and admitted past the closed check.
   * {@link #executeAsync} invokes this directly from its spawned thread: the async command was
   * admitted and counted at submission time, so during close() it drains to completion instead of
   * being re-checked — and spuriously rejected — when its thread starts.
   *
   * @param key nullable idempotency key; {@code null} for unkeyed (hot-path) execution
   */
  @SuppressWarnings("unchecked")
  private <C extends Command> CommandResult executeCounted(C command, IdempotencyKey key) {
    // Guarded like every other meter on this method. This is the FIRST statement of the
    // command path and it sits outside the try below, so an unguarded throw here propagated
    // straight out of execute(): the decider never ran, nothing was appended, no interceptor and no
    // DLQ was notified — a metrics-backend problem became a total command outage.
    recordMetric(metrics::recordCommandDispatched, "recordCommandDispatched");

    long start = System.nanoTime();
    try {
      // Extract the aggregate id and compose the stream id early: interceptors never observe a
      // null id or an invalid stream, and every later step — the veto result, the replay guard,
      // the lock, the load, the dead-letter request — uses this one stream id. Failures here
      // happen before any interceptor's before() ran, so no interceptor is notified.
      DeciderRegistration<C, ?, ?> reg;
      AggregateId aggregateId;
      StreamId streamId;
      try {
        reg = (DeciderRegistration<C, ?, ?>) findRegistration(command);
        aggregateId = reg.idExtractor().apply(command);
        if (aggregateId == null) {
          throw new IllegalArgumentException(
              "idExtractor returned null for command: " + command.getClass().getSimpleName());
        }
        streamId = StreamId.of(reg.aggregateType(), aggregateId);
      } catch (RuntimeException e) {
        recordMetric(metrics::recordCommandFailed, "recordCommandFailed");
        throw e;
      }
      CommandId commandId = IdGenerator.generateCommandId();
      Instant commandTimestamp = Instant.now();
      CommandContext beforeCtx =
          new CommandContext(
              command,
              command.getClass().getSimpleName(),
              commandId,
              reg.aggregateType(),
              aggregateId,
              null,
              commandTimestamp);

      // Phase 1: before() in registration order. Track how many completed so callbacks are
      // only ever delivered to interceptors whose before() actually ran to completion.
      int completedBefores = 0;
      String shortCircuitedBy = null;
      try {
        for (CommandInterceptor interceptor : interceptors) {
          if (!interceptor.before(beforeCtx)) {
            shortCircuitedBy = interceptorName(interceptor);
            break;
          }
          completedBefores++;
        }
      } catch (RuntimeException e) {
        // before() threw: notify only interceptors whose before() completed, in reverse order.
        // Neither the thrower nor interceptors that never ran are notified.
        notifyOnError(beforeCtx, e, completedBefores);
        recordMetric(metrics::recordCommandFailed, "recordCommandFailed");
        throw e;
      }

      if (shortCircuitedBy != null) {
        // Short-circuit: the command is NOT executed. Interceptors whose before() returned
        // true receive after() with a distinguishable, never-null result; onError is not
        // invoked (nothing failed), and the short-circuiting interceptor gets no callback.
        recordMetric(
            () -> metrics.recordCommandShortCircuited(command.getClass().getSimpleName()),
            "recordCommandShortCircuited");
        CommandResult shortCircuitResult =
            new CommandResult(
                List.of(),
                streamId,
                Version.initial(),
                List.of(),
                List.of(),
                shortCircuitedBy,
                CommandBus.ShortCircuitReason.VETOED);
        notifyAfter(withResult(beforeCtx, shortCircuitResult), completedBefores);
        return shortCircuitResult;
      }

      // Idempotency pre-check: runs AFTER the interceptors' before() chain (so authorization,
      // validation, and audit interceptors observe a replay exactly like a fresh execution) and
      // BEFORE the aggregate is loaded and the handler runs (so a HIT never re-decides or
      // re-appends). A veto above already returned; only a HIT reachable here can short-circuit.
      // This is the fast path for a redelivery whose original has already committed; it runs
      // outside the aggregate lock, so a duplicate that OVERLAPS the original misses it — those are
      // caught by the second lookup under the lock in executeWithRetry.
      if (key != null) {
        java.util.Optional<CommandInbox.InboxResult> prior;
        try {
          prior = commandInbox.find(key);
        } catch (RuntimeException e) {
          // The inbox pre-check is the only step between the before() chain and
          // executeInternal() that was NOT guarded — a find() failure (e.g. an EventStoreException
          // on a transient DB outage) must route through the SAME error path as a handler failure.
          // Otherwise every interceptor whose before() completed leaks its resource (the OTel Scope
          // is never closed, the ThreadLocal-backed audit user is never cleaned, the circuit
          // breaker never counts), no FAILURE audit row is written, and the failure metric is never
          // recorded — precisely during an infrastructure incident. Nothing executed, so
          // exactly-once semantics are unaffected.
          notifyOnError(beforeCtx, e, completedBefores);
          recordMetric(metrics::recordCommandFailed, "recordCommandFailed");
          throw e;
        }
        if (prior.isPresent()) {
          recordMetric(
              () -> metrics.recordInboxReplayHit(command.getClass().getSimpleName()),
              RECORD_INBOX_REPLAY_HIT);
          CommandResult replayResult;
          try {
            replayResult = buildReplayResult(command, key, streamId, prior.get());
          } catch (RuntimeException e) {
            // Key reused across commands (different type, or a different target stream): never
            // leak the stored result to the wrong caller. Treated like any other pre-commit
            // failure — onError fires, nothing was executed or committed.
            notifyOnError(beforeCtx, e, completedBefores);
            recordMetric(metrics::recordCommandFailed, "recordCommandFailed");
            throw e;
          }
          notifyAfter(withResult(beforeCtx, replayResult), completedBefores);
          try {
            metrics.recordCommandSucceeded();
          } catch (RuntimeException e) {
            LOG.error("Metrics recordCommandSucceeded failed after idempotent replay", e);
          }
          return replayResult;
        }
      }

      // completedBefores is reassigned by the before()-chain loop above, so it is not
      // effectively final; the lambda below needs its own, never-reassigned copy of the value.
      int completedBeforesAtDispatch = completedBefores;
      CommandResult result;
      try {
        result =
            executeInternal(
                command,
                reg,
                commandId,
                streamId,
                commandTimestamp,
                key,
                // Invoked from INSIDE executeWithRetry's
                // try-with-resources — i.e. while the per-stream lock is still held; see that
                // method's javadoc. notifyAfter() itself already swallows and logs every
                // interceptor's after() exception, so this can never turn a committed command into
                // a reported failure.
                committedResult ->
                    notifyAfter(
                        withResult(beforeCtx, committedResult), completedBeforesAtDispatch));
      } catch (RuntimeException e) {
        notifyOnError(beforeCtx, e, completedBefores);
        recordMetric(metrics::recordCommandFailed, "recordCommandFailed");
        throw e;
      }

      // The command succeeded and any produced events are committed. From here on nothing may
      // turn the result into a failure — a misreported failure would be replayed from the DLQ
      // and duplicate the committed events. after() was already invoked above, from INSIDE the
      // locked section (this call used to sit here, AFTER the aggregate lock was released,
      // letting a second command's after() phase on the same aggregate interleave with — and
      // outrun — this one's; see executeWithRetry).
      try {
        metrics.recordCommandSucceeded();
      } catch (RuntimeException e) {
        LOG.error("Metrics recordCommandSucceeded failed after command was committed", e);
      }
      return result;
    } finally {
      try {
        metrics.recordCommandDuration(System.nanoTime() - start);
      } catch (RuntimeException e) {
        LOG.error("Metrics recordCommandDuration failed", e);
      }
    }
  }

  /**
   * Builds the result for an idempotency-key HIT: the command already ran under {@code key} and its
   * outcome is recorded in {@code stored}. Verifies FIRST that the row was recorded by this very
   * command — same command type AND same target stream — through the one shared {@link
   * CommandInbox.InboxResult#requireBoundTo} guard; a mismatch means the key was reused across
   * commands, and the stored result must never be leaked to the wrong caller.
   *
   * <p>This pre-check is NOT the only path that turns a recorded row into a caller-visible result.
   * Two concurrent executions of the same key both MISS it (neither inbox row is committed yet);
   * the loser is then answered by the lookup under the aggregate lock, by the lookup after a
   * decider rejection ({@link #recordedOutcomeAfterRejection}), or — when both reached the append —
   * by {@link org.streamrune.core.EventStore#appendWithKey}'s claim-loser branch, which used to
   * return the winner's offsets as a success. Every path calls this same guard with the same two
   * discriminators, so the outcome no longer depends on timing. The stream check is part of that:
   * without it this path still replayed another aggregate's {@code streamId}, {@code finalVersion}
   * and offsets whenever one key was presented on two streams.
   *
   * @throws IllegalArgumentException if {@code key} was recorded for a different command type or a
   *     different stream
   */
  private static <C extends Command> CommandResult buildReplayResult(
      C command, IdempotencyKey key, StreamId streamId, CommandInbox.InboxResult stored) {
    stored.requireBoundTo(command.getClass().getName(), streamId);
    return new CommandResult(
        List.of(),
        stored.streamId(),
        stored.finalVersion(),
        stored.globalOffsets(),
        List.of(),
        key.value(),
        CommandBus.ShortCircuitReason.IDEMPOTENT_REPLAY);
  }

  /**
   * Turns a recorded inbox row into the caller-visible replay for a duplicate detected AFTER the
   * pre-execution lookup — under the aggregate lock, or after the decider rejected the duplicate.
   * Same binding guard and the same replay-hit metric as the pre-execution HIT in {@link
   * #executeCounted}, so the three lookups are indistinguishable to the caller and to a dashboard.
   */
  private <C extends Command> CommandResult replayResult(
      C command, IdempotencyKey key, StreamId streamId, CommandInbox.InboxResult recorded) {
    CommandResult replay = buildReplayResult(command, key, streamId, recorded);
    recordMetric(
        () -> metrics.recordInboxReplayHit(command.getClass().getSimpleName()),
        RECORD_INBOX_REPLAY_HIT);
    return replay;
  }

  /**
   * Consults the inbox after a keyed command's {@code guard()}/{@code decide()} threw. Returns the
   * recorded outcome when the key is recorded, or {@code null} when it is not — the rejection then
   * stands as the business answer it is.
   *
   * <p>A lookup that itself fails, or a recorded row bound to a different command or stream,
   * propagates THAT failure with the rejection attached as a suppressed exception — never the
   * rejection alone. The caller cannot know whether the rejection contradicts a recorded success,
   * so the honest answer is "outcome unknown": an infrastructure failure a saga classifies as
   * retry-later and redelivers, where the redelivery either finds the key or earns the rejection
   * again. Propagating the rejection instead would compensate a succeeded step exactly when the
   * database is already struggling. That failure is never retried by the bus and never
   * dead-lettered, whatever {@code dlqEligible} says: a replay could only repeat the recorded
   * outcome or re-run the rejected decision.
   */
  private <C extends Command> CommandResult recordedOutcomeAfterRejection(
      C command, IdempotencyKey key, StreamId streamId, RuntimeException rejection) {
    try {
      java.util.Optional<CommandInbox.InboxResult> recorded = commandInbox.find(key);
      if (recorded.isEmpty()) {
        return null;
      }
      return replayResult(command, key, streamId, recorded.get());
    } catch (RuntimeException lookupFailure) {
      lookupFailure.addSuppressed(rejection);
      throw lookupFailure;
    }
  }

  /**
   * Runs a metric-recording action, logging and swallowing any failure. Metric backends must never
   * disrupt command execution: a throwing recorder neither fails the command nor masks its outcome.
   */
  private void recordMetric(Runnable recorder, String meter) {
    try {
      recorder.run();
    } catch (RuntimeException e) {
      LOG.warn("Metrics {} failed", meter, e);
    }
  }

  /** Returns a stable display name for an interceptor, even for anonymous classes. */
  private static String interceptorName(CommandInterceptor interceptor) {
    String simpleName = interceptor.getClass().getSimpleName();
    return simpleName.isEmpty() ? interceptor.getClass().getName() : simpleName;
  }

  /** Returns a copy of the context with the result populated for after() callbacks. */
  private static CommandContext withResult(CommandContext ctx, CommandResult result) {
    return new CommandContext(
        ctx.command(),
        ctx.commandType(),
        ctx.commandId(),
        ctx.aggregateType(),
        ctx.aggregateId(),
        result,
        ctx.timestamp());
  }

  /**
   * Invokes after() in reverse registration order on the first {@code count} interceptors. The
   * command outcome is already decided when after() runs, so exceptions are logged and swallowed: a
   * throwing callback must neither change the outcome nor starve sibling interceptors.
   */
  private void notifyAfter(CommandContext ctx, int count) {
    for (int i = count - 1; i >= 0; i--) {
      CommandInterceptor interceptor = interceptors.get(i);
      try {
        interceptor.after(ctx);
      } catch (RuntimeException e) {
        LOG.error(
            "after() of interceptor {} failed for command {}; command result is unaffected",
            interceptorName(interceptor),
            LogSanitizer.sanitizeForLog(ctx.commandId().value()),
            e);
      }
    }
  }

  /**
   * Invokes onError() in reverse registration order on the first {@code count} interceptors (those
   * whose before() completed). Exceptions are logged and swallowed so a throwing callback neither
   * suppresses the original failure nor starves sibling interceptors.
   */
  private void notifyOnError(CommandContext ctx, Throwable error, int count) {
    for (int i = count - 1; i >= 0; i--) {
      CommandInterceptor interceptor = interceptors.get(i);
      try {
        interceptor.onError(ctx, error);
      } catch (RuntimeException e) {
        LOG.error(
            "onError() of interceptor {} failed for command {}; original failure still propagates",
            interceptorName(interceptor),
            LogSanitizer.sanitizeForLog(ctx.commandId().value()),
            e);
      }
    }
  }

  /**
   * Executes the command on a new virtual thread — one thread per call. The threads themselves are
   * cheap, and the resources a running command competes for are already bounded (the aggregate
   * locker's stripes, the event store's connection pool, the commit-ordered global append path a
   * transactional store serializes on) — a submission burst that gets admitted queues on those
   * instead of exhausting anything. <b>Admission itself is bounded</b>: {@link
   * Builder#maxInFlightAsyncCommands(int)} caps how many async commands may be admitted and not yet
   * complete at once; once that budget is exhausted, a further call returns a future failed with
   * {@link CommandBusOverloadedException} immediately — nothing is attempted, and the caller's own
   * thread is never blocked waiting for a slot. This bounds the memory held by pending futures and
   * in-flight command state that an unbounded submission rate would otherwise grow without limit.
   * The synchronous {@link #execute} path is unaffected: it is bounded by the caller's own threads,
   * not by this budget.
   *
   * <p>A command's slot is freed <b>before</b> its future completes, normally or exceptionally, so
   * a caller that resubmits from a stage chained on the future, or as soon as {@code get()}/{@code
   * join()} returns, is never refused on account of its own previous command. A future the caller
   * cancels does not free the slot: cancelling does not stop the command, which keeps its slot
   * until it finishes. If the {@link Builder#asyncTaskWrapper(UnaryOperator) asyncTaskWrapper}'s
   * runnable throws before running the command, the future fails with that throwable.
   *
   * <p>Caller context that does not cross {@code Thread.start()} is captured at submission: {@link
   * StreamRuneContext#CURRENT}, {@link #DLQ_REPLAY}, and {@link StreamRuneContext#SAGA_OWNED} are
   * re-bound on the new thread, and the configured {@link Builder#asyncTaskWrapper(UnaryOperator)
   * asyncTaskWrapper} can capture other thread-bound state such as the current OpenTelemetry
   * context.
   */
  @Override
  public <C extends Command> CompletableFuture<CommandResult> executeAsync(C command) {
    // Admission control BEFORE anything else touches inFlightCommands or the closed flag. A
    // command that never acquires a permit was never admitted, so it must not affect the count
    // close() drains against. tryAcquire() never blocks — an over-budget submission is rejected
    // immediately ("fails fast"), never queued waiting for a slot.
    if (!asyncAdmission.tryAcquire()) {
      recordMetric(metrics::recordCommandAsyncRejected, "recordCommandAsyncRejected");
      return CompletableFuture.failedFuture(
          new CommandBusOverloadedException(
              "Command bus in-flight async budget of "
                  + maxInFlightAsyncCommands
                  + " commands is exhausted; refusing "
                  + command.getClass().getSimpleName()
                  + " — nothing was attempted. Retry later, or size"
                  + " Builder.maxInFlightAsyncCommands to a measured capacity profile."));
    }
    // Count the command as in-flight at submission, before spawning the thread: close() then
    // waits for it even when the thread has not started executing yet. Admission is decided here
    // exactly once — the spawned thread runs executeCounted() without re-checking the closed
    // flag, so a command accepted before close() drains to completion instead of failing
    // mid-shutdown.
    var slot = new AdmissionSlot(asyncAdmission);
    inFlightCommands.incrementAndGet();
    if (closed.get()) {
      inFlightCommands.decrementAndGet();
      slot.release();
      return CompletableFuture.failedFuture(
          new CommandBusClosedException("Command bus is closed, cannot accept new commands"));
    }
    var future = new CompletableFuture<CommandResult>();
    Runnable task;
    try {
      task = prepareAsyncTask(command, future, slot);
    } catch (RuntimeException e) {
      inFlightCommands.decrementAndGet();
      slot.release();
      return CompletableFuture.failedFuture(e);
    }
    Thread.ofVirtual()
        .name("streamrune-cmd-" + command.getClass().getSimpleName())
        .start(
            () -> {
              try {
                task.run();
              } catch (Throwable t) {
                // Only the asyncTaskWrapper's own code can throw here — the command's outcome,
                // failures included, is caught inside the task. A wrapper that failed before
                // running the command must not leave the caller waiting forever: fail the future
                // with the wrapper's throwable, freeing the slot first as on every other path. A
                // wrapper that failed after the command keeps the command's outcome.
                slot.release();
                if (!future.completeExceptionally(t)) {
                  LOG.warn(
                      "asyncTaskWrapper failed after command {} completed; the command's"
                          + " outcome is unaffected",
                      command.getClass().getSimpleName(),
                      t);
                }
              } finally {
                // A no-op whenever the task ran the command, which freed the slot before
                // completing the future; this covers a wrapper that never ran it.
                slot.release();
                // Last, after the wrapper's own cleanup: close() drains against this count and
                // must not return while this thread still runs code.
                inFlightCommands.decrementAndGet();
              }
            });
    return future;
  }

  /**
   * One admitted async command's hold on the {@code executeAsync} budget. {@link #release()} frees
   * the slot at most once, however many of the command's exit paths reach it, so the budget can
   * neither leak a slot nor gain one.
   */
  private static final class AdmissionSlot {
    private final Semaphore budget;
    private final AtomicBoolean held = new AtomicBoolean(true);

    AdmissionSlot(Semaphore budget) {
      this.budget = budget;
    }

    void release() {
      if (held.compareAndSet(true, false)) {
        budget.release();
      }
    }
  }

  /**
   * Builds the runnable that {@link #executeAsync} runs on its new virtual thread. Runs on the
   * submitting thread so caller context can be captured before it is lost: ScopedValues ({@link
   * StreamRuneContext#CURRENT}, {@link #DLQ_REPLAY}, {@link StreamRuneContext#SAGA_OWNED}) don't
   * inherit across {@code Thread.start()} and are re-bound via a carrier, and the configured {@link
   * Builder#asyncTaskWrapper(java.util.function.UnaryOperator) asyncTaskWrapper} is applied here so
   * it can capture thread-bound caller state such as the current OpenTelemetry context.
   */
  private <C extends Command> Runnable prepareAsyncTask(
      C command, CompletableFuture<CommandResult> future, AdmissionSlot slot) {
    var capturedCtx = StreamRuneContext.CURRENT.isBound() ? StreamRuneContext.CURRENT.get() : null;
    var capturedDlqReplay = DLQ_REPLAY.isBound() ? DLQ_REPLAY.get() : null;
    var capturedSagaOwned =
        StreamRuneContext.SAGA_OWNED.isBound() ? StreamRuneContext.SAGA_OWNED.get() : null;
    // The slot is freed BEFORE the future completes, on success and failure alike: a stage chained
    // on the future runs inside complete() on this very thread, and a caller blocked in get() or
    // join() may resubmit the instant it wakes — either must find its own command's slot free, or
    // a strictly sequential caller is refused with CommandBusOverloadedException by a command that
    // has already finished.
    Runnable completeFuture =
        () -> {
          CommandResult result;
          try {
            result = executeCounted(command, null);
          } catch (Throwable e) {
            slot.release();
            future.completeExceptionally(e);
            return;
          }
          slot.release();
          future.complete(result);
        };
    ScopedValue.Carrier carrier = null;
    if (capturedCtx != null) {
      carrier = ScopedValue.where(StreamRuneContext.CURRENT, capturedCtx);
    }
    if (capturedDlqReplay != null) {
      carrier =
          (carrier == null)
              ? ScopedValue.where(DLQ_REPLAY, capturedDlqReplay)
              : carrier.where(DLQ_REPLAY, capturedDlqReplay);
    }
    if (capturedSagaOwned != null) {
      carrier =
          (carrier == null)
              ? ScopedValue.where(StreamRuneContext.SAGA_OWNED, capturedSagaOwned)
              : carrier.where(StreamRuneContext.SAGA_OWNED, capturedSagaOwned);
    }
    ScopedValue.Carrier boundCarrier = carrier;
    Runnable task = boundCarrier != null ? () -> boundCarrier.run(completeFuture) : completeFuture;
    if (asyncTaskWrapper != null) {
      task = asyncTaskWrapper.apply(task);
      if (task == null) {
        throw new IllegalStateException("asyncTaskWrapper returned null");
      }
    }
    return task;
  }

  private <C extends Command> CommandResult executeInternal(
      C command,
      DeciderRegistration<C, ?, ?> reg,
      CommandId commandId,
      StreamId streamId,
      Instant commandTimestamp,
      IdempotencyKey key,
      Consumer<CommandResult> postCommitAfterPhase) {
    return executeWithRetry(
        command, reg, commandId, streamId, commandTimestamp, key, postCommitAfterPhase);
  }

  /**
   * Executes the command with retry on optimistic-lock/lock-acquisition conflicts, holding the
   * per-stream lock for the entire critical section — the keyed inbox lookup, load, decide, append
   * and, for an attempt that reaches a return statement here, {@code postCommitAfterPhase} too.
   *
   * <p><b>A keyed duplicate is answered from the inbox, whenever it arrives.</b> The pre-execution
   * lookup in {@link #executeCounted} runs outside the lock and catches a redelivery whose original
   * has already committed. A duplicate that overlaps its original misses it, so every attempt here
   * looks the key up again as its first step under the lock — a duplicate serialized behind the
   * winner's lock ({@code PgAdvisoryLocker} across replicas, or two threads in one process) returns
   * {@code IDEMPOTENT_REPLAY} without loading state or running the decider. When the lock does not
   * serialize the two (a per-process locker on two replicas), the duplicate may still load the
   * winner's committed state and be rejected by the decider; a keyed rejection therefore consults
   * the inbox before it propagates ({@link #recordedOutcomeAfterRejection}). The remaining overlap
   * — both dispatches reach the append — is settled by {@code appendWithKey}'s claim-loser branch.
   * Without the two lookups here, a saga treated the lock-serialized loser's rejection as a
   * permanent failure and compensated a forward step that had succeeded.
   *
   * <p>The lock is a try-with-resources scoped to this method. Before this fix it was released as
   * soon as this method returned, and the command bus's {@code after()} phase — including {@link
   * InlineProjectionInterceptor} — ran afterward, unlocked. Two commands committed against the same
   * aggregate could then apply their inline projections out of commit order: the second commits and
   * projects while the first is still mid-projection, and the first then overwrites the read model
   * with its now-stale value. {@code postCommitAfterPhase} is invoked here, still inside the lock's
   * try-with-resources, so a second command targeting the same aggregate cannot even start its own
   * critical section until the first's after() phase — including its inline projection — has
   * finished and released the lock.
   *
   * <p><b>Reentrant on every locker, via the bus:</b> a nested same-thread dispatch from inside
   * {@code postCommitAfterPhase} (e.g. an inline projection that dispatches a follow-up command
   * against the same stream) re-enters instead of deadlocking or self-blocking — {@link
   * #acquireLockTimed} tracks, per thread, which streams this thread already holds and returns a
   * no-op handle for a re-entry, never calling the configured {@link AggregateLocker} a second time
   * for the same stream on the same thread. This is deliberately NOT a property of any one locker:
   * {@link LocalStripedLocker}'s {@link java.util.concurrent.locks.ReentrantLock} happens to
   * re-enter for free, but {@code PgAdvisoryLocker} — the documented multi-instance production
   * locker — checks out a NEW pool connection and takes a session-scoped advisory lock on every
   * {@code acquireLock} call, so without this tracking the identical nested-dispatch pattern would
   * self-block on it until {@link Builder#lockTimeout(Duration) lock timeout} per retry attempt
   * instead of re-entering (see its javadoc for the resulting lock-pool sizing consequence). A
   * different thread — or a nested dispatch against a DIFFERENT stream, including another aggregate
   * type with the same id value — still contends for a real, independent acquisition exactly as
   * before, bounded by the same lock timeout, so a stuck after() callback still fails through the
   * existing {@link LockException}/retry path rather than hanging indefinitely.
   *
   * @param postCommitAfterPhase invoked, still under the stream lock, with the {@link
   *     CommandResult} of every attempt that reaches a return statement inside the locked section
   *     (a committed append, a keyed zero-event no-op, a keyed duplicate answered from the inbox,
   *     or an inbox-already-applied race loser) — never for a retried (transient-failure) attempt
   *     or a permanent failure, neither of which produces a result.
   */
  private <C extends Command, S extends AggregateState, E extends DomainEvent>
      CommandResult executeWithRetry(
          C command,
          DeciderRegistration<C, S, E> reg,
          CommandId commandId,
          StreamId streamId,
          Instant commandTimestamp,
          IdempotencyKey key,
          Consumer<CommandResult> postCommitAfterPhase) {
    Decider<C, S, E> decider = reg.decider();
    String commandType = command.getClass().getSimpleName();

    for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
      boolean committed = false;
      CommandResult committedResult = null;
      // The failure of the inbox lookup that would tell whether a keyed rejection contradicts a
      // recorded outcome; see the catch block below.
      RuntimeException outcomeUnknownFailure = null;
      long lockWaitStart = System.nanoTime();
      try (var _ = acquireLockTimed(streamId, lockWaitStart)) {
        if (key != null) {
          // Second inbox lookup, now under the aggregate lock and repeated on every attempt. The
          // pre-execution lookup in executeCounted() runs BEFORE the lock, so two dispatches of the
          // same key that overlap both miss it: the winner commits under the lock, and the loser,
          // serialized behind it, would otherwise load the winner's state and hand it to the
          // decider — which rejects a command it has already applied ("order already confirmed").
          // That rejection is indistinguishable from a genuine business rejection downstream: a
          // saga classifies it as permanent and compensates a step that succeeded. Looking the key
          // up here answers the loser with the recorded outcome before any state is loaded.
          java.util.Optional<CommandInbox.InboxResult> recorded = commandInbox.find(key);
          if (recorded.isPresent()) {
            // The binding guard runs first: a key recorded for another command or stream since the
            // pre-execution lookup is refused through the ordinary failure path, because nothing
            // was committed.
            committedResult = replayResult(command, key, streamId, recorded.get());
            committed = true;
            postCommitAfterPhase.accept(committedResult);
            return committedResult;
          }
        }
        // Under SnapshotPolicy.never() there is no version to match, and — critically —
        // no stored snapshot may be consulted at all (EventStore.IGNORE_SNAPSHOT), never the old
        // "0 = skip the check, use anything" sentinel: a snapshot left over from before the policy
        // switched to never() must not silently keep rehydrating a state whose schema may have
        // moved on since.
        int expectedSnapshotVersion =
            (snapshotPolicy instanceof SnapshotPolicy.EveryNEvents e)
                ? e.snapshotVersion()
                : EventStore.IGNORE_SNAPSHOT;
        AggregateHistory history = eventStore.load(streamId, expectedSnapshotVersion);
        S state = reconstructState(decider, history);
        List<E> newEvents;
        try {
          decider.guard(command, state);
          newEvents = decider.decide(command, state);
        } catch (RuntimeException rejection) {
          if (key == null) {
            throw rejection;
          }
          // Defence in depth for a keyed command: the lookup above only helps when the lock
          // serializes the two dispatches. A per-process locker on two replicas serializes
          // nothing, so the duplicate can pass that lookup, load the winner's committed state and
          // be rejected by the decider here. A recorded key means the rejection is an artefact of
          // the duplicate and the caller gets the recorded outcome; an unrecorded key means it is
          // the business answer and propagates unchanged.
          CommandResult recordedOutcome;
          try {
            recordedOutcome = recordedOutcomeAfterRejection(command, key, streamId, rejection);
          } catch (RuntimeException lookupFailure) {
            outcomeUnknownFailure = lookupFailure;
            throw lookupFailure;
          }
          if (recordedOutcome == null) {
            throw rejection;
          }
          committed = true;
          committedResult = recordedOutcome;
          postCommitAfterPhase.accept(committedResult);
          return committedResult;
        }

        if (newEvents.isEmpty()) {
          if (key == null) {
            // Nothing to persist, but the command has its result: from here on it is a success
            // exactly like a committed append — a lock release that throws below must not deliver
            // onError() on top of the after() phase, dead-letter the no-op, or report a failure.
            committed = true;
            committedResult = new CommandResult(List.of(), streamId, history.version(), List.of());
            postCommitAfterPhase.accept(committedResult);
            return committedResult;
          }
          // Keyed zero-event: record the key atomically so re-runs short-circuit via inbox.
          var keyed =
              appendWithKeyTimed(
                  streamId, List.of(), history.version(), key, command.getClass().getName());
          committed = true;
          committedResult =
              new CommandResult(
                  List.of(),
                  streamId,
                  keyed.finalVersion(),
                  keyed.globalOffsets(),
                  List.of(),
                  keyed.alreadyApplied() ? key.value() : null,
                  keyed.alreadyApplied()
                      ? CommandBus.ShortCircuitReason.IDEMPOTENT_REPLAY
                      : CommandBus.ShortCircuitReason.NONE);
          postCommitAfterPhase.accept(committedResult);
          return committedResult;
        }

        List<EventEnvelope> envelopes =
            buildEnvelopes(streamId, newEvents, history.version(), commandId);

        if (key == null) {
          var appendResult = appendTimed(streamId, envelopes, history.version());

          // COMMIT POINT: the events are durably appended. Nothing below may fail the command —
          // a reported failure would be replayed from the DLQ and duplicate the committed events.
          committed = true;
          committedResult =
              finishCommitted(
                  streamId, decider, state, newEvents, history, envelopes, appendResult);
          postCommitAfterPhase.accept(committedResult);
          return committedResult;
        } else {
          var keyed =
              appendWithKeyTimed(
                  streamId, envelopes, history.version(), key, command.getClass().getName());
          committed = true;
          if (keyed.alreadyApplied()) {
            // Concurrent winner recorded it first — return an already-applied result, no
            // projections re-run.
            recordMetric(
                () -> metrics.recordInboxReplayHit(command.getClass().getSimpleName()),
                RECORD_INBOX_REPLAY_HIT);
            committedResult =
                new CommandResult(
                    List.of(),
                    streamId,
                    keyed.finalVersion(),
                    keyed.globalOffsets(),
                    List.of(),
                    key.value(),
                    CommandBus.ShortCircuitReason.IDEMPOTENT_REPLAY);
            postCommitAfterPhase.accept(committedResult);
            return committedResult;
          }
          var appendResult =
              new EventStore.AppendResult(keyed.globalOffsets(), keyed.finalVersion());
          committedResult =
              finishCommitted(
                  streamId, decider, state, newEvents, history, envelopes, appendResult);
          postCommitAfterPhase.accept(committedResult);
          return committedResult;
        }
      } catch (Exception e) {
        if (committed) {
          // E.g. lock release failing after the append committed. The events are persisted,
          // so the command is a success; never report failure or publish to the DLQ here.
          LOG.error(
              "Failure after events were committed for command {} on stream {} — "
                  + "reporting success to avoid a duplicate-producing DLQ replay",
              LogSanitizer.sanitizeForLog(commandId.value()),
              LogSanitizer.sanitizeForLog(streamId.value()),
              e);
          if (committedResult != null) {
            return committedResult;
          }
          // Defensive: every path assigns committedResult before anything that can throw, so this
          // is unreachable in practice.
          if (e instanceof RuntimeException re) {
            throw re;
          }
          throw new CommandExecutionException(command, "Failed to execute command", e);
        }
        if (outcomeUnknownFailure != null) {
          // The domain rejected the command and the inbox could not say whether the key was
          // already recorded. The failure propagates so a saga redelivers, but it is neither
          // retried here nor dead-lettered: a replay could only repeat the recorded outcome (an
          // inbox no-op) or re-run a decision the domain rejected, and a re-run after the state
          // has moved on could execute a command whose caller was told it was rejected.
          LOG.info(
              "Not dead-lettering command {} ({}): the decider rejected it and the inbox lookup"
                  + " for its idempotency key failed ({}), so the outcome is unknown; the caller"
                  + " gets the lookup failure with the rejection attached",
              LogSanitizer.sanitizeForLog(commandId.value()),
              command.getClass().getSimpleName(),
              outcomeUnknownFailure.getClass().getSimpleName());
          throw outcomeUnknownFailure;
        }
        // LockAcquisitionException (LocalStripedLocker) is a subtype of LockException
        // (PgAdvisoryLocker), so the single LockException check covers both lockers.
        boolean transientFailure =
            e instanceof OptimisticLockException || e instanceof LockException;
        if (transientFailure && attempt < retryPolicy.maxAttempts()) {
          LOG.debug(
              "Transient {} on attempt {}/{} for command {} on stream {} — retrying",
              e.getClass().getSimpleName(),
              attempt,
              retryPolicy.maxAttempts(),
              LogSanitizer.sanitizeForLog(commandId.value()),
              LogSanitizer.sanitizeForLog(streamId.value()));
          recordMetric(() -> metrics.recordCommandRetried(commandType), "recordCommandRetried");
          if (sleepForRetryBackoff(retryPolicy.delayForAttempt(attempt))) {
            continue;
          }
          // Interrupted mid-backoff (typically shutdown): abandon the retries, but record the
          // command in the DLQ first — like any other abandoned failure it must not be lost.
          maybePublishToDeadLetterQueue(
              command, commandId, streamId, e, attempt, commandTimestamp, key);
          throw new CommandExecutionException(
              command, "Interrupted during retry backoff after attempt " + attempt, e);
        }
        // Permanent failure or retries exhausted — publish to DLQ before propagating, unless the
        // failure is a business rejection (see dlqEligible).
        maybePublishToDeadLetterQueue(
            command, commandId, streamId, e, attempt, commandTimestamp, key);
        if (e instanceof RuntimeException re) {
          throw re;
        }
        throw new CommandExecutionException(command, "Failed to execute command", e);
      }
    }
    // This should never be reached but compiler needs it
    throw new IllegalStateException("Retry policy exhausted without result");
  }

  /**
   * Appends through the event store and records {@code streamrune.events.duration} — the store
   * round trip an operator's append-latency alert is written against. Timing here rather than
   * inside one store implementation keeps the timer live for every {@link EventStore}.
   *
   * <p>The sample is taken in a {@code finally} so a slow FAILING append (lock timeout, statement
   * timeout, conflict) is visible in the latency series too — the degradation an alert must catch
   * is exactly the one that also starts failing. Recording never alters control flow: a throwing
   * collector is logged and swallowed.
   */
  private EventStore.AppendResult appendTimed(
      StreamId streamId, List<EventEnvelope> envelopes, Version expectedVersion) {
    long startNanos = System.nanoTime();
    try {
      return eventStore.append(streamId, envelopes, expectedVersion);
    } finally {
      recordAppendDuration(envelopes, startNanos);
    }
  }

  /**
   * Keyed sibling of {@link #appendTimed}. A keyed ZERO-event append writes only the inbox claim —
   * no event is appended — so it contributes no sample to {@code streamrune.events.duration}; see
   * {@link #recordAppendDuration}.
   */
  private EventStore.IdempotentAppendResult appendWithKeyTimed(
      StreamId streamId,
      List<EventEnvelope> envelopes,
      Version expectedVersion,
      IdempotencyKey key,
      String commandType) {
    long startNanos = System.nanoTime();
    try {
      return eventStore.appendWithKey(streamId, envelopes, expectedVersion, key, commandType);
    } finally {
      recordAppendDuration(envelopes, startNanos);
    }
  }

  /**
   * Records one {@code streamrune.events.duration} sample for an append that actually carried
   * events. An empty append (the keyed zero-event inbox claim) appends nothing, so counting it
   * would dilute the append-latency distribution with a different operation.
   */
  private void recordAppendDuration(List<EventEnvelope> envelopes, long startNanos) {
    if (envelopes.isEmpty()) {
      return;
    }
    recordMetric(
        () -> metrics.recordEventAppendDuration(System.nanoTime() - startNanos),
        "recordEventAppendDuration");
  }

  /**
   * Acquires the stream's lock and records the time spent waiting. The wait is recorded whether
   * acquisition succeeds or fails (a failed acquisition still consumed wait time up to the
   * timeout), so the lock-wait timer reflects real contention.
   *
   * <p>Reentrant on the SAME thread for the SAME stream, regardless of which {@link
   * AggregateLocker} is configured — see {@link #heldStreamLocks}. When {@code streamId} is already
   * held by this thread, this returns a no-op handle instead of calling the real locker again;
   * otherwise it acquires normally and registers the stream for the duration of the returned
   * handle. A different stream — including another aggregate type with the same id value — is
   * unaffected and still acquires its own lock through the real locker.
   */
  private AutoCloseable acquireLockTimed(StreamId streamId, long startNanos) {
    try {
      Set<StreamId> held = heldStreamLocks.get();
      if (held.contains(streamId)) {
        return NOOP_LOCK;
      }
      AutoCloseable real = locker.acquireLock(streamId, lockTimeout);
      held.add(streamId);
      Thread owner = Thread.currentThread();
      return () -> {
        try {
          real.close();
        } finally {
          held.remove(streamId);
          // Drop the thread's (now empty) set so a pooled thread keeps nothing between commands;
          // only on the owning thread, whose set this is.
          if (held.isEmpty() && Thread.currentThread() == owner) {
            heldStreamLocks.remove();
          }
        }
      };
    } finally {
      recordMetric(() -> metrics.recordLockWait(System.nanoTime() - startNanos), "recordLockWait");
    }
  }

  /**
   * Completes a committed command: records append metrics, rebuilds envelopes with the
   * store-assigned global offsets, and saves a snapshot if due. Called after {@code
   * eventStore.append()} committed, so every failure in here is logged and swallowed — the events
   * are durable and the command must report success.
   */
  private <C extends Command, S extends AggregateState, E extends DomainEvent>
      CommandResult finishCommitted(
          StreamId streamId,
          Decider<C, S, E> decider,
          S stateBeforeDecide,
          List<E> newEvents,
          AggregateHistory history,
          List<EventEnvelope> envelopes,
          EventStore.AppendResult appendResult) {
    try {
      for (int i = 0; i < appendResult.globalOffsets().size(); i++) {
        metrics.recordEventAppended();
      }
    } catch (RuntimeException e) {
      LOG.error(
          "Metrics recordEventAppended failed after commit on stream {}",
          LogSanitizer.sanitizeForLog(streamId.value()),
          e);
    }

    List<EventEnvelope> finalEnvelopes = envelopes;
    if (appendResult.globalOffsets().size() != newEvents.size()) {
      LOG.error(
          "Global offset count mismatch after commit on stream {}: expected {} but got {} — "
              + "events are committed; result envelopes keep their placeholder offsets",
          LogSanitizer.sanitizeForLog(streamId.value()),
          newEvents.size(),
          appendResult.globalOffsets().size());
    } else {
      List<EventEnvelope> rebuilt = new ArrayList<>(newEvents.size());
      for (int i = 0; i < newEvents.size(); i++) {
        var original = envelopes.get(i);
        rebuilt.add(
            new EventEnvelope(
                appendResult.globalOffsets().get(i),
                original.streamId(),
                original.version(),
                original.eventType(),
                original.event(),
                original.metadata()));
      }
      finalEnvelopes = rebuilt;
    }

    try {
      maybeSnapshot(streamId, decider, stateBeforeDecide, newEvents, history, finalEnvelopes);
    } catch (RuntimeException e) {
      LOG.error(
          "Snapshot save failed after commit on stream {} — events are committed; "
              + "a later command will retry snapshotting",
          LogSanitizer.sanitizeForLog(streamId.value()),
          e);
    }

    return new CommandResult(
        newEvents,
        streamId,
        appendResult.finalVersion(),
        appendResult.globalOffsets(),
        List.copyOf(finalEnvelopes));
  }

  /**
   * Gates {@link #publishToDeadLetterQueue} on {@link #dlqEligible}: a business rejection (e.g. a
   * {@link DomainException} thrown by {@code decider.guard}/{@code decide}) is a correct, final
   * answer from the domain, not a transient or infrastructural fault — dead-lettering it would let
   * DLQ retry infrastructure blindly replay the command later and re-run a decision the domain
   * already, correctly rejected. Ineligible failures are neither published nor counted as
   * dead-lettered; they are logged and still propagate exactly as eligible ones do.
   */
  private void maybePublishToDeadLetterQueue(
      Command command,
      CommandId commandId,
      StreamId streamId,
      Exception exception,
      int attempts,
      Instant timestamp,
      IdempotencyKey key) {
    if (!dlqEligible.test(exception)) {
      LOG.info(
          "Not dead-lettering command {} ({}): {} is a business rejection, not an "
              + "infrastructure failure — replaying it would re-run a decision the domain "
              + "already, correctly rejected",
          LogSanitizer.sanitizeForLog(commandId.value()),
          command.getClass().getSimpleName(),
          exception.getClass().getSimpleName());
      return;
    }
    publishToDeadLetterQueue(command, commandId, streamId, exception, attempts, timestamp, key);
  }

  private void publishToDeadLetterQueue(
      Command command,
      CommandId commandId,
      StreamId streamId,
      Exception exception,
      int attempts,
      Instant timestamp,
      IdempotencyKey key) {
    if (deadLetterQueue == null) return;
    if (DLQ_REPLAY.isBound() && Boolean.TRUE.equals(DLQ_REPLAY.get())) {
      // This execution replays an existing DLQ entry; the retry infrastructure tracks attempts
      // on the original entry. Publishing again would add a new entry on every retry cycle.
      LOG.debug(
          "Skipping DLQ publish for command {} ({}) — execution replays an existing DLQ entry",
          LogSanitizer.sanitizeForLog(commandId.value()),
          command.getClass().getSimpleName());
      return;
    }
    if (StreamRuneContext.isSagaDispatch()) {
      // The reason used to read "the saga compensates on failure", which later
      // became untrue — a forward failure with proven retry-later evidence now propagates for
      // redelivery instead of compensating, and the interceptor veto joined that
      // set. The suppression is still correct, but for the broader reason: the SAGA subsystem, not
      // this queue, owns the recovery of a saga-issued command. Every outcome is covered by one of
      // its own durable channels — compensate (claim-first, terminal), propagate for redelivery
      // (subscription checkpoint unadvanced, re-dispatched under the same deterministic forward
      // key), the saga's own timeout() into SagaTimeoutRunner compensation, or the compensation
      // resume paths bounded by SagaCompensationRetrySweeper's give-up -> FAULTED. A dead-letter
      // entry would add a SECOND, uncoordinated retry channel for the same command, whose replay
      // would re-dispatch a command the saga may already have compensated or superseded.
      LOG.debug(
          "Skipping DLQ publish for command {} ({}) — saga-owned; the saga subsystem owns its"
              + " recovery (compensation, redelivery, timeout, or the compensation resume paths)",
          LogSanitizer.sanitizeForLog(commandId.value()),
          command.getClass().getSimpleName());
      return;
    }
    String commandPayload;
    try {
      commandPayload = objectMapper.writeValueAsString(command);
    } catch (JsonProcessingException | RuntimeException e) {
      // A serialization fault — INCLUDING the crypto module's deliberate fail-fast
      // CryptoOperationException (thrown on a Vault/KMS outage or a null @Encrypted subjectId) —
      // must NEVER fall back to command.toString(): that re-emits raw @Encrypted PII, which either
      // leaks plaintext on a text store or, on PostgresDeadLetterQueue, fails the ?::jsonb cast so
      // the entry is silently DROPPED. Instead persist a metadata-only entry: a valid JSON-null
      // payload (survives the jsonb cast, is retained) plus the command type + error metadata the
      // request already carries, so an operator still sees that a command failed and why — without
      // leaking the payload. Mirrors the saga poison quarantine's metadata-only philosophy.
      commandPayload = METADATA_ONLY_PAYLOAD;
      LOG.warn(
          "Serializing command {} ({}) for the dead letter queue failed ({}); recording a "
              + "metadata-only entry with a JSON-null payload to avoid leaking @Encrypted PII and "
              + "to survive the JSONB payload constraint — never command.toString()",
          LogSanitizer.sanitizeForLog(commandId.value()),
          command.getClass().getSimpleName(),
          e.getClass().getName(),
          e);
    }
    // Capture the bound request context so a replay can rebind the original correlation/user/trace.
    // Without it the replay runs anonymous: events lose correlation and fail-closed authorization
    // rejects @RequireRole commands that originally passed, burning their retries. When no context
    // is bound (e.g. a background command) all three are null and the replay runs unbound.
    CorrelationId correlationId = null;
    UserId userId = null;
    TraceId traceId = null;
    if (StreamRuneContext.CURRENT.isBound()) {
      StreamRuneContext.RequestContext ctx = StreamRuneContext.CURRENT.get();
      correlationId = ctx.correlationId();
      userId = ctx.userId();
      traceId = ctx.traceId();
    }
    try {
      deadLetterQueue.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              commandPayload,
              // Persist the FULLY-QUALIFIED class name (matching what the CommandInbox
              // already stores), so DLQ replay resolves by FQN. The simple name collides silently
              // when two bounded contexts declare same-named commands (e.g. orders.CancelCommand vs
              // billing.CancelCommand) — deserializing one context's payload into the other's
              // decider commits wrong-context events with no error.
              command.getClass().getName(),
              commandId,
              streamId,
              exception.getClass().getName(),
              // dead_letter_queue.error_message is a persisted-text sink; a NUL in the raw
              // message would fail this INSERT and the entry — the command's only recovery channel
              // — would never exist.
              LogSanitizer.sanitizeFreeText(exception.getMessage()),
              attempts,
              timestamp,
              correlationId,
              userId,
              traceId,
              // Persist the caller's original IdempotencyKey (null for unkeyed commands) so the
              // retry runner replays under it instead of a synthetic dlq-replay:<commandId> key.
              // Otherwise a keyed effectively-once command that failed on transient infra would be
              // replayed under a different key, and the client's own at-least-once redelivery under
              // the original key would NOT dedup against it at the inbox — running the handler
              // twice and duplicating the committed domain events (the double-execution defect).
              key));
      recordMetric(
          () -> metrics.recordDeadLetterPublished(command.getClass().getSimpleName()),
          "recordDeadLetterPublished");
    } catch (RuntimeException e) {
      // The original command failure still propagates to the caller; attach the DLQ failure as
      // suppressed so it is visible on the propagated exception, not only in this log line.
      if (e != exception) {
        exception.addSuppressed(e);
      }
      LOG.error(
          "Failed to publish command {} ({}) to the dead letter queue; "
              + "the original command failure still propagates",
          LogSanitizer.sanitizeForLog(commandId.value()),
          command.getClass().getSimpleName(),
          e);
    }
  }

  @SuppressWarnings("unchecked")
  /**
   * Folds the loaded history into aggregate state and emits the reconstruction metrics: {@code
   * streamrune.snapshots.loaded} once when the store actually returned a snapshot to rehydrate
   * from, and {@code streamrune.events.replayed} once per event folded through {@link
   * Decider#evolve} (the events after the snapshot, or the whole stream when no snapshot exists).
   * Emitting here rather than inside one event-store implementation keeps the series live for EVERY
   * {@link EventStore} — Postgres, in-memory and third-party alike — which is exactly what the
   * operator contract in {@code docs/guide/production.md} promises; {@code snapshots.discarded}
   * stays in the store because only the store can observe a discard.
   *
   * <p>The recording is wrapped: this runs on the PRE-commit path, so a throwing metrics collector
   * must never fail (and thus dead-letter/retry) an otherwise valid command.
   */
  private <C extends Command, S extends AggregateState, E extends DomainEvent> S reconstructState(
      Decider<C, S, E> decider, AggregateHistory history) {
    S state;
    if (history.snapshotState() != null) {
      state = (S) history.snapshotState();
    } else {
      state = decider.initialState();
    }
    for (EventEnvelope envelope : history.events()) {
      state = decider.evolve(state, (E) envelope.event());
    }
    try {
      if (history.snapshotState() != null) {
        metrics.recordSnapshotLoaded();
      }
      for (int i = 0; i < history.events().size(); i++) {
        metrics.recordEventReplayed();
      }
    } catch (RuntimeException e) {
      LOG.warn("Metrics recordSnapshotLoaded/recordEventReplayed failed", e);
    }
    return state;
  }

  private <E extends DomainEvent> List<EventEnvelope> buildEnvelopes(
      StreamId streamId, List<E> events, Version currentVersion, CommandId commandId) {
    CorrelationId correlationId = getCorrelationId();
    List<EventEnvelope> envelopes = new ArrayList<>(events.size());
    Version version = currentVersion;
    for (E event : events) {
      version = version.next();
      EventType eventType = EventType.fromClass(event.getClass());
      EventMetadata metadata = buildMetadata(correlationId, commandId);
      envelopes.add(
          new EventEnvelope(GlobalOffset.initial(), streamId, version, eventType, event, metadata));
    }
    return envelopes;
  }

  private CorrelationId getCorrelationId() {
    if (StreamRuneContext.CURRENT.isBound()) {
      return StreamRuneContext.CURRENT.get().correlationId();
    }
    return IdGenerator.generateCorrelationId();
  }

  /**
   * Builds the metadata for one produced event. The timestamp is stamped from the bus {@link
   * #clock} at the moment of envelope construction (append time) — never the {@link
   * StreamRuneContext.RequestContext#timestamp() request-creation instant}. Reusing the
   * request-creation instant gave every event of a request — and every event of a replayed or
   * re-dispatched request bound to the same context — the same stale timestamp, undermining audit
   * ordering. Each call reads the clock afresh, so events of one command, and successive commands
   * under one bound context, receive distinct, monotonically-sensible timestamps. Correlation,
   * causation, trace and user identity are still taken from the bound context.
   */
  private EventMetadata buildMetadata(CorrelationId correlationId, CommandId commandId) {
    EventId eventId = IdGenerator.generateEventId();
    Instant now = clock.instant();

    if (StreamRuneContext.CURRENT.isBound()) {
      StreamRuneContext.RequestContext ctx = StreamRuneContext.CURRENT.get();
      return new EventMetadata(
          eventId,
          commandId,
          ctx.traceId(),
          null,
          correlationId,
          causationIdFrom(ctx),
          ctx.userId(),
          now,
          ctx.baggage());
    }
    return new EventMetadata(eventId, commandId, null, null, correlationId, null, null, now);
  }

  /**
   * Resolves the causation id for produced events. Per the {@link EventMetadata} contract, {@code
   * causationId} references the direct predecessor message (typically the event a saga or
   * subscriber reacted to) — never the command's own id, which lives in {@link
   * EventMetadata#commandId()}. The causing message id is read from the {@link
   * #CAUSATION_ID_BAGGAGE_KEY} baggage entry; when absent, produced events are causation roots.
   */
  private static CausationId causationIdFrom(StreamRuneContext.RequestContext ctx) {
    String causingMessageId = ctx.baggage().get(CAUSATION_ID_BAGGAGE_KEY);
    if (causingMessageId == null || causingMessageId.isBlank()) {
      return null;
    }
    return CausationId.of(causingMessageId);
  }

  @SuppressWarnings("unchecked")
  private <C extends Command, S extends AggregateState, E extends DomainEvent> void maybeSnapshot(
      StreamId streamId,
      Decider<C, S, E> decider,
      S stateBeforeDecide,
      List<E> newEvents,
      AggregateHistory history,
      List<EventEnvelope> envelopes) {
    if (!(snapshotPolicy instanceof SnapshotPolicy.EveryNEvents policy)) {
      return;
    }
    // Events since the last snapshot:
    // - history.events() = events loaded after the last snapshot (all events if none exists)
    // - newEvents = events just appended (not yet in history)
    // For a brand-new aggregate or a command immediately following a snapshot, history.events()
    // is empty (version == lastSnapshotVersion) and the just-appended events alone may already
    // reach the threshold — they must count too.
    long eventsSinceSnapshot = (long) history.events().size() + newEvents.size();
    if (eventsSinceSnapshot >= policy.n()) {
      S finalState = stateBeforeDecide;
      for (E event : newEvents) {
        finalState = decider.evolve(finalState, event);
      }
      Version newVersion = envelopes.getLast().version();
      eventStore.saveSnapshot(streamId, newVersion, finalState, policy.snapshotVersion());
      // The ONLY production snapshot writer — emit streamrune.snapshots
      // .created here, after the store accepted the write, so the series an operator is told to
      // watch when enabling SnapshotPolicy.EveryNEvents actually moves.
      recordMetric(metrics::recordSnapshotCreated, "recordSnapshotCreated");
    }
  }

  /**
   * Resolves the registration for a command deterministically: a registration for the command's
   * exact concrete class always wins; otherwise the registry is scanned in registration order and
   * the first registered type the command is an instance of wins. The registry preserves builder
   * insertion order, so overlapping registrations (e.g. a sealed command interface plus one of its
   * subtypes) never route nondeterministically between JVM runs.
   */
  private DeciderRegistration<?, ? extends AggregateState, ? extends DomainEvent> findRegistration(
      Command command) {
    var exact = registry.get(command.getClass());
    if (exact != null) {
      return exact;
    }
    for (var entry : registry.entrySet()) {
      if (entry.getKey().isInstance(command)) {
        return entry.getValue();
      }
    }
    throw new NoDeciderException(command.getClass(), registry.keySet().stream().toList());
  }

  /**
   * Sleeps for the retry backoff. Returns {@code false} with the interrupt flag restored when
   * interrupted (typically process shutdown) instead of throwing: the caller must still record the
   * abandoned command in the dead letter queue before propagating, and an exception thrown from
   * here would bypass that publish entirely.
   */
  private static boolean sleepForRetryBackoff(Duration duration) {
    try {
      Thread.sleep(duration);
      return true;
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

  /**
   * Closes the command bus: new commands are rejected, and the call blocks until all in-flight
   * commands — including async commands accepted by {@link #executeAsync} whose threads have not
   * started yet — have completed, bounded by a 30-second timeout. Idempotent per the {@link
   * AutoCloseable} convention: a repeated call does not throw; it waits for any remaining in-flight
   * commands and returns.
   *
   * @throws IllegalStateException if in-flight commands do not complete within the timeout
   */
  @Override
  public void close() {
    closed.set(true);
    // Wait for in-flight commands to complete with bounded timeout
    long deadline = System.nanoTime() + CLOSE_TIMEOUT.toNanos();
    while (inFlightCommands.get() > 0) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException(
            "Timed out waiting for "
                + inFlightCommands.get()
                + " in-flight commands to complete during close()");
      }
      try {
        Thread.sleep(Duration.ofMillis(10));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for in-flight commands", e);
      }
    }
  }

  /** Returns true if the command bus has been closed. */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Returns the interceptor chain this bus runs, in {@code before()} order: exactly the
   * interceptors handed to {@link Builder#interceptors}, in the order they were added. Startup
   * validators read it to check the chain a bus actually runs, which for a bus built by hand need
   * not match the interceptor beans of the container.
   *
   * @return the chain, unmodifiable
   */
  public List<CommandInterceptor> interceptors() {
    return interceptors;
  }

  /**
   * Returns the command types registered with this bus through {@link Builder#register}, in
   * registration order.
   *
   * @return the registered command types, unmodifiable
   */
  public Set<Class<?>> registeredCommandTypes() {
    return registry.keySet();
  }

  /** Builder for {@link VirtualThreadCommandBus}. */
  public static final class Builder {

    private static final int DEFAULT_STRIPE_COUNT = 1024;

    /**
     * Default {@code executeAsync} in-flight budget. Chosen as a large number that a well-behaved
     * caller submitting bursts should never brush against in normal operation, while still being a
     * REAL bound — every prior release shipped with no bound at all. Not a measured capacity number
     * for any deployment (see {@code Builder#maxInFlightAsyncCommands} and the production guide's
     * capacity section) — size it to your own measured profile if 10,000 is not appropriate.
     */
    static final int DEFAULT_MAX_IN_FLIGHT_ASYNC_COMMANDS = 10_000;

    private EventStore eventStore;
    private AggregateLocker locker;
    private RetryPolicy retryPolicy = RetryPolicy.DEFAULT;
    private SnapshotPolicy snapshotPolicy = SnapshotPolicy.never();
    private Duration lockTimeout = DEFAULT_LOCK_TIMEOUT;
    private int stripeCount = DEFAULT_STRIPE_COUNT;
    private int maxInFlightAsyncCommands = DEFAULT_MAX_IN_FLIGHT_ASYNC_COMMANDS;
    private ObjectMapper objectMapper;
    private DeadLetterQueue deadLetterQueue;
    private StreamRuneMetrics metrics;
    private final List<CommandInterceptor> interceptors = new ArrayList<>();
    private UnaryOperator<Runnable> asyncTaskWrapper;
    private Clock clock;
    private CommandInbox commandInbox;
    private Predicate<Throwable> dlqEligible;
    private final Map<
            Class<?>, DeciderRegistration<?, ? extends AggregateState, ? extends DomainEvent>>
        registry = new LinkedHashMap<>();
    // The registration rule's view of what has been registered, in registration order.
    private final List<DeciderRegistrations.Registration> acceptedRegistrations = new ArrayList<>();

    Builder() {}

    /** Sets the event store for persistence. */
    public Builder eventStore(EventStore eventStore) {
      this.eventStore = eventStore;
      return this;
    }

    /**
     * Returns the configured event store, or null if not set.
     *
     * @apiNote This method is for internal use by {@link StreamRune.Builder}. It is not intended
     *     for public use.
     */
    EventStore eventStore() {
      return eventStore;
    }

    /** Sets the aggregate locker for concurrency control. */
    public Builder locker(AggregateLocker locker) {
      this.locker = locker;
      return this;
    }

    /** Sets the retry policy for optimistic lock conflicts. */
    public Builder retryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    /** Sets the snapshot policy. */
    public Builder snapshotPolicy(SnapshotPolicy snapshotPolicy) {
      this.snapshotPolicy = snapshotPolicy;
      return this;
    }

    /**
     * Sets the lock timeout for aggregate locking. Defaults to 5 seconds. See {@link
     * AggregateLocker#acquireLock} for the timeout contract every locker honours.
     *
     * @throws NullPointerException if {@code lockTimeout} is null — refused here rather than on the
     *     first command's lock acquisition
     */
    public Builder lockTimeout(Duration lockTimeout) {
      this.lockTimeout = Objects.requireNonNull(lockTimeout, "lockTimeout is required");
      return this;
    }

    /** Sets the stripe count for the default striped locker. Defaults to 1024. */
    public Builder stripeCount(int stripeCount) {
      this.stripeCount = stripeCount;
      return this;
    }

    /**
     * Sets the admission-control budget for {@link #executeAsync}'s in-flight commands. Defaults to
     * {@value #DEFAULT_MAX_IN_FLIGHT_ASYNC_COMMANDS}. Once this many async commands are admitted
     * and not yet complete, a further {@code executeAsync} call returns a future failed with {@link
     * CommandBusOverloadedException} immediately — it does not block waiting for a slot, and
     * nothing is attempted for the rejected command. A command frees its slot before its future
     * completes, so a caller that awaits each result before submitting the next command needs a
     * budget of only one. The synchronous {@link #execute} path is NOT gated by this budget; it is
     * bounded by the caller's own threads instead, exactly as before.
     *
     * <p>This is a memory/queueing safety valve, not a measured throughput or latency SLO — the
     * default is deliberately generic (see the constant's javadoc). Size it to your own measured
     * capacity profile; see the production guide's capacity section.
     *
     * @param maxInFlightAsyncCommands must be {@literal >} 0
     * @throws IllegalArgumentException if not positive
     */
    public Builder maxInFlightAsyncCommands(int maxInFlightAsyncCommands) {
      if (maxInFlightAsyncCommands <= 0) {
        throw new IllegalArgumentException("maxInFlightAsyncCommands must be positive");
      }
      this.maxInFlightAsyncCommands = maxInFlightAsyncCommands;
      return this;
    }

    /**
     * Sets the Jackson ObjectMapper that serializes a command when the bus dead-letters it.
     * Required if deadLetterQueue is configured.
     *
     * <p>This is the one place the framework serializes a command, and {@link
     * DeadLetterRetryRunner} rebuilds the command from that JSON on replay, so a command type that
     * can be dead-lettered must round-trip through the mapper (a {@code record} does by default;
     * see {@link Command}). Nothing checks that when a command is registered. {@link
     * DeadLetterRetryRunner#createObjectMapper} builds a mapper that suits both sides.
     */
    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    /**
     * Sets the dead letter queue for failed commands. When a command exhausts all retry attempts,
     * it is published to the DLQ before the exception propagates.
     */
    public Builder deadLetterQueue(DeadLetterQueue deadLetterQueue) {
      this.deadLetterQueue = deadLetterQueue;
      return this;
    }

    /**
     * Overrides which failures are eligible for the dead letter queue. Defaults to {@link
     * #DEFAULT_DLQ_ELIGIBLE}, which excludes {@link DomainException} and {@link
     * IllegalArgumentException} — business rejections that the domain has already, correctly and
     * finally decided, and which must never be blindly re-appended by a later DLQ replay. A failure
     * for which this predicate returns {@code false} is neither published to the DLQ nor counted as
     * dead-lettered; it still propagates to the caller exactly as an eligible failure does. One
     * failure is never dead-lettered whatever the predicate says: an inbox lookup that failed after
     * the decider rejected a keyed command, because a replay could only repeat the recorded outcome
     * or re-run the rejected decision.
     */
    public Builder dlqEligible(Predicate<Throwable> dlqEligible) {
      this.dlqEligible = dlqEligible;
      return this;
    }

    /** Sets the metrics collector. If not set, metrics are not recorded. */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    /**
     * Adds command interceptors for cross-cutting concerns (audit logging, rate limiting, etc.).
     * Interceptors are called in registration order for {@code before()}, and in reverse order for
     * {@code after()} and {@code onError()}. See {@link CommandInterceptor} for the full lifecycle
     * contract (short-circuiting, error notification, callback isolation).
     *
     * <p>Repeated calls accumulate: interceptors from every call (either overload) run in the order
     * they were added — a later call never silently replaces earlier ones.
     */
    public Builder interceptors(List<CommandInterceptor> interceptors) {
      this.interceptors.addAll(interceptors);
      return this;
    }

    /**
     * Adds command interceptors. Convenience varargs overload; repeated calls accumulate — see
     * {@link #interceptors(List)}.
     */
    public Builder interceptors(CommandInterceptor... interceptors) {
      this.interceptors.addAll(List.of(interceptors));
      return this;
    }

    /**
     * Sets a wrapper applied to the task that {@link #executeAsync} runs on its new virtual thread.
     * The wrapper is invoked on the submitting thread for each async command, so it can capture
     * thread-bound caller context that does not cross {@code Thread.start()} — for example the
     * current OpenTelemetry context, which {@link OpenTelemetryCommandInterceptor} needs to parent
     * the command span instead of starting an orphaned root span:
     *
     * <pre>{@code
     * VirtualThreadCommandBus.builder()
     *     .asyncTaskWrapper(task -> Context.current().wrap(task))
     *     ...
     * }</pre>
     *
     * <p>Not applied to synchronous {@link #execute}, which runs on the caller's thread where the
     * context is already present. The returned runnable must run the task on the thread it is run
     * on; if it throws before running the task, the command's future fails with that throwable.
     */
    public Builder asyncTaskWrapper(UnaryOperator<Runnable> asyncTaskWrapper) {
      this.asyncTaskWrapper = asyncTaskWrapper;
      return this;
    }

    /**
     * Sets the {@link Clock} used to stamp each produced event's {@link EventMetadata#timestamp()}
     * at append time. Defaults to {@link Clock#systemUTC()}. Inject a fixed or stepping clock in
     * tests to assert append-time stamping. The clock is read once per produced event, so events of
     * one command and successive commands under one bound request context receive distinct
     * timestamps rather than sharing the request-creation instant.
     */
    public Builder clock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Sets the {@link CommandInbox} used for idempotent command execution. Required when {@link
     * VirtualThreadCommandBus#execute(Command, IdempotencyKey)} is called; nullable — only
     * validated at call time, not at build time.
     */
    public Builder commandInbox(CommandInbox commandInbox) {
      this.commandInbox = commandInbox;
      return this;
    }

    /**
     * Registers a decider for a command type under an aggregate type, with the id extractor that
     * picks the aggregate. The stream a command writes to is {@code <aggregateType>:<id>}.
     *
     * <p>Dispatch is deterministic when registered types overlap (e.g. a sealed command interface
     * plus one of its subtypes): a command whose exact concrete class was registered uses that
     * registration; otherwise the first registered type (in registration order) that the command is
     * an instance of wins. Several unrelated command types may share an aggregate type — that
     * states that they are one aggregate.
     *
     * @throws IllegalArgumentException at this call, before anything is stored, if {@code
     *     aggregateType} is outside {@link AggregateType#SYNTAX}, {@code commandType} is already
     *     registered, it overlaps a registered command type under a different aggregate type,
     *     {@code decider} is already registered under a different aggregate type, or any argument
     *     is null
     */
    public <C extends Command, S extends AggregateState, E extends DomainEvent> Builder register(
        AggregateType aggregateType,
        Class<C> commandType,
        Function<C, AggregateId> idExtractor,
        Decider<C, S, E> decider) {
      var registration =
          new DeciderRegistration<C, S, E>(aggregateType, commandType, idExtractor, decider);
      var rule = new DeciderRegistrations.Registration(aggregateType, commandType, decider);
      DeciderRegistrations.requireCompatible(acceptedRegistrations, rule);
      acceptedRegistrations.add(rule);
      registry.put(commandType, registration);
      return this;
    }

    /**
     * Registers a decider for a command type under an aggregate type, with an aggregate ID
     * extractor. Alias for register() for clearer intent.
     */
    public <C extends Command, S extends AggregateState, E extends DomainEvent>
        Builder registerDecider(
            AggregateType aggregateType,
            Class<C> commandType,
            Function<C, AggregateId> idExtractor,
            Decider<C, S, E> decider) {
      return register(aggregateType, commandType, idExtractor, decider);
    }

    /**
     * Refuses a registered command type that requires authorization while no {@link
     * AnnotationAuthorizationInterceptor} is among the interceptors — the check {@link #build()}
     * runs. {@link StreamRune.Builder} runs it before it creates the event store, so a refused
     * configuration acquires nothing.
     *
     * @throws IllegalStateException if a registered command would run unguarded
     */
    void requireAuthorizationEnforcement() {
      AuthorizationConfigurationValidator.requireEnforcement(registry.keySet(), interceptors);
    }

    /**
     * Builds the command bus. Requires {@link #eventStore}; a {@link #locker} left unset defaults
     * to an in-process {@link LocalStripedLocker} with {@link #stripeCount} stripes.
     *
     * <p>A registered command type that declares {@code @RequireRole}/{@code @RequirePermission} —
     * on itself, a supertype, or a permitted subtype of a sealed registered type — must be enforced
     * by this bus: the build is refused unless an {@link AnnotationAuthorizationInterceptor} is
     * among the {@link #interceptors}. The rule is checked against what this builder was given, so
     * a bus built by hand is held to it exactly like one a framework integration assembles; see
     * {@link AuthorizationConfigurationValidator}. The interceptor counts only as a member of the
     * chain itself: wrapped in {@link CommandInterceptor#compose} or a decorator it is not seen.
     *
     * @throws IllegalStateException if no event store was set, or if a registered command type
     *     requires authorization and no {@link AnnotationAuthorizationInterceptor} is among the
     *     interceptors — the annotations would be ignored and the command would run unguarded
     * @throws IllegalArgumentException if a dead letter queue is configured without an objectMapper
     *     — without one, {@code publishToDeadLetterQueue} cannot serialize the command, and failing
     *     fast here is clearer than a later NullPointerException that would shadow the original
     *     command failure
     */
    public VirtualThreadCommandBus build() {
      if (eventStore == null) {
        throw new IllegalStateException("eventStore is required");
      }
      if (locker == null) {
        locker = new LocalStripedLocker(stripeCount);
      }
      if (deadLetterQueue != null && objectMapper == null) {
        throw new IllegalArgumentException(
            "objectMapper is required when a dead-letter queue is configured");
      }
      requireAuthorizationEnforcement();
      for (var reg : registry.values()) {
        LOG.info(
            "Registered aggregate type '{}' for command root {} ({} registrations)",
            LogSanitizer.sanitizeForLog(reg.aggregateType().value()),
            reg.commandType().getSimpleName(),
            registry.size());
      }
      return new VirtualThreadCommandBus(
          eventStore,
          locker,
          retryPolicy,
          snapshotPolicy,
          lockTimeout,
          registry,
          objectMapper,
          deadLetterQueue,
          metrics,
          interceptors,
          asyncTaskWrapper,
          clock,
          commandInbox,
          dlqEligible,
          maxInFlightAsyncCommands);
    }
  }
}
