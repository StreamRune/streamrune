package org.streamrune.core;

import java.util.List;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/** Dispatches commands to the appropriate {@link Decider} for processing. */
public interface CommandBus {

  /**
   * Executes the given command synchronously.
   *
   * @param command the command to execute
   * @param <C> command type
   * @return result containing the generated events and metadata
   */
  <C extends Command> CommandResult execute(C command);

  /**
   * Executes {@code command} idempotently under {@code key}: if {@code key} was already processed,
   * the handler does not run and the previously recorded result is returned (with {@link
   * CommandResult#shortCircuited()} true). Otherwise the command runs and its events + the inbox
   * row commit atomically. The honest guarantee is <b>effectively-once</b> for the command's
   * events.
   *
   * <p><b>Interceptors always run, including on a replay.</b> The {@link CommandInterceptor} chain
   * — authorization, validation, audit logging, etc. — executes for every call, whether or not the
   * key was already processed. Only the handler (decider) and persistence (aggregate load, append,
   * projections) are skipped on a hit; an idempotent replay is authorized and audited exactly like
   * a first execution. If an interceptor's {@code before()} vetoes the call, that veto wins over
   * the inbox: the returned result is the short-circuit result naming the vetoing interceptor, not
   * the previously recorded result.
   *
   * <p><b>Key reuse across commands is rejected.</b> {@code key} must always be presented with the
   * same command type it was first recorded under, targeting the same stream. Any other combination
   * throws {@link IllegalArgumentException} instead of returning the stored result — a guessed or
   * accidentally reused key must never leak another command's, or another aggregate's, result
   * metadata. This holds for a CONCURRENT collision as well as a sequential one: two executions of
   * one key can both miss the bus's pre-execution lookup, and {@link EventStore#appendWithKey}'s
   * claim-loser branch enforces the identical check.
   *
   * <p><b>Within one command type on one stream, the key is a bearer token.</b> Whoever presents a
   * recorded key receives that execution's outcome, and their own command is deduplicated away
   * rather than executed — by design for a redelivery. The bus does not compare caller identities
   * (see {@link IdempotencyKey} for why), so an application deriving keys from client input must
   * namespace them per principal or tenant via {@link IdempotencyKey#scopedTo}. Authorization is
   * unaffected: the interceptor chain runs on a replay exactly as on a first execution (above).
   *
   * <p>Implementations without a command inbox throw {@link UnsupportedOperationException}
   * (default).
   */
  default <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
    throw new UnsupportedOperationException("This CommandBus does not support idempotency keys");
  }

  /**
   * Reports whether {@link #execute(Command, IdempotencyKey)} is usable on this bus — i.e. an
   * idempotency inbox is configured so a keyed call can record and look up prior executions.
   * Callers that need to choose between keyed and unkeyed execution (e.g. the runtime's {@code
   * DeadLetterRetryRunner} deciding how to replay a dead-lettered command) must check this
   * capability up front rather than attempting the keyed call and catching an exception: the
   * failure mode for "no inbox configured" is implementation-specific (some throw {@link
   * UnsupportedOperationException}, others {@link IllegalStateException}), and catching by
   * exception type risks mistaking an unrelated failure — e.g. a decider that itself throws {@link
   * UnsupportedOperationException} — for "keyed execution is unsupported," triggering a spurious
   * unkeyed double-execute.
   *
   * <p>Defaults to {@code false}. Implementations that support keyed execution must override this
   * to report {@code true} once (and only once) their idempotency inbox is configured.
   */
  default boolean supportsIdempotentExecution() {
    return false;
  }

  /**
   * Why a {@link CommandResult} short-circuited — i.e. why the handler (decider) did not run this
   * call. These two cases are <b>semantically opposite</b> and must never be conflated:
   *
   * <ul>
   *   <li>{@link #VETOED} — a <b>rejection</b>. A {@link CommandInterceptor}'s {@code before()}
   *       returned {@code false}; the command never executed and no events were produced or
   *       persisted. Map this to an authorization/validation failure (e.g. HTTP 403/409).
   *   <li>{@link #IDEMPOTENT_REPLAY} — a <b>success</b>. The command was already applied by a prior
   *       execution under the same {@link org.streamrune.core.types.IdempotencyKey IdempotencyKey}
   *       and <b>its events were persisted then</b>; this call returns the previously-recorded
   *       outcome without re-running the handler. This is a deduplicated at-least-once redelivery —
   *       map it to success (e.g. HTTP 200 OK), <b>never</b> to a rejection.
   * </ul>
   *
   * <p>Before this discriminator existed, {@link CommandResult#shortCircuited()} alone could not
   * tell these apart, so an application returning an error on {@code shortCircuited() == true}
   * would misreport an idempotent replay (a success) as a rejection. Prefer {@link
   * CommandResult#vetoed()} / {@link CommandResult#idempotentReplay()} over raw {@link
   * CommandResult#shortCircuited()} when the distinction matters.
   */
  enum ShortCircuitReason {
    /** The command ran to completion (or produced no events); this is not a short-circuit. */
    NONE,
    /** An interceptor vetoed the command — a rejection; nothing was executed or persisted. */
    VETOED,
    /**
     * The command was already applied by a prior keyed execution — a success; its events were
     * persisted then and this call returns the recorded outcome.
     */
    IDEMPOTENT_REPLAY
  }

  /**
   * Result of a command execution containing generated events and metadata.
   *
   * <p>A result may also represent a <em>short-circuit</em>: the handler did not run this call.
   * {@link #reason()} discriminates the two unrelated causes — a {@link ShortCircuitReason#VETOED
   * veto} (a rejection) versus an {@link ShortCircuitReason#IDEMPOTENT_REPLAY idempotent replay} (a
   * success whose events were already persisted). Callers <b>must not</b> treat every {@link
   * #shortCircuited()} result as an error: use {@link #vetoed()} for the rejection case and {@link
   * #idempotentReplay()} for the success case. Short-circuited results carry no {@code events}; the
   * former name of the vetoing interceptor or the idempotency-key value is in {@link
   * #shortCircuitedBy()}.
   *
   * <p><b>Events vs envelopes.</b> {@code envelopes} is the authoritative representation: when
   * non-empty it corresponds one-to-one, in order, with {@code events}, and additionally carries
   * the assigned global offsets and event metadata. Producers must leave {@code envelopes} empty
   * only when there is nothing to wrap — a short-circuited result or a command that produced no
   * events. Consumers that need offsets or metadata must read {@link #envelopes()}; {@link
   * #events()} is a convenience view of the raw domain events and must never diverge from the
   * envelope contents when both are populated.
   *
   * @param events the list of domain events produced by the command
   * @param streamId the stream ID where events were appended
   * @param finalVersion the final version of the stream after appending
   * @param globalOffsets the global offsets assigned to each event
   * @param envelopes the fully-constructed {@link EventEnvelope} list with correct global offsets;
   *     populated by the runtime command bus after a successful append. Empty for short-circuited
   *     and zero-event results; otherwise matches {@code events} one-to-one in order
   * @param shortCircuitedBy name of the {@link CommandInterceptor} whose {@code before()}
   *     short-circuited this result, or the {@link org.streamrune.core.types.IdempotencyKey#value()
   *     idempotency key value} when the result replays a prior execution, or {@code null} when the
   *     command actually executed in this call. This is a human/diagnostic label only — to branch
   *     on behaviour use {@link #reason()} (or {@link #vetoed()} / {@link #idempotentReplay()}),
   *     not a string comparison.
   * @param reason the {@link ShortCircuitReason discriminator} explaining why the handler did not
   *     run: {@link ShortCircuitReason#NONE NONE} (the command executed), {@link
   *     ShortCircuitReason#VETOED VETOED} (an interceptor rejected it — no events), or {@link
   *     ShortCircuitReason#IDEMPOTENT_REPLAY IDEMPOTENT_REPLAY} (a prior keyed execution already
   *     persisted the events — this is a success). Never {@code null}.
   */
  record CommandResult(
      List<? extends DomainEvent> events,
      StreamId streamId,
      Version finalVersion,
      List<GlobalOffset> globalOffsets,
      List<EventEnvelope> envelopes,
      String shortCircuitedBy,
      ShortCircuitReason reason) {

    /** Canonical constructor — validates that {@code reason} is non-null. */
    public CommandResult {
      java.util.Objects.requireNonNull(reason, "reason");
    }

    /**
     * Convenience constructor: an executed, not short-circuited result. Defaults {@code
     * shortCircuitedBy} to {@code null} and {@code reason} to {@link ShortCircuitReason#NONE}.
     */
    public CommandResult(
        List<? extends DomainEvent> events,
        StreamId streamId,
        Version finalVersion,
        List<GlobalOffset> globalOffsets,
        List<EventEnvelope> envelopes) {
      this(events, streamId, finalVersion, globalOffsets, envelopes, null, ShortCircuitReason.NONE);
    }

    /**
     * Convenience constructor for an executed result with no events: {@code envelopes} defaults to
     * an empty list and {@code reason} to {@link ShortCircuitReason#NONE}. A result carrying events
     * must use a constructor that takes {@code envelopes}, so consumers can rely on offsets and
     * metadata.
     */
    public CommandResult(
        List<? extends DomainEvent> events,
        StreamId streamId,
        Version finalVersion,
        List<GlobalOffset> globalOffsets) {
      this(events, streamId, finalVersion, globalOffsets, List.of(), null, ShortCircuitReason.NONE);
    }

    /**
     * Returns {@code true} when the handler did not run to produce new events this call: either an
     * interceptor's {@code before()} vetoed it ({@link #vetoed()}), or it was an idempotency replay
     * of a prior execution ({@link #idempotentReplay()}). Short-circuited results never carry
     * events. <b>A {@code true} value does not mean failure</b> — an {@link
     * ShortCircuitReason#IDEMPOTENT_REPLAY} is a success. Branch on {@link #vetoed()} / {@link
     * #idempotentReplay()} (or {@link #reason()}) to tell the two apart.
     */
    public boolean shortCircuited() {
      return reason != ShortCircuitReason.NONE;
    }

    /**
     * Returns {@code true} when this result is a <b>rejection</b>: an interceptor's {@code
     * before()} vetoed the command, so nothing was executed or persisted. Distinct from {@link
     * #idempotentReplay()}.
     */
    public boolean vetoed() {
      return reason == ShortCircuitReason.VETOED;
    }

    /**
     * Returns {@code true} when this result is a <b>successful idempotent replay</b>: the command
     * was already applied by a prior keyed execution and its events were persisted then. This call
     * re-runs no handler and appends nothing, but the outcome ({@code streamId}, {@code
     * finalVersion}, {@code globalOffsets}) is the original success. <b>Not a rejection</b> — do
     * not map this onto an error status.
     */
    public boolean idempotentReplay() {
      return reason == ShortCircuitReason.IDEMPOTENT_REPLAY;
    }
  }
}
