package org.streamrune.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

/**
 * Immutable request context propagated via {@link ScopedValue}. Replaces ThreadLocal for safe,
 * bounded metadata propagation across virtual threads.
 *
 * <p>Note: Reactive support (Project Reactor, Mutiny, RxJava) is deferred. The current
 * implementation supports synchronous processing. Future versions will add reactive context
 * propagation when needed.
 */
public final class StreamRuneContext {

  /**
   * The scoped value carrying the current request context.
   *
   * <p><strong>Async boundary hazard:</strong> {@link ScopedValue} bindings do not propagate across
   * {@code Thread.start()}, thread pools, or {@code CompletableFuture.supplyAsync}. A missed
   * boundary fails silently: the task observes no bound context, so the caller's identity
   * downgrades to anonymous ({@code userId == null}) instead of being rejected — audit entries and
   * event metadata then record no user. Use {@link #wrap(Runnable)} or {@link #wrap(Callable)} to
   * capture the binding before handing work to another thread.
   */
  public static final ScopedValue<RequestContext> CURRENT = ScopedValue.newInstance();

  /**
   * Bound to {@code TRUE} around every command a saga dispatches, forward and compensation alike
   * ({@code SagaCommandDispatch} binds it; {@code VirtualThreadCommandBus.executeAsync} carries it
   * onto its virtual thread). Such a command runs as the system, not as an end user: it is usually
   * dispatched from a subscription poll thread, a saga timeout thread or the compensation retry
   * sweeper, where no user is bound.
   *
   * <p>While it is bound, both shipped authorization interceptors run the command as a trusted
   * system principal (each has an opt-out), the command bus leaves a failure of the command to the
   * saga instead of publishing it to the dead letter queue, {@link
   * Authorization#requireOwnerOrSaga} admits it and {@link Authorization#requireOwner} denies it.
   * Domain code reads it through {@link #isSagaDispatch()}; only the saga dispatch path binds it.
   *
   * <p>It states authority and dead-letter suppression, nothing else. Which saga caused a dispatch
   * is stated by {@link #CAUSE}.
   */
  public static final ScopedValue<Boolean> SAGA_OWNED = ScopedValue.newInstance();

  /**
   * What caused the command dispatch in progress: the causing event, the saga origin, or both.
   *
   * <p>The command bus copies both parts into the metadata of every event the command produces:
   * {@link EventMetadata#causationId()} and {@link EventMetadata#sagaOrigin()}, and takes neither
   * from anywhere else. A request context has no component for either, and the bus reads no baggage
   * entry into a metadata field, so nothing a request carries reaches them.
   *
   * @param causationId the id of the event the dispatch reacts to, or {@code null} when it reacts
   *     to none
   * @param sagaOrigin the saga that caused the dispatch, or {@code null} when no saga did
   */
  public record DispatchCause(CausationId causationId, SagaOrigin sagaOrigin) {

    public DispatchCause {
      if (causationId == null && sagaOrigin == null) {
        throw new IllegalArgumentException(
            "a dispatch cause names a causing event, a saga origin, or both");
      }
    }
  }

  /**
   * The cause of the command dispatch in progress on this thread. It has two binders: saga
   * dispatch, around every command a saga sends ({@code SagaCommandDispatch}), and {@link
   * #continuing(EventEnvelope)}, around the commands a listener dispatches in reaction to an event.
   * {@code VirtualThreadCommandBus.executeAsync} and {@link #wrap(Runnable)} / {@link
   * #wrap(Callable)} carry it onto another thread. Unbound for a command a request, a background
   * job or a dead-letter replay dispatches: the events of such a command carry no saga origin and
   * no causation id.
   *
   * <p>While a cause is bound, {@code VirtualThreadCommandBus} publishes a failed command to no
   * dead letter queue. Whoever bound the cause retries the command: the saga subsystem for a saga's
   * dispatch, the listener's subscription for a continuation.
   *
   * <p>Application code reads it through {@link #cause()} and binds it only through {@link
   * #continuing(EventEnvelope)}.
   */
  public static final ScopedValue<DispatchCause> CAUSE = ScopedValue.newInstance();

  /**
   * Returns the {@link DispatchCause} bound to the current thread, or {@code null} when none is
   * bound.
   *
   * @return the cause of the dispatch in progress, or {@code null}
   */
  public static DispatchCause cause() {
    return CAUSE.isBound() ? CAUSE.get() : null;
  }

  /**
   * A saga's forward command in dispatch: the saga, and the idempotency key of that one command.
   *
   * @param origin the saga whose forward command is dispatched
   * @param key the idempotency key the command is executed under
   */
  public record SagaForward(SagaOrigin origin, IdempotencyKey key) {

    public SagaForward {
      Objects.requireNonNull(origin, "origin is required");
      Objects.requireNonNull(key, "key is required");
    }
  }

  /**
   * Bound by saga dispatch around a saga's forward command, and around no other command: not around
   * a compensation command, not by {@link #continuing(EventEnvelope)}. The command bus asks it,
   * through {@link #sagaForwardOf(IdempotencyKey)}, which saga to fence the keyed append of that
   * command against: the event store then refuses the append once the saga has left its forward
   * phase (see {@code EventStore.appendWithKey} with a {@link SagaOrigin}).
   *
   * <p>It names one command on one thread: {@link #wrap(Runnable)}, {@link #wrap(Callable)} and
   * {@code VirtualThreadCommandBus.executeAsync} do not carry it, and it answers only for the key
   * it names, so a keyed command dispatched inside the forward command (by an interceptor or an
   * inline projection) is not fenced on the saga's behalf.
   *
   * <p>A {@code CommandBus} decorator that runs the command on another thread, or binds the scoped
   * values itself, leaves the marker behind on the dispatching thread: the event store then
   * receives no saga to fence, and the runner's wiring check cannot see it. A decorator in front of
   * a fencing bus calls it on the calling thread.
   */
  public static final ScopedValue<SagaForward> SAGA_FORWARD = ScopedValue.newInstance();

  /**
   * The saga to fence the keyed append of {@code key} against, or {@code null}: answers only for
   * the bound key.
   *
   * @param key the idempotency key of the command whose append is about to run
   * @return the bound {@link SagaForward#origin()} when {@link #SAGA_FORWARD} is bound to a value
   *     whose key equals {@code key}; {@code null} when {@code key} is {@code null}, nothing (or
   *     {@code null}) is bound, or the bound marker names another key
   */
  public static SagaOrigin sagaForwardOf(IdempotencyKey key) {
    if (key == null || !SAGA_FORWARD.isBound()) {
      return null;
    }
    SagaForward forward = SAGA_FORWARD.get();
    return forward != null && forward.key().equals(key) ? forward.origin() : null;
  }

  /**
   * Bindings for commands dispatched in reaction to {@code cause}, an event read from the store. A
   * listener that reacts to an event by sending a command (a process manager) runs the dispatch
   * inside them:
   *
   * <pre>{@code
   * StreamRuneContext.continuing(event).run(() -> commandBus.execute(command, key));
   * }</pre>
   *
   * <p><b>What the command inherits from the event.</b> The command bus writes four values of the
   * causing event into the metadata of every event the command produces:
   *
   * <ul>
   *   <li>the correlation id and the trace id, unchanged: one business flow keeps one correlation
   *       id and one trace. For an event a saga caused that is the saga's reserved correlation id
   *       ({@link SagaOrigin#correlationId()}), which only this method and saga dispatch bind, and
   *       which this method takes from the event's origin;
   *   <li>the causing event's id, as the causation id ({@link EventMetadata#causationId()});
   *   <li>the causing event's saga origin, one hop further ({@link SagaOrigin#nextHop()}), or no
   *       origin when the causing event carries none.
   * </ul>
   *
   * <p><b>What it does not inherit.</b> A continuation runs like any command dispatched from a
   * background thread with no user. The bound request context has no user, no captured authority
   * and empty baggage, whatever the causing event's metadata and the calling thread's own context
   * hold: the user who caused the first event did not issue this command, and a baggage {@code
   * role} is authority too. {@link #SAGA_OWNED} is bound to {@code FALSE}, so {@link
   * #isSagaDispatch()} is {@code false} even when the listener runs on the thread of a saga's
   * dispatch, and no authorization interceptor treats the command as the system's. A saga origin
   * states who caused an event; it grants nothing. An application whose continuation must pass an
   * authorization gate decides that in its own policy, which can read {@link #cause()}. Such a
   * policy reads the cause's saga origin, not the mere presence of a cause: a cause without an
   * origin says only that the command continues some stored event, and nothing about who wrote that
   * event — a client's own request stores events too.
   *
   * <p><b>The envelope is trusted as given.</b> This method copies the event id, the correlation
   * id, the trace id and the saga origin out of {@code cause} and checks none of them. It is meant
   * for an envelope read from the event store, which is what a subscription delivers: the command
   * bus wrote that metadata. An envelope built by hand, or from request data, carries whatever
   * origin, causation id and correlation id its author wrote, and the framework vouches for none of
   * it. Two combinations no command bus writes are settled here and at the bus: for an event with
   * an origin and another correlation id, this method binds the origin's correlation id, so the
   * bound context, the command audit and the events of the command name the same saga; for an event
   * with a reserved correlation id and no origin, the bus refuses the command with a {@link
   * ReservedCorrelationIdException}.
   *
   * <p><b>A failure belongs to the caller.</b> {@code VirtualThreadCommandBus} publishes a command
   * dispatched inside these bindings to no dead letter queue, whether or not the causing event
   * carries a saga origin: a dead-letter entry has no place for the causing event or for an origin,
   * and its replay would store the command's events without either. The listener's subscription is
   * the one thing that runs the command again, so what the listener does with a failure decides the
   * command's fate:
   *
   * <ul>
   *   <li>A failure a later attempt can cure — an unreachable store, a lock timeout — leaves the
   *       listener: the subscription keeps its checkpoint and delivers the causing event again. A
   *       listener that catches such a failure and returns loses the command.
   *   <li>A failure no later attempt cures is the command's answer: a rejection by the domain
   *       ({@link DomainException} and its validation and authorization subtypes, {@link
   *       IllegalArgumentException}). The bus dead-letters a rejection under no rule, and a plain
   *       {@link org.streamrune.core.subscription.EventListener} has no skip or dead-letter
   *       handling for the event, so a listener that lets a rejection propagate is handed the same
   *       event again until someone intervenes. Nothing is dead-lettered for it; what shows it is
   *       the subscription — the {@code ERROR} line its poll loop logs for every attempt, its count
   *       of consecutive failures, and its health entry where the subscription is registered with a
   *       {@code SubscriptionHealthContributor}. Decide it in the listener: catch the rejection and
   *       return, which gives the command up on purpose, or dispatch the command that records the
   *       refusal. Skipping or dead-lettering the <em>event</em> exists one level up, for a
   *       projection run by a projection runner ({@link
   *       org.streamrune.core.projection.ProjectionErrorStrategy}).
   *   <li>Under {@code executeAsync}, or in a task handed to another thread through {@link
   *       #wrap(Runnable)}, the failure reaches only the returned future, or only that thread. A
   *       listener that relies on redelivery waits for the outcome before it returns; a failure it
   *       does not see is a failure it swallowed.
   * </ul>
   *
   * <p><b>Parking a failed command instead.</b> A listener that prefers to move on and leave a
   * failed command to the dead letter queue dispatches it outside these bindings, under a request
   * context of its own, and catches the failure. That command is an ordinary one: the bus
   * dead-letters it, and its events carry no causation id and no saga origin, so a saga that caused
   * the event the listener reacted to does not receive them by origin.
   *
   * <p><b>Reruns.</b> Everything bound here except the context's timestamp is a function of the
   * stored event, so a redelivery of that event binds the same saga origin and causation id. A
   * delivery whose command failed stored nothing, so however many deliveries fail, the delivery
   * that succeeds stores the command's events once. Give the command an idempotency key derived
   * from the causing event: a redelivery after the command committed — the listener, or the
   * process, stopped between the command's commit and the subscription's checkpoint — is then
   * answered from the command inbox and stores no second event. Without a key that redelivery
   * executes the command again, with the same origin and causation id.
   *
   * @param cause the event the dispatched commands react to, as read from the event store
   *     (required); its metadata is trusted as given
   * @return the bindings to run the dispatch in
   * @throws NullPointerException if {@code cause} is {@code null}
   * @throws IllegalStateException if the saga origin of {@code cause} is at the hop limit ({@link
   *     SagaOrigin#nextHop()})
   */
  public static ScopedValue.Carrier continuing(EventEnvelope cause) {
    Objects.requireNonNull(cause, "cause is required");
    EventMetadata metadata = cause.metadata();
    SagaOrigin origin = metadata.sagaOrigin();
    // For an event the command bus wrote the two are the same value. Taking the origin's keeps
    // the bound context and the bound cause naming one saga for metadata no bus writes.
    CorrelationId correlationId =
        origin == null ? metadata.correlationId() : origin.correlationId();
    RequestContext context =
        new RequestContext(metadata.traceId(), null, correlationId, Instant.now(), Map.of(), null);
    return ScopedValue.where(CURRENT, context)
        .where(SAGA_OWNED, Boolean.FALSE)
        .where(
            CAUSE,
            new DispatchCause(
                CausationId.of(metadata.eventId().value()),
                origin == null ? null : origin.nextHop()));
  }

  /**
   * Refuses the dispatch in progress on this thread when the bound correlation id is in the
   * reserved form ({@link IdConstraints#isReserved(String)}) and is not the bound saga origin's
   * ({@link SagaOrigin#correlationId()}): when no origin is bound, or when the origin bound names
   * another saga.
   *
   * <p>A reserved correlation id is bound in two places, saga dispatch and {@link
   * #continuing(EventEnvelope)}, and both bind the saga's origin with it; {@link #wrap(Runnable)},
   * {@link #wrap(Callable)} and {@code VirtualThreadCommandBus.executeAsync} carry the two
   * together. So this check passes for everything the framework binds, and what it refuses was
   * bound by something else. With it, and with the buses taking the correlation id of an event from
   * a bound origin, what a shipped bus records carries a reserved correlation id if and only if a
   * saga origin was bound, and then that origin's: the stored events, the command audit and the
   * audit of a query.
   *
   * <p>The shipped buses call it first in every dispatch — {@code VirtualThreadCommandBus}, {@code
   * SimpleQueryBus}, {@code CachingQueryBus}, {@code AuditingQueryBus}, and the in-memory buses of
   * the test module — before a handler is looked up, before an interceptor, an authorizer or a
   * cache is reached, and before anything is stored or audited. What a refusal leaves behind is the
   * {@code WARN} line of the production buses and, for a command, its count: {@code
   * VirtualThreadCommandBus} counts the command as dispatched before the check and as failed after
   * it; the query buses count nothing for a refused query. A command or query bus of your own calls
   * it at the same point — and so does a hand-written test double of {@code CommandBus} or {@code
   * QueryBus}, which is such a bus: a double that skips the call accepts in a unit test what every
   * shipped bus refuses in production.
   *
   * <p>It passes when no context is bound, and when {@link #CURRENT} is bound to {@code null}
   * (which {@code ScopedValue.where(CURRENT, capture())} does on a thread with nothing bound).
   *
   * @throws ReservedCorrelationIdException if the bound correlation id is reserved and is not the
   *     bound saga origin's; the message never holds the bound value
   */
  public static void requireNoForeignReservedCorrelationId() {
    RequestContext context = capture();
    if (context == null || !IdConstraints.isReserved(context.correlationId().value())) {
      return;
    }
    DispatchCause cause = cause();
    SagaOrigin origin = cause == null ? null : cause.sagaOrigin();
    if (origin == null || !origin.correlationId().equals(context.correlationId())) {
      throw new ReservedCorrelationIdException();
    }
  }

  /**
   * Whether the command executing on this thread was dispatched by a saga ({@link #SAGA_OWNED} is
   * bound to {@code TRUE}). Use it in {@link Decider#guard} or an interceptor to treat saga
   * commands differently from user commands, which carry the caller in {@link #CURRENT}.
   *
   * @return {@code true} inside a saga dispatch, {@code false} otherwise
   */
  public static boolean isSagaDispatch() {
    return SAGA_OWNED.isBound() && Boolean.TRUE.equals(SAGA_OWNED.get());
  }

  /**
   * Returns the {@link RequestContext} bound to the current thread, or {@code null} when none is
   * bound.
   *
   * <p>For crossing async boundaries prefer {@link #wrap(Runnable)} / {@link #wrap(Callable)},
   * which capture and re-bind in one step, and carry the {@link DispatchCause} with the context. A
   * context captured here inside a saga's dispatch, or inside a continuation of a saga-caused
   * event, holds a reserved correlation id; rebound on another thread without the cause, it makes
   * the command and query buses refuse what is dispatched under it ({@link
   * #requireNoForeignReservedCorrelationId()}).
   */
  public static RequestContext capture() {
    return CURRENT.isBound() ? CURRENT.get() : null;
  }

  /**
   * Returns a {@link Runnable} that runs {@code task} with the {@link RequestContext} ({@link
   * #CURRENT}) and the {@link DispatchCause} ({@link #CAUSE}) bound at the time this method was
   * called, each when it was bound. Use this when handing work to another thread ({@code
   * Thread.start()}, executors, {@code CompletableFuture.supplyAsync}), where {@link ScopedValue}
   * bindings are otherwise lost.
   *
   * <p>If neither is bound when this method is called, {@code task} is returned unchanged and runs
   * without a binding.
   *
   * @param task the task to wrap (required)
   * @return a runnable that re-binds the captured context and cause around {@code task}
   */
  public static Runnable wrap(Runnable task) {
    Objects.requireNonNull(task, "task is required");
    ScopedValue.Carrier bindings = capturedBindings();
    if (bindings == null) {
      return task;
    }
    return () -> bindings.run(task);
  }

  /**
   * Returns a {@link Callable} that calls {@code task} with the {@link RequestContext} ({@link
   * #CURRENT}) and the {@link DispatchCause} ({@link #CAUSE}) bound at the time this method was
   * called, each when it was bound. The {@link Callable} counterpart of {@link #wrap(Runnable)}.
   *
   * <p>If neither is bound when this method is called, {@code task} is returned unchanged and runs
   * without a binding.
   *
   * @param task the task to wrap (required)
   * @param <T> the task's result type
   * @return a callable that re-binds the captured context and cause around {@code task}
   */
  public static <T> Callable<T> wrap(Callable<T> task) {
    Objects.requireNonNull(task, "task is required");
    ScopedValue.Carrier bindings = capturedBindings();
    if (bindings == null) {
      return task;
    }
    return () -> bindings.call(task::call);
  }

  /**
   * The bindings {@link #wrap(Runnable)} and {@link #wrap(Callable)} carry to another thread:
   * {@link #CURRENT} and {@link #CAUSE}, each when bound on the calling thread; {@code null} when
   * neither is.
   */
  private static ScopedValue.Carrier capturedBindings() {
    ScopedValue.Carrier bindings = null;
    if (CURRENT.isBound()) {
      bindings = ScopedValue.where(CURRENT, CURRENT.get());
    }
    if (CAUSE.isBound()) {
      DispatchCause cause = CAUSE.get();
      bindings = bindings == null ? ScopedValue.where(CAUSE, cause) : bindings.where(CAUSE, cause);
    }
    return bindings;
  }

  /**
   * Immutable container for request metadata.
   *
   * @param traceId OpenTelemetry Trace ID (nullable)
   * @param userId who triggered the action (nullable)
   * @param correlationId identifies the entire business process (required)
   * @param timestamp when the request was created (required)
   * @param baggage arbitrary key-value pairs; defaults to empty map if null
   * @param authority the authorities {@link UserRoleResolver#resolve(UserId)} returned for {@link
   *     #userId()} while the request was still in flight, or {@code null} when none was captured.
   *     <p><b>Why this exists.</b> Every shipped {@link UserRoleResolver} reads its framework's
   *     request-scoped security state (Spring's {@code SecurityContextHolder} ThreadLocal,
   *     Quarkus's request-scoped {@code SecurityIdentity}, Micronaut's {@code SecurityService}).
   *     None of that follows a command onto {@code executeAsync}'s virtual thread, so an
   *     off-request re-resolve returns {@link UserAuthority#EMPTY} and a legitimately authorized
   *     caller is denied. Capturing the answer once, at the request edge, into the same object that
   *     already crosses every async boundary makes off-request execution decide exactly as the
   *     request thread did — uniformly on all three integrations, with no framework-specific
   *     context propagation. Integrations populate it only when {@link
   *     UserRoleResolver#requiresRequestContext()} is {@code true}; a resolver that is a pure
   *     function of {@code userId} needs no capture and gets none.
   *     <p><b>Trust and lifetime — read before using this field.</b> It is process-local, trusted,
   *     in-memory state with the same trust level as {@link #userId()} itself (which {@code
   *     Authorization.requireOwner} already authorizes against directly). It is deliberately
   *     <em>never</em> persisted: {@code DeadLetterQueue} stores {@code userId}/{@code
   *     correlationId}/{@code traceId} and NOT this field, so a role set can never outlive the
   *     process that resolved it, be replayed from a database row hours later, or be read back from
   *     a store an operator can write to. Off-request execution that must cross a durability
   *     boundary (DLQ replay) is handled by the recorded system-principal path in {@code
   *     AnnotationAuthorizationInterceptor}, not by resurrecting a stale authority.
   *     <p>{@code AnnotationAuthorizationInterceptor} honours a captured authority only when it was
   *     captured for the very {@code userId} the context carries, so it can never be applied to a
   *     different principal.
   */
  public record RequestContext(
      TraceId traceId,
      UserId userId,
      CorrelationId correlationId,
      Instant timestamp,
      Map<String, String> baggage,
      UserAuthority authority) {

    public RequestContext {
      if (correlationId == null) {
        throw new IllegalArgumentException("correlationId is required");
      }
      if (timestamp == null) {
        throw new IllegalArgumentException("timestamp is required");
      }
      // WHY THE ID BOUND IS NOT ENFORCED HERE. This constructor is not an ingress
      // boundary; it only looked like one. It is ALSO the reconstruction path: the DLQ retry runner
      // rebuilds a context from the persisted dead_letter_queue columns to replay an entry, and
      // SagaCommandDispatch rebuilds one from a saga id that PostgresSagaStore read back out of
      // saga_state. Running IdConstraints here therefore re-created, one call further down, exactly
      // the defect already removed from CorrelationId/TraceId: a value already persisted
      // through a path that never crossed the ingress door (this constructor called directly, a
      // saga-derived correlation id) became unreadable, so a DLQ entry carrying it burned its whole
      // retry ladder on a deterministic IllegalArgumentException and was permanently discarded, and
      // an in-flight saga whose id carried it terminally FAILED with its undo never dispatched.
      //
      // The bound lives on the ingress DOOR instead — RequestContext#fromRequest, which every
      // framework request filter calls with the values it just read off the wire (IdConstraints).
      //
      // The reserved correlation prefix is a rule of that door for the same reason: saga dispatch
      // builds its context here with the reserved form of the saga id, and continuing() copies a
      // stored event's correlation id through here. Checked here, a saga could dispatch nothing.

      // The value policy is enforced HERE, not only in
      // BaggageAllowlist.filter. filter() is the shared key-allowlist choke point the three
      // framework request filters call — but each of them then adds the X-User-Role header (in the
      // trusted-gateway mode) into baggage under the `role` key AFTER the filter has run,
      // past its value policy. This constructor is the one boundary every path crosses on the way
      // to EventMetadata#baggage and the append-only, plaintext event_stream.metadata column, so
      // capping and de-controlling values here closes the bypass for all three frameworks and for
      // any application code that builds a context itself. Also returns an immutable copy.
      //
      // And this one may stay in the constructor precisely because it SANITIZES rather
      // than REJECTS. Reconstruction can survive a transformation (baggage is telemetry: a dropped
      // or capped entry loses information but breaks nothing), whereas it cannot survive a
      // rejection, and it must not survive a rewrite — a truncated or scrubbed correlation id is a
      // DIFFERENT id, which would silently break the very correlation the id exists to provide and
      // orphan the replayed command from the events it already produced. That asymmetry is the
      // whole reason the two halves of the baggage value policy now live in two places.
      baggage = BaggageAllowlist.sanitize(baggage);
    }

    /**
     * Builds a context from values that have just ENTERED the system — the {@code X-Correlation-Id}
     * / {@code X-Trace-Id} / {@code X-User-Id} headers of an inbound HTTP request, the identity an
     * authenticated principal resolved to, or the equivalent metadata of an inbound RPC or message.
     * This is the ingress door, and the only place the {@link IdConstraints} length-and-charset
     * bound on these header values is enforced. (The ingress factories an application calls itself
     * apply it too: {@code AggregateId.of} the whole bound, {@code IdempotencyKey.of}/{@code
     * scopedTo} its charset arm, {@link IdConstraints#requireNoControlCharacters}.)
     *
     * <p><b>All three attacker-influenced values are bounded here, identity included.</b> {@code
     * userId} was the one this door originally waved through, on the unstated assumption that an
     * identity is trustworthy because it is authenticated. It is not the same thing: in
     * header-identity mode ({@code X-User-Id} with no resolver wired, or {@code
     * trust-user-id-header=true}) it is client-supplied outright, and in authenticated mode it is
     * whatever the identity provider asserted — a JWT {@code sub}, a self-service signup username.
     * "Authenticated" says who sent it, not that it is bounded or charset-safe. It then travels the
     * identical path the other two bounds exist to protect ({@code EventMetadata.userId} into
     * plaintext, immutable, unshreddable {@code event_stream.metadata}), plus one they share and
     * one they do not: an identity longer than {@link IdConstraints#MAX_LENGTH} cannot be written
     * to {@code dead_letter_queue.user_id VARCHAR(255)} at all, and that INSERT failure is caught
     * and logged rather than propagated — so an accepted command's ONLY recovery channel is
     * silently never created.
     *
     * <p><b>Why rejected and not truncated.</b> Baggage is truncated on the constructor because it
     * is telemetry: a capped value loses information and breaks nothing. An identity is not
     * telemetry. Two distinct principals that share a 255-character prefix truncate to the SAME
     * identity, and {@code Authorization.requireOwner} authorizes against exactly this field — so
     * truncating would silently convert an over-long identity into a different, possibly existing
     * user's, which is an authorization decision made on a value nobody chose. Rejection surfaces
     * at the filter as a client error, before any command is dispatched or any event is written.
     *
     * <p><b>The reserved correlation prefix.</b> A correlation id that starts with {@value
     * IdConstraints#RESERVED_PREFIX} is refused here as well ({@link IdConstraints#isReserved}; an
     * exact, case-sensitive prefix match). That form is the framework's own: saga dispatch binds
     * {@code streamrune:saga:<sagaId>} around a saga's commands ({@link
     * SagaOrigin#correlationId()}), and {@link StreamRuneContext#continuing(EventEnvelope)}
     * inherits it from a saga-caused event. A request that sends it — a client, or an upstream
     * service forwarding the correlation id it read from a saga-caused event — is refused; an
     * upstream sends an id of its own. Every other value within the bound is kept verbatim, so an
     * upstream's correlation ids keep tracing across services. The prefix is reserved for the
     * correlation id only: a trace id, a user id and a baggage value may start with it.
     *
     * <p><b>Use this, not the constructor, whenever the id values are attacker-controlled</b> — all
     * three shipped request filters do, and an application that writes its own edge filter must
     * too. The plain constructor deliberately runs none of these checks because it is also the
     * <em>reconstruction</em> path for values already at rest (DLQ replay, saga dispatch, a
     * continuation); see the comment on the compact constructor above and {@link IdConstraints} for
     * the full argument. An ingress that builds its context with the constructor from values it
     * read off the wire therefore admits an unbounded id, a control character and the reserved
     * prefix. For the prefix the shipped command and query buses are a second door: they refuse a
     * command or a query whose bound correlation id is reserved and is not a bound saga origin's,
     * before any interceptor runs and before anything is stored or audited ({@link
     * StreamRuneContext#requireNoForeignReservedCorrelationId()}), so such a request fails at its
     * first command or query with a {@link ReservedCorrelationIdException} the application maps
     * itself. An ingress that neither calls this method nor dispatches through a shipped bus — it
     * appends to the event store directly, or uses a bus of its own that does not make that call —
     * can store a reserved correlation id on an event that carries no saga origin. No framework
     * decision reads a correlation id, so that matters to application code that keys on correlation
     * ids, which it should not.
     *
     * <p>Rejecting rather than truncating is deliberate (see {@link IdConstraints}): a caller
     * sending a 4 KB "id" is not making a typo, and a truncated identifier is a wrong identifier.
     * The resulting {@link IllegalArgumentException} surfaces at the filter, before any command is
     * dispatched or any event is written; the three shipped request filters answer it with {@code
     * 400 Bad Request}.
     *
     * @param traceId the trace id read from the request, or {@code null} when absent
     * @param userId the resolved caller, or {@code null} when anonymous
     * @param correlationId the correlation id read from the request, or a freshly generated one
     *     (required)
     * @param timestamp when the request was received (required)
     * @param baggage request baggage; sanitized and copied like any other context
     * @return a validated context carrying no captured authority
     * @throws IllegalArgumentException if any of the correlation id, trace id or user id exceeds
     *     {@link IdConstraints#MAX_LENGTH} or contains a Unicode control character, or if the
     *     correlation id starts with the reserved prefix; the message names the field and never
     *     echoes the value
     */
    public static RequestContext fromRequest(
        TraceId traceId,
        UserId userId,
        CorrelationId correlationId,
        Instant timestamp,
        Map<String, String> baggage) {
      // X-Correlation-Id and X-Trace-Id are unauthenticated and
      // reach event_stream.metadata (plaintext, immutable, outside crypto-shredding's reach) on
      // every event this request produces, audit_log.correlation_id (unbounded TEXT) and the
      // VARCHAR(255) request-context columns of dead_letter_queue. Unbounded that is a
      // write-amplified storage-and-disclosure channel with no erasure path; a NUL breaks the jsonb
      // metadata write outright and CR/LF forge log lines. Stopping it HERE stops the write itself,
      // which is the only point at which stopping it accomplishes anything.
      if (correlationId != null) {
        IdConstraints.requireBoundedAndPrintable(correlationId.value(), "correlationId");
        // The reserved prefix marks a correlation id the framework itself bound (saga dispatch, and
        // a continuation of a saga-caused event). A request never supplies one. The value is not
        // echoed, like the two refusals above.
        if (IdConstraints.isReserved(correlationId.value())) {
          throw new IllegalArgumentException(
              "correlationId uses the reserved '" + IdConstraints.RESERVED_PREFIX + "' prefix");
        }
      }
      if (traceId != null) {
        IdConstraints.requireBoundedAndPrintable(traceId.value(), "traceId");
      }
      // The third header-sourced value, and the one this door used to skip. Same
      // channels as the two above, plus the one that makes it worse than a disclosure vector: an
      // identity over MAX_LENGTH cannot be written to dead_letter_queue.user_id VARCHAR(255), and
      // that INSERT failure is caught and logged inside VirtualThreadCommandBus rather than
      // propagated — so the accepted command's only recovery channel is silently never created.
      // The bound is enforced here and NOT on UserId itself: that type is rebuilt from stored
      // columns by the DLQ, command-audit and event-audit row mappers, and a constructor bound
      // there would reproduce that same defect exactly, failing every retry of a stored entry whose
      // user id
      // the bound refuses (one bound through the plain constructor) deterministically and burning
      // its whole ladder.
      if (userId != null) {
        IdConstraints.requireBoundedAndPrintable(userId.value(), "userId");
      }
      return new RequestContext(traceId, userId, correlationId, timestamp, baggage, null);
    }

    /**
     * Convenience constructor for a context that carries no captured {@link #authority()}.
     * Authorization then resolves through the configured {@link UserRoleResolver} when the decision
     * is made. Like the canonical constructor, it does not run {@link #fromRequest}'s ingress
     * checks: it accepts an id of any length, a control character and a correlation id with the
     * reserved prefix. Build a context from request input with {@link #fromRequest}.
     */
    public RequestContext(
        TraceId traceId,
        UserId userId,
        CorrelationId correlationId,
        Instant timestamp,
        Map<String, String> baggage) {
      this(traceId, userId, correlationId, timestamp, baggage, null);
    }

    /**
     * Returns a copy of this context carrying {@code capturedAuthority}. Called at the request edge
     * by each integration's request filter, while the framework's request-scoped security state is
     * still readable.
     *
     * <p>A copy is not an ingress: it re-uses ids this context already carries, so it goes through
     * the plain constructor and does not re-run {@link #fromRequest}'s check. That matters beyond
     * tidiness — a reconstructed context (DLQ replay, saga dispatch) that later has an authority
     * attached must not be rejected for the provenance of the id it faithfully preserved.
     *
     * @param capturedAuthority the authority resolved for {@link #userId()}; {@code null} clears it
     * @return a copy with the given authority
     */
    public RequestContext withAuthority(UserAuthority capturedAuthority) {
      return new RequestContext(
          traceId, userId, correlationId, timestamp, baggage, capturedAuthority);
    }
  }

  private StreamRuneContext() {}
}
