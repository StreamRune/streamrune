package org.streamrune.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdConstraints;
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
   */
  public static final ScopedValue<Boolean> SAGA_OWNED = ScopedValue.newInstance();

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
   * which capture and re-bind in one step.
   */
  public static RequestContext capture() {
    return CURRENT.isBound() ? CURRENT.get() : null;
  }

  /**
   * Returns a {@link Runnable} that runs {@code task} with the {@link RequestContext} bound at the
   * time this method was called. Use this when handing work to another thread ({@code
   * Thread.start()}, executors, {@code CompletableFuture.supplyAsync}), where {@link ScopedValue}
   * bindings are otherwise lost.
   *
   * <p>If no context is bound when this method is called, {@code task} is returned unchanged and
   * runs without a binding.
   *
   * @param task the task to wrap (required)
   * @return a runnable that re-binds the captured context around {@code task}
   */
  public static Runnable wrap(Runnable task) {
    Objects.requireNonNull(task, "task is required");
    if (!CURRENT.isBound()) {
      return task;
    }
    RequestContext captured = CURRENT.get();
    return () -> ScopedValue.where(CURRENT, captured).run(task);
  }

  /**
   * Returns a {@link Callable} that calls {@code task} with the {@link RequestContext} bound at the
   * time this method was called. The {@link Callable} counterpart of {@link #wrap(Runnable)}.
   *
   * <p>If no context is bound when this method is called, {@code task} is returned unchanged and
   * runs without a binding.
   *
   * @param task the task to wrap (required)
   * @param <T> the task's result type
   * @return a callable that re-binds the captured context around {@code task}
   */
  public static <T> Callable<T> wrap(Callable<T> task) {
    Objects.requireNonNull(task, "task is required");
    if (!CURRENT.isBound()) {
      return task;
    }
    RequestContext captured = CURRENT.get();
    return () -> ScopedValue.where(CURRENT, captured).call(task::call);
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
     * <p><b>Use this, not the constructor, whenever the id values are attacker-controlled</b> — all
     * three shipped request filters do, and an application that writes its own edge filter must
     * too. The plain constructor deliberately does not run the check because it is also the
     * <em>reconstruction</em> path for values already at rest (DLQ replay, saga dispatch); see the
     * comment on the compact constructor above and {@link IdConstraints} for the full argument.
     *
     * <p>Rejecting rather than truncating is deliberate (see {@link IdConstraints}): a caller
     * sending a 4 KB "id" is not making a typo, and a truncated identifier is a wrong identifier.
     * The resulting {@link IllegalArgumentException} surfaces at the filter, before any command is
     * dispatched or any event is written.
     *
     * @param traceId the trace id read from the request, or {@code null} when absent
     * @param userId the resolved caller, or {@code null} when anonymous
     * @param correlationId the correlation id read from the request, or a freshly generated one
     *     (required)
     * @param timestamp when the request was received (required)
     * @param baggage request baggage; sanitized and copied like any other context
     * @return a validated context carrying no captured authority
     * @throws IllegalArgumentException if any of the correlation id, trace id or user id exceeds
     *     {@link IdConstraints#MAX_LENGTH} or contains a Unicode control character
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
     * check.
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
