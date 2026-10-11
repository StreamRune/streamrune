package org.streamrune.micronaut;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.sse.Event;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

/**
 * Micronaut HTTP controller for Server-Sent Events. Streams events from {@link SseEventPublisher}
 * for one aggregate stream.
 *
 * <p>Endpoint: {@code GET /api/sse/{aggregateType}/{aggregateId}} — the two parts of the typed
 * {@link StreamId}; an invalid part is answered with {@code 400 Bad Request}.
 *
 * <p><b>Where the frames come from.</b> {@link SseEventFeedLifecycle} runs an {@link
 * org.streamrune.runtime.SseEventFeed} beside this controller. It starts at the head of the global
 * stream when the application starts and publishes every event stored from then on to the
 * subscribers of the event's own stream, every {@code streamrune.sse.polling-interval}. Delivery is
 * live, best-effort and at-most-once: a frame reaches only the clients connected at that moment,
 * nothing is redelivered after a reconnect, and {@code Last-Event-ID} is not honoured. When the
 * application context closes, every open stream is completed and what it holds is released. The
 * embedded server stops before the context destroys its beans, so a client connected over HTTP has
 * by then seen the server close its connection; an {@code EventSource} reconnects either way.
 *
 * <p>Each SSE event carries the full domain event as JSON in the {@code data} field and the global
 * offset in the {@code id} field, matching the Spring integration's {@code SseController}.
 * Micronaut serializes the {@link Event} wrapper to the SSE wire format — no hand-rolled framing.
 *
 * <p>Back-pressure: events for a slow client are buffered up to {@link #SLOW_CLIENT_BUFFER} items;
 * beyond that the stream fails and the client must reconnect, instead of growing heap without
 * bound. This is the SECOND of two independent slow-consumer bounds: {@code SseEventPublisher} also
 * caps its own per-subscriber queue. Because this buffer absorbs each emit promptly, the delivery
 * worker rarely stalls and the publisher's queue rarely fills — for a steadily slow client the
 * bound here is the one that trips, and the publisher's slow-consumer eviction is reached only when
 * the publish rate outruns the worker's drain rate (the disconnect hook wired below therefore acts
 * as defence in depth on this integration, unlike on Spring where the publisher queue is the only
 * bound). Worst case a slow client holds this buffer's items PLUS the publisher's queue — decrypted
 * domain events in both — before either bound fires.
 *
 * <p><b>Opening frame.</b> The last step of a subscription writes one {@code : keepalive} comment
 * frame, the same frame on the wire as the Spring and Quarkus integrations write. It commits the
 * response: the client sees the status line, the headers and this frame as soon as it is
 * subscribed, not with the first event or the first periodic keepalive. It is emitted after the
 * client is registered with the {@link SseEventPublisher}, and nothing of the response is written
 * before the first frame: a client that has received the first bytes of the response receives every
 * event of the stream published from then on, for as long as it stays connected.
 *
 * <p><b>Dead-client reaping.</b> Every stream gets a <em>finite</em> lifetime ({@code
 * streamrune.sse.timeout}, default 5m) and a periodic keepalive comment frame ({@code
 * streamrune.sse.keep-alive-interval}, default 30s). A half-open TCP client (a mobile/NAT drop with
 * no FIN/RST) on an idle stream produces no writes on its own, and {@link #SLOW_CLIENT_BUFFER} only
 * bounds a stream that is still <em>emitting</em> — so without these the {@code SseEventPublisher}
 * subscription, its delivery worker, and the socket FD would linger forever, accumulating across
 * reconnect churn until FD exhaustion. The keepalive turns a dead connection into a failed socket
 * write, which the server surfaces by cancelling the subscription; {@code sink.onDispose} then
 * unsubscribes, so eviction rides the framework's own cancellation path rather than a hand-rolled
 * catch. The finite timeout is the backstop; SSE clients auto-reconnect. Matches the Spring and
 * Quarkus integrations.
 *
 * <p><b>A JVM-fatal error poisons the sink.</b> {@code Flux.create}'s serialized sink claims a
 * work-in-progress flag around every {@code next}; it catches and swallows a downstream {@code
 * RuntimeException}, but rethrows a JVM-fatal error ({@code LinkageError}, {@code
 * VirtualMachineError}) <em>before</em> releasing that flag. From then on every {@code sink.error}
 * merely records the error: no terminal signal is delivered and {@code onDispose} never runs, so a
 * stream whose keepalive tick or delivery hit such an error would keep its publisher subscription
 * and its ticker for the life of the JVM. Reactive Streams §2.13 says a subscriber that throws from
 * {@code onNext} must be considered cancelled, so both paths that observe the failure (the
 * keepalive tick and the publisher's disconnect hook) run the stream's cancel teardown themselves
 * after attempting {@code sink.error}. The teardown is idempotent: on the normal path {@code
 * sink.error} has already run it through {@code onDispose}.
 *
 * <p><b>Security:</b> registered only when {@code streamrune.sse.enabled=true}. Access is
 * authorized by the {@link SseAuthorizer} bean using the caller and the requested {@link StreamId};
 * a denied request is rejected with {@code 403 Forbidden}. When SSE is enabled but the application
 * provides no authorizer, the framework installs a fail-closed deny-all authorizer.
 *
 * <p><b>Threads.</b> The controller declares no executor: it runs, and calls the authorizer, on the
 * thread the request filter chain leaves the request on. With the framework's {@link
 * StreamRuneContextFilter} that is a thread of the blocking executor, inside the filter's binding
 * of the request context, so an authorizer may read a database. An application that replaces that
 * filter chooses the thread itself: a filter that stays on a Netty event loop takes the authorizer
 * there, and such an application runs its filter on the blocking executor
 * ({@code @ExecuteOn(TaskExecutors.BLOCKING)}) or keeps its authorizer from blocking. An
 * application that keeps the framework's filter and adds one behind it that runs on another
 * executor gives up the guarantee too: the controller and the authorizer can run on that executor's
 * thread, outside the framework filter's binding of the request context, and which thread a given
 * request goes on from depends on timing inside Micronaut.
 *
 * <p><b>Caller identity.</b> The caller handed to the authorizer is resolved by the same {@link
 * RequestIdentityPolicy} bean the {@link StreamRuneContextFilter} binds {@code
 * RequestContext.userId} with, so a stream subscription and a command from the same request see the
 * same user: the authenticated principal when an {@link AuthenticatedUserResolver} is available
 * (the {@code X-User-Id} header ignored), the {@code X-User-Id} header only in the explicit
 * trusted-gateway mode ({@code streamrune.security.trust-user-id-header=true}), and otherwise
 * nobody ({@code null}). Both read the header through {@link
 * StreamRuneContextFilter#userIdHeaderValues}, every value as received, so a repeated {@code
 * X-User-Id} cannot resolve to one caller here and another there.
 */
@Controller("/api/sse")
@Requires(property = "streamrune.sse.enabled", value = "true")
public class SseController {

  private static final Logger log = LoggerFactory.getLogger(SseController.class);

  /** Maximum number of events buffered for a slow SSE client before the stream is terminated. */
  static final int SLOW_CLIENT_BUFFER = 256;

  /**
   * The single keepalive frame instance, written once when a stream opens ({@link
   * #emitOpeningFrame}) and then every keepalive interval. Comment-only: the data payload is the
   * EMPTY string, which {@code TextStreamCodec} writes verbatim as a {@code CharSequence} — zero
   * bytes, so no {@code data:} line is emitted at all and the wire form is a bare {@code :
   * keepalive} comment, ignored by every SSE client. It carries no id, so it never disturbs a
   * client's {@code Last-Event-ID} resume position.
   */
  static final Event<?> KEEP_ALIVE = Event.of("").comment("keepalive");

  private final SseEventPublisher publisher;
  private final SseAuthorizer authorizer;
  private final RequestIdentityPolicy identityPolicy;
  private final Duration timeout;
  private final Duration keepAliveInterval;

  /** The open streams, tracked so the shutdown can complete them. */
  private final Set<FluxSink<Event<?>>> openStreams = ConcurrentHashMap.newKeySet();

  /**
   * Set once the application context closes; a stream opened from then on is answered already
   * complete. Written before the shutdown takes its snapshot of {@link #openStreams}, and read by a
   * stream after it added itself to them, so a stream is either in the snapshot or sees the flag.
   */
  private volatile boolean shuttingDown;

  /**
   * Creates the SSE controller.
   *
   * @param publisher the SSE event publisher
   * @param authorizer authorizes access to a requested stream
   * @param identityPolicy resolves the caller — the same policy bean the request filter uses
   * @param properties supplies the dead-client reaping windows ({@code streamrune.sse.timeout} and
   *     {@code streamrune.sse.keep-alive-interval})
   * @throws IllegalArgumentException if {@code identityPolicy} is {@code null}
   */
  @Inject
  public SseController(
      SseEventPublisher publisher,
      SseAuthorizer authorizer,
      RequestIdentityPolicy identityPolicy,
      StreamRuneMicronautProperties properties) {
    this(
        publisher,
        authorizer,
        identityPolicy,
        properties.sseTimeout(),
        properties.sseKeepAliveInterval());
  }

  /** Test convenience: production dead-client reaping defaults. */
  SseController(
      SseEventPublisher publisher, SseAuthorizer authorizer, RequestIdentityPolicy identityPolicy) {
    this(publisher, authorizer, identityPolicy, Duration.ofMinutes(5), Duration.ofSeconds(30));
  }

  /** Test convenience: explicit dead-client reaping windows. */
  SseController(
      SseEventPublisher publisher,
      SseAuthorizer authorizer,
      RequestIdentityPolicy identityPolicy,
      Duration timeout,
      Duration keepAliveInterval) {
    if (identityPolicy == null) {
      throw new IllegalArgumentException("identityPolicy is required");
    }
    this.publisher = publisher;
    this.authorizer = authorizer;
    this.identityPolicy = identityPolicy;
    this.timeout = positiveOrNull(timeout);
    this.keepAliveInterval = positiveOrNull(keepAliveInterval);
  }

  private static Duration positiveOrNull(Duration d) {
    return (d == null || d.isZero() || d.isNegative()) ? null : d;
  }

  /**
   * Completes every open stream when the application context closes, so its subscription, keepalive
   * and buffered events are released with the context instead of with the connection. The embedded
   * server has stopped by then and closed the clients' connections (an {@code EventSource}
   * reconnects, reaching another replica); a stream served by another transport sees the
   * completion. A stream opened from here on is answered already complete.
   */
  @PreDestroy
  void completeOpenStreams() {
    shuttingDown = true;
    List<FluxSink<Event<?>>> open = List.copyOf(openStreams);
    if (open.isEmpty()) {
      return;
    }
    log.info("Completing {} open SSE stream(s) as the application shuts down", open.size());
    for (FluxSink<Event<?>> sink : open) {
      try {
        sink.complete();
      } catch (RuntimeException e) {
        log.warn("Failed to complete an SSE stream at shutdown: {}", e.toString());
      }
    }
  }

  /** The number of streams currently open; for tests. */
  int openStreamCount() {
    return openStreams.size();
  }

  /**
   * Opens an SSE stream of one aggregate's events: {@code GET
   * /api/sse/{aggregateType}/{aggregateId}} (for example {@code /api/sse/order/o-1}). Stays open
   * until the client disconnects.
   *
   * @param aggregateType the aggregate type of the stream to subscribe to
   * @param aggregateId the aggregate id of the stream to subscribe to
   * @param request the HTTP request; every {@code X-User-Id} value it carries is handed to the
   *     {@link RequestIdentityPolicy}, an identity only in the trusted-gateway mode (see the class
   *     documentation)
   * @return reactive Flux that emits one SSE event per published domain event
   * @throws HttpStatusException with {@code 400 Bad Request} if the aggregate type or the aggregate
   *     id is invalid, {@code 403 Forbidden} if the {@link SseAuthorizer} denies access to the
   *     stream
   */
  @Get("/{aggregateType}/{aggregateId}")
  @Produces(MediaType.TEXT_EVENT_STREAM)
  public Flux<Event<?>> stream(
      @PathVariable("aggregateType") String aggregateType,
      @PathVariable("aggregateId") String aggregateId,
      HttpRequest<?> request) {
    return stream(aggregateType, aggregateId, StreamRuneContextFilter.userIdHeaderValues(request));
  }

  /**
   * Opens an SSE stream of one aggregate's events for a caller whose {@code X-User-Id} values were
   * already read off the request.
   *
   * @param aggregateType the aggregate type of the stream to subscribe to
   * @param aggregateId the aggregate id of the stream to subscribe to
   * @param userIdHeaderValues every {@code X-User-Id} value the request carried, as received;
   *     {@code null} or empty when absent
   * @return reactive Flux that emits one SSE event per published domain event
   * @throws HttpStatusException with {@code 400 Bad Request} if the aggregate type or the aggregate
   *     id is invalid (the {@link SseAuthorizer} is never consulted), {@code 403 Forbidden} if the
   *     {@link SseAuthorizer} denies access to the stream
   */
  public Flux<Event<?>> stream(
      String aggregateType, String aggregateId, List<String> userIdHeaderValues) {
    StreamId sid = streamIdOf(aggregateType, aggregateId);
    UserId principal = identityPolicy.resolve(userIdHeaderValues, SseController::logMismatch);
    if (!authorizer.isAuthorized(principal, sid)) {
      throw new HttpStatusException(HttpStatus.FORBIDDEN, "Not authorized for this stream");
    }
    Flux<Event<?>> stream =
        Flux.<Event<?>>create(
                sink -> {
                  // This stream's ONE teardown (publisher subscription, open-stream entry,
                  // keepalive ticker), run by onDispose on every normal termination and explicitly
                  // by terminate() when a JVM-fatal error has poisoned the sink. Idempotent; a
                  // resource added after it ran is disposed on the spot.
                  Disposable.Composite resources = Disposables.composite();
                  // Attached first, before anything is acquired: whatever ends the stream from
                  // here on, a failure of a later step of this set-up included, releases what the
                  // stream holds.
                  sink.onDispose(resources);
                  if (shuttingDown) {
                    // The application is shutting down: a stream that is already complete makes
                    // the client reconnect (elsewhere) and holds nothing here.
                    sink.complete();
                    return;
                  }
                  Consumer<Throwable> terminate = cause -> terminate(sink, sid, cause, resources);
                  SseEventPublisher.SseSubscriber subscriber =
                      envelope ->
                          sink.next(
                              Event.of(envelope.event())
                                  .id(String.valueOf(envelope.globalOffset().value())));
                  // Without a disconnect hook the publisher's slow-consumer eviction
                  // unregisters the subscriber but never terminates the Flux, so the client keeps
                  // an open, apparently-healthy stream that receives zero further events while its
                  // keepalive still succeeds (it is slow, not dead) — a permanent silent event gap
                  // plus a leaked sink, ticker and FD. sink.error() is a terminal signal
                  // Flux.create settles for us, and it fires onDispose, which unsubscribes
                  // and cancels the ticker (terminate() also runs that teardown itself, for the
                  // poisoned-sink case where sink.error() is swallowed).
                  publisher.subscribe(sid, subscriber, terminate);
                  resources.add(() -> publisher.unsubscribe(sid, subscriber));
                  openStreams.add(sink);
                  resources.add(() -> openStreams.remove(sink));
                  if (shuttingDown) {
                    // The shutdown began after the first look at the flag. It may have taken its
                    // snapshot of the open streams before this one was added, so the stream ends
                    // itself; completing a stream the shutdown also completes is harmless. The
                    // flag is set before the snapshot is taken and read here after the stream was
                    // added, so one of the two always sees the other.
                    sink.complete();
                    return;
                  }
                  // Flux.create serializes the sink, so the keepalive tick and the delivery worker
                  // may both call next() without extra locking.
                  resources.add(scheduleKeepAlive(sink, terminate));
                  // Last, when the client is registered with the publisher and the teardown is in
                  // place: the frame that commits the response.
                  emitOpeningFrame(sink, terminate);
                },
                FluxSink.OverflowStrategy.ERROR)
            .onBackpressureBuffer(SLOW_CLIENT_BUFFER);
    // Dead-client backstop: complete the stream after the configured lifetime so even a stream
    // whose
    // keepalive is disabled cannot hold a dead subscription/FD open indefinitely. Completion
    // cancels upstream, which fires onDispose above (unsubscribe + ticker dispose).
    return timeout == null ? stream : stream.take(timeout);
  }

  /** The two path segments through their ingress doors; an invalid part is the caller's error. */
  private static StreamId streamIdOf(String aggregateType, String aggregateId) {
    try {
      return StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    } catch (IllegalArgumentException _) {
      throw new HttpStatusException(
          HttpStatus.BAD_REQUEST, "Invalid aggregate type or aggregate id");
    }
  }

  private static void logMismatch(UserId headerUserId, UserId principal) {
    // The header is attacker-controlled — sanitize it (and the
    // principal, an IdP assertion) before it reaches the log record (CWE-117).
    log.warn(
        "X-User-Id header '{}' on an SSE subscription does not match authenticated principal '{}';"
            + " ignoring the header and authorizing as the authenticated principal",
        LogSanitizer.sanitizeForLog(headerUserId.value()),
        LogSanitizer.sanitizeForLog(principal.value()));
  }

  /**
   * Terminates a stream the controller itself evicts: the publisher's disconnect hook, or a failed
   * keepalive tick. {@code sink.error} is the normal terminal signal and runs {@code resources}
   * through {@code onDispose}; disposing {@code resources} here as well covers the sink a JVM-fatal
   * error poisoned (see the class documentation), where {@code sink.error} is swallowed and {@code
   * onDispose} would never run.
   */
  private static void terminate(
      FluxSink<Event<?>> sink, StreamId sid, Throwable cause, Disposable resources) {
    try {
      sink.error(cause);
    } catch (Throwable t) {
      // Both parts arrive percent-decoded from the request path; every
      // id in a log line is rendered through the sanitizer so no value can forge a line (CWE-117).
      log.warn(
          "Failed to terminate an evicted SSE stream for {}: {}",
          LogSanitizer.sanitizeForLog(sid.value()),
          t.toString());
    } finally {
      resources.dispose();
    }
  }

  /**
   * Writes the comment frame that opens a stream: the {@link #KEEP_ALIVE} frame, once, as the last
   * step of the subscription.
   *
   * <p><b>What it guarantees.</b> The server writes the status line and the headers of the response
   * with the first frame of the stream, and every frame comes out of this sink. This one is emitted
   * after {@link SseEventPublisher#subscribe} returned, so the first bytes of the response cannot
   * reach the client before the client is registered: a client that has received them receives
   * every event of the stream published from then on, for as long as it stays connected. Without
   * this frame the response would be committed by the first event or the first periodic keepalive,
   * up to a whole {@code streamrune.sse.keep-alive-interval} later.
   *
   * <p>An event published between the registration and this call is emitted by the delivery worker
   * first and is the frame before this one; none is lost, and events keep their order because one
   * worker delivers them. The sink is serialized, so the two emissions need no lock.
   *
   * <p>A failed emission ends the stream like a failed keepalive tick, through {@code terminate}. A
   * write the socket refuses is reported by the server as a cancellation, which disposes the
   * stream's resources and with them the publisher subscription.
   */
  private static void emitOpeningFrame(FluxSink<Event<?>> sink, Consumer<Throwable> terminate) {
    try {
      sink.next(KEEP_ALIVE);
    } catch (Throwable t) {
      // Only a JVM-fatal error escapes the serialized sink's next(), and it leaves the sink
      // poisoned: terminate() therefore tears the stream down itself.
      log.warn("Evicting an SSE subscriber after a failed opening frame: {}", t.toString());
      terminate.accept(t);
    }
  }

  /**
   * Starts this stream's keepalive ticks, or returns a no-op {@link Disposable} when the keepalive
   * is disabled. Each subscriber gets its OWN periodic task, so a throwing tick can only stop that
   * stream's keepalive — never every other subscriber's (the Spring integration, whose reaper is a
   * single shared tick over all emitters, guards that tick against it instead). A failed tick
   * terminates the stream through {@code terminate}, which also disposes this ticker.
   */
  private Disposable scheduleKeepAlive(FluxSink<Event<?>> sink, Consumer<Throwable> terminate) {
    if (keepAliveInterval == null) {
      return () -> {}; // nothing scheduled, nothing to cancel
    }
    long intervalMillis = keepAliveInterval.toMillis();
    // Initial delay = interval: the opening frame is this stream's write at time zero.
    return Schedulers.parallel()
        .schedulePeriodically(
            () -> {
              try {
                sink.next(KEEP_ALIVE);
              } catch (Throwable t) {
                // Only a JVM-fatal error escapes the serialized sink's next(), and it leaves the
                // sink poisoned: terminate() therefore tears the stream down itself.
                log.warn(
                    "Evicting an SSE subscriber after a failed keepalive tick: {}", t.toString());
                terminate.accept(t);
              }
            },
            intervalMillis,
            intervalMillis,
            TimeUnit.MILLISECONDS);
  }
}
