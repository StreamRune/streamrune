package org.streamrune.quarkus;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.ShutdownDelayInitiatedEvent;
import io.quarkus.runtime.ShutdownEvent;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.OutboundSseEvent;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestStreamElementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;

/**
 * Quarkus JAX-RS endpoint for Server-Sent Events. Streams events from {@link SseEventPublisher} for
 * one aggregate stream.
 *
 * <p>Endpoint: {@code GET /api/sse/{aggregateType}/{aggregateId}} — the two parts of the typed
 * {@link StreamId}; an invalid part is answered with {@code 400 Bad Request}.
 *
 * <p><b>Where the frames come from.</b> {@link SseEventFeedLifecycle} runs an {@link
 * org.streamrune.runtime.SseEventFeed} beside this resource. It starts at the head of the global
 * stream when the application starts and publishes every event stored from then on to the
 * subscribers of the event's own stream, every {@code streamrune.sse.polling-interval}. Delivery is
 * live, best-effort and at-most-once: a frame reaches only the clients connected at that moment,
 * nothing is redelivered after a reconnect, and {@code Last-Event-ID} is not honoured.
 *
 * <p><b>Shutdown.</b> An open stream is a request in flight that never finishes on its own, and
 * Quarkus stops in this order: the shutdown-delay event and the delay ({@code
 * quarkus.shutdown.delay}), the graceful phase that waits up to {@code quarkus.shutdown.timeout}
 * for the requests in flight, the HTTP server, and only then {@link ShutdownEvent}. The
 * shutdown-delay event is the one point ahead of the HTTP server a bean can observe, and Quarkus
 * fires it only in an application built with {@code quarkus.shutdown.delay-enabled=true}:
 *
 * <ul>
 *   <li><b>Built with {@code quarkus.shutdown.delay-enabled=true}.</b> The resource completes every
 *       open stream on that event ({@link #completeOpenStreams(ShutdownDelayInitiatedEvent)}): each
 *       client sees its stream end normally while the server is still serving (an {@code
 *       EventSource} reconnects, reaching another replica), a stream opened from then on is
 *       answered already complete, and the graceful phase has no stream to wait for.
 *   <li><b>Built without it</b> (the Quarkus default). Nothing ends the streams ahead of the HTTP
 *       server: the server closes the clients' connections as it stops, so a connected client sees
 *       its connection cut, not a completed stream (an {@code EventSource} reconnects either way),
 *       and Quarkus REST releases each stream as its connection closes. With {@code
 *       quarkus.shutdown.timeout} set, every connected client is a request the graceful phase waits
 *       for: one open stream holds the shutdown for that whole timeout.
 * </ul>
 *
 * <p>On {@link ShutdownEvent} the resource completes whatever is still open ({@link
 * #completeOpenStreams(ShutdownEvent)}); the HTTP server has stopped by then, so no client sees
 * that completion.
 *
 * <p>Each SSE event carries the full domain event as JSON in the {@code data} field and the global
 * offset in the {@code id} field, matching the Spring integration's {@code SseController}. The
 * payload is written by the application's own JSON message body writer: {@link #stream(String,
 * String, HttpHeaders)} declares {@code application/json} as the stream element type, the media
 * type Quarkus REST looks a writer up for when it serializes the data of a frame. The application
 * therefore needs a JSON extension (e.g. {@code quarkus-rest-jackson}). With none, the only writer
 * Quarkus REST finds for an event is its built-in text one, and the {@code data} field carries the
 * event's {@code toString()}.
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
 * frame, the same frame on the wire as the Spring and Micronaut integrations write; it is the first
 * frame of every stream. It is emitted after the client is registered with the {@link
 * SseEventPublisher}: a client that has read it receives every event of the stream published from
 * then on, for as long as it stays connected. Quarkus REST sends the status line and the headers of
 * the response before it subscribes to the stream, so on Quarkus the response head arrives an
 * instant before the registration and the comment frame, not the head, is the signal that the
 * client is subscribed.
 *
 * <p><b>Dead-client reaping.</b> Every stream gets a <em>finite</em> lifetime ({@code
 * streamrune.sse.timeout}, default 5m) and a periodic keepalive comment frame ({@code
 * streamrune.sse.keep-alive-interval}, default 30s). A half-open TCP client (a mobile/NAT drop with
 * no FIN/RST) on an idle stream produces no writes on its own, and {@link #SLOW_CLIENT_BUFFER} only
 * bounds a stream that is still <em>emitting</em> — so without these the {@code SseEventPublisher}
 * subscription, its delivery worker, and the socket FD would linger, accumulating across reconnect
 * churn, until the HTTP server's idle timeout closes the connection ({@code
 * quarkus.http.idle-timeout}, 30 minutes unless configured). The keepalive turns a dead connection
 * into a failed socket write, which Quarkus REST surfaces by cancelling the subscription — {@code
 * onTermination} then unsubscribes, so eviction rides the framework's own cancellation path rather
 * than a hand-rolled catch. The finite timeout is the backstop; SSE clients auto-reconnect. Matches
 * the Spring and Micronaut integrations.
 *
 * <p><b>A downstream that throws.</b> Quarkus REST serializes a frame inside its subscriber's
 * {@code onNext}, so a message body writer that throws an unchecked exception is a downstream that
 * throws. The last operator of every stream is a {@link DownstreamFailureGuard}: it catches the
 * exception, cancels the stream — the termination hook runs the teardown — and signals the failure
 * to Quarkus REST's subscriber, which ends the HTTP response. The client is not left on an open
 * connection nothing writes to, and an {@code EventSource} reconnects.
 *
 * <p>Behind the guard stands a second line. Mutiny's serialized emitter claims a work-in-progress
 * flag around every {@code emit} and does not release it when an exception does reach it; from then
 * on {@code fail} and {@code complete} only record the signal and the termination hook never runs.
 * Every path that observes a failed emission (the opening frame, a keepalive tick, the publisher's
 * disconnect hook) therefore runs the stream's teardown itself after attempting {@code
 * emitter.fail}. The teardown runs once: on the normal path the termination hook has already run
 * it.
 *
 * <p><b>The shared scheduler thread is never parked.</b> One single-threaded {@link
 * ScheduledThreadPoolExecutor} drives every stream's keepalive ticks and every stream's deadline,
 * so neither may block on a per-stream send lock: both probe it with {@code tryLock()}. A keepalive
 * that loses the probe is skipped (the in-flight write is that stream's liveness signal); a
 * deadline that loses it is retried, because a missed completion would leak the subscription.
 * Spring and Micronaut need neither: Spring's deadline is enforced by the servlet container ({@code
 * SseEmitter(timeout)}) and Micronaut's runs as {@code Flux.take(Duration)} on the multi-threaded
 * {@code Schedulers.parallel()} against a self-serializing sink.
 *
 * <p><b>Security:</b> registered only when {@code streamrune.sse.enabled=true} (build-time gate).
 * Access is authorized by the {@link SseAuthorizer} bean using the caller and the requested {@link
 * StreamId}; a denied request is rejected with {@code 403 Forbidden}. When SSE is enabled but the
 * application provides no authorizer, the framework installs a fail-closed deny-all authorizer. The
 * authorizer runs on a worker thread with the CDI request scope active, never on a Vert.x event
 * loop: the resource method is a blocking one (see {@link #stream(String, String, HttpHeaders)}).
 *
 * <p><b>Caller identity.</b> The caller handed to the authorizer is resolved by the same {@link
 * RequestIdentityPolicy} bean the {@link StreamRuneRequestFilter} binds {@code
 * RequestContext.userId} with, so a stream subscription and a command from the same request see the
 * same user: the authenticated principal when an {@link AuthenticatedUserResolver} is available
 * (the {@code X-User-Id} header ignored), the {@code X-User-Id} header only in the explicit
 * trusted-gateway mode ({@code streamrune.security.trust-user-id-header=true}), and otherwise
 * nobody ({@code null}). Both read every {@code X-User-Id} value as received ({@link
 * HttpHeaders#getRequestHeader} here, the same multivalued view as the filter's {@code
 * getHeaders()}), never a single-valued {@code @HeaderParam} that takes the first value, so a
 * repeated header cannot resolve to one caller here and another there.
 *
 * <p><b>Runtime kill switch.</b> {@code @IfBuildProperty} only decides whether this resource is
 * <em>registered</em> at build/augmentation time — it cannot be flipped on a running or redeployed
 * instance. {@link #stream} therefore re-reads the <em>runtime</em> value of {@code
 * streamrune.sse.enabled} and returns {@code 404 Not Found} when it is {@code false}, so an
 * operator disabling SSE at runtime (e.g. during an incident) actually disables it — matching the
 * Spring ({@code @ConditionalOnProperty}) and Micronaut ({@code @Requires}) integrations, where
 * flipping the flag removes the endpoint entirely. Without this, the flag would be a silent no-op
 * on Quarkus and the endpoint would keep streaming decrypted domain events.
 */
@Path("/api/sse")
@ApplicationScoped
@IfBuildProperty(name = "streamrune.sse.enabled", stringValue = "true")
public class SseController {

  private static final Logger log = LoggerFactory.getLogger(SseController.class);

  /** Maximum number of events buffered for a slow SSE client before the stream is terminated. */
  static final int SLOW_CLIENT_BUFFER = 256;

  /**
   * How soon a deadline task that found its stream's send lock held retries.
   *
   * <p>A deadline may be <em>retried</em> but never <em>skipped</em> — unlike a keepalive tick,
   * whose whole purpose is served by the in-flight write it collided with. Short enough that the
   * dead-client FD-reclaim window is not meaningfully widened, long enough that a stream stalled
   * for minutes costs a handful of cheap probe wakeups rather than a spin.
   */
  static final long DEADLINE_RETRY_MILLIS = 50L;

  /**
   * The single keepalive frame instance, written once when a stream opens ({@link
   * #emitOpeningFrame}) and then every keepalive interval. Comment-only: {@code getData()} is
   * {@code null}, and Quarkus REST's SSE serializer omits the {@code data} field entirely for a
   * null payload, so the wire form is a bare {@code : keepalive} comment — ignored by every SSE
   * client, but still a socket write.
   */
  static final OutboundSseEvent KEEP_ALIVE = new KeepAliveSseFrame();

  /** How long the shutdown waits for one stream's write in progress before completing it. */
  static final long SHUTDOWN_LOCK_WAIT_MILLIS = 1_000L;

  private final SseEventPublisher publisher;
  private final SseAuthorizer authorizer;
  private final RequestIdentityPolicy identityPolicy;
  // A Provider so each request re-reads the current runtime value rather than a value frozen at
  // bean creation — the runtime kill switch.
  private final Provider<Boolean> sseEnabled;
  private final Duration timeout;
  private final Duration keepAliveInterval;

  /**
   * Shared keepalive/deadline scheduler; {@code null} when both are disabled.
   *
   * <p>An explicit {@link ScheduledThreadPoolExecutor} (not {@link
   * java.util.concurrent.Executors#newSingleThreadScheduledExecutor}, whose returned {@code
   * DelegatedScheduledExecutorService} wrapper exposes no way to reach {@code
   * setRemoveOnCancelPolicy}) so a cancelled deadline/keepalive task is purged from the delay queue
   * immediately instead of lingering there until it would have fired — see the constructor.
   */
  private final ScheduledThreadPoolExecutor keepAlive;

  /** The open streams, tracked so the shutdown can complete them. */
  private final Set<OpenStream> openStreams = ConcurrentHashMap.newKeySet();

  /**
   * Set once the shutdown reaches this resource; a stream opened from then on is answered already
   * complete. What that turns away depends on which signal sets it (see the class documentation):
   *
   * <ul>
   *   <li>the shutdown-delay event, while the HTTP server is still serving: every stream a client
   *       opens during the delay and the graceful phase, which would otherwise be one more request
   *       that phase waits for;
   *   <li>{@link ShutdownEvent} or the destruction of the bean, after the HTTP server has stopped:
   *       only a stream whose request was still on a worker thread (in the request filter or the
   *       {@link SseAuthorizer}) when the server stopped and is subscribed afterwards. It would
   *       register with a publisher and a keepalive scheduler that are being closed.
   * </ul>
   *
   * <p>Written before the shutdown takes its snapshot of {@link #openStreams}, and read by a stream
   * after it added itself to them, so a stream is either in the snapshot or sees the flag.
   */
  private volatile boolean shuttingDown;

  /** One open stream and the lock that serializes the writes to it. */
  private record OpenStream(
      MultiEmitter<? super OutboundSseEvent> emitter, ReentrantLock sendLock) {}

  /**
   * The constructor Quarkus/Arc uses.
   *
   * @param publisher the event fan-out the subscriptions attach to
   * @param authorizer decides whether the caller may subscribe to a stream
   * @param identityPolicy resolves the caller — the same policy bean the request filter uses
   * @param sseEnabled the runtime value of {@code streamrune.sse.enabled} (runtime kill switch)
   * @param properties the bound StreamRune Quarkus properties (dead-client reaping windows)
   * @throws IllegalArgumentException if {@code identityPolicy} is {@code null}
   */
  @Inject
  public SseController(
      SseEventPublisher publisher,
      SseAuthorizer authorizer,
      RequestIdentityPolicy identityPolicy,
      @ConfigProperty(name = "streamrune.sse.enabled", defaultValue = "true")
          Provider<Boolean> sseEnabled,
      StreamRuneQuarkusProperties properties) {
    this(
        publisher,
        identityPolicy,
        authorizer,
        sseEnabled,
        properties.sse().timeout(),
        properties.sse().keepAliveInterval());
  }

  /** Test convenience: SSE always enabled at runtime, production reaping defaults. */
  SseController(
      SseEventPublisher publisher, SseAuthorizer authorizer, RequestIdentityPolicy identityPolicy) {
    this(
        publisher,
        identityPolicy,
        authorizer,
        () -> Boolean.TRUE,
        Duration.ofMinutes(5),
        Duration.ofSeconds(30));
  }

  /** Test convenience: explicit runtime kill switch, production dead-client reaping defaults. */
  SseController(
      SseEventPublisher publisher,
      SseAuthorizer authorizer,
      RequestIdentityPolicy identityPolicy,
      Provider<Boolean> sseEnabled) {
    this(
        publisher,
        identityPolicy,
        authorizer,
        sseEnabled,
        Duration.ofMinutes(5),
        Duration.ofSeconds(30));
  }

  /** Test convenience: SSE always enabled at runtime, explicit dead-client reaping windows. */
  SseController(
      SseEventPublisher publisher,
      SseAuthorizer authorizer,
      RequestIdentityPolicy identityPolicy,
      Duration timeout,
      Duration keepAliveInterval) {
    this(publisher, identityPolicy, authorizer, () -> Boolean.TRUE, timeout, keepAliveInterval);
  }

  // Canonical constructor. The parameter order differs from the public one only to keep the
  // delegating overloads above unambiguous.
  private SseController(
      SseEventPublisher publisher,
      RequestIdentityPolicy identityPolicy,
      SseAuthorizer authorizer,
      Provider<Boolean> sseEnabled,
      Duration timeout,
      Duration keepAliveInterval) {
    if (identityPolicy == null) {
      throw new IllegalArgumentException("identityPolicy is required");
    }
    this.publisher = publisher;
    this.authorizer = authorizer;
    this.identityPolicy = identityPolicy;
    this.sseEnabled = sseEnabled;
    this.timeout = positiveOrNull(timeout);
    this.keepAliveInterval = positiveOrNull(keepAliveInterval);
    // One shared daemon scheduler drives both dead-client reapers (per-stream keepalive ticks and
    // per-stream deadlines); none is created when both are disabled.
    if (this.keepAliveInterval == null && this.timeout == null) {
      this.keepAlive = null;
    } else {
      this.keepAlive =
          new ScheduledThreadPoolExecutor(
              1,
              r -> {
                Thread t = new Thread(r, "streamrune-sse-keepalive");
                t.setDaemon(true);
                return t;
              });
      // ScheduledThreadPoolExecutor defaults removeOnCancelPolicy to false, so a
      // cancelled ScheduledFutureTask (every normal disconnect calls Future.cancel(false) — see
      // emitter.onTermination below) stays parked in the DelayedWorkQueue until it would have
      // fired, i.e. for up to the full configured timeout. At churny reconnect rates that retains
      // one dead emitter chain (and its MultiEmitter, subscriber, and REST request context) per
      // disconnect for the whole timeout window — heap growth proportional to reconnect rate x
      // timeout that looks exactly like a leak. true purges a cancelled task from the queue
      // immediately instead.
      this.keepAlive.setRemoveOnCancelPolicy(true);
      // A cancelled/shutdown task must never fire after shutdown — matches shutdown()'s
      // shutdownNow() below, which already discards queued tasks; explicit for clarity.
      this.keepAlive.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }
  }

  private static Duration positiveOrNull(Duration d) {
    return (d == null || d.isZero() || d.isNegative()) ? null : d;
  }

  /**
   * Completes every open stream at the start of the shutdown, ahead of the HTTP server: each client
   * sees its stream end normally (an {@code EventSource} reconnects, reaching another replica), and
   * the graceful phase that follows ({@code quarkus.shutdown.timeout}) has no stream to wait for. A
   * stream opened from here on — the server keeps serving during the delay and the graceful phase —
   * is answered already complete.
   *
   * <p>Quarkus fires this event only in an application built with {@code
   * quarkus.shutdown.delay-enabled=true}; see the class documentation for an application built
   * without it. Unlike {@link #completeOpenStreams(ShutdownEvent)} this observer is notified
   * whether or not the resource exists yet: a resource no request has created so far has no stream
   * to complete, but it must still turn away the streams opened after the event.
   *
   * @param event the Quarkus shutdown-delay event
   */
  public void completeOpenStreams(@Observes ShutdownDelayInitiatedEvent event) {
    completeOpenStreams();
  }

  /**
   * Completes every stream that is still open when the application's shutdown event is fired, and
   * turns away every stream opened afterwards. Quarkus fires this event after it has stopped the
   * HTTP server, so the clients' connections are closed by then and Quarkus REST has released the
   * streams that were open: no client sees this completion. What it ends is a stream that is
   * subscribed after the server stopped, because its request was still on a worker thread. Observed
   * only by a resource that exists: the shutdown does not create one to find no stream.
   *
   * @param event the Quarkus shutdown event
   */
  public void completeOpenStreams(
      @Observes(notifyObserver = Reception.IF_EXISTS) ShutdownEvent event) {
    completeOpenStreams();
  }

  /**
   * Sets {@link #shuttingDown} and completes every open stream. A stream's send lock is awaited for
   * at most {@link #SHUTDOWN_LOCK_WAIT_MILLIS}: a write stalled on one client does not hold the
   * shutdown, and that stream is completed anyway.
   */
  private void completeOpenStreams() {
    shuttingDown = true;
    List<OpenStream> open = List.copyOf(openStreams);
    if (open.isEmpty()) {
      return;
    }
    log.info("Completing {} open SSE stream(s) as the application shuts down", open.size());
    for (OpenStream stream : open) {
      boolean locked = false;
      try {
        locked = stream.sendLock().tryLock(SHUTDOWN_LOCK_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        stream.emitter().complete();
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        return;
      } catch (RuntimeException e) {
        log.warn("Failed to complete an SSE stream at shutdown: {}", e.toString());
      } finally {
        if (locked) {
          stream.sendLock().unlock();
        }
      }
    }
  }

  /** The number of streams currently open; for tests. */
  int openStreamCount() {
    return openStreams.size();
  }

  /**
   * Stops the keepalive scheduler when the bean is destroyed. A stream opened afterwards is
   * answered already complete, like one opened after the shutdown event: a stopped scheduler
   * accepts neither its keepalive nor its deadline.
   */
  @PreDestroy
  void shutdown() {
    shuttingDown = true;
    if (keepAlive != null) {
      keepAlive.shutdownNow();
    }
  }

  /** Test seam: exposes the shared scheduler so a test can inspect its delay queue. */
  ScheduledThreadPoolExecutor testKeepAliveExecutor() {
    return keepAlive;
  }

  /**
   * Opens an SSE stream of one aggregate's events: {@code GET
   * /api/sse/{aggregateType}/{aggregateId}} (for example {@code /api/sse/order/o-1}). Stays open
   * until the client disconnects.
   *
   * <p>{@code @RestStreamElementType(APPLICATION_JSON)} is what makes the {@code data} field JSON.
   * Quarkus REST serializes the payload of each frame of this stream with the message body writer
   * registered for the stream element type, and it takes that type from this annotation: {@link
   * OutboundSseEvent#getMediaType()} is honoured only on a frame built by Quarkus REST's own event
   * builder, not on an {@link OutboundSseEvent} the application implements. With no element type
   * declared the payload is written as {@code text/plain}, which is the event's {@code toString()}.
   * Quarkus REST also names the element type in the {@code X-SSE-Content-Type} response header.
   *
   * <p>{@code @Blocking} is what keeps the checks before the subscription off the Vert.x event
   * loop. Quarkus REST treats a resource method that returns a {@link Multi} as non-blocking and
   * calls it on the event-loop thread the request arrived on; the {@link SseAuthorizer} would run
   * there, and an authorizer that decides by the ownership of a stream reads a database. With the
   * annotation Quarkus REST dispatches the request to the worker pool before the request filters
   * and the method run, as it does for every blocking endpoint. On that worker thread, in this
   * order:
   *
   * <ol>
   *   <li>{@link StreamRuneRequestFilter} resolves the caller and the caller's roles and stores the
   *       request context in the request-scoped {@link StreamRuneRequestContextHolder};
   *   <li>this method resolves the caller, asks the {@link SseAuthorizer} and throws for a refused
   *       caller, so a refusal is the HTTP error response ({@code 403}, {@code 400}, {@code 404})
   *       and never a stream that fails after a {@code 200}.
   * </ol>
   *
   * <p>Quarkus REST activates the CDI request scope of the request on the worker thread, so an
   * authorizer may read {@link StreamRuneRequestContextHolder} or any other request-scoped bean.
   * The worker thread is held for these two steps only. With the returned {@link Multi} in hand
   * Quarkus REST writes the response head and subscribes to the stream from the event loop that
   * finished that write: the subscription registers the client with the {@link SseEventPublisher}
   * there, and nothing in it blocks. Frames are written without blocking, by the thread that emits
   * them or by the event loop that finished the previous write.
   *
   * <p>The annotation is preferred over moving the checks to a worker inside the method (a {@code
   * Uni} with {@code runSubscriptionOn}, or Vert.x {@code executeBlocking}): that would leave the
   * request filter, and with it the role resolution, on the event loop; a refusal would become a
   * failure of the returned stream, which Quarkus REST's stream subscriber maps to a status only
   * while no frame has been written; and the request scope would have to follow the checks to the
   * other thread through context propagation.
   *
   * @param aggregateType the aggregate type of the stream to subscribe to
   * @param aggregateId the aggregate id of the stream to subscribe to
   * @param headers the request headers; every {@code X-User-Id} value they carry is handed to the
   *     {@link RequestIdentityPolicy}, an identity only in the trusted-gateway mode (see the class
   *     documentation)
   * @return reactive Multi that emits one SSE event per published domain event
   * @throws BadRequestException if the aggregate type or the aggregate id is invalid
   * @throws ForbiddenException if the {@link SseAuthorizer} denies access to the stream
   */
  @GET
  @Path("/{aggregateType}/{aggregateId}")
  @Produces(MediaType.SERVER_SENT_EVENTS)
  @RestStreamElementType(MediaType.APPLICATION_JSON)
  @Blocking
  public Multi<OutboundSseEvent> stream(
      @PathParam("aggregateType") String aggregateType,
      @PathParam("aggregateId") String aggregateId,
      @Context HttpHeaders headers) {
    return stream(
        aggregateType, aggregateId, headers.getRequestHeader(RequestIdentityPolicy.USER_ID_HEADER));
  }

  /**
   * Opens an SSE stream of one aggregate's events for a caller whose {@code X-User-Id} values were
   * already read off the request.
   *
   * @param aggregateType the aggregate type of the stream to subscribe to
   * @param aggregateId the aggregate id of the stream to subscribe to
   * @param userIdHeaderValues every {@code X-User-Id} value the request carried, as received;
   *     {@code null} or empty when absent
   * @return reactive Multi that emits one SSE event per published domain event
   * @throws BadRequestException if the aggregate type or the aggregate id is invalid (the {@link
   *     SseAuthorizer} is never consulted)
   * @throws ForbiddenException if the {@link SseAuthorizer} denies access to the stream
   */
  public Multi<OutboundSseEvent> stream(
      String aggregateType, String aggregateId, List<String> userIdHeaderValues) {
    // Runtime kill switch. The @IfBuildProperty class gate is fixed at build time, so
    // without this re-check flipping streamrune.sse.enabled to false on a running/redeployed
    // instance would silently keep streaming decrypted domain events. Return 404 (matching
    // Spring/Micronaut, where the endpoint bean does not exist when disabled).
    if (!Boolean.TRUE.equals(sseEnabled.get())) {
      throw new NotFoundException("SSE endpoint is disabled (streamrune.sse.enabled=false)");
    }
    StreamId sid = streamIdOf(aggregateType, aggregateId);
    UserId principal = identityPolicy.resolve(userIdHeaderValues, SseController::logMismatch);
    if (!authorizer.isAuthorized(principal, sid)) {
      throw new ForbiddenException("Not authorized for this stream");
    }
    return Multi.createFrom()
        .<OutboundSseEvent>emitter(
            emitter -> {
              // Serializes event delivery (on the subscriber's worker thread) with keepalive
              // writes (on the scheduler thread): a Mutiny emitter must not be driven
              // concurrently, and Reactive Streams requires serial onNext. A ReentrantLock
              // (not a monitor) so the shared keepalive tick can PROBE it non-blockingly with
              // tryLock() — see scheduleKeepAlive() (mirroring Spring's
              // SseController#sendKeepAlives). The event path takes it unconditionally.
              ReentrantLock sendLock = new ReentrantLock();
              // This stream's ONE teardown (publisher subscription, open-stream entry, keepalive
              // ticker, deadline), run by the emitter's termination hook on every normal end and
              // by each eviction itself, for the emitter a throwing downstream has left unable to
              // terminate (see the class documentation).
              Teardown teardown = new Teardown();
              SseEventPublisher.SseSubscriber subscriber =
                  envelope -> {
                    sendLock.lock();
                    try {
                      emitter.emit(
                          new EnvelopeSseFrame(
                              String.valueOf(envelope.globalOffset().value()), envelope.event()));
                    } finally {
                      sendLock.unlock();
                    }
                  };
              // The termination hook comes first, before anything is acquired: whatever ends the
              // stream from here on, a failure of a later step of this set-up included, runs the
              // teardown, and a step added to a teardown that has already run is run on the spot.
              emitter.onTermination(teardown);
              if (shuttingDown) {
                // The application is shutting down: a stream that is already complete makes the
                // client reconnect (elsewhere) and holds nothing here.
                emitter.complete();
                return;
              }
              // The send lock is held from before the client is registered until its opening
              // frame is emitted. Taken here, where nothing else knows the lock, it is free: this
              // lambda runs on the event loop Quarkus REST subscribes from, which must not wait.
              // An event published in the meantime waits for the lock in its delivery worker and
              // is emitted after the opening frame.
              sendLock.lock();
              try {
                // Without a disconnect hook the publisher's slow-consumer eviction
                // unregisters the subscriber but never terminates the Multi, so the client keeps
                // an open, apparently-healthy stream that receives zero further events while its
                // keepalive still succeeds (it is slow, not dead) — a permanent silent event gap
                // plus a leaked emitter, ticker and FD. Deliberately does NOT take sendLock: this
                // hook can run on the PUBLISHER's thread and that lock is held by the stalled
                // write that caused the eviction; emitter.fail() is a terminal signal the emitter
                // settles atomically, and the teardown does the rest.
                publisher.subscribe(
                    sid,
                    subscriber,
                    cause -> {
                      try {
                        emitter.fail(cause);
                      } catch (Throwable t) {
                        // Both parts arrive percent-decoded from the
                        // request path; every id in a log line is rendered through the sanitizer
                        // so no value can forge a line (CWE-117).
                        log.warn(
                            "Failed to fail an evicted SSE stream for {}: {}",
                            LogSanitizer.sanitizeForLog(sid.value()),
                            t.toString());
                      } finally {
                        teardown.run();
                      }
                    });
                teardown.add(() -> publisher.unsubscribe(sid, subscriber));
                OpenStream open = new OpenStream(emitter, sendLock);
                openStreams.add(open);
                teardown.add(() -> openStreams.remove(open));
                if (shuttingDown) {
                  // The shutdown began after the first look at the flag. It may have taken its
                  // snapshot of the open streams before this one was added, so the stream ends
                  // itself; completing a stream the shutdown also completes is harmless. The flag
                  // is set before the snapshot is taken and read here after the stream was added,
                  // so one of the two always sees the other.
                  emitter.complete();
                  return;
                }
                teardown.add(cancelling(scheduleKeepAlive(emitter, sendLock, teardown)));
                teardown.add(cancelling(scheduleTimeout(emitter, sendLock)));
                // Last, when the client is registered with the publisher and the teardown is in
                // place: the first frame of the body.
                emitOpeningFrame(emitter, teardown);
              } finally {
                sendLock.unlock();
              }
            },
            BackPressureStrategy.ERROR)
        .onOverflow()
        .buffer(SLOW_CLIENT_BUFFER)
        // Directly in front of the subscriber that writes the response: a frame that subscriber
        // cannot write ends the stream and the response (see DownstreamFailureGuard).
        .plug(DownstreamFailureGuard::new);
  }

  /** The two path segments through their ingress doors; an invalid part is the caller's error. */
  private static StreamId streamIdOf(String aggregateType, String aggregateId) {
    try {
      return StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    } catch (IllegalArgumentException _) {
      throw new BadRequestException("Invalid aggregate type or aggregate id");
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
   * Schedules this stream's deadline: completing the <em>emitter</em> (not merely the downstream)
   * at the configured lifetime, so the termination hook runs and the {@code SseEventPublisher}
   * subscription is released. A downstream-only operator such as {@code select().first(Duration)}
   * completes the subscriber but leaves the emitter — and therefore the subscription, its worker
   * and the FD — alive, which is the leak the deadline exists to close.
   *
   * <p><b>Never blocks on a mid-send stream.</b> {@link #keepAlive} is the process's SINGLE shared
   * scheduler thread for every stream's keepalive ticks AND deadline tasks, which is exactly why
   * {@link #scheduleKeepAlive}'s tick is a {@code tryLock()} probe. Were this task to take {@code
   * lock.lock()} unconditionally, the fleet-wide stall the probe removes on the keepalive path
   * would be back here: a delivery worker parked inside a slow {@code emitter.emit()} holds its
   * stream's send lock, this task parks the one scheduler thread behind it, and EVERY other
   * stream's keepalives stop (idle proxies drop healthy connections) while their own deadlines fire
   * late — reopening, for all of them, the FD-leak window the deadline exists to close.
   *
   * <p>A deadline differs from a keepalive tick in one way that decides the remedy: a missed
   * keepalive is harmless (the in-flight write it collided with IS the liveness signal), whereas a
   * missed completion would leak the stream forever. So the lock is probed rather than taken, and a
   * lost probe is <em>retried</em> — the task is periodic ({@value #DEADLINE_RETRY_MILLIS} ms) and
   * cancels itself once the completion has actually landed. Completing without the lock was
   * rejected: the delivery path drives {@code emitter.emit()} under it, and Reactive Streams
   * forbids a terminal signal racing an {@code onNext} — the disconnect hook in {@link #stream} may
   * skip the lock only because it runs on the publisher's thread while THIS stream's writer is the
   * stalled one, which is not the case here.
   */
  private Future<?> scheduleTimeout(
      MultiEmitter<? super OutboundSseEvent> emitter, ReentrantLock lock) {
    if (timeout == null || keepAlive == null) {
      return null;
    }
    // Self-reference so the task can stop rescheduling once it has completed the emitter; mirrors
    // scheduleKeepAlive below. A run that observes a not-yet-published reference (possible only for
    // a sub-retry-interval timeout) simply completes again on the next run — completion is
    // idempotent — and cancels then.
    AtomicReference<Future<?>> self = new AtomicReference<>();
    Future<?> deadline =
        keepAlive.scheduleWithFixedDelay(
            () -> {
              if (!lock.tryLock()) {
                // Mid-send: retry on the next run rather than park the ONE shared scheduler thread
                // behind this stream's stalled write, starving every other stream.
                return;
              }
              try {
                try {
                  emitter.complete();
                } finally {
                  lock.unlock();
                }
              } catch (Throwable t) {
                log.warn(
                    "Failed to complete an SSE stream at its configured timeout: {}", t.toString());
              } finally {
                // The completion above fires onTermination, which cancels this future too; doing it
                // here as well keeps the task self-terminating even if the emitter was already
                // settled (a terminated emitter never fires the hook a second time).
                Future<?> f = self.get();
                if (f != null) {
                  f.cancel(false);
                }
              }
            },
            timeout.toMillis(),
            DEADLINE_RETRY_MILLIS,
            TimeUnit.MILLISECONDS);
    self.set(deadline);
    return deadline;
  }

  /**
   * Starts this stream's keepalive ticks, or returns {@code null} when the keepalive is disabled.
   * Each subscriber gets its OWN periodic task, so a throwing tick can only stop that stream's
   * keepalive — never every other subscriber's (the Spring integration, whose reaper is a single
   * shared tick over all emitters, guards that tick against it instead).
   *
   * <p><b>Never blocks on a mid-send stream.</b> {@link #keepAlive} is the process's SINGLE shared
   * scheduler thread for every stream's keepalive ticks AND deadline tasks. Parking that one thread
   * waiting for {@code lock} — held by a delivery worker stalled inside a slow/blocked {@code
   * emitter.emit()} — would starve every OTHER stream's keepalive and deadline tasks too, not just
   * this one, exactly the fleet-wide stall {@code SseController#sendKeepAlives} on Spring already
   * guards against with the identical {@code tryLock()} probe. So the lock is only probed: if it is
   * held, the emit is in flight and IS this subscriber's liveness signal, so this tick is skipped
   * rather than blocking; the next scheduled tick probes again. The event-delivery path (the {@code
   * subscriber} lambda in {@link #stream}) still takes the lock unconditionally, so emits are never
   * interleaved.
   */
  private Future<?> scheduleKeepAlive(
      MultiEmitter<? super OutboundSseEvent> emitter, ReentrantLock lock, Runnable teardown) {
    if (keepAliveInterval == null || keepAlive == null) {
      return null;
    }
    long intervalMillis = keepAliveInterval.toMillis();
    // Self-reference so a failing tick can cancel itself: scheduleAtFixedRate already stops
    // rescheduling a task that throws, but we swallow throwables to fail the stream cleanly
    // instead.
    AtomicReference<Future<?>> self = new AtomicReference<>();
    // Initial delay = interval: the opening frame is this stream's write at time zero.
    Future<?> ticker =
        keepAlive.scheduleAtFixedRate(
            () -> {
              if (!lock.tryLock()) {
                // Mid-send: the in-flight event write is this subscriber's liveness probe; skip
                // this tick rather than park the shared scheduler thread for every other stream.
                return;
              }
              try {
                try {
                  emitter.emit(KEEP_ALIVE);
                } finally {
                  lock.unlock();
                }
              } catch (Throwable t) {
                Future<?> f = self.get();
                if (f != null) {
                  f.cancel(false);
                }
                evictAfterFailedWrite(emitter, teardown, "keepalive tick", t);
              }
            },
            intervalMillis,
            intervalMillis,
            TimeUnit.MILLISECONDS);
    self.set(ticker);
    return ticker;
  }

  /**
   * Emits the comment frame that opens a stream: the {@link #KEEP_ALIVE} frame, once, as the last
   * step of the subscription. The caller holds the stream's send lock, taken before it registered
   * the client.
   *
   * <p><b>What it guarantees.</b> The frame is emitted after {@link SseEventPublisher#subscribe}
   * returned, so it cannot reach the client before the client is registered: a client that has read
   * it receives every event of the stream published from then on, for as long as it stays
   * connected. Without it the first bytes of the body would be the first event or the first
   * periodic keepalive, up to a whole {@code streamrune.sse.keep-alive-interval} later, and a
   * client could not tell an open stream from one still being set up.
   *
   * <p><b>The response head is not that signal on Quarkus.</b> Quarkus REST writes the status line
   * and the headers of a Server-Sent Events response before it subscribes to the returned {@link
   * Multi}, and the client is registered in that subscription. The head therefore reaches the
   * client an instant before the registration; this frame is the first thing written after it.
   *
   * <p>An event published between the registration and this call waits for the send lock in its
   * delivery worker and is emitted after this frame, which is therefore the first frame of every
   * stream; none is lost, and events keep their order because one worker delivers them.
   *
   * <p>A subscriber that throws while it is handed this frame is ended by the {@link
   * DownstreamFailureGuard}. An emission that fails nevertheless ends the stream like a failed
   * keepalive tick ({@link #evictAfterFailedWrite}): the emitter is failed and the stream's
   * teardown unsubscribes the client from the publisher. A write the socket refuses is reported by
   * Quarkus REST as a cancellation, whose termination hook runs the same teardown.
   */
  private static void emitOpeningFrame(
      MultiEmitter<? super OutboundSseEvent> emitter, Runnable teardown) {
    try {
      emitter.emit(KEEP_ALIVE);
    } catch (Throwable t) {
      evictAfterFailedWrite(emitter, teardown, "opening frame", t);
    }
  }

  /**
   * Ends a stream one of whose comment frames could not be emitted: fails the emitter, then runs
   * the stream's teardown. {@code emitter.fail} is the normal terminal signal and runs the teardown
   * through the termination hook; running it here as well covers an emitter left unable to
   * terminate by an exception that reached it (see the class documentation), whose hook would never
   * run.
   */
  private static void evictAfterFailedWrite(
      MultiEmitter<? super OutboundSseEvent> emitter,
      Runnable teardown,
      String write,
      Throwable cause) {
    log.warn("Evicting an SSE subscriber after a failed {}: {}", write, cause.toString());
    try {
      emitter.fail(cause);
    } catch (Throwable _) {
      // already terminated; the termination hook has run the teardown
    } finally {
      teardown.run();
    }
  }

  /** A teardown step that cancels a scheduled task; nothing to do when none was scheduled. */
  private static Runnable cancelling(Future<?> task) {
    return () -> {
      if (task != null) {
        task.cancel(false);
      }
    };
  }

  /**
   * The teardown of one stream: the steps that release what the stream holds, added while the
   * stream is set up and run together, once, when it ends — whichever of the termination hook and
   * the evictions asks first. A step added after the teardown ran is run on the spot, so a stream
   * that ends while it is still being set up releases what is acquired afterwards too. The steps
   * run under this object's monitor, possibly on the publisher's thread: a step must not block.
   */
  private static final class Teardown implements Runnable {

    private final List<Runnable> steps = new ArrayList<>();
    private boolean ran;

    synchronized void add(Runnable step) {
      if (ran) {
        step.run();
      } else {
        steps.add(step);
      }
    }

    @Override
    public synchronized void run() {
      if (ran) {
        return;
      }
      ran = true;
      steps.forEach(Runnable::run);
    }
  }

  /**
   * A comment-only SSE frame used as the keepalive. {@code getData()} is {@code null}, which
   * Quarkus REST's {@code SseUtil.serialiseEvent} handles by omitting the {@code data} field, and
   * no {@code content-type} field is written for a non-{@code OutboundSseEventImpl} event — so the
   * wire form is exactly {@code : keepalive}, a comment every SSE client ignores. It carries no id,
   * so it never disturbs a client's {@code Last-Event-ID} resume position.
   */
  record KeepAliveSseFrame() implements OutboundSseEvent {

    @Override
    public Class<?> getType() {
      return null;
    }

    @Override
    public Type getGenericType() {
      return null;
    }

    @Override
    public MediaType getMediaType() {
      return null;
    }

    @Override
    public String getId() {
      return null;
    }

    @Override
    public String getName() {
      return null;
    }

    // The leading space is there for the three integrations to write the same bytes: Micronaut's
    // Server-Sent Events codec puts one space after the colon of every line it writes, a comment
    // included. A client ignores a comment line whatever it holds.
    @Override
    public String getComment() {
      return " keepalive";
    }

    @Override
    public long getReconnectDelay() {
      return -1;
    }

    @Override
    public boolean isReconnectDelaySet() {
      return false;
    }

    @Override
    public Object getData() {
      return null;
    }
  }

  /**
   * Explicit {@link OutboundSseEvent} for a published domain event: {@code id} is the global
   * offset, {@code data} is the domain event, which Quarkus REST serializes as JSON because {@link
   * SseController#stream(String, String, HttpHeaders)} declares that stream element type. No event
   * name is set, so clients receive default {@code message} events — the same wire contract as the
   * Spring integration.
   *
   * @param id the SSE event id (the event's global offset)
   * @param data the domain event payload
   */
  record EnvelopeSseFrame(String id, DomainEvent data) implements OutboundSseEvent {

    @Override
    public Class<?> getType() {
      return data.getClass();
    }

    @Override
    public Type getGenericType() {
      return data.getClass();
    }

    // States the payload's media type for a reader of the frame. Quarkus REST does not read it
    // from a frame the application implements: the endpoint's stream element type decides.
    @Override
    public MediaType getMediaType() {
      return MediaType.APPLICATION_JSON_TYPE;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public String getComment() {
      return null;
    }

    @Override
    public long getReconnectDelay() {
      return -1;
    }

    @Override
    public boolean isReconnectDelaySet() {
      return false;
    }

    @Override
    public Object getData() {
      return data;
    }
  }
}
