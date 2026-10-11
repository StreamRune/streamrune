package org.streamrune.spring;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
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
 * Server-Sent Events endpoint. Streams one aggregate's domain events: {@code GET
 * /api/sse/{aggregateType}/{aggregateId}}, the two parts of the typed {@link StreamId}; an invalid
 * part is answered with {@code 400 Bad Request}.
 *
 * <p>Access is gated by an {@link SseAuthorizer}: the caller and the requested {@link StreamId} are
 * checked before any subscription is created; a denied request is rejected with {@code 403
 * Forbidden}. The endpoint is registered only when {@code streamrune.sse.enabled=true}.
 *
 * <p><b>Where the frames come from.</b> The auto-configuration registers an {@link
 * org.streamrune.runtime.SseEventFeed} beside this controller. It starts at the head of the global
 * stream when the application starts and publishes every event stored from then on to the
 * subscribers of the event's own stream, every {@code streamrune.sse.polling-interval}. Delivery is
 * live, best-effort and at-most-once: a frame reaches only the clients connected at that moment,
 * nothing is redelivered after a reconnect, and {@code Last-Event-ID} is not honoured.
 *
 * <p><b>Caller identity.</b> The caller handed to the authorizer is resolved by the same {@link
 * RequestIdentityPolicy} the {@link ScopedValueFilter} binds {@code RequestContext.userId} with, so
 * a stream subscription and a command from the same request see the same user: the authenticated
 * principal when an {@link AuthenticatedUserResolver} is available (the {@code X-User-Id} header
 * ignored), the {@code X-User-Id} header only in the explicit trusted-gateway mode ({@code
 * streamrune.security.trust-user-id-header=true}), and otherwise nobody ({@code null}). Both read
 * the header through {@link ScopedValueFilter#userIdHeaderValues}, every value as received, so a
 * repeated {@code X-User-Id} cannot resolve to one caller here and another there.
 *
 * <p><b>Opening frame.</b> The last step of a subscription sends one {@code : keepalive} comment
 * frame, the same frame on the wire as the Quarkus and Micronaut integrations write. It commits the
 * response: the client sees the status line, the headers and this frame as soon as it is
 * subscribed, not with the first event or the first periodic keepalive. It is sent after the client
 * is registered with the {@link SseEventPublisher}, and nothing of the response is written before
 * it: a client that has received the first bytes of the response receives every event of the stream
 * published from then on, for as long as it stays connected.
 *
 * <p><b>Dead-client reaping.</b> Each emitter is given a <em>finite</em> timeout ({@code
 * streamrune.sse.timeout}) and a periodic keepalive comment frame is written to every subscriber
 * ({@code streamrune.sse.keep-alive-interval}). A half-open TCP client (mobile/NAT drop with no
 * FIN/RST) on an idle stream produces no writes on its own, so without these it would never trigger
 * {@code onCompletion}/{@code onError}/{@code onTimeout} and its subscription, delivery worker, and
 * socket FD would leak forever across reconnect churn. The keepalive turns a dead connection into a
 * failed send (prompt eviction); the finite timeout is the backstop. SSE clients auto-reconnect.
 * The shared keepalive tick probes each registration's send lock <em>non-blockingly</em> and skips
 * a mid-send subscriber for that tick, so one stalled client's blocking write can never suspend
 * keepalives — and with them dead-client eviction — for every other subscriber; the
 * completion/timeout/error cleanup path never touches the send lock, so eviction itself cannot
 * block on a stalled send either.
 *
 * <p><b>A client that resets before the response is written.</b> Spring MVC writes the opening
 * frame while it takes the emitter over, before it attaches the emitter's callbacks to the request.
 * When that write fails, no callback runs and nothing ends the asynchronous request. The stream is
 * therefore remembered on the request, and a {@link SseHandoverFailureResolver} — a bean of the
 * auto-configuration, consulted before every other exception resolver of the application — releases
 * it the moment Spring MVC reports the failure: the client is unsubscribed and the request ended.
 * See {@link #releaseStreamOf}.
 *
 * <p><b>Slow-consumer eviction.</b> The publisher evicts a client whose queue is full on the thread
 * that publishes: the feed's polling thread, which serves every stream. The disconnect hook
 * unsubscribes the client inline and fails its emitter on a virtual thread of its own, because
 * completing an emitter waits for a write in progress on it and the write to a client that has
 * stopped reading lasts until the container's write timeout. The feed is not held; the stalled
 * connection is closed when that write gives up.
 *
 * <p><b>Timeout contract.</b> A <em>non-positive</em> {@code streamrune.sse.timeout} disables the
 * deadline — identical semantics on Spring, Quarkus and Micronaut — leaving the keepalive as the
 * sole reaper. Disabling both knobs re-opens the dead-client FD leak.
 *
 * <p><b>Shutdown.</b> An open stream is an asynchronous servlet request that never finishes on its
 * own, and Spring Boot's graceful shutdown waits for in-flight requests before it stops the web
 * server — so a connected client would hold every shutdown for the whole {@code
 * spring.lifecycle.timeout-per-shutdown-phase} and then have its connection cut. The controller is
 * therefore a {@link SmartLifecycle} whose {@link #PHASE} stops it <em>before</em> the web server's
 * graceful-shutdown phase: {@link #stop(Runnable) stop} unsubscribes and completes every open
 * stream, so the drain finds nothing to wait for and each client sees its stream end normally (an
 * {@code EventSource} reconnects, reaching another replica). A subscription that arrives after the
 * stop is answered with a stream that is already complete. Each completion runs on its own virtual
 * thread — completing an emitter waits for a write that is in progress on it, and a stalled client
 * must not hold the others — and the stop reports done when all of them have returned, so the
 * lifecycle phase timeout bounds the wait. {@link #start()} (a context restart) admits
 * subscriptions again; the keepalive scheduler runs until {@link #close()}.
 */
@RestController
@RequestMapping("/api/sse")
public class SseController implements SmartLifecycle, AutoCloseable {

  /**
   * The lifecycle phase: above Spring Boot's web-server graceful-shutdown phase ({@code
   * SmartLifecycle.DEFAULT_PHASE - 1024}), so the open streams are completed before the web server
   * starts draining in-flight requests.
   */
  public static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 512;

  private static final Logger log = LoggerFactory.getLogger(SseController.class);

  /**
   * The text of the keepalive comment; the line on the wire is {@code : keepalive}. The leading
   * space is there for the three integrations to write the same bytes: Micronaut's Server-Sent
   * Events codec puts one space after the colon of every line it writes, a comment included. A
   * client ignores a comment line whatever it holds.
   */
  static final String KEEP_ALIVE_COMMENT = " keepalive";

  /**
   * The request attribute that holds the stream a request opened ({@link RequestStream}), for
   * {@link #releaseStreamOf}.
   */
  static final String STREAM_ATTRIBUTE = SseController.class.getName() + ".stream";

  private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration DEFAULT_KEEP_ALIVE_INTERVAL = Duration.ofSeconds(30);

  private final SseEventPublisher publisher;
  private final SseAuthorizer authorizer;
  private final RequestIdentityPolicy identityPolicy;
  private final long timeoutMillis;

  /** Live emitters, tracked so the keepalive tick can reach them and reap dead ones. */
  private final Set<ActiveEmitter> active = ConcurrentHashMap.newKeySet();

  /** Shared keepalive scheduler; {@code null} when the keepalive is disabled. */
  private final ScheduledExecutorService keepAlive;

  /**
   * Orders admitting a subscription against stopping: a subscription is either registered (and
   * subscribed to the publisher) before a stop takes its snapshot of {@link #active}, or it sees
   * the stop and is answered with a completed stream. Never held across emitter I/O.
   */
  private final ReentrantLock admission = new ReentrantLock();

  /** Whether new subscriptions are admitted; cleared by {@link #stop(Runnable)}. */
  private volatile boolean running = true;

  /**
   * Creates the controller with the default reaping windows (5-minute timeout, 30-second
   * keepalive).
   *
   * @param publisher the event fan-out the subscriptions attach to
   * @param authorizer decides whether the caller may subscribe to a stream
   * @param identityPolicy resolves the caller — the same policy the request filter uses
   * @throws IllegalArgumentException if {@code identityPolicy} is {@code null}
   */
  public SseController(
      SseEventPublisher publisher, SseAuthorizer authorizer, RequestIdentityPolicy identityPolicy) {
    this(publisher, authorizer, identityPolicy, DEFAULT_TIMEOUT, DEFAULT_KEEP_ALIVE_INTERVAL);
  }

  /**
   * Creates the controller with explicit reaping windows.
   *
   * @param publisher the event fan-out the subscriptions attach to
   * @param authorizer decides whether the caller may subscribe to a stream
   * @param identityPolicy resolves the caller — the same policy the request filter uses
   * @param timeout the emitter timeout; {@code null} keeps the default, non-positive disables it
   * @param keepAliveInterval the keepalive period; {@code null} or non-positive disables it
   * @throws IllegalArgumentException if {@code identityPolicy} is {@code null}
   */
  public SseController(
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
    this.timeoutMillis = timeoutMillisFor(timeout);

    if (keepAliveInterval != null
        && !keepAliveInterval.isZero()
        && !keepAliveInterval.isNegative()) {
      long intervalMillis = keepAliveInterval.toMillis();
      this.keepAlive =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "streamrune-sse-keepalive");
                t.setDaemon(true);
                return t;
              });
      // Initial delay = interval: a stream's opening frame is its own write at time zero.
      this.keepAlive.scheduleAtFixedRate(
          this::sendKeepAlives, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    } else {
      this.keepAlive = null;
    }
  }

  /**
   * Maps the configured {@code streamrune.sse.timeout} onto the millisecond value handed to {@link
   * SseEmitter}: {@code null} (unset) keeps the finite {@link #DEFAULT_TIMEOUT}, and a
   * <strong>non-positive</strong> value disables the deadline entirely.
   *
   * <p>One contract across the three integrations: the Quarkus and Micronaut controllers drop the
   * deadline for a non-positive value ({@code positiveOrNull}), and so does this one, leaving the
   * keepalive as the dead-client reaper. {@code SseEmitter(0)} is the servlet-async "never time
   * out" value ({@code AsyncContext.setTimeout(0)}), so no separate null path is needed.
   */
  private static long timeoutMillisFor(Duration timeout) {
    if (timeout == null) {
      return DEFAULT_TIMEOUT.toMillis();
    }
    if (timeout.isZero() || timeout.isNegative()) {
      return 0L;
    }
    // A positive sub-millisecond duration must not round down to 0 and silently disable the
    // deadline.
    return Math.max(1L, timeout.toMillis());
  }

  /**
   * Opens an SSE stream of one aggregate's events: {@code GET
   * /api/sse/{aggregateType}/{aggregateId}} (for example {@code /api/sse/order/o-1}).
   *
   * @param aggregateType the aggregate type of the stream to subscribe to
   * @param aggregateId the aggregate id of the stream to subscribe to
   * @param request the HTTP request; every {@code X-User-Id} value it carries is handed to the
   *     {@link RequestIdentityPolicy}, an identity only in the trusted-gateway mode (see the class
   *     documentation)
   * @return the open emitter; once the controller is stopped, an emitter that is already complete
   * @throws ResponseStatusException with {@code 400 Bad Request} when the aggregate type or the
   *     aggregate id is invalid, {@code 403 Forbidden} when the authorizer denies the caller
   */
  @GetMapping(
      value = "/{aggregateType}/{aggregateId}",
      produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter stream(
      @PathVariable("aggregateType") String aggregateType,
      @PathVariable("aggregateId") String aggregateId,
      HttpServletRequest request) {
    RequestStream stream =
        open(aggregateType, aggregateId, ScopedValueFilter.userIdHeaderValues(request));
    // From here Spring MVC takes the emitter over. Should that fail, the stream is found on the
    // request and released: see releaseStreamOf.
    request.setAttribute(STREAM_ATTRIBUTE, stream);
    return stream.emitter();
  }

  /**
   * Opens an SSE stream of the given aggregate's events for a caller whose {@code X-User-Id} values
   * were already read off the request.
   *
   * @param aggregateType the aggregate type of the stream to subscribe to
   * @param aggregateId the aggregate id of the stream to subscribe to
   * @param userIdHeaderValues every {@code X-User-Id} value the request carried, as received;
   *     {@code null} or empty when absent
   * @return the open emitter; once the controller is stopped, an emitter that is already complete
   * @throws ResponseStatusException with {@code 400 Bad Request} when the aggregate type or the
   *     aggregate id is invalid (the authorizer is never consulted), {@code 403 Forbidden} when the
   *     authorizer denies the caller
   */
  public SseEmitter stream(
      String aggregateType, String aggregateId, List<String> userIdHeaderValues) {
    return open(aggregateType, aggregateId, userIdHeaderValues).emitter();
  }

  /** Authorizes the caller and opens the stream: its emitter and the cleanup that releases it. */
  private RequestStream open(
      String aggregateType, String aggregateId, List<String> userIdHeaderValues) {
    StreamId sid = streamIdOf(aggregateType, aggregateId);
    UserId principal = identityPolicy.resolve(userIdHeaderValues, SseController::logMismatch);
    if (!authorizer.isAuthorized(principal, sid)) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Not authorized for this stream");
    }

    SseEmitter emitter = new SseEmitter(timeoutMillis);
    // Serializes event delivery (on the subscriber's worker thread) with keepalive writes (on the
    // scheduler thread): SseEmitter is not safe for concurrent send(). A ReentrantLock (not a
    // monitor) so the shared keepalive tick can PROBE it non-blockingly with tryLock() — see
    // sendKeepAlives(). The event path takes it unconditionally.
    ReentrantLock sendLock = new ReentrantLock();

    SseEventPublisher.SseSubscriber subscriber =
        envelope -> {
          sendLock.lock();
          try {
            emitter.send(
                SseEmitter.event()
                    .data(envelope.event())
                    .id(String.valueOf(envelope.globalOffset().value()))
                    .build());
          } catch (IOException e) {
            emitter.completeWithError(e);
          } finally {
            sendLock.unlock();
          }
        };
    ActiveEmitter registration =
        new ActiveEmitter(emitter, sendLock, () -> publisher.unsubscribe(sid, subscriber));

    Runnable cleanup =
        () -> {
          active.remove(registration);
          registration.unsubscribe.run();
        };

    // The publisher's slow-consumer eviction is only as good as this hook: an evicted client
    // whose emitter stayed open would receive no further event while its keepalive still wrote
    // successfully (the client is slow, not dead), and with streamrune.sse.timeout=0 no deadline
    // would end it either. The hook can run on the PUBLISHER's thread, inline in publish(), so it
    // waits for nothing: the cleanup takes no lock a write holds, and the emitter is failed on a
    // thread of its own (see failOnItsOwnThread).
    boolean admitted;
    admission.lock();
    try {
      // Registered and subscribed together, or not at all: a stop that has already taken its
      // snapshot of the open streams would never end a registration added after it.
      admitted = running;
      if (admitted) {
        active.add(registration);
        publisher.subscribe(
            sid,
            subscriber,
            cause -> {
              cleanup.run();
              failOnItsOwnThread(emitter, cause);
            });
      }
    } finally {
      admission.unlock();
    }
    if (!admitted) {
      // Stopping: a stream that is already complete makes the client reconnect (elsewhere)
      // instead of opening a request the web server's graceful-shutdown drain would wait for.
      emitter.complete();
      return new RequestStream(emitter, () -> {});
    }
    emitter.onCompletion(cleanup);
    emitter.onTimeout(cleanup);
    emitter.onError(t -> cleanup.run());
    // Last, when the client is registered with the publisher and the cleanup is in place: the
    // frame that commits the response.
    sendOpeningFrame(registration, sid);

    return new RequestStream(emitter, cleanup);
  }

  /**
   * Releases the stream a request opened when Spring MVC reports that handling the request failed;
   * does nothing for a request that opened none. Called by {@link SseHandoverFailureResolver}, on
   * the request's own thread.
   *
   * <p><b>Why the emitter's callbacks are not enough.</b> The opening frame is handed to the
   * emitter before the handler method returns, so Spring MVC writes it while it takes the emitter
   * over ({@code ResponseBodyEmitter.initialize}), and it attaches the emitter's completion,
   * timeout and error callbacks to the request only after that write. A client that reset its
   * connection in the meantime makes the write fail: the failure is thrown out of the hand-over, no
   * callback of the emitter ever runs, and the request stays in asynchronous mode with nobody left
   * to end it. The client would stay subscribed until a keepalive write failed (never with {@code
   * streamrune.sse.keep-alive-interval=0}), and the request would hold the web server's graceful
   * shutdown for its whole phase.
   *
   * <p><b>What this does.</b> It runs the stream's cleanup — the registration leaves {@link
   * #active} and the client is unsubscribed from the publisher — and fails the emitter, which hands
   * Spring MVC the result that ends the asynchronous request. Both are idempotent: the cleanup may
   * already have run through a callback, an eviction or a stop, and failing an emitter that is
   * already complete changes nothing.
   *
   * @param request the request whose handling failed
   * @param failure what Spring MVC reported
   */
  static void releaseStreamOf(HttpServletRequest request, Exception failure) {
    if (!(request.getAttribute(STREAM_ATTRIBUTE) instanceof RequestStream stream)) {
      return;
    }
    request.removeAttribute(STREAM_ATTRIBUTE);
    try {
      stream.cleanup().run();
    } catch (RuntimeException e) {
      log.warn("The cleanup of an SSE stream threw after its request failed", e);
    }
    try {
      stream.emitter().completeWithError(failure);
    } catch (RuntimeException alreadyCompleted) {
      log.debug("The SSE stream of a failed request was already complete", alreadyCompleted);
    }
    log.debug("Released the SSE stream of a request Spring MVC reported as failed", failure);
  }

  /**
   * Hands the emitter the comment frame that opens a stream: one {@code : keepalive} comment, the
   * frame {@link #sendKeepAlives()} writes.
   *
   * <p><b>What it guarantees.</b> Spring MVC writes nothing for an {@link SseEmitter} until the
   * handler method has returned; a frame sent before that is held by the emitter and written, with
   * the status line and the headers, when Spring MVC takes the emitter over. This call comes after
   * {@link SseEventPublisher#subscribe} returned, so the first bytes of the response cannot reach
   * the client before the client is registered: a client that has received them receives every
   * event of the stream published from then on, for as long as it stays connected. Without this
   * frame the response would be committed by the first event or the first periodic keepalive, up to
   * a whole {@code streamrune.sse.keep-alive-interval} later.
   *
   * <p>An event published between the registration and this call is held by the emitter ahead of
   * this frame and written before it; none is lost, and events keep their order because one worker
   * delivers them. The send lock serializes this call with that worker's.
   *
   * <p>The only failure possible here is an emitter that is already complete: the controller was
   * stopped, or the publisher evicted the client, between the registration and this call. Whoever
   * completed it has unsubscribed the client, so there is nothing to write and nothing to clean up.
   * A failure to write the held frames to the socket happens later, inside Spring MVC, before the
   * emitter's callbacks are attached to the request: no callback reports it, and {@link
   * #releaseStreamOf} releases the stream instead.
   */
  private static void sendOpeningFrame(ActiveEmitter registration, StreamId sid) {
    registration.sendLock.lock();
    try {
      registration.emitter.send(SseEmitter.event().comment(KEEP_ALIVE_COMMENT));
    } catch (IOException | IllegalStateException alreadyCompleted) {
      // Both parts arrive percent-decoded from the request path; every id in a log line is rendered
      // through the sanitizer so no value can forge a line (CWE-117).
      log.debug(
          "SSE stream {} was completed before its opening frame was sent",
          LogSanitizer.sanitizeForLog(sid.value()),
          alreadyCompleted);
    } finally {
      registration.sendLock.unlock();
    }
  }

  /**
   * Fails a stream's emitter on a virtual thread of its own and returns at once.
   *
   * <p>Spring's emitter serializes {@code send} and completion on one lock, and a {@code send} is a
   * blocking servlet write: to a client that has stopped reading it holds that lock until the
   * container's write timeout (Tomcat: {@code server.tomcat.connection-timeout}, 60 seconds unless
   * configured). Completing the emitter therefore waits for the write in progress on it. The two
   * callers each serve every stream of the application — the publisher's thread on a slow-consumer
   * eviction (the {@link org.streamrune.runtime.SseEventFeed}'s one polling thread) and the shared
   * keepalive tick — so neither waits: the stalled client's own thread does. By the time this is
   * called the client is unsubscribed and out of {@link #active}; what the thread ends is the HTTP
   * response.
   */
  private static void failOnItsOwnThread(SseEmitter emitter, Throwable cause) {
    Thread.ofVirtual()
        .name("streamrune-sse-evict")
        .start(
            () -> {
              try {
                emitter.completeWithError(cause);
              } catch (RuntimeException alreadyCompleted) {
                log.debug("SSE eviction: the stream was already complete", alreadyCompleted);
              }
            });
  }

  /** The two path segments through their ingress doors; an invalid part is the caller's error. */
  private static StreamId streamIdOf(String aggregateType, String aggregateId) {
    try {
      return StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    } catch (IllegalArgumentException _) {
      throw new ResponseStatusException(
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
   * Writes a keepalive comment to every live emitter. A comment frame ({@code : keepalive}) is
   * ignored by SSE clients but forces a socket write, so a half-open/dead connection surfaces as a
   * failed send here and is evicted immediately instead of lingering until the timeout. Package
   * private so a test can drive one tick deterministically.
   *
   * <p><b>Never blocks on a mid-send registration.</b> This is the process's SINGLE shared tick:
   * parking it on one registration's send lock — held by a delivery worker stalled inside a
   * blocking servlet {@code emitter.send()} (TCP zero-window client; the container write timeout
   * can be minutes) — would starve every OTHER subscriber of keepalives and suspend the dead-client
   * eviction fleet-wide (the same invariant that holds for a THROWING emitter, violated by a
   * BLOCKED one). So the lock is only {@code tryLock()}'d: if it is held, the emitter is mid-send,
   * and the in-flight write is itself the liveness probe — a dead client fails that write, {@code
   * completeWithError} fires, and the normal cleanup path evicts it. The skip is per-tick; the next
   * tick probes the registration again. The event-send path still takes the lock unconditionally,
   * so emitter writes are never interleaved (SseEmitter is not thread-safe).
   */
  void sendKeepAlives() {
    for (ActiveEmitter registration : active) {
      if (!registration.sendLock.tryLock()) {
        // Mid-send: the in-flight event write is this subscriber's liveness probe; skip this tick
        // rather than park the shared keepalive thread for every other subscriber.
        continue;
      }
      try {
        try {
          registration.emitter.send(SseEmitter.event().comment(KEEP_ALIVE_COMMENT));
        } finally {
          registration.sendLock.unlock();
        }
      } catch (Throwable t) {
        // Catch Throwable, not Exception. sendKeepAlives runs as a scheduleAtFixedRate
        // task, so ANY throwable that escapes it — an Error from send(), or a RuntimeException from
        // the unsubscribe/complete reaping below — silently cancels ALL future keepalive ticks for
        // the process, degrading dead-client reaping back to timeout-only reaping with no log line.
        // A reaper's
        // contract is availability: one bad emitter must never kill the keepalive for every other
        // subscriber, so catch broadly here, log loudly, and continue to the next registration.
        // Dead connection: reap it directly (do not rely on the servlet firing onError), then
        // fail the emitter on a thread of its own: this tick serves every stream, and a delivery
        // worker may have entered a blocking write on the emitter since the send lock was
        // released. unsubscribe is idempotent, so a later container-driven onError is harmless.
        active.remove(registration);
        try {
          registration.unsubscribe.run();
        } catch (Throwable unsubscribeError) {
          // Guarded: a throwing unsubscribe must not abort the tick or skip completeWithError.
          log.warn(
              "SSE keepalive: unsubscribe hook threw while reaping a dead subscriber",
              unsubscribeError);
        }
        try {
          failOnItsOwnThread(registration.emitter, t);
        } catch (Throwable startFailure) {
          // Guarded like the unsubscribe: nothing may abort the tick.
          log.warn("SSE keepalive: could not fail a reaped subscriber's emitter", startFailure);
        }
        log.warn("Evicted an SSE subscriber on keepalive after a failed tick: {}", t.toString());
      }
    }
  }

  /**
   * Package-private test seam: registers a raw emitter + unsubscribe hook into the live set so a
   * test can drive {@link #sendKeepAlives()} against a controllable emitter (e.g. one whose send()
   * throws an Error, or a throwing unsubscribe) without standing up a full servlet request. Returns
   * the registration's send lock so a test can occupy it like a mid-send delivery worker. Not used
   * in production.
   */
  ReentrantLock trackForTest(SseEmitter emitter, Runnable unsubscribe) {
    ReentrantLock sendLock = new ReentrantLock();
    active.add(new ActiveEmitter(emitter, sendLock, unsubscribe));
    return sendLock;
  }

  /** Package-private test seam: number of currently-tracked live emitters. */
  int activeCountForTest() {
    return active.size();
  }

  /**
   * Admits subscriptions again after a {@linkplain #stop(Runnable) stop} (a context restart). A new
   * controller already admits them, so the context's start-up does not call this.
   */
  @Override
  public void start() {
    admission.lock();
    try {
      running = true;
    } finally {
      admission.unlock();
    }
  }

  /**
   * Stops admitting subscriptions and ends every open stream, returning once each completion has
   * returned. The application context calls {@link #stop(Runnable)} instead, whose wait its
   * shutdown-phase timeout bounds.
   */
  @Override
  public void stop() {
    CountDownLatch ended = new CountDownLatch(1);
    stop(ended::countDown);
    try {
      ended.await();
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Stops admitting subscriptions, then unsubscribes every open stream from the publisher and
   * completes its emitter, so the client sees the stream end normally and the web server has no
   * request left to drain. Each stream is ended on its own virtual thread — completing an emitter
   * waits for a write in progress on it, and one stalled client must not hold the others or the
   * shutdown thread. {@code callback} runs once every completion has returned, at once when no
   * stream is open.
   *
   * @param callback told when every open stream has been ended
   */
  @Override
  public void stop(Runnable callback) {
    List<ActiveEmitter> open = new ArrayList<>();
    admission.lock();
    try {
      running = false;
      for (ActiveEmitter registration : active) {
        // Whoever removes a registration ends it: the completion, timeout, error and keepalive
        // paths race this one, and their own cleanup is idempotent.
        if (active.remove(registration)) {
          open.add(registration);
        }
      }
    } finally {
      admission.unlock();
    }
    if (open.isEmpty()) {
      callback.run();
      return;
    }
    log.info("Completing {} open SSE stream(s) before the web server shuts down", open.size());
    AtomicInteger remaining = new AtomicInteger(open.size());
    for (ActiveEmitter registration : open) {
      Thread.ofVirtual()
          .name("streamrune-sse-shutdown")
          .start(
              () -> {
                try {
                  end(registration);
                } finally {
                  if (remaining.decrementAndGet() == 0) {
                    callback.run();
                  }
                }
              });
    }
  }

  private static void end(ActiveEmitter registration) {
    try {
      registration.unsubscribe.run();
    } catch (RuntimeException e) {
      log.warn("SSE shutdown: the unsubscribe hook threw while ending a stream", e);
    }
    try {
      registration.emitter.complete();
    } catch (RuntimeException alreadyCompleted) {
      // The client went away, the deadline fired or the publisher evicted it: nothing to end.
      log.debug("SSE shutdown: the stream was already complete", alreadyCompleted);
    }
  }

  /**
   * Whether new subscriptions are admitted: from construction until {@link #stop(Runnable)}.
   *
   * @return {@code true} while subscriptions are admitted
   */
  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * Returns {@link #PHASE}, which stops the controller before the web server's graceful shutdown.
   *
   * @return {@link #PHASE}
   */
  @Override
  public int getPhase() {
    return PHASE;
  }

  /**
   * Ends any stream still open — when no lifecycle stopped the controller — and stops the keepalive
   * scheduler. Spring invokes this as the inferred destroy method.
   */
  @Override
  public void close() {
    stop(() -> {});
    if (keepAlive != null) {
      keepAlive.shutdownNow();
    }
  }

  /**
   * The stream a request opened: its emitter and the idempotent cleanup that drops its registration
   * and unsubscribes its client. A request answered with an emitter that is already complete has
   * nothing to clean up.
   */
  private record RequestStream(SseEmitter emitter, Runnable cleanup) {}

  /**
   * A live emitter, the lock that serializes its event and keepalive writes, and the hook that
   * removes its subscription from the publisher when it is reaped. The lock is a {@link
   * ReentrantLock} (not a monitor) so the shared keepalive tick can probe it non-blockingly.
   */
  private static final class ActiveEmitter {
    private final SseEmitter emitter;
    private final ReentrantLock sendLock;
    private final Runnable unsubscribe;

    ActiveEmitter(SseEmitter emitter, ReentrantLock sendLock, Runnable unsubscribe) {
      this.emitter = emitter;
      this.sendLock = sendLock;
      this.unsubscribe = unsubscribe;
    }
  }
}
