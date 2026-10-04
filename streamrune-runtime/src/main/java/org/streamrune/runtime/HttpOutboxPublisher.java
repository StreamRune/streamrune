package org.streamrune.runtime;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.types.LogSanitizer;

/**
 * {@link OutboxPublisher} that delivers outbox entries to an HTTP endpoint.
 *
 * <p>POSTs the {@link OutboxEntry#payload()} as the request body with {@code Content-Type:
 * application/json}. Sets the following custom headers for receiver-side correlation:
 *
 * <ul>
 *   <li>{@code X-Outbox-Entry-Id} — the {@link OutboxEntry#id()} value
 *   <li>{@code X-Outbox-Payload-Type} — the {@link OutboxEntry#payloadType()} value
 * </ul>
 *
 * <p>Throws {@link RuntimeException} on any non-2xx HTTP status. The {@link
 * org.streamrune.runtime.OutboxPoller} will retry on any thrown exception.
 *
 * <p>Uses {@link HttpClient#newHttpClient()} — no external dependencies required.
 *
 * <p><b>Client ownership.</b> {@link #of(URI)} <em>owns</em> the {@link HttpClient} it creates — a
 * selector thread plus a per-request executor — and {@link #close()} releases it. Every DI
 * container invokes {@code close()} on an {@link AutoCloseable} bean by convention (Spring's
 * inferred destroy method, a Quarkus {@code @Disposes} / Micronaut {@code preDestroy = "close"}
 * producer), so a context refresh, a dev-mode live reload or a multi-context test suite no longer
 * strands the previous cycle's client. {@link #of(URI, HttpClient)} <em>borrows</em> a
 * caller-supplied client instead: {@code close()} then leaves it untouched, because the caller who
 * built it is the one that closes it. This is the same owned-vs-borrowed split {@code
 * PgAdvisoryLocker} applies to its {@code ownedPool}; the two broker transports need no such split
 * because they only ever borrow (both {@code KafkaOutboxPublisher} and {@code
 * RabbitMqOutboxPublisher} require a caller-built producer/channel and document that its lifecycle
 * is the caller's).
 */
public final class HttpOutboxPublisher implements OutboxPublisher, AutoCloseable {

  /**
   * Per-request timeout applied to every {@link #publish} exchange. This bounds the whole
   * request/response envelope, so it is also the publisher's {@link #inFlightHorizon() in-flight
   * horizon}: once it elapses a non-connect {@link java.net.http.HttpTimeoutException} is thrown
   * and the request is abandoned client-side (the server may still have processed it — the {@link
   * FailureKind#IN_FLIGHT} case).
   */
  static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

  private final URI endpoint;
  private final HttpClient httpClient;

  /**
   * Whether {@link #httpClient} was created by this publisher (and is therefore released by {@link
   * #close()}) or handed in by the caller (borrowed — closing it here would tear down a client the
   * caller may still be using, and would leave the caller's own {@code close()} to fail).
   */
  private final boolean ownsClient;

  private HttpOutboxPublisher(URI endpoint, HttpClient httpClient, boolean ownsClient) {
    this.endpoint = endpoint;
    this.httpClient = httpClient;
    this.ownsClient = ownsClient;
  }

  /**
   * Creates a publisher that <b>owns</b> a freshly built {@link HttpClient}. Release it with {@link
   * #close()} when the owning scope ends.
   *
   * @param endpoint the endpoint every entry is POSTed to (required)
   */
  public static HttpOutboxPublisher of(URI endpoint) {
    Objects.requireNonNull(endpoint, "endpoint is required");
    return new HttpOutboxPublisher(endpoint, HttpClient.newHttpClient(), true);
  }

  /**
   * Creates a publisher over a <b>caller-supplied</b> {@link HttpClient} — use this to configure
   * proxies, TLS, connect timeouts, HTTP/2 settings or an executor, or to share one client across
   * several publishers. The client is <em>borrowed</em>: {@link #close()} does not touch it, and
   * the caller stays responsible for closing it.
   *
   * @param endpoint the endpoint every entry is POSTed to (required)
   * @param httpClient the client to send with (required); not closed by this publisher
   */
  public static HttpOutboxPublisher of(URI endpoint, HttpClient httpClient) {
    Objects.requireNonNull(endpoint, "endpoint is required");
    Objects.requireNonNull(httpClient, "httpClient is required");
    return new HttpOutboxPublisher(endpoint, httpClient, false);
  }

  /**
   * Releases the {@link HttpClient} this publisher created, or does nothing when the client was
   * supplied by the caller (see {@link #of(URI, HttpClient)}).
   *
   * <p>{@link HttpClient#close()} shuts down in an orderly fashion: it stops accepting new requests
   * and blocks until the in-flight ones finish. That wait is bounded by {@link #REQUEST_TIMEOUT} —
   * the same envelope as the {@link #inFlightHorizon()} — because {@link #publish} times out every
   * exchange. Close the {@code OutboxPoller} before the publisher so no relay thread is mid-{@code
   * publish}; a publish attempted after this call fails, and the poller's classification of that
   * failure is the conservative {@link FailureKind#IN_FLIGHT} one (the claim is held until
   * lease-reclaim rather than re-armed), which never duplicates.
   *
   * <p>Idempotent: {@code HttpClient.close()} on an already-closed client returns immediately.
   */
  @Override
  public void close() {
    if (ownsClient) {
      httpClient.close();
    }
  }

  @Override
  public void publish(OutboxEntry entry) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(endpoint)
            .timeout(REQUEST_TIMEOUT)
            .header("Content-Type", "application/json")
            .header("X-Outbox-Entry-Id", entry.id().value())
            .header("X-Outbox-Payload-Type", entry.payloadType())
            .POST(HttpRequest.BodyPublishers.ofString(entry.payload()))
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new HttpDeliveryException(
          response.statusCode(),
          "HTTP delivery failed: status="
              + response.statusCode()
              + ", endpoint="
              + endpoint
              + ", entry="
              + LogSanitizer.sanitizeForLog(entry.id().value()));
    }
  }

  /**
   * Classifies a delivery failure for the poller's retry accounting. The rule for I/O failures is
   * <b>phase-based</b>, not type-based: what matters is whether the request could already have
   * reached the receiver.
   *
   * <ul>
   *   <li><b>Pre-hand-off</b> — the failure can only occur before the <em>first byte of the request
   *       left this process</em>, so nothing is in flight and an immediate re-arm cannot duplicate
   *       anything: {@link FailureKind#TRANSPORT} (retried forever with backoff, never
   *       terminal-fails the backlog). Exactly {@link #isPreHandoff the enumerated set}, plus
   *       {@link java.net.http.HttpConnectTimeoutException} — checked <em>before</em> its
   *       superclass {@link java.net.http.HttpTimeoutException} for that reason (Minor-1).
   *   <li><b>Post-hand-off or unclassifiable</b> — every other {@link java.io.IOException}, and a
   *       <em>non-connect</em> {@code HttpTimeoutException}: the request may have been fully sent
   *       and the receiver may be executing it right now (a connection reset or EOF while awaiting
   *       the response is the common case — an LB idle timeout, a NAT reset, a peer restart).
   *       {@link FailureKind#IN_FLIGHT}: the claim is <b>held</b> and only lease-reclaim
   *       redelivers, because re-arming would let a relay re-POST within ~1s into a request the
   *       receiver is still processing — a concurrent duplicate <em>and</em> a same-aggregate
   *       reorder. Holding is the conservative side: it costs one claim lease of recovery latency,
   *       never a duplicate.
   *   <li>A {@code 5xx}/{@code 429} response is a transient, endpoint-wide condition — the request
   *       is fully resolved (the endpoint answered), so {@link FailureKind#TRANSPORT}. So is {@link
   *       #isFleetWideStatus a credential rejection} ({@code 401}/{@code 407}), which is a property
   *       of the relay rather than of the message. Any other non-2xx status — including {@code
   *       403}, see {@link #isFleetWideStatus} for that decision — is a per-entry rejection the
   *       endpoint attributes to this message: {@link FailureKind#ENTRY} (counts toward terminal
   *       {@code FAILED}).
   * </ul>
   *
   * <p>A bare {@code IOException} is <em>not</em> a phase marker, only the absence of one, so it
   * never short-circuits the cause walk: a definitive marker anywhere in the chain (a wrapped
   * {@code ConnectException}, an {@code HttpDeliveryException} status) wins, and the in-flight
   * fallback applies only when the whole chain carries no phase information at all.
   */
  @Override
  public FailureKind classifyFailure(Exception failure) {
    // A bare IOException seen so far — the "unclassifiable phase" fallback, applied only once the
    // whole chain has been walked without finding a definitive marker.
    boolean unclassifiedIoFailure = false;
    // Bounded walk (depth 50) so a self-referential/cyclic cause chain
    // terminates instead of spinning forever, matching PostgresEventStore.hasCryptoCause.
    Throwable c = failure;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof HttpDeliveryException h) {
        return (h.statusCode() >= 500 || h.statusCode() == 429 || isFleetWideStatus(h.statusCode()))
            ? FailureKind.TRANSPORT
            : FailureKind.ENTRY;
      }
      if (c instanceof java.net.http.HttpConnectTimeoutException) {
        // Minor-1: a CONNECT timeout fired before the request was handed off — the connection was
        // never established, so nothing is in flight (identical to "connection refused"). Re-arm
        // and retry with backoff (TRANSPORT); do NOT hold the claim for a full lease. Must be
        // tested before HttpTimeoutException, its superclass.
        return FailureKind.TRANSPORT;
      }
      if (c instanceof java.net.http.HttpTimeoutException) {
        // A request/response timeout — the request WAS sent, so the server may
        // still have received and processed it. Do not release the claim; only lease-reclaim may
        // redeliver.
        return FailureKind.IN_FLIGHT;
      }
      if (isPreHandoff(c)) {
        // Subtypes of IOException (except UnresolvedAddressException) — must be tested BEFORE the
        // catch-all below, which would otherwise shadow them.
        return FailureKind.TRANSPORT;
      }
      if (c instanceof java.io.IOException) {
        // Any I/O failure that is not provably pre-hand-off may have
        // hit AFTER the request was fully written — connection reset / EOF while awaiting the
        // response, the receiver mid-handler. Treat it like the response-timeout case: hold the
        // claim. Recorded rather than returned, so a definitive marker further down the chain
        // (e.g. IOException("wrapped", new ConnectException(...))) still decides the phase.
        unclassifiedIoFailure = true;
      }
    }
    return unclassifiedIoFailure ? FailureKind.IN_FLIGHT : FailureKind.ENTRY;
  }

  /**
   * Whether {@code status} is a rejection of the <b>relay's credential</b> rather than of the
   * message — the HTTP member of the same fleet-wide-versus-message taxonomy {@code
   * KafkaOutboxPublisher.isFleetWide} applies to broker errors.
   *
   * <p><b>Why it is not just "a 4xx counts".</b> Every entry this publisher relays targets the SAME
   * {@code endpoint} (fixed at construction) under the SAME credential; only the body and the two
   * {@code X-Outbox-*} headers differ. A status decided <em>before</em> those can matter is
   * therefore identical for the entire backlog. With the shipped ladder (10 attempts, 60s cap) a
   * ~5-minute token rotation or an expired mTLS client certificate used to terminal-{@code FAILED}
   * every entry it touched: permanent silent loss, plus the silent same-aggregate reorder that
   * follows once a {@code FAILED} head stops gating its higher-seq successors.
   *
   * <ul>
   *   <li><b>401 Unauthorized</b> — by definition an authentication challenge (the response must
   *       carry {@code WWW-Authenticate}); it is decided on the {@code Authorization} credential,
   *       which belongs to the relay, and cannot depend on the body.
   *   <li><b>407 Proxy Authentication Required</b> — the same, one hop earlier.
   * </ul>
   *
   * <p>Both are fully <em>resolved</em> exchanges — the endpoint answered, and answering "you are
   * not authenticated" means it did not act on the request — so re-arming cannot duplicate, exactly
   * as for the {@code 5xx}/{@code 429} statuses already in this bin.
   *
   * <p><b>403 Forbidden is deliberately NOT a member.</b> RFC 9110 lets a 403 depend on the
   * request's <em>content</em> (a tenant id or a resource reference in the body), so unlike 401 it
   * is not provably message-independent — and this bin requires proof, not likelihood, which is the
   * same standard that keeps a generic {@code IllegalStateException} out of Kafka's fleet set. The
   * ambiguity is broken by which mistake is recoverable: a wrong {@code ENTRY} leaves terminal
   * {@code FAILED} rows an operator resets with {@code OutboxFailedReplayer} (retained 30 days by
   * default), whereas a wrong {@code TRANSPORT} on a genuinely per-entry 403 never terminalizes,
   * never dead-letters, and blocks every later entry for the same aggregate behind it — an
   * unbounded stall with no operator handle. If your receiver signals a FLEET condition with 403 (a
   * revoked scope, an IP allowlist), either have it answer 401/429/503 or expect to replay the
   * FAILED backlog after fixing it.
   *
   * <p>The residual risk of this bin is the same one {@code isFleetWide} accepts: a credential
   * nobody restores stalls the relay forever, loudly (the poller's aggregated transport WARN fires
   * every cycle). A loud stall is recoverable; the silent loss plus ordering break it replaces was
   * not.
   */
  private static boolean isFleetWideStatus(int status) {
    return status == 401 || status == 407;
  }

  /**
   * Whether {@code c} can <b>only</b> be raised before the first byte of the request left this
   * process — the pre-hand-off set of {@link #classifyFailure}. Membership requires that the JDK
   * cannot raise the type from an established connection carrying a written request; everything
   * else falls through to the conservative {@link FailureKind#IN_FLIGHT} bin.
   *
   * <ul>
   *   <li>{@link java.net.ConnectException} — the JDK HTTP client's canonical connect-phase
   *       failure. {@code jdk.internal.net.http.common.Utils.toConnectException} wraps
   *       <em>every</em> throwable raised while establishing the connection (including a
   *       connect-time {@code NoRouteToHostException} or {@code UnresolvedAddressException}) into
   *       this type, so it also covers the connect-phase flavour of exceptions whose post-connect
   *       flavour must stay in-flight.
   *       <p><b>Caveat — {@code -Djdk.httpclient.enableAllMethodRetry=true} breaks this
   *       membership.</b> By default the JDK client only auto-retries <em>idempotent</em> methods,
   *       so a POST is never re-sent and any {@code ConnectException} the caller observes belongs
   *       to the single, never-established attempt. With that flag set, {@code
   *       MultiExchange.canRetryRequest} returns true for <em>every</em> method (verified against
   *       the JDK 25 sources): a POST fully written on a pooled connection that is then abruptly
   *       closed raises {@code ConnectionExpiredException}, which {@code retryOnFailure} accepts,
   *       and the client transparently re-drives the exchange on a NEW connection — so failing to
   *       establish <em>that</em> connection surfaces a {@code ConnectException} for an exchange
   *       whose first attempt was already handed off. This publisher would then classify TRANSPORT,
   *       re-arm within ~1s and duplicate. The flag is narrow and opt-in, and setting it already
   *       makes the JDK itself re-POST (it opts the deployment into at-least-once below
   *       StreamRune), so the classification is deliberately not changed for it — but do not set it
   *       without receivers that are idempotent on {@code X-Outbox-Entry-Id}.
   *   <li>{@link java.net.UnknownHostException} — name resolution, strictly before connect.
   *   <li>{@link java.net.BindException} — binding the local socket, strictly before connect.
   *   <li>{@link java.nio.channels.UnresolvedAddressException} — not an {@code IOException} at all
   *       (an {@code IllegalArgumentException}); an address that never resolved was never
   *       connected. Listed explicitly so it lands in TRANSPORT rather than falling through to the
   *       counting {@link FailureKind#ENTRY} default.
   *   <li>{@link javax.net.ssl.SSLHandshakeException} — the TLS handshake completes before the
   *       {@code SSLEngine} emits any application-data record, so a handshake failure (expired or
   *       untrusted certificate, no common cipher suite, hostname mismatch) means no request byte
   *       was transmitted even if the request was already queued for encryption.
   *       <p><b>Known counter-examples, deliberately accepted.</b> A handshake can also run on an
   *       ALREADY-ESTABLISHED connection that may already carry a written request: a TLS 1.2
   *       <em>renegotiation</em> requested by the server (a {@code HelloRequest} mid-stream), and a
   *       TLS 1.3 <em>post-handshake</em> re-authentication. Either failing raises {@code
   *       SSLHandshakeException} post-hand-off, which this set then mis-bins as TRANSPORT and
   *       re-arms — a possible duplicate. The trade is intentional and was chosen on frequency and
   *       blast radius: the JDK HTTP client never initiates renegotiation, both flavours are rare
   *       and require a server that asks for one, whereas the common case — an expired or untrusted
   *       certificate — is fleet-wide, and binning it in-flight would park EVERY entry for a full
   *       claim lease per attempt for the whole outage. Receivers must be idempotent on {@code
   *       X-Outbox-Entry-Id} regardless. A non-handshake {@link javax.net.ssl.SSLException} (a
   *       reset or bad record mid-stream) is <em>not</em> in the set: it can fire after the request
   *       was written.
   * </ul>
   *
   * <p>Deliberately excluded: {@link java.net.NoRouteToHostException} (a pending ICMP error can
   * surface on a read of an <em>established</em> connection — the connect-phase flavour arrives
   * wrapped as {@code ConnectException} anyway) and {@link java.net.PortUnreachableException} (a
   * datagram-socket condition; were the client ever to run over QUIC, a UDP ICMP error could arrive
   * at any phase). Both stay in the conservative in-flight bin.
   */
  private static boolean isPreHandoff(Throwable c) {
    return c instanceof java.net.ConnectException
        || c instanceof java.net.UnknownHostException
        || c instanceof java.net.BindException
        || c instanceof java.nio.channels.UnresolvedAddressException
        || c instanceof javax.net.ssl.SSLHandshakeException;
  }

  /**
   * In-flight horizon = the {@link #REQUEST_TIMEOUT request timeout}. {@link #publish} bounds each
   * exchange with this timeout, so after a non-connect {@code HttpTimeoutException} fires the
   * client abandons the request and cannot observe or retry it until lease-reclaim; the request
   * timeout is the client-side envelope on how long the attempt may still be resolving. The same
   * envelope bounds the other {@link FailureKind#IN_FLIGHT} flavour: a post-hand-off connection
   * reset/EOF ends the client's view of the exchange no later than this timeout would have. Neither
   * flavour bounds how long the <em>receiver</em> keeps executing a request it already accepted —
   * no client-side value can — so receivers must stay idempotent on {@code X-Outbox-Entry-Id}. The
   * claim lease must strictly exceed this.
   */
  @Override
  public Duration inFlightHorizon() {
    return REQUEST_TIMEOUT;
  }

  /**
   * A non-2xx HTTP response. Carries the status code so {@link #classifyFailure} can split
   * retryable {@code 5xx}/{@code 429} (transport) from a permanent {@code 4xx} rejection (entry).
   */
  static final class HttpDeliveryException extends RuntimeException {
    private final int statusCode;

    HttpDeliveryException(int statusCode, String message) {
      super(message);
      this.statusCode = statusCode;
    }

    int statusCode() {
      return statusCode;
    }
  }
}
