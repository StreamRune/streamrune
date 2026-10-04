package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher.FailureKind;

class HttpOutboxPublisherTest {

  HttpServer server;
  List<String> receivedBodies;
  List<String> receivedEntryIds;
  List<String> receivedPayloadTypes;
  List<String> receivedContentTypes;
  int responseStatus;

  @Test
  void inFlightHorizon_isTheRequestTimeout() {
    // The horizon is the per-request timeout (10s) — the client-side envelope on
    // how
    // long an exchange may still be resolving after publish() throws a non-connect timeout.
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost:1/webhook"));
    assertEquals(Duration.ofSeconds(10), publisher.inFlightHorizon());
  }

  @BeforeEach
  void startServer() throws Exception {
    receivedBodies = new ArrayList<>();
    receivedEntryIds = new ArrayList<>();
    receivedPayloadTypes = new ArrayList<>();
    receivedContentTypes = new ArrayList<>();
    responseStatus = 200;

    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/webhook",
        exchange -> {
          receivedBodies.add(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          receivedEntryIds.add(exchange.getRequestHeaders().getFirst("X-Outbox-Entry-Id"));
          receivedPayloadTypes.add(exchange.getRequestHeaders().getFirst("X-Outbox-Payload-Type"));
          receivedContentTypes.add(exchange.getRequestHeaders().getFirst("Content-Type"));
          exchange.sendResponseHeaders(responseStatus, -1);
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  URI webhookUri() {
    return URI.create("http://localhost:" + server.getAddress().getPort() + "/webhook");
  }

  @Test
  void delivers_entry_payload_as_request_body() throws Exception {
    var entry =
        OutboxEntry.pending(OutboxEntryId.of("test-id"), "{\"order\":\"1\"}", "OrderCreated");
    HttpOutboxPublisher.of(webhookUri()).publish(entry);

    assertEquals(1, receivedBodies.size());
    assertEquals("{\"order\":\"1\"}", receivedBodies.get(0));
  }

  @Test
  void sets_entry_id_header() throws Exception {
    var entry = OutboxEntry.pending(OutboxEntryId.of("entry-42"), "{}", "Event");
    HttpOutboxPublisher.of(webhookUri()).publish(entry);

    assertEquals("entry-42", receivedEntryIds.get(0));
  }

  @Test
  void sets_payload_type_header() throws Exception {
    var entry = OutboxEntry.pending(OutboxEntryId.of("id-1"), "{}", "OrderShipped");
    HttpOutboxPublisher.of(webhookUri()).publish(entry);

    assertEquals("OrderShipped", receivedPayloadTypes.get(0));
  }

  @Test
  void sets_content_type_header() throws Exception {
    var entry = OutboxEntry.pending(OutboxEntryId.of("id-ct"), "{}", "Event");
    HttpOutboxPublisher.of(webhookUri()).publish(entry);

    assertEquals("application/json", receivedContentTypes.get(0));
  }

  @Test
  void throws_on_non_2xx_response() {
    responseStatus = 500;
    var entry = OutboxEntry.pending(OutboxEntryId.of("fail-id"), "{}", "Event");
    assertThrows(RuntimeException.class, () -> HttpOutboxPublisher.of(webhookUri()).publish(entry));
  }

  @Test
  void of_rejects_null_endpoint() {
    assertThrows(NullPointerException.class, () -> HttpOutboxPublisher.of(null));
  }

  @Test
  void classifyFailure_5xx_and_429_areTransport() {
    // A 5xx server error and a 429 rate-limit are transient/broker-wide — non-counting,
    // never terminal-FAIL.
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new HttpOutboxPublisher.HttpDeliveryException(500, "server error")));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new HttpOutboxPublisher.HttpDeliveryException(429, "too many requests")));
  }

  @Test
  void classifyFailure_preHandoffIoExceptions_areTransport() {
    // Only IOExceptions that can occur exclusively BEFORE the first
    // request byte leaves the process re-arm the claim (TRANSPORT). Each of these is raised by the
    // DNS/bind/connect/TLS-handshake phase, all of which strictly precede writing the request.
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(new java.net.ConnectException("connection refused")),
        "connect refused — never handed off");
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(new java.net.UnknownHostException("no-such-host")),
        "DNS failure — never handed off");
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(new java.net.BindException("Address already in use")),
        "local bind failure — strictly before connect");
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(new java.nio.channels.UnresolvedAddressException()),
        "unresolved address — no connection was ever made (not an IOException)");
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(new javax.net.ssl.SSLHandshakeException("bad certificate")),
        "TLS handshake fails before any application-data record is emitted");
    // Also when wrapped deeper in a cause chain.
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new java.io.IOException("wrapped", new java.net.ConnectException("refused"))));
  }

  @Test
  void classifyFailure_postHandoffIoExceptions_areInFlight() {
    // A reset/EOF while awaiting the response means the
    // request was FULLY SENT and the receiver may be executing it. Releasing the claim (TRANSPORT,
    // ~1s re-arm) lets a relay re-POST into that window — duplicate execution plus a same-aggregate
    // reorder. These must hold the claim until lease-reclaim, exactly like a response timeout.
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(new java.net.SocketException("Connection reset")),
        "reset while awaiting the response — receiver may still be processing");
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(new java.io.EOFException("EOF reached while reading")),
        "peer closed mid-response — the request was delivered");
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(
            new java.io.IOException("HTTP/1.1 header parser received no bytes")),
        "unclassifiable IOException — conservative side is to hold the claim");
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(
            new javax.net.ssl.SSLException("Connection reset", new java.io.IOException("reset"))),
        "a non-handshake SSLException can fire after the request was written");
  }

  @Test
  void classifyFailure_serverDropsConnectionAfterReceivingRequest_isInFlight() throws Exception {
    // End-to-end: the receiver reads the WHOLE request (proof of
    // hand-off) and then drops the connection without responding — an LB idle-timeout / NAT reset.
    // HttpClient.send throws a plain IOException, NOT an HttpTimeoutException. The entry must stay
    // claimed (IN_FLIGHT), never re-armed within ~1s while the receiver may still be running it.
    var dropServer = HttpServer.create(new InetSocketAddress(0), 0);
    var receivedBody = new java.util.concurrent.atomic.AtomicReference<String>();
    dropServer.createContext(
        "/drop",
        exchange -> {
          receivedBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          // No sendResponseHeaders: close the exchange (and its connection) mid-flight.
          exchange.close();
        });
    dropServer.start();
    try {
      var publisher =
          HttpOutboxPublisher.of(
              URI.create("http://localhost:" + dropServer.getAddress().getPort() + "/drop"));
      var entry = OutboxEntry.pending(OutboxEntryId.of("drop-1"), "{\"a\":1}", "Event");

      Exception thrown = assertThrows(Exception.class, () -> publisher.publish(entry));

      assertEquals("{\"a\":1}", receivedBody.get(), "the receiver got the complete request body");
      assertFalse(
          thrown instanceof java.net.http.HttpTimeoutException,
          "this is the plain-IOException flavour, not the already-handled timeout: " + thrown);
      assertEquals(
          FailureKind.IN_FLIGHT,
          publisher.classifyFailure(thrown),
          "post-hand-off drop must hold the claim, not re-arm it: " + thrown);
    } finally {
      dropServer.stop(0);
    }
  }

  @Test
  void classifyFailure_4xx_isEntry() {
    // A 4xx client error is a permanent per-entry rejection that still terminal-FAILs at
    // maxAttempts.
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.ENTRY,
        publisher.classifyFailure(
            new HttpOutboxPublisher.HttpDeliveryException(400, "bad request")));
    assertEquals(
        FailureKind.ENTRY,
        publisher.classifyFailure(new HttpOutboxPublisher.HttpDeliveryException(404, "not found")));
    // A bare IOException is the absence of phase information, not a phase marker, so it must not
    // short-circuit the walk past a definitive status deeper in the chain.
    assertEquals(
        FailureKind.ENTRY,
        publisher.classifyFailure(
            new java.io.IOException(
                "wrapped", new HttpOutboxPublisher.HttpDeliveryException(400, "bad request"))));
  }

  // The same fleet-wide-versus-message shape as the Kafka authorization case: every entry this
  // publisher relays targets the SAME endpoint under the SAME
  // credential; only the body and the two X-Outbox-* headers differ. A rejection that cannot
  // depend on those is a property of the RELAY, not of the message, so counting it toward FAILED
  // terminal-fails the whole backlog during a credential rotation — permanent silent loss, plus
  // the silent same-aggregate reorder that follows once a FAILED head stops gating its successors.

  @Test
  void classifyFailure_401_isTransport_notEntry() {
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new HttpOutboxPublisher.HttpDeliveryException(401, "unauthorized")),
        "a rotated bearer token or an expired mTLS client cert 401s the ENTIRE backlog — the"
            + " credential is a property of the relay, and 401 is decided before the body is even"
            + " considered");
    // The whole cause chain is walked, exactly as for the 5xx case.
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new java.io.IOException(
                "wrapped", new HttpOutboxPublisher.HttpDeliveryException(401, "unauthorized"))));
  }

  @Test
  void classifyFailure_407_isTransport_notEntry() {
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new HttpOutboxPublisher.HttpDeliveryException(407, "proxy authentication required")),
        "the proxy credential is fleet-wide for exactly the same reason as the origin one");
  }

  @Test
  void classifyFailure_403_staysEntry_deliberately() {
    // The decided half of the ambiguity. RFC 9110 lets a 403 depend on the request's CONTENT (a
    // tenant id or resource reference in the body), so — unlike 401, which is decided purely on
    // the Authorization credential and must carry a WWW-Authenticate challenge — it is NOT
    // provably message-independent. The fleet bin requires proof, not likelihood: the same
    // standard that keeps a generic IllegalStateException out of Kafka's isFleetWide set.
    //
    // The tie is broken by which mistake is recoverable. A wrong ENTRY leaves terminal FAILED rows
    // an operator resets with OutboxFailedReplayer (retained 30d by default). A wrong TRANSPORT on
    // a genuinely per-entry 403 never terminalizes, never dead-letters, and blocks every later
    // entry for the same aggregate behind it — an unbounded stall with no operator handle.
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.ENTRY,
        publisher.classifyFailure(new HttpOutboxPublisher.HttpDeliveryException(403, "forbidden")));
  }

  @Test
  void classifyFailure_connectTimeout_isTransport_notInFlight() {
    // Minor-1: a CONNECT timeout means the request was never handed off to the endpoint — nothing
    // is in flight, exactly like a "connection refused". It must re-arm/retry with backoff
    // (TRANSPORT), NOT wait out a full claim lease (IN_FLIGHT). HttpConnectTimeoutException is a
    // subclass of HttpTimeoutException, so it must be tested BEFORE its superclass.
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new java.net.http.HttpConnectTimeoutException("connect timed out")));
    // Also when wrapped deeper in a cause chain.
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new RuntimeException(
                "wrapped", new java.net.http.HttpConnectTimeoutException("connect timed out"))));
  }

  @Test
  void classifyFailure_responseTimeout_isInFlight() {
    // A request/response timeout (plain HttpTimeoutException, not a connect timeout) means the
    // request WAS sent — the endpoint may still process it — so it stays IN_FLIGHT.
    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(new java.net.http.HttpTimeoutException("request timed out")));
  }

  // classifyFailure's getCause() walk had no cycle guard at all — a
  // self-referential/cyclic cause chain would spin forever.

  /** A throwable whose getCause() returns a settable field, so two of them can form a cycle. */
  static final class CyclicThrowable extends RuntimeException {
    private transient Throwable next;

    void setNext(Throwable next) {
      this.next = next;
    }

    @Override
    public synchronized Throwable getCause() {
      return next;
    }
  }

  @Test
  void classifyFailure_cyclicChainWithoutMatch_terminatesAndReturnsEntry() {
    var a = new CyclicThrowable();
    var b = new CyclicThrowable();
    a.setNext(b);
    b.setNext(a); // a -> b -> a -> ... an unbounded getCause() walk would never terminate

    var publisher = HttpOutboxPublisher.of(URI.create("http://localhost/webhook"));
    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () ->
            assertEquals(
                FailureKind.ENTRY,
                publisher.classifyFailure(a),
                "no HttpDeliveryException/IOException anywhere in the cycle"));
  }
}
