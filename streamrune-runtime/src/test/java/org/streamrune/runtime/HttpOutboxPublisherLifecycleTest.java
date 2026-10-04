package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;

/**
 * The HTTP outbox transport must be releasable, and the owned-vs-borrowed contract of its two
 * factories must be pinned.
 *
 * <p>{@link HttpOutboxPublisher#of(URI)} allocates a JDK {@link HttpClient} (selector thread +
 * per-request executor) and stores it in a private field. Without a {@code close()} the container
 * has nothing to invoke on shutdown and the application cannot reach the client either, so every
 * context cycle (Spring refresh, Quarkus live reload, a multi-context test suite) strands the
 * previous cycle's client until the JDK cleaner eventually runs — the same leak already fixed for
 * the Vault and AWS KMS clients.
 */
class HttpOutboxPublisherLifecycleTest {

  private HttpServer server;
  private AtomicInteger requestsReceived;

  @BeforeEach
  void startServer() throws Exception {
    requestsReceived = new AtomicInteger();
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/webhook",
        exchange -> {
          requestsReceived.incrementAndGet();
          exchange.getRequestBody().readAllBytes();
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private URI webhookUri() {
    return URI.create("http://localhost:" + server.getAddress().getPort() + "/webhook");
  }

  private static OutboxEntry entry(String id) {
    return OutboxEntry.pending(OutboxEntryId.of(id), "{}", "Event");
  }

  @Test
  void publisher_isAutoCloseable() {
    // Fail-first: on the audited code HttpOutboxPublisher implements only OutboxPublisher, so the
    // owner of the client it created has no way to release it.
    try (var publisher = HttpOutboxPublisher.of(URI.create("http://localhost:1/webhook"))) {
      assertInstanceOf(
          AutoCloseable.class,
          publisher,
          "HttpOutboxPublisher owns the HttpClient it created and must be closeable");
    }
  }

  @Test
  void close_releasesTheClientTheFactoryCreated() throws Exception {
    var publisher = HttpOutboxPublisher.of(webhookUri());
    publisher.publish(entry("before-close"));
    assertEquals(1, requestsReceived.get(), "the pre-close publish must reach the endpoint");

    publisher.close();

    // The owned client is now shut down: nothing more leaves this process, and no selector
    // thread/executor survives the close.
    assertThrows(
        Exception.class,
        () -> publisher.publish(entry("after-close")),
        "a publish after close() must fail — the owned client is released");
    assertEquals(1, requestsReceived.get(), "no request may leave the process after close()");
  }

  @Test
  void close_leavesACallerSuppliedClientOpen() throws Exception {
    HttpClient borrowed = HttpClient.newHttpClient();
    try {
      HttpOutboxPublisher.of(webhookUri(), borrowed).close();

      assertFalse(
          borrowed.isTerminated(), "close() must not shut down a client the caller handed in");
      // Still fully usable: a second publisher over the same borrowed client delivers.
      HttpOutboxPublisher.of(webhookUri(), borrowed).publish(entry("borrowed"));
      assertEquals(1, requestsReceived.get());
    } finally {
      borrowed.close();
    }
    assertTrue(borrowed.isTerminated(), "the caller closes the client it built");
  }

  @Test
  void close_isIdempotent() throws Exception {
    var publisher = HttpOutboxPublisher.of(webhookUri());
    publisher.publish(entry("id-1"));
    publisher.close();
    publisher.close();
    assertEquals(1, requestsReceived.get());
  }
}
