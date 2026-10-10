package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The request-supplied stream id must not be able to forge log lines.
 *
 * <p>{@code StreamId} validates non-blank only, and the SSE controllers hand it straight through
 * percent-decoded from the request path, so it accepts CR/LF and any other control character. This
 * publisher interpolates it into four WARN messages, into {@link
 * SseEventPublisher.SlowConsumerException}'s message, and — the sink argument-level sanitization
 * can never reach — into the delivery worker's <b>thread name</b>, which every conventional layout
 * renders via {@code %thread} on EVERY line that thread emits.
 *
 * <p>The two sinks asserted here are the ones with a directly observable value; the WARN sites use
 * the same computed log-safe form, so they cannot diverge from what these tests pin.
 */
class SseEventPublisherLogForgingTest {

  /** A stream id shaped like a genuine log line, long enough to also exercise the length bound. */
  private static final String FORGED =
      "cart-1\r\n2026-08-10 00:00:00.000 ERROR 1 --- [main] o.s.Boot : authorization bypassed for"
          + " every tenant, no action required";

  private static final AggregateType CART = AggregateType.of("cart");

  record Added(String sku) implements DomainEvent {}

  private static EventEnvelope envelope(StreamId stream, long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        stream,
        new Version(offset),
        new EventType("Added"),
        new Added("X"),
        new EventMetadata(
            EventId.of("evt-" + offset),
            CommandId.of("cmd-" + offset),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  private static void assertNoForgedLine(String rendered, String what) {
    assertNotNull(rendered, what);
    assertFalse(
        rendered.chars().anyMatch(Character::isISOControl),
        what + " must carry no control character (a CR/LF forges a whole log line): " + rendered);
    assertFalse(
        rendered.contains("authorization bypassed"),
        what + " must be length-bounded so an attacker cannot amplify log volume: " + rendered);
  }

  @Test
  void deliveryWorkerThreadNameCannotCarryAForgedLogLine() throws Exception {
    var publisher = new SseEventPublisher();
    var stream = StreamId.of(CART, new AggregateId(FORGED));
    var workerThreadName = new AtomicReference<String>();
    var delivered = new CountDownLatch(1);

    publisher.subscribe(
        stream,
        envelope -> {
          workerThreadName.set(Thread.currentThread().getName());
          delivered.countDown();
        });
    publisher.publish(envelope(stream, 1L));

    assertTrue(delivered.await(5, TimeUnit.SECONDS), "the delivery worker must have run");
    assertNoForgedLine(workerThreadName.get(), "the SSE delivery worker's thread name");
    assertTrue(
        workerThreadName.get().startsWith("streamrune-sse-"),
        "the thread name must stay recognizable: " + workerThreadName.get());
    publisher.close();
  }

  @Test
  void slowConsumerExceptionMessageCannotCarryAForgedLogLine() throws Exception {
    // Capacity 1 + a subscriber parked inside send() fills the queue on the third publish, which
    // is the slow-consumer eviction path: it builds a SlowConsumerException whose message embeds
    // the stream id and hands it to the transport's disconnect hook (which logs it).
    var publisher = new SseEventPublisher(1);
    var stream = StreamId.of(CART, new AggregateId(FORGED));
    var releaseSend = new CountDownLatch(1);
    var insideSend = new CountDownLatch(1);
    var cause = new AtomicReference<Throwable>();
    var evicted = new CountDownLatch(1);

    publisher.subscribe(
        stream,
        envelope -> {
          insideSend.countDown();
          try {
            releaseSend.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
          }
        },
        throwable -> {
          cause.set(throwable);
          evicted.countDown();
        });

    publisher.publish(envelope(stream, 1L));
    assertTrue(insideSend.await(5, TimeUnit.SECONDS), "the worker must be parked inside send()");
    publisher.publish(envelope(stream, 2L));
    publisher.publish(envelope(stream, 3L));

    assertTrue(evicted.await(5, TimeUnit.SECONDS), "the slow consumer must have been evicted");
    releaseSend.countDown();

    var slowConsumer =
        org.junit.jupiter.api.Assertions.assertInstanceOf(
            SseEventPublisher.SlowConsumerException.class, cause.get());
    assertNoForgedLine(slowConsumer.getMessage(), "the SlowConsumerException message");
    assertEquals(
        FORGED,
        slowConsumer.streamId().aggregateId().value(),
        "only the human-readable MESSAGE is sanitized — the carried StreamId stays the exact value"
            + " the caller supplied, because it is data, not a log line");
    publisher.close();
  }
}
