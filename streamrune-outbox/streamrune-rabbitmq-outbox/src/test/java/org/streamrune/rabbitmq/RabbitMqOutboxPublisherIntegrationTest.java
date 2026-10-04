package org.streamrune.rabbitmq;

import static org.junit.jupiter.api.Assertions.*;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher.FailureKind;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RabbitMqOutboxPublisherIntegrationTest {

  private static final String EXCHANGE = "outbox-test";
  private static final String QUEUE = "outbox-test-queue";
  private static final String ROUTING_KEY = "test";

  @Container
  static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4-management-alpine");

  static Connection connection;
  static Channel publishChannel;
  static Channel consumeChannel;

  @BeforeAll
  static void init() throws Exception {
    var factory = new ConnectionFactory();
    factory.setHost(RABBIT.getHost());
    factory.setPort(RABBIT.getAmqpPort());
    factory.setUsername(RABBIT.getAdminUsername());
    factory.setPassword(RABBIT.getAdminPassword());
    connection = factory.newConnection();

    // No explicit confirmSelect: RabbitMqOutboxPublisher.Builder#build enables confirm mode.
    publishChannel = connection.createChannel();
    consumeChannel = connection.createChannel();

    consumeChannel.exchangeDeclare(EXCHANGE, "direct", true);
    consumeChannel.queueDeclare(QUEUE, true, false, false, null);
    consumeChannel.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);
  }

  @BeforeEach
  void purgeQueues() throws Exception {
    consumeChannel.queuePurge(QUEUE);
  }

  @AfterAll
  static void cleanup() throws Exception {
    if (publishChannel != null) publishChannel.close();
    if (consumeChannel != null) consumeChannel.close();
    if (connection != null) connection.close();
  }

  @Test
  void roundtrip_publish_and_consume() throws Exception {
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey(ROUTING_KEY)
            .build();
    var entry =
        OutboxEntry.pending(OutboxEntryId.of("rt-1"), "{\"order\":\"abc\"}", "OrderCreated");

    publisher.publish(entry);

    GetResponse response = consumeChannel.basicGet(QUEUE, true);
    assertNotNull(response);
    assertEquals("{\"order\":\"abc\"}", new String(response.getBody(), StandardCharsets.UTF_8));
  }

  @Test
  void amqp_properties_set() throws Exception {
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey(ROUTING_KEY)
            .build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("props-1"), "{}", "OrderShipped");

    publisher.publish(entry);

    GetResponse response = consumeChannel.basicGet(QUEUE, true);
    assertNotNull(response);
    var props = response.getProps();
    assertEquals("application/json", props.getContentType());
    assertEquals(2, props.getDeliveryMode());
    assertEquals("props-1", props.getMessageId());
    assertEquals("OrderShipped", props.getType());
  }

  @Test
  void custom_headers_propagated() throws Exception {
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey(ROUTING_KEY)
            .build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("hdr-1"), "{}", "OrderCreated");

    publisher.publish(entry);

    GetResponse response = consumeChannel.basicGet(QUEUE, true);
    assertNotNull(response);
    var headers = response.getProps().getHeaders();
    assertNotNull(headers);
    assertEquals("hdr-1", headers.get("X-Outbox-Entry-Id").toString());
    assertEquals("OrderCreated", headers.get("X-Outbox-Payload-Type").toString());
  }

  @Test
  void routing_key_routes_correctly() throws Exception {
    // Create a second queue bound with different routing key
    String otherQueue = "outbox-other-queue";
    consumeChannel.queueDeclare(otherQueue, true, false, false, null);
    consumeChannel.queueBind(otherQueue, EXCHANGE, "other");

    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey("other")
            .build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("rk-1"), "{}", "Event");

    publisher.publish(entry);

    // Message should be in otherQueue, not QUEUE
    try {
      GetResponse wrongQueue = consumeChannel.basicGet(QUEUE, true);
      assertNull(wrongQueue, "Message should not be in default queue");
      GetResponse correctQueue = consumeChannel.basicGet(otherQueue, true);
      assertNotNull(correctQueue, "Message should be in other queue");
    } finally {
      consumeChannel.queueDelete(otherQueue);
    }
  }

  @Test
  void publishBatch_to_unbound_routing_key_is_failed_not_confirmed() throws Exception {
    // A producer that starts before the consumer declared its
    // queue/binding (normal deploy ordering), or a removed/mistyped binding, publishes to a routing
    // key with NO bound queue. In confirm mode the broker still ACKs an unroutable message, so
    // without mandatory+return handling it was marked DELIVERED and silently discarded — a
    // silent at-least-once violation. With mandatory=true + a return listener the broker RETURNS
    // the message and it must land in failed() (retried), never confirmed().
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey("no-queue-bound-to-this-key")
            .confirmTimeout(Duration.ofSeconds(5))
            .build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("unroutable-batch-1"), "{}", "Event");

    var result = publisher.publishBatch(List.of(entry));

    assertTrue(
        result.confirmed().isEmpty(),
        "an unroutable message must NOT be confirmed — it was never enqueued: "
            + result.confirmed());
    assertTrue(
        result.failed().containsKey(OutboxEntryId.of("unroutable-batch-1")),
        "an unroutable message must be failed so the poller retries it instead of marking DELIVERED");
  }

  @Test
  void publish_to_unbound_routing_key_throws() throws Exception {
    // Single-entry path: an unroutable mandatory message is returned then acked; publish() must
    // throw (so the poller retries) rather than let waitForConfirmsOrDie's ack mark it delivered.
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey("no-queue-bound-to-this-key")
            .confirmTimeout(Duration.ofSeconds(5))
            .build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("unroutable-single-1"), "{}", "Event");

    assertThrows(
        Exception.class,
        () -> publisher.publish(entry),
        "an unroutable message must throw so the poller retries instead of marking it delivered");
  }

  @Test
  void publish_survivesRealBrokerNack_classifiesTRANSPORT_andChannelStaysUsable() throws Exception {
    // Real-broker nack check. A queue
    // declared with x-max-length=1 + x-overflow=reject-publish NACKs (publisher confirms) every
    // publish that would overflow it — and that nack is a QUEUE-STATE condition, not a property of
    // the message: the identical publish succeeds once the queue drains (step 3 below proves it).
    // That is exactly why a nack must be non-counting TRANSPORT — binning it ENTRY terminal-FAILed
    // entries the broker would happily take moments later. The channel-survival half is
    // Pre-fix, publish() blocked on waitForConfirmsOrDie,
    // which CLOSES the channel before throwing on the nack, so every later publish failed with
    // AlreadyClosedException forever.
    String nackExchange = "nack-ex";
    String nackQueue = "nack-q";
    String nackKey = "nack";
    var nackChannel = connection.createChannel();
    try {
      nackChannel.exchangeDeclare(nackExchange, "direct", true);
      nackChannel.queueDeclare(
          nackQueue, true, false, false, Map.of("x-max-length", 1, "x-overflow", "reject-publish"));
      nackChannel.queueBind(nackQueue, nackExchange, nackKey);

      var publisher =
          RabbitMqOutboxPublisher.builder()
              .channel(nackChannel)
              .exchange(nackExchange)
              .routingKey(nackKey)
              .confirmTimeout(Duration.ofSeconds(10))
              .build();

      // 1. Fills the length-1 queue — routable and acked.
      publisher.publish(OutboxEntry.pending(OutboxEntryId.of("nack-it-1"), "{}", "Event"));

      // 2. Overflows the queue — the broker NACKs it. Must throw the typed nack exception,
      // classified non-counting TRANSPORT, and must NOT kill the channel.
      Exception nackFailure =
          assertThrows(
              Exception.class,
              () ->
                  publisher.publish(
                      OutboxEntry.pending(OutboxEntryId.of("nack-it-2"), "{}", "Event")));
      assertInstanceOf(
          RabbitMqOutboxPublisher.NackedPublishException.class,
          nackFailure,
          "a real broker nack must surface as the typed NackedPublishException — got "
              + nackFailure);
      assertEquals(
          org.streamrune.core.outbox.OutboxPublisher.FailureKind.TRANSPORT,
          publisher.classifyFailure(nackFailure),
          "a broker nack is a broker-side refusal to enqueue (queue overflow here) — non-counting"
              + " TRANSPORT, matching publishBatch — got "
              + nackFailure);

      // 3. Drain the queue, then publish again ON THE SAME CHANNEL — the channel must have
      // survived the nack (pre-fix: AlreadyClosedException, backlog stalled forever).
      consumeChannel.queuePurge(nackQueue);
      assertDoesNotThrow(
          () ->
              publisher.publish(OutboxEntry.pending(OutboxEntryId.of("nack-it-3"), "{}", "Event")),
          "the channel must survive a broker nack on the single-entry publish path");
      assertTrue(nackChannel.isOpen(), "publish() must never close its channel on a nack");
    } finally {
      // The channel may have been killed by the pre-fix behavior; clean up defensively.
      try {
        consumeChannel.queueDelete(nackQueue);
        consumeChannel.exchangeDelete(nackExchange);
      } catch (Exception _) {
        // best-effort cleanup
      }
      if (nackChannel.isOpen()) {
        nackChannel.close();
      }
    }
  }

  @Test
  void publishBatch_all_entries_consumable_and_all_confirmed() throws Exception {
    int batchSize = 5;
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey(ROUTING_KEY)
            .build();

    var entries = new ArrayList<OutboxEntry>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      entries.add(
          OutboxEntry.pending(
              OutboxEntryId.of("batch-" + i), "{\"index\":" + i + "}", "BatchEvent"));
    }

    var result = publisher.publishBatch(entries);

    // All entries must be confirmed (broker acked)
    assertEquals(batchSize, result.confirmed().size(), "all entries must be broker-confirmed");
    assertTrue(result.failed().isEmpty(), "no entries must fail");
    for (int i = 0; i < batchSize; i++) {
      assertTrue(
          result.confirmed().contains(OutboxEntryId.of("batch-" + i)),
          "batch-" + i + " must be in confirmed set");
    }

    // All messages must be consumable from the queue
    for (int i = 0; i < batchSize; i++) {
      GetResponse response = consumeChannel.basicGet(QUEUE, true);
      assertNotNull(response, "message " + i + " must be in the queue");
    }
    // Queue must now be empty
    assertNull(consumeChannel.basicGet(QUEUE, true), "queue must be empty after consuming all");
  }

  /**
   * Stress: repeated 100-entry batches against a real broker with publisher confirms enabled. Every
   * run must confirm ALL entries — a single dropped confirm would stall the batch to timeout and
   * surface here as a failed entry (the old LinkedHashSet-based confirm tracking could drop
   * confirms under the connection I/O thread's concurrency).
   */
  @Test
  void publishBatch_100_entry_batches_repeatedly_all_confirmed() throws Exception {
    int batchSize = 100;
    int runs = 10;
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey(ROUTING_KEY)
            .build();

    for (int run = 0; run < runs; run++) {
      var entries = new ArrayList<OutboxEntry>(batchSize);
      for (int i = 0; i < batchSize; i++) {
        entries.add(
            OutboxEntry.pending(
                OutboxEntryId.of("stress-" + run + "-" + i), "{\"i\":" + i + "}", "StressEvent"));
      }

      var result = publisher.publishBatch(entries);

      assertTrue(
          result.failed().isEmpty(),
          "run " + run + ": no entry may fail; failed=" + result.failed().keySet());
      assertEquals(
          batchSize,
          result.confirmed().size(),
          "run " + run + ": every entry must be broker-confirmed");
      for (int i = 0; i < batchSize; i++) {
        assertTrue(
            result.confirmed().contains(OutboxEntryId.of("stress-" + run + "-" + i)),
            "run " + run + ": stress-" + run + "-" + i + " must be confirmed");
      }
      consumeChannel.queuePurge(QUEUE);
    }
  }

  @Test
  void publishBatch_messageId_and_headers_intact() throws Exception {
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(publishChannel)
            .exchange(EXCHANGE)
            .routingKey(ROUTING_KEY)
            .build();

    var entries =
        List.of(
            OutboxEntry.pending(OutboxEntryId.of("hdr-batch-1"), "{}", "TypeA"),
            OutboxEntry.pending(OutboxEntryId.of("hdr-batch-2"), "{}", "TypeB"));

    var result = publisher.publishBatch(entries);

    assertEquals(2, result.confirmed().size());
    assertTrue(result.failed().isEmpty());

    // Consume and verify each message's properties. The order may differ, so key every message by
    // its messageId and compare the whole id -> payload-type mapping with the entries at the end.
    var payloadTypeByMessageId = new HashMap<String, String>();
    for (int i = 0; i < entries.size(); i++) {
      GetResponse response = consumeChannel.basicGet(QUEUE, true);
      assertNotNull(response);
      var props = response.getProps();
      assertEquals("application/json", props.getContentType());
      assertEquals(2, props.getDeliveryMode());
      var headers = props.getHeaders();
      assertNotNull(headers);
      // The messageId and the X-Outbox-Entry-Id header must name the same entry.
      assertEquals(props.getMessageId(), headers.get("X-Outbox-Entry-Id").toString());
      payloadTypeByMessageId.put(
          props.getMessageId(), headers.get("X-Outbox-Payload-Type").toString());
    }
    assertEquals(Map.of("hdr-batch-1", "TypeA", "hdr-batch-2", "TypeB"), payloadTypeByMessageId);
  }

  /**
   * A channel wrapper that behaves like a caching channel proxy: once its current channel has
   * closed, the next call opens a fresh channel on the same connection and carries on with it —
   * without confirm mode and without the listeners registered on the old one.
   */
  private static Channel replacingChannel(AtomicReference<Channel> current) {
    return (Channel)
        Proxy.newProxyInstance(
            Channel.class.getClassLoader(),
            new Class<?>[] {Channel.class},
            (proxy, method, args) -> {
              Channel target = current.get();
              if (!method.getName().equals("isOpen") && !target.isOpen()) {
                target = connection.createChannel();
                current.set(target);
              }
              try {
                return method.invoke(target, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  @Test
  void publish_failsClosed_onAChannelReplacedByOneWithoutConfirmMode() throws Exception {
    var current = new AtomicReference<>(connection.createChannel());
    try {
      var publisher =
          RabbitMqOutboxPublisher.builder()
              .channel(replacingChannel(current))
              .exchange(EXCHANGE)
              .routingKey(ROUTING_KEY)
              .confirmTimeout(Duration.ofSeconds(2))
              .build();
      publisher.publish(OutboxEntry.pending(OutboxEntryId.of("swap-1"), "{}", "Event"));
      assertNotNull(consumeChannel.basicGet(QUEUE, true), "swap-1 is delivered and confirmed");

      // The broker closes the channel (a channel error here; a broker restart closes them all).
      Channel original = current.get();
      assertThrows(
          IOException.class,
          () -> original.exchangeDeclarePassive("no-such-exchange-" + UUID.randomUUID()));
      long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (original.isOpen() && System.nanoTime() < until) {
        Thread.onSpinWait();
      }
      assertFalse(original.isOpen(), "the broker must have closed the original channel");

      Exception failure =
          assertThrows(
              Exception.class,
              () ->
                  publisher.publish(
                      OutboxEntry.pending(OutboxEntryId.of("swap-2"), "{}", "Event")));

      GetResponse leaked = consumeChannel.basicGet(QUEUE, true);
      assertAll(
          () -> assertInstanceOf(RabbitMqOutboxPublisher.NotInConfirmModeException.class, failure),
          () ->
              assertNotSame(original, current.get(), "the wrapper must have replaced its channel"),
          () ->
              assertNull(
                  leaked,
                  "nothing may be published on a channel that cannot confirm it — it would be"
                      + " re-published after every claim lease"),
          () ->
              assertEquals(
                  FailureKind.TRANSPORT,
                  publisher.classifyFailure(failure),
                  "nothing was handed off; got " + failure));
    } finally {
      if (current.get().isOpen()) {
        current.get().close();
      }
    }
  }
}
