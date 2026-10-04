package org.streamrune.kafka;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher.FailureKind;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

@SuppressWarnings("unchecked")
class KafkaOutboxPublisherTest {

  private final KafkaProducer<String, String> producer = mock(KafkaProducer.class);

  private void stubSuccess() {
    var metadata = new RecordMetadata(new TopicPartition("t", 0), 0, 0, 0, 0, 0);
    when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(metadata));
  }

  @Test
  void inFlightHorizon_defaultsToKafkaMaxBlockPlusDeliveryTimeout_whenNoProducerConfig() {
    // The horizon is a TWO-PHASE envelope measured from the START of a
    // publish attempt — producer.send() itself blocks up to max.block.ms (60s default) waiting for
    // metadata or accumulator space, and delivery.timeout.ms (120s default) only starts counting
    // once send() RETURNS and the record is in the accumulator. With no producer config supplied
    // both Kafka defaults apply: 60s + 120s = 180s.
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(Duration.ofMillis(180_000), publisher.inFlightHorizon());
  }

  @Test
  void inFlightHorizon_reflectsConfiguredDeliveryTimeout() {
    // A user override of delivery.timeout.ms must be reflected, so the wiring guard sizes the lease
    // against the ACTUAL configured horizon. max.block.ms is unset here, so Kafka's 60s default is
    // added: 60s + 200s = 260s (well above the default lease → would fail fast).
    var publisher =
        KafkaOutboxPublisher.builder()
            .producer(producer)
            .topic("t")
            .producerConfig(Map.of(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 200_000))
            .build();
    assertEquals(Duration.ofMillis(260_000), publisher.inFlightHorizon());
  }

  @Test
  void inFlightHorizon_parsesStringDeliveryTimeout() {
    // Kafka config values may arrive as Strings (e.g. loaded from properties) — parse them too.
    // 60s default max.block.ms + 45s delivery.timeout.ms = 105s.
    var publisher =
        KafkaOutboxPublisher.builder()
            .producer(producer)
            .topic("t")
            .producerConfig(Map.of(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "45000"))
            .build();
    assertEquals(Duration.ofMillis(105_000), publisher.inFlightHorizon());
  }

  @Test
  void inFlightHorizon_reflectsConfiguredMaxBlock() {
    // Lowering max.block.ms is the operator's lever for SHRINKING the
    // horizon (and therefore the minimum safe claim lease). 5s send-block + 30s delivery = 35s.
    var publisher =
        KafkaOutboxPublisher.builder()
            .producer(producer)
            .topic("t")
            .producerConfig(
                Map.of(
                    ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000))
            .build();
    assertEquals(Duration.ofMillis(35_000), publisher.inFlightHorizon());
  }

  @Test
  void inFlightHorizon_parsesStringMaxBlock() {
    // Same String-valued config path as delivery.timeout.ms (properties-loaded config).
    var publisher =
        KafkaOutboxPublisher.builder()
            .producer(producer)
            .topic("t")
            .producerConfig(
                Map.of(
                    ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000"))
            .build();
    assertEquals(Duration.ofMillis(35_000), publisher.inFlightHorizon());
  }

  @Test
  void inFlightHorizon_addsDefaultDeliveryTimeout_whenOnlyMaxBlockConfigured() {
    // Only max.block.ms overridden: Kafka's 120s delivery.timeout.ms default still applies.
    var publisher =
        KafkaOutboxPublisher.builder()
            .producer(producer)
            .topic("t")
            .producerConfig(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1_000))
            .build();
    assertEquals(Duration.ofMillis(121_000), publisher.inFlightHorizon());
  }

  @Test
  void shippedDefaultClaimLease_strictlyExceedsShippedDefaultKafkaHorizon_withRecommendedMargin() {
    // Internal-consistency guard: the framework's OWN defaults must boot.
    // OutboxPoller.build() refuses to wire a lease that does not STRICTLY exceed the publisher's
    // in-flight horizon, and its recommended-lease hint adds a 30s clock-skew margin on top. This
    // is the only module that can see both sides of that contract (streamrune-core's OutboxStore
    // SPI and the shipped Kafka publisher), so the cross-module check lives here: raising either
    // Kafka default, or lowering the SPI default lease, must fail HERE rather than at a user's
    // boot. Shipped numbers: horizon 60s + 120s = 180s; SPI default lease 240s (4 min).
    Duration shippedHorizon =
        KafkaOutboxPublisher.builder().producer(producer).topic("t").build().inFlightHorizon();
    Duration spiDefaultLease = minimalOutboxStore().claimLease();
    Duration recommendedMargin = Duration.ofSeconds(30);

    assertTrue(
        spiDefaultLease.compareTo(shippedHorizon) > 0,
        "OutboxStore.claimLease() default ("
            + spiDefaultLease
            + ") must STRICTLY exceed the shipped Kafka in-flight horizon ("
            + shippedHorizon
            + ") or the framework's own default wiring fails OutboxPoller.build()");
    assertTrue(
        spiDefaultLease.compareTo(shippedHorizon.plus(recommendedMargin)) >= 0,
        "the default lease ("
            + spiDefaultLease
            + ") must also clear the guard's own recommended lease (horizon + 30s margin = "
            + shippedHorizon.plus(recommendedMargin)
            + ")");
  }

  /** A do-nothing {@link OutboxStore} used only to read the {@code claimLease()} SPI default. */
  private static OutboxStore minimalOutboxStore() {
    return new OutboxStore() {
      @Override
      public String claimedBy() {
        return "test";
      }

      @Override
      public void save(OutboxEntry entry) {}

      @Override
      public java.util.List<OutboxEntry> loadPending(int limit) {
        return java.util.List.of();
      }

      @Override
      public boolean markDelivered(OutboxEntryId id, String claimedBy) {
        return true;
      }

      @Override
      public boolean markFailed(OutboxEntryId id, int attempts, String error, String claimedBy) {
        return true;
      }

      @Override
      public boolean markRetry(
          OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy) {
        return true;
      }

      @Override
      public java.util.List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
        return java.util.List.of();
      }

      @Override
      public boolean resetFailedToPending(OutboxEntryId id) {
        return false;
      }

      @Override
      public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
        return java.util.Optional.empty();
      }

      @Override
      public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
        return false;
      }

      @Override
      public void delete(OutboxEntryId id) {}

      @Override
      public java.time.Duration claimLease() {
        return java.time.Duration.ofMinutes(4);
      }
    };
  }

  @Test
  void sends_entry_to_configured_topic() throws Exception {
    stubSuccess();
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("my-topic").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{\"a\":1}", "OrderCreated");

    publisher.publish(entry);

    var captor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer).send(captor.capture());
    assertEquals("my-topic", captor.getValue().topic());
  }

  @Test
  void uses_entry_id_as_key_when_the_entry_has_no_stream() throws Exception {
    stubSuccess();
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("entry-42"), "{}", "Event");

    publisher.publish(entry);

    var captor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer).send(captor.capture());
    assertEquals("entry-42", captor.getValue().key());
  }

  @Test
  void uses_the_streams_text_form_as_key_for_per_stream_ordering() throws Exception {
    stubSuccess();
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    var order7 = StreamId.of(AggregateType.of("order"), AggregateId.of("order-7"));
    // Two entries of the same stream must share a key (and therefore a partition).
    publisher.publish(OutboxEntry.pending(OutboxEntryId.of("e-1"), "{}", "OrderPlaced", order7));
    publisher.publish(OutboxEntry.pending(OutboxEntryId.of("e-2"), "{}", "OrderShipped", order7));

    var captor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer, times(2)).send(captor.capture());
    assertEquals("order:order-7", captor.getAllValues().get(0).key());
    assertEquals("order:order-7", captor.getAllValues().get(1).key());
  }

  @Test
  void two_aggregate_types_sharing_an_id_value_get_two_keys() throws Exception {
    stubSuccess();
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    publisher.publish(
        OutboxEntry.pending(
            OutboxEntryId.of("e-1"),
            "{}",
            "ProductCreated",
            StreamId.of(AggregateType.of("product"), AggregateId.of("p-1"))));
    publisher.publish(
        OutboxEntry.pending(
            OutboxEntryId.of("e-2"),
            "{}",
            "StockReserved",
            StreamId.of(AggregateType.of("inventory"), AggregateId.of("p-1"))));

    var captor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer, times(2)).send(captor.capture());
    assertEquals("product:p-1", captor.getAllValues().get(0).key());
    assertEquals("inventory:p-1", captor.getAllValues().get(1).key());
  }

  @Test
  void uses_payload_as_value() throws Exception {
    stubSuccess();
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{\"order\":\"abc\"}", "OrderCreated");

    publisher.publish(entry);

    var captor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer).send(captor.capture());
    assertEquals("{\"order\":\"abc\"}", captor.getValue().value());
  }

  @Test
  void sets_entry_id_header() throws Exception {
    stubSuccess();
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("hdr-id"), "{}", "Event");

    publisher.publish(entry);

    var captor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer).send(captor.capture());
    Headers headers = captor.getValue().headers();
    var header = headers.lastHeader("X-Outbox-Entry-Id");
    assertNotNull(header);
    assertEquals("hdr-id", new String(header.value(), StandardCharsets.UTF_8));
  }

  @Test
  void sets_payload_type_header() throws Exception {
    stubSuccess();
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "OrderShipped");

    publisher.publish(entry);

    var captor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer).send(captor.capture());
    Headers headers = captor.getValue().headers();
    var header = headers.lastHeader("X-Outbox-Payload-Type");
    assertNotNull(header);
    assertEquals("OrderShipped", new String(header.value(), StandardCharsets.UTF_8));
  }

  @Test
  void throws_on_send_failure() {
    var future = new CompletableFuture<RecordMetadata>();
    future.completeExceptionally(new RuntimeException("broker down"));
    when(producer.send(any())).thenReturn(future);

    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("fail"), "{}", "Event");

    assertThrows(Exception.class, () -> publisher.publish(entry));
  }

  @Test
  void restores_interrupt_flag_when_interrupted_during_send() {
    // Never-completing future: get() observes the pre-set interrupt and throws.
    when(producer.send(any())).thenReturn(new CompletableFuture<>());

    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("int"), "{}", "Event");

    Thread.currentThread().interrupt();
    try {
      assertThrows(InterruptedException.class, () -> publisher.publish(entry));
    } finally {
      // Thread.interrupted() both asserts the flag was restored and clears it for other tests.
      assertTrue(
          Thread.interrupted(),
          "interrupt flag must be restored so the OutboxPoller loop condition sees it");
    }
  }

  @Test
  void wraps_non_exception_cause_in_runtime_exception() {
    var future = new CompletableFuture<RecordMetadata>();
    future.completeExceptionally(new OutOfMemoryError("heap"));
    when(producer.send(any())).thenReturn(future);

    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("err"), "{}", "Event");

    var ex = assertThrows(RuntimeException.class, () -> publisher.publish(entry));
    assertInstanceOf(OutOfMemoryError.class, ex.getCause());
  }

  @Test
  void publish_times_out_when_send_does_not_complete_within_send_timeout() {
    // producer.send(record).get() must be bounded by a configurable send timeout, so a
    // degraded broker cannot block one publish for delivery.timeout.ms (120s default) and push the
    // publish cycle past the claim lease. A never-completing send must surface a TimeoutException
    // (a delivery failure the poller retries) within ~the configured timeout, not hang.
    when(producer.send(any())).thenReturn(new CompletableFuture<>()); // never completes

    var publisher =
        KafkaOutboxPublisher.builder()
            .producer(producer)
            .topic("t")
            .sendTimeout(java.time.Duration.ofMillis(150))
            .build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("slow-send"), "{}", "Event");

    long start = System.nanoTime();
    assertThrows(java.util.concurrent.TimeoutException.class, () -> publisher.publish(entry));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertTrue(
        elapsedMs < 5_000,
        "publish must abort near the 150ms send timeout, not block on the unbounded get() (was "
            + elapsedMs
            + "ms)");
  }

  @Test
  void builder_acceptsDeliveryTimeoutBelowSendTimeout_withWarn_notRefusal() {
    // delivery.timeout.ms below sendTimeout means the producer
    // expires sends before our bounded get() ever fires — surfacing Kafka's own TimeoutException
    // (IN_FLIGHT) instead of ours. The builder WARNs about the inverted tuning but must still
    // build: the config map is advisory (a caller may withhold the real producer settings), so a
    // hard refusal would punish an honest map more than a withheld one. Both branches of the check
    // are exercised here (inverted and healthy ordering).
    var inverted =
        KafkaOutboxPublisher.builder()
            .producer(producer)
            .topic("t")
            .sendTimeout(Duration.ofSeconds(10))
            .producerConfig(Map.of(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5_000))
            .build();
    assertNotNull(inverted, "the inverted tuning WARNs but still builds");

    var healthy =
        KafkaOutboxPublisher.builder()
            .producer(producer)
            .topic("t")
            .sendTimeout(Duration.ofSeconds(10))
            .producerConfig(Map.of(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000))
            .build();
    assertNotNull(healthy);
  }

  @Test
  void builder_rejects_null_send_timeout() {
    assertThrows(
        NullPointerException.class,
        () ->
            KafkaOutboxPublisher.builder().producer(producer).topic("t").sendTimeout(null).build());
  }

  @Test
  void builder_rejects_non_positive_send_timeout() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaOutboxPublisher.builder()
                .producer(producer)
                .topic("t")
                .sendTimeout(java.time.Duration.ZERO)
                .build());
  }

  @Test
  void classifyFailure_preAppendRetriableIsTransport_sendTimeoutIsInFlight() {
    // A Kafka RetriableException whose shape PROVES the broker answered
    // without appending (a leader-election error response) is a TRANSPORT outage — non-counting,
    // re-armed immediately. The bounded send get() java.util.concurrent.TimeoutException means the
    // record is still buffered in the producer and retried until delivery.timeout.ms (handed off,
    // unconfirmed) — IN_FLIGHT, so the poller must keep it claimed for lease-reclaim rather than
    // releasing it (which would duplicate + reorder).
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(new java.util.concurrent.TimeoutException("send bound elapsed")));
    // The cause chain is inspected (an unwrapped ExecutionException cause).
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new RuntimeException(
                new org.apache.kafka.common.errors.NotLeaderOrFollowerException("no leader"))));
    // NotEnoughReplicas WITHOUT "AfterAppend" is the broker refusing to append (ISR too small
    // before the write): a definitive negative verdict, nothing in flight — TRANSPORT.
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.NotEnoughReplicasException("isr too small")));
  }

  // Three of Kafka's RetriableExceptions can surface AFTER the
  // record was handed off — Kafka's own TimeoutException (delivery.timeout.ms expiry: a produce
  // request may still be in flight and the broker may still append it), NetworkException (the
  // connection dropped after the request was sent; the broker may have processed it), and
  // NotEnoughReplicasAfterAppendException (the broker APPENDED to the leader log and then failed
  // the ISR check — Kafka's own javadoc says the write may become visible). The old blanket
  // "retriable => TRANSPORT" binned all three as TRANSPORT, so markRetry RELEASED the claim while
  // the record could still land: a second relay re-published it — duplicate + same-aggregate
  // reorder, reachable simply by lowering delivery.timeout.ms below sendTimeout (a tuning the
  // PostgresOutboxStore lease javadoc recommends as a horizon lever). Per the SPI contract, a
  // phase not provable from the failure alone must classify IN_FLIGHT.

  @Test
  void classifyFailure_kafkaTimeoutException_isInFlight_notTransport() {
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.TimeoutException(
                "Expiring 1 record(s) for t-0: 5000 ms has passed since batch creation")),
        "delivery.timeout.ms expiry does NOT prove the record failed — a produce request may still"
            + " be in flight at the broker; releasing the claim risks duplicate + reorder");
    // The cause chain is inspected, exactly as for the other kinds.
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(
            new RuntimeException(
                new org.apache.kafka.common.errors.TimeoutException("batch expired"))));
  }

  @Test
  void classifyFailure_networkException_isInFlight_notTransport() {
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.NetworkException(
                "The server disconnected before a response was received")),
        "a disconnect after the produce request was sent leaves the broker free to append it —"
            + " the claim must be held for lease-reclaim");
  }

  @Test
  void classifyFailure_notEnoughReplicasAfterAppend_isInFlight_notTransport() {
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException(
                "appended to leader log but ISR shrank")),
        "the broker APPENDED the record before failing the request — Kafka documents the write may"
            + " become visible, so this is the definitional post-hand-off case");
  }

  @Test
  void classifyFailure_nonRetriableKafkaAndGeneric_areEntry() {
    // A non-retriable Kafka error (record too large, serialization) and any generic failure are
    // per-entry rejections that still terminal-FAIL once the ladder is exhausted.
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.ENTRY,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.RecordTooLargeException("too big")));
    assertEquals(FailureKind.ENTRY, publisher.classifyFailure(new RuntimeException("weird")));
  }

  // The classifier used to be a deny-list ("retriable => TRANSPORT,
  // everything else => ENTRY"). Kafka's auth/authorization errors are NOT RetriableException
  // (verified against kafka-clients 3.9.0: SaslAuthenticationException -> AuthenticationException
  // -> ApiException; TopicAuthorizationException -> AuthorizationException -> ApiException), so a
  // rotated SASL credential or a mis-applied ACL fell through to the counting ENTRY bin and
  // terminal-FAILED every entry relayed during the outage — permanent silent loss, plus the silent
  // same-aggregate reorder a FAILED head causes once it stops gating its successors. These are
  // fleet conditions, not properties of a message, and none of them can have handed the record off.

  @Test
  void classifyFailure_saslAuthFailure_isTransport_notEntry() {
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.SaslAuthenticationException(
                "Authentication failed: Invalid username or password")),
        "a rotated/expired SASL credential is fleet-wide — it must never burn the retry ladder");
    // The whole cause chain is walked, exactly as for the retriable case.
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new RuntimeException(
                new org.apache.kafka.common.errors.SaslAuthenticationException("bad creds"))));
  }

  @Test
  void classifyFailure_topicAuthorizationFailure_isTransport_notEntry() {
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.TopicAuthorizationException("t")),
        "a revoked/mis-applied topic ACL is fleet-wide — the broker rejected the produce request"
            + " without appending, so nothing is in flight and re-arming is safe");
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.ClusterAuthorizationException("no cluster acl")),
        "the sibling AuthorizationException subtypes must classify identically");
  }

  @Test
  void classifyFailure_closedProducerIllegalState_isTransport_notEntry() {
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new IllegalStateException("Cannot perform operation after producer has been closed")),
        "a closed producer rejects EVERY entry before send() enqueues anything — a lifecycle"
            + " condition of the relay, not a property of the message");
  }

  @Test
  void classifyFailure_invalidTopic_isTransport_notEntry() {
    // The topic is fixed at publisher construction, so an invalid/being-deleted topic can never be
    // a property of one message: it rejects the entire backlog identically.
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new org.apache.kafka.common.errors.InvalidTopicException("bad topic")));
  }

  @Test
  void classifyFailure_unrelatedIllegalState_staysEntry() {
    // Only the closed-producer lifecycle IllegalStateException is fleet-wide. A generic
    // IllegalStateException carries no fleet evidence and must keep the counting ENTRY default,
    // so a genuine per-entry bug cannot stall the whole backlog forever.
    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertEquals(
        FailureKind.ENTRY, publisher.classifyFailure(new IllegalStateException("something else")));
    assertEquals(FailureKind.ENTRY, publisher.classifyFailure(new IllegalStateException()));
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

    var publisher = KafkaOutboxPublisher.builder().producer(producer).topic("t").build();
    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () ->
            assertEquals(
                FailureKind.ENTRY,
                publisher.classifyFailure(a),
                "no RetriableException/TimeoutException anywhere in the cycle"));
  }

  @Test
  void builder_rejects_null_producer() {
    assertThrows(
        NullPointerException.class,
        () -> KafkaOutboxPublisher.builder().producer(null).topic("t").build());
  }

  @Test
  void builder_rejects_null_topic() {
    assertThrows(
        NullPointerException.class,
        () -> KafkaOutboxPublisher.builder().producer(producer).topic(null).build());
  }

  @Test
  void builder_rejects_blank_topic() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaOutboxPublisher.builder().producer(producer).topic("  ").build());
  }
}
