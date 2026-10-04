package org.streamrune.rabbitmq;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxPublisher.FailureKind;

class RabbitMqOutboxPublisherTest {

  private final Channel channel = mock(Channel.class);

  RabbitMqOutboxPublisherTest() throws IOException {
    // Like a real channel, the mock reports publish sequence number 0 until confirmSelect() puts
    // it into confirm mode. Tests that script sequence numbers re-stub getNextPublishSeqNo().
    var confirmMode = new AtomicBoolean();
    when(channel.confirmSelect())
        .thenAnswer(
            inv -> {
              confirmMode.set(true);
              return null;
            });
    when(channel.getNextPublishSeqNo()).thenAnswer(inv -> confirmMode.get() ? 1L : 0L);
  }

  /**
   * Scripts the publish sequence numbers 1, 2, 3, ...: each publish reads the next one right before
   * its basicPublish. The read build() makes to check confirm mode does not take a number — on a
   * real channel getNextPublishSeqNo() only reads, and basicPublish advances the number.
   */
  private void stubSeqNos(AtomicLong seqCounter) {
    when(channel.getNextPublishSeqNo())
        .thenAnswer(inv -> readByBuild() ? seqCounter.get() : seqCounter.getAndIncrement());
  }

  /** Whether the current call comes from {@link RabbitMqOutboxPublisher.Builder#build()}. */
  private static boolean readByBuild() {
    return StackWalker.getInstance()
        .walk(
            frames ->
                frames.anyMatch(
                    f ->
                        f.getClassName().equals(RabbitMqOutboxPublisher.Builder.class.getName())
                            && f.getMethodName().equals("build")));
  }

  /**
   * Publisher whose mock channel immediately acks every publish — the plain happy path for
   * single-entry {@code publish()} tests, which settle through the batch confirm listener and
   * therefore need a broker confirm to return at all.
   */
  private RabbitMqOutboxPublisher autoAckingPublisher(String exchange, String routingKey)
      throws Exception {
    var listenerRef = new AtomicReference<ConfirmListener>();
    modelJarFaithfulChannel(new AtomicBoolean(false), true, listenerRef, List.of());
    var builder = RabbitMqOutboxPublisher.builder().channel(channel).exchange(exchange);
    if (routingKey != null) {
      builder.routingKey(routingKey);
    }
    var publisher = builder.build();
    listenerRef.set(captureListener());
    return publisher;
  }

  @Test
  void inFlightHorizon_defaultsToPublishBlockBudgetPlusConfirmTimeout() throws Exception {
    // The horizon must span BOTH phases of an attempt, measured
    // from the moment it STARTS (the poller gates attempt starts against `lease - horizon`):
    //   phase 1 — basicPublish blocks on the socket write while the broker is flow-controlled
    //             (connection.blocked memory/disk alarm, or plain TCP backpressure). The AMQP
    //             client applies NO timeout there, so confirmTimeout bounds none of it.
    //   phase 2 — the published-but-unconfirmed window, bounded by confirmTimeout.
    // Reporting only phase 2 let a lease sized at 60s (> 30s, so the OutboxPoller build guard
    // passed) be reclaimed by a second relay while phase 1 was still buffered — a duplicate plus
    // a same-aggregate reorder. Same two-phase sum as Kafka's max.block.ms + delivery.timeout.ms.
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    assertEquals(Duration.ofSeconds(60), publisher.inFlightHorizon());
  }

  @Test
  void inFlightHorizon_reflectsConfiguredConfirmTimeout() throws Exception {
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(45))
            .build();
    assertEquals(Duration.ofSeconds(45).plus(Duration.ofSeconds(30)), publisher.inFlightHorizon());
  }

  @Test
  void inFlightHorizon_reflectsConfiguredPublishBlockBudget() throws Exception {
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(5))
            .publishBlockBudget(Duration.ofSeconds(12))
            .build();
    assertEquals(Duration.ofSeconds(17), publisher.inFlightHorizon());
  }

  @Test
  void publishBlockBudget_zeroIsAllowed_andCollapsesTheHorizonToTheConfirmTimeout()
      throws Exception {
    // A deployment that can PROVE the write cannot block (e.g. NIO mode with a small
    // writeEnqueuingTimeoutInMs that turns the block into a thrown exception) may opt out.
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(5))
            .publishBlockBudget(Duration.ZERO)
            .build();
    assertEquals(Duration.ofSeconds(5), publisher.inFlightHorizon());
  }

  @Test
  void publishBlockBudget_rejectsNegative() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RabbitMqOutboxPublisher.builder()
                .channel(channel)
                .exchange("ex")
                .publishBlockBudget(Duration.ofSeconds(-1)));
  }

  @Test
  void publishBlockBudget_rejectsNull() {
    assertThrows(
        NullPointerException.class,
        () ->
            RabbitMqOutboxPublisher.builder()
                .channel(channel)
                .exchange("ex")
                .publishBlockBudget(null));
  }

  @Test
  void publishes_to_configured_exchange() throws Exception {
    var publisher = autoAckingPublisher("my-exchange", null);
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{\"a\":1}", "OrderCreated");

    publisher.publish(entry);

    verify(channel)
        .basicPublish(
            eq("my-exchange"),
            eq(""),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));
  }

  @Test
  void uses_configured_routing_key() throws Exception {
    var publisher = autoAckingPublisher("ex", "orders");
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");

    publisher.publish(entry);

    verify(channel)
        .basicPublish(
            eq("ex"),
            eq("orders"),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));
  }

  @Test
  void sends_payload_as_body() throws Exception {
    var publisher = autoAckingPublisher("ex", null);
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{\"order\":\"abc\"}", "OrderCreated");

    publisher.publish(entry);

    var bodyCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), bodyCaptor.capture());
    assertEquals("{\"order\":\"abc\"}", new String(bodyCaptor.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  void sets_persistent_delivery_mode() throws Exception {
    var publisher = autoAckingPublisher("ex", null);
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");

    publisher.publish(entry);

    var propsCaptor = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
    verify(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), propsCaptor.capture(), any());
    assertEquals(2, propsCaptor.getValue().getDeliveryMode());
  }

  @Test
  void sets_content_type() throws Exception {
    var publisher = autoAckingPublisher("ex", null);
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");

    publisher.publish(entry);

    var propsCaptor = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
    verify(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), propsCaptor.capture(), any());
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  void sets_message_id_from_entry_id() throws Exception {
    var publisher = autoAckingPublisher("ex", null);
    var entry = OutboxEntry.pending(OutboxEntryId.of("msg-42"), "{}", "Event");

    publisher.publish(entry);

    var propsCaptor = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
    verify(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), propsCaptor.capture(), any());
    assertEquals("msg-42", propsCaptor.getValue().getMessageId());
  }

  @Test
  void sets_type_from_payload_type() throws Exception {
    var publisher = autoAckingPublisher("ex", null);
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "OrderShipped");

    publisher.publish(entry);

    var propsCaptor = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
    verify(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), propsCaptor.capture(), any());
    assertEquals("OrderShipped", propsCaptor.getValue().getType());
  }

  @Test
  void sets_custom_headers() throws Exception {
    var publisher = autoAckingPublisher("ex", null);
    var entry = OutboxEntry.pending(OutboxEntryId.of("hdr-1"), "{}", "OrderCreated");

    publisher.publish(entry);

    var propsCaptor = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
    verify(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), propsCaptor.capture(), any());
    var headers = propsCaptor.getValue().getHeaders();
    assertNotNull(headers);
    assertEquals("hdr-1", headers.get("X-Outbox-Entry-Id"));
    assertEquals("OrderCreated", headers.get("X-Outbox-Payload-Type"));
  }

  @Test
  void throws_on_channel_io_exception() throws Exception {
    doThrow(new IOException("connection lost"))
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("fail"), "{}", "Event");

    assertThrows(IOException.class, () -> publisher.publish(entry));
  }

  @Test
  void enables_publisher_confirms_on_build() throws Exception {
    RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();

    verify(channel).confirmSelect();
  }

  @Test
  void publish_requiresBrokerConfirm_notJustBasicPublish() throws Exception {
    // The hand-off alone must never count as delivery: with no broker confirm, publish() must
    // throw (TimeoutException -> IN_FLIGHT) rather than return, even though basicPublish
    // succeeded. Replaces the pre-fix waitForConfirmsOrDie call-order verification with the
    // behavior it stood for.
    var listenerRef = new AtomicReference<ConfirmListener>();
    modelJarFaithfulChannel(
        new AtomicBoolean(false), true, listenerRef, java.util.Arrays.asList((Boolean) null));
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(150))
            .build();
    listenerRef.set(captureListener());
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");

    assertThrows(TimeoutException.class, () -> publisher.publish(entry));

    verify(channel).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
  }

  @Test
  void restores_interrupt_flag_when_confirm_wait_is_interrupted() throws Exception {
    // Interrupted while awaiting the broker confirm: publish() must restore the interrupt flag
    // and throw the checked InterruptedException itself (SPI contract), exactly as before the
    // batch-path delegation.
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(10))
            .build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("int"), "{}", "Event");

    // Interrupt from inside the hand-off, after the publish loop's interrupt check has passed: the
    // message IS handed off, so publish() reaches the confirm wait, and latch.await aborts on the
    // already-set flag. The former "sleep 50ms on an executor, then interrupt" version could fire
    // before publish() was even entered, in which case the loop skips the hand-off entirely and the
    // test passes through a completely different branch (unattempted entry) while still asserting
    // green — the failure mode a fixed sleep always hides.
    doAnswer(
            inv -> {
              Thread.currentThread().interrupt(); // interrupts the publishing (test) thread
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    try {
      assertThrows(InterruptedException.class, () -> publisher.publish(entry));
      assertTrue(Thread.currentThread().isInterrupted());
      verify(channel).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    } finally {
      Thread.interrupted(); // clear the flag so it does not leak into other tests
    }
  }

  @Test
  void propagates_confirm_select_failure_from_build() throws Exception {
    doThrow(new IOException("confirms unavailable")).when(channel).confirmSelect();

    assertThrows(
        IOException.class,
        () -> RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build());
  }

  @Test
  void default_routing_key_is_empty() throws Exception {
    var publisher = autoAckingPublisher("ex", null);
    var entry = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");

    publisher.publish(entry);

    verify(channel).basicPublish(eq("ex"), eq(""), anyBoolean(), any(), any());
  }

  @Test
  void builder_rejects_null_channel() {
    assertThrows(
        NullPointerException.class,
        () -> RabbitMqOutboxPublisher.builder().channel(null).exchange("ex").build());
  }

  @Test
  void builder_rejects_null_exchange() {
    assertThrows(
        NullPointerException.class,
        () -> RabbitMqOutboxPublisher.builder().channel(channel).exchange(null).build());
  }

  @Test
  void builder_rejects_blank_exchange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RabbitMqOutboxPublisher.builder().channel(channel).exchange("  ").build());
  }

  @Test
  void builder_rejects_null_confirm_timeout() {
    assertThrows(
        NullPointerException.class,
        () ->
            RabbitMqOutboxPublisher.builder()
                .channel(channel)
                .exchange("ex")
                .confirmTimeout(null)
                .build());
  }

  @Test
  void builder_rejects_non_positive_confirm_timeout() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RabbitMqOutboxPublisher.builder()
                .channel(channel)
                .exchange("ex")
                .confirmTimeout(Duration.ZERO)
                .build());
  }

  @Test
  void builder_rejects_negative_confirm_timeout() {
    // Distinct branch from the ZERO case above: isZero() is false here, so the guard must fall
    // through to isNegative() to reject a genuinely negative (non-zero) duration.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RabbitMqOutboxPublisher.builder()
                .channel(channel)
                .exchange("ex")
                .confirmTimeout(Duration.ofSeconds(-5))
                .build());
  }

  // ---------------------------------------------------------------------------
  // publishBatch tests — drive the ConfirmListener via ArgumentCaptor
  // ---------------------------------------------------------------------------

  /** Builds a publisher with a mock channel whose getNextPublishSeqNo returns 1, 2, 3, ... */
  private RabbitMqOutboxPublisher buildBatchPublisher() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    return RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
  }

  /** Delivers a broker ack for {@code seq} on a joined "connection I/O" thread. */
  private static void deliverAck(ConfirmListener listener, long seq) throws InterruptedException {
    deliverConfirm(listener, seq, false, true);
  }

  /**
   * Delivers one broker confirm on a joined "connection I/O" thread — the deterministic replacement
   * for the "sleep N ms on an executor, then ack" scaffolds this class used to rely on.
   *
   * <p><b>Why the sleep was a race, not a delay.</b> {@code publishBatch} registers each entry's
   * seqNo in {@code outstanding} immediately BEFORE its own {@code basicPublish}, and {@code
   * settleConfirms} settles a confirm only by atomically claiming that registration. A confirm
   * fabricated before the registration therefore finds nothing to claim and is dropped outright:
   * the entry's latch slot never counts down, the batch stalls to its full {@code confirmTimeout},
   * and the entry is reported with a spurious {@code TimeoutException} instead of the ack/nack the
   * test scripted. Whether the executor's fixed sleep expired before or after the publishing thread
   * got that far was pure scheduling luck — under a loaded, parallel {@code build} it lost.
   * (Confirmed by delivering the nack ahead of {@code publishBatch}: {@code
   * publishBatch_timeout_sweep_skips_entry_already_nacked} then fails with "an entry nacked before
   * the timeout must keep its nack cause".) There is a symmetric second race on the short-timeout
   * tests, where an executor descheduled past the confirm timeout delivers its ack after the sweep.
   *
   * <p>Calling this from inside the {@code basicPublish} stub of the entry it settles removes both:
   * the registration provably happened (production does it before the call this stub is answering)
   * and the confirm provably lands inside the wait. Delivery still happens on a separate thread, so
   * the confirm callback runs off the publishing thread exactly as the connection's I/O thread
   * does; the join keeps the interleaving pinned.
   */
  private static void deliverConfirm(
      ConfirmListener listener, long seq, boolean multiple, boolean ack)
      throws InterruptedException {
    var confirmIo =
        new Thread(
            () -> {
              try {
                if (ack) {
                  listener.handleAck(seq, multiple);
                } else {
                  listener.handleNack(seq, multiple);
                }
              } catch (IOException e) {
                throw new AssertionError("confirm dispatch must not throw", e);
              }
            },
            "confirm-io");
    confirmIo.start();
    confirmIo.join();
  }

  /**
   * Captures the ConfirmListener that was registered on the mock channel during build(). The
   * listener is used to simulate broker acks/nacks in tests.
   */
  private ConfirmListener captureListener() {
    var captor = ArgumentCaptor.forClass(ConfirmListener.class);
    verify(channel).addConfirmListener(captor.capture());
    return captor.getValue();
  }

  @Test
  void publishBatch_all_acked_returns_all_confirmed() throws Exception {
    var publisher = buildBatchPublisher();
    var listener = captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("b1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("b2"), "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("b3"), "{}", "Event");

    // The broker acks all three in one multiple=true frame, delivered from inside the THIRD
    // basicPublish: by then seqNos 1, 2 and 3 are all registered (production registers each seq
    // before its own basicPublish), so the multiple-ack sweeps every one of them. See
    // deliverConfirm for why the former "sleep 50ms, then ack 3" scaffold was a race: a
    // multiple-ack fired before seqs 2/3 were registered settles only what it can already see, and
    // the two unseen entries stall to the 30s confirm timeout and land in failed().
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              if (publishCount.incrementAndGet() == 3) {
                deliverConfirm(listener, 3L, true, true); // acks 1, 2, 3
              }
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var result = publisher.publishBatch(List.of(e1, e2, e3));

    assertEquals(3, result.confirmed().size());
    assertTrue(result.confirmed().contains(OutboxEntryId.of("b1")));
    assertTrue(result.confirmed().contains(OutboxEntryId.of("b2")));
    assertTrue(result.confirmed().contains(OutboxEntryId.of("b3")));
    assertTrue(result.failed().isEmpty());
  }

  @Test
  void publishBatch_ack_1_and_3_nack_2_returns_correct_split() throws Exception {
    var publisher = buildBatchPublisher();
    var listener = captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("b1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("b2"), "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("b3"), "{}", "Event");

    // Each entry is settled from inside its own basicPublish — ack, nack, ack — so every confirm
    // provably lands after its seqNo was registered (see deliverConfirm).
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              long seq = publishCount.incrementAndGet();
              deliverConfirm(listener, seq, false, seq != 2); // ack b1, nack b2, ack b3
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var result = publisher.publishBatch(List.of(e1, e2, e3));

    assertEquals(2, result.confirmed().size());
    assertTrue(result.confirmed().contains(OutboxEntryId.of("b1")));
    assertTrue(result.confirmed().contains(OutboxEntryId.of("b3")));

    assertEquals(1, result.failed().size());
    assertTrue(result.failed().containsKey(OutboxEntryId.of("b2")));
    // The batch path's nack must carry the SAME typed exception and
    // non-counting TRANSPORT classification as the single-entry path — a bare RuntimeException
    // here fell through classifyFailure to the counting bin.
    assertInstanceOf(
        RabbitMqOutboxPublisher.NackedPublishException.class,
        result.failed().get(OutboxEntryId.of("b2")));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(result.failed().get(OutboxEntryId.of("b2"))));
  }

  @Test
  void publishBatch_multipleNack_sweepsWholeBatch_asNonCountingTransport() throws Exception {
    // The fleet shape itself: a broker-wide condition (queue-process crash,
    // memory/disk alarm with reject-publish) settles the outstanding window with ONE
    // multiple=true nack frame, sweeping the whole in-flight batch at once. Every swept entry used
    // to surface as a bare RuntimeException -> counting ENTRY, so a transient broker-wide alarm
    // burned one ladder rung PER CYCLE for the entire backlog and terminal-FAILed it in
    // ~maxAttempts cycles (~243s at the shipped defaults): permanent gaps plus the FAILED-head
    // same-aggregate reorder. A nack is the broker reporting the message was NOT enqueued — a
    // broker-side condition, never a property of the message — so the sweep must produce the
    // typed, TRANSPORT-classified exception for every entry, exactly as Http bins a 5xx/429 and
    // Kafka bins a retriable error response.
    var publisher = buildBatchPublisher();
    var listener = captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("mn1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("mn2"), "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("mn3"), "{}", "Event");

    // One multiple=true nack delivered from inside the THIRD basicPublish: seqNos 1-3 are all
    // registered by then, so the frame sweeps the entire batch (mirrors the all-acked test above).
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              if (publishCount.incrementAndGet() == 3) {
                deliverConfirm(listener, 3L, true, false); // nacks 1, 2, 3
              }
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var result = publisher.publishBatch(List.of(e1, e2, e3));

    assertTrue(result.confirmed().isEmpty(), "a multiple-nack confirms nothing");
    assertEquals(3, result.failed().size(), "every swept entry is reported failed");
    for (var id : List.of("mn1", "mn2", "mn3")) {
      Exception failure = result.failed().get(OutboxEntryId.of(id));
      assertInstanceOf(
          RabbitMqOutboxPublisher.NackedPublishException.class,
          failure,
          "entry " + id + " must carry the typed nack exception");
      assertEquals(
          FailureKind.TRANSPORT,
          publisher.classifyFailure(failure),
          "entry "
              + id
              + " must classify non-counting TRANSPORT so a broker-wide alarm can never"
              + " mass-terminal-FAIL the in-flight set");
    }
  }

  @Test
  void publishBatch_timeout_only_acked_entry_confirmed_rest_failed() throws Exception {
    // Use a very short timeout
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(100))
            .build();
    var listener = captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("b1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("b2"), "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("b3"), "{}", "Event");

    // Only ack seqNo 1 — from inside its own basicPublish, so it can neither precede its
    // registration nor be descheduled past the 100ms confirm timeout (both directions of the
    // former "sleep 20ms on an executor" race; see deliverConfirm). seqNos 2 and 3 are never
    // confirmed, so the batch still runs its real timeout sweep over them.
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              if (publishCount.incrementAndGet() == 1) {
                deliverConfirm(listener, 1L, false, true); // acks seqNo 1 (b1)
              }
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var result = publisher.publishBatch(List.of(e1, e2, e3));

    // Should not throw — timeout is handled gracefully
    assertEquals(1, result.confirmed().size());
    assertTrue(result.confirmed().contains(OutboxEntryId.of("b1")));

    assertEquals(2, result.failed().size());
    assertTrue(result.failed().containsKey(OutboxEntryId.of("b2")));
    assertTrue(result.failed().containsKey(OutboxEntryId.of("b3")));
    assertInstanceOf(TimeoutException.class, result.failed().get(OutboxEntryId.of("b2")));
  }

  @Test
  void publishBatch_basicPublish_ioexception_mid_batch_entry_in_failed() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("b1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("b2"), "{}", "Event"); // will fail
    var e3 = OutboxEntry.pending(OutboxEntryId.of("b3"), "{}", "Event"); // cascades

    // basicPublish throws on the 2nd call (seqNo 2). b1 publishes successfully and is acked from
    // inside that very call — seq 1 is already registered there (see deliverConfirm), so the ack
    // cannot be dropped the way the former "sleep 30ms on an executor" scaffold allowed.
    var ioex = new IOException("channel died");
    doAnswer(
            inv -> {
              deliverConfirm(listener, 1L, false, true); // b1 published successfully, then acked
              return null;
            })
        .doThrow(ioex)
        .doNothing()
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    var result = publisher.publishBatch(List.of(e1, e2, e3));

    // b1 was acked
    assertTrue(result.confirmed().contains(OutboxEntryId.of("b1")));
    // b2 threw IOException, b3 cascaded with same cause
    assertTrue(result.failed().containsKey(OutboxEntryId.of("b2")));
    assertTrue(result.failed().containsKey(OutboxEntryId.of("b3")));
    assertInstanceOf(IOException.class, result.failed().get(OutboxEntryId.of("b2")));
  }

  @Test
  void publishBatch_basicPublish_shutdownSignal_mid_batch_entry_in_failed() throws Exception {
    // channel.basicPublish also throws AlreadyClosedException (an unchecked
    // ShutdownSignalException) when the channel/connection is already closed — a broker restart or
    // a
    // network blip mid-reconnect. The old catch handled only IOException, so this unchecked throw
    // escaped the whole batch: it leaked the outstanding-seq entry registered before basicPublish
    // and abandoned the batch with no BatchResult. publishBatch must instead handle it
    // symmetrically
    // to the IOException path — fail the entry (and the cascading remainder), return a complete
    // BatchResult, and never leak a registered seq.
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("s1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("s2"), "{}", "Event"); // basicPublish throws here
    var e3 = OutboxEntry.pending(OutboxEntryId.of("s3"), "{}", "Event"); // cascades

    var closed =
        new AlreadyClosedException(
            new ShutdownSignalException(true, false, null, "test-channel-closed"));
    // e1 (seq 1) is published successfully before the close; ack it on the connection thread as
    // soon as it has been published. Delivering the ack from inside e1's basicPublish — after the
    // publisher registered seq 1 in `outstanding`, which it does before every basicPublish — is
    // deterministic: a former "sleep 30ms on an executor, then ack" scaffold raced the batch setup
    // and dropped the ack whenever the first publish took longer than the sleep to get going.
    var connectionThreadFailure = new AtomicReference<Throwable>();
    doAnswer(
            inv -> {
              var connectionThread =
                  new Thread(
                      () -> {
                        try {
                          listener.handleAck(1L, false);
                        } catch (Throwable t) {
                          connectionThreadFailure.set(t);
                        }
                      },
                      "confirm-io");
              connectionThread.start();
              connectionThread.join();
              return null;
            })
        .doThrow(closed)
        .doNothing()
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    var result = assertDoesNotThrow(() -> publisher.publishBatch(List.of(e1, e2, e3)));
    assertNull(connectionThreadFailure.get(), "confirm dispatch must not throw");

    assertTrue(result.confirmed().contains(OutboxEntryId.of("s1")), "e1 was published and acked");
    assertTrue(
        result.failed().containsKey(OutboxEntryId.of("s2")),
        "the entry whose basicPublish threw AlreadyClosedException must land in failed()");
    assertTrue(
        result.failed().containsKey(OutboxEntryId.of("s3")),
        "entries after a dead channel cascade into failed()");
    assertInstanceOf(ShutdownSignalException.class, result.failed().get(OutboxEntryId.of("s2")));
    assertEquals(
        0,
        publisher.outstandingSize(),
        "the seq registered before the failing basicPublish must not be leaked in outstanding");
  }

  @Test
  void publishBatch_all_entries_fail_basicPublish_skips_confirm_wait() throws Exception {
    // When the very FIRST entry's basicPublish throws, `published` stays empty for the whole
    // batch (every entry cascades into failed()) — distinct from the mid-batch-failure tests
    // above, where at least one entry publishes successfully before the throw. This exercises the
    // `!published.isEmpty()` guard's false branch: no confirm wait is even attempted.
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("allfail-1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("allfail-2"), "{}", "Event");

    doThrow(new IOException("broker unreachable"))
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var result = publisher.publishBatch(List.of(e1, e2));

    assertTrue(result.confirmed().isEmpty());
    assertEquals(2, result.failed().size());
    assertInstanceOf(IOException.class, result.failed().get(OutboxEntryId.of("allfail-1")));
    assertInstanceOf(IOException.class, result.failed().get(OutboxEntryId.of("allfail-2")));
    assertEquals(0, publisher.outstandingSize(), "no seq may be leaked when nothing published");
  }

  @Test
  void publishBatch_timeout_sweep_skips_entry_already_nacked() throws Exception {
    // An entry nacked shortly BEFORE the batch times out must keep its nack cause; the timeout
    // sweep must skip it (acked=false, nacked=true) rather than overwriting it with a spurious
    // TimeoutException. The genuinely-unconfirmed sibling entry must still time out normally.
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(150))
            .build();
    var listener = captureListener();

    var nackedEarly = OutboxEntry.pending(OutboxEntryId.of("nack-early"), "{}", "Event");
    var neverSettled = OutboxEntry.pending(OutboxEntryId.of("never-settled"), "{}", "Event");

    // Nack seqNo 1 from inside its own basicPublish: registered-then-nacked, well before the 150ms
    // timeout, with no scheduling luck involved. This is the case that proved the diagnosis —
    // delivering the same nack one step earlier (before publishBatch registers seq 1) drops it and
    // the sweep overwrites the entry with a spurious TimeoutException. seqNo 2 is never settled, so
    // the sweep still runs for real.
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              if (publishCount.incrementAndGet() == 1) {
                deliverConfirm(listener, 1L, false, false); // nack seqNo 1
              }
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var result = publisher.publishBatch(List.of(nackedEarly, neverSettled));

    assertTrue(result.confirmed().isEmpty());
    assertEquals(2, result.failed().size());
    assertFalse(
        result.failed().get(OutboxEntryId.of("nack-early")) instanceof TimeoutException,
        "an entry nacked before the timeout must keep its nack cause, not be overwritten by the"
            + " sweep");
    assertInstanceOf(
        TimeoutException.class, result.failed().get(OutboxEntryId.of("never-settled")));
  }

  @Test
  void publishBatch_interruptedConfirmWait_returnsTheSettledOutcomes_andTheUnsettledAsInterrupted()
      throws Exception {
    // A shutdown interrupt during the confirm wait must not discard what the broker already
    // settled: the acked entry is confirmed (so the poller records it DELIVERED), the nacked one
    // failed as a nack, and the published-but-unsettled one failed with the interruption — the
    // broker may already hold it, so the poller keeps its claim. The interrupt flag is restored.
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();

    var acked = OutboxEntry.pending(OutboxEntryId.of("int-acked"), "{}", "Event");
    var nacked = OutboxEntry.pending(OutboxEntryId.of("int-nacked"), "{}", "Event");
    var pending = OutboxEntry.pending(OutboxEntryId.of("int-pending"), "{}", "Event");

    // Settle entry 1 (ack) and entry 2 (nack) from inside their own basicPublish calls, then raise
    // the interrupt from inside entry 3's — after the publish loop's per-entry interrupt check for
    // entry 3 has already passed, so all three entries are published and the confirm wait is
    // entered with exactly one slot outstanding. latch.await then aborts on the already-set flag,
    // hitting the same catch block, with the mixed acked/nacked/pending states this test names
    // guaranteed rather than hoped for. The former "sleep 20ms, settle, sleep 20ms, interrupt"
    // scaffold could drop both confirms (see deliverConfirm) and leave nothing mixed at all.
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              long seq = publishCount.incrementAndGet();
              if (seq == 1) {
                deliverConfirm(listener, 1L, false, true);
              } else if (seq == 2) {
                deliverConfirm(listener, 2L, false, false);
              } else {
                Thread.currentThread().interrupt(); // interrupts the publishing (test) thread
              }
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    OutboxPublisher.BatchResult result;
    try {
      result = publisher.publishBatch(List.of(acked, nacked, pending));
      assertTrue(
          Thread.currentThread().isInterrupted(),
          "interrupt flag must be restored even with mixed acked/nacked/pending entries");
    } finally {
      Thread.interrupted(); // clear flag so it does not leak into other tests
    }
    assertEquals(Set.of(acked.id()), result.confirmed());
    assertEquals(Set.of(nacked.id(), pending.id()), result.failed().keySet());
    assertInstanceOf(
        RabbitMqOutboxPublisher.NackedPublishException.class, result.failed().get(nacked.id()));
    assertInstanceOf(InterruptedException.class, result.failed().get(pending.id()));
  }

  @Test
  void publishBatch_interruptedConfirmWait_failsThePublishedEntryAsInterrupted_restoresFlag()
      throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("b1"), "{}", "Event");

    // Interrupt from inside the hand-off itself, after the publish loop's interrupt check for this
    // (only) entry has passed: the entry IS published, so publishBatch enters the confirm wait, and
    // latch.await aborts immediately on the already-set flag — the same catch block the old "sleep
    // 30ms on an executor, then interrupt" scaffold reached only if the executor happened to fire
    // while the main thread was inside the wait.
    doAnswer(
            inv -> {
              Thread.currentThread().interrupt(); // interrupts the publishing (test) thread
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    OutboxPublisher.BatchResult result;
    try {
      result = publisher.publishBatch(List.of(e1));
      assertTrue(
          Thread.currentThread().isInterrupted(),
          "interrupt flag must be restored after publishBatch interrupt");
    } finally {
      Thread.interrupted(); // clear flag so it does not leak into other tests
    }
    assertTrue(result.confirmed().isEmpty());
    assertInstanceOf(InterruptedException.class, result.failed().get(e1.id()));
  }

  @Test
  void publishBatch_preClears_stale_returned_marker_so_delivered_entry_is_not_spuriously_retried()
      throws Exception {
    // publishBatch must drain stale `returned` markers for its ids before publishing, the
    // way publish() already does. Otherwise a late basic.return from a PRIOR timed-out attempt of
    // the same id (recorded in `returned` after that batch finished) makes this batch's
    // now-routable
    // entry appear unroutable at result-build time — one spurious retry of an already-delivered
    // entry. Fail-first: without the pre-clear the entry lands in failed(), not confirmed().
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("stale-ret-1"), "{}", "Event");

    // Seed a STALE returned marker for e1 (as if a prior attempt's basic.return arrived late).
    publisher.handleReturn(new AMQP.BasicProperties.Builder().messageId("stale-ret-1").build());

    // This batch's publish is routable and acked — the ack comes from inside its own basicPublish,
    // so it lands after seq 1 is registered (see deliverConfirm) instead of racing a fixed sleep.
    doAnswer(
            inv -> {
              deliverConfirm(listener, 1L, false, true);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var result = publisher.publishBatch(List.of(e1));

    assertEquals(
        Set.of(OutboxEntryId.of("stale-ret-1")),
        result.confirmed(),
        "a stale returned marker from a prior attempt must be pre-cleared so a now-delivered entry"
            + " is confirmed, not spuriously retried");
    assertTrue(result.failed().isEmpty());
  }

  @Test
  void publishBatch_empty_list_returns_empty_result() throws Exception {
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();

    var result = publisher.publishBatch(List.of());

    assertTrue(result.confirmed().isEmpty());
    assertTrue(result.failed().isEmpty());
    verify(channel, never()).basicPublish(any(), any(), anyBoolean(), any(), any());
  }

  @Test
  void classifyFailure_connectionLossIsTransport_confirmTimeoutIsInFlight() throws Exception {
    // A dead channel/connection (basicPublish threw — nothing handed off)
    // is
    // a TRANSPORT outage the poller re-arms immediately; a confirm-wait TIMEOUT means the message
    // was
    // already published (handed off) but unconfirmed, so it is IN_FLIGHT — the poller must keep it
    // claimed for lease-reclaim rather than releasing it (which would duplicate + reorder).
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new AlreadyClosedException(
                new ShutdownSignalException(true, false, null, "channel closed"))));
    assertEquals(
        FailureKind.TRANSPORT, publisher.classifyFailure(new IOException("connection lost")));
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(new TimeoutException("no confirm in time")));
  }

  @Test
  void publish_connectionLostWhileAwaitingConfirm_isInFlight() throws Exception {
    // Single-entry publish:
    // the connection dies AFTER basicPublish handed the message off but BEFORE the broker confirm
    // arrives. The broker may have enqueued the message, so the claim must be held (IN_FLIGHT) —
    // classifying it TRANSPORT (as a basicPublish-phase ShutdownSignalException correctly is)
    // releases the claim, so a peer relay re-publishes a message the broker may already hold:
    // duplicate + same-aggregate reorder. publish() reports this phase as
    // UnconfirmedPublishException with the channel's close reason as cause.
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var closed = new AtomicBoolean(false);
    when(channel.isOpen()).thenAnswer(inv -> !closed.get());
    when(channel.getCloseReason())
        .thenReturn(new ShutdownSignalException(false, false, null, "connection closed"));
    doAnswer(
            inv -> {
              // The hand-off succeeds; the connection dies right afterwards, so no confirm can
              // ever arrive for it.
              closed.set(true);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(200))
            .build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("lost"), "{}", "Event");

    Exception thrown = assertThrows(Exception.class, () -> publisher.publish(entry));

    // The message WAS handed off before the failure — that is what makes this post-hand-off.
    verify(channel).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    assertInstanceOf(
        RabbitMqOutboxPublisher.UnconfirmedPublishException.class,
        thrown,
        "a confirm that can never arrive (channel dead after hand-off) must be reported as the"
            + " post-hand-off UnconfirmedPublishException, not a bare timeout: "
            + thrown);
    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(thrown),
        "post-hand-off connection loss must hold the claim: " + thrown);
  }

  // ---------------------------------------------------------------------------
  // publish() must never kill the channel.
  //
  // amqp-client 5.25.0's ChannelN.waitForConfirmsOrDie(long) — verified by disassembling the
  // pinned jar — CLOSES the channel BEFORE throwing: close(200, "NACKS RECEIVED") then
  // IOException("nacks received") on a nack; close(406, "TIMEOUT WAITING FOR ACK") then the
  // TimeoutException on a timeout. The close is client-initiated, so automatic connection
  // recovery never reopens the channel: every later basicPublish throws AlreadyClosedException,
  // which classifies TRANSPORT (non-counting, retried forever) — the whole backlog stalls
  // permanently while logs report a transient outage. The pre-fix stubs in this class threw from
  // waitForConfirmsOrDie WITHOUT the close side effect, which is exactly why mock tests never
  // saw the defect; the harness below models the close faithfully.
  // ---------------------------------------------------------------------------

  /**
   * Stubs the mock channel to behave like amqp-client 5.25.0: {@code closed} flips on {@code
   * waitForConfirmsOrDie} (before its throw), a closed channel fails {@code basicPublish} with
   * {@code AlreadyClosedException}, seqNos increment from 1, and each successful {@code
   * basicPublish} delivers the scripted confirm for its own seq on a separate "connection I/O"
   * thread (joined, so the test stays deterministic). {@code confirmScript[i]} is the outcome of
   * the i-th publish: TRUE = ack, FALSE = nack, null = no confirm (times out).
   */
  private void modelJarFaithfulChannel(
      AtomicBoolean closed,
      boolean nackNotTimeoutOnOrDie,
      AtomicReference<ConfirmListener> listenerRef,
      List<Boolean> confirmScript)
      throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    when(channel.isOpen()).thenAnswer(inv -> !closed.get());
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              if (closed.get()) {
                throw new AlreadyClosedException(
                    new ShutdownSignalException(true, false, null, "clean channel shutdown"));
              }
              long seq = seqCounter.get() - 1; // the seq handed out for THIS message
              int call = (int) publishCount.getAndIncrement();
              Boolean outcome =
                  call < confirmScript.size() ? confirmScript.get(call) : Boolean.TRUE;
              if (outcome != null) {
                var confirmIo =
                    new Thread(
                        () -> {
                          try {
                            if (outcome) {
                              listenerRef.get().handleAck(seq, false);
                            } else {
                              listenerRef.get().handleNack(seq, false);
                            }
                          } catch (IOException e) {
                            throw new AssertionError("confirm dispatch must not throw", e);
                          }
                        },
                        "confirm-io");
                confirmIo.start();
                confirmIo.join();
              }
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    doAnswer(
            inv -> {
              closed.set(true); // ChannelN.close(...) runs BEFORE the throw — the defect's core
              if (nackNotTimeoutOnOrDie) {
                throw new IOException("nacks received");
              }
              throw new TimeoutException();
            })
        .when(channel)
        .waitForConfirmsOrDie(anyLong());
  }

  @Test
  void publish_brokerNack_classifiesTRANSPORT_andChannelStaysUsable() throws Exception {
    var closed = new AtomicBoolean(false);
    var listenerRef = new AtomicReference<ConfirmListener>();
    // First publish is nacked by the broker; the second is acked.
    modelJarFaithfulChannel(closed, true, listenerRef, java.util.Arrays.asList(false, true));
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(2))
            .build();
    listenerRef.set(captureListener());

    var nacked = OutboxEntry.pending(OutboxEntryId.of("nacked-1"), "{}", "Event");
    var next = OutboxEntry.pending(OutboxEntryId.of("after-nack"), "{}", "Event");

    Exception nackFailure = assertThrows(Exception.class, () -> publisher.publish(nacked));

    // A broker nack is the broker's own report that IT could not enqueue the
    // message — RabbitMQ nacks on internal errors of the queue process and on reject-publish
    // overflow, broker-side conditions that hit whatever happens to be in flight, never a verdict
    // about the message. Under the old ENTRY binning a transient broker-wide alarm mass-nacked the
    // in-flight set and terminal-FAILed it in ~maxAttempts ladders (permanent gap + the FAILED-head
    // reorder). It must be a typed exception classified TRANSPORT: definitively NOT enqueued, so an
    // immediate re-arm cannot duplicate — matching how Http (5xx/429) and Kafka (retriable error
    // responses) bin the structurally identical broker-side transient.
    assertInstanceOf(
        RabbitMqOutboxPublisher.NackedPublishException.class,
        nackFailure,
        "a broker nack must surface as the typed NackedPublishException, not a bare"
            + " RuntimeException that falls through to a counting classification");
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(nackFailure),
        "a broker nack must be non-counting TRANSPORT (broker-side condition, message provably not"
            + " enqueued, re-arm cannot duplicate) — got "
            + nackFailure);
    assertDoesNotThrow(
        () -> publisher.publish(next),
        "the channel must survive a nacked publish: waitForConfirmsOrDie CLOSES it"
            + " client-initiated, automatic recovery never reopens it, and every later publish"
            + " fails TRANSPORT forever — a permanently stalled backlog");
  }

  @Test
  void publish_confirmTimeout_keepsChannelUsable_andStaysInFlight() throws Exception {
    var closed = new AtomicBoolean(false);
    var listenerRef = new AtomicReference<ConfirmListener>();
    // First publish gets no confirm at all (times out); the second is acked.
    modelJarFaithfulChannel(closed, false, listenerRef, java.util.Arrays.asList(null, true));
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(200))
            .build();
    listenerRef.set(captureListener());

    var slow = OutboxEntry.pending(OutboxEntryId.of("slow-1"), "{}", "Event");
    var next = OutboxEntry.pending(OutboxEntryId.of("after-timeout"), "{}", "Event");

    Exception timeoutFailure = assertThrows(Exception.class, () -> publisher.publish(slow));

    assertEquals(
        FailureKind.IN_FLIGHT,
        publisher.classifyFailure(timeoutFailure),
        "a confirm timeout stays IN_FLIGHT (the broker may still hold the message): "
            + timeoutFailure);
    assertDoesNotThrow(
        () -> publisher.publish(next),
        "the channel must survive a confirm timeout: waitForConfirmsOrDie CLOSES it before"
            + " rethrowing, permanently stalling every later publish");
  }

  @Test
  void publish_neverCallsWaitForConfirmsOrDie() throws Exception {
    // Regression pin for the fix itself: the single-entry path must not touch the channel-killing
    // API at all — it shares the batch path's listener-based confirm settlement.
    var closed = new AtomicBoolean(false);
    var listenerRef = new AtomicReference<ConfirmListener>();
    modelJarFaithfulChannel(closed, true, listenerRef, java.util.Arrays.asList(true));
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    listenerRef.set(captureListener());

    publisher.publish(OutboxEntry.pending(OutboxEntryId.of("no-ordie"), "{}", "Event"));

    verify(channel, never()).waitForConfirmsOrDie();
    verify(channel, never()).waitForConfirmsOrDie(anyLong());
  }

  @Test
  void publish_interruptedBeforeAttempt_throwsInterruptedException_flagStaysSet() throws Exception {
    // A thread that arrives already interrupted (shutdown) must not hand the message off at all,
    // and publish() must keep its SPI contract: InterruptedException with the flag still set, so
    // OutboxPublisher.publishSequentially's isInterruption handling leaves the entry unattempted
    // for lease-reclaim instead of burning a retry attempt.
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("pre-int"), "{}", "Event");

    try {
      Thread.currentThread().interrupt();
      assertThrows(InterruptedException.class, () -> publisher.publish(entry));
      assertTrue(
          Thread.currentThread().isInterrupted(),
          "the interrupt flag must stay set for the poller's shutdown handling");
      verify(channel, never()).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    } finally {
      Thread.interrupted(); // clear the flag so it does not leak into other tests
    }
  }

  @Test
  void classifyFailure_basicPublishShutdownStaysTransport() throws Exception {
    // The pre-hand-off flavour must NOT regress: an AlreadyClosedException from basicPublish means
    // nothing was handed off, so it re-arms immediately (TRANSPORT) instead of parking the entry
    // for a full claim lease.
    doThrow(new AlreadyClosedException(new ShutdownSignalException(true, false, null, "closed")))
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var entry = OutboxEntry.pending(OutboxEntryId.of("dead"), "{}", "Event");

    Exception thrown = assertThrows(Exception.class, () -> publisher.publish(entry));

    verify(channel, never()).waitForConfirmsOrDie(anyLong());
    assertEquals(FailureKind.TRANSPORT, publisher.classifyFailure(thrown));
  }

  @Test
  void classifyFailure_unroutableIsEntry_nackIsTransport() throws Exception {
    // Unroutable (no queue bound) stays a per-entry rejection that counts toward the ladder — the
    // UnroutableException subtype must classify ENTRY despite extending IOException. A broker nack
    // is the opposite: a broker-side failure to enqueue (queue-process
    // error, reject-publish overflow), typed NackedPublishException and non-counting TRANSPORT —
    // both directly and through a cause chain.
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    assertEquals(
        FailureKind.ENTRY,
        publisher.classifyFailure(
            new RabbitMqOutboxPublisher.UnroutableException("no queue bound: e1")));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new RabbitMqOutboxPublisher.NackedPublishException("broker nacked message: e1")));
    assertEquals(
        FailureKind.TRANSPORT,
        publisher.classifyFailure(
            new RuntimeException(
                new RabbitMqOutboxPublisher.NackedPublishException("broker nacked message: e1"))));
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
  void classifyFailure_cyclicChainWithoutMatch_terminatesAndReturnsEntry() throws Exception {
    var a = new CyclicThrowable();
    var b = new CyclicThrowable();
    a.setNext(b);
    b.setNext(a); // a -> b -> a -> ... an unbounded getCause() walk would never terminate

    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () ->
            assertEquals(
                FailureKind.ENTRY,
                publisher.classifyFailure(a),
                "no ShutdownSignalException/TimeoutException/IOException anywhere in the cycle"));
  }

  @Test
  void publishBatch_deadlineAlreadyPassed_stopsPublishLoop_leavesEntriesUnattempted()
      throws Exception {
    // The publish LOOP (not only the confirm wait) must respect the deadline.
    // basicPublish blocks the thread while the broker is flow-controlled (memory/disk alarm);
    // without a per-entry deadline check the loop publishes past the claim lease and a second relay
    // reclaims and re-publishes the tail -> duplicate + same-aggregate reorder. With the deadline
    // already elapsed, the loop must NOT call basicPublish at all and must leave every entry in
    // NEITHER bucket, so the poller keeps them IN_PROGRESS for lease-reclaim.
    var publisher = buildBatchPublisher();
    captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("dl-loop-1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("dl-loop-2"), "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("dl-loop-3"), "{}", "Event");

    var result =
        publisher.publishBatch(List.of(e1, e2, e3), java.time.Instant.now().minusMillis(1));

    assertTrue(result.confirmed().isEmpty(), "no entry may be confirmed past the deadline");
    assertTrue(
        result.failed().isEmpty(),
        "entries not attempted before the deadline must be in NEITHER bucket (IN_PROGRESS for"
            + " lease-reclaim), not failed");
    verify(channel, never()).basicPublish(any(), any(), anyBoolean(), any(), any());
  }

  @Test
  void publishBatch_boundsConfirmWaitByDeadline_notFullConfirmTimeout() throws Exception {
    // With a 5s confirmTimeout but a ~200ms publish deadline, the batch must return near
    // the
    // deadline (not block the full 5s) so a slow broker cannot hold the claim past its lease. The
    // never-acked entry times out and is retried, exactly as a normal confirm timeout.
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(5))
            .build();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("dl-1"), "{}", "Event"); // never acked

    long start = System.nanoTime();
    var result = publisher.publishBatch(List.of(e1), java.time.Instant.now().plusMillis(200));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertTrue(
        elapsedMs < 3_000,
        "confirm wait must be bounded by the ~200ms deadline, not the 5s confirmTimeout (was "
            + elapsedMs
            + "ms)");
    assertTrue(result.confirmed().isEmpty());
    assertInstanceOf(TimeoutException.class, result.failed().get(OutboxEntryId.of("dl-1")));
  }

  @Test
  void build_registers_confirm_listener() throws Exception {
    RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();

    verify(channel).addConfirmListener(any(ConfirmListener.class));
  }

  // ---------------------------------------------------------------------------
  // handleReturn / resolveEntryId branch coverage — the unroutable-return path.
  //
  // handleReturn is package-private specifically so these tests can drive it directly with
  // crafted AMQP.BasicProperties, exercising resolveEntryId's messageId-then-header resolution
  // without needing a real broker return frame. Each test wires the call into the mocked
  // channel's basicPublish (mirroring how the broker interleaves basic.return before basic.ack)
  // and asserts the OBSERVABLE effect on publish()'s outcome — not just that the branch executed.
  // ---------------------------------------------------------------------------

  @Test
  void handleReturn_with_null_properties_is_ignored_without_throwing() throws Exception {
    // Defensive branch: the real ReturnListener callback never hands us null properties, but
    // resolveEntryId guards against it explicitly rather than trusting the caller.
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();

    assertDoesNotThrow(() -> publisher.handleReturn(null));
  }

  @Test
  void publish_falls_back_to_header_when_messageId_is_null() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();
    var entry = OutboxEntry.pending(OutboxEntryId.of("hdr-fb-null"), "{}", "Event");

    // Simulates a returned message whose messageId property is absent — resolveEntryId must fall
    // back to the X-Outbox-Entry-Id header instead of giving up.
    var returnProps =
        new AMQP.BasicProperties.Builder()
            .headers(Map.of("X-Outbox-Entry-Id", "hdr-fb-null"))
            .build();
    doAnswer(
            inv -> {
              // The broker dispatches basic.return BEFORE basic.ack, in frame order.
              publisher.handleReturn(returnProps);
              deliverAck(listener, seqCounter.get() - 1);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var ex = assertThrows(IOException.class, () -> publisher.publish(entry));
    assertTrue(ex.getMessage().contains("hdr-fb-null"));
  }

  @Test
  void publish_falls_back_to_header_when_messageId_is_blank() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();
    var entry = OutboxEntry.pending(OutboxEntryId.of("hdr-fb-blank"), "{}", "Event");

    // messageId present but blank (not null) is a distinct branch from the null case above.
    var returnProps =
        new AMQP.BasicProperties.Builder()
            .messageId("")
            .headers(Map.of("X-Outbox-Entry-Id", "hdr-fb-blank"))
            .build();
    doAnswer(
            inv -> {
              // The broker dispatches basic.return BEFORE basic.ack, in frame order.
              publisher.handleReturn(returnProps);
              deliverAck(listener, seqCounter.get() - 1);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    var ex = assertThrows(IOException.class, () -> publisher.publish(entry));
    assertTrue(ex.getMessage().contains("hdr-fb-blank"));
  }

  @Test
  void publish_ignores_return_with_no_messageId_and_no_headers() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();
    var entry = OutboxEntry.pending(OutboxEntryId.of("no-id-at-all"), "{}", "Event");

    // Neither messageId nor a headers map at all — resolveEntryId must return null rather than
    // NPE, and the entry must NOT be marked returned (a malformed return must not falsely fail a
    // publish that the broker otherwise confirms).
    var returnProps = new AMQP.BasicProperties.Builder().build();
    doAnswer(
            inv -> {
              // The broker dispatches basic.return BEFORE basic.ack, in frame order.
              publisher.handleReturn(returnProps);
              deliverAck(listener, seqCounter.get() - 1);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    assertDoesNotThrow(() -> publisher.publish(entry));
  }

  @Test
  void publish_ignores_return_with_headers_present_but_missing_entry_id_key() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();
    var entry = OutboxEntry.pending(OutboxEntryId.of("no-matching-header"), "{}", "Event");

    // Headers map is non-null but does not contain X-Outbox-Entry-Id — distinct from the
    // headers-map-is-null case: the fallback must tolerate a present-but-non-matching map too.
    var returnProps =
        new AMQP.BasicProperties.Builder().headers(Map.of("Other-Header", "irrelevant")).build();
    doAnswer(
            inv -> {
              // The broker dispatches basic.return BEFORE basic.ack, in frame order.
              publisher.handleReturn(returnProps);
              deliverAck(listener, seqCounter.get() - 1);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    assertDoesNotThrow(() -> publisher.publish(entry));
  }

  @Test
  void publish_ignores_return_with_blank_header_value() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var listener = captureListener();
    var entry = OutboxEntry.pending(OutboxEntryId.of("blank-header-value"), "{}", "Event");

    // The header key is present but its value is blank — after the fallback, the final
    // null-or-blank check must still reject it.
    var returnProps =
        new AMQP.BasicProperties.Builder().headers(Map.of("X-Outbox-Entry-Id", "")).build();
    doAnswer(
            inv -> {
              // The broker dispatches basic.return BEFORE basic.ack, in frame order.
              publisher.handleReturn(returnProps);
              deliverAck(listener, seqCounter.get() - 1);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    assertDoesNotThrow(() -> publisher.publish(entry));
  }

  // ---------------------------------------------------------------------------
  // Regression tests for concurrency defects (FIX 1, FIX 3)
  // ---------------------------------------------------------------------------

  /**
   * FIX 1 regression: register-before-send.
   *
   * <p>Simulates a broker ack arriving synchronously on the same thread that calls basicPublish —
   * i.e., the ConfirmListener fires DURING the basicPublish invocation, before outstanding.put()
   * used to happen. Before FIX 1, the seqNo was not yet in the outstanding map when the listener
   * fired, so the entry's latch slot was never counted down → the entry ended up in failed
   * (timeout). After FIX 1, the entry is registered BEFORE basicPublish, so the listener finds it
   * and counts the latch down correctly → the entry is confirmed.
   */
  @Test
  void publishBatch_confirm_arriving_during_publish_is_registered_and_confirmed() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);

    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(500))
            .build();
    var listener = captureListener();

    var entry = OutboxEntry.pending(OutboxEntryId.of("race-1"), "{}", "OrderCreated");

    // Stub basicPublish to fire the ConfirmListener synchronously (simulates confirm arriving
    // before basicPublish returns — the worst-case race that FIX 1 eliminates).
    doAnswer(
            invocation -> {
              // seqNo 1 was assigned by getNextPublishSeqNo() before this call.
              listener.handleAck(1L, false);
              return null;
            })
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    var result = publisher.publishBatch(List.of(entry));

    assertTrue(
        result.confirmed().contains(OutboxEntryId.of("race-1")),
        "entry confirmed by broker during publish must appear in confirmed, not failed");
    assertTrue(
        result.failed().isEmpty(),
        "no entry should be in failed when broker acked before basicPublish returned");
  }

  /**
   * FIX 3 regression: the batch timeout sweep must complete without throwing even when the confirm
   * thread already settled (and removed from {@code outstanding}) some of the batch's entries.
   * Historically the sweep did a per-entry {@code outstanding.remove(seqNoOf(...))} where {@code
   * seqNoOf} could return {@code null} and NPE; the sweep now removes by id via {@code
   * values().removeIf}, which is null-safe and tolerant of concurrent settles by design.
   *
   * <p>Deterministic setup (no timing races — this test was flaky when it raced acks against a
   * fixed {@code Thread.sleep}): e1 is acked the instant it is published. Production registers each
   * entry in {@code outstanding} BEFORE calling {@code basicPublish} (FIX 1), so seqNo 1 is already
   * tracked inside the publish stub and the ack lands within the confirm window every time. e2 is
   * never acked, so the confirm latch times out and the sweep runs over it: it must not throw, e1
   * must be confirmed, and e2 must fail with a {@link TimeoutException}.
   */
  @Test
  void publishBatch_timeout_path_does_not_throw_npe_when_entry_already_removed() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);

    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(80))
            .build();
    var listener = captureListener();

    var e1 = OutboxEntry.pending(OutboxEntryId.of("npe-1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("npe-2"), "{}", "Event");

    // Ack e1 deterministically the moment it is published — it is already in `outstanding` (FIX 1
    // registers before basicPublish), so no race against a fixed sleep. e2 is left unacked so the
    // confirm latch times out and the sweep processes it.
    var firstPublish = new AtomicBoolean(true);
    doAnswer(
            inv -> {
              if (firstPublish.compareAndSet(true, false)) {
                listener.handleAck(1L, false); // settle e1 (seqNo 1) within the confirm window
              }
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    // Timeout sweep must run for e2 without throwing — FIX 3 guards outstanding.remove(null).
    var result = assertDoesNotThrow(() -> publisher.publishBatch(List.of(e1, e2)));

    // e1 acked before timeout → confirmed; e2 never confirmed → failed (TimeoutException).
    assertTrue(result.confirmed().contains(OutboxEntryId.of("npe-1")));
    assertTrue(result.failed().containsKey(OutboxEntryId.of("npe-2")));
    assertInstanceOf(TimeoutException.class, result.failed().get(OutboxEntryId.of("npe-2")));
  }

  // ---------------------------------------------------------------------------
  // Regression tests: confirm-tracking data race (batchSeqNos removal)
  // ---------------------------------------------------------------------------

  /**
   * Deterministic settle-during-populate: the confirm callback runs on a SEPARATE thread for entry
   * k-1 while publishBatch is still inside basicPublish of entry k — i.e. confirms settle
   * mid-populate, interleaved with the publishing loop. Every confirm must be counted: all entries
   * confirmed, none failed, no stall to timeout.
   *
   * <p>Note on fail-first honesty: the submit/get handoff used here establishes a happens-before
   * edge between the publishing thread and the confirm thread, so this test cannot reproduce the
   * OLD LinkedHashSet visibility race — it deterministically pins the interleaving (settlement
   * strictly interleaved with population) that the race made unsafe. The unsynchronized-spinner
   * stress test below is the one that hits the old code's drop window.
   */
  @Test
  void publishBatch_confirms_settling_mid_publish_on_confirm_thread_all_counted() throws Exception {
    int batchSize = 5;
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(2))
            .build();
    var listener = captureListener();

    var entries = new ArrayList<OutboxEntry>(batchSize);
    for (int i = 1; i <= batchSize; i++) {
      entries.add(OutboxEntry.pending(OutboxEntryId.of("mid-" + i), "{}", "Event"));
    }

    var confirmThread = Executors.newSingleThreadExecutor();
    try {
      var publishCount = new AtomicLong(0);
      doAnswer(
              inv -> {
                long k = publishCount.incrementAndGet(); // entry k being published; its seq == k
                if (k > 1) {
                  // Settle the PREVIOUS entry on the confirm thread and wait for it to finish:
                  // the confirm listener runs while publishBatch is still mid-loop.
                  confirmThread
                      .submit(
                          () -> {
                            listener.handleAck(k - 1, false);
                            return null;
                          })
                      .get();
                }
                if (k == batchSize) {
                  // Settle the last entry too, while its own basicPublish is still in flight
                  // (FIX 1 registered its seq in `outstanding` before this call).
                  confirmThread
                      .submit(
                          () -> {
                            listener.handleAck(k, false);
                            return null;
                          })
                      .get();
                }
                return null;
              })
          .when(channel)
          .basicPublish(
              anyString(),
              anyString(),
              anyBoolean(),
              any(AMQP.BasicProperties.class),
              any(byte[].class));

      var result = publisher.publishBatch(entries);

      assertTrue(
          result.failed().isEmpty(),
          "no confirm may be dropped when settling interleaves publishing; failed="
              + result.failed().keySet());
      assertEquals(batchSize, result.confirmed().size(), "every entry must end confirmed");
    } finally {
      confirmThread.shutdown();
    }
  }

  /**
   * Stress regression for the confirm-tracking data race: a confirm thread with NO synchronization
   * against the publishing thread fires acks for sequence numbers the moment they are handed out by
   * getNextPublishSeqNo — modelling the connection I/O thread, whose reads have no happens-before
   * edge with the poller's writes.
   *
   * <p>The old implementation shadowed `outstanding` with a plain LinkedHashSet (`batchSeqNos`)
   * that the poller populated AFTER registering the seq in `outstanding`; a confirm processed in
   * that window (or reading a stale view of the set) failed the `contains` gate but still consumed
   * the `outstanding` entry — the confirm was dropped and the batch stalled to timeout with a
   * spurious failed entry. Gating settlement on the concurrent `outstanding` map itself makes the
   * drop structurally impossible.
   */
  @Test
  void publishBatch_unsynchronized_confirm_thread_never_drops_a_confirm() throws Exception {
    int batchSize = 256;
    int iterations = 20;

    var seqCounter = new AtomicLong(1);
    var maxHandedOutSeq = new AtomicLong(0);
    when(channel.getNextPublishSeqNo())
        .thenAnswer(
            inv -> {
              if (readByBuild()) {
                return seqCounter.get();
              }
              long seq = seqCounter.getAndIncrement();
              maxHandedOutSeq.set(seq);
              return seq;
            });

    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(2))
            .build();
    var listener = captureListener();

    var done = new AtomicBoolean(false);
    var spinner =
        new Thread(
            () -> {
              while (!done.get()) {
                long max = maxHandedOutSeq.get();
                for (long s = 1; s <= max; s++) {
                  try {
                    listener.handleAck(s, false); // re-fires are no-ops once a seq is settled
                  } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                  }
                }
              }
            },
            "test-confirm-spinner");
    spinner.start();

    try {
      for (int iter = 0; iter < iterations; iter++) {
        var entries = new ArrayList<OutboxEntry>(batchSize);
        for (int i = 0; i < batchSize; i++) {
          entries.add(
              OutboxEntry.pending(OutboxEntryId.of("race-" + iter + "-" + i), "{}", "Event"));
        }

        var result = publisher.publishBatch(entries);

        assertTrue(
            result.failed().isEmpty(),
            "iteration "
                + iter
                + ": confirm(s) dropped — batch stalled to timeout for "
                + result.failed().keySet());
        assertEquals(batchSize, result.confirmed().size(), "iteration " + iter);
      }
    } finally {
      done.set(true);
      spinner.join(5_000);
      assertFalse(spinner.isAlive(), "confirm spinner must terminate");
    }
  }

  /**
   * Runs {@code publishBatch(List.of(entry))} on a throwaway thread and interrupts it during the
   * confirm wait. The interrupt path reports the entry WITHOUT sweeping {@code outstanding}, so the
   * entry's seqNo stays registered — the stale leftover the batch-scoping tests need.
   */
  private void abandonBatchLeavingSeqOutstanding(
      RabbitMqOutboxPublisher publisher, OutboxEntry entry, CountDownLatch published)
      throws Exception {
    var outcome = new AtomicReference<Object>();
    var thread =
        new Thread(
            () -> {
              try {
                outcome.set(publisher.publishBatch(List.of(entry)));
              } catch (Throwable t) {
                outcome.set(t);
              }
            },
            "abandoned-batch");
    thread.start();
    assertTrue(published.await(2, TimeUnit.SECONDS), "abandoned batch must have published");
    thread.interrupt();
    thread.join(2_000);
    assertFalse(thread.isAlive(), "abandoned batch thread must have exited");
    var result =
        assertInstanceOf(
            OutboxPublisher.BatchResult.class,
            outcome.get(),
            "an interrupted publishBatch returns its outcome");
    assertInstanceOf(InterruptedException.class, result.failed().get(entry.id()));
  }

  /**
   * Batch scoping: a late confirm for a seqNo left over from a PREVIOUS batch (abandoned by an
   * interrupt, its seq still in `outstanding`) must NOT count down the new batch's latch. If it
   * did, the new batch's latch would hit zero prematurely and its genuinely-unconfirmed entry would
   * be misreported as nacked instead of timing out.
   */
  @Test
  void publishBatch_stale_confirm_from_previous_batch_does_not_settle_new_batch() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(300))
            .build();
    var listener = captureListener();

    var stale = OutboxEntry.pending(OutboxEntryId.of("stale-old"), "{}", "Event"); // seq 1
    var fresh = OutboxEntry.pending(OutboxEntryId.of("fresh-1"), "{}", "Event"); // seq 2
    var never = OutboxEntry.pending(OutboxEntryId.of("fresh-2"), "{}", "Event"); // seq 3

    var stalePublished = new CountDownLatch(1);
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              long n = publishCount.incrementAndGet();
              if (n == 1) {
                stalePublished.countDown(); // batch 1: `stale` published (seq 1)
              }
              if (n == 2) { // batch 2: `fresh` being published (seq 2)
                listener.handleAck(1L, false); // STALE confirm — different batch, must not count
                listener.handleAck(2L, false); // genuine confirm for `fresh`
              }
              return null; // n == 3: `never` (seq 3) is never confirmed
            })
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    abandonBatchLeavingSeqOutstanding(publisher, stale, stalePublished);

    var result = publisher.publishBatch(List.of(fresh, never));

    assertEquals(Set.of(OutboxEntryId.of("fresh-1")), result.confirmed());
    assertInstanceOf(
        TimeoutException.class,
        result.failed().get(OutboxEntryId.of("fresh-2")),
        "the unconfirmed entry must TIME OUT — a stale confirm counting toward the new latch"
            + " would have released the batch early and misreported it as nacked");
  }

  /**
   * A RETRIED entry (same OutboxEntryId republished after its batch was abandoned) can settle via
   * BOTH its stale seqNo and its fresh seqNo. The latch must count down at most once for it — a
   * double count-down would release the latch early and misreport the still-unconfirmed entry as
   * nacked instead of timed out.
   */
  @Test
  void publishBatch_retried_entry_settling_via_stale_and_fresh_seq_counts_once() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(300))
            .build();
    var listener = captureListener();

    var retried = OutboxEntry.pending(OutboxEntryId.of("retry-x"), "{}", "Event"); // seq 1, seq 2
    var never = OutboxEntry.pending(OutboxEntryId.of("retry-y"), "{}", "Event"); // seq 3

    var firstPublished = new CountDownLatch(1);
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              long n = publishCount.incrementAndGet();
              if (n == 1) {
                firstPublished.countDown(); // batch 1: `retried` published (seq 1), then abandoned
              }
              if (n == 2) { // batch 2: `retried` republished (seq 2)
                listener.handleAck(2L, false); // fresh confirm — settles `retried`
                listener.handleAck(1L, false); // stale seq, SAME id — must not count down again
              }
              return null; // n == 3: `never` (seq 3) is never confirmed
            })
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    abandonBatchLeavingSeqOutstanding(publisher, retried, firstPublished);

    var result = publisher.publishBatch(List.of(retried, never));

    assertEquals(Set.of(OutboxEntryId.of("retry-x")), result.confirmed());
    assertInstanceOf(
        TimeoutException.class,
        result.failed().get(OutboxEntryId.of("retry-y")),
        "double count-down for the retried entry would have released the latch early and"
            + " misreported the unconfirmed entry as nacked");
  }

  /** A stale ACK (via a leftover seqNo of a retried id) must not overturn a recorded NACK. */
  @Test
  void publishBatch_stale_ack_does_not_overturn_recorded_nack() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(2))
            .build();
    var listener = captureListener();

    var retried = OutboxEntry.pending(OutboxEntryId.of("nk-x"), "{}", "Event"); // seq 1, seq 2
    var other = OutboxEntry.pending(OutboxEntryId.of("nk-y"), "{}", "Event"); // seq 3

    var firstPublished = new CountDownLatch(1);
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              long n = publishCount.incrementAndGet();
              if (n == 1) {
                firstPublished.countDown();
              }
              if (n == 2) { // batch 2: `retried` republished (seq 2)
                listener.handleNack(2L, false); // broker NACKS the fresh publish
                listener.handleAck(1L, false); // stale ACK, same id — must not flip to confirmed
              }
              if (n == 3) {
                listener.handleAck(3L, false); // `other` confirmed normally
              }
              return null;
            })
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    abandonBatchLeavingSeqOutstanding(publisher, retried, firstPublished);

    var result = publisher.publishBatch(List.of(retried, other));

    assertEquals(Set.of(OutboxEntryId.of("nk-y")), result.confirmed());
    assertTrue(
        result.failed().containsKey(OutboxEntryId.of("nk-x")),
        "the nacked entry must stay failed — a stale ack must not overturn the nack");
  }

  /** Mirror case: a stale NACK must not overturn a recorded ACK. */
  @Test
  void publishBatch_stale_nack_does_not_overturn_recorded_ack() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(2))
            .build();
    var listener = captureListener();

    var retried = OutboxEntry.pending(OutboxEntryId.of("ak-x"), "{}", "Event"); // seq 1, seq 2

    var firstPublished = new CountDownLatch(1);
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              long n = publishCount.incrementAndGet();
              if (n == 1) {
                firstPublished.countDown();
              }
              if (n == 2) { // batch 2: `retried` republished (seq 2)
                listener.handleAck(2L, false); // broker ACKS the fresh publish
                listener.handleNack(1L, false); // stale NACK, same id — must not flip to failed
              }
              return null;
            })
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    abandonBatchLeavingSeqOutstanding(publisher, retried, firstPublished);

    var result = publisher.publishBatch(List.of(retried));

    assertEquals(Set.of(OutboxEntryId.of("ak-x")), result.confirmed());
    assertTrue(result.failed().isEmpty(), "a stale nack must not overturn the recorded ack");
  }

  /**
   * A confirm arriving when no batch has ever been active (publish() single-entry path) is
   * discarded without error, and a later publishBatch is unaffected.
   */
  @Test
  void confirm_without_active_batch_is_discarded_and_next_batch_unaffected() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(2))
            .build();
    var listener = captureListener();

    assertDoesNotThrow(() -> listener.handleAck(7L, false));
    assertDoesNotThrow(() -> listener.handleAck(9L, true));

    var entry = OutboxEntry.pending(OutboxEntryId.of("after-noop"), "{}", "Event");
    doAnswer(
            inv -> {
              listener.handleAck(1L, false);
              return null;
            })
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    var result = publisher.publishBatch(List.of(entry));

    assertEquals(Set.of(OutboxEntryId.of("after-noop")), result.confirmed());
    assertTrue(result.failed().isEmpty());
  }

  /**
   * Mirror of {@link #publishBatch_retried_entry_settling_via_stale_and_fresh_seq_counts_once()}
   * for the NACK side: a retried entry can be nacked via BOTH its stale and its fresh seqNo. The
   * second nack for the same id must find it already in {@code batch.nacked} (the {@code Set.add}
   * returns {@code false}) and must NOT double count-down the latch — otherwise the
   * still-unconfirmed sibling entry would be released early and misreported as nacked instead of
   * timing out.
   */
  @Test
  void publishBatch_duplicate_nack_for_same_entry_counts_once() throws Exception {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(300))
            .build();
    var listener = captureListener();

    var retried = OutboxEntry.pending(OutboxEntryId.of("dup-nack-x"), "{}", "Event"); // seq 1, 2
    var never = OutboxEntry.pending(OutboxEntryId.of("dup-nack-y"), "{}", "Event"); // seq 3

    var firstPublished = new CountDownLatch(1);
    var publishCount = new AtomicLong(0);
    doAnswer(
            inv -> {
              long n = publishCount.incrementAndGet();
              if (n == 1) {
                firstPublished.countDown(); // batch 1: `retried` published (seq 1), then abandoned
              }
              if (n == 2) { // batch 2: `retried` republished (seq 2)
                listener.handleNack(2L, false); // fresh nack — settles `retried`
                listener.handleNack(1L, false); // stale seq, SAME id — must not count down again
              }
              return null; // n == 3: `never` (seq 3) is never confirmed
            })
        .when(channel)
        .basicPublish(
            anyString(),
            anyString(),
            anyBoolean(),
            any(AMQP.BasicProperties.class),
            any(byte[].class));

    abandonBatchLeavingSeqOutstanding(publisher, retried, firstPublished);

    var result = publisher.publishBatch(List.of(retried, never));

    assertTrue(result.failed().containsKey(OutboxEntryId.of("dup-nack-x")));
    assertInstanceOf(
        TimeoutException.class,
        result.failed().get(OutboxEntryId.of("dup-nack-y")),
        "double count-down for the duplicate nack would have released the latch early and"
            + " misreported the unconfirmed entry as nacked");
  }

  /**
   * Concurrency race for the {@code multiple=true} settlement path in {@link
   * RabbitMqOutboxPublisher#settleConfirms}: two threads race to claim overlapping seqNo ranges via
   * {@code handleAck(seq, true)}. Both iterate the same live view of {@code outstanding} and both
   * attempt {@code outstanding.remove(key, value)} on the same keys — the atomic-claim CAS this
   * test targets. Exactly one of the two racing removals for a given key must win (settle it) and
   * the other must lose and skip it, so every entry is confirmed exactly once with no
   * double-settlement and no drop.
   */
  @Test
  void settleConfirms_concurrent_multiple_acks_on_overlapping_range_claim_atomically()
      throws Exception {
    int batchSize = 200;
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(5))
            .build();
    var listener = captureListener();

    var entries = new ArrayList<OutboxEntry>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      entries.add(OutboxEntry.pending(OutboxEntryId.of("multi-race-" + i), "{}", "Event"));
    }

    var done = new AtomicBoolean(false);
    Runnable spin =
        () -> {
          // Deflake gate (pre-existing flake; fires under parallel `check` load): do not fabricate
          // multiple-acks until the batch has registered at least one seq. A fabricated ack whose
          // settleConfirms read a still-null (or previous) currentBatch, and whose headMap clear /
          // remove then consumed seqs the live batch registered in the meantime, silently dropped
          // those settles — the batch stalled to its confirm timeout. A real broker cannot produce
          // that interleaving: a genuine confirm exists only after ITS basicPublish, which
          // happens-after the batch holder's volatile write. Observing a registered seq (the
          // concurrent map read below) gives this racer that same happens-before edge — mirroring
          // how publishBatch_unsynchronized_confirm_thread_never_drops_a_confirm gates its spinner
          // on maxHandedOutSeq — while leaving the overlapping-range claim race fully exercised.
          while (!done.get() && publisher.outstandingSize() == 0) {
            Thread.onSpinWait();
          }
          while (!done.get()) {
            try {
              listener.handleAck(batchSize, true);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
    var t1 = new Thread(spin, "multi-ack-racer-1");
    var t2 = new Thread(spin, "multi-ack-racer-2");
    t1.start();
    t2.start();

    try {
      var result = publisher.publishBatch(entries);

      assertTrue(
          result.failed().isEmpty(),
          "no confirm may be double-claimed or dropped under a multiple=true race; failed="
              + result.failed().keySet());
      assertEquals(batchSize, result.confirmed().size());
      assertEquals(
          0, publisher.outstandingSize(), "every seq must be claimed exactly once by one racer");
    } finally {
      done.set(true);
      t1.join(5_000);
      t2.join(5_000);
      assertFalse(t1.isAlive(), "racer thread 1 must terminate");
      assertFalse(t2.isAlive(), "racer thread 2 must terminate");
    }
  }

  // ---------------------------------------------------------------------------
  // A channel that is no longer in confirm mode, or fails with an unchecked exception
  // ---------------------------------------------------------------------------

  private static final String NOT_IN_CONFIRM_MODE = "not in publisher-confirm mode";

  private static String repeat(String unit, int times) {
    return unit.repeat(times);
  }

  @Test
  void publishBatch_channelNotInConfirmMode_failsClosedWithoutPublishing() throws Exception {
    // A channel wrapper that replaced its closed channel with a fresh one hands out sequence
    // number 0: confirm mode and the listeners were lost with the old channel, so nothing
    // published now would ever be confirmed. Publishing anyway delivers the message, times out,
    // holds the claim and re-publishes it after every claim lease. Fail closed instead: publish
    // nothing, report a transport failure (nothing was handed off).
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(300))
            .build();
    when(channel.getNextPublishSeqNo()).thenReturn(0L);
    var e1 = OutboxEntry.pending(OutboxEntryId.of("ncm-1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("ncm-2"), "{}", "Event");

    var result = publisher.publishBatch(List.of(e1, e2));

    verify(channel, never()).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    assertTrue(result.confirmed().isEmpty());
    for (var entry : List.of(e1, e2)) {
      Exception failure = result.failed().get(entry.id());
      assertNotNull(failure, entry.id().value() + " must be failed");
      assertEquals(FailureKind.TRANSPORT, publisher.classifyFailure(failure), "got " + failure);
      assertInstanceOf(RabbitMqOutboxPublisher.NotInConfirmModeException.class, failure);
      assertTrue(failure.getMessage().contains(NOT_IN_CONFIRM_MODE), "got " + failure);
    }
    assertEquals(0, publisher.outstandingSize());
  }

  @Test
  void publish_channelNotInConfirmMode_throwsTransportWithoutPublishing() throws Exception {
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofMillis(300))
            .build();
    when(channel.getNextPublishSeqNo()).thenReturn(0L);

    Exception thrown =
        assertThrows(
            Exception.class,
            () -> publisher.publish(OutboxEntry.pending(OutboxEntryId.of("ncm"), "{}", "Event")));

    verify(channel, never()).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    assertInstanceOf(RabbitMqOutboxPublisher.NotInConfirmModeException.class, thrown);
    assertEquals(FailureKind.TRANSPORT, publisher.classifyFailure(thrown), "got " + thrown);
    assertTrue(thrown.getMessage().contains(NOT_IN_CONFIRM_MODE), "got " + thrown);
  }

  @Test
  void publishBatch_confirmModeLostMidBatch_keepsWhatWasPublished_andFailsTheRestClosed()
      throws Exception {
    var listenerRef = new AtomicReference<ConfirmListener>();
    var publisher =
        RabbitMqOutboxPublisher.builder()
            .channel(channel)
            .exchange("ex")
            .confirmTimeout(Duration.ofSeconds(1))
            .build();
    listenerRef.set(captureListener());
    // e1 goes out on the original channel (seq 1); the channel is then replaced (seq 0).
    var seqs = new java.util.ArrayDeque<>(List.of(1L));
    when(channel.getNextPublishSeqNo()).thenAnswer(inv -> seqs.isEmpty() ? 0L : seqs.poll());
    doAnswer(
            inv -> {
              deliverAck(listenerRef.get(), 1);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    var e1 = OutboxEntry.pending(OutboxEntryId.of("mid-1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("mid-2"), "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("mid-3"), "{}", "Event");

    var result = publisher.publishBatch(List.of(e1, e2, e3));

    verify(channel, times(1)).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    assertEquals(Set.of(e1.id()), result.confirmed());
    for (var entry : List.of(e2, e3)) {
      Exception failure = result.failed().get(entry.id());
      assertNotNull(failure, entry.id().value() + " must be failed");
      assertEquals(FailureKind.TRANSPORT, publisher.classifyFailure(failure), "got " + failure);
    }
    assertEquals(0, publisher.outstandingSize());
  }

  @Test
  void build_refusesAChannelThatDoesNotEnterConfirmMode() throws Exception {
    // A wrapper whose confirmSelect() does not reach a real channel leaves sequence number 0.
    // (doReturn: a when(channel.confirmSelect()) call would run the default stub and enter it.)
    doReturn(null).when(channel).confirmSelect();

    var thrown =
        assertThrows(
            IllegalStateException.class,
            () -> RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build());

    assertTrue(thrown.getMessage().contains(NOT_IN_CONFIRM_MODE), "got " + thrown);
    verify(channel, never()).addConfirmListener(any(ConfirmListener.class));
  }

  @Test
  void publishBatch_uncheckedExceptionFromBasicPublish_isReturnedAsAFailure_notThrown()
      throws Exception {
    // basicPublish may fail with an unchecked exception that is not a connection shutdown (a
    // channel wrapper's own, or a metrics/observation collector the client calls after writing
    // the message). It must come back as a per-entry outcome — the poll cycle must not abort with
    // the batch claimed. Whether that message was written is not provable, so it is held in
    // flight; the entries after it were never attempted, a transport failure.
    var publisher = buildBatchPublisher();
    var cause = new IllegalStateException("channel is closed");
    doThrow(cause).when(channel).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    var e1 = OutboxEntry.pending(OutboxEntryId.of("rt-1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("rt-2"), "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("rt-3"), "{}", "Event");

    var result = assertDoesNotThrow(() -> publisher.publishBatch(List.of(e1, e2, e3)));

    verify(channel, times(1)).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    assertTrue(result.confirmed().isEmpty());
    Exception first = result.failed().get(e1.id());
    assertNotNull(first, "rt-1 must be failed");
    assertSame(cause, first.getCause(), "the channel's exception must be the cause");
    assertEquals(FailureKind.IN_FLIGHT, publisher.classifyFailure(first), "got " + first);
    for (var entry : List.of(e2, e3)) {
      Exception failure = result.failed().get(entry.id());
      assertNotNull(failure, entry.id().value() + " must be failed");
      assertInstanceOf(RabbitMqOutboxPublisher.ChannelFailureException.class, failure);
      assertSame(cause, failure.getCause(), "the channel's exception must be the cause");
      assertEquals(FailureKind.TRANSPORT, publisher.classifyFailure(failure), "got " + failure);
    }
    assertEquals(0, publisher.outstandingSize(), "the failed publish's seq must be unregistered");
  }

  @Test
  void publishBatch_uncheckedExceptionFromGetNextPublishSeqNo_isReturnedAsTransportFailure()
      throws Exception {
    // A wrapper checks its channel on every call, so the first call of a publish — reading the
    // sequence number — is where it fails.
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var cause = new IllegalStateException("channel is closed");
    when(channel.getNextPublishSeqNo()).thenThrow(cause);
    var e1 = OutboxEntry.pending(OutboxEntryId.of("rt-seq-1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("rt-seq-2"), "{}", "Event");

    var result = assertDoesNotThrow(() -> publisher.publishBatch(List.of(e1, e2)));

    verify(channel, never()).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    for (var entry : List.of(e1, e2)) {
      Exception failure = result.failed().get(entry.id());
      assertNotNull(failure, entry.id().value() + " must be failed");
      assertInstanceOf(RabbitMqOutboxPublisher.ChannelFailureException.class, failure);
      assertSame(cause, failure.getCause(), "the channel's exception must be the cause");
      assertEquals(FailureKind.TRANSPORT, publisher.classifyFailure(failure), "got " + failure);
    }
  }

  @Test
  void publish_uncheckedExceptionFromBasicPublish_throwsAnInFlightFailureCausedByIt()
      throws Exception {
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var cause = new IllegalStateException("channel is closed");
    doThrow(cause).when(channel).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

    Exception thrown =
        assertThrows(
            Exception.class,
            () -> publisher.publish(OutboxEntry.pending(OutboxEntryId.of("rt"), "{}", "Event")));

    assertInstanceOf(RabbitMqOutboxPublisher.UnconfirmedPublishException.class, thrown);
    assertSame(cause, thrown.getCause(), "got " + thrown);
    assertEquals(FailureKind.IN_FLIGHT, publisher.classifyFailure(thrown), "got " + thrown);
  }

  /**
   * Models the amqp-client's own check: the message id, the type and the exchange/routing key are
   * AMQP short strings (at most 255 UTF-8 bytes). The real channel throws only after it has taken
   * the next publish sequence number, so the broker's confirm numbering and the channel's drift
   * apart — such an entry must never reach the channel.
   */
  private void rejectOverlongShortStrings(AtomicReference<ConfirmListener> listenerRef)
      throws IOException {
    var seqCounter = new AtomicLong(1);
    stubSeqNos(seqCounter);
    doAnswer(
            inv -> {
              long seq = seqCounter.get() - 1;
              AMQP.BasicProperties props = inv.getArgument(3);
              for (String s : List.of(props.getMessageId(), props.getType())) {
                if (s.getBytes(StandardCharsets.UTF_8).length > 255) {
                  throw new IllegalArgumentException("Short string too long");
                }
              }
              deliverAck(listenerRef.get(), seq);
              return null;
            })
        .when(channel)
        .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
  }

  @Test
  void publishBatch_entryWhosePayloadTypeExceedsTheShortStringLimit_isAnEntryFailure()
      throws Exception {
    var listenerRef = new AtomicReference<ConfirmListener>();
    rejectOverlongShortStrings(listenerRef);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    listenerRef.set(captureListener());
    // 128 characters, 256 UTF-8 bytes: the limit is in bytes.
    var tooLong = OutboxEntry.pending(OutboxEntryId.of("long-type"), "{}", repeat("é", 128));
    var fine = OutboxEntry.pending(OutboxEntryId.of("fine"), "{}", repeat("t", 255));

    var result = publisher.publishBatch(List.of(tooLong, fine));

    verify(channel, times(1)).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    assertEquals(Set.of(fine.id()), result.confirmed());
    Exception failure = result.failed().get(tooLong.id());
    assertInstanceOf(RabbitMqOutboxPublisher.UnpublishableEntryException.class, failure);
    assertEquals(FailureKind.ENTRY, publisher.classifyFailure(failure), "got " + failure);
  }

  @Test
  void publish_entryWhoseIdExceedsTheShortStringLimit_throwsAnEntryFailure() throws Exception {
    var listenerRef = new AtomicReference<ConfirmListener>();
    rejectOverlongShortStrings(listenerRef);
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    listenerRef.set(captureListener());

    Exception thrown =
        assertThrows(
            Exception.class,
            () ->
                publisher.publish(
                    OutboxEntry.pending(OutboxEntryId.of(repeat("i", 256)), "{}", "Event")));

    verify(channel, never()).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    assertInstanceOf(RabbitMqOutboxPublisher.UnpublishableEntryException.class, thrown);
    assertEquals(FailureKind.ENTRY, publisher.classifyFailure(thrown), "got " + thrown);
  }

  @Test
  void classifyFailure_newChannelFailuresAreTransport_overLongEntryIsEntry_alsoInACauseChain()
      throws Exception {
    var publisher = RabbitMqOutboxPublisher.builder().channel(channel).exchange("ex").build();
    var notInConfirmMode = new RabbitMqOutboxPublisher.NotInConfirmModeException("seq 0");
    var channelFailure =
        new RabbitMqOutboxPublisher.ChannelFailureException(
            "failed", new IllegalStateException("closed"));
    var overLong = new RabbitMqOutboxPublisher.UnpublishableEntryException("too long");
    assertEquals(FailureKind.TRANSPORT, publisher.classifyFailure(notInConfirmMode));
    assertEquals(FailureKind.TRANSPORT, publisher.classifyFailure(channelFailure));
    assertEquals(FailureKind.ENTRY, publisher.classifyFailure(overLong));
    assertEquals(
        FailureKind.TRANSPORT, publisher.classifyFailure(new RuntimeException(notInConfirmMode)));
    assertEquals(FailureKind.ENTRY, publisher.classifyFailure(new RuntimeException(overLong)));
  }

  @Test
  void builder_rejectsExchangeAndRoutingKeyLongerThanTheShortStringLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RabbitMqOutboxPublisher.builder().exchange(repeat("é", 128)));
    assertThrows(
        IllegalArgumentException.class,
        () -> RabbitMqOutboxPublisher.builder().routingKey(repeat("k", 256)));
    assertDoesNotThrow(
        () ->
            RabbitMqOutboxPublisher.builder()
                .exchange(repeat("x", 255))
                .routingKey(repeat("k", 255)));
  }
}
