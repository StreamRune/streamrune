package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.SagaEnvelopes;

/**
 * A replay of a command dead-letter entry supplies no provenance: the events of the replayed
 * command carry no saga origin and no causation id, whatever the entry's columns hold and whatever
 * the thread that asked for the replay has bound.
 *
 * <p>A dead-letter entry stores the correlation id, the user and the trace id of the failed
 * command. A correlation id is a value a client chooses, a saga id included, so the replay must not
 * read it as a statement of who caused the command. The replay binds the entry's request context
 * and the replay marker; it runs on the runner's poll thread or on a thread of its own, where no
 * dispatch cause is bound.
 */
class DeadLetterReplayProvenanceTest {

  /** Stores one {@link Paid} per command. */
  public record Pay(String paymentId) implements Command {}

  /** The event of {@link Pay}. */
  public record Paid(String paymentId) implements DomainEvent {}

  private record Payment(int payments) implements AggregateState {}

  private record OrderSagaState(SagaStatus status) implements SagaState {}

  private static final AggregateType PAYMENT = AggregateType.of("payment");
  private static final SagaId SAGA_ID = SagaId.of("order-saga-42");

  /** A decider that fails with an infrastructure fault while {@code failing} is set. */
  private static Decider<Pay, Payment, Paid> decider(AtomicBoolean failing) {
    return new Decider<>() {
      @Override
      public Payment initialState() {
        return new Payment(0);
      }

      @Override
      public List<Paid> decide(Pay command, Payment state) {
        if (failing.get()) {
          throw new IllegalStateException("payment provider unreachable");
        }
        return List.of(new Paid(command.paymentId()));
      }

      @Override
      public Payment evolve(Payment state, Paid event) {
        return new Payment(state.payments() + 1);
      }
    };
  }

  /** The production bus, an inbox and a dead letter queue, and the runner that replays it. */
  private static final class Harness implements AutoCloseable {
    final ObjectMapper mapper = new ObjectMapper();
    final AtomicBoolean failing = new AtomicBoolean(false);
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    final InMemoryDeadLetterQueue dlq = new InMemoryDeadLetterQueue();
    final VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(mapper)
            .commandInbox(inbox)
            .deadLetterQueue(dlq)
            // The decider's fault is an IllegalStateException; every failure is dead-letterable.
            .dlqEligible(failure -> true)
            .register(PAYMENT, Pay.class, pay -> AggregateId.of(pay.paymentId()), decider(failing))
            .build();
    final DeadLetterRetryRunner runner = runner(Duration.ofMinutes(1));

    DeadLetterRetryRunner runner(Duration pollInterval) {
      return DeadLetterRetryRunner.builder()
          .deadLetterQueue(dlq)
          .commandBus(bus)
          .objectMapper(mapper)
          .policy(new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false))
          .pollInterval(pollInterval)
          .registerCommand(Pay.class)
          .build();
    }

    /** Publishes an entry for {@code Pay(paymentId)} holding the given request-context columns. */
    void publishEntry(
        String paymentId, String correlationId, String userId, String traceId, String key)
        throws Exception {
      dlq.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              mapper.writeValueAsString(new Pay(paymentId)),
              Pay.class.getName(),
              CommandId.of("cmd-" + paymentId),
              StreamId.of(PAYMENT, AggregateId.of(paymentId)),
              IllegalStateException.class.getName(),
              "payment provider unreachable",
              1,
              Instant.now(),
              correlationId == null ? null : CorrelationId.of(correlationId),
              userId == null ? null : UserId.of(userId),
              traceId == null ? null : TraceId.of(traceId),
              key == null ? null : IdempotencyKey.of(key)));
    }

    List<EventEnvelope> stored(String paymentId) {
      return eventStore.load(StreamId.of(PAYMENT, AggregateId.of(paymentId))).events();
    }

    @Override
    public void close() {
      runner.close();
      bus.close();
    }
  }

  /** The reply to a saga's own command, as a listener reads it: origin at hop 0. */
  private static EventEnvelope sagaCausedEvent() {
    return SagaEnvelopes.causedBy(
        OrderSagaState.class,
        SAGA_ID,
        StreamId.of(PAYMENT, AggregateId.of("pay-initiated")),
        new Paid("pay-initiated"));
  }

  /** An event a user's request caused: no origin. */
  private static EventEnvelope requestCausedEvent() {
    return new EventEnvelope(
        GlobalOffset.of(3),
        StreamId.of(PAYMENT, AggregateId.of("pay-requested")),
        Version.initial(),
        EventType.fromClass(Paid.class),
        new Paid("pay-requested"),
        new EventMetadata(
            EventId.of("evt-request-1"),
            CommandId.of("cmd-request-1"),
            TraceId.of("trace-7"),
            null,
            CorrelationId.of("client-corr-7"),
            null,
            UserId.of("alice"),
            Instant.now()));
  }

  private static void assertNoProvenance(List<EventEnvelope> stored) {
    assertEquals(1, stored.size());
    assertNull(stored.getFirst().metadata().sagaOrigin());
    assertNull(stored.getFirst().metadata().causationId());
  }

  @TestFactory
  Stream<DynamicTest> aReplayedEntry_storesNoOriginAndNoCausationId() {
    // The correlation column may hold anything a failed command ran under: a client's id, the id
    // of a saga (a client may send one, and saga dispatch binds one), or nothing at all.
    List<String> correlationIds = Arrays.asList("client-corr-7", SAGA_ID.value(), null);
    List<String> users = Arrays.asList(null, "alice");
    List<String> traces = Arrays.asList(null, "trace-7");
    List<String> keys = Arrays.asList(null, "saga:" + SAGA_ID.value() + ":7:0");

    List<DynamicTest> tests = new ArrayList<>();
    for (String correlationId : correlationIds) {
      for (String user : users) {
        for (String trace : traces) {
          for (String key : keys) {
            tests.add(
                dynamicTest(
                    "correlationId="
                        + correlationId
                        + ", user="
                        + user
                        + ", trace="
                        + trace
                        + ", key="
                        + key,
                    () -> {
                      try (Harness harness = new Harness()) {
                        harness.publishEntry("pay-1", correlationId, user, trace, key);

                        harness.runner.processBatch();

                        assertTrue(harness.dlq.all().isEmpty(), "the replay succeeded");
                        List<EventEnvelope> stored = harness.stored("pay-1");
                        assertNoProvenance(stored);
                        // What the entry does restore, when it holds a correlation id at all.
                        EventMetadata metadata = stored.getFirst().metadata();
                        if (correlationId != null) {
                          assertEquals(correlationId, metadata.correlationId().value());
                          assertEquals(user, value(metadata.userId()));
                          assertEquals(trace, value(metadata.traceId()));
                        } else {
                          assertNull(metadata.userId());
                          assertNull(metadata.traceId());
                        }
                      }
                    }));
          }
        }
      }
    }
    return tests.stream();
  }

  private static String value(Object id) {
    return id == null ? null : id.toString();
  }

  @Test
  void retry_doesNotInheritTheCallersCause() throws Exception {
    try (Harness harness = new Harness()) {
      harness.publishEntry("pay-1", SAGA_ID.value(), "alice", "trace-7", null);
      harness.publishEntry("pay-2", SAGA_ID.value(), null, null, null);
      harness.publishEntry("pay-3", "client-corr-7", null, null, null);

      // An operator's retry issued from a listener that continues a saga-caused event.
      StreamRuneContext.continuing(sagaCausedEvent())
          .run(
              () -> {
                assertEquals(1, StreamRuneContext.cause().sagaOrigin().hop());
                assertTrue(harness.runner.retry(CommandId.of("cmd-pay-1")));
              });
      assertNoProvenance(harness.stored("pay-1"));
      assertEquals(UserId.of("alice"), harness.stored("pay-1").getFirst().metadata().userId());

      // From inside a saga's own dispatch: the cause and the saga flag both stay behind.
      StreamRuneContext.DispatchCause sagaDispatch =
          new StreamRuneContext.DispatchCause(
              CausationId.of("evt-trigger"), sagaCausedEvent().metadata().sagaOrigin());
      ScopedValue.where(StreamRuneContext.CAUSE, sagaDispatch)
          .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
          .run(() -> assertTrue(harness.runner.retry(CommandId.of("cmd-pay-2"))));
      assertNoProvenance(harness.stored("pay-2"));

      // From a task that carried a cause across a thread boundary.
      Runnable retry = () -> assertTrue(harness.runner.retry(CommandId.of("cmd-pay-3")));
      Runnable wrapped =
          StreamRuneContext.continuing(sagaCausedEvent()).call(() -> StreamRuneContext.wrap(retry));
      Thread thread = new Thread(wrapped);
      thread.start();
      thread.join();
      assertNoProvenance(harness.stored("pay-3"));

      assertTrue(harness.dlq.all().isEmpty());
    }
  }

  @Test
  void aScheduledReplay_doesNotInheritTheCauseOfTheThreadThatStartedTheRunner() throws Exception {
    try (Harness harness = new Harness();
        DeadLetterRetryRunner polling = harness.runner(Duration.ofMillis(5))) {
      harness.publishEntry("pay-1", SAGA_ID.value(), null, null, null);

      // The poll thread is started while a continuation's cause is bound.
      StreamRuneContext.continuing(sagaCausedEvent()).run(polling::start);

      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> !harness.stored("pay-1").isEmpty());
      assertNoProvenance(harness.stored("pay-1"));
    }
  }

  /**
   * A failed continuation is dead-lettered for neither kind of causing event, so no replay can
   * store its events without the causing event. The listener's redelivery is the one retry, and its
   * events name the event it reacted to — and the saga, when that event has an origin.
   */
  @TestFactory
  Stream<DynamicTest> aFailedContinuation_leavesNothingToReplay() {
    Map<String, EventEnvelope> causes =
        Map.of(
            "an event a saga caused", sagaCausedEvent(),
            "an event a request caused", requestCausedEvent());
    return causes.entrySet().stream()
        .map(
            entry ->
                dynamicTest(
                    entry.getKey(),
                    () -> {
                      try (Harness harness = new Harness()) {
                        EventEnvelope cause = entry.getValue();
                        IdempotencyKey key =
                            IdempotencyKey.of("payments:" + cause.metadata().eventId().value());
                        harness.failing.set(true);

                        assertThrows(
                            IllegalStateException.class,
                            () ->
                                StreamRuneContext.continuing(cause)
                                    .run(() -> harness.bus.execute(new Pay("pay-1"), key)));
                        assertTrue(harness.dlq.all().isEmpty());
                        harness.failing.set(false);
                        harness.runner.processBatch();
                        assertTrue(harness.stored("pay-1").isEmpty());

                        StreamRuneContext.continuing(cause)
                            .run(() -> harness.bus.execute(new Pay("pay-1"), key));

                        List<EventEnvelope> stored = harness.stored("pay-1");
                        assertEquals(1, stored.size());
                        EventMetadata metadata = stored.getFirst().metadata();
                        assertEquals(
                            CausationId.of(cause.metadata().eventId().value()),
                            metadata.causationId());
                        assertEquals(cause.metadata().correlationId(), metadata.correlationId());
                        if (cause.metadata().sagaOrigin() == null) {
                          assertNull(metadata.sagaOrigin());
                        } else {
                          assertEquals(SAGA_ID, metadata.sagaOrigin().sagaId());
                          assertEquals(1, metadata.sagaOrigin().hop());
                        }
                      }
                    }));
  }

  /**
   * What a listener gives up when it dispatches outside {@code continuing} to have a failed command
   * parked in the dead letter queue: the entry has no place for the event the listener reacted to,
   * and the replay stores the command's events as causation roots. A later dispatch under the same
   * key finds the command committed and stores nothing.
   */
  @Test
  void aCommandDispatchedOutsideContinuing_isReplayedWithoutACausationId() {
    try (Harness harness = new Harness()) {
      EventEnvelope reactedTo = requestCausedEvent();
      IdempotencyKey key = IdempotencyKey.of("payments:" + reactedTo.metadata().eventId().value());
      var plain =
          new StreamRuneContext.RequestContext(
              reactedTo.metadata().traceId(),
              null,
              reactedTo.metadata().correlationId(),
              Instant.now(),
              Map.of());
      harness.failing.set(true);

      assertThrows(
          IllegalStateException.class,
          () ->
              ScopedValue.where(StreamRuneContext.CURRENT, plain)
                  .run(() -> harness.bus.execute(new Pay("pay-1"), key)));
      assertEquals(1, harness.dlq.all().size());
      assertEquals(key, harness.dlq.all().getFirst().idempotencyKey());

      harness.failing.set(false);
      harness.runner.processBatch();

      List<EventEnvelope> stored = harness.stored("pay-1");
      assertNoProvenance(stored);
      assertEquals(CorrelationId.of("client-corr-7"), stored.getFirst().metadata().correlationId());
      assertNull(stored.getFirst().metadata().userId());
      assertTrue(harness.dlq.all().isEmpty());

      // The same key again, this time inside a continuation: answered from the inbox.
      StreamRuneContext.continuing(reactedTo).run(() -> harness.bus.execute(new Pay("pay-1"), key));
      assertEquals(1, harness.stored("pay-1").size());
    }
  }
}
