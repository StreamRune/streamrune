package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterQueue.DeadLetterEntry;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.ReservedCorrelationIdException;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.SagaEnvelopes;

/**
 * No command the bus dead-letters carries a correlation id in the reserved form.
 *
 * <p>A dead-letter entry stores the correlation id of the context its command failed under, in a
 * column as wide as the request edge's bound ({@code dead_letter_queue.correlation_id
 * VARCHAR(255)}). The reserved form saga dispatch binds is the saga id behind a 16-character
 * prefix, so the one of a saga id at its own 255-character bound does not fit that column. It never
 * gets there: a reserved correlation id is bound together with a saga origin (saga dispatch, a
 * continuation of a saga-caused event), a command dispatched with a dispatch cause bound is not
 * dead-lettered, and a reserved correlation id bound without an origin is refused before the
 * command runs. Each failing command below fails with an error the queue would otherwise accept.
 */
class ReservedCorrelationIdDeadLetterTest {

  /** The command of every case: its decider always fails with an infrastructure fault. */
  public record Charge(String paymentId) implements Command {}

  record Charged(String paymentId) implements DomainEvent {}

  record Payment() implements AggregateState {}

  record FulfillmentState(SagaStatus status) implements SagaState {}

  private static final AggregateType PAYMENT = AggregateType.of("payment");

  /** A saga id at its bound: its reserved correlation id is 271 characters long. */
  private static final SagaId LONGEST_SAGA = SagaId.of("s".repeat(SagaId.MAX_LENGTH));

  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
  private final InMemoryDeadLetterQueue deadLetters = new InMemoryDeadLetterQueue();
  private final VirtualThreadCommandBus bus =
      VirtualThreadCommandBus.builder()
          .eventStore(eventStore)
          .locker(new LocalStripedLocker(16))
          .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
          .objectMapper(new ObjectMapper())
          .commandInbox(inbox)
          .deadLetterQueue(deadLetters)
          .register(
              PAYMENT,
              Charge.class,
              charge -> AggregateId.of(charge.paymentId()),
              new Decider<Charge, Payment, Charged>() {
                @Override
                public Payment initialState() {
                  return new Payment();
                }

                @Override
                public List<Charged> decide(Charge command, Payment state) {
                  throw new IllegalStateException("payment provider unreachable");
                }

                @Override
                public Payment evolve(Payment state, Charged event) {
                  return state;
                }
              })
          .build();

  @AfterEach
  void close() {
    bus.close();
  }

  private void assertFailsAtTheProvider(Runnable dispatch) {
    assertThatThrownBy(dispatch::run)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("payment provider unreachable");
  }

  private static void assertNoEntryIsReserved(List<DeadLetterEntry> entries) {
    assertThat(entries)
        .allSatisfy(
            entry -> {
              if (entry.correlationId() != null) {
                assertThat(IdConstraints.isReserved(entry.correlationId().value()))
                    .as("correlation id of the entry of %s", entry.commandId())
                    .isFalse();
                assertThat(entry.correlationId().value())
                    .hasSizeLessThanOrEqualTo(IdConstraints.MAX_LENGTH);
              }
            });
  }

  /** A stored reply to a command the saga dispatched itself, as a subscription delivers it. */
  private EventEnvelope storedSagaCausedEvent(SagaId sagaId) {
    StreamId stream = StreamId.of(AggregateType.of("order"), AggregateId.of("o-1"));
    eventStore.append(
        stream,
        List.of(
            SagaEnvelopes.causedBy(FulfillmentState.class, sagaId, stream, new Charged("pay-0"))),
        Version.initial());
    return eventStore.readGlobalStream(GlobalOffset.initial(), 1).getFirst();
  }

  @Test
  void aFailedSagaCommand_isNotDeadLettered_whateverTheLengthOfTheSagaId() {
    for (SagaId sagaId : List.of(SagaId.of("order-saga-42"), LONGEST_SAGA)) {
      SagaOrigin origin = SagaOrigins.of(sagaId.value());

      assertFailsAtTheProvider(
          () ->
              SagaCommandDispatch.dispatch(
                  bus,
                  origin,
                  EventId.of("evt-trigger"),
                  new Charge("pay-1"),
                  new IdempotencyKey("saga:" + sagaId.value() + ":7:0")));
    }

    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void aFailedContinuationOfASagaCausedEvent_isNotDeadLettered_whateverTheLengthOfTheSagaId() {
    EventEnvelope reply = storedSagaCausedEvent(LONGEST_SAGA);
    assertThat(reply.metadata().correlationId().value()).hasSize(271).startsWith("streamrune:");

    assertFailsAtTheProvider(
        () ->
            StreamRuneContext.continuing(reply)
                .run(() -> bus.execute(new Charge("pay-1"), IdempotencyKey.of("capture:pay-1"))));
    assertThatThrownBy(
            () ->
                StreamRuneContext.continuing(reply)
                    .call(() -> bus.executeAsync(new Charge("pay-1")))
                    .join())
        .isInstanceOf(CompletionException.class)
        .hasRootCauseMessage("payment provider unreachable");

    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void aReservedCorrelationIdBoundWithoutAnOrigin_isRefusedAndNotDeadLettered() {
    var copiedByHand =
        new StreamRuneContext.RequestContext(
            null, null, SagaOrigins.of("order-saga-42").correlationId(), Instant.now(), Map.of());

    assertThatThrownBy(
            () ->
                ScopedValue.where(StreamRuneContext.CURRENT, copiedByHand)
                    .run(() -> bus.execute(new Charge("pay-1"))))
        .isInstanceOf(ReservedCorrelationIdException.class)
        .hasMessageContaining("reserved 'streamrune:' prefix");

    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void theCommandsThatAreDeadLettered_carryTheCallersCorrelationIdOrNone() {
    // A request's command: the correlation id the request edge accepted.
    var request =
        StreamRuneContext.RequestContext.fromRequest(
            null, null, CorrelationId.of("c".repeat(255)), Instant.now(), Map.of());
    assertFailsAtTheProvider(
        () ->
            ScopedValue.where(StreamRuneContext.CURRENT, request)
                .run(() -> bus.execute(new Charge("pay-request"))));
    // A continuation of an event a request caused inherits the client's correlation id, and is
    // not dead-lettered: its cause is bound.
    EventEnvelope requestCaused =
        new EventEnvelope(
            GlobalOffset.of(3),
            StreamId.of(AggregateType.of("order"), AggregateId.of("o-2")),
            Version.initial(),
            EventType.fromClass(Charged.class),
            new Charged("pay-0"),
            new EventMetadata(
                EventId.of("evt-request-1"),
                CommandId.of("cmd-request-1"),
                null,
                null,
                CorrelationId.of("client-corr-7"),
                null,
                null,
                Instant.now()));
    assertFailsAtTheProvider(
        () ->
            StreamRuneContext.continuing(requestCaused)
                .run(() -> bus.execute(new Charge("pay-continued"))));
    // A background command: no context, no correlation id.
    assertFailsAtTheProvider(() -> bus.execute(new Charge("pay-background")));

    assertThat(deadLetters.all())
        .extracting(entry -> entry.correlationId() == null ? null : entry.correlationId().value())
        .containsExactlyInAnyOrder("c".repeat(255), null);
    assertNoEntryIsReserved(deadLetters.all());
  }

  /**
   * The queue is a table an operator or another writer can insert into. An entry written that way
   * with a reserved correlation id is replayed under that id and no saga origin — a replay binds
   * the entry's context and never a cause — so the bus refuses the replay like any other command in
   * that state: nothing is stored, and the entry counts one failed attempt per cycle until the
   * retry policy gives it up.
   */
  @Test
  void anEntryWrittenByOtherMeansWithAReservedCorrelationId_isRefusedOnEveryReplay() {
    CommandId commandId = CommandId.of("cmd-written-by-hand");
    deadLetters.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{\"paymentId\":\"pay-9\"}",
            Charge.class.getName(),
            commandId,
            StreamId.of(PAYMENT, AggregateId.of("pay-9")),
            IllegalStateException.class.getName(),
            "payment provider unreachable",
            1,
            Instant.now(),
            SagaOrigins.of("order-saga-42").correlationId(),
            null,
            null,
            null));

    try (DeadLetterRetryRunner replay =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(deadLetters)
            .commandBus(bus)
            .objectMapper(new ObjectMapper())
            .policy(new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false))
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(Charge.class)
            .build()) {
      replay.processBatch();
      replay.processBatch();
    }

    assertThat(eventStore.readGlobalStream(GlobalOffset.initial(), 10)).isEmpty();
    assertThat(deadLetters.all())
        .singleElement()
        .satisfies(
            entry -> {
              assertThat(entry.commandId()).isEqualTo(commandId);
              assertThat(entry.dlqAttempts()).isEqualTo(2);
            });
  }
}
