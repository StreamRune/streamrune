package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Audit;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Audited;
import org.streamrune.runtime.SagaDispatchProvenanceTest.OneEventDecider;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Opened;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Reserve;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Reserved;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * One step of a saga with a listener in between, on the production command bus: the saga's command
 * stores a reply, a listener reacts to that reply with a command of its own inside {@link
 * StreamRuneContext#continuing(EventEnvelope)}, and the event of that command comes back to the
 * saga.
 *
 * <p>The saga declares no routing that could bring it back: its {@code correlateByEventData} throws
 * when it is asked. The reply and the listener's event reach the saga by the origin the command bus
 * wrote into them — on live delivery, on a second delivery of the whole log, and when the saga
 * dead-letter replayer feeds a quarantined one.
 */
class SagaContinuationRoutingTest {

  private static final SagaType TYPE = SagaType.fromClass(FlowState.class);

  record FlowState(SagaStatus status, String orderId, int audits) implements SagaState {}

  private static SagaId sagaOf(String orderId) {
    return SagaId.of("order-saga-" + orderId);
  }

  /** Reserves on the start event and completes on the listener's {@link Audited}. */
  static final class ReserveThenAuditSaga implements SagaOrchestrator<FlowState> {
    volatile boolean poisonAudited;

    @Override
    public Class<FlowState> stateType() {
      return FlowState.class;
    }

    @Override
    public FlowState initialState(SagaId sagaId) {
      return new FlowState(SagaStatus.STARTED, null, 0);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Opened;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return sagaOf(((Opened) event.event()).orderId());
    }

    @Override
    public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
      throw new IllegalStateException("every event of this flow carries the saga's origin");
    }

    @Override
    public FlowState evolve(FlowState state, EventEnvelope event) {
      return switch (event.event()) {
        case Opened opened -> new FlowState(SagaStatus.RUNNING, opened.orderId(), 0);
        case Audited _ -> {
          if (poisonAudited) {
            throw new IllegalStateException("evolve refuses this event");
          }
          yield new FlowState(SagaStatus.COMPLETED, state.orderId(), state.audits() + 1);
        }
        default -> state;
      };
    }

    @Override
    public List<SagaCommand> handle(FlowState state, EventEnvelope event) {
      return event.event() instanceof Opened opened
          ? List.of(SagaCommand.of(new Reserve(opened.orderId()), AggregateId.of(opened.orderId())))
          : List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FlowState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final class Harness implements AutoCloseable {
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemorySagaStore sagaStore = new InMemorySagaStore();
    final InMemoryEventStore eventStore =
        new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(sagaStore);
    final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(sagaStore);
    final ReserveThenAuditSaga saga = new ReserveThenAuditSaga();
    final VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .commandInbox(inbox)
            .register(
                AggregateType.of("reservation"),
                Reserve.class,
                c -> AggregateId.of(c.orderId()),
                new OneEventDecider<Reserve>(c -> new Reserved(c.orderId())))
            .register(
                AggregateType.of("audit"),
                Audit.class,
                c -> AggregateId.of(c.orderId()),
                new OneEventDecider<Audit>(c -> new Audited(c.orderId())))
            .build();
    final SagaRunner<FlowState> runner =
        SagaRunner.<FlowState>builder()
            .orchestrator(saga)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetters)
            .build();
    final SagaDeadLetterReplayer replayer =
        SagaDeadLetterReplayer.builder()
            .sagaDeadLetterStore(deadLetters)
            .sagaStore(sagaStore)
            .eventStore(eventStore)
            .sagaRunner(runner)
            .build();

    /** Reacts to the saga's reply with a keyed command of its own, continuing the reply. */
    final EventListener processManager =
        batch -> {
          for (EventEnvelope event : batch) {
            if (event.event() instanceof Reserved reserved) {
              StreamRuneContext.continuing(event)
                  .run(
                      () ->
                          bus.execute(
                              new Audit(reserved.orderId()),
                              IdempotencyKey.of("audit:" + event.metadata().eventId().value())));
            }
          }
        };

    private GlobalOffset checkpoint = GlobalOffset.initial();

    /** Stores the start event as a request does: no origin, a correlation id of its own. */
    void open(String orderId) {
      StreamId streamId = StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + orderId));
      eventStore.append(
          streamId,
          List.of(
              new EventEnvelope(
                  GlobalOffset.initial(),
                  streamId,
                  Version.initial(),
                  EventType.fromClass(Opened.class),
                  new Opened(orderId),
                  new EventMetadata(
                      EventId.of("evt-" + UUID.randomUUID()),
                      CommandId.of("cmd-" + UUID.randomUUID()),
                      null,
                      null,
                      CorrelationId.of("request-" + UUID.randomUUID()),
                      null,
                      null,
                      Instant.now()))),
          Version.initial());
    }

    /** Delivers everything stored since the last call to the runner and then to the listener. */
    void deliver() {
      checkpoint = deliverFrom(checkpoint);
    }

    /** Delivers the whole log again, from its first event. */
    void deliverEverythingAgain() {
      deliverFrom(GlobalOffset.initial());
    }

    private GlobalOffset deliverFrom(GlobalOffset from) {
      GlobalOffset at = from;
      while (true) {
        List<EventEnvelope> batch = eventStore.readGlobalStream(at, 100);
        if (batch.isEmpty()) {
          return at;
        }
        runner.asEventListener().onEvents(batch);
        processManager.onEvents(batch);
        at = batch.getLast().globalOffset();
      }
    }

    LoadedSaga<FlowState> row(String orderId) {
      return sagaStore.load(sagaOf(orderId), TYPE, FlowState.class).orElseThrow();
    }

    List<EventEnvelope> stored(Class<? extends DomainEvent> type) {
      return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
          .filter(e -> type.isInstance(e.event()))
          .toList();
    }

    @Override
    public void close() {
      bus.close();
    }
  }

  @Test
  void theEventOfAContinuation_reachesTheSagaItsOriginNames_withNoRoutingOfTheApplications() {
    try (var h = new Harness()) {
      h.open("42");
      h.deliver();

      assertThat(h.row("42").status()).isEqualTo(SagaStatus.COMPLETED);
      EventEnvelope reserved = h.stored(Reserved.class).getFirst();
      assertThat(reserved.metadata().sagaOrigin()).isEqualTo(new SagaOrigin(TYPE, sagaOf("42"), 0));
      assertThat(h.stored(Audited.class))
          .singleElement()
          .satisfies(
              audited -> {
                assertThat(audited.metadata().sagaOrigin())
                    .isEqualTo(new SagaOrigin(TYPE, sagaOf("42"), 1));
                assertThat(audited.metadata().causationId())
                    .isEqualTo(CausationId.of(reserved.metadata().eventId().value()));
                assertThat(h.runner.resolveTarget(audited))
                    .contains(new SagaRunner.ReplayTarget(sagaOf("42"), false));
              });
      assertThat(h.deadLetters.all()).isEmpty();
    }
  }

  @Test
  void twoSagasOfOneType_eachReceivesTheContinuationOfItsOwnReply() {
    try (var h = new Harness()) {
      h.open("1");
      h.open("2");
      h.deliver();

      for (String order : List.of("1", "2")) {
        assertThat(h.row(order).status()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(h.row(order).state()).isEqualTo(new FlowState(SagaStatus.COMPLETED, order, 1));
      }
      assertThat(h.stored(Audited.class))
          .extracting(e -> e.metadata().sagaOrigin().sagaId())
          .containsExactlyInAnyOrder(sagaOf("1"), sagaOf("2"));
      assertThat(h.deadLetters.all()).isEmpty();
    }
  }

  /**
   * A second delivery of the whole log — the start event, the saga's reply and the listener's event
   * — routes each event to the saga it reached the first time and stores nothing: the saga's
   * command and the listener's command are answered from the command inbox.
   */
  @Test
  void aSecondDeliveryOfTheWholeLog_routesEachEventToTheSameSaga_andStoresNothing() {
    try (var h = new Harness()) {
      h.open("42");
      h.deliver();
      LoadedSaga<FlowState> completed = h.row("42");
      int storedEvents = h.eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).size();

      h.deliverEverythingAgain();

      assertThat(h.row("42")).isEqualTo(completed);
      assertThat(h.eventStore.readGlobalStream(GlobalOffset.initial(), 1_000))
          .hasSize(storedEvents);
      assertThat(h.deadLetters.all()).isEmpty();
    }
  }

  /**
   * The listener's event poisons the saga and is quarantined under it. The replayer reads the event
   * back from the store and feeds it to the saga its stored origin names; the application's routing
   * method, which throws, is not asked on the replay either.
   */
  @Test
  void aQuarantinedContinuationEvent_isFedByTheReplayerToTheSagaItsStoredOriginNames() {
    try (var h = new Harness()) {
      h.saga.poisonAudited = true;
      h.open("42");
      h.deliver();
      assertThat(h.row("42").status()).isEqualTo(SagaStatus.FAULTED);
      EventEnvelope audited = h.stored(Audited.class).getFirst();
      assertThat(h.deadLetters.all())
          .singleElement()
          .satisfies(
              entry -> {
                assertThat(entry.sagaId()).isEqualTo(sagaOf("42"));
                assertThat(entry.eventOffset()).isEqualTo(audited.globalOffset());
              });

      h.saga.poisonAudited = false;
      assertThat(h.replayer.replayAll(sagaOf("42"))).containsExactly(ReplayOutcome.REPLAYED);

      assertThat(h.row("42").status()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(h.row("42").state().audits()).isEqualTo(1);
      assertThat(h.deadLetters.all()).isEmpty();
    }
  }
}
