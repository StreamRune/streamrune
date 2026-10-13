package org.streamrune.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.SagaDeadLetterReplayer;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A running saga behind an HTTP endpoint: the real {@link VirtualThreadCommandBus} over the
 * in-memory event store and command inbox, an in-memory saga store and saga dead-letter store, and
 * a {@link SagaRunner} that {@link #deliver()} feeds from the event store as a subscription would.
 *
 * <p>The saga of a flow starts on {@link FlowOpened} and dispatches {@link Prepare}. {@link
 * Prepared} is the reply to that command. The next step waits for {@link Confirmed}: either the
 * saga dispatches {@link Confirm} itself ({@code confirmsItself}), or a request writes it. On
 * {@link Confirmed} the saga dispatches {@link Finish}, and {@link Finished} completes it.
 *
 * <p>The saga of one flow can be made to fault on its {@link Prepared} ({@link #poisonPreparedOf}):
 * its row is then {@code FAULTED} and shielded and owns one dead-letter entry, which {@link
 * #replayAll} drains through the {@link SagaDeadLetterReplayer} once the poison is lifted.
 *
 * <p>Any caller may execute {@link Confirm} for a flow of its own. With {@link Routing#BUILT_IN}
 * the saga declares no routing for the events of others; with {@link Routing#EVENT_DATA_HOOK} it
 * routes {@link Confirmed} to the saga of the flow the event itself names.
 */
final class SagaOriginHttpFixture implements AutoCloseable {

  /** How the saga under test routes an event it did not cause. */
  enum Routing {
    BUILT_IN,
    EVENT_DATA_HOOK
  }

  /** A command on a flow; the flow id is the aggregate id. */
  sealed interface FlowCommand extends Command {
    String flowId();
  }

  record OpenFlow(String flowId) implements FlowCommand {}

  record Prepare(String flowId) implements FlowCommand {}

  record Confirm(String flowId) implements FlowCommand {}

  record Finish(String flowId) implements FlowCommand {}

  record FlowOpened(String flowId) implements DomainEvent {}

  record Prepared(String flowId) implements DomainEvent {}

  record Confirmed(String flowId) implements DomainEvent {}

  record Finished(String flowId) implements DomainEvent {}

  record FlowAggregate() implements AggregateState {}

  record FlowState(SagaStatus status, String flowId, String step) implements SagaState {}

  static final SagaType SAGA_TYPE = SagaType.fromClass(FlowState.class);
  private static final AggregateType FLOW = AggregateType.of("flow");

  static SagaId sagaIdOf(String flowId) {
    return SagaId.of("flow-saga-" + flowId);
  }

  /** Stores the one event its command stands for, whatever the stream holds. */
  private static final class FlowDecider
      implements Decider<FlowCommand, FlowAggregate, DomainEvent> {
    @Override
    public FlowAggregate initialState() {
      return new FlowAggregate();
    }

    @Override
    public List<DomainEvent> decide(FlowCommand command, FlowAggregate state) {
      return switch (command) {
        case OpenFlow open -> List.of(new FlowOpened(open.flowId()));
        case Prepare prepare -> List.of(new Prepared(prepare.flowId()));
        case Confirm confirm -> List.of(new Confirmed(confirm.flowId()));
        case Finish finish -> List.of(new Finished(finish.flowId()));
      };
    }

    @Override
    public FlowAggregate evolve(FlowAggregate state, DomainEvent event) {
      return state;
    }
  }

  /** The saga with no routing of its own for non-start events. */
  static class FlowSaga implements SagaOrchestrator<FlowState> {
    private final boolean confirmsItself;

    /** The flow whose saga refuses its {@link Prepared}; {@code null} for none. */
    volatile String poisonPreparedFor;

    FlowSaga(boolean confirmsItself) {
      this.confirmsItself = confirmsItself;
    }

    @Override
    public Class<FlowState> stateType() {
      return FlowState.class;
    }

    @Override
    public FlowState initialState(SagaId sagaId) {
      return new FlowState(SagaStatus.STARTED, null, "NEW");
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof FlowOpened;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return sagaIdOf(((FlowOpened) event.event()).flowId());
    }

    @Override
    public FlowState evolve(FlowState state, EventEnvelope event) {
      return switch (event.event()) {
        case FlowOpened opened -> new FlowState(SagaStatus.RUNNING, opened.flowId(), "OPENED");
        case Prepared _ -> {
          if (state.flowId().equals(poisonPreparedFor)) {
            throw new IllegalStateException("evolve refuses this event");
          }
          yield new FlowState(SagaStatus.RUNNING, state.flowId(), "PREPARED");
        }
        case Confirmed _ -> new FlowState(SagaStatus.RUNNING, state.flowId(), "CONFIRMED");
        case Finished _ -> new FlowState(SagaStatus.COMPLETED, state.flowId(), "FINISHED");
        default -> state;
      };
    }

    @Override
    public List<SagaCommand> handle(FlowState state, EventEnvelope event) {
      AggregateId flow = new AggregateId(state.flowId());
      return switch (event.event()) {
        case FlowOpened _ -> List.of(SagaCommand.of(new Prepare(state.flowId()), flow));
        case Prepared _ ->
            confirmsItself ? List.of(SagaCommand.of(new Confirm(state.flowId()), flow)) : List.of();
        case Confirmed _ -> List.of(SagaCommand.of(new Finish(state.flowId()), flow));
        default -> List.of();
      };
    }

    @Override
    public List<SagaCommand> compensate(
        FlowState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  /** The saga that routes a {@link Confirmed} it did not cause by the flow the event names. */
  static final class FlowSagaWithEventDataHook extends FlowSaga {
    FlowSagaWithEventDataHook(boolean confirmsItself) {
      super(confirmsItself);
    }

    @Override
    public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
      return event instanceof Confirmed confirmed
          ? Optional.of(sagaIdOf(confirmed.flowId()))
          : Optional.empty();
    }
  }

  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
  private final SagaStore sagaStore = new InMemorySagaStore();
  final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(sagaStore);
  private final VirtualThreadCommandBus bus;
  private final FlowSaga saga;
  private final SagaRunner<FlowState> runner;
  private final SagaDeadLetterReplayer replayer;
  private GlobalOffset checkpoint = GlobalOffset.initial();

  SagaOriginHttpFixture(Routing routing, boolean confirmsItself) {
    this.bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .commandInbox(inbox)
            .register(FLOW, FlowCommand.class, c -> AggregateId.of(c.flowId()), new FlowDecider())
            .build();
    this.saga =
        routing == Routing.EVENT_DATA_HOOK
            ? new FlowSagaWithEventDataHook(confirmsItself)
            : new FlowSaga(confirmsItself);
    this.runner =
        SagaRunner.<FlowState>builder()
            .orchestrator(saga)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetters)
            .build();
    this.replayer =
        SagaDeadLetterReplayer.builder()
            .sagaDeadLetterStore(deadLetters)
            .sagaStore(sagaStore)
            .eventStore(eventStore)
            .sagaRunner(runner)
            .build();
  }

  /**
   * Makes the saga of {@code flowId} fault on its {@link Prepared}: opened afterwards, its row is
   * {@code FAULTED} and shielded and owns one dead-letter entry. {@code null} lifts the poison.
   */
  void poisonPreparedOf(String flowId) {
    saga.poisonPreparedFor = flowId;
  }

  /** Drains the dead-letter entries of a flow's saga through the replayer, oldest first. */
  List<SagaDeadLetterReplayer.ReplayOutcome> replayAll(String flowId) {
    return replayer.replayAll(sagaIdOf(flowId));
  }

  /** Executes a command as the HTTP endpoint does: under whatever the request filter bound. */
  void execute(FlowCommand command) {
    bus.execute(command);
  }

  /** Opens a flow from outside any request and runs its saga up to the step that waits. */
  void open(String flowId) {
    bus.execute(new OpenFlow(flowId));
    deliver();
  }

  /** Feeds the runner every event stored since the last call, until no new event appears. */
  void deliver() {
    while (true) {
      List<EventEnvelope> batch = eventStore.readGlobalStream(checkpoint, 100);
      if (batch.isEmpty()) {
        return;
      }
      runner.asEventListener().onEvents(batch);
      checkpoint = batch.getLast().globalOffset();
    }
  }

  /** The saga row of a flow. */
  LoadedSaga<FlowState> row(String flowId) {
    return sagaStore.load(sagaIdOf(flowId), SAGA_TYPE, FlowState.class).orElseThrow();
  }

  /** The stored events of {@code type}, in global order. */
  List<EventEnvelope> stored(Class<? extends DomainEvent> type) {
    return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
        .filter(e -> type.isInstance(e.event()))
        .toList();
  }

  /** The origin saga dispatch writes for the saga of a flow. */
  static SagaOrigin originOf(String flowId) {
    return new SagaOrigin(SAGA_TYPE, sagaIdOf(flowId), 0);
  }

  @Override
  public void close() {
    bus.close();
  }
}
