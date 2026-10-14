package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaForwardFenceDispatchTest.MarkerCapturingBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A runner fences its forward commands unless its builder switches the fence off; {@link
 * SagaRunner.Builder#build()} refuses a bus that cannot fence against the runner's saga rows; and
 * an integration's default reaches only the runners whose builder named nothing.
 */
class SagaForwardFenceBuilderTest {

  record Opened(String orderId) implements DomainEvent {}

  record Reserve(String orderId) implements Command {}

  record Reserved(String orderId) implements DomainEvent {}

  record Untouched() implements AggregateState {}

  record State(SagaStatus status, String orderId) implements SagaState {}

  private static final String TYPE = State.class.getName();

  /** Reserve on the start event; nothing else. */
  static final class OrderSaga implements SagaDecider<State> {
    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      return new State(SagaStatus.RUNNING, state.orderId());
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      return event.event() instanceof Opened opened
          ? List.of(SagaCommand.of(new Reserve(opened.orderId()), AggregateId.of(opened.orderId())))
          : List.of();
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final class ReserveDecider implements Decider<Reserve, Untouched, DomainEvent> {
    @Override
    public Untouched initialState() {
      return new Untouched();
    }

    @Override
    public List<DomainEvent> decide(Reserve command, Untouched state) {
      return List.of(new Reserved(command.orderId()));
    }

    @Override
    public Untouched evolve(Untouched state, DomainEvent event) {
      return state;
    }
  }

  private final InMemorySagaStore rows = new InMemorySagaStore();
  private final List<VirtualThreadCommandBus> buses = new ArrayList<>();

  @AfterEach
  void closeBuses() {
    buses.forEach(VirtualThreadCommandBus::close);
  }

  /** A bus over an in-memory event store wired, or not, with the saga rows it fences against. */
  private VirtualThreadCommandBus busFencingAgainst(InMemorySagaStore fencedRows) {
    var inbox = new InMemoryCommandInbox();
    var eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    if (fencedRows != null) {
      eventStore.withSagaStore(fencedRows);
    }
    var bus =
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
                new ReserveDecider())
            .build();
    buses.add(bus);
    return bus;
  }

  private static SagaRunner.Builder<State> runner(
      SagaStore store, org.streamrune.core.CommandBus bus) {
    return SagaRunner.<State>builder()
        .decider(new OrderSaga())
        .startWhen(e -> e.event() instanceof Opened)
        .extractSagaId(e -> SagaId.of("order-saga-" + ((Opened) e.event()).orderId()))
        .sagaStore(store)
        .commandBus(bus)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(new InMemorySagaStore()));
  }

  @Test
  void aRunnerFencesByDefault() {
    SagaRunner<State> runner = runner(rows, busFencingAgainst(rows)).build();

    assertThat(SagaForwardFenceDefault.effectiveOf(runner)).isTrue();
  }

  @Test
  void forwardFenceFalseSwitchesItOff() {
    SagaRunner<State> runner = runner(rows, busFencingAgainst(rows)).forwardFence(false).build();

    assertThat(SagaForwardFenceDefault.effectiveOf(runner)).isFalse();
  }

  @Test
  void aBusThatCannotFenceIsRefused_namingTheBusAndTheOptOut() {
    var builder = runner(rows, busFencingAgainst(null));

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The SagaRunner for '"
                + TYPE
                + "' fences its forward commands at their commit, and its command bus cannot:"
                + " org.streamrune.runtime.VirtualThreadCommandBus reports no saga rows to fence"
                + " against (CommandBus.sagaFenceStorageIdentity() is null). Without the fence a"
                + " forward command of a saga can be stored after a compensation that another"
                + " process claimed, and nothing undoes it. Build the bus over an event store that"
                + " fences (PostgresEventStore; in tests InMemoryEventStore.withSagaStore("
                + "theSagaStore)), or switch the fence off for this runner with"
                + " SagaRunner.Builder.forwardFence(false).");
  }

  @Test
  void aBusWithoutAnInboxIsRefusedForTheInboxFirst() {
    var neither =
        new org.streamrune.core.CommandBus() {
          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException();
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean supportsIdempotentExecution() {
            return false;
          }
        };
    var builder = runner(rows, neither);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("requires idempotent execution")
        .hasMessageNotContaining("fences");
  }

  @Test
  void aBusFencingOtherRowsIsRefused() {
    var builder = runner(rows, busFencingAgainst(new InMemorySagaStore())).forwardFence(true);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The SagaRunner for '"
                + TYPE
                + "' fences its forward commands at their commit, and its command bus fences"
                + " against other saga rows than its SagaStore keeps"
                + " (CommandBus.sagaFenceStorageIdentity() does not equal"
                + " SagaStore.storageIdentity()). The saga row is read in the transaction of the"
                + " forward command's append: keep the saga rows in the database of the event"
                + " stream and build the PostgresSagaStore and the PostgresEventStore over the same"
                + " DataSource object (in tests InMemoryEventStore.withSagaStore(theSagaStore)), or"
                + " switch the fence off for this runner with"
                + " SagaRunner.Builder.forwardFence(false).");
  }

  @Test
  void aRunnerThatOptedOutBuildsOverAnyBus() {
    SagaRunner<State> overABusThatCannotFence =
        runner(rows, busFencingAgainst(null)).forwardFence(false).build();
    SagaRunner<State> overOtherRows =
        runner(rows, busFencingAgainst(new InMemorySagaStore())).forwardFence(false).build();

    assertThat(SagaForwardFenceDefault.effectiveOf(overABusThatCannotFence)).isFalse();
    assertThat(SagaForwardFenceDefault.effectiveOf(overOtherRows)).isFalse();
  }

  @Test
  void aWrappedSagaStoreIsTheRowsItWraps() {
    SagaRunner<State> runner =
        runner(new ForwardingSagaStore(rows), busFencingAgainst(rows)).build();

    assertThat(SagaForwardFenceDefault.effectiveOf(runner)).isTrue();
  }

  @Test
  void theDefaultReachesOnlyRunnersThatNamedNothing() {
    SagaRunner<State> namedNothing = runner(rows, busFencingAgainst(rows)).build();
    SagaRunner<State> namedOn = runner(rows, busFencingAgainst(rows)).forwardFence(true).build();

    assertThat(SagaForwardFenceDefault.applyDefault(List.of(namedNothing, namedOn), false))
        .isEqualTo(1);

    assertThat(SagaForwardFenceDefault.effectiveOf(namedNothing)).isFalse();
    assertThat(SagaForwardFenceDefault.effectiveOf(namedOn))
        .as("the explicit value is kept")
        .isTrue();

    SagaRunner<State> namedOff = runner(rows, busFencingAgainst(rows)).forwardFence(false).build();
    assertThat(SagaForwardFenceDefault.applyDefault(List.of(namedOff), true)).isZero();
    assertThat(SagaForwardFenceDefault.effectiveOf(namedOff)).isFalse();
    assertThat(SagaForwardFenceDefault.applyDefault(null, false)).isZero();
    assertThat(SagaForwardFenceDefault.applyDefault(List.of(), false)).isZero();
    assertThat(SagaForwardFenceDefault.applyDefault(Arrays.asList(null, namedNothing), true))
        .isEqualTo(1);
    assertThat(SagaForwardFenceDefault.effectiveOf(namedNothing)).isTrue();
  }

  @Test
  void aDefaultAppliedLateTakesEffectOnTheNextDispatch() {
    var bus = new MarkerCapturingBus(busFencingAgainst(rows));
    SagaRunner<State> runner = runner(rows, bus).build();
    runner.asEventListener().onEvents(List.of(opened("1", 1)));

    SagaForwardFenceDefault.applyDefault(List.of(runner), false);
    runner.asEventListener().onEvents(List.of(opened("2", 2)));

    assertThat(bus.of(Reserve.class)).hasSize(2);
    assertThat(bus.of(Reserve.class).get(0).marker()).as("built fenced").isNotNull();
    assertThat(bus.of(Reserve.class).get(1).marker()).as("the default, from then on").isNull();
  }

  private static EventEnvelope opened(String orderId, long offset) {
    StreamId streamId =
        StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + UUID.randomUUID()));
    return new EventEnvelope(
        GlobalOffset.of(offset),
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
            Instant.now()));
  }
}
