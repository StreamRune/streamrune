package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.saga.*;
import org.streamrune.core.types.*;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

class SagaDeciderAdapterTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  // ========== Test domain types ==========
  record OrderCreated(String orderId) implements DomainEvent {}

  record PaymentReceived(String orderId) implements DomainEvent {}

  record FulfillOrder(String orderId) implements Command {}

  record CompensateCommand(String reason) implements Command {}

  record OrderState(SagaId sagaId, SagaStatus status, boolean paid) implements SagaState {
    @JsonCreator
    OrderState(
        @JsonProperty("sagaId") SagaId sagaId,
        @JsonProperty("status") SagaStatus status,
        @JsonProperty("paid") boolean paid) {
      this.sagaId = sagaId;
      this.status = status;
      this.paid = paid;
    }
  }

  // ========== Test decider ==========
  static class OrderSagaDecider implements SagaDecider<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(sagaId, SagaStatus.STARTED, false);
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      if (event.event() instanceof PaymentReceived) {
        return new OrderState(state.sagaId(), SagaStatus.RUNNING, true);
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      if (event.event() instanceof PaymentReceived pr) {
        return List.of(
            SagaCommand.of(new FulfillOrder(pr.orderId()), AggregateId.of(pr.orderId())));
      }
      return List.of();
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(30));
    }
  }

  // ========== Capturing CommandBus ==========
  static class CapturingCommandBus implements CommandBus {
    final List<Object> dispatched = new ArrayList<>();

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      dispatched.add(command);
      return new CommandResult(
          List.of(),
          StreamId.of(TYPE, AggregateId.of("stream")),
          new Version(1),
          List.of(),
          List.of());
    }
  }

  // ========== Helpers ==========
  private EventEnvelope envelope(DomainEvent event, String streamId) {
    return envelope(event, streamId, 1L);
  }

  /**
   * A live correlated event at or below the row's recorded {@code last_applied_offset} is a
   * redelivery and dedups, so a correlated event delivered after the start event needs a LATER
   * global offset.
   */
  private EventEnvelope envelope(DomainEvent event, String streamId, long globalOffset) {
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(TYPE, AggregateId.of(streamId)),
        new Version(1),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            EventId.of(UUID.randomUUID().toString()),
            CommandId.of(UUID.randomUUID().toString()),
            null,
            null,
            CorrelationId.of(streamId),
            null,
            null,
            Instant.now(),
            java.util.Map.of()));
  }

  @Test
  void deciderBasedSagaRunnerProcessesEvents() {
    var store = new InMemorySagaStore();
    var bus = new CapturingCommandBus();

    var runner =
        SagaRunner.<OrderState>builder()
            .decider(new OrderSagaDecider())
            .sagaStore(store)
            .commandBus(bus)
            .startWhen(e -> e.event() instanceof OrderCreated)
            .extractSagaId(e -> SagaId.of(((OrderCreated) e.event()).orderId()))
            .correlateBy(
                e ->
                    Optional.ofNullable(e.metadata().correlationId())
                        .map(c -> SagaId.of(c.value())))
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
            .build();

    var listener = runner.asEventListener();

    // Start event
    listener.onEvents(List.of(envelope(new OrderCreated("order-1"), "order-1")));

    // Verify saga created
    var loaded =
        store.load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class);
    assertTrue(loaded.isPresent());
    assertEquals(SagaStatus.STARTED, loaded.get().status());

    // Correlated event (a later offset than the start's — the redelivery check dedups at or below
    // it) triggers
    // the command
    listener.onEvents(List.of(envelope(new PaymentReceived("order-1"), "order-1", 2L)));
    assertEquals(1, bus.dispatched.size());
    assertInstanceOf(FulfillOrder.class, bus.dispatched.getFirst());
  }

  @Test
  void adapterDelegatesStateType() {
    var decider = new OrderSagaDecider();
    var adapter =
        new SagaDeciderAdapter<>(
            decider,
            e -> e.event() instanceof OrderCreated,
            e -> SagaId.of("id"),
            e -> Optional.empty());
    assertEquals(OrderState.class, adapter.stateType());
  }

  @Test
  void adapterDelegatesCompensate() {
    var decider =
        new SagaDecider<OrderState>() {
          @Override
          public Class<OrderState> stateType() {
            return OrderState.class;
          }

          @Override
          public OrderState initialState(SagaId sagaId) {
            return new OrderState(sagaId, SagaStatus.STARTED, false);
          }

          @Override
          public OrderState evolve(OrderState state, EventEnvelope event) {
            return state;
          }

          @Override
          public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
            return List.of();
          }

          @Override
          public List<SagaCommand> compensate(
              OrderState state, Throwable failure, SagaCommand failedCommand) {
            return List.of(
                SagaCommand.of(new CompensateCommand("compensate"), AggregateId.of("agg-1")));
          }
        };

    var adapter =
        new SagaDeciderAdapter<>(decider, e -> false, e -> SagaId.of("id"), e -> Optional.empty());

    var result =
        adapter.compensate(
            new OrderState(SagaId.of("s1"), SagaStatus.RUNNING, false),
            new RuntimeException("fail"),
            SagaCommand.of(new CompensateCommand("cmd"), AggregateId.of("agg-1")));

    assertEquals(1, result.size());
  }

  @Test
  void compensationOnly_delegatesCompensate_butRoutersThrow() {
    // The timeout-path factory backs a SagaStepExecutor with the bare decider: compensate is
    // delegated, but the forward routers (isStartEvent/extractSagaId/correlate) are never used on
    // the timeout path and must throw if reached.
    var decider =
        new SagaDecider<OrderState>() {
          @Override
          public Class<OrderState> stateType() {
            return OrderState.class;
          }

          @Override
          public OrderState initialState(SagaId sagaId) {
            return new OrderState(sagaId, SagaStatus.STARTED, false);
          }

          @Override
          public OrderState evolve(OrderState state, EventEnvelope event) {
            return state;
          }

          @Override
          public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
            return List.of();
          }

          @Override
          public List<SagaCommand> compensate(
              OrderState state, Throwable failure, SagaCommand failedCommand) {
            return List.of(SagaCommand.of(new CompensateCommand("undo"), AggregateId.of("agg-1")));
          }
        };

    var adapter = SagaDeciderAdapter.compensationOnly(decider);

    org.assertj.core.api.Assertions.assertThat(
            adapter.compensate(
                new OrderState(SagaId.of("s1"), SagaStatus.RUNNING, false),
                new RuntimeException("boom"),
                null))
        .as("compensate must be delegated to the decider")
        .hasSize(1);

    var event = envelope(new OrderCreated("order-1"), "order-1");
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> adapter.isStartEvent(event))
        .isInstanceOf(UnsupportedOperationException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> adapter.extractSagaId(event))
        .isInstanceOf(UnsupportedOperationException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> adapter.correlate(event))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void builderRejectsDeciderWithoutStartWhen() {
    assertThrows(
        NullPointerException.class,
        () ->
            SagaRunner.<OrderState>builder()
                .decider(new OrderSagaDecider())
                .sagaStore(new InMemorySagaStore())
                .commandBus(new CapturingCommandBus())
                .extractSagaId(e -> SagaId.of("id"))
                .correlateBy(e -> Optional.empty())
                .build());
  }

  @Test
  void builderRejectsDeciderWithoutExtractSagaId() {
    assertThrows(
        NullPointerException.class,
        () ->
            SagaRunner.<OrderState>builder()
                .decider(new OrderSagaDecider())
                .sagaStore(new InMemorySagaStore())
                .commandBus(new CapturingCommandBus())
                .startWhen(e -> true)
                .correlateBy(e -> Optional.empty())
                .build());
  }

  @Test
  void builderRejectsDeciderWithoutCorrelateBy() {
    assertThrows(
        NullPointerException.class,
        () ->
            SagaRunner.<OrderState>builder()
                .decider(new OrderSagaDecider())
                .sagaStore(new InMemorySagaStore())
                .commandBus(new CapturingCommandBus())
                .startWhen(e -> true)
                .extractSagaId(e -> SagaId.of("id"))
                .build());
  }
}
