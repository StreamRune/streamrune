package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A saga type — a saga state class — has one runner over a saga store. Two runners over the same
 * state class and the same store load and write the same rows, and each resolves the replies to the
 * other's commands to that row; {@link SagaTypeOwnership#requireOneRunnerPerSagaType} refuses the
 * pair and names both.
 */
class SagaTypeOwnershipTest {

  record OrderState(SagaStatus status) implements SagaState {}

  record ShipmentState(SagaStatus status) implements SagaState {}

  /** An orchestrator over {@link OrderState}. */
  static class OrderOrchestrator implements SagaOrchestrator<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(SagaStatus.STARTED);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("unused");
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
      return List.of();
    }
  }

  /** A second saga whose author reused {@link OrderState} as its state class. */
  static final class RefundDecider implements SagaDecider<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(SagaStatus.STARTED);
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      return List.of();
    }
  }

  /** A saga with a state class of its own. */
  static final class ShipmentOrchestrator implements SagaOrchestrator<ShipmentState> {
    @Override
    public Class<ShipmentState> stateType() {
      return ShipmentState.class;
    }

    @Override
    public ShipmentState initialState(SagaId sagaId) {
      return new ShipmentState(SagaStatus.STARTED);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("unused");
    }

    @Override
    public ShipmentState evolve(ShipmentState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(ShipmentState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        ShipmentState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  private static final CommandBus KEYED_BUS =
      new CommandBus() {
        @Override
        public boolean supportsIdempotentExecution() {
          return true;
        }

        @Override
        public <C extends Command> CommandResult execute(C command) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
          throw new UnsupportedOperationException();
        }
      };

  private static <S extends SagaState> SagaRunner<S> runner(
      SagaOrchestrator<S> orchestrator, SagaStore store) {
    return SagaRunner.<S>builder()
        .orchestrator(orchestrator)
        .sagaStore(store)
        .commandBus(KEYED_BUS)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
        .build();
  }

  private static SagaRunner<OrderState> refundRunner(SagaStore store) {
    return SagaRunner.<OrderState>builder()
        .decider(new RefundDecider())
        .startWhen(event -> false)
        .extractSagaId(event -> SagaId.of("unused"))
        .sagaStore(store)
        .commandBus(KEYED_BUS)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
        .build();
  }

  @Test
  void twoRunnersOverOneStateClassAndOneStore_areRefused_andTheMessageNamesBoth() {
    var store = new InMemorySagaStore();
    var order = runner(new OrderOrchestrator(), store);
    var refund = refundRunner(store);

    assertThatThrownBy(() -> SagaTypeOwnership.requireOneRunnerPerSagaType(List.of(order, refund)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("the saga type " + OrderState.class.getName())
        .hasMessageContaining("the orchestrator " + OrderOrchestrator.class.getName())
        .hasMessageContaining("the decider " + RefundDecider.class.getName())
        .hasMessageContaining("A saga state class belongs to one saga");
  }

  @Test
  void twoRunnersOfTheSameOrchestratorClassOverOneStore_areRefusedToo() {
    var store = new InMemorySagaStore();

    assertThatThrownBy(
            () ->
                SagaTypeOwnership.requireOneRunnerPerSagaType(
                    List.of(
                        runner(new OrderOrchestrator(), store),
                        runner(new OrderOrchestrator(), store))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(OrderState.class.getName());
  }

  @Test
  void runnersOfDifferentStateClassesOverOneStore_areAccepted() {
    var store = new InMemorySagaStore();

    assertThatCode(
            () ->
                SagaTypeOwnership.requireOneRunnerPerSagaType(
                    List.of(
                        runner(new OrderOrchestrator(), store),
                        runner(new ShipmentOrchestrator(), store))))
        .doesNotThrowAnyException();
  }

  /** Two stores share no row, so one saga type may run over each. */
  @Test
  void runnersOfOneStateClassOverDifferentStores_areAccepted() {
    assertThatCode(
            () ->
                SagaTypeOwnership.requireOneRunnerPerSagaType(
                    List.of(
                        runner(new OrderOrchestrator(), new InMemorySagaStore()),
                        runner(new OrderOrchestrator(), new InMemorySagaStore()))))
        .doesNotThrowAnyException();
  }

  @Test
  void theSameRunnerListedTwice_nullElements_andNoRunners_areAccepted() {
    var order = runner(new OrderOrchestrator(), new InMemorySagaStore());

    assertThatCode(
            () -> {
              SagaTypeOwnership.requireOneRunnerPerSagaType(Arrays.asList(order, null, order));
              SagaTypeOwnership.requireOneRunnerPerSagaType(List.of());
              SagaTypeOwnership.requireOneRunnerPerSagaType(null);
            })
        .doesNotThrowAnyException();
  }
}
