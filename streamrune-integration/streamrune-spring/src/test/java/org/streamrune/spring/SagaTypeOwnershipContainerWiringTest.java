package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.postgres.PostgresSagaStore;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The startup refusal of two saga runners over one saga state class, in the integration's DEFAULT
 * wiring: the application declares two {@code SagaRunner} beans that each take the auto-configured
 * {@code SagaStore} bean as a parameter, and the context is refreshed.
 *
 * <p>{@link SagaEventPathRetentionWiringTest} covers the same refusal over a {@code SagaStore} bean
 * of the application's own.
 */
class SagaTypeOwnershipContainerWiringTest {

  record OrderState(SagaStatus status, String id) implements SagaState {}

  record ShipmentState(SagaStatus status, String id) implements SagaState {}

  abstract static class NoOpOrchestrator<S extends SagaState> implements SagaOrchestrator<S> {
    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("x");
    }

    @Override
    public S evolve(S state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(S state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(S state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static class OrderOrchestrator extends NoOpOrchestrator<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(SagaStatus.STARTED, sagaId.value());
    }
  }

  static final class RefundOrchestrator extends OrderOrchestrator {}

  static final class ShipmentOrchestrator extends NoOpOrchestrator<ShipmentState> {
    @Override
    public Class<ShipmentState> stateType() {
      return ShipmentState.class;
    }

    @Override
    public ShipmentState initialState(SagaId sagaId) {
      return new ShipmentState(SagaStatus.STARTED, sagaId.value());
    }
  }

  static final CommandBus NOOP_BUS =
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

  static <S extends SagaState> SagaRunner<S> runnerOver(
      SagaStore store, SagaOrchestrator<S> orchestrator) {
    return SagaRunner.<S>builder()
        .orchestrator(orchestrator)
        .sagaStore(store)
        .commandBus(NOOP_BUS)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
        .build();
  }

  /** Two sagas over one state class, each runner built from the auto-configured saga store. */
  @Configuration(proxyBeanMethods = false)
  static class TwoRunnersOverOneStateClass {
    @Bean
    SagaRunner<OrderState> orderRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new OrderOrchestrator());
    }

    @Bean
    SagaRunner<OrderState> refundRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new RefundOrchestrator());
    }
  }

  /** Two sagas, a state class each, both runners built from the auto-configured saga store. */
  @Configuration(proxyBeanMethods = false)
  static class TwoRunnersOverTwoStateClasses {
    @Bean
    SagaRunner<OrderState> orderRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new OrderOrchestrator());
    }

    @Bean
    SagaRunner<ShipmentState> shipmentRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new ShipmentOrchestrator());
    }
  }

  /**
   * One state class over two stores that share no row: the auto-configured one, and one of the
   * application's own that is not a bean.
   */
  @Configuration(proxyBeanMethods = false)
  static class TwoRunnersOverTwoStores {
    @Bean
    SagaRunner<OrderState> orderRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new OrderOrchestrator());
    }

    @Bean
    SagaRunner<OrderState> refundRunner() {
      return runnerOver(new InMemorySagaStore(), new RefundOrchestrator());
    }
  }

  /**
   * What an application supplies: the {@code DataSource}. The {@code SagaStore} is the
   * auto-configured one. The event store stands in for the framework's, which would query the mock
   * {@code DataSource} for its schema.
   */
  private final ApplicationContextRunner application =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore);

  /**
   * The compensation-retry sweepers stay off where the context is expected to start: they would
   * poll the mock {@code DataSource}.
   */
  private ApplicationContextRunner applicationWithoutSweepers() {
    return application.withPropertyValues("streamrune.saga.compensation-retry-enabled=false");
  }

  @Test
  void twoRunnerBeansOverOneStateClass_eachTakingTheAutoConfiguredSagaStore_stopTheContext() {
    application
        .withUserConfiguration(TwoRunnersOverOneStateClass.class)
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Two SagaRunner instances drive the saga type")
                    .hasMessageContaining(OrderState.class.getName())
                    .hasMessageContaining(OrderOrchestrator.class.getName())
                    .hasMessageContaining(RefundOrchestrator.class.getName()));
  }

  @Test
  void theAutoConfiguredSagaStoreIsOneBeanOverTheApplicationsDataSource() {
    applicationWithoutSweepers()
        .withUserConfiguration(TwoRunnersOverTwoStateClasses.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              SagaStore store = ctx.getBean(SagaStore.class);
              assertThat(store).isInstanceOf(PostgresSagaStore.class);
              assertThat(store.storageIdentity()).isSameAs(ctx.getBean(DataSource.class));
            });
  }

  @Test
  void twoRunnerBeansOverTwoStateClasses_start() {
    applicationWithoutSweepers()
        .withUserConfiguration(TwoRunnersOverTwoStateClasses.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBeansOfType(SagaRunner.class)).hasSize(2);
            });
  }

  @Test
  void twoRunnerBeansOverOneStateClassAndTwoStores_start() {
    applicationWithoutSweepers()
        .withUserConfiguration(TwoRunnersOverTwoStores.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBeansOfType(SagaRunner.class)).hasSize(2);
            });
  }
}
