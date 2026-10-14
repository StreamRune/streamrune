package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.postgres.PostgresSagaStore;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The startup refusal of two saga runners over one saga state class, in the integration's DEFAULT
 * wiring and in a real application context: the application declares two {@code SagaRunner} beans
 * that each take the integration's own {@code SagaStore} bean as a parameter, and the context is
 * started.
 *
 * <p>{@link SagaEventPathRetentionWiringTest} calls the configurer with runners built by hand; it
 * shows neither that the container collects the application's runner beans for the check nor that
 * the throw stops the start.
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
        // The bus is a test double that stores nothing; the fence is tested in
        // SagaForwardFence*Test.
        .forwardFence(false)
        .build();
  }

  /**
   * What an application supplies: the {@code DataSource}. The {@code SagaStore} is the
   * integration's own. The event store stands in for the framework's, which would query the mock
   * {@code DataSource} for its schema.
   */
  @Factory
  @Requires(property = "saga-ownership.infrastructure", value = "true")
  static class Infrastructure {
    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Singleton
    EventStore eventStore() {
      return mock(EventStore.class);
    }
  }

  /** Two sagas over one state class, each runner built from the integration's saga store. */
  @Factory
  @Requires(property = "spec.name", value = "saga-ownership-one-state-class")
  static class TwoRunnersOverOneStateClass {
    /** Told of every compensation-retry sweeper the integration starts. */
    static final BackgroundRelayHealthContributor RELAY_HEALTH =
        mock(BackgroundRelayHealthContributor.class);

    @Singleton
    BackgroundRelayHealthContributor relayHealth() {
      return RELAY_HEALTH;
    }

    @Singleton
    @Named("orderRunner")
    SagaRunner<OrderState> orderRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new OrderOrchestrator());
    }

    @Singleton
    @Named("refundRunner")
    SagaRunner<OrderState> refundRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new RefundOrchestrator());
    }
  }

  /** Two sagas, a state class each, both runners built from the integration's saga store. */
  @Factory
  @Requires(property = "spec.name", value = "saga-ownership-two-state-classes")
  static class TwoRunnersOverTwoStateClasses {
    @Singleton
    @Named("orderRunner")
    SagaRunner<OrderState> orderRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new OrderOrchestrator());
    }

    @Singleton
    @Named("shipmentRunner")
    SagaRunner<ShipmentState> shipmentRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new ShipmentOrchestrator());
    }
  }

  /**
   * One state class over two stores that share no row: the integration's, and one of the
   * application's own.
   */
  @Factory
  @Requires(property = "spec.name", value = "saga-ownership-two-stores")
  static class TwoRunnersOverTwoStores {
    @Singleton
    @Named("orderRunner")
    SagaRunner<OrderState> orderRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new OrderOrchestrator());
    }

    @Singleton
    @Named("refundRunner")
    SagaRunner<OrderState> refundRunner() {
      return runnerOver(new InMemorySagaStore(), new RefundOrchestrator());
    }
  }

  /**
   * The default wiring of an application with a {@code DataSource}. The compensation-retry sweepers
   * stay off where the context is expected to start: they would poll the mock {@code DataSource}.
   */
  private static Map<String, Object> application(String specName, boolean sweepers) {
    return Map.of(
        "spec.name",
        specName,
        "saga-ownership.infrastructure",
        "true",
        "streamrune.saga.compensation-retry-enabled",
        String.valueOf(sweepers));
  }

  private static Throwable rootOf(Throwable thrown) {
    Throwable root = thrown;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    return root;
  }

  @Test
  void twoRunnerBeansOverOneStateClass_eachTakingTheIntegrationsSagaStore_stopTheStart() {
    var thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                ApplicationContext.run(application("saga-ownership-one-state-class", true))
                    .close());

    var refusal = assertInstanceOf(IllegalStateException.class, rootOf(thrown));
    assertTrue(
        refusal.getMessage().contains("Two SagaRunner instances drive the saga type"),
        refusal.getMessage());
    assertTrue(refusal.getMessage().contains(OrderState.class.getName()), refusal.getMessage());
    assertTrue(
        refusal.getMessage().contains(OrderOrchestrator.class.getName()), refusal.getMessage());
    assertTrue(
        refusal.getMessage().contains(RefundOrchestrator.class.getName()), refusal.getMessage());
    // The refusal comes first: the integration has started no sweeper for either runner.
    verify(TwoRunnersOverOneStateClass.RELAY_HEALTH, never())
        .registerSagaCompensationRetrySweeper(any());
  }

  @Test
  void theIntegrationsSagaStoreIsOneBeanOverTheApplicationsDataSource() {
    try (var ctx = ApplicationContext.run(application("saga-ownership-two-state-classes", false))) {
      SagaStore store = ctx.getBean(SagaStore.class);

      assertInstanceOf(PostgresSagaStore.class, store);
      assertSame(store, ctx.getBean(SagaStore.class));
      assertSame(ctx.getBean(DataSource.class), store.storageIdentity());
    }
  }

  @Test
  void twoRunnerBeansOverTwoStateClasses_start() {
    try (var ctx = ApplicationContext.run(application("saga-ownership-two-state-classes", false))) {
      assertEquals(2, ctx.getBeansOfType(SagaRunner.class).size());
    }
  }

  @Test
  void twoRunnerBeansOverOneStateClassAndTwoStores_start() {
    try (var ctx = ApplicationContext.run(application("saga-ownership-two-stores", false))) {
      assertEquals(2, ctx.getBeansOfType(SagaRunner.class).size());
    }
  }

  /**
   * The refusal is a startup validator: it runs in the validators' tier, ahead of the listeners
   * that start the integration's own runners and sweepers, so no sweeper of a refused application
   * is started.
   */
  @Test
  void theRefusalRunsAheadOfTheListenersThatStartRunners() {
    int refusal = new SagaEventPathRetentionConfigurer(List.of(), null).getOrder();
    int runnerStarter =
        new StreamRuneLifecycle(null, null, null, null, null, null, null, null, null).getOrder();
    int sweeperStarter =
        new SagaCompensationRetryLifecycle(
                null, null, null, null, null, null, null, null, null, false)
            .getOrder();

    assertTrue(refusal < runnerStarter, "before StreamRuneLifecycle");
    assertTrue(refusal < sweeperStarter, "before SagaCompensationRetryLifecycle");
    assertEquals(new SagaStateCryptoValidator(List.of(), List.of()).getOrder(), refusal);
  }
}
