package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
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
 * wiring and in a real Arc container: the application declares two {@code SagaRunner} producers
 * that each take the integration's own {@code SagaStore} bean as a parameter — the shape a Quarkus
 * application writes — and the startup event is fired.
 *
 * <p>{@link SagaEventPathRetentionWiringTest} builds the configurer by hand around one store; it
 * shows neither that the container hands the two producers one store, nor that it collects the
 * application's runner beans for the check, nor that the throw leaves the startup event.
 */
class SagaTypeOwnershipContainerWiringTest {

  public record OrderState(SagaStatus status, String id) implements SagaState {}

  public record ShipmentState(SagaStatus status, String id) implements SagaState {}

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

  static StreamRuneQuarkusProperties properties(Map<String, String> values) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .withDefaultValues(values)
            .build();
    return config.getConfigMapping(StreamRuneQuarkusProperties.class);
  }

  /**
   * What a Quarkus application supplies that raw Arc cannot: the {@code DataSource} and the bound
   * configuration. Every other bean, the {@code SagaStore} among them, is the integration's own.
   */
  @ApplicationScoped
  public static class Infrastructure {
    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return SagaTypeOwnershipContainerWiringTest.properties(Map.of());
    }
  }

  /** Two sagas over one state class, each runner built from the integration's saga store. */
  public static class TwoRunnersOverOneStateClass {
    @Produces
    @Singleton
    @Named("orderRunner")
    public SagaRunner<OrderState> orderRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new OrderOrchestrator());
    }

    @Produces
    @Singleton
    @Named("refundRunner")
    public SagaRunner<OrderState> refundRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new RefundOrchestrator());
    }
  }

  /** Two sagas, a state class each, both runners built from the integration's saga store. */
  public static class TwoRunnersOverTwoStateClasses {
    @Produces
    @Singleton
    @Named("orderRunner")
    public SagaRunner<OrderState> orderRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new OrderOrchestrator());
    }

    @Produces
    @Singleton
    @Named("shipmentRunner")
    public SagaRunner<ShipmentState> shipmentRunner(SagaStore sagaStore) {
      return runnerOver(sagaStore, new ShipmentOrchestrator());
    }
  }

  /** One state class over two stores of the application's own: the runners share no row. */
  public static class TwoRunnersOverTwoStores {
    @Produces
    @Singleton
    @Named("orderRunner")
    public SagaRunner<OrderState> orderRunner() {
      return runnerOver(new InMemorySagaStore(), new OrderOrchestrator());
    }

    @Produces
    @Singleton
    @Named("refundRunner")
    public SagaRunner<OrderState> refundRunner() {
      return runnerOver(new InMemorySagaStore(), new RefundOrchestrator());
    }
  }

  /** The failure itself, whether the container hands it over as thrown or wrapped. */
  private static Throwable rootOf(Throwable thrown) {
    Throwable root = thrown;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    return root;
  }

  private static void fireStartup(RealArcTestContainer arc) {
    arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
  }

  @Test
  void twoRunnerBeansOverOneStateClass_eachTakingTheIntegrationsSagaStore_failTheStartup()
      throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                SagaEventPathRetentionConfigurer.class,
                Infrastructure.class,
                TwoRunnersOverOneStateClass.class))) {

      assertThatThrownBy(() -> fireStartup(arc))
          .satisfies(
              thrown ->
                  assertThat(rootOf(thrown))
                      .isInstanceOf(IllegalStateException.class)
                      .hasMessageContaining("Two SagaRunner instances drive the saga type")
                      .hasMessageContaining(OrderState.class.getName())
                      .hasMessageContaining(OrderOrchestrator.class.getName())
                      .hasMessageContaining(RefundOrchestrator.class.getName()));
    }
  }

  @Test
  void everyInjectionPointReceivesTheOneSagaStoreOfTheApplication() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(List.of(StreamRuneProducers.class, Infrastructure.class))) {
      SagaStore first = arc.container().select(SagaStore.class).get();
      SagaStore second = arc.container().select(SagaStore.class).get();

      assertThat(first).isInstanceOf(PostgresSagaStore.class);
      assertThat(second)
          .as("one store per application, not one per injection point")
          .isSameAs(first);
    }
  }

  @Test
  void twoRunnerBeansOverTwoStateClasses_start() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                SagaEventPathRetentionConfigurer.class,
                Infrastructure.class,
                TwoRunnersOverTwoStateClasses.class))) {
      assertThatCode(() -> fireStartup(arc)).doesNotThrowAnyException();
    }
  }

  @Test
  void twoRunnerBeansOverOneStateClassAndTwoStoresOfTheApplication_start() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                SagaEventPathRetentionConfigurer.class,
                Infrastructure.class,
                TwoRunnersOverTwoStores.class))) {
      assertThatCode(() -> fireStartup(arc)).doesNotThrowAnyException();
    }
  }
}
