package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The CONTAINER half of the Quarkus event-path retention wiring. {@link
 * SagaEventPathRetentionWiringTest} pins the configurer's logic by constructing it with {@code new}
 * and calling {@code apply(null)} — which exercises none of the three container obligations the
 * bean actually relies on: (1) Arc discovers the {@code @ApplicationScoped} configurer at all, (2)
 * its {@code @All List<SagaRunner<?>>} injection point collects the APPLICATION's runner beans, and
 * (3) its {@code @Observes StartupEvent} observer fires on startup. This test boots the real Arc
 * container through {@link RealArcTestContainer} and pins all three: an application-produced runner
 * with no explicit window takes the configured default the moment the startup event fires, and one
 * with an explicit window keeps it.
 */
class SagaEventPathRetentionContainerWiringTest {

  record FakeState(SagaStatus status, String id) implements SagaState {}

  static final class FakeOrchestrator implements SagaOrchestrator<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("x");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
    }

    @Override
    public FakeState evolve(FakeState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
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

  /**
   * The application's runner beans — static so the test can inspect the very instances Arc
   * injected.
   */
  static final InMemorySagaStore APP_SAGA_STORE = new InMemorySagaStore();

  static final SagaRunner<FakeState> APP_RUNNER =
      SagaRunner.<FakeState>builder()
          .orchestrator(new FakeOrchestrator())
          .sagaStore(APP_SAGA_STORE)
          .commandBus(NOOP_BUS)
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(APP_SAGA_STORE))
          .build();

  static final InMemorySagaStore EXPLICIT_SAGA_STORE = new InMemorySagaStore();

  static final SagaRunner<FakeState> EXPLICIT_RUNNER =
      SagaRunner.<FakeState>builder()
          .orchestrator(new FakeOrchestrator())
          .sagaStore(EXPLICIT_SAGA_STORE)
          .commandBus(NOOP_BUS)
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(EXPLICIT_SAGA_STORE))
          .inboxRetentionMaxAge(Duration.ofHours(2))
          .build();

  /**
   * Application-side beans: the real {@code @ConfigMapping} binding with a configured window, and
   * two {@code SagaRunner} producers the configurer must discover through {@code @All}.
   */
  public static class ApplicationBeans {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      SmallRyeConfig config =
          new SmallRyeConfigBuilder()
              .withMapping(StreamRuneQuarkusProperties.class)
              .withConverter(
                  Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
              .withDefaultValues(Map.of("streamrune.inbox.retention-max-age", "3d"))
              .build();
      return config.getConfigMapping(StreamRuneQuarkusProperties.class);
    }

    @Produces
    @Singleton
    @Named("appRunner")
    public SagaRunner<FakeState> appRunner() {
      return APP_RUNNER;
    }

    @Produces
    @Singleton
    @Named("explicitRunner")
    public SagaRunner<FakeState> explicitRunner() {
      return EXPLICIT_RUNNER;
    }
  }

  @Test
  void realContainer_discoversTheConfigurer_collectsAllRunners_andAppliesOnStartupEvent()
      throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(SagaEventPathRetentionConfigurer.class, ApplicationBeans.class))) {
      // Obligation 1: Arc bean discovery of the framework's @ApplicationScoped configurer.
      assertTrue(
          arc.container().instance(SagaEventPathRetentionConfigurer.class).isAvailable(),
          "the @ApplicationScoped configurer must be a resolvable Arc bean");
      assertNull(
          SagaEventPathRetention.effectiveWindowOf(APP_RUNNER),
          "precondition: nothing has applied the default before the startup event");

      // Obligation 3: the @Observes StartupEvent observer fires when the container starts up.
      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());

      // Obligation 2: the @All List<SagaRunner<?>> injection point collected BOTH application
      // runner beans — the default landed on the one without a window, and the explicit one kept
      // its own value.
      assertEquals(
          Duration.ofDays(3),
          SagaEventPathRetention.effectiveWindowOf(APP_RUNNER),
          "the configured window must reach the application runner's event-path guard");
      assertEquals(
          Duration.ofHours(2),
          SagaEventPathRetention.effectiveWindowOf(EXPLICIT_RUNNER),
          "an application-supplied window is never overwritten");
    }
  }
}
