package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
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
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.test.InMemorySagaStore;

/**
 * The SagaTimeoutRunner liveness registration lives in an always-present registrar, NOT inside the
 * compensation-retry sweeper lifecycle (which used to be disabled by {@code
 * compensation-retry-enabled=false}; it now only switches its sweepers to sampling-only), so a
 * timeout runner's dead-thread signal survives even in the documented re-drive-off config —
 * matching Spring and Quarkus.
 */
class SagaTimeoutRunnerHealthRegistrarTest {

  record FakeState(SagaStatus status) implements SagaState {}

  static final class FakeDecider implements SagaDecider<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED);
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
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofHours(1));
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

  private static SagaTimeoutRunner<FakeState> timeoutRunner() {
    return SagaTimeoutRunner.<FakeState>builder()
        .decider(new FakeDecider())
        .sagaStore(new InMemorySagaStore())
        .commandBus(NOOP_BUS)
        .sagaType(SagaType.fromClass(FakeState.class))
        .build();
  }

  @Test
  void registersTimeoutRunnerHealth_independentlyOfTheSweeper() {
    var contributor = new BackgroundRelayHealthContributor();

    var registrar = new SagaTimeoutRunnerHealthRegistrar(List.of(timeoutRunner()), contributor);
    registrar.registerTimeoutRunnerHealth();

    assertTrue(
        contributor.components().stream()
            .anyMatch(
                c ->
                    c.name().equals("saga-timeout:" + SagaType.fromClass(FakeState.class).value())),
        "the timeout runner must be health-registered even with the sweeper disabled");
  }

  @Test
  void noOp_whenNoHealthContributor() {
    // No health contributor bean → nothing to register into; must not throw.
    var registrar = new SagaTimeoutRunnerHealthRegistrar(List.of(timeoutRunner()), null);
    assertDoesNotThrow(registrar::registerTimeoutRunnerHealth);
  }

  /**
   * Compile-time bean fixture for the kill-switch context test: an application-owned {@link
   * SagaTimeoutRunner} bean plus a user-supplied contributor to assert registrations against
   * (runtime singletons are not reliably collected into injected {@code List}s — see {@code
   * SagaCompensationRetryLifecycleWiringTest}'s same caveat).
   */
  @Factory
  @Requires(property = "spec.name", value = "health-registrar-kill-switch")
  static class KillSwitchWithRunnerFixture {
    static final BackgroundRelayHealthContributor CONTRIBUTOR =
        new BackgroundRelayHealthContributor();
    static final SagaTimeoutRunner<FakeState> RUNNER = timeoutRunner();

    @Singleton
    BackgroundRelayHealthContributor contributor() {
      return CONTRIBUTOR;
    }

    @Singleton
    SagaTimeoutRunner<FakeState> runner() {
      return RUNNER;
    }
  }

  /** Contributor-only fixture: sagas disabled and NO runner beans declared. */
  @Factory
  @Requires(property = "spec.name", value = "health-registrar-kill-switch-no-runner")
  static class KillSwitchNoRunnerFixture {
    static final BackgroundRelayHealthContributor CONTRIBUTOR =
        new BackgroundRelayHealthContributor();

    @Singleton
    BackgroundRelayHealthContributor contributor() {
      return CONTRIBUTOR;
    }
  }

  @Test
  void registrarSurvivesSagaKillSwitch_andReflectsTheApplicationOwnedRunner() {
    // INTG parity: the `streamrune.saga.enabled` kill switch governs FRAMEWORK-driven saga
    // processing (subscription wiring, sweepers) — it must NOT hide the dead-thread health signal
    // of an APPLICATION-owned SagaTimeoutRunner bean. With the registrar gated on the kill switch,
    // /health reported UP while a started runner's poll thread died and every timed-out saga
    // stranded silently. Spring (@ConditionalOnBean on runner+contributor, no property gate) and
    // Quarkus (unconditional bean) both register regardless of the kill switch; Micronaut must
    // match.
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name", "health-registrar-kill-switch",
                "streamrune.saga.enabled", "false"))) {
      assertTrue(
          ctx.containsBean(SagaTimeoutRunnerHealthRegistrar.class),
          "the health registrar must exist even with streamrune.saga.enabled=false — the kill"
              + " switch disables framework saga processing, not observability of the app's own"
              + " runner");
      ctx.getBean(SagaTimeoutRunnerHealthRegistrar.class).registerTimeoutRunnerHealth();
      assertTrue(
          KillSwitchWithRunnerFixture.CONTRIBUTOR.components().stream()
              .anyMatch(
                  c ->
                      c.name()
                          .equals("saga-timeout:" + SagaType.fromClass(FakeState.class).value())),
          "the application-owned timeout runner must be health-registered");
    }
  }

  @Test
  void sagaKillSwitchWithNoRunner_bootsClean_reportsNoFalseHealthEntry() {
    // saga.enabled=false and NO SagaTimeoutRunner bean: the context must boot cleanly and the
    // registrar must register nothing — an app that genuinely has no saga runners must never gain
    // a phantom saga-timeout health component (no false DOWN, no false UP).
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name", "health-registrar-kill-switch-no-runner",
                "streamrune.saga.enabled", "false"))) {
      if (ctx.containsBean(SagaTimeoutRunnerHealthRegistrar.class)) {
        ctx.getBean(SagaTimeoutRunnerHealthRegistrar.class).registerTimeoutRunnerHealth();
      }
      assertTrue(
          KillSwitchNoRunnerFixture.CONTRIBUTOR.components().stream()
              .noneMatch(c -> c.name().startsWith("saga-timeout:")),
          "no runner beans -> no saga-timeout health component");
    }
  }
}
