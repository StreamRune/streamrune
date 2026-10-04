package org.streamrune.spring;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SagaTimeoutRunner;

/**
 * Registers every application-declared {@link SagaTimeoutRunner} bean with the {@link
 * BackgroundRelayHealthContributor} for liveness, from a path that is <b>NOT</b> gated on {@code
 * streamrune.saga.compensation-retry-enabled} and <b>NOT</b> short-circuited by an empty set of
 * {@code SagaRunner} beans.
 *
 * <p>Previously the timeout-runner registration lived inside {@link
 * SagaCompensationRetryLifecycle}, which was disabled when the compensation re-drive was turned off
 * — the documented "switch the re-drive off, rely on a SagaTimeoutRunner" configuration. In that
 * config the timeout runner is the SOLE compensation driver, yet its poll thread dying left {@code
 * /health} UP while every timed-out saga stranded (refunds never issued). Registering here,
 * independently of the sweeper, restores the dead-driver signal in exactly the config where the
 * timeout runner matters most, and gives Spring / Quarkus / Micronaut identical behavior.
 */
public final class SagaTimeoutRunnerHealthRegistrar implements SmartLifecycle {

  private final ObjectProvider<SagaTimeoutRunner<?>> sagaTimeoutRunners;
  private final BackgroundRelayHealthContributor relayHealth;
  private volatile boolean running;
  private boolean registered;

  public SagaTimeoutRunnerHealthRegistrar(
      ObjectProvider<SagaTimeoutRunner<?>> sagaTimeoutRunners,
      BackgroundRelayHealthContributor relayHealth) {
    this.sagaTimeoutRunners = sagaTimeoutRunners;
    this.relayHealth = relayHealth;
  }

  @Override
  public void start() {
    // Register exactly once (guarded by `registered`, which a stop/start cycle does not reset), so
    // a SmartLifecycle restart never double-registers the same runner.
    if (!registered) {
      sagaTimeoutRunners.orderedStream().forEach(relayHealth::registerSagaTimeoutRunner);
      registered = true;
    }
    running = true;
  }

  @Override
  public void stop() {
    running = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /** Same phase as the runners it reports on. */
  @Override
  public int getPhase() {
    return RunnerLifecycle.PHASE;
  }
}
