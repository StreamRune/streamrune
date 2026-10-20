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
 * <p>The timeout-runner registration does not live inside {@link SagaCompensationRetryLifecycle},
 * whose work follows the compensation re-drive. In the documented "switch the re-drive off, rely on
 * a SagaTimeoutRunner" configuration the timeout runner is the SOLE compensation driver, and its
 * poll thread dying must not leave {@code /health} UP while every timed-out saga strands (refunds
 * never issued). Registering here, independently of the sweeper, keeps the dead-driver signal in
 * exactly the config where the timeout runner matters most, and gives Spring / Quarkus / Micronaut
 * identical behavior.
 *
 * <p>The beans are the application's: each is built with {@code
 * SagaTimeoutRunner.Builder.sagaRunner(...)}, naming the {@code SagaRunner} bean of its saga type,
 * so that its claim at a saga's deadline waits for a delivery that is dispatching that saga's
 * forward commands in this process (the builder refuses a timeout runner that names neither a
 * runner nor a step lock of its own). The component registered here reads {@code DEGRADED} while
 * such a claim has been waited for and is still deferred ({@link
 * SagaTimeoutRunner#claimsDeferredPastTheWait()}): a step of that saga has not returned, and the
 * forward fence of its runner is off.
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
