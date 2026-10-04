package org.streamrune.micronaut;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Singleton;
import java.util.List;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SagaTimeoutRunner;

/**
 * Registers every application-declared {@link SagaTimeoutRunner} bean with the {@link
 * BackgroundRelayHealthContributor} for liveness on {@link StartupEvent}, from a path that is
 * <b>NOT</b> gated on {@code streamrune.saga.compensation-retry-enabled} and <b>NOT</b>
 * short-circuited by an empty set of {@code SagaRunner} beans.
 *
 * <p>Previously this registration lived in {@link SagaCompensationRetryLifecycle}, which was
 * disabled ({@code @Requires(notEquals="false")}) when the compensation re-drive was turned off,
 * and returns early when no {@code SagaRunner} bean exists. In the documented "switch the re-drive
 * off, rely on a SagaTimeoutRunner" config the timeout runner is the SOLE compensation driver, so
 * its dead-thread signal must survive independently of the sweeper — matching the Spring and
 * Quarkus integrations.
 *
 * <p><b>Deliberately NOT gated on {@code streamrune.saga.enabled}.</b> The kill switch governs
 * <em>framework-driven</em> saga processing (subscription wiring, sweepers) — not observability of
 * an <em>application-owned</em> runner bean. A {@code SagaTimeoutRunner} the application declared
 * and started keeps polling regardless of the switch, and gating this registrar on it made {@code
 * /health} report UP while that runner's poll thread was dead and every timed-out saga stranded
 * silently. Spring ({@code @ConditionalOnBean} on runner + contributor, no property condition) and
 * Quarkus (unconditional bean) both register regardless of the switch; Micronaut matches. With
 * sagas disabled and no runner beans declared, the injected list is empty and nothing is registered
 * — no phantom health component either way.
 */
@Singleton
@Requires(beans = BackgroundRelayHealthContributor.class)
public class SagaTimeoutRunnerHealthRegistrar implements ApplicationEventListener<StartupEvent> {

  private final List<SagaTimeoutRunner<?>> sagaTimeoutRunners;
  private final BackgroundRelayHealthContributor relayHealth;

  public SagaTimeoutRunnerHealthRegistrar(
      List<SagaTimeoutRunner<?>> sagaTimeoutRunners, BackgroundRelayHealthContributor relayHealth) {
    this.sagaTimeoutRunners = sagaTimeoutRunners;
    this.relayHealth = relayHealth;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    registerTimeoutRunnerHealth();
  }

  /** Package-private for tests: register every discovered SagaTimeoutRunner's liveness. */
  void registerTimeoutRunnerHealth() {
    if (relayHealth == null || sagaTimeoutRunners == null) {
      return;
    }
    sagaTimeoutRunners.forEach(relayHealth::registerSagaTimeoutRunner);
  }
}
