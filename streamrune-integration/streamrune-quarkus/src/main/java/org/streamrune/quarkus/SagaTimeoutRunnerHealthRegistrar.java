package org.streamrune.quarkus;

import io.quarkus.arc.All;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SagaTimeoutRunner;

/**
 * Registers every application-declared {@link SagaTimeoutRunner} bean with the {@link
 * BackgroundRelayHealthContributor} for liveness on {@link StartupEvent}, from a path that is
 * <b>NOT</b> gated on {@code streamrune.saga.compensation-retry-enabled} and <b>NOT</b>
 * short-circuited by an empty set of {@code SagaRunner} beans.
 *
 * <p>Previously this registration lived in {@link SagaCompensationRetryLifecycle}, which returned
 * early when the compensation re-drive was disabled (and, before this fix, also when no {@code
 * SagaRunner} bean exists). In the documented "switch the re-drive off, rely on a
 * SagaTimeoutRunner" config the timeout runner is the SOLE compensation driver, so its dead-thread
 * signal must survive independently of the sweeper — matching the Spring and Micronaut
 * integrations.
 */
@ApplicationScoped
public class SagaTimeoutRunnerHealthRegistrar {

  private final List<SagaTimeoutRunner<?>> sagaTimeoutRunners;
  private final Instance<BackgroundRelayHealthContributor> relayHealthInstance;

  public SagaTimeoutRunnerHealthRegistrar(
      @All List<SagaTimeoutRunner<?>> sagaTimeoutRunners,
      Instance<BackgroundRelayHealthContributor> relayHealthInstance) {
    this.sagaTimeoutRunners = sagaTimeoutRunners;
    this.relayHealthInstance = relayHealthInstance;
  }

  void onStart(@Observes StartupEvent event) {
    registerTimeoutRunnerHealth();
  }

  /** Package-private for tests: register every discovered SagaTimeoutRunner's liveness. */
  void registerTimeoutRunnerHealth() {
    if (relayHealthInstance.isUnsatisfied() || sagaTimeoutRunners == null) {
      return;
    }
    BackgroundRelayHealthContributor health = relayHealthInstance.get();
    sagaTimeoutRunners.forEach(health::registerSagaTimeoutRunner);
  }
}
