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
