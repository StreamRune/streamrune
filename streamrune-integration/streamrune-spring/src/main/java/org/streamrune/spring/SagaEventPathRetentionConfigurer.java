package org.streamrune.spring;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaRunner;

/**
 * Supplies {@code streamrune.inbox.retention-max-age} to every application {@link SagaRunner}
 * bean's <b>event-path</b> key-age guard.
 *
 * <p>Every other consumer of that window is configured for you — {@code
 * SagaCompensationRetrySweeper} by {@link SagaCompensationRetryLifecycle} — but a {@code
 * SagaRunner} is application-constructed, so before this configurer a deployment that set the
 * property got the guard on the sweeper and NOT on the event path: the only re-driver with no
 * leadership gate, running on every replica, and therefore the likeliest of the four to fire.
 *
 * <p>An application that named {@code SagaRunner.Builder.inboxRetentionMaxAge} itself keeps its
 * value — including a deliberate zero/negative "leave the guard off".
 *
 * <p>Runs after all singletons are instantiated so every {@link SagaRunner} bean is visible, and
 * before {@code SmartLifecycle} starts any subscription — the same hook, for the same reason, as
 * {@link SagaStateCryptoValidator}.
 */
public class SagaEventPathRetentionConfigurer implements SmartInitializingSingleton {

  private static final Logger logger =
      LoggerFactory.getLogger(SagaEventPathRetentionConfigurer.class);

  private final ObjectProvider<SagaRunner<?>> sagaRunnerProvider;
  private final StreamRuneProperties properties;

  public SagaEventPathRetentionConfigurer(
      ObjectProvider<SagaRunner<?>> sagaRunnerProvider, StreamRuneProperties properties) {
    this.sagaRunnerProvider = sagaRunnerProvider;
    this.properties = properties;
  }

  @Override
  public void afterSingletonsInstantiated() {
    Duration window = properties.inbox().retentionMaxAge();
    int applied =
        SagaEventPathRetention.applyDefault(sagaRunnerProvider.orderedStream().toList(), window);
    if (applied > 0) {
      logger.debug(
          "StreamRune: applied streamrune.inbox.retention-max-age={} as the event-path"
              + " compensation key-age bound on {} SagaRunner bean(s)",
          window,
          applied);
    }
  }
}
