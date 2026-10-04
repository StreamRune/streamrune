package org.streamrune.micronaut;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * <p>Deliberately not gated on {@code streamrune.saga.compensation-retry-enabled}: the event path
 * has no enable knob of its own, so the guard must not disappear with the sweeper. Mirrors the
 * {@link SagaStateCryptoValidator} startup-hook pattern.
 */
@Singleton
public class SagaEventPathRetentionConfigurer implements ApplicationEventListener<StartupEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(SagaEventPathRetentionConfigurer.class);

  private final List<SagaRunner<?>> sagaRunners;
  private final StreamRuneMicronautProperties properties;

  public SagaEventPathRetentionConfigurer(
      List<SagaRunner<?>> sagaRunners, StreamRuneMicronautProperties properties) {
    this.sagaRunners = sagaRunners;
    this.properties = properties;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    apply(sagaRunners);
  }

  /** Package-private seam: applies the configured window to {@code runners}. */
  void apply(List<SagaRunner<?>> runners) {
    Duration window = properties.inboxRetentionMaxAge();
    int applied = SagaEventPathRetention.applyDefault(runners, window);
    if (applied > 0) {
      LOG.debug(
          "StreamRune: applied streamrune.inbox.retention-max-age={} as the event-path"
              + " compensation key-age bound on {} SagaRunner bean(s)",
          window,
          applied);
    }
  }
}
