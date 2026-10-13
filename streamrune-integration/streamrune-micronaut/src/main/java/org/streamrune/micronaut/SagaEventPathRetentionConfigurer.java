package org.streamrune.micronaut;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTypeOwnership;

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
 * <p><b>One runner per saga type.</b> The same startup hook first refuses two {@link SagaRunner}
 * beans that drive one saga type — one saga state class — over the same {@code SagaStore} ({@link
 * SagaTypeOwnership#requireOneRunnerPerSagaType}): the application does not start, and the message
 * names the state class and what each runner is built from. Two such runners load and write the
 * same rows, and each takes the replies to the other's commands for its own. One store is one
 * instance, or two instances that keep the same rows ({@code SagaStore.storageIdentity()}: two
 * {@code PostgresSagaStore}s over one {@code DataSource}).
 *
 * <p>Deliberately not gated on {@code streamrune.saga.compensation-retry-enabled}: the event path
 * has no enable knob of its own, so the guard must not disappear with the sweeper. Mirrors the
 * {@link SagaStateCryptoValidator} startup-hook pattern, its order included: the hook runs with the
 * startup validators, ahead of the listeners that start the integration's runners and sweepers.
 */
@Singleton
public class SagaEventPathRetentionConfigurer
    implements ApplicationEventListener<StartupEvent>, Ordered {

  private static final Logger LOG = LoggerFactory.getLogger(SagaEventPathRetentionConfigurer.class);

  private final List<SagaRunner<?>> sagaRunners;
  private final StreamRuneMicronautProperties properties;

  public SagaEventPathRetentionConfigurer(
      List<SagaRunner<?>> sagaRunners, StreamRuneMicronautProperties properties) {
    this.sagaRunners = sagaRunners;
    this.properties = properties;
  }

  // The validators' tier: a lower value than the listeners that start the integration's runners
  // and sweepers (1100), so an application this hook refuses has started none of them, and a
  // sweeper never runs before its runner has the configured window.
  @Override
  public int getOrder() {
    return 1000;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    apply(sagaRunners);
  }

  /**
   * Package-private seam: refuses two of {@code runners} that share a saga type and a store, then
   * applies the configured window to them.
   */
  void apply(List<SagaRunner<?>> runners) {
    SagaTypeOwnership.requireOneRunnerPerSagaType(runners);
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
