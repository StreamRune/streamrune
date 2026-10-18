package org.streamrune.micronaut;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.integration.SagaForwardFenceSetting;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaForwardFenceDefault;
import org.streamrune.runtime.SagaLeaseWiring;
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
 * <p><b>Forward fence.</b> The same hook applies {@code streamrune.saga.forward-fence} to every
 * runner whose builder named no forward fence ({@link SagaForwardFenceDefault}): {@code on} and
 * {@code off} as written, {@code auto} (the default) as {@code
 * streamrune.subscription.single-active-consumer.enabled} says. A runner that named its fence keeps
 * it. An unknown value fails startup.
 *
 * <p><b>Leadership.</b> With a real {@link SubscriptionLeadership} bean (not the always-leading
 * {@code NOOP}), the same hook logs one WARN per {@link SagaRunner} that carries no lease: its saga
 * type is delivered on every replica ({@link SagaLeaseWiring#warnUngatedUnderLeadership}). The
 * runner is reported, not refused.
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
  private final SubscriptionLeadership leadership;

  public SagaEventPathRetentionConfigurer(
      List<SagaRunner<?>> sagaRunners,
      StreamRuneMicronautProperties properties,
      @Nullable SubscriptionLeadership leadership) {
    this.sagaRunners = sagaRunners;
    this.properties = properties;
    this.leadership = leadership;
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
   * applies the configured window and forward fence to them.
   */
  void apply(List<SagaRunner<?>> runners) {
    SagaTypeOwnership.requireOneRunnerPerSagaType(runners);
    SagaLeaseWiring.warnUngatedUnderLeadership(runners, leadership);
    Duration window = properties.inboxRetentionMaxAge();
    int applied = SagaEventPathRetention.applyDefault(runners, window);
    if (applied > 0) {
      LOG.debug(
          "StreamRune: applied streamrune.inbox.retention-max-age={} as the event-path"
              + " compensation key-age bound on {} SagaRunner bean(s)",
          window,
          applied);
    }
    SagaForwardFenceSetting fence = SagaForwardFenceSetting.parse(properties.sagaForwardFence());
    boolean singleActiveConsumer = properties.subscriptionSingleActiveConsumerEnabled();
    boolean fenceOn = fence.enabled(singleActiveConsumer);
    int fenced = SagaForwardFenceDefault.applyDefault(runners, fenceOn);
    if (fenced > 0) {
      LOG.info(
          "StreamRune: saga forward fence {} on {} SagaRunner bean(s) that name none themselves"
              + " (streamrune.saga.forward-fence={}{})",
          fenceOn ? "on" : "off",
          fenced,
          fence.name().toLowerCase(Locale.ROOT),
          fence == SagaForwardFenceSetting.AUTO
              ? ", following streamrune.subscription.single-active-consumer.enabled="
                  + singleActiveConsumer
              : "");
    }
  }
}
