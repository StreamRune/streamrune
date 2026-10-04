package org.streamrune.quarkus;

import io.quarkus.arc.All;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.interceptor.Interceptor;
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
 *
 * <p><b>Startup-order parity.</b> {@link #apply} carries
 * {@code @Priority(Interceptor.Priority.LIBRARY_BEFORE)} on the event parameter, mirroring {@link
 * SagaStateCryptoValidator}. Before this fix {@code apply} had no {@code @Priority} at all (CDI
 * default 2500), so once Arc honoured the two runner starters' own priorities ({@link
 * StreamRuneLifecycle#onStart} and {@link SagaCompensationRetryLifecycle#onStart}, both {@code
 * LIBRARY_BEFORE + 100}), this configurer ran DETERMINISTICALLY AFTER them — the event-path key-age
 * guard landed only after the framework-hosted runners had already started, so a backlog of events
 * delivered to the saga event path during that window saw the guard still inert. Spring's sibling
 * ({@code SmartInitializingSingleton}) and Micronaut's (no {@code Ordered}, default order 0) both
 * already run before their runners start; this restores parity.
 */
@ApplicationScoped
public class SagaEventPathRetentionConfigurer {

  private static final Logger logger =
      LoggerFactory.getLogger(SagaEventPathRetentionConfigurer.class);

  private final List<SagaRunner<?>> sagaRunners;
  private final StreamRuneQuarkusProperties properties;

  @jakarta.inject.Inject
  public SagaEventPathRetentionConfigurer(
      @All List<SagaRunner<?>> sagaRunners, StreamRuneQuarkusProperties properties) {
    this.sagaRunners = sagaRunners;
    this.properties = properties;
  }

  /**
   * Client-proxy constructor. A normal-scoped bean needs a non-private no-arg constructor for its
   * client proxy; Quarkus's build step adds one by bytecode transformation, but the module's
   * container-level test ({@code RealArcTestContainer}) runs the raw Arc processor without a
   * transformer, so the bean must be proxyable as written. {@code protected} rather than
   * package-private: the proxy is a generated SUBCLASS that may live in a different classloader (a
   * different runtime package), where only protected access reaches a {@code super()} constructor.
   * Never used for a real instance — the injecting constructor above is the {@code @Inject} one.
   */
  protected SagaEventPathRetentionConfigurer() {
    this.sagaRunners = List.of();
    this.properties = null;
  }

  /**
   * Startup observer. {@code public}: the observer invoker Arc generates is a separate class that
   * may live in a different classloader than this bean — as in the module's raw-Arc container test
   * — where package-private access does not reach; the visibility is otherwise inert.
   *
   * <p>{@code @Priority} sits on the EVENT PARAMETER, not the method — see {@code
   * StreamRuneConfigValidator#validate} for why Arc only reads it there.
   */
  public void apply(@Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event) {
    Duration window = properties.inbox().retentionMaxAge();
    int applied = SagaEventPathRetention.applyDefault(sagaRunners, window);
    if (applied > 0) {
      logger.debug(
          "StreamRune: applied streamrune.inbox.retention-max-age={} as the event-path"
              + " compensation key-age bound on {} SagaRunner bean(s)",
          window,
          applied);
    }
  }
}
