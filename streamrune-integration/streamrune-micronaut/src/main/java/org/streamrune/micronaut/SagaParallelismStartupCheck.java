package org.streamrune.micronaut;

import io.micronaut.context.env.Environment;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateLocker;
import org.streamrune.integration.SagaConnectionBudget;
import org.streamrune.postgres.ConnectionPools;
import org.streamrune.runtime.SagaParallelismDefault;
import org.streamrune.runtime.SagaParallelismSettings;
import org.streamrune.runtime.SagaRunner;

/**
 * After every other startup listener has run: refuses a subscription name no saga runner is bound
 * to, logs one INFO line per saga runner with its parallelism, the property that decided it and the
 * connections its steps hold, refuses the start when the aggregate lock shares the application's
 * pool and its locks alone can take all of it, and logs a WARN when the steps can hold more than
 * half the pool.
 *
 * <p>It runs this late because a subscription that a saga is delivered through is built by the
 * application, often in a startup listener; only then is each runner bound to its subscriptions.
 * Without a saga runner, a per-subscription property still fails the start: it names a subscription
 * no runner is bound to. The locker is the {@link AggregateLocker} bean; the integration supplies
 * the default striped locker, which holds no pool connection, when the application defines none.
 * The check runs once, however many startup events are delivered.
 */
@Singleton
public class SagaParallelismStartupCheck
    implements ApplicationEventListener<StartupEvent>, Ordered {

  private static final Logger logger = LoggerFactory.getLogger(SagaParallelismStartupCheck.class);

  private final List<SagaRunner<?>> sagaRunners;
  private final StreamRuneMicronautProperties properties;
  private final Environment environment;
  private final DataSource dataSource;
  private final AggregateLocker locker;
  private final AtomicBoolean checked = new AtomicBoolean();

  public SagaParallelismStartupCheck(
      List<SagaRunner<?>> sagaRunners,
      StreamRuneMicronautProperties properties,
      Environment environment,
      @Nullable DataSource dataSource,
      AggregateLocker locker) {
    this.sagaRunners = sagaRunners;
    this.properties = properties;
    this.environment = environment;
    this.dataSource = dataSource;
    this.locker = locker;
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (!checked.compareAndSet(false, true)) {
      return;
    }
    List<SagaRunner<?>> runners = sagaRunners;
    SagaParallelismSettings settings =
        SagaParallelismProperties.read(environment, properties.sagaParallelism());
    SagaParallelismDefault.requireKnownSubscriptions(runners, settings);
    if (runners.isEmpty()) {
      return;
    }

    int perStep = SagaConnectionBudget.connectionsPerStep(locker);
    int perLock = perStep - 1;
    for (String line : SagaParallelismDefault.startupLines(runners, perStep)) {
      logger.info(line);
    }
    int steps = SagaParallelismDefault.stepsInFlightAtMost(runners);
    OptionalInt poolMaximum = ConnectionPools.maximumSize(dataSource);
    SagaConnectionBudget.refusal(steps, perLock, poolMaximum)
        .ifPresent(
            text -> {
              throw new IllegalStateException(text);
            });
    SagaConnectionBudget.warning(steps, perStep, runners.size(), poolMaximum)
        .ifPresent(logger::warn);
  }
}
