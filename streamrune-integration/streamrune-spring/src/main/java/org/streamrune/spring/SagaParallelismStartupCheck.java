package org.streamrune.spring;

import java.util.List;
import java.util.OptionalInt;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.streamrune.core.AggregateLocker;
import org.streamrune.integration.SagaConnectionBudget;
import org.streamrune.postgres.ConnectionPools;
import org.streamrune.runtime.SagaParallelismDefault;
import org.streamrune.runtime.SagaParallelismSettings;
import org.streamrune.runtime.SagaRunner;

/**
 * After every {@code SmartLifecycle} has started: refuses a subscription name no saga runner is
 * bound to, logs one INFO line per saga runner with its parallelism, the property that decided it
 * and the connections its steps hold, refuses the start when the aggregate lock shares the
 * application's pool and its locks alone can take all of it, and logs a WARN when the steps can
 * hold more than half the pool.
 *
 * <p>It runs this late because a subscription that a saga is delivered through is built by the
 * application, often in a {@code SmartLifecycle}; only then is each runner bound to its
 * subscriptions. Without a saga runner, a per-subscription property still fails the start: it names
 * a subscription no runner is bound to. The locker is the {@link AggregateLocker} bean; the
 * auto-configuration supplies the default striped locker, which holds no pool connection, when the
 * application defines none.
 */
public class SagaParallelismStartupCheck
    implements ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware {

  private static final Logger logger = LoggerFactory.getLogger(SagaParallelismStartupCheck.class);

  private final ObjectProvider<SagaRunner<?>> sagaRunners;
  private final StreamRuneProperties properties;
  private final Environment environment;
  private final ObjectProvider<DataSource> dataSource;
  private final ObjectProvider<AggregateLocker> locker;
  private ApplicationContext context;
  private boolean checked;

  public SagaParallelismStartupCheck(
      ObjectProvider<SagaRunner<?>> sagaRunners,
      StreamRuneProperties properties,
      Environment environment,
      ObjectProvider<DataSource> dataSource,
      ObjectProvider<AggregateLocker> locker) {
    this.sagaRunners = sagaRunners;
    this.properties = properties;
    this.environment = environment;
    this.dataSource = dataSource;
    this.locker = locker;
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.context = applicationContext;
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent event) {
    if (event.getApplicationContext() != context || checked) {
      return;
    }
    checked = true;
    List<SagaRunner<?>> runners = sagaRunners.orderedStream().toList();
    SagaParallelismSettings settings = SagaParallelismProperties.read(environment, properties);
    SagaParallelismDefault.requireKnownSubscriptions(runners, settings);
    if (runners.isEmpty()) {
      return;
    }

    int perStep = SagaConnectionBudget.connectionsPerStep(locker.getObject());
    int perLock = perStep - 1;
    for (String line : SagaParallelismDefault.startupLines(runners, perStep)) {
      logger.info(line);
    }
    int steps = SagaParallelismDefault.stepsInFlightAtMost(runners);
    OptionalInt poolMaximum = ConnectionPools.maximumSize(dataSource.getIfUnique());
    SagaConnectionBudget.refusal(steps, perLock, poolMaximum)
        .ifPresent(
            text -> {
              throw new IllegalStateException(text);
            });
    SagaConnectionBudget.warning(steps, perStep, runners.size(), poolMaximum)
        .ifPresent(logger::warn);
  }
}
