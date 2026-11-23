package org.streamrune.quarkus;

import io.quarkus.arc.All;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
import java.util.List;
import java.util.OptionalInt;
import javax.sql.DataSource;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateLocker;
import org.streamrune.integration.SagaConnectionBudget;
import org.streamrune.postgres.ConnectionPools;
import org.streamrune.runtime.SagaParallelismDefault;
import org.streamrune.runtime.SagaParallelismSettings;
import org.streamrune.runtime.SagaRunner;

/**
 * After the other startup observers have run: refuses a subscription name no saga runner is bound
 * to, logs one INFO line per saga runner with its parallelism, the property that decided it and the
 * connections its steps hold, refuses the start when the aggregate lock shares the application's
 * pool and its locks alone can take all of it, and logs a WARN when the steps can hold more than
 * half the pool.
 *
 * <p>It runs this late because a subscription that a saga is delivered through is built by the
 * application, often in a startup observer; only then is each runner bound to its subscriptions.
 * Build such a subscription in a {@code StartupEvent} observer at the default priority ({@code
 * Interceptor.Priority.APPLICATION + 500}) or a lower value: this check observes at {@code
 * Interceptor.Priority.LIBRARY_AFTER + 500}, and a subscription built by an observer at that value
 * or a higher one may be built after it and is not seen. Without a saga runner, a per-subscription
 * property still fails the start: it names a subscription no runner is bound to. The locker is the
 * {@link AggregateLocker} bean, which always exists: the producers supply a {@code
 * LocalStripedLocker}, which holds no pool connection, when the application defines none. The
 * pool's maximum is read from an Agroal {@code DataSource} when the application's is one, from a
 * HikariCP pool otherwise; a pool whose size cannot be read is neither refused nor warned about.
 * The per-subscription keys are read from the application's {@code Config} bean. The check runs
 * once.
 */
@ApplicationScoped
public class SagaParallelismStartupCheck {

  private static final Logger logger = LoggerFactory.getLogger(SagaParallelismStartupCheck.class);

  private final List<SagaRunner<?>> sagaRunners;
  private final StreamRuneQuarkusProperties properties;
  private final Instance<DataSource> dataSource;
  private final Instance<AggregateLocker> locker;
  private final Instance<Config> config;
  private boolean checked;

  @jakarta.inject.Inject
  public SagaParallelismStartupCheck(
      @All List<SagaRunner<?>> sagaRunners,
      StreamRuneQuarkusProperties properties,
      Instance<DataSource> dataSource,
      Instance<AggregateLocker> locker,
      Instance<Config> config) {
    this.sagaRunners = sagaRunners;
    this.properties = properties;
    this.dataSource = dataSource;
    this.locker = locker;
    this.config = config;
  }

  /**
   * Client-proxy constructor, as in {@link SagaEventPathRetentionConfigurer}: never used for a real
   * instance.
   */
  protected SagaParallelismStartupCheck() {
    this.sagaRunners = List.of();
    this.properties = null;
    this.dataSource = null;
    this.locker = null;
    this.config = null;
  }

  /**
   * Startup observer, after the application's own observers at the default priority. {@code public}
   * for the observer invoker Arc generates, see {@link SagaEventPathRetentionConfigurer}.
   */
  public void check(
      @Observes @Priority(Interceptor.Priority.LIBRARY_AFTER + 500) StartupEvent event) {
    if (checked) {
      return;
    }
    checked = true;
    List<SagaRunner<?>> runners = List.copyOf(sagaRunners);
    SagaParallelismSettings settings =
        SagaParallelismProperties.read(
            SagaParallelismProperties.applicationConfig(config), properties.saga().parallelism());
    SagaParallelismDefault.requireKnownSubscriptions(runners, settings);
    if (runners.isEmpty()) {
      return;
    }

    int perStep = SagaConnectionBudget.connectionsPerStep(locker.get());
    int perLock = perStep - 1;
    for (String line : SagaParallelismDefault.startupLines(runners, perStep)) {
      logger.info(line);
    }
    int steps = SagaParallelismDefault.stepsInFlightAtMost(runners);
    OptionalInt poolMaximum = poolMaximum();
    SagaConnectionBudget.refusal(steps, perLock, poolMaximum)
        .ifPresent(
            text -> {
              throw new IllegalStateException(text);
            });
    SagaConnectionBudget.warning(steps, perStep, runners.size(), poolMaximum)
        .ifPresent(logger::warn);
  }

  private OptionalInt poolMaximum() {
    DataSource pool = dataSource.isResolvable() ? dataSource.get() : null;
    if (pool == null) {
      return OptionalInt.empty();
    }
    if (AgroalTypes.isAgroal(pool.getClass())) {
      return AgroalPools.maximumSize(pool);
    }
    return ConnectionPools.maximumSize(pool);
  }

  /** Touches Agroal types; loaded only for a {@code DataSource} that is an Agroal one. */
  private static final class AgroalPools {
    private AgroalPools() {}

    static OptionalInt maximumSize(DataSource pool) {
      int max =
          ((io.agroal.api.AgroalDataSource) pool)
              .getConfiguration()
              .connectionPoolConfiguration()
              .maxSize();
      return max > 0 ? OptionalInt.of(max) : OptionalInt.empty();
    }
  }
}
