package org.streamrune.integration.tck;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.EventStoreOptions;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.integration.BackendSelection;
import org.streamrune.integration.ProjectionStoreRouting;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.SubscriptionHealthContributor;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.sqlite.SqliteBackend;

/**
 * An application on the SQLite backend wired without a framework, as an integration wires one, for
 * the tests of {@link SingleInstanceMultiReadModelStoreScenario} in this module: the backend's
 * stores opened on the application's {@code DataSource}, its event store, a command bus with the
 * application's command registrations, and one projection runner whose registrations are routed
 * through {@link ProjectionStoreRouting} over the default store and the application's stores, under
 * {@code NOOP} leadership. Its health is each registration's store and lag; closing it stops the
 * runner, closes the stores and then the {@code DataSource}, as a container closes a pool.
 */
final class SqliteReferenceApplication implements Application {

  /** The event types of an application that declares none: the contract command's. */
  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder()
          .registerEvent(ContractBeans.ContractCommandExecuted.class)
          .registerState("ContractCommandState", ContractBeans.ContractCommandState.class)
          .build();

  private final List<Object> beans = new ArrayList<>();
  private final DataSource dataSource;
  private final String failure;
  private BackendStores stores;
  private SubscriptionHealthContributor health;
  private MultiProjectionRunner runner;

  private SqliteReferenceApplication(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    this.dataSource = dataSource;
    String refusal = null;
    try {
      start(properties, extraBeans);
    } catch (RuntimeException e) {
      refusal = messages(e);
      stop();
    }
    this.failure = refusal;
  }

  /**
   * Builds a constructor-injected bean as a container does: through its public constructor that
   * takes the application's {@code ProjectionRepository}.
   */
  private static Object injected(Class<?> beanClass, ProjectionRepository repository) {
    try {
      return beanClass.getConstructor(ProjectionRepository.class).newInstance(repository);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(
          beanClass.getName() + " needs a public constructor taking a ProjectionRepository", e);
    }
  }

  static Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    return new SqliteReferenceApplication(dataSource, properties, extraBeans);
  }

  private void start(Map<String, String> properties, Set<Class<?>> extraBeans) {
    if (!"sqlite".equals(properties.get(BackendSelection.PROPERTY))) {
      throw new IllegalStateException("this application runs on streamrune.backend=sqlite only");
    }
    var backend = new SqliteBackend();
    backend.verify(dataSource);
    stores =
        backend.open(
            new BackendContext(
                dataSource,
                null,
                Clock.systemUTC(),
                StreamRuneMetrics.NOOP,
                StreamRuneTracing.NOOP,
                true,
                true,
                true));
    ReadModelStore defaultStore = stores.defaultReadModelStore();
    beans.add(defaultStore.repository());
    for (Class<?> beanClass : extraBeans) {
      // A constructor-injected bean of the scenario takes the default store's repository.
      beans.add(
          ContractBeans.injected(beanClass)
              ? injected(beanClass, defaultStore.repository())
              : ContractBeans.create(beanClass));
    }

    EventStore eventStore =
        stores
            .eventStoreFactory(
                new EventStoreOptions(
                    beansOf(EventTypeRegistry.class).stream().findFirst().orElse(TYPES),
                    List.of(),
                    List.of(),
                    null,
                    null,
                    null,
                    null,
                    null))
            .create();
    var bus = VirtualThreadCommandBus.builder().eventStore(eventStore);
    for (DeciderRegistration<?, ?, ?> registration : beansOf(DeciderRegistration.class)) {
      register(bus, registration);
    }
    beans.add(bus.build());

    List<ReadModelStore> readModelStores = new ArrayList<>();
    readModelStores.add(defaultStore);
    readModelStores.addAll(beansOf(ReadModelStore.class));
    CheckpointTarget subscriptionCheckpoint = stores.offsetStore().subscriptionCheckpoint();
    health = new SubscriptionHealthContributor(eventStore, stores.offsetStore());
    var runnerBuilder =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(stores.offsetStore())
            .atomicProcessor(subscriptionCheckpoint.processor())
            .subscriptionConfig(
                new SubscriptionConfig(true, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(SubscriptionLeadership.NOOP)
            .healthContributor(health);
    for (Projection projection : beansOf(Projection.class)) {
      ProjectionConfig config = projection.getClass().getAnnotation(ProjectionConfig.class);
      CheckpointTarget target =
          ProjectionStoreRouting.route(
              new ProjectionStoreRouting.Registration(
                  config.name(), config.deliveryMode(), config.readModelStore(), projection),
              readModelStores,
              defaultStore.name(),
              subscriptionCheckpoint,
              SubscriptionLeadership.NOOP);
      runnerBuilder.register(
          ProjectionName.of(config.name()),
          projection,
          ProjectionErrorStrategy.HALT,
          config.deliveryMode(),
          target);
    }
    runner = runnerBuilder.build();
    runner.start();
  }

  private static <
          C extends org.streamrune.core.Command,
          S extends org.streamrune.core.AggregateState,
          E extends org.streamrune.core.DomainEvent>
      void register(
          VirtualThreadCommandBus.Builder bus, DeciderRegistration<C, S, E> registration) {
    bus.register(
        registration.aggregateType(),
        registration.commandType(),
        registration.idExtractor(),
        registration.decider());
  }

  private <T> List<T> beansOf(Class<T> type) {
    return beans.stream().filter(type::isInstance).map(type::cast).toList();
  }

  @Override
  public boolean started() {
    return failure == null;
  }

  @Override
  public String startupFailureMessage() {
    return failure;
  }

  /**
   * A bean of the application: its own beans, the default store's {@code ProjectionRepository} and
   * the command bus.
   */
  @Override
  public <T> T bean(Class<T> type) {
    if (type == ProjectionRepository.class) {
      return type.cast(stores.defaultReadModelStore().repository());
    }
    return beansOf(type).getFirst();
  }

  @Override
  public HttpResponse http(String method, String path, Map<String, String> headers, String body) {
    throw new UnsupportedOperationException("this application serves no HTTP");
  }

  @Override
  public Map<String, Object> health() {
    Map<String, Object> details = new LinkedHashMap<>();
    for (SubscriptionHealth subscription : health.health()) {
      String prefix = "subscription." + subscription.name().value();
      details.put(prefix + ".store", subscription.store());
      details.put(prefix + ".lag", subscription.lag());
    }
    return Map.of("status", "UP", "details", details);
  }

  @Override
  public List<String> startupLogLines() {
    return List.of();
  }

  @Override
  public void close() {
    stop();
  }

  private void stop() {
    if (runner != null) {
      runner.close();
      runner = null;
    }
    if (stores != null) {
      stores.close();
      stores = null;
    }
    if (dataSource instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        throw new IllegalStateException("cannot close the DataSource", e);
      }
    }
  }

  private static String messages(Throwable failure) {
    List<String> messages = new ArrayList<>();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      messages.add(t.getMessage() != null ? t.getMessage() : t.toString());
    }
    return String.join("\n", messages);
  }
}
