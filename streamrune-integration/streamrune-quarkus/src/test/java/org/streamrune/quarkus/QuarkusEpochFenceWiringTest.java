package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.Typed;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.PostgresReadModelStore;
import org.streamrune.postgres.PostgresSubscriptionCheckpointProcessor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Container-level wiring of each projection's checkpoint target and of the epoch fence, on a real
 * PostgreSQL.
 *
 * <p>Every other projection-runner test drives {@link ProjectionProducer} with hand-built mocks
 * ({@code ProjectionProducerTest}), which proves a bean is forwarded, not that a container produces
 * it. These tests boot a REAL Arc container ({@link RealArcTestContainer}) over the real {@link
 * StreamRuneProducers} and {@link ProjectionProducer}, register real {@code @ProjectionConfig}
 * beans, and read the target each registration of the framework-assembled {@link
 * MultiProjectionRunner} was routed to: the default {@code postgres} read-model store for a {@code
 * TRANSACTIONAL_LOCAL} {@link BaseProjection} over the injected {@code ProjectionRepository}, the
 * event store's subscription checkpoint — fenced by {@link
 * PostgresSubscriptionCheckpointProcessor}, never {@link
 * AtomicBatchProcessor#nonAtomicAtLeastOnce()}, while leadership is on — for an at-least-once
 * projection with no write target.
 *
 * <p>The proxy test boots a {@code TRANSACTIONAL_LOCAL} projection over an application store whose
 * repository is an application-scoped bean, so the projection and the store both hold Arc's client
 * proxy, never the instance behind it: routing must match the write target through the proxy.
 */
@Testcontainers
class QuarkusEpochFenceWiringTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_routing");

  static PGSimpleDataSource dataSource;

  /** What the routed projections read. */
  public record CustomerRegistered(String customerId, String email) implements DomainEvent {}

  static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder()
          .registerEvent("CustomerRegistered", CustomerRegistered.class)
          .build();

  /** Fast polling so a projection sees an appended event within the test's patience. */
  static final Map<String, String> PROPERTIES =
      Map.of("streamrune.polling-interval", "PT0.05S", "streamrune.polling-jitter", "PT0S");

  @BeforeAll
  static void openDataSource() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  /**
   * What a Quarkus application's extensions supply and raw Arc does not: the {@code DataSource}
   * (quarkus-agroal), the bound mapping and the configuration (SmallRye Config), and the
   * application's event types.
   */
  @ApplicationScoped
  public static class PostgresInfrastructure {
    @Produces
    @Singleton
    public DataSource dataSource() {
      return dataSource;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return TestProperties.of(PROPERTIES);
    }

    @Produces
    @Singleton
    public Config config() {
      return TestProperties.config(PROPERTIES);
    }

    @Produces
    @Singleton
    public EventTypeRegistry eventTypeRegistry() {
      return TYPES;
    }
  }

  // Public: RealArcTestContainer's generated producer bean classes live in an isolated
  // URLClassLoader, so a package-private nested class here is inaccessible cross-classloader
  // (IllegalAccessError) — matches the visibility QuarkusBeanWiringTest's own fixtures use.

  /** An at-least-once projection with no write target. */
  @ProjectionConfig(
      name = "fenced_orders",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  public static class FencedProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Registers {@link FencedProjection}. */
  @ApplicationScoped
  public static class FencedBeans {
    @Produces
    @Singleton
    public FencedProjection fencedProjection() {
      return new FencedProjection();
    }
  }

  /** Writes each registration's email through the handed repository. */
  @ProjectionConfig(
      name = RoutedTransactional.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static class RoutedTransactional extends BaseProjection {
    static final String NAME = "routed_tl";

    public RoutedTransactional(ProjectionRepository repository) {
      super(repository, NAME);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (EventEnvelope envelope : batch) {
        if (envelope.event() instanceof CustomerRegistered registered) {
          save(registered.customerId(), registered.email());
        }
      }
    }
  }

  /** Counts the events it sees; writes nowhere the framework knows. */
  @ProjectionConfig(
      name = RoutedAtLeastOnce.NAME,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  public static class RoutedAtLeastOnce implements Projection {
    static final String NAME = "routed_alo";
    final AtomicInteger seen = new AtomicInteger();

    @Override
    public void process(List<EventEnvelope> batch) {
      seen.addAndGet(batch.size());
    }
  }

  /**
   * One projection of each kind: the transactional one over the injected {@code
   * ProjectionRepository}, the at-least-once one over nothing.
   */
  @ApplicationScoped
  public static class RoutedBeans {
    @Produces
    @Singleton
    public RoutedTransactional routedTransactional(ProjectionRepository repository) {
      return new RoutedTransactional(repository);
    }

    @Produces
    @Singleton
    public RoutedAtLeastOnce routedAtLeastOnce() {
      return new RoutedAtLeastOnce();
    }
  }

  /** A TRANSACTIONAL_LOCAL read model over the application-scoped repository's client proxy. */
  @ProjectionConfig(
      name = "proxied_orders",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static class ProxiedOrders extends BaseProjection {
    public ProxiedOrders(ProjectionRepository repository) {
      super(repository, "proxied_orders");
    }

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /**
   * The bean type of the application-scoped repository. {@link JdbcProjectionRepository} is {@code
   * final}: a Quarkus build removes that modifier at augmentation so Arc can subclass it into a
   * client proxy, but this in-process harness cannot rewrite a class the test class loader has
   * already loaded. Declaring the bean by an interface over the two contracts the store hands out
   * gives Arc an interface to proxy, with a real {@code JdbcProjectionRepository} behind it.
   */
  public interface TransactionalRepository extends ProjectionRepository, AtomicBatchProcessor {}

  /**
   * The application's own store, named {@code app}: its repository and processor are an
   * {@code @ApplicationScoped} bean — every injection point and every {@code Instance} lookup
   * receives Arc's client proxy, which forwards to a contextual instance that forwards to the
   * {@link PostgresReadModelStore}'s {@link JdbcProjectionRepository}. Producing a store replaces
   * the default one.
   */
  @ApplicationScoped
  public static class ApplicationStore {
    @Produces
    @ApplicationScoped
    public TransactionalRepository repo(PostgresReadModelStore backing) {
      Object jdbc = backing.repository();
      return (TransactionalRepository)
          Proxy.newProxyInstance(
              TransactionalRepository.class.getClassLoader(),
              new Class<?>[] {TransactionalRepository.class},
              (proxy, method, args) -> {
                try {
                  return method.invoke(jdbc, args);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    }

    /** Typed to its class only: the store bean is {@code appStore}, not this one. */
    @Produces
    @Singleton
    @Typed(PostgresReadModelStore.class)
    public PostgresReadModelStore backing(DataSource dataSource) {
      return PostgresReadModelStore.create("app", dataSource, new ObjectMapper(), true, true);
    }

    @Produces
    @Singleton
    public ReadModelStore appStore(
        TransactionalRepository repository, PostgresReadModelStore backing) {
      return ReadModelStore.of("app", repository, backing.checkpoints(), repository);
    }

    @Produces
    @Singleton
    public ProxiedOrders proxiedOrders(TransactionalRepository repository) {
      return new ProxiedOrders(repository);
    }
  }

  private static MultiProjectionRunner frameworkRunner(RealArcTestContainer arc) {
    var runner =
        arc.container()
            .instance(
                MultiProjectionRunner.class,
                NamedLiteral.of(ProjectionProducer.FRAMEWORK_MULTI_RUNNER));
    assertThat(runner.isAvailable())
        .as("the framework-assembled MultiProjectionRunner must exist once a CONTINUOUS projection")
        .isTrue();
    return runner.get();
  }

  @Test
  void frameworkProjectionRunner_routesAnAtLeastOnceProjectionToTheFencingCheckpoint()
      throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ProjectionProducer.class,
                PostgresInfrastructure.class,
                FencedBeans.class,
                FencedProjection.class))) {
      try {
        MultiProjectionRunner runner = frameworkRunner(arc);

        // Leadership is on, so an at-least-once projection with no write target is fenced in the
        // event store's subscription checkpoint — never on the nonatomic processor, where the
        // epoch fence and first-offset overlap guard are inert.
        assertThat(processorOf(runner, "fenced_orders"))
            .isInstanceOf(PostgresSubscriptionCheckpointProcessor.class);
        assertThat(targetNameOf(runner, "fenced_orders")).isEqualTo("subscription_checkpoint");
      } finally {
        closeLeadership(arc);
      }
    }
  }

  /**
   * The routing on the default stores, end to end: a {@code TRANSACTIONAL_LOCAL} {@link
   * BaseProjection} over the injected {@code ProjectionRepository} (the default {@code postgres}
   * read-model store's repository) is routed to that store by its {@code writeTarget()}, so its
   * checkpoint lands in {@code readmodel_checkpoint}; an {@code AT_LEAST_ONCE_IDEMPOTENT}
   * projection with no write target is routed to the event store's subscription checkpoint, fenced
   * by {@link PostgresSubscriptionCheckpointProcessor} under the real leadership, so its checkpoint
   * lands in {@code subscription_checkpoint}. Neither projection has a row in the other table.
   */
  @Test
  void
      transactionalProjectionCheckpointsInReadModelTable_atLeastOnceWithoutTargetInSubscriptionTable()
          throws Exception {
    var streamId =
        StreamId.of(AggregateType.of("customer"), AggregateId.of("routed-" + UUID.randomUUID()));
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ProjectionProducer.class,
                PostgresInfrastructure.class,
                RoutedBeans.class,
                RoutedTransactional.class,
                RoutedAtLeastOnce.class))) {
      MultiProjectionRunner runner = frameworkRunner(arc);
      try {
        assertThat(arc.container().select(SubscriptionLeadership.class).get())
            .as("the leadership is real")
            .isNotSameAs(SubscriptionLeadership.NOOP);
        assertThat(processorOf(runner, RoutedAtLeastOnce.NAME))
            .isInstanceOf(PostgresSubscriptionCheckpointProcessor.class);
        assertThat(targetNameOf(runner, RoutedAtLeastOnce.NAME))
            .isEqualTo("subscription_checkpoint");
        assertThat(processorOf(runner, RoutedTransactional.NAME))
            .isInstanceOf(JdbcProjectionRepository.class);
        assertThat(targetNameOf(runner, RoutedTransactional.NAME)).isEqualTo("postgres");

        runner.start();
        EventStore store = arc.container().select(EventStore.class).get();
        long offset =
            store
                .append(
                    streamId,
                    List.of(envelope(streamId, new CustomerRegistered("cust-r", "r@example.com"))),
                    Version.initial())
                .globalOffsets()
                .getLast()
                .value();

        Awaitility.await()
            .atMost(Duration.ofSeconds(20))
            .untilAsserted(
                () -> {
                  assertThat(checkpoint("readmodel_checkpoint", RoutedTransactional.NAME))
                      .as("the TRANSACTIONAL_LOCAL projection checkpoints with its read model")
                      .hasValueSatisfying(o -> assertThat(o).isGreaterThanOrEqualTo(offset));
                  assertThat(checkpoint("subscription_checkpoint", RoutedAtLeastOnce.NAME))
                      .as("the at-least-once projection checkpoints in the event store")
                      .hasValueSatisfying(o -> assertThat(o).isGreaterThanOrEqualTo(offset));
                });
        assertThat(checkpoint("subscription_checkpoint", RoutedTransactional.NAME)).isEmpty();
        assertThat(checkpoint("readmodel_checkpoint", RoutedAtLeastOnce.NAME)).isEmpty();
        assertThat(
                arc.container()
                    .select(ProjectionRepository.class)
                    .get()
                    .findById(ProjectionName.of(RoutedTransactional.NAME), "cust-r", String.class))
            .as("the TRANSACTIONAL_LOCAL read model is in the default store")
            .contains("r@example.com");
        assertThat(arc.container().select(RoutedAtLeastOnce.class).get().seen.get()).isPositive();
      } finally {
        runner.stop();
        closeLeadership(arc);
      }
    }
  }

  @Test
  void transactionalProjection_overAnApplicationStoresScopedRepositoryProxy_isRoutedThere()
      throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ProjectionProducer.class,
                PostgresInfrastructure.class,
                ApplicationStore.class,
                ProxiedOrders.class))) {
      try {
        MultiProjectionRunner runner = frameworkRunner(arc);

        Object processor = processorOf(runner, "proxied_orders");
        assertThat(processor).isInstanceOf(io.quarkus.arc.ClientProxy.class);
        assertThat(processor)
            .as("the registration's processor is the application-scoped bean's client proxy")
            .isSameAs(arc.container().instance(TransactionalRepository.class).get());
        assertThat(targetNameOf(runner, "proxied_orders")).isEqualTo("app");
      } finally {
        closeLeadership(arc);
      }
    }
  }

  /** The processor of the pair a registration was routed to. */
  private static Object processorOf(MultiProjectionRunner runner, String projection)
      throws Exception {
    return pairPart(runner, projection, "processor");
  }

  /** The name of the target a registration was routed to. */
  private static String targetNameOf(MultiProjectionRunner runner, String projection)
      throws Exception {
    return (String) pairPart(runner, projection, "targetName");
  }

  private static Object pairPart(MultiProjectionRunner runner, String projection, String part)
      throws Exception {
    Field field = MultiProjectionRunner.class.getDeclaredField("checkpoints");
    field.setAccessible(true);
    Object pair = ((Map<?, ?>) field.get(runner)).get(ProjectionName.of(projection));
    assertThat(pair).as("the routed pair of %s", projection).isNotNull();
    Method accessor = pair.getClass().getDeclaredMethod(part);
    accessor.setAccessible(true);
    return accessor.invoke(pair);
  }

  /** Stops the lease heartbeat, as {@link StreamRuneLifecycle} does on shutdown. */
  private static void closeLeadership(RealArcTestContainer arc) throws Exception {
    arc.container().select(SubscriptionLeadership.class).get().close();
  }

  private static Optional<Long> checkpoint(String table, String projectionName) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT last_offset FROM " + table + " WHERE projection_name = ?")) {
      ps.setString(1, projectionName);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty();
      }
    }
  }

  private static EventEnvelope envelope(StreamId streamId, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(1),
        new EventType("CustomerRegistered"),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of("corr-it"),
            null,
            null,
            Instant.now()));
  }
}
