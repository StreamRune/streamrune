package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
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
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
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
import org.streamrune.postgres.LeaseBasedLeadership;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.postgres.PostgresSubscriptionCheckpointProcessor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Container-level wiring of the read-model stores and of each projection's checkpoint target, in a
 * real Micronaut context over the module's own beans.
 *
 * <p>On a real PostgreSQL: the default {@code postgres} read-model store is the module's {@link
 * org.streamrune.postgres.PostgresReadModelStore}, the injectable {@link ProjectionRepository} is
 * its repository, and {@link ProjectionFactory} routes a {@code TRANSACTIONAL_LOCAL} {@link
 * BaseProjection} over that repository to the store (its checkpoint in {@code
 * readmodel_checkpoint}) and an at-least-once projection with no write target to the event store's
 * subscription checkpoint, fenced by {@link PostgresSubscriptionCheckpointProcessor} under the real
 * lease leadership (its checkpoint in {@code subscription_checkpoint}). Every test that needs a
 * database gets a fresh one, so what a test finds in it was created by that test's context.
 *
 * <p>Without a database: a {@link ReadModelStore} the application registers as a runtime singleton
 * is collected by the factory (Micronaut does not reliably collect runtime singletons into an
 * injected {@code List}) and replaces the default store.
 */
@Testcontainers
class MicronautReadModelStoreWiringTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_routing");

  /** The database of the running test; the infrastructure factory hands it out. */
  static volatile PGSimpleDataSource applicationDataSource;

  /** What the routed projections read. */
  public record CustomerRegistered(String customerId, String email) implements DomainEvent {}

  static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder()
          .registerEvent("CustomerRegistered", CustomerRegistered.class)
          .build();

  static final String ROUTED = "readmodel-store-routed";
  static final String DATABASE_ONLY = "readmodel-store-database-only";
  static final String RUNTIME_STORE = "readmodel-store-runtime-singleton";

  /** What an application supplies: the {@code DataSource} and its event types. */
  @Factory
  @Requires(property = "spec.name", pattern = "readmodel-store-(routed|database-only)")
  static class PostgresInfrastructure {
    @Singleton
    DataSource dataSource() {
      return applicationDataSource;
    }

    @Singleton
    EventTypeRegistry eventTypeRegistry() {
      return TYPES;
    }
  }

  /** Writes each registration's email through the handed repository. */
  @Singleton
  @Requires(property = "spec.name", value = ROUTED)
  @ProjectionConfig(
      name = RoutedTransactional.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class RoutedTransactional extends BaseProjection {
    static final String NAME = "routed_tl";

    RoutedTransactional(ProjectionRepository repository) {
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
  @Singleton
  @Requires(property = "spec.name", value = ROUTED)
  @ProjectionConfig(
      name = RoutedAtLeastOnce.NAME,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class RoutedAtLeastOnce implements Projection {
    static final String NAME = "routed_alo";
    final AtomicInteger seen = new AtomicInteger();

    @Override
    public void process(List<EventEnvelope> batch) {
      seen.addAndGet(batch.size());
    }
  }

  /**
   * A {@code TRANSACTIONAL_LOCAL} projection with no write target that writes through the handed
   * repository: routed to the one transactional store, whatever its name.
   */
  @Singleton
  @Requires(property = "spec.name", value = RUNTIME_STORE)
  @ProjectionConfig(name = "runtime_tl", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class RuntimeStoreTransactional implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {}

    @Override
    public boolean writesThroughRepository() {
      return true;
    }
  }

  /** An at-least-once projection that names its store. */
  @Singleton
  @Requires(property = "spec.name", value = RUNTIME_STORE)
  @ProjectionConfig(
      name = "runtime_alo",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
      readModelStore = "memory")
  static class RuntimeStoreDeclared implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @Test
  void
      transactionalProjectionCheckpointsInReadModelTable_atLeastOnceWithoutTargetInSubscriptionTable()
          throws Exception {
    applicationDataSource = freshDatabase();
    var streamId =
        StreamId.of(AggregateType.of("customer"), AggregateId.of("routed-" + UUID.randomUUID()));
    try (var ctx = ApplicationContext.run(properties(ROUTED))) {
      MultiProjectionRunner runner = frameworkRunner(ctx);

      assertThat(ctx.getBean(SubscriptionLeadership.class))
          .as("the leadership is real")
          .isInstanceOf(LeaseBasedLeadership.class);
      assertThat(processorOf(runner, RoutedAtLeastOnce.NAME))
          .isInstanceOf(PostgresSubscriptionCheckpointProcessor.class);
      assertThat(targetNameOf(runner, RoutedAtLeastOnce.NAME)).isEqualTo("subscription_checkpoint");
      assertThat(processorOf(runner, RoutedTransactional.NAME))
          .isInstanceOf(JdbcProjectionRepository.class)
          .isSameAs(ctx.getBean(ProjectionRepository.class));
      assertThat(targetNameOf(runner, RoutedTransactional.NAME)).isEqualTo("postgres");

      long offset =
          ctx.getBean(EventStore.class)
              .append(
                  streamId,
                  List.of(envelope(streamId, new CustomerRegistered("cust-r", "r@example.com"))),
                  Version.initial())
              .globalOffsets()
              .getLast()
              .value();

      // The lifecycle started the runner at startup; the projections catch up on their own.
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
              ctx.getBean(ProjectionRepository.class)
                  .findById(ProjectionName.of(RoutedTransactional.NAME), "cust-r", String.class))
          .as("the TRANSACTIONAL_LOCAL read model is in the default store")
          .contains("r@example.com");
      assertThat(ctx.getBean(RoutedAtLeastOnce.class).seen.get()).isPositive();
    }
  }

  /**
   * The default store touches the database when it is created (server version, migration,
   * validation), and it must be created while the context starts — on the starting thread, by the
   * lifecycle resolving the framework's runner — not first by a request. The read-model series'
   * history table exists in a fresh database the moment {@code run} returns, before the test looks
   * up any bean.
   */
  @Test
  void theDefaultStoreIsCreatedWhileTheContextStarts_whenATransactionalProjectionIsDiscovered()
      throws Exception {
    applicationDataSource = freshDatabase();
    try (var ctx = ApplicationContext.run(properties(ROUTED))) {
      assertThat(tableExists("flyway_schema_history_readmodel")).isTrue();
      assertThat(tableExists("readmodel_checkpoint")).isTrue();
    }
  }

  @Test
  void
      theOnlyStoreIsThePostgresStore_andTheProjectionRepositoryAndCheckpointAreItsAndTheEventStores()
          throws Exception {
    applicationDataSource = freshDatabase();
    try (var ctx = ApplicationContext.run(properties(DATABASE_ONLY))) {
      var stores = List.copyOf(ctx.getBeansOfType(ReadModelStore.class));
      assertThat(stores).hasSize(1);
      ReadModelStore store = stores.getFirst();
      assertThat(store.name()).isEqualTo("postgres");
      assertThat(ctx.getBean(ReadModelStore.class, Qualifiers.byName("postgres"))).isSameAs(store);
      assertThat(ctx.getBean(ProjectionRepository.class)).isSameAs(store.repository());

      CheckpointTarget subscription =
          ctx.getBean(
              CheckpointTarget.class,
              Qualifiers.byName(StreamRuneMicronautModule.SUBSCRIPTION_CHECKPOINT));
      assertThat(subscription.name()).isEqualTo(OffsetStore.SUBSCRIPTION_CHECKPOINT);
      assertThat(subscription.checkpoints())
          .isInstanceOf(PostgresOffsetStore.class)
          .isSameAs(ctx.getBean(OffsetStore.class));
      assertThat(subscription.processor())
          .isInstanceOf(PostgresSubscriptionCheckpointProcessor.class);
      assertThat(tableExists("readmodel_checkpoint")).isTrue();
    }
  }

  @Test
  void theReadModelKeyOff_appliesNothing_andTheStoreRefusesTheMissingSchema() throws Exception {
    applicationDataSource = freshDatabase();
    var props = properties(DATABASE_ONLY);
    props.put("streamrune.read-model.schema.auto-initialize", "false");
    try (var ctx = ApplicationContext.run(props)) {
      assertThatThrownBy(() -> ctx.getBean(ReadModelStore.class))
          .hasStackTraceContaining("SchemaValidationException");
      assertThat(tableExists("flyway_schema_history_readmodel")).isFalse();
    }
  }

  @Test
  void theReadModelKeyUnset_followsTheEventStoreKey_off() throws Exception {
    applicationDataSource = freshDatabase();
    migrateTheEventStoreOnly();
    var props = properties(DATABASE_ONLY);
    props.put("streamrune.event-store.schema.auto-initialize", "false");
    try (var ctx = ApplicationContext.run(props)) {
      assertThatThrownBy(() -> ctx.getBean(ReadModelStore.class))
          .hasStackTraceContaining("SchemaValidationException");
      assertThat(tableExists("flyway_schema_history_readmodel")).isFalse();
    }
  }

  @Test
  void theReadModelKeyOn_migratesEvenWhenTheEventStoreKeyIsOff() throws Exception {
    applicationDataSource = freshDatabase();
    migrateTheEventStoreOnly();
    var props = properties(DATABASE_ONLY);
    props.put("streamrune.event-store.schema.auto-initialize", "false");
    props.put("streamrune.read-model.schema.auto-initialize", "true");
    try (var ctx = ApplicationContext.run(props)) {
      assertThat(ctx.getBean(ReadModelStore.class).name()).isEqualTo("postgres");
      assertThat(tableExists("flyway_schema_history_readmodel")).isTrue();
    }
  }

  @Test
  void theDefaultStoresMapperEncryptsWithTheCryptoEngine_andIsCryptoBlindWithout()
      throws Exception {
    applicationDataSource = freshDatabase();
    var module = new StreamRuneMicronautModule();
    var withCrypto =
        module.postgresReadModelStore(
            applicationDataSource,
            StreamRuneMicronautProperties.withDefaults(),
            List.of(new InMemoryCryptoEngine()),
            null);
    assertThat(mapperOf(withCrypto).getRegisteredModuleIds()).contains("CryptoShreddingModule");
    var withoutCrypto =
        module.postgresReadModelStore(
            applicationDataSource, StreamRuneMicronautProperties.withDefaults(), List.of(), null);
    assertThat(mapperOf(withoutCrypto).getRegisteredModuleIds())
        .doesNotContain("CryptoShreddingModule");
  }

  /**
   * A store the application registers at run time — not a compiled bean definition — is collected
   * by the projection factory and routes registrations, and the default store backs off for it
   * although a {@code DataSource} is present (the default store, created on this mock, would fail).
   */
  @Test
  void aRuntimeSingletonStore_isCollectedAndRoutedTo_andReplacesTheDefaultStore() throws Exception {
    var repository = new InMemoryProjectionRepository();
    ReadModelStore memory = ReadModelStore.of("memory", repository, repository, repository);
    try (var ctx =
        ApplicationContext.builder()
            .properties(
                Map.of(
                    "spec.name",
                    RUNTIME_STORE,
                    "streamrune.runner-lifecycle-enabled",
                    "false",
                    "streamrune.subscription.single-active-consumer.enabled",
                    "false"))
            .singletons(
                memory,
                mock(DataSource.class),
                new InMemoryEventStore(TYPES),
                new InMemoryOffsetStore())
            .start()) {
      assertThat(List.copyOf(ctx.getBeansOfType(ReadModelStore.class))).containsExactly(memory);

      MultiProjectionRunner runner = frameworkRunner(ctx);
      assertThat(targetNameOf(runner, "runtime_tl")).isEqualTo("memory");
      assertThat(processorOf(runner, "runtime_tl")).isSameAs(repository);
      assertThat(targetNameOf(runner, "runtime_alo")).isEqualTo("memory");
      assertThat(processorOf(runner, "runtime_alo")).isSameAs(repository);
    }
  }

  // === helpers ===

  /**
   * Applies the event-store series to the test's database with the key on, so a context with the
   * event-store key off can start (its command bus validates that schema): a context with no
   * projection and no index bean creates the event store at startup but not the read-model store.
   */
  private static void migrateTheEventStoreOnly() throws Exception {
    try (var ctx = ApplicationContext.run(properties(DATABASE_ONLY))) {
      ctx.getBean(EventStore.class);
    }
    assertThat(tableExists("flyway_schema_history_readmodel"))
        .as("nothing created the read-model store")
        .isFalse();
  }

  private static Map<String, Object> properties(String spec) {
    var props = new HashMap<String, Object>();
    props.put("spec.name", spec);
    props.put("streamrune.polling-interval", "50ms");
    props.put("streamrune.polling-jitter", "0s");
    props.put("streamrune.saga.compensation-retry-enabled", "false");
    return props;
  }

  private static MultiProjectionRunner frameworkRunner(ApplicationContext ctx) {
    return ctx.getBean(
        MultiProjectionRunner.class, Qualifiers.byName(ProjectionFactory.FRAMEWORK_MULTI_RUNNER));
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

  private static ObjectMapper mapperOf(ReadModelStore store) throws Exception {
    Object repository = store.repository();
    Field field = repository.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    return (ObjectMapper) field.get(repository);
  }

  /** A new, empty database on the container, so a test sees only what its own context created. */
  private static PGSimpleDataSource freshDatabase() throws Exception {
    String name = "rm_" + UUID.randomUUID().toString().replace("-", "");
    var admin = new PGSimpleDataSource();
    admin.setUrl(PG.getJdbcUrl());
    admin.setUser(PG.getUsername());
    admin.setPassword(PG.getPassword());
    try (var conn = admin.getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE DATABASE " + name);
    }
    var dataSource = new PGSimpleDataSource();
    dataSource.setServerNames(new String[] {PG.getHost()});
    dataSource.setPortNumbers(new int[] {PG.getMappedPort(5432)});
    dataSource.setDatabaseName(name);
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    return dataSource;
  }

  private static boolean tableExists(String table) throws Exception {
    try (var conn = applicationDataSource.getConnection();
        var ps = conn.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
      ps.setString(1, table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    }
  }

  private static Optional<Long> checkpoint(String table, String projectionName) throws Exception {
    try (var conn = applicationDataSource.getConnection();
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
