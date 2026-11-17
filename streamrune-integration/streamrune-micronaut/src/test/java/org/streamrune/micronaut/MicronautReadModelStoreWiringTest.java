package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.management.health.indicator.HealthResult;
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
import org.assertj.core.api.InstanceOfAssertFactories;
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
import org.streamrune.core.Sort;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.LeaseBasedLeadership;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.postgres.PostgresReadModelStore;
import org.streamrune.postgres.PostgresSubscriptionCheckpointProcessor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.SubscriptionHealthContributor;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Mono;

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
 * <p>Several stores: the default store is replaced only by a store named {@code postgres}, a store
 * of another name is added beside it, duplicate names fail the start, each index is declared on the
 * store its projection is routed to, and the {@code ProjectionRepository} bean stays the default
 * store's repository.
 *
 * <p>Without a database: a {@link ReadModelStore} named {@code postgres} the application registers
 * as a runtime singleton before the context starts is collected (with {@code getBeansOfType}, the
 * codebase's established multi-bean collection pattern) and replaces the default store.
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
  static final String REPLACED = "readmodel-stores-replaced";
  static final String ADDED = "readmodel-stores-added";
  static final String DUPLICATE = "readmodel-stores-duplicate";
  static final String INDEXED = "readmodel-stores-indexed";
  static final String PRIMARY = "readmodel-stores-primary";

  /** What an application supplies: the {@code DataSource} and its event types. */
  @Factory
  @Requires(
      property = "spec.name",
      pattern = "readmodel-store-(routed|database-only)|readmodel-stores-.*")
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
      readModelStore = "postgres")
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
   * A {@code TRANSACTIONAL_LOCAL} projection keeps its checkpoint in {@code readmodel_checkpoint}
   * and has no row in {@code subscription_checkpoint}. Once it caught up, its health entry reads
   * lag 0 and UP — measured against its own store's checkpoint — and names the store, in the
   * contributor and in the rendered health indicator. With a lag threshold of 1, a lag measured
   * against {@code subscription_checkpoint} would read DEGRADED and grow with every append.
   */
  @Test
  void aCaughtUpTransactionalProjection_reportsLagZeroAndUp_andNamesItsStore() throws Exception {
    applicationDataSource = freshDatabase();
    var streamId =
        StreamId.of(AggregateType.of("customer"), AggregateId.of("health-" + UUID.randomUUID()));
    var props = properties(ROUTED);
    props.put("streamrune.subscription.health.lag-threshold", "1");
    try (var ctx = ApplicationContext.run(props)) {
      long offset =
          ctx.getBean(EventStore.class)
              .append(
                  streamId,
                  List.of(envelope(streamId, new CustomerRegistered("cust-h", "h@example.com"))),
                  Version.initial())
              .globalOffsets()
              .getLast()
              .value();
      var contributor = ctx.getBean(SubscriptionHealthContributor.class);

      Awaitility.await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> {
                assertThat(checkpoint("readmodel_checkpoint", RoutedTransactional.NAME))
                    .hasValueSatisfying(o -> assertThat(o).isGreaterThanOrEqualTo(offset));
                SubscriptionHealth transactional = healthOf(contributor, RoutedTransactional.NAME);
                assertThat(transactional.lag()).isZero();
                assertThat(transactional.status()).isEqualTo(SubscriptionHealth.Status.UP);
                assertThat(transactional.store()).isEqualTo("postgres");
                SubscriptionHealth atLeastOnce = healthOf(contributor, RoutedAtLeastOnce.NAME);
                assertThat(atLeastOnce.lag()).isZero();
                assertThat(atLeastOnce.status()).isEqualTo(SubscriptionHealth.Status.UP);
                assertThat(atLeastOnce.store()).isEqualTo(OffsetStore.SUBSCRIPTION_CHECKPOINT);
              });
      assertThat(checkpoint("subscription_checkpoint", RoutedTransactional.NAME))
          .as("the lag was not read from the subscription checkpoint")
          .isEmpty();

      HealthResult result =
          Mono.from(ctx.getBean(StreamRuneHealthIndicator.class).getResult()).block();
      assertThat(result).isNotNull();
      assertThat(((Map<?, ?>) result.getDetails()).get("subscription." + RoutedTransactional.NAME))
          .asInstanceOf(InstanceOfAssertFactories.MAP)
          .containsEntry("lag", 0L)
          .containsEntry("status", "UP")
          .containsEntry("store", "postgres");
    }
  }

  private static SubscriptionHealth healthOf(
      SubscriptionHealthContributor contributor, String name) {
    return contributor.health().stream()
        .filter(h -> h.name().value().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no health entry for " + name));
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
      assertThat(
              ctx.getBean(
                  ReadModelStore.class,
                  Qualifiers.byName(StreamRuneMicronautModule.DEFAULT_READ_MODEL_STORE_BEAN)))
          .isSameAs(store);
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

  /**
   * Selecting the stores creates the default store, as in Spring and Quarkus: an application whose
   * projections are all at least once and write nowhere routed never uses a part of the store, yet
   * its read-model series must be migrated (and validated) at startup like everywhere else. The
   * selected store is the Postgres store itself, so messages and health name its class.
   */
  @Test
  void selectingTheStores_createsTheDefaultStore() throws Exception {
    applicationDataSource = freshDatabase();
    try (var ctx = ApplicationContext.run(properties(DATABASE_ONLY))) {
      List<ReadModelStore> stores = ctx.getBean(ReadModelStores.class).stores();

      assertThat(tableExists("readmodel_checkpoint"))
          .as("the default store is created when the stores are selected")
          .isTrue();
      assertThat(stores).singleElement().isInstanceOf(PostgresReadModelStore.class);
    }
  }

  @Test
  void theReadModelKeyOff_appliesNothing_andTheStoreRefusesTheMissingSchema() throws Exception {
    applicationDataSource = freshDatabase();
    var props = properties(DATABASE_ONLY);
    props.put("streamrune.read-model.schema.auto-initialize", "false");
    try (var ctx = ApplicationContext.run(props)) {
      assertThatThrownBy(() -> ctx.getBean(ReadModelStore.class).repository())
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
      assertThatThrownBy(() -> ctx.getBean(ReadModelStore.class).repository())
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
      ReadModelStore store = ctx.getBean(ReadModelStore.class);
      assertThat(store.name()).isEqualTo("postgres");
      store.repository(); // the deferred store is created on first use
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
   * A store named {@code postgres} the application registers at run time, before the context starts
   * — not a compiled bean definition — is collected and routes registrations, and replaces the
   * default store by its name although a {@code DataSource} is present (the default store, created
   * on this mock, would fail).
   */
  @Test
  void aRuntimeSingletonStoreNamedPostgres_isCollectedAndRoutedTo_andReplacesTheDefaultStore()
      throws Exception {
    var repository = new InMemoryProjectionRepository();
    ReadModelStore memory = ReadModelStore.of("postgres", repository, repository, repository);
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
      assertThat(ctx.getBean(ReadModelStores.class).stores()).containsExactly(memory);

      MultiProjectionRunner runner = frameworkRunner(ctx);
      assertThat(targetNameOf(runner, "runtime_tl")).isEqualTo("postgres");
      assertThat(processorOf(runner, "runtime_tl")).isSameAs(repository);
      assertThat(targetNameOf(runner, "runtime_alo")).isEqualTo("postgres");
      assertThat(processorOf(runner, "runtime_alo")).isSameAs(repository);
    }
  }

  // ── several read-model stores: the default replaced only by name ─────────────────

  /** The application's own store named {@code postgres}: it replaces the default one. */
  @Factory
  @Requires(property = "spec.name", value = REPLACED)
  static class ReplacementStore {
    static volatile InMemoryProjectionRepository repository = new InMemoryProjectionRepository();

    @Singleton
    @jakarta.inject.Named("appPostgres")
    ReadModelStore appPostgres() {
      return ReadModelStore.of("postgres", repository, repository, repository);
    }
  }

  /** The application's own store named {@code search}: it is added beside the default one. */
  @Factory
  @Requires(property = "spec.name", pattern = "readmodel-stores-(added|duplicate|indexed|primary)")
  static class SearchStore {
    static volatile InMemoryProjectionRepository repository = new InMemoryProjectionRepository();

    @Singleton
    @jakarta.inject.Named("searchStore")
    ReadModelStore searchStore() {
      return ReadModelStore.of("search", repository, repository, repository);
    }
  }

  /** A second store named {@code search}. */
  @Factory
  @Requires(property = "spec.name", value = DUPLICATE)
  static class SecondSearchStore {
    @Singleton
    @jakarta.inject.Named("secondSearch")
    ReadModelStore secondSearch() {
      var repository = new InMemoryProjectionRepository();
      return ReadModelStore.of("search", repository, repository, repository);
    }
  }

  /** A transactional projection over the injected {@code ProjectionRepository} bean. */
  @Singleton
  @Requires(property = "spec.name", pattern = "readmodel-stores-(replaced|added)")
  @ProjectionConfig(
      name = StoresTransactional.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class StoresTransactional extends BaseProjection {
    static final String NAME = "stores_tl";

    StoresTransactional(ProjectionRepository repository) {
      super(repository, NAME);
    }

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** An at-least-once projection that declares the {@code search} store. */
  @Singleton
  @Requires(property = "spec.name", pattern = "readmodel-stores-(added|duplicate|indexed)")
  @ProjectionConfig(
      name = SearchCatalog.NAME,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
      readModelStore = "search")
  static class SearchCatalog implements Projection {
    static final String NAME = "search_catalog";

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  static final ProjectionIndex ROUTED_INDEX =
      ProjectionIndex.forSort(ProjectionName.of(SearchCatalog.NAME), Sort.by("title"));
  static final ProjectionIndex UNREGISTERED_INDEX =
      ProjectionIndex.forSort(ProjectionName.of("unregistered_view_model"), Sort.by("title"));

  /** An index of the routed projection, and one of a projection with no registration. */
  @Factory
  @Requires(property = "spec.name", value = INDEXED)
  static class Indexes {
    @Singleton
    @jakarta.inject.Named("routedIndex")
    ProjectionIndex routedIndex() {
      return ROUTED_INDEX;
    }

    @Singleton
    @jakarta.inject.Named("unregisteredIndex")
    ProjectionIndex unregisteredIndex() {
      return UNREGISTERED_INDEX;
    }
  }

  /** An application bean that injects a {@code ReadModelStore} without a qualifier. */
  @Singleton
  @Requires(property = "spec.name", value = PRIMARY)
  static class InjectsAReadModelStore {
    final ReadModelStore store;

    InjectsAReadModelStore(ReadModelStore store) {
      this.store = store;
    }
  }

  /** The test's properties, leadership off: the in-memory stores need no lease heartbeat. */
  private static Map<String, Object> storesProperties(String spec) {
    var props = properties(spec);
    props.put("streamrune.subscription.single-active-consumer.enabled", "false");
    return props;
  }

  /**
   * An application store named {@code postgres} replaces the default store: it is the only store,
   * the {@code ProjectionRepository} bean is its repository, a projection over that repository is
   * routed to it, and the default store is never created: its series never reaches the database.
   */
  @Test
  void userStoreNamedPostgresReplacesTheDefault() throws Exception {
    applicationDataSource = freshDatabase();
    ReplacementStore.repository = new InMemoryProjectionRepository();
    try (var ctx = ApplicationContext.run(storesProperties(REPLACED))) {
      assertThat(ctx.getBean(ReadModelStores.class).stores())
          .extracting(ReadModelStore::name)
          .containsExactly("postgres");
      assertThat(ctx.getBean(ProjectionRepository.class)).isSameAs(ReplacementStore.repository);
      MultiProjectionRunner runner = frameworkRunner(ctx);
      assertThat(targetNameOf(runner, StoresTransactional.NAME)).isEqualTo("postgres");
      assertThat(processorOf(runner, StoresTransactional.NAME))
          .isSameAs(ReplacementStore.repository);
      assertThat(tableExists("readmodel_checkpoint"))
          .as("the replaced default store is never created")
          .isFalse();
    }
  }

  /**
   * Even a context that creates every singleton at startup ({@code eagerInitSingletons}) never
   * creates a replaced default store: its series never reaches the database.
   */
  @Test
  void aReplacedDefaultStore_isNotCreated_evenWithEagerSingletons() throws Exception {
    applicationDataSource = freshDatabase();
    ReplacementStore.repository = new InMemoryProjectionRepository();
    try (var ctx =
        ApplicationContext.builder()
            .properties(storesProperties(REPLACED))
            .eagerInitSingletons(true)
            .start()) {
      assertThat(ctx.getBean(ReadModelStores.class).stores())
          .extracting(ReadModelStore::name)
          .containsExactly("postgres");
      assertThat(tableExists("readmodel_checkpoint"))
          .as("the replaced default store is never created")
          .isFalse();
    }
  }

  /**
   * An application store of another name is added beside the default store: a projection declaring
   * it goes there, a projection over the {@code ProjectionRepository} bean goes to the default
   * store, which is created on the {@code DataSource}.
   */
  @Test
  void additionalUserStoreKeepsTheDefault() throws Exception {
    applicationDataSource = freshDatabase();
    SearchStore.repository = new InMemoryProjectionRepository();
    try (var ctx = ApplicationContext.run(storesProperties(ADDED))) {
      assertThat(ctx.getBean(ReadModelStores.class).stores())
          .extracting(ReadModelStore::name)
          .containsExactly("postgres", "search");
      MultiProjectionRunner runner = frameworkRunner(ctx);
      assertThat(targetNameOf(runner, SearchCatalog.NAME)).isEqualTo("search");
      assertThat(processorOf(runner, SearchCatalog.NAME)).isSameAs(SearchStore.repository);
      assertThat(targetNameOf(runner, StoresTransactional.NAME)).isEqualTo("postgres");
      assertThat(processorOf(runner, StoresTransactional.NAME))
          .isInstanceOf(JdbcProjectionRepository.class);
      assertThat(tableExists("readmodel_checkpoint"))
          .as("the default store is created on the DataSource")
          .isTrue();
    }
  }

  /**
   * Two stores with the same name fail the start as a store error, while the stores are collected —
   * before any registration is routed and before the default store is created.
   */
  @Test
  void duplicateStoreNamesFailStartup() throws Exception {
    applicationDataSource = freshDatabase();
    assertThatThrownBy(() -> ApplicationContext.run(storesProperties(DUPLICATE)).close())
        .hasStackTraceContaining("Two read-model stores are named 'search'");
    assertThat(tableExists("readmodel_checkpoint"))
        .as("the default store is not created for an invalid store set")
        .isFalse();
  }

  /**
   * Each index is declared on the store its projection is routed to while the context starts: the
   * added store's repository receives the routed projection's index and the default store does not;
   * an index for a projection with no registration goes to the default store's database.
   */
  @Test
  void indexIsDeclaredOnTheRoutedStore() throws Exception {
    applicationDataSource = freshDatabase();
    SearchStore.repository = new InMemoryProjectionRepository();
    try (var ctx = ApplicationContext.run(storesProperties(INDEXED))) {
      assertThat(SearchStore.repository.declaredIndexes()).containsExactly(ROUTED_INDEX);
      assertThat(indexCount("unregistered_view_model_view"))
          .as("the unregistered projection's index is on the default store")
          .isPositive();
      assertThat(tableExists(SearchCatalog.NAME + "_view"))
          .as("the routed projection's index never reaches the default store")
          .isFalse();
    }
  }

  /**
   * With an added store beside it, the injectable {@code ProjectionRepository} bean stays the
   * default store's repository.
   */
  @Test
  void primaryProjectionRepositoryBeanIsTheDefaultStoresRepository() throws Exception {
    applicationDataSource = freshDatabase();
    SearchStore.repository = new InMemoryProjectionRepository();
    try (var ctx = ApplicationContext.run(storesProperties(PRIMARY))) {
      ReadModelStore defaultStore =
          ctx.getBean(
              ReadModelStore.class,
              Qualifiers.byName(StreamRuneMicronautModule.DEFAULT_READ_MODEL_STORE_BEAN));
      assertThat(defaultStore.name()).isEqualTo("postgres");
      assertThat(ctx.getBean(ProjectionRepository.class))
          .isSameAs(defaultStore.repository())
          .isNotSameAs(SearchStore.repository);
    }
  }

  /**
   * An unqualified {@code ReadModelStore} injection point of the application resolves to its own
   * store, not to the default one beside it.
   */
  @Test
  void anUnqualifiedReadModelStoreInjection_resolvesToTheApplicationsStore() throws Exception {
    applicationDataSource = freshDatabase();
    SearchStore.repository = new InMemoryProjectionRepository();
    try (var ctx = ApplicationContext.run(storesProperties(PRIMARY))) {
      ReadModelStore injected = ctx.getBean(InjectsAReadModelStore.class).store;
      assertThat(injected.name()).isEqualTo("search");
      assertThat(injected.repository()).isSameAs(SearchStore.repository);
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

  /** The indexes on {@code table} besides its primary key: the declared ones. */
  private static int indexCount(String table) throws Exception {
    try (var conn = applicationDataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT count(*) FROM pg_indexes WHERE tablename = ? AND indexname NOT LIKE"
                    + " '%_pkey'")) {
      ps.setString(1, table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
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
