package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.Sort;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.integration.ProjectionRoutes;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.postgres.PostgresReadModelStore;
import org.streamrune.postgres.PostgresSubscriptionCheckpointProcessor;
import org.streamrune.postgres.SchemaValidationException;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryProjectionRepository;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The default read-model store and the event store's subscription checkpoint the Quarkus
 * integration produces, on a real PostgreSQL: creating the store touches the database (server
 * version, migration, validation), so these run where {@code StreamRuneProducersTest}'s mocks
 * cannot.
 */
@Testcontainers
class QuarkusReadModelStoreWiringTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_readmodel");

  private static final StreamRuneProducers PRODUCERS = new StreamRuneProducers();

  @BeforeAll
  static void noop() {}

  /** A database of its own, with no schema in it. */
  private static PGSimpleDataSource freshDatabase() throws Exception {
    String database = "rm_" + UUID.randomUUID().toString().replace("-", "");
    try (var conn = PG.createConnection("");
        var statement = conn.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
    }
    var dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl().replace("/streamrune_readmodel", "/" + database));
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    return dataSource;
  }

  private static boolean tableExists(DataSource dataSource, String table) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
      ps.setString(1, table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> satisfied(T value) {
    Instance<T> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.isResolvable()).thenReturn(true);
    Mockito.when(instance.get()).thenReturn(value);
    return instance;
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unsatisfied() {
    Instance<T> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(true);
    Mockito.when(instance.isResolvable()).thenReturn(false);
    return instance;
  }

  private static ObjectMapper mapperOf(ProjectionRepository repository) throws Exception {
    Field field = repository.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    return (ObjectMapper) field.get(repository);
  }

  // ── the producer ────────────────────────────────────────────────────────────────

  /**
   * The default store, named {@code postgres}, migrates the read-model series by default and
   * encrypts {@code @Encrypted} read-model fields with the {@code CryptoEngine} bean: its
   * repository's mapper carries {@code CryptoShreddingModule}.
   */
  @Test
  void theDefaultStoreMigratesItsSeries_andItsRepositoryEncryptsWithTheCryptoEngine()
      throws Exception {
    DataSource dataSource = freshDatabase();
    CryptoEngine engine = new InMemoryCryptoEngine();

    ReadModelStore store =
        PRODUCERS.postgresReadModelStore(
            dataSource, TestProperties.defaults(), satisfied(engine), unsatisfied());

    assertThat(store).isInstanceOf(PostgresReadModelStore.class);
    assertThat(store.name()).isEqualTo(StreamRuneProducers.DEFAULT_READ_MODEL_STORE);
    assertThat(tableExists(dataSource, "readmodel_checkpoint")).isTrue();
    assertThat(mapperOf(store.repository()).getRegisteredModuleIds())
        .contains("CryptoShreddingModule");
  }

  @Test
  void withoutACryptoEngine_theRepositorysMapperIsCryptoBlind() throws Exception {
    ReadModelStore store =
        PRODUCERS.postgresReadModelStore(
            freshDatabase(), TestProperties.defaults(), unsatisfied(), unsatisfied());

    assertThat(mapperOf(store.repository()).getRegisteredModuleIds())
        .doesNotContain("CryptoShreddingModule");
  }

  /**
   * {@code streamrune.read-model.schema.auto-initialize=false} leaves the read-model series
   * unapplied, so validation — always on — refuses the store on a database without it.
   */
  @Test
  void theReadModelSwitchOff_appliesNothing_andValidationRefusesAnEmptyDatabase() throws Exception {
    DataSource dataSource = freshDatabase();
    var properties =
        TestProperties.of(Map.of("streamrune.read-model.schema.auto-initialize", "false"));

    assertThatThrownBy(
            () ->
                PRODUCERS.postgresReadModelStore(
                    dataSource, properties, unsatisfied(), unsatisfied()))
        .isInstanceOf(SchemaValidationException.class);
    assertThat(tableExists(dataSource, "readmodel_checkpoint")).isFalse();
  }

  /** Unset, the read-model switch follows {@code streamrune.event-store.schema.auto-initialize}. */
  @Test
  void unset_theReadModelSwitchFollowsTheEventStoreSwitch() throws Exception {
    DataSource dataSource = freshDatabase();
    var properties =
        TestProperties.of(Map.of("streamrune.event-store.schema.auto-initialize", "false"));

    assertThatThrownBy(
            () ->
                PRODUCERS.postgresReadModelStore(
                    dataSource, properties, unsatisfied(), unsatisfied()))
        .isInstanceOf(SchemaValidationException.class);
    assertThat(tableExists(dataSource, "readmodel_checkpoint")).isFalse();

    var on =
        TestProperties.of(
            Map.of(
                "streamrune.event-store.schema.auto-initialize", "false",
                "streamrune.read-model.schema.auto-initialize", "true"));
    PRODUCERS.postgresReadModelStore(dataSource, on, unsatisfied(), unsatisfied());
    assertThat(tableExists(dataSource, "readmodel_checkpoint")).isTrue();
  }

  // ── in a real Arc container ─────────────────────────────────────────────────────

  /** The DataSource and the bound mapping a Quarkus application's extensions supply. */
  @ApplicationScoped
  public static class Infrastructure {
    static volatile DataSource dataSource;

    @Produces
    @Singleton
    public DataSource dataSource() {
      return dataSource;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return TestProperties.defaults();
    }
  }

  /**
   * In an application with a {@code DataSource}: the {@code postgres} store bean, the {@code
   * ProjectionRepository} bean that is its repository, and the subscription checkpoint named
   * {@value StreamRuneProducers#SUBSCRIPTION_CHECKPOINT} — the {@code OffsetStore} bean and the
   * fencing processor built from it.
   */
  @Test
  void inArc_theDefaultStore_itsRepository_andTheFencingSubscriptionCheckpoint() throws Exception {
    Infrastructure.dataSource = freshDatabase();
    try (var arc =
        RealArcTestContainer.boot(List.of(StreamRuneProducers.class, Infrastructure.class))) {
      var stores =
          arc.container().select(ReadModelStore.class, Any.Literal.INSTANCE).stream().toList();
      assertThat(stores).extracting(ReadModelStore::name).containsExactly("postgres");
      ReadModelStore store =
          arc.container()
              .select(
                  ReadModelStore.class,
                  NamedLiteral.of(StreamRuneProducers.DEFAULT_READ_MODEL_STORE_BEAN))
              .get();
      assertThat(store).isSameAs(stores.getFirst());
      assertThat(arc.container().select(ProjectionRepository.class).get())
          .isSameAs(store.repository());

      CheckpointTarget subscription =
          arc.container()
              .select(
                  CheckpointTarget.class,
                  NamedLiteral.of(StreamRuneProducers.SUBSCRIPTION_CHECKPOINT))
              .get();
      OffsetStore offsetStore = arc.container().select(OffsetStore.class).get();
      assertThat(offsetStore).isInstanceOf(PostgresOffsetStore.class);
      assertThat(subscription.checkpoints()).isSameAs(offsetStore);
      assertThat(subscription.processor())
          .isInstanceOf(PostgresSubscriptionCheckpointProcessor.class);
      assertThat(subscription.name()).isEqualTo("subscription_checkpoint");
    }
  }

  // ── several read-model stores: the default replaced only by name ─────────────────

  /** Leadership off: the in-memory stores need no fencing, and no lease heartbeat starts. */
  static final Map<String, String> ROUTING_PROPERTIES =
      Map.of("streamrune.subscription.single-active-consumer.enabled", "false");

  /** The DataSource, the bound mapping and the configuration the routing producers read. */
  @ApplicationScoped
  public static class RoutingInfrastructure {
    static volatile DataSource dataSource;

    @Produces
    @Singleton
    public DataSource dataSource() {
      return dataSource;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return TestProperties.of(ROUTING_PROPERTIES);
    }

    @Produces
    @Singleton
    public org.eclipse.microprofile.config.Config config() {
      return TestProperties.config(ROUTING_PROPERTIES);
    }
  }

  /** The application's own store named {@code postgres}: it replaces the default one. */
  @ApplicationScoped
  public static class ApplicationPostgresStore {
    static final InMemoryProjectionRepository REPOSITORY = new InMemoryProjectionRepository();

    @Produces
    @Singleton
    public ReadModelStore appPostgres() {
      return ReadModelStore.of("postgres", REPOSITORY, REPOSITORY, REPOSITORY);
    }
  }

  /** The application's own store named {@code search}: it is added beside the default one. */
  @ApplicationScoped
  public static class SearchStore {
    static volatile InMemoryProjectionRepository repository = new InMemoryProjectionRepository();

    @Produces
    @Singleton
    public ReadModelStore searchStore() {
      return ReadModelStore.of("search", repository, repository, repository);
    }
  }

  /** A second store named {@code search}. */
  @ApplicationScoped
  public static class SecondSearchStore {
    @Produces
    @Singleton
    public ReadModelStore secondSearch() {
      var repository = new InMemoryProjectionRepository();
      return ReadModelStore.of("search", repository, repository, repository);
    }
  }

  /** A transactional projection over the injected {@code ProjectionRepository} bean. */
  @ProjectionConfig(
      name = RoutedTransactional.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static class RoutedTransactional extends BaseProjection {
    static final String NAME = "routed_tl";

    public RoutedTransactional(ProjectionRepository repository) {
      super(repository, NAME);
    }

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Registers {@link RoutedTransactional}. */
  @ApplicationScoped
  public static class RoutedTransactionalBeans {
    @Produces
    @Singleton
    public RoutedTransactional routedTransactional(ProjectionRepository repository) {
      return new RoutedTransactional(repository);
    }
  }

  /** An at-least-once projection that declares the {@code search} store. */
  @ProjectionConfig(
      name = SearchCatalog.NAME,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
      readModelStore = "search")
  public static class SearchCatalog implements Projection {
    static final String NAME = "search_catalog";

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Registers {@link SearchCatalog}. */
  @ApplicationScoped
  public static class SearchCatalogBeans {
    @Produces
    @Singleton
    public SearchCatalog searchCatalog() {
      return new SearchCatalog();
    }
  }

  static final ProjectionIndex ROUTED_INDEX =
      ProjectionIndex.forSort(ProjectionName.of(SearchCatalog.NAME), Sort.by("title"));
  static final ProjectionIndex UNREGISTERED_INDEX =
      ProjectionIndex.forSort(ProjectionName.of("unregistered_view_model"), Sort.by("title"));

  /** An index of the routed projection, and one of a projection with no registration. */
  @ApplicationScoped
  public static class Indexes {
    @Produces
    @Singleton
    @jakarta.inject.Named("routedIndex")
    public ProjectionIndex routedIndex() {
      return ROUTED_INDEX;
    }

    @Produces
    @Singleton
    @jakarta.inject.Named("unregisteredIndex")
    public ProjectionIndex unregisteredIndex() {
      return UNREGISTERED_INDEX;
    }
  }

  private static RealArcTestContainer bootRouting(DataSource dataSource, Class<?>... beans)
      throws Exception {
    RoutingInfrastructure.dataSource = dataSource;
    var classes =
        new java.util.ArrayList<Class<?>>(
            List.of(
                StreamRuneProducers.class,
                ProjectionProducer.class,
                RoutingInfrastructure.class,
                ProjectionIndex.class));
    classes.addAll(List.of(beans));
    return RealArcTestContainer.boot(classes);
  }

  private static void start(RealArcTestContainer arc) {
    arc.container()
        .beanManager()
        .getEvent()
        .select(io.quarkus.runtime.StartupEvent.class)
        .fire(new io.quarkus.runtime.StartupEvent());
  }

  private static List<String> storeNames(RealArcTestContainer arc) {
    return arc.container().select(ReadModelStores.class).get().stores().stream()
        .map(ReadModelStore::name)
        .toList();
  }

  private static ProjectionRoutes routes(RealArcTestContainer arc) {
    return arc.container().select(ProjectionRoutes.class).get();
  }

  private static int indexCount(DataSource dataSource, String table) throws Exception {
    try (var conn = dataSource.getConnection();
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

  private static String causes(Throwable failure) {
    var messages = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append(" | ");
    }
    return messages.toString();
  }

  /**
   * An application store named {@code postgres} replaces the default store: it is the only store,
   * the {@code ProjectionRepository} bean is its repository, a projection over that repository is
   * routed to it, and the default store is never created: its series never reaches the database.
   */
  @Test
  void userStoreNamedPostgresReplacesTheDefault() throws Exception {
    DataSource dataSource = freshDatabase();
    try (var arc =
        bootRouting(
            dataSource,
            ApplicationPostgresStore.class,
            RoutedTransactionalBeans.class,
            RoutedTransactional.class)) {
      assertThat(storeNames(arc)).containsExactly("postgres");
      assertThat(arc.container().select(ProjectionRepository.class).get())
          .isSameAs(ApplicationPostgresStore.REPOSITORY);
      CheckpointTarget target = routes(arc).target(RoutedTransactional.NAME);
      assertThat(target.name()).isEqualTo("postgres");
      assertThat(target.processor()).isSameAs(ApplicationPostgresStore.REPOSITORY);
      assertThat(tableExists(dataSource, "readmodel_checkpoint"))
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
    DataSource dataSource = freshDatabase();
    SearchStore.repository = new InMemoryProjectionRepository();
    try (var arc =
        bootRouting(
            dataSource,
            SearchStore.class,
            RoutedTransactionalBeans.class,
            RoutedTransactional.class,
            SearchCatalogBeans.class,
            SearchCatalog.class)) {
      assertThat(storeNames(arc)).containsExactly("postgres", "search");
      assertThat(routes(arc).target(SearchCatalog.NAME).name()).isEqualTo("search");
      assertThat(routes(arc).target(SearchCatalog.NAME).processor())
          .isSameAs(SearchStore.repository);
      assertThat(routes(arc).target(RoutedTransactional.NAME).name()).isEqualTo("postgres");
      assertThat(routes(arc).target(RoutedTransactional.NAME).processor())
          .isInstanceOf(org.streamrune.postgres.JdbcProjectionRepository.class);
      assertThat(tableExists(dataSource, "readmodel_checkpoint"))
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
    DataSource dataSource = freshDatabase();
    try (var arc =
        bootRouting(
            dataSource,
            SearchStore.class,
            SecondSearchStore.class,
            SearchCatalogBeans.class,
            SearchCatalog.class,
            Indexes.class,
            ProjectionIndexDeclarer.class)) {
      var failure =
          org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> start(arc));

      assertThat(causes(failure)).contains("Two read-model stores are named 'search'");
      assertThat(tableExists(dataSource, "readmodel_checkpoint"))
          .as("the default store is not created for an invalid store set")
          .isFalse();
    }
  }

  /**
   * Each index is declared on the store its projection is routed to on the startup event: the added
   * store's repository receives the routed projection's index and the default store does not; an
   * index for a projection with no registration goes to the default store's database.
   */
  @Test
  void indexIsDeclaredOnTheRoutedStore() throws Exception {
    DataSource dataSource = freshDatabase();
    SearchStore.repository = new InMemoryProjectionRepository();
    try (var arc =
        bootRouting(
            dataSource,
            SearchStore.class,
            SearchCatalogBeans.class,
            SearchCatalog.class,
            Indexes.class,
            ProjectionIndexDeclarer.class)) {
      start(arc);

      assertThat(SearchStore.repository.declaredIndexes()).containsExactly(ROUTED_INDEX);
      assertThat(indexCount(dataSource, "unregistered_view_model_view"))
          .as("the unregistered projection's index is on the default store")
          .isPositive();
      assertThat(tableExists(dataSource, SearchCatalog.NAME + "_view"))
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
    SearchStore.repository = new InMemoryProjectionRepository();
    try (var arc = bootRouting(freshDatabase(), SearchStore.class)) {
      ReadModelStore defaultStore =
          arc.container()
              .select(
                  ReadModelStore.class,
                  NamedLiteral.of(StreamRuneProducers.DEFAULT_READ_MODEL_STORE_BEAN))
              .get();
      assertThat(defaultStore.name()).isEqualTo("postgres");
      assertThat(arc.container().select(ProjectionRepository.class).get())
          .isSameAs(defaultStore.repository())
          .isNotSameAs(SearchStore.repository);
    }
  }

  /**
   * Without a usable database the default store cannot be created, and the failure says what an
   * application without a database does: Quarkus always has a DataSource bean, so it produces its
   * own store named {@code postgres}.
   */
  @Test
  void withoutAUsableDatabase_theDefaultStoreSaysToProduceAStore() throws Exception {
    DataSource unusable = Mockito.mock(DataSource.class);
    Mockito.when(unusable.getConnection()).thenThrow(new java.sql.SQLException("no database"));

    assertThatThrownBy(
            () ->
                PRODUCERS.postgresReadModelStore(
                    unusable, TestProperties.defaults(), unsatisfied(), unsatisfied()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "an application without a database produces its own ReadModelStore bean")
        .hasMessageContaining("named 'postgres'")
        .hasRootCauseMessage("no database");
  }
}
