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
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
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

  /** The application's own read-model store. */
  @ApplicationScoped
  public static class ApplicationStore {
    static final InMemoryProjectionRepository REPOSITORY = new InMemoryProjectionRepository();

    @Produces
    @Singleton
    public ReadModelStore memoryStore() {
      return ReadModelStore.of("memory", REPOSITORY, REPOSITORY, REPOSITORY);
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
                  NamedLiteral.of(StreamRuneProducers.DEFAULT_READ_MODEL_STORE))
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

  /**
   * A {@code ReadModelStore} the application produces replaces the default store: it is the only
   * store the container hands out, the default one is never created (its series is never applied),
   * and with no store named {@code postgres} the {@code ProjectionRepository} bean refuses.
   */
  @Test
  void inArc_anApplicationStoreReplacesTheDefaultOne() throws Exception {
    DataSource dataSource = freshDatabase();
    Infrastructure.dataSource = dataSource;
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, Infrastructure.class, ApplicationStore.class))) {
      var stores =
          arc.container().select(ReadModelStore.class, Any.Literal.INSTANCE).stream().toList();
      assertThat(stores).extracting(ReadModelStore::name).containsExactly("memory");

      assertThatThrownBy(() -> arc.container().select(ProjectionRepository.class).get().toString())
          .hasStackTraceContaining("No read-model store is named 'postgres' (stores: 'memory')");
      assertThat(tableExists(dataSource, "readmodel_checkpoint"))
          .as("the default store is never created")
          .isFalse();
    }
  }
}
