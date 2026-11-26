package org.streamrune.crypto.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteEventStoreFactory;
import org.streamrune.sqlite.SqliteSagaStore;
import org.streamrune.sqlite.SqliteSchemaSeries;

/**
 * The crypto series beside the event store's and the read model's in one file, applied by the
 * event-store factory for the engine; and the stores that encrypt outside a projection batch do so
 * before they take the write lock or borrow a connection, so a key minted for them on the same file
 * never waits for their own lock: shown on a pool of one connection, where a borrow after the
 * store's own would wait for its connection.
 */
class SqliteCryptoOnTheEventStoreFileTest {

  private static final EventType REGISTERED = EventType.of("CustomerRegistered");
  private static final SagaType SAGA = SagaType.of("CustomerSaga");

  /** An event carrying one {@code @Encrypted} PII field keyed off {@code customerId}. */
  record CustomerRegistered(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements DomainEvent {
    @JsonCreator
    CustomerRegistered(
        @JsonProperty("customerId") String customerId, @JsonProperty("email") String email) {
      this.customerId = customerId;
      this.email = email;
    }
  }

  /** Saga state carrying one {@code @Encrypted} PII field keyed off {@code customerId}. */
  record CustomerSagaState(
      SagaStatus status, String customerId, @Encrypted(subjectId = "customerId") String email)
      implements SagaState {
    @JsonCreator
    CustomerSagaState(
        @JsonProperty("status") SagaStatus status,
        @JsonProperty("customerId") String customerId,
        @JsonProperty("email") String email) {
      this.status = status;
      this.customerId = customerId;
      this.email = email;
    }
  }

  @TempDir Path dir;

  private HikariDataSource pool;
  private SqliteDatabase database;

  @AfterEach
  void close() {
    if (database != null) {
      database.close();
    }
    if (pool != null) {
      pool.close();
    }
  }

  private void openPoolOfOne() {
    pool = SqliteFiles.pool(dir.resolve("app.db"), 1);
    database = SqliteDatabase.open(pool);
  }

  private static SimpleEventTypeRegistry registry() {
    return SimpleEventTypeRegistry.builder()
        .registerEvent(REGISTERED, CustomerRegistered.class)
        .build();
  }

  @Test
  void theCryptoSeriesListsExactlyTheScriptsInItsDirectory() throws Exception {
    URL url = getClass().getClassLoader().getResource(SqliteSchemaSeries.CRYPTO.directory());
    List<String> onDisk = new ArrayList<>();
    try (Stream<Path> files = Files.list(Path.of(url.toURI()))) {
      files
          .map(file -> file.getFileName().toString())
          .filter(name -> name.endsWith(".sql"))
          .sorted()
          .forEach(onDisk::add);
    }
    assertThat(SqliteSchemaSeries.CRYPTO.scripts()).isEqualTo(onDisk);
  }

  @Test
  void theThreeSeriesShareOneFileWithSeparateHistories_andEveryCryptoTableIsStrict()
      throws Exception {
    openPoolOfOne();
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    database.migrate(SqliteSchemaSeries.CRYPTO);
    database.migrate(SqliteSchemaSeries.CRYPTO);

    assertThat(
            SqliteFiles.query(
                pool, "SELECT version, script FROM flyway_schema_history_crypto ORDER BY 1"))
        .containsExactly(List.of("0", "<< Flyway Baseline >>"), List.of("001", "V001__crypto.sql"));
    assertThat(
            SqliteFiles.query(
                pool,
                "SELECT name, strict FROM pragma_table_list WHERE name IN"
                    + " ('encryption_keys', 'forgotten_subjects', 'erased_key_generations')"
                    + " ORDER BY name"))
        .containsExactly(
            List.of("encryption_keys", 1),
            List.of("erased_key_generations", 1),
            List.of("forgotten_subjects", 1));
    database.validate(SqliteSchemaSeries.EVENT_STORE);
    database.validate(SqliteSchemaSeries.READ_MODEL);
    database.validate(SqliteSchemaSeries.CRYPTO);
  }

  /**
   * The crypto series creates a table only where it is missing: tables the application provisioned
   * itself stay as they are.
   */
  @Test
  void theCryptoSeriesKeepsTablesTheApplicationProvisioned() throws Exception {
    openPoolOfOne();
    SqliteFiles.execute(
        pool,
        "CREATE TABLE forgotten_subjects (subject_id TEXT PRIMARY KEY, forgotten_at INTEGER NOT"
            + " NULL, key_created_at INTEGER, note TEXT)");

    database.migrate(SqliteSchemaSeries.CRYPTO);

    assertThat(SqliteFiles.query(pool, "SELECT name FROM pragma_table_info('forgotten_subjects')"))
        .contains(List.of("note"));
    database.validate(SqliteSchemaSeries.CRYPTO);
  }

  @Test
  void theEventStoreFactoryAppliesTheCryptoSeriesForTheEngine_andRequiresACustomKeyTable() {
    openPoolOfOne();
    var engine = SqliteCryptoEngine.builder().database(database).build();

    EventStore store =
        new SqliteEventStoreFactory(database, registry()).cryptoEngine(engine).create();

    assertThat(store).isNotNull();
    database.validate(SqliteSchemaSeries.CRYPTO);
    var custom = SqliteCryptoEngine.builder().database(database).tableName("tenant_keys").build();
    assertThatThrownBy(
            () -> new SqliteEventStoreFactory(database, registry()).cryptoEngine(custom).create())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tenant_keys");
  }

  /**
   * The event store serializes an event, and so encrypts its {@code @Encrypted} fields, before its
   * append begins: the first event of a subject mints its key on the same file, on a pool of one.
   */
  @Test
  void anAppendEncryptsBeforeItBegins_soItsFirstKeyIsMintedOnTheSameFile() {
    openPoolOfOne();
    var engine = SqliteCryptoEngine.builder().database(database).build();
    EventStore store =
        new SqliteEventStoreFactory(database, registry()).cryptoEngine(engine).create();
    StreamId stream = StreamId.of(AggregateType.of("customer"), AggregateId.of("c-1"));

    store.append(
        stream,
        List.of(event(stream, new CustomerRegistered("c-1", "kim@example.com"))),
        Version.initial());

    assertThat(engine.isKeyAvailable(SubjectId.of("c-1"))).isTrue();
    var loaded =
        (CustomerRegistered) store.readStream(stream, Version.initial(), 10).getFirst().event();
    assertThat(loaded.email()).isEqualTo("kim@example.com");
  }

  /**
   * The saga store serializes a state, and so encrypts its {@code @Encrypted} fields, before it
   * borrows the connection of its write: the first state of a subject mints its key on the same
   * file, on a pool of one connection, without waiting for the saga store's own lock or connection.
   */
  @Test
  void aSagaStoreEncryptsBeforeItBorrows_soItsFirstKeyIsMintedOnTheSameFile() {
    openPoolOfOne();
    var engine = SqliteCryptoEngine.builder().database(database).build();
    new SqliteEventStoreFactory(database, registry()).cryptoEngine(engine).create();
    var mapper = new ObjectMapper();
    mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    mapper.registerModule(new CryptoShreddingModule(engine));
    var sagas = new SqliteSagaStore(database, mapper);
    var sagaId = SagaId.of("saga-1");

    sagas.create(
        sagaId,
        SAGA,
        new CustomerSagaState(SagaStatus.RUNNING, "c-2", "lee@example.com"),
        SagaStatus.RUNNING);

    assertThat(engine.isKeyAvailable(SubjectId.of("c-2"))).isTrue();
    assertThat(sagas.load(sagaId, SAGA, CustomerSagaState.class).orElseThrow().state().email())
        .isEqualTo("lee@example.com");
  }

  private static EventEnvelope event(StreamId stream, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        Version.initial(),
        REGISTERED,
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of(UUID.randomUUID().toString()),
            null,
            null,
            Instant.parse("2026-11-24T10:00:00Z")));
  }
}
