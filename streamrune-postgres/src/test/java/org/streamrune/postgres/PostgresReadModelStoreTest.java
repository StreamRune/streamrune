package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * {@link PostgresReadModelStore#create} checks the server, applies the read-model series on its own
 * history table and validates it, each schema step switchable; and the store it builds is one write
 * target whose checkpoints are the ones its processor commits. Each test that touches a schema runs
 * on a database of its own, so "fresh database" means one.
 */
@Testcontainers
class PostgresReadModelStoreTest {

  private static final AtomicInteger DATABASES = new AtomicInteger();

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("readmodel_store");

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void createMigratesIntoItsOwnHistoryTable() throws SQLException {
    var dataSource = freshDatabase();

    PostgresReadModelStore.create("postgres", dataSource, mapper, true, true);

    assertTrue(tableExists(dataSource, PostgresReadModelStore.READ_MODEL_HISTORY_TABLE));
    assertEquals(
        List.of("V001__readmodel_baseline.sql"),
        appliedScripts(dataSource, "flyway_schema_history_readmodel"));
    assertTrue(tableExists(dataSource, "readmodel_checkpoint"));
    // The read-model database need not hold the events: the event-store series is not applied.
    assertFalse(tableExists(dataSource, "event_stream"));
    assertFalse(tableExists(dataSource, PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE));
  }

  @Test
  void createWithValidateOnlyFailsOnMissingTable() throws SQLException {
    var dataSource = freshDatabase();

    var refused =
        assertThrows(
            SchemaValidationException.class,
            () -> PostgresReadModelStore.create("postgres", dataSource, mapper, false, true));

    assertTrue(refused.getMessage().contains("readmodel_checkpoint"), refused.getMessage());
    assertFalse(refused.result().isValid());
    assertFalse(tableExists(dataSource, "readmodel_checkpoint"), "nothing was migrated");
    assertFalse(tableExists(dataSource, PostgresReadModelStore.READ_MODEL_HISTORY_TABLE));
  }

  @Test
  void repositoryAndProcessorShareTheWriteTarget() throws SQLException {
    var store = PostgresReadModelStore.create("postgres", freshDatabase(), mapper, true, true);

    assertSame(store.repository(), store.processor());
    assertTrue(store.processor().writesTo(store.repository()));
    assertTrue(store.processor().transactional());
    assertTrue(store.processor().fencesCheckpoint());
    assertEquals("postgres", store.name());
    assertInstanceOf(PostgresReadModelCheckpoints.class, store.checkpoints());
    var target = store.checkpointTarget();
    assertEquals("postgres", target.name());
    assertSame(store.checkpoints(), target.checkpoints());
    assertSame(store.processor(), target.processor());
  }

  /**
   * A batch the processor commits is the checkpoint {@code checkpoints()} reads, and it lands in
   * {@code readmodel_checkpoint}, never in the event store's {@code subscription_checkpoint}.
   */
  @Test
  void theProcessorCommitsTheCheckpointsTheStoreReads() throws SQLException {
    var dataSource = freshDatabase();
    new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
        .initializeSchema();
    ReadModelStore store =
        PostgresReadModelStore.create("postgres", dataSource, mapper, true, true);
    var name = ProjectionName.of("orders");

    store
        .processor()
        .executeAtomically(
            name,
            List.of(),
            GlobalOffset.of(7),
            0L,
            repo -> repo.save(name, "o-1", new View("o-1")),
            store.checkpoints());

    assertEquals(GlobalOffset.of(7), store.checkpoints().getLastOffset(name));
    assertEquals(
        GlobalOffset.initial(),
        new PostgresOffsetStore(dataSource).getLastOffset(name),
        "the read-model checkpoint is not the subscription checkpoint");
    assertTrue(store.repository().findById(name, "o-1", View.class).isPresent());
  }

  /**
   * A {@link JdbcProjectionRepository} built by either public constructor is the same processor as
   * the store's: its batches and its fence stamps land in {@code readmodel_checkpoint}, read
   * through {@link PostgresReadModelCheckpoints} on the same DataSource, and never in the event
   * store's {@code subscription_checkpoint}.
   */
  @Test
  void aRepositoryFromEitherPublicConstructorCheckpointsInTheReadModelTable() throws SQLException {
    var dataSource = freshDatabase();
    new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
        .initializeSchema();
    PostgresReadModelStore.create("postgres", dataSource, mapper, true, true);
    var readModelCheckpoints = new PostgresReadModelCheckpoints(dataSource);
    var subscriptionCheckpoints = new PostgresOffsetStore(dataSource);

    var repositories =
        List.of(
            new JdbcProjectionRepository(dataSource),
            new JdbcProjectionRepository(dataSource, mapper));
    for (int i = 0; i < repositories.size(); i++) {
      var repository = repositories.get(i);
      var name = ProjectionName.of("public_ctor_" + i);

      repository.stampFencingEpoch(name, 3L);
      repository.executeAtomically(
          name,
          List.of(),
          GlobalOffset.of(5),
          3L,
          repo -> repo.save(name, "v-1", new View("v-1")),
          readModelCheckpoints);

      assertEquals(GlobalOffset.of(5), readModelCheckpoints.getLastOffset(name));
      assertEquals(3L, epochIn(dataSource, "readmodel_checkpoint", name));
      assertEquals(
          GlobalOffset.initial(),
          subscriptionCheckpoints.getLastOffset(name),
          "nothing lands in the subscription checkpoint");
      assertEquals(-1L, epochIn(dataSource, "subscription_checkpoint", name), "no row stamped");
    }
  }

  /**
   * The read-model series is baselined at 0 like the framework's other series, so it applies in
   * full to a schema that already holds the event-store series (the default single-database
   * deployment), and a second start applies nothing. The event-store factory, in turn, leaves the
   * read-model series alone.
   */
  @Test
  void createAppliesTheSeriesBesideTheEventStoreSeriesAndAgainAsANoOp() throws SQLException {
    var dataSource = freshDatabase();
    new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
        .initializeSchema();
    // The event-store factory applies its own series only, never the read-model one.
    assertFalse(tableExists(dataSource, "readmodel_checkpoint"));
    assertFalse(tableExists(dataSource, PostgresReadModelStore.READ_MODEL_HISTORY_TABLE));

    PostgresReadModelStore.create("postgres", dataSource, mapper, true, true);
    PostgresReadModelStore.create("postgres", dataSource, mapper, true, true);

    assertEquals(
        List.of("V001__readmodel_baseline.sql"),
        appliedScripts(dataSource, PostgresReadModelStore.READ_MODEL_HISTORY_TABLE));
    assertEquals(
        List.of("V001__streamrune_baseline.sql"),
        appliedScripts(dataSource, PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE));
    assertTrue(tableExists(dataSource, "readmodel_checkpoint"));
    assertTrue(tableExists(dataSource, "subscription_checkpoint"));
  }

  /** With both schema steps off, the store is built on the schema as it is. */
  @Test
  void createWithNeitherStepLeavesTheSchemaAlone() throws SQLException {
    var dataSource = freshDatabase();

    var store = PostgresReadModelStore.create("search", dataSource, mapper, false, false);

    assertEquals("search", store.name());
    assertFalse(tableExists(dataSource, "readmodel_checkpoint"));
    assertFalse(tableExists(dataSource, PostgresReadModelStore.READ_MODEL_HISTORY_TABLE));
  }

  /**
   * The server-version check runs first and cannot be switched off: an older server is refused with
   * the one probe connection, before any migration or validation borrows another.
   */
  @Test
  void anOlderServerIsRefusedBeforeAnythingIsMigrated() throws SQLException {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData meta = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(meta);
    when(meta.getDatabaseMajorVersion()).thenReturn(16);
    when(meta.getDatabaseProductVersion()).thenReturn("16.4");

    for (boolean flag : new boolean[] {true, false}) {
      assertThrows(
          UnsupportedServerVersionException.class,
          () -> PostgresReadModelStore.create("postgres", dataSource, mapper, flag, flag));
    }
    verify(dataSource, times(2)).getConnection();
  }

  @Test
  void requiredArgumentsAreCheckedBeforeTheDatabaseIsTouched() {
    DataSource untouched = mock(DataSource.class);

    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresReadModelStore.create(null, untouched, mapper, true, true));
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresReadModelStore.create(" ", untouched, mapper, true, true));
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresReadModelStore.create("postgres", null, mapper, true, true));
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresReadModelStore.create("postgres", untouched, null, true, true));
    verifyNoInteractions(untouched);
  }

  record View(String id) {}

  // --- helpers ---

  private static PGSimpleDataSource freshDatabase() throws SQLException {
    String db = "readmodel_" + DATABASES.incrementAndGet();
    try (Connection conn = dataSource(PG.getDatabaseName()).getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE DATABASE " + db);
    }
    return dataSource(db);
  }

  private static PGSimpleDataSource dataSource(String database) {
    var ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {PG.getHost()});
    ds.setPortNumbers(new int[] {PG.getMappedPort(5432)});
    ds.setDatabaseName(database);
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    return ds;
  }

  /** The epoch stamped on {@code name}'s row in {@code table}, or {@code -1} when it has none. */
  private static long epochIn(DataSource dataSource, String table, ProjectionName name)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement("SELECT epoch FROM " + table + " WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : -1L;
      }
    }
  }

  private static boolean tableExists(DataSource dataSource, String table) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
      ps.setString(1, "public." + table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    }
  }

  private static List<String> appliedScripts(DataSource dataSource, String historyTable)
      throws SQLException {
    var scripts = new ArrayList<String>();
    try (Connection conn = dataSource.getConnection();
        var st = conn.createStatement();
        var rs =
            st.executeQuery(
                "SELECT script FROM "
                    + historyTable
                    + " WHERE success AND version IS NOT NULL AND version <> '0'"
                    + " ORDER BY installed_rank")) {
      while (rs.next()) {
        scripts.add(rs.getString(1));
      }
    }
    return scripts;
  }
}
