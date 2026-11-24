package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.streamrune.integration.tck.IntegrationWiringContract.assertRefused;
import static org.streamrune.integration.tck.IntegrationWiringContract.assertStarted;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.READ_MODEL_CHECKPOINT;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.SECONDARY_STORE;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.SUBSCRIPTION_CHECKPOINT;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.appendEvents;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.applied;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.assertNoCheckpoint;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.checkpoint;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.epoch;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.eventOffsets;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.onceEach;
import static org.streamrune.integration.tck.MultiReadModelStoreScenario.query;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.integration.BackendSelection;
import org.streamrune.integration.tck.ContractBeans.ContractCommandRegistration;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.AmbiguousProjection;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.AppliedEvent;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.AppliedEventProjection;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.ContractEventTypes;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.DefaultStoreProjection;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.UnroutedProjection;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteGuardException;
import org.streamrune.sqlite.SqliteProjectionRepository;
import org.streamrune.sqlite.SqliteReadModelStore;

/**
 * Two read-model stores on two SQLite database files in one application, on the SQLite backend: the
 * routing and per-store checkpoint assertions of {@link MultiReadModelStoreScenario}, for a backend
 * that serves one application instance. A subclass implements {@link #start} over its framework —
 * the way its {@link IntegrationWiringContract} subclass starts an application — and inherits the
 * tests; the scenario makes its database files itself, HikariCP pools with the connection settings
 * the backend requires ({@link SqliteTestDatabases}).
 *
 * <p>The application ({@link #APPLICATION}), with {@code streamrune.backend=sqlite}:
 *
 * <ul>
 *   <li>file A (the application's {@code DataSource}) holds the event store and the default
 *       read-model store {@code sqlite};
 *   <li>file B holds the read-model store {@code secondary}, an application bean ({@link
 *       SecondaryFileStoreBean}) built with {@code SqliteReadModelStore.create} on a {@code
 *       SqliteDatabase} of its own, so file B has its own instance guard;
 *   <li>{@code p1} ({@link DefaultStoreProjection}) is a {@code TRANSACTIONAL_LOCAL} projection on
 *       the application's {@code ProjectionRepository} — the default store's — routed by its {@code
 *       writeTarget()};
 *   <li>{@code p2} ({@link SecondaryFileProjection}) is a {@code TRANSACTIONAL_LOCAL} projection on
 *       the secondary store's repository that also declares {@code readModelStore = "secondary"};
 *   <li>{@code p3} ({@link UnroutedProjection}) is {@code AT_LEAST_ONCE_IDEMPOTENT} with no write
 *       target: the event store's subscription checkpoint.
 * </ul>
 *
 * <p>Two more projections, each on the application's {@code ProjectionRepository}, run in tests of
 * their own, on file A alone: {@link LookupProjection}, whose updater looks {@code p1}'s read model
 * up through that repository.
 *
 * <p>Leadership is {@code NOOP}, the only one the backend has: no lease is taken and every
 * checkpoint row keeps the epoch {@code 0}. So there is no takeover and no fencing case: the
 * instance guard of each file, which a running application holds, keeps a second instance off both
 * files instead, and the tests assert that both are held.
 *
 * <p>Where a test needs the application to start again, it closes it, closes the handle on file B,
 * and starts a second one on the same two files, file A on a new pool since the container closed
 * the first with its application. The secondary store is a new instance on each start ({@link
 * SecondaryFile#use}).
 */
public abstract class SingleInstanceMultiReadModelStoreScenario {

  /** The name of the default read-model store, the one each integration builds on file A. */
  static final String DEFAULT_STORE = "sqlite";

  /** The name of {@code p1}, routed to the default store by its write target. */
  static final String ON_DEFAULT = DefaultStoreProjection.NAME;

  /** The name of {@code p2}, routed to the secondary store by its declaration. */
  static final String ON_SECONDARY = SecondaryFileProjection.NAME;

  /** The name of {@code p3}, routed to the subscription checkpoint. */
  static final String UNROUTED = UnroutedProjection.NAME;

  /**
   * The application's configuration: the SQLite backend, whose single-active-consumer lease is off
   * by default, and runners that poll every 50 ms.
   */
  static final Map<String, String> PROPERTIES =
      Map.of(
          BackendSelection.PROPERTY,
          "sqlite",
          "streamrune.polling-interval",
          "PT0.05S",
          "streamrune.polling-jitter",
          "PT0S");

  /**
   * The application's beans: the secondary store, the three projections, and a command with its
   * event type.
   */
  static final Set<Class<?>> APPLICATION =
      Set.of(
          ContractEventTypes.class,
          SecondaryFileStoreBean.class,
          DefaultStoreProjection.class,
          SecondaryFileProjection.class,
          UnroutedProjection.class,
          ContractCommandRegistration.class);

  /** Where each test's database files are made; JUnit deletes it after the test. */
  @TempDir Path files;

  private SqliteTestDatabases databases;

  /**
   * Starts an application of this integration on {@code dataSource}, as {@link
   * IntegrationWiringContract#start} does.
   *
   * <p>Besides the contract's beans, an integration declares the scenario's as it declares those of
   * {@link MultiReadModelStoreScenario}: {@link SecondaryFileStoreBean} supplies a {@link
   * ReadModelStore}, {@link DefaultStoreProjection} is built by the container with the
   * application's {@link ProjectionRepository} bean injected into its constructor ({@link
   * ContractBeans#injected}), and the other projections are plain classes.
   *
   * @param dataSource file A, the application's {@code DataSource}
   * @param properties the application's configuration
   * @param extraBeans the beans the application declares
   * @return the application, started or refused
   */
  protected abstract Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans);

  /**
   * How long a test waits for a running application to reach the state it asserts.
   *
   * @return twenty seconds: no lease is waited out on a single-instance backend
   */
  protected Duration patience() {
    return Duration.ofSeconds(20);
  }

  /** Closes the handle on file B and every pool the test made. */
  @AfterEach
  void closeTheFiles() {
    SecondaryFile.close();
    if (databases != null) {
      databases.close();
    }
  }

  /**
   * Each registration's checkpoint lands in the table of the place it is routed to, and nowhere
   * else: {@code p1}'s in file A's {@code readmodel_checkpoint}, {@code p2}'s in file B's {@code
   * readmodel_checkpoint}, {@code p3}'s in file A's {@code subscription_checkpoint}, each at the
   * epoch {@code 0} of a backend without leadership. Each read model lands in its store's file. The
   * health check reports each projection's store and a lag of {@code 0}, measured against that
   * store's checkpoint. While the application runs, the instance guard of each file is held:
   * another process cannot open either.
   */
  @Test
  public void checkpointsLandInEachStoresOwnTable() {
    DataSource a = newDatabaseFile();
    DataSource b = newDatabaseFile();
    SecondaryFile.use(b);
    try (Application application = start(a, PROPERTIES, APPLICATION)) {
      assertStarted(application);
      assertGuardHeld(databases().fileOf(a), "file A, the event store's");
      assertGuardHeld(databases().fileOf(b), "file B, the secondary store's");

      long top = appendEvents(application, a, "landing", 3);
      awaitCheckpoint(a, READ_MODEL_CHECKPOINT, ON_DEFAULT, top);
      awaitCheckpoint(b, READ_MODEL_CHECKPOINT, ON_SECONDARY, top);
      awaitCheckpoint(a, SUBSCRIPTION_CHECKPOINT, UNROUTED, top);

      assertNoCheckpoint(a, READ_MODEL_CHECKPOINT, ON_SECONDARY);
      assertNoCheckpoint(a, READ_MODEL_CHECKPOINT, UNROUTED);
      assertNoCheckpoint(a, SUBSCRIPTION_CHECKPOINT, ON_DEFAULT);
      assertNoCheckpoint(a, SUBSCRIPTION_CHECKPOINT, ON_SECONDARY);
      assertNoCheckpoint(b, READ_MODEL_CHECKPOINT, ON_DEFAULT);
      assertNoCheckpoint(b, READ_MODEL_CHECKPOINT, UNROUTED);
      assertFalse(
          tableExists(b, SUBSCRIPTION_CHECKPOINT),
          "file B holds the read-model series only: no subscription_checkpoint");
      assertEquals(0L, epoch(a, READ_MODEL_CHECKPOINT, ON_DEFAULT), "p1 is unfenced: epoch 0");
      assertEquals(0L, epoch(b, READ_MODEL_CHECKPOINT, ON_SECONDARY), "p2 is unfenced: epoch 0");
      assertEquals(0L, epoch(a, SUBSCRIPTION_CHECKPOINT, UNROUTED), "p3 is unfenced: epoch 0");

      Map<Long, Integer> once = onceEach(eventOffsets(a));
      assertEquals(
          once, applied(application.bean(ProjectionRepository.class), ON_DEFAULT), "p1 in A");
      assertEquals(once, applied(SecondaryFile.store().repository(), ON_SECONDARY), "p2 in B");
      assertTrue(tableExists(a, ON_DEFAULT + "_view"), "p1's read model is in file A");
      assertTrue(tableExists(b, ON_SECONDARY + "_view"), "p2's read model is in file B");
      assertFalse(tableExists(b, ON_DEFAULT + "_view"), "p1's read model is not in file B");
      assertFalse(tableExists(a, ON_SECONDARY + "_view"), "p2's read model is not in file A");

      Map<String, String> stores =
          Map.of(
              ON_DEFAULT, DEFAULT_STORE,
              ON_SECONDARY, SECONDARY_STORE,
              UNROUTED, SUBSCRIPTION_CHECKPOINT);
      Map<String, Object> health =
          MultiReadModelStoreScenario.awaitHealth(
              patience(),
              application,
              details -> MultiReadModelStoreScenario.reportsCaughtUp(details, stores));
      Map<String, Object> details = IntegrationWiringContract.details(health);
      stores.forEach(
          (projection, store) -> {
            assertEquals(
                store,
                String.valueOf(details.get("subscription." + projection + ".store")),
                () -> projection + " must report its store: " + health);
            assertEquals(
                "0",
                String.valueOf(details.get("subscription." + projection + ".lag")),
                () -> projection + " must report lag 0 against its own checkpoint: " + health);
          });
    }
  }

  /**
   * Dropping file B's tables — every table of the secondary store, its checkpoints and its schema
   * history among them — and starting again resets {@code p2} alone: it replays every event from
   * the beginning into the re-created store, while {@code p1}'s checkpoint in file A stays where it
   * was and {@code p1} and {@code p3} re-apply nothing. A last event after the restart is applied
   * exactly once by each, so a replay of {@code p1} would show as a second application of an
   * earlier event.
   */
  @Test
  public void droppingOneStoresTablesResetsOnlyItsProjections() {
    DataSource a = newDatabaseFile();
    DataSource b = newDatabaseFile();
    SecondaryFile.use(b);
    long top;
    try (Application first = start(a, PROPERTIES, APPLICATION)) {
      assertStarted(first);
      top = appendEvents(first, a, "before-drop", 3);
      awaitCheckpoint(a, READ_MODEL_CHECKPOINT, ON_DEFAULT, top);
      awaitCheckpoint(b, READ_MODEL_CHECKPOINT, ON_SECONDARY, top);
      awaitCheckpoint(a, SUBSCRIPTION_CHECKPOINT, UNROUTED, top);
    } finally {
      SecondaryFile.close();
    }

    dropEveryTable(b);
    assertFalse(tableExists(b, READ_MODEL_CHECKPOINT), "file B's tables are dropped");

    // The container closed file A's pool with the first application: the second opens a new one.
    a = databases().reopen(a);
    SecondaryFile.use(b);
    try (Application second = start(a, PROPERTIES, APPLICATION)) {
      assertStarted(second);
      awaitCheckpoint(b, READ_MODEL_CHECKPOINT, ON_SECONDARY, top);
      assertEquals(
          top,
          checkpoint(a, READ_MODEL_CHECKPOINT, ON_DEFAULT),
          "p1's checkpoint in A is untouched");
      assertEquals(
          top,
          checkpoint(a, SUBSCRIPTION_CHECKPOINT, UNROUTED),
          "p3's checkpoint in A is untouched");

      long last = appendEvents(second, a, "after-drop", 1);
      awaitCheckpoint(a, READ_MODEL_CHECKPOINT, ON_DEFAULT, last);
      awaitCheckpoint(b, READ_MODEL_CHECKPOINT, ON_SECONDARY, last);
      awaitCheckpoint(a, SUBSCRIPTION_CHECKPOINT, UNROUTED, last);

      Map<Long, Integer> once = onceEach(eventOffsets(a));
      assertEquals(
          once,
          applied(SecondaryFile.store().repository(), ON_SECONDARY),
          "p2 replayed every event from the beginning into the re-created store, once each");
      assertEquals(
          once,
          applied(second.bean(ProjectionRepository.class), ON_DEFAULT),
          "p1 re-applied nothing: each event once, the last one included");
      assertEquals(
          Map.of(last, 1),
          second.bean(UnroutedProjection.class).applied(),
          "p3 resumed from its checkpoint: after the restart it applied the last event only");
    }
  }

  /**
   * After a restart, a batch whose updater looks another projection's read model up through the
   * application's {@code ProjectionRepository} applies, even when that lookup is the first touch of
   * the other projection's table in the new process. The first application fills {@code p1}'s read
   * model; the second runs {@link LookupProjection} alone, so nothing but its batches reads {@code
   * p1}'s table: each batch finds the table the first process created, without waiting for the
   * write lock the batch itself holds, and records what it looked up.
   */
  @Test
  public void afterARestartABatchLooksAnotherProjectionsReadModelUp() {
    DataSource a = newDatabaseFile();
    long top;
    try (Application first =
        start(
            a,
            PROPERTIES,
            Set.of(
                ContractEventTypes.class,
                DefaultStoreProjection.class,
                ContractCommandRegistration.class))) {
      assertStarted(first);
      top = appendEvents(first, a, "looked-up", 3);
      awaitCheckpoint(a, READ_MODEL_CHECKPOINT, ON_DEFAULT, top);
    }

    // The container closed file A's pool with the first application: the second opens a new one.
    a = databases().reopen(a);
    try (Application second =
        start(
            a,
            PROPERTIES,
            Set.of(
                ContractEventTypes.class,
                LookupProjection.class,
                ContractCommandRegistration.class))) {
      assertStarted(second);
      awaitCheckpoint(a, READ_MODEL_CHECKPOINT, LookupProjection.NAME, top);
      assertEquals(
          onceEach(eventOffsets(a)),
          LookupProjection.lookedUp(second.bean(ProjectionRepository.class)),
          "each batch looked p1's row of its event up: applied once by the first application");
    }
  }

  /**
   * With two transactional stores, a {@code TRANSACTIONAL_LOCAL} projection that exposes no {@code
   * writeTarget()} and declares no store cannot be routed: the start is refused, and the refusal
   * names the projection and both stores.
   */
  @Test
  public void ambiguousTransactionalProjectionFailsStartupNamingBothStores() {
    DataSource a = newDatabaseFile();
    SecondaryFile.use(newDatabaseFile());
    try (Application application =
        start(a, PROPERTIES, Set.of(SecondaryFileStoreBean.class, AmbiguousProjection.class))) {
      assertRefused(
          application,
          AmbiguousProjection.NAME,
          "'" + DEFAULT_STORE + "'",
          "'" + SECONDARY_STORE + "'");
    }
  }

  // ---- the application's beans --------------------------------------------------------------

  /**
   * File B and the secondary store on it. A test names the file before each start; the store is
   * built once per start, by whichever of {@link SecondaryFileStoreBean} and {@link
   * SecondaryFileProjection} the container creates first, so both see the same store. The handle on
   * file B is the application's, as an application opens a second file for its own store; the test
   * closes it once the application has closed ({@link #close()}).
   */
  public static final class SecondaryFile {

    private static DataSource dataSource;
    private static SqliteDatabase database;
    private static SqliteReadModelStore store;

    private SecondaryFile() {}

    /**
     * Names file B for the next start, closing the handle of an earlier start; the next {@link
     * #store()} opens a new handle and builds a new store on it.
     *
     * @param file a pool on file B
     */
    public static synchronized void use(DataSource file) {
      close();
      dataSource = file;
    }

    /**
     * The secondary store of the current start, built on first use: {@code SqliteDatabase.open} on
     * file B (its checks and its instance guard), then {@code SqliteReadModelStore.create} with the
     * read-model series migrated and validated.
     *
     * @return the store
     * @throws IllegalStateException when no file was named
     */
    public static synchronized SqliteReadModelStore store() {
      if (store == null) {
        if (dataSource == null) {
          throw new IllegalStateException("name file B with SecondaryFile.use first");
        }
        database = SqliteDatabase.open(dataSource);
        store =
            SqliteReadModelStore.create(
                SECONDARY_STORE,
                database,
                SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
                true,
                true);
      }
      return store;
    }

    /**
     * Closes the handle on file B, releasing its instance guard; the next {@link #store()} opens a
     * new one. Call it only after the application that used the store has closed.
     */
    public static synchronized void close() {
      if (database != null) {
        database.close();
      }
      database = null;
      store = null;
    }
  }

  /** The application's second read-model store, {@code secondary}, on file B. */
  public static final class SecondaryFileStoreBean implements Supplier<ReadModelStore> {
    @Override
    public ReadModelStore get() {
      return SecondaryFile.store();
    }
  }

  /**
   * {@code p2}: a {@code TRANSACTIONAL_LOCAL} projection on the secondary store's repository that
   * declares that store by name; its declaration and its {@code writeTarget()} agree.
   */
  @ProjectionConfig(
      name = SecondaryFileProjection.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL,
      readModelStore = SECONDARY_STORE)
  public static final class SecondaryFileProjection extends AppliedEventProjection {

    /** The projection's name. */
    public static final String NAME = "p2_secondary";

    /** A projection on the current start's secondary store. */
    public SecondaryFileProjection() {
      super(SecondaryFile.store().repository(), NAME);
    }
  }

  /** What {@link LookupProjection} keeps per event: what it found in {@code p1}'s read model. */
  public record LookedUp(long offset, int applications) {}

  /**
   * A {@code TRANSACTIONAL_LOCAL} projection on the application's {@code ProjectionRepository}
   * whose updater looks another projection's read model up, {@code p1}'s, through that injected
   * repository, and keeps what it found: one {@link LookedUp} per event, with how often {@code p1}
   * applied the event ({@code 0} when {@code p1} has no row for it).
   */
  @ProjectionConfig(
      name = LookupProjection.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static final class LookupProjection extends BaseProjection {

    /** The projection's name. */
    public static final String NAME = "p4_lookup";

    private final ProjectionRepository repository;

    /**
     * A projection on the application's repository.
     *
     * @param repository the application's {@code ProjectionRepository} bean
     */
    public LookupProjection(ProjectionRepository repository) {
      super(repository, NAME);
      this.repository = repository;
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (EventEnvelope envelope : batch) {
        long offset = envelope.globalOffset().value();
        String id = Long.toString(offset);
        int found =
            repository
                .findById(ProjectionName.of(ON_DEFAULT), id, AppliedEvent.class)
                .map(AppliedEvent::applications)
                .orElse(0);
        save(id, new LookedUp(offset, found));
      }
    }

    /**
     * What the projection looked up, read from {@code repository}.
     *
     * @param repository the application's {@code ProjectionRepository} bean
     * @return {@code p1}'s applications by global offset, as the batches found them
     */
    static Map<Long, Integer> lookedUp(ProjectionRepository repository) {
      Map<Long, Integer> lookedUp = new TreeMap<>();
      for (LookedUp row : repository.findAll(ProjectionName.of(NAME), LookedUp.class)) {
        lookedUp.put(row.offset(), row.applications());
      }
      return lookedUp;
    }
  }

  // ---- the instance guard -------------------------------------------------------------------

  /**
   * Asserts that another process cannot open {@code file}: a forked JVM on this class path runs
   * {@link GuardProbe} on it and is refused with the {@code SqliteGuardException} that says the
   * database {@value GuardProbe#HELD}. Inside this JVM every handle shares the guard, so only
   * another process can tell that it is held. A guard refusal of another kind (the child may not
   * call flock(2), the lock file cannot be opened, the file has more than one hard link) fails the
   * assertion: it says nothing about whether the file is held.
   *
   * @param file the database file
   * @param which what the file is, for the failure message
   */
  public static void assertGuardHeld(Path file, String which) {
    List<String> output = probe(file);
    assertTrue(
        output.contains(GuardProbe.REFUSED),
        () ->
            "the instance guard of "
                + which
                + " must be held while the application runs; another process opening it said:\n  "
                + String.join("\n  ", output));
  }

  /** The output of {@link GuardProbe} run on {@code file} in another JVM. */
  private static List<String> probe(Path file) {
    String java = ProcessHandle.current().info().command().orElse("java");
    List<String> output = new ArrayList<>();
    try {
      Process process =
          new ProcessBuilder(
                  java,
                  "--enable-native-access=ALL-UNNAMED",
                  "-cp",
                  System.getProperty("java.class.path"),
                  GuardProbe.class.getName(),
                  file.toString())
              .redirectErrorStream(true)
              .start();
      try (BufferedReader lines =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        for (String line = lines.readLine(); line != null; line = lines.readLine()) {
          output.add(line);
        }
      }
      if (!process.waitFor(60, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        fail("the guard probe did not end within 60 s: " + output);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("cannot run the guard probe", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while the guard probe ran", e);
    }
    return output;
  }

  /**
   * Another process that tries to open a SQLite file: prints {@value #REFUSED} when its instance
   * guard is held elsewhere (the refusal says the database {@value #HELD}), {@value #OPENED} when
   * the file opened (it is closed again at once), or the failure otherwise, a guard refusal of
   * another kind included, with its message.
   */
  public static final class GuardProbe {

    /** Printed when another process holds the guard. */
    public static final String REFUSED = "REFUSED";

    /** What the guard's refusal says when another process holds it. */
    public static final String HELD = "is in use by another process";

    /** Printed when the file opened: no other process held the guard. */
    public static final String OPENED = "OPENED";

    private GuardProbe() {}

    /**
     * Tries to open the database file given as the only argument.
     *
     * @param args the database file
     */
    public static void main(String[] args) {
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(SqliteTestDatabases.url(Path.of(args[0])));
      config.setMaximumPoolSize(1);
      try (HikariDataSource pool = new HikariDataSource(config)) {
        try (SqliteDatabase database = SqliteDatabase.open(pool)) {
          System.out.println(OPENED);
        } catch (SqliteGuardException refused) {
          String message = String.valueOf(refused.getMessage());
          System.out.println(message.contains(HELD) ? REFUSED : "GUARD REFUSED OTHERWISE");
          System.out.println(message);
        }
      } catch (RuntimeException e) {
        System.out.println("FAILED " + e);
      }
    }
  }

  // ---- what the files hold ------------------------------------------------------------------

  private DataSource newDatabaseFile() {
    return databases().create();
  }

  private SqliteTestDatabases databases() {
    if (databases == null) {
      databases = new SqliteTestDatabases(files);
    }
    return databases;
  }

  private void awaitCheckpoint(DataSource database, String table, String projection, long offset) {
    MultiReadModelStoreScenario.awaitCheckpoint(patience(), database, table, projection, offset);
  }

  /** Whether {@code database} has a table named {@code table}. */
  static boolean tableExists(DataSource database, String table) {
    return query(
        database,
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        List.of(table),
        rs -> rs.next() && rs.getInt(1) > 0);
  }

  /**
   * Drops every table of {@code database}, SQLite's own aside, with foreign keys off on the
   * connection that drops them, so their order does not matter.
   */
  static void dropEveryTable(DataSource database) {
    List<String> tables =
        query(
            database,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            List.of(),
            rs -> {
              List<String> names = new ArrayList<>();
              while (rs.next()) {
                names.add(rs.getString(1));
              }
              return names;
            });
    try (Connection connection = database.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("PRAGMA foreign_keys = OFF");
      try {
        for (String table : tables) {
          statement.execute("DROP TABLE \"" + table.replace("\"", "\"\"") + "\"");
        }
      } finally {
        statement.execute("PRAGMA foreign_keys = ON");
      }
    } catch (SQLException e) {
      throw new IllegalStateException("cannot drop the tables " + tables, e);
    }
  }
}
