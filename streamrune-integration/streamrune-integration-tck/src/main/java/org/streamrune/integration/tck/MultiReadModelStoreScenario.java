package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.streamrune.integration.tck.IntegrationWiringContract.assertRefused;
import static org.streamrune.integration.tck.IntegrationWiringContract.assertStarted;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.tck.ContractBeans.ContractCommand;
import org.streamrune.integration.tck.ContractBeans.ContractCommandExecuted;
import org.streamrune.integration.tck.ContractBeans.ContractCommandRegistration;
import org.streamrune.integration.tck.ContractBeans.ContractCommandState;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.PostgresReadModelStore;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Two PostgreSQL read-model stores on two databases in one application, as tests that run in each
 * integration's own test sources against its own container: the routing proof of the read-model
 * stores design (spec §7) without a second technology. A subclass implements the two hooks over its
 * framework — the way its {@link IntegrationWiringContract} subclass starts an application — and
 * inherits the tests.
 *
 * <p>The application ({@link #APPLICATION}):
 *
 * <ul>
 *   <li>database A ({@link #postgresql()}, the application's {@code DataSource}) holds the event
 *       store and the default read-model store {@code postgres};
 *   <li>database B (a second {@link #postgresql()}: another database on the same server) holds the
 *       read-model store {@code secondary}, an application bean ({@link SecondaryStoreBean}) built
 *       with {@code PostgresReadModelStore.create} on it;
 *   <li>{@code p1} ({@link DefaultStoreProjection}) is a {@code TRANSACTIONAL_LOCAL} {@link
 *       BaseProjection} on the application's {@code ProjectionRepository} — the default store's —
 *       routed by its {@code writeTarget()};
 *   <li>{@code p2} ({@link SecondaryStoreProjection}) is a {@code TRANSACTIONAL_LOCAL} {@link
 *       BaseProjection} on the secondary store's repository that also declares {@code
 *       readModelStore = "secondary"}: the declaration and the write target agree;
 *   <li>{@code p3} ({@link UnroutedProjection}) is {@code AT_LEAST_ONCE_IDEMPOTENT} with no write
 *       target: the event store's subscription checkpoint.
 * </ul>
 *
 * <p>The single-active-consumer lease is on, so every registration runs under the real leadership
 * the integration builds on database A, and the epoch it issues is stamped into each registration's
 * own checkpoint row — in database B for {@code p2}.
 *
 * <p>Where a test needs the application to start again, it closes it and starts a second one on the
 * same two databases: a restart, which is also a takeover of every lease by a new leader. The
 * secondary store is a new instance on each start ({@link SecondaryDatabase#use}).
 */
public abstract class MultiReadModelStoreScenario {

  /** The name of the default read-model store, the one each integration builds on database A. */
  static final String DEFAULT_STORE = "postgres";

  /** The name of the application's second read-model store, on database B. */
  static final String SECONDARY_STORE = "secondary";

  /** The name of {@code p1}, routed to the default store by its write target. */
  static final String ON_DEFAULT = DefaultStoreProjection.NAME;

  /** The name of {@code p2}, routed to the secondary store by its declaration. */
  static final String ON_SECONDARY = SecondaryStoreProjection.NAME;

  /** The name of {@code p3}, routed to the subscription checkpoint. */
  static final String UNROUTED = UnroutedProjection.NAME;

  /** The checkpoint table of a PostgreSQL read-model store, in the store's own database. */
  static final String READ_MODEL_CHECKPOINT = "readmodel_checkpoint";

  /** The checkpoint table of the event store's subscription checkpoint, in database A. */
  static final String SUBSCRIPTION_CHECKPOINT = OffsetStore.SUBSCRIPTION_CHECKPOINT;

  /**
   * The application's configuration: the single-active-consumer lease on (the default with a {@code
   * DataSource}, stated), and runners that poll every 50 ms.
   */
  static final Map<String, String> PROPERTIES =
      Map.of(
          "streamrune.subscription.single-active-consumer.enabled", "true",
          "streamrune.polling-interval", "PT0.05S",
          "streamrune.polling-jitter", "PT0S");

  /**
   * The application's beans: the secondary store, the three projections, and a command with its
   * event type.
   */
  static final Set<Class<?>> APPLICATION =
      Set.of(
          ContractEventTypes.class,
          SecondaryStoreBean.class,
          DefaultStoreProjection.class,
          SecondaryStoreProjection.class,
          UnroutedProjection.class,
          ContractCommandRegistration.class);

  /**
   * Starts an application of this integration on {@code dataSource}, as {@link
   * IntegrationWiringContract#start} does.
   *
   * <p>Besides the contract's beans, an integration declares the scenario's: {@link
   * SecondaryStoreBean} supplies a {@link ReadModelStore}, {@link DefaultStoreProjection} is built
   * by the container with the application's {@link ProjectionRepository} bean injected into its
   * constructor ({@link ContractBeans#injected}), and the other projections are plain classes.
   *
   * @param dataSource database A, the application's {@code DataSource}
   * @param properties the application's configuration
   * @param extraBeans the beans the application declares
   * @return the application, started or refused
   */
  protected abstract Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans);

  /**
   * A {@code DataSource} on an empty PostgreSQL database that no earlier call returned, on the same
   * server for every call of one test class.
   *
   * @return the data source
   */
  protected abstract DataSource postgresql();

  /**
   * How long a test waits for a running application to reach the state it asserts.
   *
   * @return forty seconds: a takeover after a restart may wait out the lease
   */
  protected Duration patience() {
    return Duration.ofSeconds(40);
  }

  /**
   * Each registration's checkpoint lands in the table of the place it is routed to, and nowhere
   * else: {@code p1}'s in database A's {@code readmodel_checkpoint}, {@code p2}'s in database B's
   * {@code readmodel_checkpoint}, {@code p3}'s in database A's {@code subscription_checkpoint}.
   * Each read model lands in its store's database. The health check reports each projection's store
   * and a lag of {@code 0}, measured against that store's checkpoint.
   */
  @Test
  public void checkpointsLandInEachStoresOwnTable() {
    DataSource a = postgresql();
    DataSource b = postgresql();
    SecondaryDatabase.use(b);
    try (Application application = start(a, PROPERTIES, APPLICATION)) {
      assertStarted(application);
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
          "database B holds the read-model series only: no subscription_checkpoint");

      Map<Long, Integer> once = onceEach(eventOffsets(a));
      assertEquals(
          once, applied(application.bean(ProjectionRepository.class), ON_DEFAULT), "p1 in A");
      assertEquals(once, applied(SecondaryDatabase.store().repository(), ON_SECONDARY), "p2 in B");
      assertFalse(tableExists(b, ON_DEFAULT + "_view"), "p1's read model is not in database B");
      assertFalse(tableExists(a, ON_SECONDARY + "_view"), "p2's read model is not in database A");

      Map<String, String> stores =
          Map.of(
              ON_DEFAULT, DEFAULT_STORE,
              ON_SECONDARY, SECONDARY_STORE,
              UNROUTED, SUBSCRIPTION_CHECKPOINT);
      Map<String, Object> health =
          awaitHealth(application, details -> reportsCaughtUp(details, stores));
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
   * Dropping database B's tables — every table of the secondary store, its checkpoints among them —
   * and starting again resets {@code p2} alone: it replays every event from the beginning into the
   * re-created store, while {@code p1}'s checkpoint in database A stays where it was and {@code p1}
   * and {@code p3} re-apply nothing. A last event after the restart is applied exactly once by
   * each, so a replay of {@code p1} would show as a second application of an earlier event.
   */
  @Test
  public void droppingOneStoresTablesResetsOnlyItsProjections() {
    DataSource a = postgresql();
    DataSource b = postgresql();
    SecondaryDatabase.use(b);
    long top;
    try (Application first = start(a, PROPERTIES, APPLICATION)) {
      assertStarted(first);
      top = appendEvents(first, a, "before-drop", 3);
      awaitCheckpoint(a, READ_MODEL_CHECKPOINT, ON_DEFAULT, top);
      awaitCheckpoint(b, READ_MODEL_CHECKPOINT, ON_SECONDARY, top);
      awaitCheckpoint(a, SUBSCRIPTION_CHECKPOINT, UNROUTED, top);
    }

    execute(b, "DROP SCHEMA public CASCADE", "CREATE SCHEMA public");
    assertFalse(tableExists(b, READ_MODEL_CHECKPOINT), "database B's tables are dropped");

    SecondaryDatabase.use(b);
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
          applied(SecondaryDatabase.store().repository(), ON_SECONDARY),
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
   * Under the real leadership, a takeover stamps the new lease epoch into database B's {@code
   * readmodel_checkpoint} row of {@code p2}, before the new leader commits anything there; a commit
   * the superseded leader still had in flight — {@code p2}'s next batch at the old epoch, on the
   * secondary store's processor — is then rejected by that row before its updater runs, and the row
   * and the read model stay as the new leader left them.
   *
   * <p>The takeover is a restart: the first application resigns its leases when it closes, and the
   * second takes each over at the next epoch (as {@code
   * ReadModelStoreOnAnotherDatabaseLeadershipIT} hands a lease from one leader to the next). The
   * stale commit is the first leader's in-flight batch, issued against database B directly.
   */
  @Test
  public void secondStoreIsFencedUnderLeadership() {
    DataSource a = postgresql();
    DataSource b = postgresql();
    SecondaryDatabase.use(b);
    long top;
    long oldEpoch;
    try (Application first = start(a, PROPERTIES, APPLICATION)) {
      assertStarted(first);
      top = appendEvents(first, a, "fenced", 2);
      awaitCheckpoint(b, READ_MODEL_CHECKPOINT, ON_SECONDARY, top);
      oldEpoch = leaseEpoch(a, ON_SECONDARY);
      assertTrue(oldEpoch > 0, "p2 runs under a lease of database A");
      assertEquals(
          oldEpoch,
          epoch(b, READ_MODEL_CHECKPOINT, ON_SECONDARY),
          "the first leader's epoch is stamped into database B's row");
    }

    SecondaryDatabase.use(b);
    try (Application second = start(a, PROPERTIES, APPLICATION)) {
      assertStarted(second);
      await(
          () ->
              leaseEpoch(a, ON_SECONDARY) > oldEpoch
                  && epoch(b, READ_MODEL_CHECKPOINT, ON_SECONDARY) > oldEpoch,
          () ->
              "the takeover must stamp a new epoch into database B's row of p2 (lease epoch "
                  + leaseEpoch(a, ON_SECONDARY)
                  + ", row epoch "
                  + epoch(b, READ_MODEL_CHECKPOINT, ON_SECONDARY)
                  + ", old epoch "
                  + oldEpoch
                  + ")");
      long newEpoch = leaseEpoch(a, ON_SECONDARY);
      assertEquals(
          newEpoch,
          epoch(b, READ_MODEL_CHECKPOINT, ON_SECONDARY),
          "database B's row carries the new leader's epoch");

      PostgresReadModelStore store = SecondaryDatabase.store();
      AtomicBoolean updaterRan = new AtomicBoolean();
      List<EventEnvelope> inFlight = List.of(envelope(top + 1));
      assertThrows(
          ProjectionCommitFencedException.class,
          () ->
              store
                  .processor()
                  .executeAtomically(
                      ProjectionName.of(ON_SECONDARY),
                      inFlight,
                      GlobalOffset.of(top + 1),
                      oldEpoch,
                      repository -> updaterRan.set(true),
                      store.checkpoints()),
          "a commit at the superseded epoch is rejected by database B's row");
      assertFalse(updaterRan.get(), "fenced before the updater ran");
      assertEquals(
          top, checkpoint(b, READ_MODEL_CHECKPOINT, ON_SECONDARY), "the checkpoint did not move");
      assertEquals(
          newEpoch, epoch(b, READ_MODEL_CHECKPOINT, ON_SECONDARY), "the row keeps the new epoch");
      assertFalse(
          applied(store.repository(), ON_SECONDARY).containsKey(top + 1),
          "the fenced batch left no read-model row");
    }
  }

  /**
   * With two transactional stores, a {@code TRANSACTIONAL_LOCAL} projection that exposes no {@code
   * writeTarget()} and declares no store cannot be routed: the start is refused, and the refusal
   * names the projection and both stores.
   */
  @Test
  public void ambiguousTransactionalProjectionFailsStartupNamingBothStores() {
    DataSource a = postgresql();
    SecondaryDatabase.use(postgresql());
    try (Application application =
        start(a, PROPERTIES, Set.of(SecondaryStoreBean.class, AmbiguousProjection.class))) {
      assertRefused(
          application,
          AmbiguousProjection.NAME,
          "'" + DEFAULT_STORE + "'",
          "'" + SECONDARY_STORE + "'");
    }
  }

  // ---- the application's beans --------------------------------------------------------------

  /**
   * Database B and the secondary store on it. A test names the database before each start; the
   * store is built once per start, by whichever of {@link SecondaryStoreBean} and {@link
   * SecondaryStoreProjection} the container creates first, so both see the same store.
   */
  public static final class SecondaryDatabase {

    private static DataSource dataSource;
    private static PostgresReadModelStore store;

    private SecondaryDatabase() {}

    /**
     * Names database B for the next start; the next {@link #store()} builds a new store on it.
     *
     * @param database database B
     */
    public static synchronized void use(DataSource database) {
      dataSource = database;
      store = null;
    }

    /**
     * The secondary store of the current start, built on first use with {@code
     * PostgresReadModelStore.create}: the read-model series migrated and validated on database B.
     *
     * @return the store
     * @throws IllegalStateException when no database was named
     */
    public static synchronized PostgresReadModelStore store() {
      if (store == null) {
        if (dataSource == null) {
          throw new IllegalStateException("name database B with SecondaryDatabase.use first");
        }
        store =
            PostgresReadModelStore.create(
                SECONDARY_STORE,
                dataSource,
                JdbcProjectionRepository.createObjectMapper(null),
                true,
                true);
      }
      return store;
    }
  }

  /** The application's second read-model store, {@code secondary}, on database B. */
  public static final class SecondaryStoreBean implements Supplier<ReadModelStore> {
    @Override
    public ReadModelStore get() {
      return SecondaryDatabase.store();
    }
  }

  /**
   * The application's event types: {@link ContractCommandExecuted}, which the projections read back
   * from the PostgreSQL event store.
   */
  public static final class ContractEventTypes implements Supplier<EventTypeRegistry> {
    @Override
    public EventTypeRegistry get() {
      return SimpleEventTypeRegistry.builder()
          .registerEvent(ContractCommandExecuted.class)
          .registerState("ContractCommandState", ContractCommandState.class)
          .build();
    }
  }

  /** What a counting projection keeps per event: its offset and how often it was applied. */
  public record AppliedEvent(long offset, int applications) {}

  /**
   * Keeps one {@link AppliedEvent} per event, through {@link BaseProjection}'s helpers: in a
   * transactional registration the count and the checkpoint commit together.
   */
  public abstract static class AppliedEventProjection extends BaseProjection {

    /**
     * A projection on {@code repository}.
     *
     * @param repository where its read models live
     * @param name the projection's name
     */
    protected AppliedEventProjection(ProjectionRepository repository, String name) {
      super(repository, name);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (EventEnvelope envelope : batch) {
        long offset = envelope.globalOffset().value();
        String id = Long.toString(offset);
        int before = findById(id, AppliedEvent.class).map(AppliedEvent::applications).orElse(0);
        save(id, new AppliedEvent(offset, before + 1));
      }
    }
  }

  /**
   * {@code p1}: a {@code TRANSACTIONAL_LOCAL} projection on the application's {@code
   * ProjectionRepository}, which the container injects — the default store's repository, so its
   * {@code writeTarget()} routes it to that store.
   */
  @ProjectionConfig(
      name = DefaultStoreProjection.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static final class DefaultStoreProjection extends AppliedEventProjection {

    /** The projection's name. */
    public static final String NAME = "p1_default";

    /**
     * A projection on the application's repository.
     *
     * @param repository the application's {@code ProjectionRepository} bean
     */
    public DefaultStoreProjection(ProjectionRepository repository) {
      super(repository, NAME);
    }
  }

  /**
   * {@code p2}: a {@code TRANSACTIONAL_LOCAL} projection on the secondary store's repository that
   * declares that store by name; its declaration and its {@code writeTarget()} agree.
   */
  @ProjectionConfig(
      name = SecondaryStoreProjection.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL,
      readModelStore = SECONDARY_STORE)
  public static final class SecondaryStoreProjection extends AppliedEventProjection {

    /** The projection's name. */
    public static final String NAME = "p2_secondary";

    /** A projection on the current start's secondary store. */
    public SecondaryStoreProjection() {
      super(SecondaryDatabase.store().repository(), NAME);
    }
  }

  /**
   * {@code p3}: an {@code AT_LEAST_ONCE_IDEMPOTENT} projection with no write target, so its
   * checkpoint is the event store's subscription checkpoint. It counts in memory how often it
   * applied each event.
   */
  @ProjectionConfig(
      name = UnroutedProjection.NAME,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  public static final class UnroutedProjection implements Projection {

    /** The projection's name. */
    public static final String NAME = "p3_unrouted";

    private final Map<Long, Integer> applied = new ConcurrentHashMap<>();

    @Override
    public void process(List<EventEnvelope> batch) {
      for (EventEnvelope envelope : batch) {
        applied.merge(envelope.globalOffset().value(), 1, Integer::sum);
      }
    }

    /**
     * How often this instance applied each event.
     *
     * @return applications by global offset
     */
    public Map<Long, Integer> applied() {
      return new TreeMap<>(applied);
    }
  }

  /**
   * A {@code TRANSACTIONAL_LOCAL} projection that writes through the repository it is handed but
   * exposes no {@code writeTarget()} and declares no store: with two transactional stores, routing
   * cannot choose.
   */
  @ProjectionConfig(
      name = AmbiguousProjection.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static final class AmbiguousProjection implements Projection {

    /** The projection's name. */
    public static final String NAME = "p4_ambiguous";

    @Override
    public void process(List<EventEnvelope> batch) {}

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {}

    @Override
    public boolean writesThroughRepository() {
      return true;
    }
  }

  // ---- what the databases hold --------------------------------------------------------------

  /**
   * Stores {@code count} events, one command each on its own aggregate, through the application's
   * command bus.
   *
   * @return the global offset of the last event in database A
   */
  private static long appendEvents(
      Application application, DataSource a, String prefix, int count) {
    VirtualThreadCommandBus bus = application.bean(VirtualThreadCommandBus.class);
    for (int i = 0; i < count; i++) {
      bus.execute(new ContractCommand(prefix + "-" + i));
    }
    List<Long> offsets = eventOffsets(a);
    assertFalse(offsets.isEmpty(), "the commands stored events");
    return offsets.getLast();
  }

  /** An event the first leader read after the last committed one: the next offset. */
  private static EventEnvelope envelope(long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(
            AggregateType.of(ContractCommandRegistration.TYPE), AggregateId.of("in-flight")),
        new Version(1),
        EventType.of(ContractCommandExecuted.class.getSimpleName()),
        new ContractCommandExecuted("in-flight"),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            IdGenerator.generateCorrelationId(),
            null,
            null,
            Instant.now()));
  }

  private static Map<Long, Integer> onceEach(List<Long> offsets) {
    Map<Long, Integer> once = new TreeMap<>();
    offsets.forEach(offset -> once.put(offset, 1));
    return once;
  }

  /** How often {@code projection} applied each event, as its read models in {@code repository}. */
  private static Map<Long, Integer> applied(ProjectionRepository repository, String projection) {
    Map<Long, Integer> applied = new TreeMap<>();
    for (AppliedEvent row : repository.findAll(ProjectionName.of(projection), AppliedEvent.class)) {
      applied.put(row.offset(), row.applications());
    }
    return applied;
  }

  private static List<Long> eventOffsets(DataSource a) {
    List<Long> offsets = new ArrayList<>();
    query(
        a,
        "SELECT global_offset FROM event_stream ORDER BY global_offset",
        List.of(),
        rs -> {
          while (rs.next()) {
            offsets.add(rs.getLong(1));
          }
          return null;
        });
    return offsets;
  }

  /** The last offset of {@code projection} in {@code table} of {@code database}; -1 for no row. */
  private static long checkpoint(DataSource database, String table, String projection) {
    return checkpointColumn(database, table, projection, "last_offset");
  }

  /** The epoch on {@code projection}'s row in {@code table} of {@code database}; -1 for no row. */
  private static long epoch(DataSource database, String table, String projection) {
    return checkpointColumn(database, table, projection, "epoch");
  }

  private static long checkpointColumn(
      DataSource database, String table, String projection, String column) {
    return query(
        database,
        "SELECT " + column + " FROM " + table + " WHERE projection_name = ?",
        List.of(projection),
        rs -> rs.next() ? rs.getLong(1) : -1L);
  }

  /** The epoch of {@code projection}'s lease in database A; 0 before any. */
  private static long leaseEpoch(DataSource a, String projection) {
    return query(
        a,
        "SELECT epoch FROM subscription_leases WHERE consumer_name = ?",
        List.of(projection),
        rs -> rs.next() ? rs.getLong(1) : 0L);
  }

  private static boolean tableExists(DataSource database, String table) {
    return query(
        database,
        "SELECT to_regclass(?) IS NOT NULL",
        List.of(table),
        rs -> rs.next() && rs.getBoolean(1));
  }

  private static void assertNoCheckpoint(DataSource database, String table, String projection) {
    assertEquals(
        -1L,
        checkpoint(database, table, projection),
        () -> projection + " must have no row in this database's " + table);
  }

  private void awaitCheckpoint(DataSource database, String table, String projection, long offset) {
    await(
        () -> checkpoint(database, table, projection) == offset,
        () ->
            projection
                + "'s checkpoint in "
                + table
                + " must reach "
                + offset
                + "; it is "
                + checkpoint(database, table, projection));
  }

  private static void execute(DataSource database, String... statements) {
    try (Connection connection = database.getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : statements) {
        statement.execute(sql);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(String.join("; ", statements), e);
    }
  }

  /** Reads one query's result. */
  @FunctionalInterface
  private interface ResultReader<T> {
    T read(ResultSet rs) throws SQLException;
  }

  private static <T> T query(
      DataSource database, String sql, List<String> parameters, ResultReader<T> reader) {
    try (Connection connection = database.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int i = 0; i < parameters.size(); i++) {
        statement.setString(i + 1, parameters.get(i));
      }
      try (ResultSet rs = statement.executeQuery()) {
        return reader.read(rs);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
  }

  // ---- waiting ------------------------------------------------------------------------------

  /**
   * Polls {@code condition} until it holds, or fails with {@code what} after {@link #patience()}.
   */
  private void await(BooleanSupplier condition, Supplier<String> what) {
    long deadline = System.nanoTime() + patience().toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        fail(what.get() + " (waited " + patience() + ")");
      }
      sleep(50);
    }
  }

  /** The health check once its flattened details satisfy {@code ready}, or its last answer. */
  private Map<String, Object> awaitHealth(
      Application application, Predicate<Map<String, Object>> ready) {
    long deadline = System.nanoTime() + patience().toNanos();
    Map<String, Object> health = application.health();
    while (!ready.test(IntegrationWiringContract.details(health)) && System.nanoTime() < deadline) {
      sleep(50);
      health = application.health();
    }
    return health;
  }

  private static boolean reportsCaughtUp(Map<String, Object> details, Map<String, String> stores) {
    return stores.entrySet().stream()
        .allMatch(
            entry ->
                entry
                        .getValue()
                        .equals(
                            String.valueOf(
                                details.get("subscription." + entry.getKey() + ".store")))
                    && "0"
                        .equals(
                            String.valueOf(
                                details.get("subscription." + entry.getKey() + ".lag"))));
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting", e);
    }
  }
}
