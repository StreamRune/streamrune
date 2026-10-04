package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.subscription.SubscriptionLeadership.Lease;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Acceptance IT — the definitive closer for the split-brain failure. The retired session-scoped
 * {@code pg_advisory_lock} leadership <b>split-brained under exactly this config</b>: a PgBouncer
 * <b>transaction-mode</b> pooler with {@code pool_size=1}. Advisory locks are session-scoped, but
 * transaction-mode pooling multiplexes every statement onto a shared, short-lived backend session,
 * so two replicas could each believe they held the session lock and both advance the same
 * projection. {@link LeaseBasedLeadership} replaces it with a persisted, DB-clocked lease whose
 * acquire/renew are each a <b>single atomic statement</b> — correct under any pooling mode. This
 * test drives two replicas <b>through the transaction pooler</b> and proves (a) exactly one leads
 * and (b) the epoch write-fence in {@link JdbcProjectionRepository#executeAtomically} still rejects
 * a superseded leader's commit through the same pooler.
 *
 * <h2>Topology</h2>
 *
 * A shared Docker {@link Network} carries the PostgreSQL image of {@link PostgresTestImage} (alias
 * {@code pg}) and {@code edoburu/pgbouncer:1.23.1} in front of it. PgBouncer runs {@code
 * POOL_MODE=transaction} with {@code DEFAULT_POOL_SIZE=1} — the single shared backend session is
 * the exact window that split-brained the old advisory-lock design. The schema is migrated by
 * Flyway <b>directly against PostgreSQL</b>, the way the production guide tells operators to run
 * migrations: Flyway holds one server connection in its lock transaction and needs a second one for
 * the migration, which a pooler with a single server connection can never give (see {@link
 * EventStorePgBouncerTxModeIT}). The test {@link javax.sql.DataSource} is built against the
 * <b>pooler's</b> mapped port so leadership and fencing are exercised through it.
 *
 * <h2>Image choice</h2>
 *
 * {@code edoburu/pgbouncer:v1.23.1-p3} is used (edoburu tags PgBouncer 1.23.1 as {@code
 * v1.23.1-pN}; {@code -p3} is the current patch) — it derives {@code userlist.txt} from {@code
 * DB_USER}/{@code DB_PASSWORD} and honours {@code AUTH_TYPE=scram-sha-256} (PgBouncer builds the
 * SCRAM verifier from the plaintext secret; PostgreSQL 14+ stores SCRAM by default). Client server-
 * side prepared statements are disabled ({@code prepareThreshold=0}), the setting the production
 * guide gives operators: PgBouncer's {@code max_prepared_statements} is 0 here, so named prepared
 * statements do not survive transaction-mode pooling (see {@link EventStorePgBouncerTxModeIT}). If
 * this image ever fails auth/startup in a given environment, the documented fallback is {@code
 * bitnami/pgbouncer:1.23.1} — its env names differ ({@code
 * POSTGRESQL_HOST/PORT/USERNAME/PASSWORD/DATABASE}, {@code PGBOUNCER_POOL_MODE=transaction}, {@code
 * PGBOUNCER_DATABASE=srune}) — and container logs should be checked on any startup failure.
 */
@Testcontainers
class LeaseLeadershipPgBouncerTxModeIT {

  private static final String DB = "srune";
  private static final String USER = "srune";
  private static final String PASSWORD = "srune";

  private static final Network NETWORK = Network.newNetwork();

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName(DB)
          .withUsername(USER)
          .withPassword(PASSWORD)
          .withNetwork(NETWORK)
          .withNetworkAliases("pg");

  // edoburu/pgbouncer: transaction pooling, pool_size=1 (reproduces the advisory-design split-brain
  // window), SCRAM client auth. See the class javadoc for the bitnami fallback image + env mapping.
  @Container
  static final GenericContainer<?> PGBOUNCER =
      new GenericContainer<>(DockerImageName.parse("edoburu/pgbouncer:v1.23.1-p3"))
          .withNetwork(NETWORK)
          .withEnv("DB_HOST", "pg")
          .withEnv("DB_PORT", "5432")
          .withEnv("DB_USER", USER)
          .withEnv("DB_PASSWORD", PASSWORD)
          .withEnv("DB_NAME", DB)
          .withEnv("POOL_MODE", "transaction")
          .withEnv("AUTH_TYPE", "scram-sha-256")
          .withEnv("DEFAULT_POOL_SIZE", "1")
          .withEnv("MAX_CLIENT_CONN", "100")
          .withExposedPorts(5432)
          .dependsOn(PG)
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)));

  record TestView(String id, String data) {}

  /** DataSource routed THROUGH the transaction pooler (PgBouncer's mapped host/port). */
  private static PGSimpleDataSource pooledDataSource() {
    var ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {PGBOUNCER.getHost()});
    ds.setPortNumbers(new int[] {PGBOUNCER.getMappedPort(5432)});
    ds.setDatabaseName(DB);
    ds.setUser(USER);
    ds.setPassword(PASSWORD);
    // Transaction-mode pooling multiplexes statements across backend sessions, and this PgBouncer
    // does not track prepared statements, so a named server-side prepared statement can land on a
    // session that never PREPAREd it. Force unnamed statements.
    ds.setPrepareThreshold(0);
    return ds;
  }

  /** Migrate the schema to the baseline DIRECTLY against PostgreSQL, bypassing the pooler. */
  private static void migrateDirectlyAgainstPostgres() {
    var direct = new PGSimpleDataSource();
    direct.setUrl(PG.getJdbcUrl());
    direct.setUser(PG.getUsername());
    direct.setPassword(PG.getPassword());
    Flyway.configure()
        .dataSource(direct)
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();
  }

  /** Reads the epoch fencing token stamped on the projection's checkpoint row (0 if no row). */
  private static long storedEpoch(PGSimpleDataSource ds, String projectionName)
      throws SQLException {
    try (var conn = ds.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT epoch FROM projection_offset WHERE projection_name = ?")) {
      ps.setString(1, projectionName);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  @Test
  void twoReplicas_throughTransactionPooler_exactlyOneLeads_andFencingHolds() throws Exception {
    migrateDirectlyAgainstPostgres();
    var pooled = pooledDataSource();

    var consumer = "orders-projection";
    // Two replicas, each on the pooled (transaction-mode) DataSource. Under the old advisory design
    // both could win here because each statement may land on a different backend session; the lease
    // acquire is a single atomic INSERT ... ON CONFLICT, so it cannot.
    var replicaA = new LeaseBasedLeadership(pooled, Duration.ofSeconds(30));
    var replicaB = new LeaseBasedLeadership(pooled, Duration.ofSeconds(30));
    try {
      Optional<Lease> a = replicaA.tryAcquire(consumer);
      Optional<Lease> b = replicaB.tryAcquire(consumer);

      // EXACTLY ONE leads — the split-brain closer, proven through the transaction pooler.
      assertTrue(
          a.isPresent() ^ b.isPresent(),
          "exactly one replica may acquire the lease through the transaction pooler (was a="
              + a.isPresent()
              + ", b="
              + b.isPresent()
              + ")");

      var winner = a.isPresent() ? replicaA : replicaB;
      var standby = a.isPresent() ? replicaB : replicaA;
      long winnerEpoch = (a.isPresent() ? a : b).orElseThrow().epoch();
      assertEquals(1L, winnerEpoch, "a fresh lease acquires at epoch 1");

      // Take the epoch to >= 2 so the fence assertion is genuine: with a stored epoch of 1 the only
      // stale caller epoch below it would be 0, and epoch 0 is UNFENCED (single-node / NOOP) and
      // always passes — the fence would appear to hold while actually never engaging. So resign the
      // winner and let the standby take over the (now expired) lease at epoch 2, which it then
      // stamps onto the checkpoint by committing. A subsequent commit at epoch 1 (the superseded
      // original winner) is then a real, non-zero fence.
      winner.resign(consumer);
      Thread.sleep(50); // let the DB clock advance past the resign's lease_until = now() stamp
      Optional<Lease> takeover = standby.tryAcquire(consumer);
      assertTrue(
          takeover.isPresent(), "the standby must take over the resigned lease through the pooler");
      long leaderEpoch = takeover.orElseThrow().epoch();
      assertEquals(2L, leaderEpoch, "takeover of the resigned lease bumps the fencing epoch to 2");

      // Fencing THROUGH the pooler: current leader (epoch 2) commits; the superseded leader (epoch
      // 1)
      // is fenced out even though its offset advances (200 > 100) and its empty batch cannot
      // overlap.
      var repo = new JdbcProjectionRepository(pooled);
      var offsetStore = new PostgresOffsetStore(pooled);
      var projection = ProjectionName.of("pgbouncer_txmode");

      repo.executeAtomically(
          projection,
          List.of(),
          GlobalOffset.of(100),
          leaderEpoch,
          tx -> tx.save(projection, "id-1", new TestView("id-1", "leader-epoch2")),
          offsetStore);
      assertEquals(GlobalOffset.of(100), offsetStore.getLastOffset(projection));
      assertEquals(2L, storedEpoch(pooled, projection.value()), "the leader's epoch is stamped");
      assertEquals(
          "leader-epoch2", repo.findById(projection, "id-1", TestView.class).orElseThrow().data());

      long staleEpoch = leaderEpoch - 1; // == 1: the superseded original winner, NOT the unfenced 0
      assertThrows(
          OptimisticLockException.class,
          () ->
              repo.executeAtomically(
                  projection,
                  List.of(),
                  GlobalOffset.of(200),
                  staleEpoch,
                  tx -> tx.save(projection, "id-1", new TestView("id-1", "stale-epoch1")),
                  offsetStore),
          "a commit at the superseded epoch 1 must be fenced out through the pooler");

      // Read model AND offset stay at the winning leader's state — the fenced write rolled back.
      assertEquals(
          GlobalOffset.of(100),
          offsetStore.getLastOffset(projection),
          "a fenced-out stale-epoch commit must not advance the offset");
      assertEquals(
          2L,
          storedEpoch(pooled, projection.value()),
          "a fenced-out stale-epoch commit must not regress the stored epoch");
      assertEquals(
          "leader-epoch2",
          repo.findById(projection, "id-1", TestView.class).orElseThrow().data(),
          "a fenced-out stale-epoch commit must not overwrite the read model");
    } finally {
      replicaA.close();
      replicaB.close();
    }
  }
}
