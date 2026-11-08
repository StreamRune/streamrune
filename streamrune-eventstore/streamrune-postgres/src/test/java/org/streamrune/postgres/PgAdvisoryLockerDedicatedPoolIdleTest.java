package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The dedicated lock pool of {@link PgAdvisoryLocker#withDedicatedPool} keeps two connections open
 * while it is idle, not its maximum: a replica that runs no command holds two lock connections of
 * PostgreSQL's {@code max_connections}, and the pool opens the others when commands hold locks at
 * once, up to its maximum.
 */
@Testcontainers
class PgAdvisoryLockerDedicatedPoolIdleTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_lock_idle");

  private static final int MAX_POOL_SIZE = 8;

  @Test
  @Timeout(120)
  void anIdleLockPoolHoldsTwoConnections_andOpensMoreUpToItsMaximumWhenCommandsNeedThem()
      throws Exception {
    String application = "lock-pool-idle";
    String url = PG.getJdbcUrl() + "&ApplicationName=" + application;
    try (PgAdvisoryLocker locker =
        PgAdvisoryLocker.withDedicatedPool(
            url, PG.getUsername(), PG.getPassword(), MAX_POOL_SIZE)) {
      await()
          .atMost(Duration.ofSeconds(20))
          .until(() -> connectionsOf(application) >= PgAdvisoryLocker.DEDICATED_POOL_MINIMUM_IDLE);
      // The pool fills its idle connections in the background; a pool that keeps its maximum
      // idle opens the rest in this window.
      Thread.sleep(2_000);
      assertThat(connectionsOf(application))
          .as("an idle lock pool keeps its minimum, not its maximum")
          .isEqualTo(PgAdvisoryLocker.DEDICATED_POOL_MINIMUM_IDLE);

      List<AutoCloseable> held = new ArrayList<>();
      CountDownLatch allHeld = new CountDownLatch(MAX_POOL_SIZE);
      List<Thread> commands = new ArrayList<>();
      CountDownLatch release = new CountDownLatch(1);
      for (int i = 0; i < MAX_POOL_SIZE; i++) {
        String aggregate = "agg-" + i;
        commands.add(
            Thread.ofVirtual()
                .start(
                    () -> {
                      try (AutoCloseable lock =
                          locker.acquireLock(
                              TestStreams.stream(aggregate), Duration.ofSeconds(10))) {
                        synchronized (held) {
                          held.add(lock);
                        }
                        allHeld.countDown();
                        release.await();
                      } catch (Exception e) {
                        throw new IllegalStateException(e);
                      }
                    }));
      }
      assertThat(allHeld.await(30, TimeUnit.SECONDS))
          .as("every command gets a lock connection, up to the pool's maximum")
          .isTrue();
      assertThat(connectionsOf(application)).isEqualTo(MAX_POOL_SIZE);
      release.countDown();
      for (Thread command : commands) {
        command.join(Duration.ofSeconds(10));
      }
      assertThat(held).hasSize(MAX_POOL_SIZE);
    }
  }

  @Test
  void aPoolSmallerThanTheMinimumKeepsItsMaximum() {
    try (PgAdvisoryLocker locker =
        PgAdvisoryLocker.withDedicatedPool(
            PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), 1)) {
      assertThat(locker.dedicatedPoolMinimumIdle()).isEqualTo(1);
    }
  }

  /** The server sessions of the lock pool, by the application name its JDBC URL sets. */
  private static int connectionsOf(String application) throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = '"
                    + application
                    + "'")) {
      rows.next();
      return rows.getInt(1);
    }
  }
}
