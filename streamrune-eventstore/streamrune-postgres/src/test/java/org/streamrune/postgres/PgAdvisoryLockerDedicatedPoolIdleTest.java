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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The dedicated lock pool of {@link PgAdvisoryLocker#withDedicatedPool} is a fixed-size pool: it
 * keeps its maximum open while it is idle, so a burst of as many commands as the pool's maximum
 * gets a connection for every one of them. A HikariCP pool that grows on demand from a smaller
 * minimum can open fewer connections than such a burst waits for, and the command left over fails
 * after the connection timeout.
 */
@Testcontainers
class PgAdvisoryLockerDedicatedPoolIdleTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_lock_idle");

  private static final int MAX_POOL_SIZE = 8;

  /** Lock pools that burst at once in one round of the burst test. */
  private static final int POOLS_PER_ROUND = 8;

  /** Rounds of the burst test. */
  private static final int ROUNDS = 25;

  @Test
  @Timeout(120)
  void anIdleLockPoolKeepsItsMaximumOpen_andGivesEveryCommandOfAFullBurstAConnection()
      throws Exception {
    String application = "lock-pool-idle";
    try (PgAdvisoryLocker locker = lockerFor(application)) {
      // pg_stat_activity lags the pool, so every count is polled to a deadline. The idle count
      // must reach the maximum and then stay there.
      await()
          .atMost(Duration.ofSeconds(20))
          .pollInterval(Duration.ofMillis(100))
          .untilAsserted(
              () ->
                  assertThat(connectionsOf(application))
                      .as("an idle lock pool opens its maximum")
                      .isEqualTo(MAX_POOL_SIZE));
      await()
          .during(Duration.ofSeconds(2))
          .atMost(Duration.ofSeconds(20))
          .pollInterval(Duration.ofMillis(100))
          .untilAsserted(
              () ->
                  assertThat(connectionsOf(application))
                      .as("an idle lock pool keeps its maximum open")
                      .isEqualTo(MAX_POOL_SIZE));

      List<Throwable> failures = new CopyOnWriteArrayList<>();
      Burst burst = Burst.start(locker, application, failures);
      try {
        assertThat(burst.settled.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(failures)
            .as("every command gets a lock connection, up to the pool's maximum")
            .extracting(PgAdvisoryLockerDedicatedPoolIdleTest::describe)
            .isEmpty();
        assertThat(connectionsOf(application))
            .as("a pool whose commands all hold a lock holds its maximum, no more")
            .isEqualTo(MAX_POOL_SIZE);
      } finally {
        burst.finish();
      }
      assertThat(burst.held).hasSize(MAX_POOL_SIZE);
    }
  }

  /**
   * Bursts of as many commands as the pool's maximum, each on a lock pool at rest. Each round
   * builds {@link #POOLS_PER_ROUND} lockers, waits until each pool has opened its minimum idle
   * connections, and starts {@link #MAX_POOL_SIZE} commands on every one of them at once. A pool
   * that keeps fewer connections idle than its maximum grows during the burst, and HikariCP's
   * growth asks for fewer connections than the commands wait for in a few bursts out of a hundred;
   * the command left over then fails its lock acquisition after the pool's 10-second connection
   * timeout. A pool that keeps two connections idle failed about one round in seven, so the rounds
   * fail a growing pool with high probability and pass a fixed-size one.
   */
  @Test
  @Timeout(300)
  void everyCommandOfABurstAsLargeAsThePoolGetsALockConnection_roundAfterRound() throws Exception {
    for (int round = 0; round < ROUNDS; round++) {
      List<PgAdvisoryLocker> lockers = new ArrayList<>();
      List<Burst> bursts = new ArrayList<>();
      List<Throwable> failures = new CopyOnWriteArrayList<>();
      try {
        for (int pool = 0; pool < POOLS_PER_ROUND; pool++) {
          lockers.add(lockerFor(application(round, pool)));
        }
        for (int pool = 0; pool < POOLS_PER_ROUND; pool++) {
          String application = application(round, pool);
          int minimumIdle = lockers.get(pool).dedicatedPoolMinimumIdle();
          await()
              .atMost(Duration.ofSeconds(20))
              .pollInterval(Duration.ofMillis(20))
              .until(() -> connectionsOf(application) >= minimumIdle);
        }
        for (int pool = 0; pool < POOLS_PER_ROUND; pool++) {
          bursts.add(Burst.start(lockers.get(pool), application(round, pool), failures));
        }
        for (Burst burst : bursts) {
          assertThat(burst.settled.await(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(failures)
            .as(
                "round %d: every command of a burst as large as the pool gets a lock connection",
                round)
            .extracting(PgAdvisoryLockerDedicatedPoolIdleTest::describe)
            .isEmpty();
      } finally {
        for (Burst burst : bursts) {
          burst.finish();
        }
        for (PgAdvisoryLocker locker : lockers) {
          locker.close();
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, MAX_POOL_SIZE})
  void theMinimumIdleOfTheLockPoolIsItsMaximum(int maxPoolSize) {
    try (PgAdvisoryLocker locker =
        PgAdvisoryLocker.withDedicatedPool(
            PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), maxPoolSize)) {
      assertThat(locker.dedicatedPoolMinimumIdle()).isEqualTo(maxPoolSize);
    }
  }

  private static PgAdvisoryLocker lockerFor(String application) {
    return PgAdvisoryLocker.withDedicatedPool(
        PG.getJdbcUrl() + "&ApplicationName=" + application,
        PG.getUsername(),
        PG.getPassword(),
        MAX_POOL_SIZE);
  }

  private static String application(int round, int pool) {
    return "lock-pool-burst-" + round + "-" + pool;
  }

  /** {@link #MAX_POOL_SIZE} commands on one locker, each holding its lock until finished. */
  private static final class Burst {

    /** Counted down by every command once it holds its lock or has failed to get it. */
    final CountDownLatch settled = new CountDownLatch(MAX_POOL_SIZE);

    final List<AutoCloseable> held = new CopyOnWriteArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);
    private final List<Thread> commands = new ArrayList<>();

    static Burst start(PgAdvisoryLocker locker, String application, List<Throwable> failures) {
      Burst burst = new Burst();
      for (int i = 0; i < MAX_POOL_SIZE; i++) {
        String aggregate = application + "-agg-" + i;
        burst.commands.add(
            Thread.ofVirtual()
                .start(
                    () -> {
                      try (AutoCloseable lock =
                          locker.acquireLock(
                              TestStreams.stream(aggregate), Duration.ofSeconds(10))) {
                        burst.held.add(lock);
                        burst.settled.countDown();
                        burst.release.await();
                      } catch (Exception e) {
                        failures.add(e);
                        burst.settled.countDown();
                      }
                    }));
      }
      return burst;
    }

    void finish() throws InterruptedException {
      release.countDown();
      for (Thread command : commands) {
        command.join(Duration.ofSeconds(15));
      }
    }
  }

  /** A failure with its cause: a lock pool that ran dry says so in the cause. */
  private static String describe(Throwable failure) {
    return failure + (failure.getCause() == null ? "" : " <- " + failure.getCause());
  }

  /** The server sessions of a lock pool, by the application name its JDBC URL sets. */
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
