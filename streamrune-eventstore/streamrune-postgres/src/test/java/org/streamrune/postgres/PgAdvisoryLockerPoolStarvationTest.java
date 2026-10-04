package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Regression: {@link PgAdvisoryLocker} holds a pooled connection for the whole command while the
 * command bus's {@code load()} and {@code append()} each borrow a <em>second</em> connection. When
 * the locker draws from the SAME pool as the event store, ~pool-size concurrent commands each hold
 * a lock connection and all block waiting for a second one that no one can release — a
 * hold-and-wait herd stall that surfaces as connection-timeout failures.
 *
 * <p>This test reproduces exactly that two-connections-per-command pattern: N workers each acquire
 * an advisory lock (holding one connection), rendezvous so all N locks are held at once, then
 * borrow a second connection (simulating {@code load()}/{@code append()}) from an event-store pool
 * of size N. With a SHARED pool the second borrows starve; with a DEDICATED lock pool the
 * event-store pool is left entirely free and every worker completes.
 */
@Testcontainers
class PgAdvisoryLockerPoolStarvationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_lock_test");

  private static final int N = 4;

  @BeforeAll
  static void awaitContainer() {
    // @Container starts it; nothing else to do.
  }

  private static HikariDataSource pool(String name, int size, long connectionTimeoutMs) {
    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(size);
    config.setPoolName(name);
    config.setConnectionTimeout(connectionTimeoutMs);
    return new HikariDataSource(config);
  }

  @Test
  @Timeout(120)
  void sharedPoolStarvesButDedicatedLockPoolLetsEveryCommandComplete() throws Exception {
    // BUG (documents the defect): the locker shares the event-store pool. N workers hold N
    // lock connections, so the N second borrows have nothing left and time out.
    try (HikariDataSource shared = pool("shared", N, 2_000)) {
      var locker = new PgAdvisoryLocker(shared);
      int failures = runTwoConnectionScenario(locker, shared);
      assertTrue(
          failures > 0,
          "sharing one pool for locks + load/append must starve the second-connection borrows at"
              + " pool size "
              + N
              + " (observed failures: "
              + failures
              + ")");
    }

    // FIX: a dedicated lock pool. Lock connections come from their own pool, so the event-store
    // pool of size N is entirely free for the N second borrows — no starvation.
    try (HikariDataSource eventStorePool = pool("event-store", N, 5_000);
        PgAdvisoryLocker locker =
            PgAdvisoryLocker.withDedicatedPool(
                PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), N)) {
      int failures = runTwoConnectionScenario(locker, eventStorePool);
      assertEquals(
          0,
          failures,
          "a dedicated lock pool must leave all "
              + N
              + " event-store connections free for load/append");
    }
  }

  /**
   * Runs N workers, each of which: (1) acquires an advisory lock on a distinct aggregate (holding
   * one connection from the locker's pool), (2) rendezvouses at a barrier so ALL N locks are held
   * simultaneously, then (3) borrows a second connection from {@code eventStorePool} and runs
   * {@code SELECT 1} (simulating the command bus's load/append). Returns the number of workers
   * whose second borrow failed.
   */
  private int runTwoConnectionScenario(PgAdvisoryLocker locker, DataSource eventStorePool)
      throws Exception {
    var barrier = new CyclicBarrier(N);
    var failures = new AtomicInteger(0);
    ExecutorService exec = Executors.newFixedThreadPool(N);
    try {
      for (int i = 0; i < N; i++) {
        final int idx = i;
        exec.submit(
            () -> {
              AutoCloseable lock = null;
              try {
                lock = locker.acquireLock(TestStreams.stream("agg-" + idx), Duration.ofSeconds(10));
                // All N lock connections are now (about to be) held; wait for the rest.
                barrier.await(60, TimeUnit.SECONDS);
                // The second connection: this is where a shared pool starves.
                try (var conn = eventStorePool.getConnection();
                    var stmt = conn.createStatement();
                    var rs = stmt.executeQuery("SELECT 1")) {
                  rs.next();
                }
              } catch (Exception _) {
                failures.incrementAndGet();
              } finally {
                if (lock != null) {
                  try {
                    lock.close();
                  } catch (Exception _) {
                    // releasing best-effort
                  }
                }
              }
            });
      }
      exec.shutdown();
      assertTrue(exec.awaitTermination(90, TimeUnit.SECONDS), "workers did not finish in time");
    } finally {
      exec.shutdownNow();
    }
    return failures.get();
  }
}
