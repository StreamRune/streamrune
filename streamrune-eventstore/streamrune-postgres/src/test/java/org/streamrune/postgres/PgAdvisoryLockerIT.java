package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.LockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The advisory lock key covers the whole typed stream: two aggregate types that share an id value
 * take independent locks, while the same stream contends with itself.
 */
@Testcontainers
class PgAdvisoryLockerIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("advisory_locker_it");

  private static final StreamId PRODUCT_X =
      StreamId.of(AggregateType.of("product"), AggregateId.of("x"));
  private static final StreamId INVENTORY_X =
      StreamId.of(AggregateType.of("inventory"), AggregateId.of("x"));

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void init() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  @Test
  void twoTypesSharingAnIdValue_takeIndependentAdvisoryLocks() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    try (var product = locker.acquireLock(PRODUCT_X, Duration.ofSeconds(5))) {
      assertNotNull(product);
      // A different session, zero wait: succeeds only if the two keys differ.
      try (var inventory = locker.acquireLock(INVENTORY_X, Duration.ZERO)) {
        assertNotNull(inventory);
      }
      var held =
          assertThrows(LockException.class, () -> locker.acquireLock(PRODUCT_X, Duration.ZERO));
      assertTrue(held.getMessage().contains("stream: product:x"), held.getMessage());
    }
  }

  /**
   * The dedicated lock pool reports to the metrics tracker factory the application hands in:
   * HikariCP creates the pool's tracker from it under the pool's name, and the tracker sees the
   * connections the locker borrows for its locks.
   */
  @Test
  void aDedicatedPoolReportsToTheMetricsTrackerFactoryItIsGiven() throws Exception {
    var factory = new RecordingTrackerFactory();
    try (var locker =
        PgAdvisoryLocker.withDedicatedPool(
            PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), 2, factory)) {
      try (var lock = locker.acquireLock(PRODUCT_X, Duration.ofSeconds(5))) {
        assertNotNull(lock);
      }

      assertEquals(List.of("streamrune-advisory-lock"), factory.pools);
      assertTrue(factory.acquisitions.get() >= 1, "the tracker sees the lock connection");
    }
  }

  /** Without a factory the dedicated pool reports to none and locks as before. */
  @Test
  void aDedicatedPoolWithoutAMetricsTrackerFactoryLocks() throws Exception {
    try (var locker =
        PgAdvisoryLocker.withDedicatedPool(
            PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), 1, null)) {
      try (var lock = locker.acquireLock(PRODUCT_X, Duration.ofSeconds(5))) {
        assertNotNull(lock);
      }
    }
  }

  /** Records the pools a tracker is created for and the connections they hand out. */
  static final class RecordingTrackerFactory implements MetricsTrackerFactory {
    final List<String> pools = new CopyOnWriteArrayList<>();
    final AtomicInteger acquisitions = new AtomicInteger();

    @Override
    public IMetricsTracker create(String poolName, PoolStats poolStats) {
      pools.add(poolName);
      return new IMetricsTracker() {
        @Override
        public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
          acquisitions.incrementAndGet();
        }
      };
    }
  }
}
