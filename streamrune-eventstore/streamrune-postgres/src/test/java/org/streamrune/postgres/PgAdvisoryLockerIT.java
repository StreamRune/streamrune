package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
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
}
