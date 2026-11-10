package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * {@link PgAdvisoryLocker#withDedicatedPool(javax.sql.DataSource)}: the locker takes a pool of any
 * library, locks through it and owns it — {@link PgAdvisoryLocker#close()} closes it once when it
 * is {@link AutoCloseable}.
 */
@Testcontainers
class PgAdvisoryLockerOwnedPoolTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_lock_owned");

  private static final StreamId ORDER = StreamId.of(AggregateType.of("order"), AggregateId.of("1"));

  /** A pool that is a plain DataSource and counts how often it is closed. */
  static final class CountingPool extends PGSimpleDataSource implements AutoCloseable {
    final AtomicInteger closes = new AtomicInteger();
    Exception failure;

    @Override
    public void close() throws Exception {
      closes.incrementAndGet();
      if (failure != null) {
        throw failure;
      }
    }
  }

  private static CountingPool countingPool() {
    var pool = new CountingPool();
    pool.setUrl(PG.getJdbcUrl());
    pool.setUser(PG.getUsername());
    pool.setPassword(PG.getPassword());
    return pool;
  }

  @Test
  void locksThroughAPoolOfAnotherLibrary_andClosesItOnce() throws Exception {
    var pool = countingPool();
    var locker = PgAdvisoryLocker.withDedicatedPool(pool);

    try (AutoCloseable lock = locker.acquireLock(ORDER, Duration.ofSeconds(1))) {
      assertThat(lock).isNotNull();
    }
    locker.close();
    locker.close();

    assertThat(pool.closes).hasValue(1);
  }

  @Test
  void closesAHikariPoolItWasHanded() {
    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(2);
    var pool = new HikariDataSource(config);

    PgAdvisoryLocker.withDedicatedPool(pool).close();

    assertThat(pool.isClosed()).isTrue();
  }

  @Test
  void aPoolThatIsNotAutoCloseable_isLeftAlone() throws Exception {
    var pool = new PGSimpleDataSource();
    pool.setUrl(PG.getJdbcUrl());
    pool.setUser(PG.getUsername());
    pool.setPassword(PG.getPassword());
    var locker = PgAdvisoryLocker.withDedicatedPool(pool);

    locker.close();

    try (AutoCloseable lock = locker.acquireLock(ORDER, Duration.ofSeconds(1))) {
      assertThat(lock).as("the DataSource still serves connections").isNotNull();
    }
  }

  @Test
  void aCheckedFailureOfTheClose_isRethrownWrapped_andARuntimeOneAsItIs() {
    var checked = countingPool();
    checked.failure = new IOException("pool did not close");
    assertThatThrownBy(PgAdvisoryLocker.withDedicatedPool(checked)::close)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("could not close the dedicated lock pool")
        .hasCause(checked.failure);

    var unchecked = countingPool();
    unchecked.failure = new IllegalArgumentException("broken");
    assertThatThrownBy(PgAdvisoryLocker.withDedicatedPool(unchecked)::close)
        .isSameAs(unchecked.failure);
  }

  @Test
  void aPoolIsRequired() {
    assertThatThrownBy(() -> PgAdvisoryLocker.withDedicatedPool((javax.sql.DataSource) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("pool is required");
  }

  @Test
  void aLockerOverACallersDataSource_closesNothing() {
    var pool = countingPool();

    new PgAdvisoryLocker(pool).close();

    assertThat(pool.closes).hasValue(0);
  }
}
