package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.OptionalInt;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class ConnectionPoolsTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static HikariDataSource hikari(int maximum) {
    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(maximum);
    config.setMinimumIdle(1);
    return new HikariDataSource(config);
  }

  private static DataSource wrapper(DataSource inner, boolean unwrapFails) {
    return (DataSource)
        Proxy.newProxyInstance(
            ConnectionPoolsTest.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "isWrapperFor" -> inner.isWrapperFor((Class<?>) args[0]);
                  case "unwrap" -> {
                    if (unwrapFails) {
                      throw new SQLException("cannot unwrap");
                    }
                    yield inner.unwrap((Class<?>) args[0]);
                  }
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  @Test
  void readsHikarisMaximumPoolSize() {
    try (var pool = hikari(7)) {
      assertEquals(OptionalInt.of(7), ConnectionPools.maximumSize(pool));
    }
  }

  @Test
  void aPoolThatNamesNoSizeAndHasNotStartedReportsHikarisDefault() {
    try (var pool = new HikariDataSource()) {
      assertEquals(OptionalInt.of(10), ConnectionPools.maximumSize(pool));
    }
  }

  @Test
  void theDefaultReportedBeforeTheStartIsTheSizeHikariUsesOnceStarted() {
    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    try (var unstarted = new HikariDataSource();
        var started = new HikariDataSource(config)) {
      assertEquals(
          OptionalInt.of(started.getMaximumPoolSize()), ConnectionPools.maximumSize(unstarted));
    }
  }

  @Test
  void aSizeNamedOnAPoolThatHasNotStartedIsReadAsItIs() {
    try (var pool = new HikariDataSource()) {
      pool.setMaximumPoolSize(7);

      assertEquals(OptionalInt.of(7), ConnectionPools.maximumSize(pool));
    }
  }

  @Test
  void readsItThroughAWrapperThatUnwraps() {
    try (var pool = hikari(9)) {
      assertEquals(OptionalInt.of(9), ConnectionPools.maximumSize(wrapper(pool, false)));
    }
  }

  @Test
  void aWrapperThatCannotUnwrapIsUnknown() {
    try (var pool = hikari(9)) {
      assertTrue(ConnectionPools.maximumSize(wrapper(pool, true)).isEmpty());
    }
  }

  @Test
  void anyOtherDataSourceIsUnknown() {
    var plain = new PGSimpleDataSource();
    plain.setUrl(PG.getJdbcUrl());

    assertTrue(ConnectionPools.maximumSize(plain).isEmpty());
    assertTrue(ConnectionPools.maximumSize(null).isEmpty());
  }
}
