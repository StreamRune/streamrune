package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.tracing.StreamRuneTracing;

class PgAdvisoryLockerConnectionBudgetTest {

  private static DataSource plainDataSource() {
    return new PGSimpleDataSource();
  }

  @Test
  void overTheApplicationsDataSourceItHoldsOne() {
    assertEquals(1, new PgAdvisoryLocker(plainDataSource()).applicationPoolConnectionsPerLock());
  }

  @Test
  void withAPoolOfItsOwnItHoldsNone() {
    var locker = PgAdvisoryLocker.withDedicatedPool(plainDataSource());

    assertEquals(0, locker.applicationPoolConnectionsPerLock());
  }

  @Test
  void aTracedCopyAnswersAsItsOriginal() {
    var shared = new PgAdvisoryLocker(plainDataSource());
    var dedicated = PgAdvisoryLocker.withDedicatedPool(plainDataSource());

    assertEquals(1, shared.traced(StreamRuneTracing.NOOP).applicationPoolConnectionsPerLock());
    assertEquals(0, dedicated.traced(StreamRuneTracing.NOOP).applicationPoolConnectionsPerLock());
  }

  @Test
  void aDedicatedHikariPoolHoldsNone() {
    try (var locker = PgAdvisoryLocker.withDedicatedPool(new HikariDataSource())) {
      assertEquals(0, locker.applicationPoolConnectionsPerLock());
      assertEquals(0, locker.traced(StreamRuneTracing.NOOP).applicationPoolConnectionsPerLock());
    }
  }

  @Test
  void aDedicatedPoolThatIsNotAutoCloseableStillAnswersNone() {
    DataSource plain =
        (DataSource)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  throw new UnsupportedOperationException(method.getName());
                });

    assertEquals(0, PgAdvisoryLocker.withDedicatedPool(plain).applicationPoolConnectionsPerLock());
  }
}
