package org.streamrune.postgres;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.OptionalInt;
import javax.sql.DataSource;

/** Reads what the framework can know of a connection pool. */
public final class ConnectionPools {

  /**
   * The maximum pool size HikariCP applies when none is configured: {@value}. A pool that has not
   * started yet still reports an unset size as -1; HikariCP replaces it with this default when the
   * pool starts.
   */
  static final int HIKARI_DEFAULT_MAXIMUM_POOL_SIZE = 10;

  private ConnectionPools() {}

  /**
   * HikariCP's effective {@code maximumPoolSize} when the {@code DataSource} is, or unwraps to, a
   * {@link HikariDataSource}. A configured size below 1 (HikariCP's "not set") is reported as the
   * default of {@value #HIKARI_DEFAULT_MAXIMUM_POOL_SIZE} that HikariCP applies when the pool
   * starts.
   *
   * @param dataSource the pool to read; may be {@code null}
   * @return the maximum pool size; empty when the pool is not a HikariCP pool
   */
  public static OptionalInt maximumSize(DataSource dataSource) {
    if (dataSource instanceof HikariDataSource hikari) {
      return OptionalInt.of(effective(hikari));
    }
    if (dataSource != null) {
      try {
        if (dataSource.isWrapperFor(HikariDataSource.class)) {
          return OptionalInt.of(effective(dataSource.unwrap(HikariDataSource.class)));
        }
      } catch (SQLException | RuntimeException cannotUnwrap) {
        return OptionalInt.empty();
      }
    }
    return OptionalInt.empty();
  }

  private static int effective(HikariDataSource hikari) {
    int configured = hikari.getMaximumPoolSize();
    return configured < 1 ? HIKARI_DEFAULT_MAXIMUM_POOL_SIZE : configured;
  }
}
