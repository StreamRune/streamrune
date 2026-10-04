package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * Pure-logic unit tests for {@link PostgresCryptoForgetSignal} — the validation, degrade-gracefully
 * (no derivable JDBC URL), idempotent-close and caller-supplied LISTEN pool lifecycle paths that
 * don't need a real PostgreSQL connection. The real {@code LISTEN}/{@code NOTIFY} round trip, and
 * the paths that DO need a live connection (a {@code HikariDataSource} source, a source with a URL
 * but no username/password accessors, and double-close of a successfully-opened listen pool) are
 * covered by {@link PostgresCryptoForgetSignalIntegrationTest}.
 */
class PostgresCryptoForgetSignalTest {

  @Test
  void constructorRejectsNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresCryptoForgetSignal(null));
  }

  @Test
  void publishIgnoresNullTokenWithoutTouchingTheDataSource() {
    // If the null-check didn't short-circuit, getConnection() below would throw and fail the test.
    var signal = new PostgresCryptoForgetSignal(new ExplodingDataSource());
    assertDoesNotThrow(() -> signal.publish(null));
  }

  @Test
  void subscribeRejectsNullListener() {
    var signal = new PostgresCryptoForgetSignal(new NoAccessorsDataSource());
    assertThrows(IllegalArgumentException.class, () -> signal.subscribe(null));
  }

  @Test
  void subscribeDegradesGracefullyWhenDataSourceExposesNoJdbcUrlAccessor() {
    // A DataSource shape (test double, proxied/wrapped bean) with none of getJdbcUrl/getUrl/getURL
    // must not fail the engine's construction: it degrades to publish-only.
    var signal = new PostgresCryptoForgetSignal(new NoAccessorsDataSource());

    assertDoesNotThrow(
        () -> signal.subscribe(token -> fail("listener must never fire: no LISTEN")));

    signal.close(); // must be a no-op even though no listen connection was ever opened
  }

  @Test
  void subscribeDegradesWhenJdbcUrlAccessorReturnsNull() {
    // getJdbcUrl() exists but yields null: firstString must reject the non-String value and fall
    // through to the next accessor name, ultimately still degrading gracefully.
    var signal = new PostgresCryptoForgetSignal(new NullJdbcUrlDataSource());

    assertDoesNotThrow(
        () -> signal.subscribe(token -> fail("listener must never fire: no LISTEN")));

    signal.close();
  }

  @Test
  void subscribeDegradesWhenJdbcUrlAccessorReturnsBlank() {
    // getJdbcUrl() exists and returns a String, but a blank one: firstString must treat it as
    // absent and fall through, ultimately still degrading gracefully.
    var signal = new PostgresCryptoForgetSignal(new BlankJdbcUrlDataSource());

    assertDoesNotThrow(
        () -> signal.subscribe(token -> fail("listener must never fire: no LISTEN")));

    signal.close();
  }

  @Test
  void secondSubscribeOnAnAlreadyDegradedChannelDoesNotRetryTheListenConnection() {
    // The first subscribe() flips `running` true and attempts (and fails) to open the dedicated
    // LISTEN pool. A second subscribe() on the same instance must observe `running` already true
    // and skip straight past the connection-attempt block (compareAndSet short-circuits), merely
    // registering its listener.
    var signal = new PostgresCryptoForgetSignal(new NoAccessorsDataSource());

    assertDoesNotThrow(
        () -> {
          signal.subscribe(token -> fail("listener 1 must never fire: no LISTEN"));
          signal.subscribe(token -> fail("listener 2 must never fire: no LISTEN"));
        });

    signal.close();
  }

  @Test
  void closeBeforeAnySubscribeIsANoOp() {
    var signal = new PostgresCryptoForgetSignal(new NoAccessorsDataSource());
    assertDoesNotThrow(signal::close);
  }

  @Test
  void closeIsIdempotent() {
    var signal = new PostgresCryptoForgetSignal(new NoAccessorsDataSource());
    signal.close();
    assertDoesNotThrow(signal::close);
  }

  // -------------------------------------------------------------------------
  // A LISTEN pool handed in by the caller
  // -------------------------------------------------------------------------

  @Test
  void constructorWithAListenPoolFactoryRejectsNullArguments() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new PostgresCryptoForgetSignal(null, ClosableDataSource::new));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PostgresCryptoForgetSignal(new NoAccessorsDataSource(), null));
  }

  @Test
  void aChannelNobodySubscribesToNeverOpensTheListenPool() {
    var opened = new AtomicInteger();
    var signal =
        new PostgresCryptoForgetSignal(
            new ExplodingDataSource(),
            () -> {
              opened.incrementAndGet();
              return new ClosableDataSource();
            });

    signal.close();

    assertEquals(0, opened.get(), "nothing subscribed, so no LISTEN pool may be opened");
  }

  @Test
  void theListenPoolIsOpenedOnceAndClosedWithTheChannel() {
    var opened = new AtomicInteger();
    var pool = new ClosableDataSource();
    var signal =
        new PostgresCryptoForgetSignal(
            new ExplodingDataSource(),
            () -> {
              opened.incrementAndGet();
              return pool;
            });

    signal.subscribe(token -> {});
    signal.subscribe(token -> {});
    assertEquals(1, opened.get(), "subscribers share one LISTEN pool");

    signal.close();
    signal.close();
    assertEquals(1, pool.closed.get(), "close() closes the pool it opened, exactly once");
  }

  @Test
  void aListenPoolFactoryThatThrowsLeavesTheChannelPublishOnly() {
    var signal =
        new PostgresCryptoForgetSignal(
            new NoAccessorsDataSource(),
            () -> {
              throw new IllegalStateException("cannot open the LISTEN pool");
            });

    assertDoesNotThrow(() -> signal.subscribe(token -> fail("no LISTEN, nothing may arrive")));
    assertDoesNotThrow(signal::close);
  }

  @Test
  void aListenPoolFactoryThatReturnsNullLeavesTheChannelPublishOnly() {
    var signal = new PostgresCryptoForgetSignal(new NoAccessorsDataSource(), () -> null);

    assertDoesNotThrow(() -> signal.subscribe(token -> fail("no LISTEN, nothing may arrive")));
    assertDoesNotThrow(signal::close);
  }

  // -------------------------------------------------------------------------
  // Test doubles
  // -------------------------------------------------------------------------

  /** A LISTEN pool that cannot connect and counts how often it is closed. */
  private static final class ClosableDataSource extends AbstractDataSourceStub
      implements AutoCloseable {
    final AtomicInteger closed = new AtomicInteger();

    @Override
    public void close() {
      closed.incrementAndGet();
    }
  }

  /** Throws on every JDBC operation — used to prove a code path never reaches the DataSource. */
  private static final class ExplodingDataSource extends AbstractDataSourceStub {
    @Override
    public Connection getConnection() throws SQLException {
      throw new SQLException("must never be called");
    }
  }

  /** Exposes none of the getJdbcUrl/getUrl/getURL/getUsername/getUser/getPassword accessors. */
  private static final class NoAccessorsDataSource extends AbstractDataSourceStub {}

  /** {@code getJdbcUrl()} exists but returns null — a non-String reflective result. */
  private static final class NullJdbcUrlDataSource extends AbstractDataSourceStub {
    public String getJdbcUrl() {
      return null;
    }
  }

  /** {@code getJdbcUrl()} exists but returns a blank string. */
  private static final class BlankJdbcUrlDataSource extends AbstractDataSourceStub {
    public String getJdbcUrl() {
      return "   ";
    }
  }

  /** Base stub implementing every {@link DataSource} method with "must not be called". */
  private abstract static class AbstractDataSourceStub implements DataSource {
    @Override
    public Connection getConnection() throws SQLException {
      throw new SQLException("not implemented by this test double");
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }
}
