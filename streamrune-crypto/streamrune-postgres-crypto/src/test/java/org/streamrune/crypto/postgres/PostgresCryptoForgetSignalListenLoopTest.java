package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * The listen loop of {@link PostgresCryptoForgetSignal} against LISTEN connections the test drives:
 * what each wait for notifications yields, whether the connection answers its liveness probe, and
 * when the pool hands out the next connection. The same behaviour against a real PostgreSQL is
 * covered by {@link PostgresCryptoForgetListenWindowIntegrationTest}.
 */
class PostgresCryptoForgetSignalListenLoopTest {

  private static final long ONE_HOUR_MS = Duration.ofHours(1).toMillis();

  @Test
  void theChannelStopsReportingThatItListensAsSoonAsItsConnectionFails() throws Exception {
    var pool = new FakeListenPool();
    FakeListenConnection first = pool.handOutNext();
    // A reconnect backoff of an hour: the loop is still backing off when the test looks.
    var signal = new PostgresCryptoForgetSignal(pool, () -> pool, ONE_HOUR_MS, ONE_HOUR_MS);
    try (signal) {
      signal.subscribe(token -> {});
      awaitUntil(signal::isListening, "the channel never listened");

      first.deliveries.add(new SQLException("connection reset"));

      awaitUntil(
          () -> !signal.isListening(),
          "the channel reported that it listens while it waits to reconnect");
      assertEquals(1, pool.borrows.get(), "the loop is still in its reconnect backoff");
      assertTrue(first.closed.get());
    }
  }

  @Test
  void aConnectionThatStopsAnsweringIsReplacedAndItsSubscribersEvictAgain() throws Exception {
    var pool = new FakeListenPool();
    FakeListenConnection first = pool.handOutNext();
    var signal = new PostgresCryptoForgetSignal(pool, () -> pool, 50, 10);
    var listeningRounds = new AtomicInteger();
    try (signal) {
      signal.subscribe(token -> {}, listeningRounds::incrementAndGet);
      awaitUntil(signal::isListening, "the channel never listened");
      assertEquals(1, listeningRounds.get());

      // The server vanished without closing the connection: every wait for notifications returns
      // nothing and no error ever arrives; only a probe that needs an answer can tell.
      first.answersProbe.set(false);

      awaitUntil(
          () -> !signal.isListening(),
          "a connection that failed its liveness probe was still reported listening");
      assertTrue(first.aborted.get(), "the dead connection is aborted, not handed back for reuse");
      assertTrue(first.closed.get());

      pool.handOutNext();
      awaitUntil(signal::isListening, "the channel never listened on the replacement connection");
      assertEquals(
          2, listeningRounds.get(), "subscribers evict again once the replacement LISTEN works");
      assertEquals(2, pool.borrows.get());
    }
  }

  @Test
  void aDeadConnectionWhoseAbortFailsIsReplacedAllTheSame() throws Exception {
    var pool = new FakeListenPool();
    FakeListenConnection first = pool.handOutNext();
    first.abortFails = true;
    var signal = new PostgresCryptoForgetSignal(pool, () -> pool, 50, 10);
    var listeningRounds = new AtomicInteger();
    try (signal) {
      signal.subscribe(token -> {}, listeningRounds::incrementAndGet);
      awaitUntil(signal::isListening, "the channel never listened");

      first.answersProbe.set(false);
      awaitUntil(() -> !signal.isListening(), "the dead connection was still reported listening");
      pool.handOutNext();

      awaitUntil(() -> listeningRounds.get() == 2, "the channel never listened again");
      assertTrue(first.aborted.get());
    }
  }

  @Test
  void aQuietConnectionThatAnswersItsProbeIsKept() throws Exception {
    var pool = new FakeListenPool();
    FakeListenConnection first = pool.handOutNext();
    var signal = new PostgresCryptoForgetSignal(pool, () -> pool, 20, 10);
    var listeningRounds = new AtomicInteger();
    try (signal) {
      signal.subscribe(token -> {}, listeningRounds::incrementAndGet);

      awaitUntil(() -> first.probes.get() >= 3, "a quiet connection was never probed");

      assertTrue(signal.isListening());
      assertEquals(1, pool.borrows.get(), "a connection that answers is kept");
      assertEquals(1, listeningRounds.get(), "nothing is evicted while the connection answers");
    }
  }

  @Test
  void aSubscriberCallbackThatThrowsAnErrorDoesNotStopTheChannel() throws Exception {
    var pool = new FakeListenPool();
    FakeListenConnection first = pool.handOutNext();
    var signal = new PostgresCryptoForgetSignal(pool, () -> pool, ONE_HOUR_MS, 10);
    BlockingQueue<String> heard = new LinkedBlockingQueue<>();
    try (signal) {
      signal.subscribe(
          token -> {
            throw new AssertionError("a subscriber that fails on every token");
          },
          () -> {
            throw new AssertionError("a subscriber that fails when the channel listens");
          });
      signal.subscribe(heard::add);

      awaitUntil(signal::isListening, "an Error thrown by onListening stopped the channel");
      first.deliveries.add("token-1");

      assertEquals("token-1", heard.poll(15, TimeUnit.SECONDS), "the other subscriber hears");
      assertTrue(signal.isListening(), "an Error thrown by onForget stopped the channel");
    }
  }

  @Test
  void aConnectionThatFailsWithARuntimeExceptionIsReplaced() throws Exception {
    var pool = new FakeListenPool();
    FakeListenConnection first = pool.handOutNext();
    var signal = new PostgresCryptoForgetSignal(pool, () -> pool, ONE_HOUR_MS, 10);
    var listeningRounds = new AtomicInteger();
    try (signal) {
      signal.subscribe(token -> {}, listeningRounds::incrementAndGet);
      awaitUntil(signal::isListening, "the channel never listened");

      first.deliveries.add(new IllegalStateException("a driver that fails unchecked"));
      awaitUntil(() -> !signal.isListening(), "the failed connection was still reported listening");
      pool.handOutNext();

      awaitUntil(() -> listeningRounds.get() == 2, "the channel never listened again");
      assertTrue(signal.isListening());
    }
  }

  @Test
  void theProbeIntervalAndTheReconnectBackoffMustBePositive() {
    var pool = new FakeListenPool();
    assertThrows(
        IllegalArgumentException.class,
        () -> new PostgresCryptoForgetSignal(pool, () -> pool, 0, 10));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PostgresCryptoForgetSignal(pool, () -> pool, 10, 0));
  }

  /** Waits until {@code condition} holds, checking it every 10 ms for up to 15 s. */
  private static void awaitUntil(BooleanSupplier condition, String failure)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(10);
    }
    fail(failure);
  }

  // -------------------------------------------------------------------------
  // Test doubles
  // -------------------------------------------------------------------------

  /**
   * A LISTEN connection the test drives. Each wait for notifications takes the next delivery: a
   * token is returned as a notification, a {@link Throwable} is thrown; with none queued the wait
   * returns nothing after its timeout, as on a quiet channel.
   */
  private static final class FakeListenConnection implements InvocationHandler {
    final BlockingQueue<Object> deliveries = new LinkedBlockingQueue<>();
    final AtomicBoolean answersProbe = new AtomicBoolean(true);
    final AtomicInteger probes = new AtomicInteger();
    final AtomicBoolean aborted = new AtomicBoolean();
    final AtomicBoolean closed = new AtomicBoolean();
    volatile boolean abortFails;

    final Connection connection =
        (Connection)
            Proxy.newProxyInstance(
                FakeListenConnection.class.getClassLoader(),
                new Class<?>[] {Connection.class, PGConnection.class},
                this);

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return switch (method.getName()) {
        case "getAutoCommit" -> true;
        case "createStatement" -> statement();
        case "unwrap" -> proxy;
        case "getNotifications" -> nextNotifications(args == null ? 0 : (int) args[0]);
        case "isValid" -> {
          probes.incrementAndGet();
          yield answersProbe.get();
        }
        case "abort" -> {
          aborted.set(true);
          if (abortFails) {
            throw new SQLException("abort failed");
          }
          yield null;
        }
        case "close" -> {
          closed.set(true);
          yield null;
        }
        case "isClosed" -> closed.get();
        case "hashCode" -> System.identityHashCode(proxy);
        case "equals" -> proxy == args[0];
        case "toString" -> "FakeListenConnection";
        default -> throw new UnsupportedOperationException(method.getName());
      };
    }

    private PGNotification[] nextNotifications(int timeoutMs) throws Throwable {
      Object next;
      try {
        next = deliveries.poll(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted", e);
      }
      if (next == null) {
        return new PGNotification[0];
      }
      if (next instanceof Throwable failure) {
        throw failure;
      }
      return new PGNotification[] {notification((String) next)};
    }

    private static Statement statement() {
      return (Statement)
          Proxy.newProxyInstance(
              FakeListenConnection.class.getClassLoader(),
              new Class<?>[] {Statement.class},
              (proxy, method, args) ->
                  switch (method.getName()) {
                    case "execute" -> false;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                  });
    }

    private static PGNotification notification(String token) {
      return new PGNotification() {
        @Override
        public String getName() {
          return PostgresCryptoForgetSignal.CHANNEL;
        }

        @Override
        public int getPID() {
          return 0;
        }

        @Override
        public String getParameter() {
          return token;
        }
      };
    }
  }

  /** Hands out the connections the test queued, one per borrow, and waits while none is queued. */
  private static final class FakeListenPool implements DataSource, AutoCloseable {
    private final BlockingQueue<FakeListenConnection> next = new LinkedBlockingQueue<>();
    final AtomicInteger borrows = new AtomicInteger();

    FakeListenConnection handOutNext() {
      var connection = new FakeListenConnection();
      next.add(connection);
      return connection;
    }

    @Override
    public Connection getConnection() throws SQLException {
      try {
        Connection connection = next.take().connection;
        borrows.incrementAndGet();
        return connection;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted while waiting for a connection", e);
      }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public void close() {}

    @Override
    public PrintWriter getLogWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(PrintWriter out) {
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
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }
}
