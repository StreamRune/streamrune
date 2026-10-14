package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A replica that cannot hear forgets — its {@code LISTEN} has not taken effect yet, or its LISTEN
 * connection is down — misses a peer's forget published meanwhile. Once its {@code LISTEN} takes
 * effect it drops every plaintext it cached, so a subject a peer forgot in that window is not
 * served from its cache any longer.
 *
 * <p>Replica B listens on a pool whose borrows the test releases one at a time, so the window in
 * which B cannot hear is held open for exactly as long as the test needs it.
 */
@Testcontainers
class PostgresCryptoForgetListenWindowIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);

  private static PGSimpleDataSource dataSource;
  private static PostgresCryptoEngine delegate;

  @BeforeAll
  static void init() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    String ddl;
    try (var in =
        PostgresCryptoForgetListenWindowIntegrationTest.class.getResourceAsStream(
            "/db/crypto-migration/V001__crypto_baseline.sql")) {
      assertNotNull(in, "shipped migration must be on the classpath");
      ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(ddl);
    }
    delegate = PostgresCryptoEngine.builder().dataSource(dataSource).build();
  }

  @Test
  void aSubjectForgottenBeforeTheFirstListenTookEffectIsNotServedFromTheCacheAfterIt()
      throws Exception {
    SubjectId subject = SubjectId.of("listen-window-first-" + UUID.randomUUID());
    byte[] ciphertext = delegate.encrypt(subject, PLAINTEXT);
    String name = "replica-b-" + UUID.randomUUID();
    var listenPool = new GatedListenPool(name);
    var signalB = new PostgresCryptoForgetSignal(dataSource, () -> listenPool);
    var replicaA =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .forgetSignal(new PostgresCryptoForgetSignal(dataSource))
            .build();
    var replicaB = CachedCryptoEngine.builder().delegate(delegate).forgetSignal(signalB).build();
    try {
      BlockingQueue<String> heardByB = new LinkedBlockingQueue<>();
      signalB.subscribe(heardByB::add);

      // B caches the plaintext while its LISTEN connection is held back.
      assertArrayEquals(PLAINTEXT, replicaB.decrypt(subject, ciphertext));
      // A forgets the subject; B cannot hear it.
      replicaA.deleteKey(subject);
      assertArrayEquals(
          PLAINTEXT,
          replicaB.decrypt(subject, ciphertext),
          "precondition: B missed the forget and still holds the plaintext");

      listenPool.releaseOneBorrow();
      awaitHeard(heardByB);

      assertThrows(
          KeyNotFoundException.class,
          () -> replicaB.decrypt(subject, ciphertext),
          "B served the plaintext of a subject forgotten before its LISTEN took effect");
    } finally {
      replicaA.close();
      replicaB.close();
    }
  }

  @Test
  void aSubjectForgottenWhileTheListenConnectionWasDownIsNotServedFromTheCacheAfterTheReListen()
      throws Exception {
    SubjectId subject = SubjectId.of("listen-window-reconnect-" + UUID.randomUUID());
    byte[] ciphertext = delegate.encrypt(subject, PLAINTEXT);
    String name = "replica-b-" + UUID.randomUUID();
    var listenPool = new GatedListenPool(name);
    var signalB = new PostgresCryptoForgetSignal(dataSource, () -> listenPool);
    var replicaA =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .forgetSignal(new PostgresCryptoForgetSignal(dataSource))
            .build();
    var replicaB = CachedCryptoEngine.builder().delegate(delegate).forgetSignal(signalB).build();
    try {
      BlockingQueue<String> heardByB = new LinkedBlockingQueue<>();
      signalB.subscribe(heardByB::add);
      listenPool.releaseOneBorrow();
      awaitHeard(heardByB);

      // B caches the plaintext while it listens, then loses its LISTEN connection; the reconnect
      // is held back.
      assertArrayEquals(PLAINTEXT, replicaB.decrypt(subject, ciphertext));
      terminateListenSession(name);

      // A forgets the subject while B's LISTEN connection is down; B cannot hear it.
      replicaA.deleteKey(subject);
      assertArrayEquals(
          PLAINTEXT,
          replicaB.decrypt(subject, ciphertext),
          "precondition: B missed the forget and still holds the plaintext");

      listenPool.releaseOneBorrow();
      awaitHeard(heardByB);

      assertThrows(
          KeyNotFoundException.class,
          () -> replicaB.decrypt(subject, ciphertext),
          "B served the plaintext of a subject forgotten while its LISTEN connection was down");
    } finally {
      replicaA.close();
      replicaB.close();
    }
  }

  @Test
  void theChannelListensOnlyWhileItsListenHasTakenEffectAndTellsItsSubscribersFirst()
      throws Exception {
    String name = "replica-b-" + UUID.randomUUID();
    var listenPool = new GatedListenPool(name);
    var signal = new PostgresCryptoForgetSignal(dataSource, () -> listenPool);
    BlockingQueue<String> heard = new LinkedBlockingQueue<>();
    List<String> events = new CopyOnWriteArrayList<>();
    try (signal) {
      assertFalse(signal.isSubscribed());
      // A subscriber whose callback throws does not keep the others from hearing.
      signal.subscribe(
          token -> {},
          () -> {
            throw new IllegalStateException("a subscriber that fails when the channel listens");
          });
      signal.subscribe(
          token -> {
            events.add(token);
            heard.add(token);
          },
          () -> events.add(LISTENING));
      assertTrue(signal.isSubscribed());
      assertFalse(signal.isListening(), "the LISTEN connection is held back");

      listenPool.releaseOneBorrow();
      awaitHeard(heard);
      assertTrue(signal.isListening());
      assertEquals(LISTENING, events.getFirst(), "subscribers are told before any token arrives");

      terminateListenSession(name);
      awaitUntil(() -> !signal.isListening(), "the channel kept reporting a lost LISTEN");
      assertTrue(signal.isSubscribed());
      events.clear();

      listenPool.releaseOneBorrow();
      awaitHeard(heard);
      assertTrue(signal.isListening());
      assertEquals(LISTENING, events.getFirst(), "subscribers are told again after a reconnect");
    }
    assertFalse(signal.isSubscribed());
    assertFalse(signal.isListening());
  }

  @Test
  void aReplicaWhoseListenBackendIsKilledStopsReportingThatItListensBeforeItTriesAgain()
      throws Exception {
    String name = "replica-b-" + UUID.randomUUID();
    var listenPool = new GatedListenPool(name);
    // A reconnect backoff of an hour: B is still waiting to try again when the test looks.
    var signal =
        new PostgresCryptoForgetSignal(
            dataSource,
            () -> listenPool,
            PostgresCryptoForgetSignal.DEFAULT_LIVENESS_PROBE_INTERVAL_MS,
            Duration.ofHours(1).toMillis());
    BlockingQueue<String> heard = new LinkedBlockingQueue<>();
    try (signal) {
      signal.subscribe(heard::add);
      listenPool.releaseOneBorrow();
      awaitHeard(heard);
      assertTrue(signal.isListening());

      terminateListenSession(name);

      awaitUntil(
          () -> !signal.isListening(),
          "B reported that it listens while it waits to re-establish its LISTEN connection");
      assertFalse(listenPool.aBorrowIsWaiting(), "B is still in its reconnect backoff");
    }
  }

  @Test
  void aSubjectForgottenWhileTheListenConnectionWasSilentlyDeadIsNotServedAfterItsProbeFailed()
      throws Exception {
    SubjectId subject = SubjectId.of("listen-window-silent-" + UUID.randomUUID());
    byte[] ciphertext = delegate.encrypt(subject, PLAINTEXT);
    String name = "replica-b-" + UUID.randomUUID();
    var listenPool = new GatedListenPool(name);
    // Probe after 200 ms of silence instead of 30 s.
    var signalB =
        new PostgresCryptoForgetSignal(
            dataSource,
            () -> listenPool,
            200,
            PostgresCryptoForgetSignal.DEFAULT_RECONNECT_BACKOFF_MS);
    var replicaA =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .forgetSignal(new PostgresCryptoForgetSignal(dataSource))
            .build();
    var replicaB = CachedCryptoEngine.builder().delegate(delegate).forgetSignal(signalB).build();
    try {
      BlockingQueue<String> heardByB = new LinkedBlockingQueue<>();
      signalB.subscribe(heardByB::add);
      listenPool.releaseOneBorrow();
      awaitHeard(heardByB);
      int listenPid = listenSessionPid(name);
      assertArrayEquals(PLAINTEXT, replicaB.decrypt(subject, ciphertext));

      // The server of B's LISTEN connection vanishes without closing it: nothing reaches B any
      // more, no error either, and its liveness probe gets no answer.
      SilenceableConnection connection = listenPool.lastHandedOut();
      connection.silence();
      connection.awaitProbe();
      assertTrue(signalB.isListening(), "precondition: until its probe fails, B cannot tell");

      // A forgets the subject; B, deaf, cannot hear it.
      replicaA.deleteKey(subject);
      assertArrayEquals(
          PLAINTEXT,
          replicaB.decrypt(subject, ciphertext),
          "precondition: B missed the forget and still holds the plaintext");

      connection.failProbe();
      awaitUntil(
          () -> !signalB.isListening(),
          "B kept reporting that it listens on a connection that failed its liveness probe");
      awaitBackendGone(listenPid, "the connection that failed its probe was not closed");

      listenPool.releaseOneBorrow();
      awaitHeard(heardByB);

      assertTrue(signalB.isListening());
      assertThrows(
          KeyNotFoundException.class,
          () -> replicaB.decrypt(subject, ciphertext),
          "B served the plaintext of a subject forgotten while its LISTEN connection was dead");
    } finally {
      replicaA.close();
      replicaB.close();
    }
  }

  private static final String LISTENING = "<listening>";

  /** Waits until {@code condition} holds, checking it every 50 ms for up to 15 s. */
  private static void awaitUntil(BooleanSupplier condition, String failure)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(50);
    }
    fail(failure);
  }

  /**
   * Publishes a fresh token until {@code heard} receives it. The channel dispatches a notification
   * only from its receive loop, which it enters after its {@code LISTEN} has taken effect and its
   * subscribers have been told so; receiving the token proves both.
   */
  static void awaitHeard(BlockingQueue<String> heard) throws InterruptedException {
    String token = "probe-" + UUID.randomUUID();
    var publisher = new PostgresCryptoForgetSignal(dataSource);
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      publisher.publish(token);
      String received;
      while ((received = heard.poll(250, TimeUnit.MILLISECONDS)) != null) {
        if (received.equals(token)) {
          return;
        }
      }
    }
    fail("the channel never heard a token published after its LISTEN was released");
  }

  /**
   * The backend of the single session of {@code applicationName}: the LISTEN pool holds one
   * connection, and a test looks while it is listening. Matched by name only: the session's last
   * statement is its {@code LISTEN}, or the empty statement of a liveness probe after it.
   */
  static int listenSessionPid(String applicationName) throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement("SELECT pid FROM pg_stat_activity WHERE application_name = ?")) {
      ps.setString(1, applicationName);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), "the LISTEN session of " + applicationName + " must exist");
        int pid = rs.getInt(1);
        assertFalse(rs.next(), "the LISTEN pool of " + applicationName + " holds one session");
        return pid;
      }
    }
  }

  /**
   * Terminates the LISTEN session of {@code applicationName} and waits until its backend is gone,
   * so a notification committed afterwards cannot reach it.
   */
  static void terminateListenSession(String applicationName) throws Exception {
    int pid = listenSessionPid(applicationName);
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT pg_terminate_backend(?)")) {
      ps.setInt(1, pid);
      ps.execute();
    }
    awaitBackendGone(pid, "the terminated LISTEN backend " + pid + " never went away");
  }

  /** Waits until the backend {@code pid} has ended, checking every 50 ms for up to 15 s. */
  static void awaitBackendGone(int pid, String failure) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (System.nanoTime() < deadline) {
      try (var conn = dataSource.getConnection();
          var ps = conn.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE pid = ?")) {
        ps.setInt(1, pid);
        try (var rs = ps.executeQuery()) {
          rs.next();
          if (rs.getInt(1) == 0) {
            return;
          }
        }
      }
      Thread.sleep(50);
    }
    fail(failure);
  }

  /**
   * A single-connection LISTEN pool whose every borrow waits for a permit the test releases, so the
   * test decides when the channel's {@code LISTEN} — the first one or the one after a lost
   * connection — can take effect. Its sessions carry {@code applicationName}. It hands out each
   * connection as a {@link SilenceableConnection}.
   */
  static final class GatedListenPool implements DataSource, AutoCloseable {
    private final HikariDataSource pool = new HikariDataSource();
    private final Semaphore borrows = new Semaphore(0);
    private volatile SilenceableConnection lastHandedOut;

    GatedListenPool(String applicationName) {
      String base = PG.getJdbcUrl();
      pool.setJdbcUrl(
          base + (base.contains("?") ? "&" : "?") + "ApplicationName=" + applicationName);
      pool.setUsername(PG.getUsername());
      pool.setPassword(PG.getPassword());
      pool.setMaximumPoolSize(1);
      pool.setPoolName("gated-listen-" + applicationName);
    }

    void releaseOneBorrow() {
      borrows.release();
    }

    /** Whether the channel waits for the test to release a borrow. */
    boolean aBorrowIsWaiting() {
      return borrows.hasQueuedThreads();
    }

    SilenceableConnection lastHandedOut() {
      return lastHandedOut;
    }

    @Override
    public Connection getConnection() throws SQLException {
      try {
        borrows.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted while waiting for the test to release a borrow", e);
      }
      var connection = new SilenceableConnection(pool.getConnection());
      lastHandedOut = connection;
      return connection.proxy();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public void close() {
      pool.close();
    }

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

  /**
   * A pooled connection that behaves as itself until the test {@link #silence() silences} it; from
   * then on it acts as a connection whose server vanished without closing it: every wait for
   * notifications returns nothing, and a liveness probe hangs until the test {@link #failProbe()
   * fails} it. Everything else, {@code abort} and {@code close} included, reaches the real
   * connection.
   */
  static final class SilenceableConnection implements InvocationHandler {
    private final Connection real;
    private final PGConnection realPg;
    private volatile boolean silenced;
    private final CountDownLatch probing = new CountDownLatch(1);
    private final CountDownLatch probeFails = new CountDownLatch(1);

    SilenceableConnection(Connection real) throws SQLException {
      this.real = real;
      this.realPg = real.unwrap(PGConnection.class);
    }

    Connection proxy() {
      return (Connection)
          Proxy.newProxyInstance(
              SilenceableConnection.class.getClassLoader(),
              new Class<?>[] {Connection.class, PGConnection.class},
              this);
    }

    void silence() {
      silenced = true;
    }

    void awaitProbe() throws InterruptedException {
      assertTrue(probing.await(15, TimeUnit.SECONDS), "the silenced connection was never probed");
    }

    void failProbe() {
      probeFails.countDown();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      if (name.equals("unwrap") && args[0] == PGConnection.class) {
        return proxy;
      }
      if (silenced && name.equals("getNotifications")) {
        Thread.sleep(args == null ? 0 : (int) args[0]);
        return new PGNotification[0];
      }
      if (silenced && name.equals("isValid")) {
        probing.countDown();
        probeFails.await();
        return false;
      }
      Object target = method.getDeclaringClass() == PGConnection.class ? realPg : real;
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}
