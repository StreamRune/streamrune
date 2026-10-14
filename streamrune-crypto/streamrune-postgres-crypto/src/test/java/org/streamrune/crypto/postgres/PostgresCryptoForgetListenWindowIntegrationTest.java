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
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
   * Terminates the LISTEN session of {@code applicationName} and waits until its backend is gone,
   * so a notification committed afterwards cannot reach it.
   */
  static void terminateListenSession(String applicationName) throws Exception {
    int pid;
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE application_name = ? AND query = ?")) {
      ps.setString(1, applicationName);
      ps.setString(2, "LISTEN " + PostgresCryptoForgetSignal.CHANNEL);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), "the LISTEN session of " + applicationName + " must exist");
        pid = rs.getInt(1);
      }
    }
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT pg_terminate_backend(?)")) {
      ps.setInt(1, pid);
      ps.execute();
    }
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
    fail("the terminated LISTEN backend " + pid + " never went away");
  }

  /**
   * A single-connection LISTEN pool whose every borrow waits for a permit the test releases, so the
   * test decides when the channel's {@code LISTEN} — the first one or the one after a lost
   * connection — can take effect. Its sessions carry {@code applicationName}.
   */
  static final class GatedListenPool implements DataSource, AutoCloseable {
    private final HikariDataSource pool = new HikariDataSource();
    private final Semaphore borrows = new Semaphore(0);

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

    @Override
    public Connection getConnection() throws SQLException {
      try {
        borrows.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted while waiting for the test to release a borrow", e);
      }
      return pool.getConnection();
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
}
