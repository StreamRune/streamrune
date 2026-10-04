package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
 * End-to-end: two {@link CachedCryptoEngine} instances (two replicas) over one shared PostgreSQL
 * key store, each with its own {@link PostgresCryptoForgetSignal} listening on the same {@code
 * LISTEN/NOTIFY} channel. Asserts that a {@code deleteKey} on replica A promptly stops replica B
 * from serving the forgotten subject's cached plaintext — the fleet-effective-erasure guarantee.
 */
@Testcontainers
class PostgresCryptoForgetSignalIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static PGSimpleDataSource dataSource;
  private static PostgresCryptoEngine delegate;

  @BeforeAll
  static void init() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    applyMigration("/db/crypto-migration/V001__crypto_baseline.sql");
    delegate = PostgresCryptoEngine.builder().dataSource(dataSource).build();
  }

  private static void applyMigration(String resource) throws Exception {
    String ddl;
    try (var in = PostgresCryptoForgetSignalIntegrationTest.class.getResourceAsStream(resource)) {
      assertNotNull(in, "shipped migration must be on the classpath: " + resource);
      ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(ddl);
    }
  }

  @Test
  void deleteKeyOnOneReplicaStopsAnotherReplicaServingCachedPlaintextViaNotify() throws Exception {
    SubjectId subject = SubjectId.of("gdpr-erasure-subject");
    byte[] ciphertext =
        delegate.encrypt(subject, "alice@example.com".getBytes(StandardCharsets.UTF_8));

    // Two replicas over the same delegate + DB, each with its own real NOTIFY channel.
    var replicaA =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .forgetSignal(new PostgresCryptoForgetSignal(dataSource))
            .build();
    var replicaB =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .forgetSignal(new PostgresCryptoForgetSignal(dataSource))
            .build();
    try {
      // B caches the decrypted plaintext for the subject (a cache hit is served without the
      // delegate).
      assertArrayEquals(
          "alice@example.com".getBytes(StandardCharsets.UTF_8),
          replicaB.decrypt(subject, ciphertext),
          "B should decrypt and cache the plaintext");

      // A processes the GDPR erasure: deletes the DB key AND publishes the cross-replica forget.
      replicaA.deleteKey(subject);

      // B must stop serving the cached plaintext promptly: once its cache is invalidated by the
      // NOTIFY, the next decrypt misses the cache, hits the (now key-less) delegate, and throws
      // KeyNotFoundException. Without the signal, B would keep serving the cached plaintext until
      // the
      // write-TTL (default 5 min), so this bounded wait would time out — the fail-first behavior.
      awaitCacheInvalidated(replicaB, subject, ciphertext, Duration.ofSeconds(15));
    } finally {
      replicaA.close();
      replicaB.close();
    }
  }

  private static void awaitCacheInvalidated(
      CachedCryptoEngine engine, SubjectId subject, byte[] ciphertext, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        engine.decrypt(subject, ciphertext);
        // Still served from cache (stale plaintext) — keep waiting for the NOTIFY to land.
        Thread.sleep(100);
      } catch (KeyNotFoundException _) {
        // Cache was invalidated and the delegate's key is gone: erasure is now fleet-effective on
        // B.
        return;
      }
    }
    // One last attempt so the failure surfaces as the actual behavior rather than a bare timeout.
    assertThrows(
        KeyNotFoundException.class,
        () -> engine.decrypt(subject, ciphertext),
        "B kept serving cached plaintext past the wait — the cross-replica forget signal did not"
            + " invalidate its cache");
    fail("unreachable");
  }

  @Test
  void publishToleratesTransportFailureWithoutThrowing() {
    // Best-effort contract: a publish that cannot reach the DB must NOT throw (it would otherwise
    // fail the originating deleteKey). Point the channel at an unreachable server and publish.
    var broken = new PGSimpleDataSource();
    broken.setUrl("jdbc:postgresql://127.0.0.1:1/nonexistent");
    broken.setConnectTimeout(1);
    var channel = new PostgresCryptoForgetSignal(broken);
    try (channel) {
      channel.publish("some-opaque-token"); // must swallow the connection failure, not throw
      assertTrue(true);
    }
  }

  @Test
  void closeIsIdempotentAfterASuccessfulSubscribe() throws Exception {
    // Exercises the double-close path: the first close() hands the dedicated LISTEN pool over for
    // closing, so the second finds none and closes nothing.
    var channel = new PostgresCryptoForgetSignal(dataSource);
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    channel.subscribe(received::add);
    channel.publish("double-close-token");
    assertEquals(
        "double-close-token",
        received.poll(15, TimeUnit.SECONDS),
        "LISTEN must be established before exercising close()");

    channel.close();
    assertDoesNotThrow(
        channel::close, "a second close() on an already-closed pool must be a no-op");
  }

  @Test
  void subscribeUsesHikariAccessorsDirectlyWhenTheAppDataSourceIsHikari() throws Exception {
    // Exercises the `dataSource instanceof HikariDataSource` branch of createListenDataSource: the
    // app's own DataSource is itself Hikari-backed, so its jdbcUrl/username/password are read via
    // the direct getters rather than the reflective firstString() fallback.
    var hikari = new HikariDataSource();
    hikari.setJdbcUrl(PG.getJdbcUrl());
    hikari.setUsername(PG.getUsername());
    hikari.setPassword(PG.getPassword());
    hikari.setMaximumPoolSize(2);
    hikari.setPoolName("test-app-hikari-pool");
    try (hikari;
        var channel = new PostgresCryptoForgetSignal(hikari)) {
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      channel.subscribe(received::add);
      channel.publish("hikari-instanceof-branch-token");
      assertEquals("hikari-instanceof-branch-token", received.poll(15, TimeUnit.SECONDS));
    }
  }

  @Test
  void subscribeSucceedsWhenDataSourceExposesOnlyAUrlWithEmbeddedCredentials() throws Exception {
    // getJdbcUrl() alone is present (credentials embedded as URL query params);
    // getUsername()/getUser()/getPassword() are absent, so firstString() must return null for both
    // and createListenDataSource must still build a working pool from the URL alone.
    String base = PG.getJdbcUrl();
    String separator = base.contains("?") ? "&" : "?";
    String urlWithCredentials =
        base + separator + "user=" + PG.getUsername() + "&password=" + PG.getPassword();
    var minimal = new UrlOnlyDataSource(urlWithCredentials);
    var channel = new PostgresCryptoForgetSignal(minimal);
    try {
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      channel.subscribe(received::add);
      channel.publish("url-only-branch-token");
      assertEquals("url-only-branch-token", received.poll(15, TimeUnit.SECONDS));
    } finally {
      channel.close();
    }
  }

  @Test
  void listensOnASuppliedPoolWhoseConnectionsDoNotAutoCommit() throws Exception {
    // A LISTEN issued inside an open transaction registers only when that transaction commits, so
    // on a pool that hands out non-auto-commit connections nothing would ever arrive unless the
    // channel switches its connection to auto-commit itself.
    var listenPool = new HikariDataSource();
    listenPool.setJdbcUrl(PG.getJdbcUrl());
    listenPool.setUsername(PG.getUsername());
    listenPool.setPassword(PG.getPassword());
    listenPool.setAutoCommit(false);
    listenPool.setMaximumPoolSize(1);
    listenPool.setPoolName("test-supplied-listen-pool");
    var channel = new PostgresCryptoForgetSignal(dataSource, () -> listenPool);
    try (channel) {
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      channel.subscribe(received::add);
      awaitDelivered(received, "supplied-pool-token");
    }
    assertTrue(listenPool.isClosed(), "closing the channel closes the LISTEN pool it was handed");
  }

  /**
   * Publishes {@code token} until it arrives: the LISTEN is issued by the channel's own thread, so
   * one publish right after subscribe can precede it.
   */
  private static void awaitDelivered(BlockingQueue<String> received, String token)
      throws InterruptedException {
    var publisher = new PostgresCryptoForgetSignal(dataSource);
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (System.nanoTime() < deadline) {
      publisher.publish(token);
      if (token.equals(received.poll(250, TimeUnit.MILLISECONDS))) {
        return;
      }
    }
    fail("the published token never reached the listener");
  }

  /**
   * A minimal {@link DataSource} exposing only {@code getJdbcUrl()} (with embedded credentials) —
   * no {@code getUsername()}/{@code getUser()}/{@code getPassword()} accessors at all — so {@link
   * PostgresCryptoForgetSignal}'s reflective {@code firstString} must fall through to null for
   * both, exercising the username/password-absent branches of {@code createListenDataSource}.
   */
  private static final class UrlOnlyDataSource implements DataSource {
    private final String jdbcUrl;

    UrlOnlyDataSource(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
    }

    /** Reflectively discovered by name via {@code PostgresCryptoForgetSignal.firstString}. */
    public String getJdbcUrl() {
      return jdbcUrl;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return java.sql.DriverManager.getConnection(jdbcUrl);
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
