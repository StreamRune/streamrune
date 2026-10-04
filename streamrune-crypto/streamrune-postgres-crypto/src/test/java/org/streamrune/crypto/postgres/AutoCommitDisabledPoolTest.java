package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.types.SubjectId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The crypto stores must make their writes durable on a pool configured with {@code
 * auto-commit=false}. On such a pool a connection opens a transaction implicitly on its first
 * statement; a tombstone INSERT or a {@code pg_notify} that assumed {@code autoCommit=true} was
 * never committed, so HikariCP rolled it back on return and the erasure evidence (or the
 * cross-replica forget) was lost silently. Every write here goes through a store on an {@code
 * autoCommit=false} HikariCP pool and is verified through a separate plain connection.
 */
@Testcontainers
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class AutoCommitDisabledPoolTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_crypto_autocommit_off_test");

  /** The schema owner and the verification path: a plain, autoCommit=true connection per call. */
  static PGSimpleDataSource plain;

  /** The pool under test: HikariCP with {@code autoCommit=false}. */
  static HikariDataSource pool;

  @BeforeAll
  static void init() throws Exception {
    plain = new PGSimpleDataSource();
    plain.setUrl(PG.getJdbcUrl());
    plain.setUser(PG.getUsername());
    plain.setPassword(PG.getPassword());

    String ddl;
    try (var in =
        AutoCommitDisabledPoolTest.class.getResourceAsStream(
            "/db/crypto-migration/V001__crypto_baseline.sql")) {
      assertNotNull(in, "the shipped crypto baseline must be on the classpath");
      ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (var conn = plain.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(ddl);
    }

    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setAutoCommit(false);
    config.setMaximumPoolSize(4);
    config.setPoolName("streamrune-crypto-autocommit-off-under-test");
    pool = new HikariDataSource(config);
  }

  @AfterAll
  static void closePool() {
    pool.close();
  }

  @Test
  void poolUnderTestHandsOutManualCommitConnections() throws Exception {
    try (var conn = pool.getConnection()) {
      assertFalse(conn.getAutoCommit(), "the fixture must exercise autoCommit=false");
    }
  }

  @Test
  void forgottenSubjectStore_tombstoneWritesAreDurable() {
    var store = new JdbcForgottenSubjectStore(pool);
    var verify = new JdbcForgottenSubjectStore(plain);
    var subject = SubjectId.of("autocommit-off@example.com");

    store.forget(subject, Instant.parse("2026-01-01T00:00:00Z"));
    assertTrue(verify.isForgotten(subject), "the tombstone with key evidence must be committed");
    assertEquals(Instant.parse("2026-01-01T00:00:00Z"), verify.keyCreatedAt(subject).orElseThrow());

    store.reinstate(subject);
    assertFalse(verify.isForgotten(subject), "the tombstone removal must be committed");

    store.forget(subject);
    assertTrue(verify.isForgotten(subject), "the plain tombstone must be committed");
  }

  @Test
  void forgetSignal_publishedOnAManualCommitConnectionReachesAListener() throws Exception {
    // pg_notify is delivered at commit: an uncommitted NOTIFY is discarded with its transaction.
    var received = new LinkedBlockingQueue<String>();
    var listening = new PostgresCryptoForgetSignal(plain);
    var publishing = new PostgresCryptoForgetSignal(pool);
    try {
      listening.subscribe(received::add);
      // The LISTEN registration races the first publish; keep publishing until one lands.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      String token = null;
      while (token == null && System.nanoTime() < deadline) {
        publishing.publish("forget-token");
        token = received.poll(200, TimeUnit.MILLISECONDS);
      }
      assertEquals("forget-token", token, "a NOTIFY published on an autoCommit=false connection");
    } finally {
      listening.close();
      publishing.close();
    }
  }
}
