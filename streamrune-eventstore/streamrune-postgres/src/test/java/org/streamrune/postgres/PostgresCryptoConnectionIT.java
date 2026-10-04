package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Reproduces (and then guards against) JDBC connection-pool starvation caused by {@code @Encrypted}
 * field serialization happening while a pooled connection is held.
 *
 * <p>{@link CryptoShreddingModule} calls {@link CryptoEngine#encrypt} synchronously from inside
 * Jackson's {@code writeValueAsString} — a real KMS/Vault backend means that call is an HTTP round
 * trip. If that serialization happens after {@code dataSource.getConnection()}, every concurrent
 * append holds its pooled connection for the full crypto round-trip, and once concurrency exceeds
 * the pool size the pool starves: later callers time out waiting for a connection that is doing
 * nothing but waiting on KMS.
 *
 * <p>This test uses a deliberately small pool ({@code maximumPoolSize=2}) and a {@link
 * SlowCryptoEngine} test double whose {@code encrypt}/{@code decrypt} sleep for {@code
 * CRYPTO_DELAY_MILLIS}, then fires {@code 3x} the pool size worth of concurrent {@code append}
 * calls (on distinct streams, so there is no optimistic-lock contention). With crypto serialization
 * inside the connection scope, each wave of {@code poolSize} concurrent appends monopolizes every
 * pooled connection for the full crypto delay, so the third wave queues behind roughly {@code 2 *
 * CRYPTO_DELAY_MILLIS} of held connections — comfortably exceeding {@code connectionTimeout} and
 * throwing {@code SQLTransientConnectionException}. With serialization moved before {@code
 * getConnection()}, each connection is only ever held for the fast DB work, so all calls complete
 * almost immediately regardless of how slow the crypto engine is.
 */
@Testcontainers
class PostgresCryptoConnectionIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("crypto_conn_test");

  private static final int POOL_SIZE = 2;
  private static final int CONCURRENT_APPENDS = POOL_SIZE * 3;
  private static final long CRYPTO_DELAY_MILLIS = 2_000;

  static PGSimpleDataSource schemaDataSource;
  static EventTypeRegistry typeRegistry;

  HikariDataSource pooledDataSource;
  PostgresEventStore store;

  record PiiEvent(String subjectId, @Encrypted(subjectId = "subjectId") String secret)
      implements DomainEvent {}

  /**
   * CryptoEngine test double whose encrypt/decrypt block, simulating slow KMS/Vault round trips.
   */
  static final class SlowCryptoEngine implements CryptoEngine {
    private final AtomicInteger encryptCalls = new AtomicInteger();

    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      encryptCalls.incrementAndGet();
      sleep();
      // Not real encryption — good enough for this test, which only cares about timing.
      return plaintext;
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      sleep();
      return ciphertext;
    }

    @Override
    public void deleteKey(SubjectId subjectId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }

    private static void sleep() {
      try {
        Thread.sleep(CRYPTO_DELAY_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    int encryptCallCount() {
      return encryptCalls.get();
    }

    /**
     * Non-blocking variant used only to pre-populate fixture data for read-path tests, so setup
     * never itself contends for the small pool under test.
     */
    static final class Fast implements CryptoEngine {
      @Override
      public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
        return plaintext;
      }

      @Override
      public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
        return ciphertext;
      }

      @Override
      public void deleteKey(SubjectId subjectId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isKeyAvailable(SubjectId subjectId) {
        return true;
      }
    }
  }

  @BeforeAll
  static void initSchema() {
    schemaDataSource = new PGSimpleDataSource();
    schemaDataSource.setUrl(PG.getJdbcUrl());
    schemaDataSource.setUser(PG.getUsername());
    schemaDataSource.setPassword(PG.getPassword());
    Flyway.configure()
        .dataSource(schemaDataSource)
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .load()
        .migrate();
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return PiiEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return null;
          }
        };
  }

  @BeforeEach
  void setUp() throws Exception {
    var hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(PG.getJdbcUrl());
    hikariConfig.setUsername(PG.getUsername());
    hikariConfig.setPassword(PG.getPassword());
    hikariConfig.setMaximumPoolSize(POOL_SIZE);
    hikariConfig.setMinimumIdle(POOL_SIZE);
    hikariConfig.setConnectionTimeout(3_000);
    hikariConfig.setPoolName("crypto-starve-test-" + System.nanoTime());
    pooledDataSource = new HikariDataSource(hikariConfig);

    var objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.registerModule(new CryptoShreddingModule(new SlowCryptoEngine()));

    store = new PostgresEventStore(pooledDataSource, objectMapper, typeRegistry, null, null);

    try (var conn = schemaDataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
    }
  }

  @AfterEach
  void tearDown() {
    if (pooledDataSource != null) {
      pooledDataSource.close();
    }
  }

  private EventEnvelope envelope(StreamId streamId) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(1),
        new EventType("PiiEvent"),
        new PiiEvent("subject-" + streamId.value(), "super-secret-pii"),
        new EventMetadata(
            EventId.of("evt-" + streamId.value()),
            CommandId.of("cmd-" + streamId.value()),
            null,
            null,
            CorrelationId.of("corr-" + streamId.value()),
            null,
            null,
            Instant.now()));
  }

  /**
   * The gate: {@code poolSize + 2} concurrent appends of {@code @Encrypted} events, each on its own
   * stream, must ALL complete without any of them throwing a HikariCP pool-timeout ({@code
   * SQLTransientConnectionException}). Before the fix (crypto serialization inside the connection
   * scope) this reliably times out because concurrency exceeds the pool size and every connection
   * is held for the full {@code CRYPTO_DELAY_MILLIS} KMS round trip. After the fix, each connection
   * is only held for the fast DB work, so the small pool is never starved no matter how slow crypto
   * is.
   */
  @Test
  void concurrentEncryptingAppends_doNotStarvePool() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_APPENDS);
    try {
      List<Future<AppendResult>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < CONCURRENT_APPENDS; i++) {
        StreamId streamId = TestStreams.stream("crypto-stream-" + i);
        futures.add(
            executor.submit(
                () -> store.append(streamId, List.of(envelope(streamId)), Version.initial())));
      }

      List<Throwable> failures = new java.util.ArrayList<>();
      int completed = 0;
      for (Future<AppendResult> future : futures) {
        try {
          future.get(30, TimeUnit.SECONDS);
          completed++;
        } catch (Exception e) {
          failures.add(rootCause(e));
        }
      }

      assertTrue(
          failures.isEmpty(),
          () ->
              "Expected all "
                  + CONCURRENT_APPENDS
                  + " concurrent encrypting appends to complete without pool starvation, but got"
                  + " failures (root cause of each, e.g. HikariCP's"
                  + " SQLTransientConnectionException means the pool starved): "
                  + failures.stream().map(Object::toString).toList());
      assertEquals(CONCURRENT_APPENDS, completed);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * The read-side gate: {@code poolSize * 3} concurrent {@code readGlobalStream} calls, each
   * decrypting an {@code @Encrypted} field via the {@link SlowCryptoEngine}, must ALL complete
   * without any HikariCP pool-timeout. The events are pre-appended with a plain (fast) crypto
   * engine on a throwaway store so the setup itself never contends for the tiny pool; only the
   * concurrent reads under test use {@code store} (backed by {@code pooledDataSource} + {@code
   * SlowCryptoEngine}). Before the fix (decrypt — {@code objectMapper.readValue} on an
   * {@code @Encrypted} payload — running inside the {@code try (var conn = ...)} scope while
   * iterating the ResultSet) this reliably times out the same way the write-side test did. After
   * the fix, raw rows are collected and the connection is closed BEFORE any decrypt runs, so the
   * small pool is never held across the slow crypto round trip.
   */
  @Test
  void concurrentDecryptingReads_doNotStarvePool() throws Exception {
    // Pre-append using a fast (non-blocking) crypto engine on a separate, larger-pool store so
    // setup never itself starves the small pool under test.
    var fastObjectMapper = new ObjectMapper();
    fastObjectMapper.registerModule(new JavaTimeModule());
    fastObjectMapper.registerModule(new CryptoShreddingModule(new SlowCryptoEngine.Fast()));
    var setupStore =
        new PostgresEventStore(schemaDataSource, fastObjectMapper, typeRegistry, null, null);
    for (int i = 0; i < CONCURRENT_APPENDS; i++) {
      StreamId streamId = TestStreams.stream("crypto-read-stream-" + i);
      setupStore.append(streamId, List.of(envelope(streamId)), Version.initial());
    }

    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_APPENDS);
    try {
      List<Future<List<EventEnvelope>>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < CONCURRENT_APPENDS; i++) {
        futures.add(executor.submit(() -> store.readGlobalStream(GlobalOffset.initial(), 100)));
      }

      List<Throwable> failures = new java.util.ArrayList<>();
      int completed = 0;
      for (Future<List<EventEnvelope>> future : futures) {
        try {
          List<EventEnvelope> events = future.get(30, TimeUnit.SECONDS);
          assertTrue(events.size() >= CONCURRENT_APPENDS, "all pre-appended events must be read");
          completed++;
        } catch (Exception e) {
          failures.add(rootCause(e));
        }
      }

      assertTrue(
          failures.isEmpty(),
          () ->
              "Expected all "
                  + CONCURRENT_APPENDS
                  + " concurrent decrypting reads to complete without pool starvation, but got"
                  + " failures (root cause of each, e.g. HikariCP's"
                  + " SQLTransientConnectionException means the pool starved): "
                  + failures.stream().map(Object::toString).toList());
      assertEquals(CONCURRENT_APPENDS, completed);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Unwraps to the innermost cause, so a wrapped {@code EventStoreException} shows the real
   * HikariCP/SQL failure (e.g. {@code SQLTransientConnectionException}) instead of just the
   * wrapper's own message.
   */
  private static Throwable rootCause(Throwable t) {
    Throwable cause = t;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }
}
