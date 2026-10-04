package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The saga-store counterpart of {@link PostgresCryptoConnectionIT}: {@code PostgresSagaStore}'s
 * create path must serialize (and therefore encrypt) <b>before</b> it acquires a pooled JDBC
 * connection, never while holding one.
 *
 * <p>{@link CryptoShreddingModule} calls {@link CryptoEngine#encrypt} synchronously from inside
 * Jackson's {@code writeValueAsString}. Doing that inside the connection scope has two distinct
 * consequences, and this IT pins both:
 *
 * <ul>
 *   <li><b>Self-deadlock</b> with {@code PostgresCryptoEngine}, which the Spring/Quarkus/Micronaut
 *       auto-configs wire onto the <em>same</em> {@code DataSource} as the saga store. Every thread
 *       holds a pooled connection and then asks the same pool for a second one, so once concurrency
 *       reaches the pool size the pool can never drain. It unwinds only at {@code
 *       connectionTimeout}. This is the case {@link
 *       #concurrentCreatesWithSamePoolCryptoEngine_doNotSelfDeadlockThePool()} proves — a test that
 *       merely asserted call ordering would not.
 *   <li><b>Starvation</b> with a remote engine (Vault/AWS-KMS): no ring-deadlock, but each
 *       connection is pinned for a full HTTP round trip, so the pool starves at a fraction of the
 *       concurrency the event-store path survives.
 * </ul>
 *
 * <p>{@code update}, {@code claimCompensating} and {@code load} already had the correct order (and
 * say so in their comments); {@code doCreate} — serving {@code create}, {@code
 * createGenesisPending} and {@code createFaulted} — did not, and nothing covered it: {@code
 * PostgresSagaStoreCryptoShreddingTest} asserts ciphertext at rest but never connection scope.
 */
@Testcontainers
class PostgresSagaStoreCryptoConnectionIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("saga_crypto_conn_test");

  private static final int POOL_SIZE = 2;
  private static final int CONCURRENT_CREATES = POOL_SIZE * 3;
  private static final long CRYPTO_DELAY_MILLIS = 2_000;
  private static final SagaType SAGA_TYPE = SagaType.of("CustomerSaga");

  static PGSimpleDataSource schemaDataSource;

  HikariDataSource pooledDataSource;

  /** Saga state carrying one {@code @Encrypted} PII field, keyed off {@code customerId}. */
  record CustomerSagaState(
      SagaStatus status, String customerId, @Encrypted(subjectId = "customerId") String email)
      implements SagaState {

    @JsonCreator
    CustomerSagaState(
        @JsonProperty("status") SagaStatus status,
        @JsonProperty("customerId") String customerId,
        @JsonProperty("email") String email) {
      this.status = status;
      this.customerId = customerId;
      this.email = email;
    }
  }

  /** CryptoEngine double whose encrypt blocks, simulating a slow KMS/Vault round trip. */
  static final class SlowCryptoEngine implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      sleep();
      // Not real encryption — this test only cares about when the call happens, not what it does.
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
  }

  @BeforeAll
  static void initSchema() {
    schemaDataSource = new PGSimpleDataSource();
    schemaDataSource.setUrl(PG.getJdbcUrl());
    schemaDataSource.setUser(PG.getUsername());
    schemaDataSource.setPassword(PG.getPassword());

    EventTypeRegistry registry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of();
          }
        };
    // Applies the event-store baseline (incl. saga_state) AND — because the engine reports required
    // crypto tables — db/crypto-migration on its own history table, so the real
    // PostgresCryptoEngine below has encryption_keys/forgotten_subjects. Runs on the unpooled
    // schema DataSource so setup never touches the tiny pool under test.
    new PostgresEventStoreFactory(schemaDataSource, registry)
        .cryptoEngine(PostgresCryptoEngine.builder().dataSource(schemaDataSource).build())
        .initializeSchema();
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
    hikariConfig.setPoolName("saga-crypto-starve-test-" + System.nanoTime());
    pooledDataSource = new HikariDataSource(hikariConfig);

    try (var conn = schemaDataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_state");
    }
  }

  @AfterEach
  void tearDown() {
    if (pooledDataSource != null) {
      pooledDataSource.close();
    }
  }

  private PostgresSagaStore storeWith(CryptoEngine engine) {
    var objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.registerModule(new CryptoShreddingModule(engine));
    return new PostgresSagaStore(pooledDataSource, objectMapper);
  }

  private static CustomerSagaState state(int i) {
    return new CustomerSagaState(
        SagaStatus.RUNNING, "customer-" + i, "customer" + i + "@example.com");
  }

  /**
   * THE DEADLOCK PROOF. A real {@link PostgresCryptoEngine} on the <em>same</em> pool the saga
   * store uses — exactly what {@code PostgresCryptoEngineConfiguration} wires in Spring Boot, where
   * the engine takes the application {@code DataSource} bean. With serialization inside the
   * connection scope, {@code POOL_SIZE} concurrent creates each hold their connection and then
   * block forever on {@code dataSource.getConnection()} inside {@code encrypt}: the pool is 100%
   * held by the very threads waiting on it. Every one of them fails with HikariCP's {@code
   * SQLTransientConnectionException} after {@code connectionTimeout}, and for that whole window the
   * pool is empty for every OTHER component too (event append, projection commit, command bus).
   */
  @Test
  void concurrentCreatesWithSamePoolCryptoEngine_doNotSelfDeadlockThePool() throws Exception {
    var sagaStore = storeWith(PostgresCryptoEngine.builder().dataSource(pooledDataSource).build());

    ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);
    try {
      List<Future<?>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < POOL_SIZE; i++) {
        int n = i;
        futures.add(
            executor.submit(
                () ->
                    sagaStore.create(
                        SagaId.of("deadlock-saga-" + n), SAGA_TYPE, state(n), SagaStatus.RUNNING)));
      }
      assertNoPoolFailures(futures, POOL_SIZE, "self-deadlocking creates");
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * The starvation gate for {@code create} with a slow REMOTE engine (Vault/AWS-KMS): {@code
   * POOL_SIZE * 3} concurrent creates on distinct saga ids must all complete without a pool
   * timeout. Before the fix each wave of {@code POOL_SIZE} monopolizes every connection for the
   * full crypto round trip, so the third wave queues behind ~{@code 2 * CRYPTO_DELAY_MILLIS} —
   * comfortably past {@code connectionTimeout}.
   */
  @Test
  void concurrentEncryptingCreates_doNotStarvePool() throws Exception {
    var sagaStore = storeWith(new SlowCryptoEngine());

    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CREATES);
    try {
      List<Future<?>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < CONCURRENT_CREATES; i++) {
        int n = i;
        futures.add(
            executor.submit(
                () ->
                    sagaStore.create(
                        SagaId.of("starve-saga-" + n), SAGA_TYPE, state(n), SagaStatus.RUNNING)));
      }
      assertNoPoolFailures(futures, CONCURRENT_CREATES, "encrypting creates");
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertNoPoolFailures(List<Future<?>> futures, int expected, String what)
      throws Exception {
    List<Throwable> failures = new java.util.ArrayList<>();
    int completed = 0;
    for (Future<?> future : futures) {
      try {
        future.get(60, TimeUnit.SECONDS);
        completed++;
      } catch (Exception e) {
        failures.add(rootCause(e));
      }
    }
    assertTrue(
        failures.isEmpty(),
        () ->
            "Expected all "
                + expected
                + " concurrent "
                + what
                + " to complete without pool exhaustion, but got failures (root cause of each — a"
                + " HikariCP SQLTransientConnectionException means the pool was held across the"
                + " crypto call): "
                + failures.stream().map(Object::toString).toList());
    assertEquals(expected, completed);
  }

  /** Unwraps to the innermost cause, so the real HikariCP/SQL failure is visible. */
  private static Throwable rootCause(Throwable t) {
    Throwable cause = t;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }
}
