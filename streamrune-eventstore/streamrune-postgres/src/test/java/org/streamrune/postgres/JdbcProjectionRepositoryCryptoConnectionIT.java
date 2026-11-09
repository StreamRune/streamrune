package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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
import org.streamrune.core.LockMode;
import org.streamrune.core.PageRequest;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The projection-repository counterpart of {@link PostgresCryptoConnectionIT} and {@link
 * PostgresSagaStoreCryptoConnectionIT}: the outer read and write paths of {@link
 * JdbcProjectionRepository} must (de)serialise an {@code @Encrypted} read model — and so call the
 * crypto engine — while they hold no pooled connection.
 *
 * <p>The engine here is what the Spring, Quarkus and Micronaut integrations build: a {@link
 * CachedCryptoEngine} over a {@link PostgresCryptoEngine} on the <em>same</em> pool as the
 * repository. Encrypt always reaches the delegate, and so does every decrypt of a ciphertext the
 * cache has not seen; the delegate borrows a connection from the pool each time. A path that held
 * its own connection while it called the engine needed two at once, so as many concurrent calls as
 * the pool has connections would each hold one and wait for a second until {@code
 * connectionTimeout}. Each test runs {@code 2 × POOL_SIZE} such calls released together on a pool
 * of {@code POOL_SIZE} with a short {@code connectionTimeout}, and every call must succeed.
 *
 * <p>The batch path ({@code executeAtomically}'s transaction-scoped repository) is out of scope by
 * design: it cannot release the batch connection, and the production guide budgets the pool for it.
 */
@Testcontainers
class JdbcProjectionRepositoryCryptoConnectionIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("projection_crypto_conn_test");

  private static final int POOL_SIZE = 3;
  private static final int CONCURRENT_CALLS = POOL_SIZE * 2;
  private static final long CONNECTION_TIMEOUT_MILLIS = 1_500;
  private static final ProjectionName PROJECTION = ProjectionName.of("encrypted_customer");

  static PGSimpleDataSource schemaDataSource;

  HikariDataSource pool;

  /** A read model with one {@code @Encrypted} component, keyed off {@code id}. */
  record CustomerView(String id, @Encrypted(subjectId = "id") String email) {}

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
          public Collection<Class<?>> registeredTypes() {
            return List.of();
          }
        };
    // The event-store baseline plus the crypto tables the PostgresCryptoEngine needs, applied on
    // the unpooled schema DataSource so setup never touches the small pool under test.
    new PostgresEventStoreFactory(schemaDataSource, registry)
        .cryptoEngine(PostgresCryptoEngine.builder().dataSource(schemaDataSource).build())
        .initializeSchema();
  }

  @BeforeEach
  void setUp() throws Exception {
    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(POOL_SIZE);
    config.setMinimumIdle(POOL_SIZE);
    config.setConnectionTimeout(CONNECTION_TIMEOUT_MILLIS);
    config.setPoolName("projection-crypto-conn-test-" + System.nanoTime());
    pool = new HikariDataSource(config);

    try (var conn = schemaDataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + PROJECTION.value() + "_view");
    }
  }

  @AfterEach
  void tearDown() {
    if (pool != null) {
      pool.close();
    }
  }

  /** The repository under test: the integrations' engine on the same small pool. */
  private JdbcProjectionRepository repositoryOnThePool() {
    CryptoEngine engine =
        CachedCryptoEngine.builder()
            .delegate(PostgresCryptoEngine.builder().dataSource(pool).build())
            .build();
    return new JdbcProjectionRepository(
        pool, JdbcProjectionRepository.createObjectMapper(engine, StreamRuneMetrics.NOOP));
  }

  /**
   * Writes {@code CONCURRENT_CALLS} encrypted rows through a repository on the unpooled schema
   * DataSource, so the repository under test starts with a cold decrypt cache.
   */
  private static void seedRows() {
    var seeder =
        new JdbcProjectionRepository(
            schemaDataSource,
            JdbcProjectionRepository.createObjectMapper(
                PostgresCryptoEngine.builder().dataSource(schemaDataSource).build(),
                StreamRuneMetrics.NOOP));
    for (int i = 0; i < CONCURRENT_CALLS; i++) {
      seeder.save(PROJECTION, id(i), view(i));
    }
  }

  private static String id(int i) {
    return "customer-" + i;
  }

  private static CustomerView view(int i) {
    return new CustomerView(id(i), "customer" + i + "@example.com");
  }

  @Test
  void concurrentEncryptedSaves_onAPoolOfN_allSucceed() throws Exception {
    var repository = repositoryOnThePool();

    runTogether(
        i ->
            () -> {
              repository.save(PROJECTION, id(i), view(i));
              return null;
            });

    readBack(repository);
  }

  @Test
  void concurrentEncryptedVersionedSaves_onAPoolOfN_allSucceed() throws Exception {
    seedRows();
    var repository = repositoryOnThePool();

    runTogether(
        i ->
            () -> {
              repository.save(
                  PROJECTION, id(i), new CustomerView(id(i), "changed" + i + "@example.com"), 0L);
              return null;
            });

    for (int i = 0; i < CONCURRENT_CALLS; i++) {
      var stored = repository.findById(PROJECTION, id(i), CustomerView.class, LockMode.OPTIMISTIC);
      assertThat(stored).isPresent();
      assertThat(stored.get().data().email()).isEqualTo("changed" + i + "@example.com");
      assertThat(stored.get().version()).isEqualTo(1L);
    }
  }

  @Test
  void concurrentColdFindById_onAPoolOfN_allSucceedAndDecrypt() throws Exception {
    seedRows();
    var repository = repositoryOnThePool();

    List<Object> results =
        runTogether(i -> () -> repository.findById(PROJECTION, id(i), CustomerView.class));

    for (int i = 0; i < CONCURRENT_CALLS; i++) {
      assertThat(results.get(i)).isEqualTo(java.util.Optional.of(view(i)));
    }
  }

  @Test
  void concurrentColdLockingFindById_onAPoolOfN_allSucceedAndDecrypt() throws Exception {
    seedRows();
    var repository = repositoryOnThePool();

    List<Object> results =
        runTogether(
            i ->
                () ->
                    repository
                        .findById(PROJECTION, id(i), CustomerView.class, LockMode.OPTIMISTIC)
                        .map(v -> v.data()));

    for (int i = 0; i < CONCURRENT_CALLS; i++) {
      assertThat(results.get(i)).isEqualTo(java.util.Optional.of(view(i)));
    }
  }

  @Test
  void concurrentColdFindAll_onAPoolOfN_allSucceedAndDecrypt() throws Exception {
    seedRows();
    // One repository (one cold cache) per caller: each findAll decrypts every row itself.
    List<JdbcProjectionRepository> repositories = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_CALLS; i++) {
      repositories.add(repositoryOnThePool());
    }

    List<Object> results =
        runTogether(i -> () -> repositories.get(i).findAll(PROJECTION, CustomerView.class));

    for (Object result : results) {
      @SuppressWarnings("unchecked")
      var rows = (List<CustomerView>) result;
      assertThat(rows).hasSize(CONCURRENT_CALLS).contains(view(0), view(1));
    }
  }

  @Test
  void concurrentColdPagedFindAll_onAPoolOfN_allSucceedAndDecrypt() throws Exception {
    seedRows();
    List<JdbcProjectionRepository> repositories = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_CALLS; i++) {
      repositories.add(repositoryOnThePool());
    }

    List<Object> results =
        runTogether(
            i ->
                () ->
                    repositories
                        .get(i)
                        .findAll(PROJECTION, CustomerView.class, PageRequest.of(0, 2)));

    for (Object result : results) {
      @SuppressWarnings("unchecked")
      var page = (org.streamrune.core.Page<CustomerView>) result;
      assertThat(page.totalElements()).isEqualTo(CONCURRENT_CALLS);
      assertThat(page.content()).containsExactly(view(0), view(1));
    }
  }

  private void readBack(JdbcProjectionRepository repository) {
    for (int i = 0; i < CONCURRENT_CALLS; i++) {
      assertThat(repository.findById(PROJECTION, id(i), CustomerView.class)).contains(view(i));
    }
  }

  /**
   * Releases {@code CONCURRENT_CALLS} calls at once and returns their results in order; fails with
   * the root cause of every call that failed (a HikariCP {@code SQLTransientConnectionException}
   * means a call held a connection while the engine waited for another).
   */
  private static List<Object> runTogether(java.util.function.IntFunction<Callable<?>> call)
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CALLS);
    try {
      var start = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < CONCURRENT_CALLS; i++) {
        Callable<?> task = call.apply(i);
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  return task.call();
                }));
      }
      start.countDown();
      List<Object> results = new ArrayList<>();
      List<String> failures = new ArrayList<>();
      for (Future<?> future : futures) {
        try {
          results.add(future.get(60, TimeUnit.SECONDS));
        } catch (java.util.concurrent.ExecutionException e) {
          results.add(null);
          failures.add(rootCause(e).toString());
        }
      }
      assertThat(failures)
          .as(
              "calls that failed (root cause) out of %d on a pool of %d",
              CONCURRENT_CALLS, POOL_SIZE)
          .isEmpty();
      return results;
    } finally {
      executor.shutdownNow();
    }
  }

  private static Throwable rootCause(Throwable t) {
    Throwable cause = t;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }
}
