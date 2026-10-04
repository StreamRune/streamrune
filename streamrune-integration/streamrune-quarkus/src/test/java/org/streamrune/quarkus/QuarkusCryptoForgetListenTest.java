package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoForgetSignal;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Cross-replica crypto-forget eviction on Quarkus, against the {@code DataSource} a Quarkus
 * application actually injects: an Agroal pool, seen through the client proxy of the
 * application-scoped {@link AgroalDataSource} bean. Agroal exposes no {@code getJdbcUrl()} or
 * {@code getUrl()}, so the LISTEN half of {@link PostgresCryptoForgetSignal} has to be opened from
 * the pool's own connection settings — the URL, credentials and JDBC properties of its connection
 * factory — on a dedicated pool of one connection.
 *
 * <p>Each Agroal pool here carries a unique {@code ApplicationName} JDBC property. The LISTEN
 * connection is copied from the application's connection settings, so it carries the same name,
 * which is how a test finds that replica's LISTEN backend in {@code pg_stat_activity}.
 */
@Testcontainers
class QuarkusCryptoForgetListenTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static final String CHANNEL = "streamrune_crypto_forget";
  private static final String POSTGRES = "streamrune.crypto.postgres.enabled";
  private static final Duration WAIT = Duration.ofSeconds(15);

  private static PGSimpleDataSource plain;

  /** The Agroal pool the infrastructure producer hands to the Arc container under test. */
  static volatile AgroalDataSource applicationPool;

  private final List<AutoCloseable> toClose = new ArrayList<>();

  @BeforeAll
  static void init() throws Exception {
    plain = new PGSimpleDataSource();
    plain.setUrl(PG.getJdbcUrl());
    plain.setUser(PG.getUsername());
    plain.setPassword(PG.getPassword());
    String ddl;
    try (var in =
        QuarkusCryptoForgetListenTest.class.getResourceAsStream(
            "/db/crypto-migration/V001__crypto_baseline.sql")) {
      assertNotNull(in, "the shipped crypto migration must be on the classpath");
      ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (var conn = plain.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(ddl);
    }
  }

  @AfterEach
  void closeResources() throws Exception {
    for (AutoCloseable c : toClose.reversed()) {
      c.close();
    }
  }

  // ---- the engine the Quarkus producer builds ------------------------------------------------

  /**
   * Two replicas: B is the engine the real Arc container builds from the PostgreSQL crypto producer
   * over an Agroal datasource, A any other replica on the same database. A forgets the subject; B
   * must stop serving the subject's cached plaintext promptly, not after the cache write-TTL.
   */
  @Test
  void aForgetOnAnotherReplicaStopsTheQuarkusEngineServingCachedPlaintext() throws Exception {
    String replicaB = "replica-b-" + UUID.randomUUID();
    applicationPool = agroalPool(replicaB, true, 4);
    toClose.add(applicationPool);

    try (var arc = RealArcTestContainer.boot(cryptoDeployment(), Map.of(POSTGRES, "true"))) {
      CryptoEngine engineB = arc.container().instance(CryptoEngine.class).get();
      SubjectId subject = SubjectId.of("quarkus-forget-" + UUID.randomUUID());
      byte[] plaintext = "alice@example.com".getBytes(StandardCharsets.UTF_8);
      byte[] ciphertext = engineB.encrypt(subject, plaintext);
      assertArrayEquals(plaintext, engineB.decrypt(subject, ciphertext), "B caches the plaintext");

      awaitListening(replicaB);

      try (var engineA =
          CachedCryptoEngine.builder()
              .delegate(PostgresCryptoEngine.builder().dataSource(plain).build())
              .forgetSignal(new PostgresCryptoForgetSignal(plain))
              .build()) {
        engineA.deleteKey(subject);
        awaitNoLongerServed(engineB, subject, ciphertext);
      }

      // What the producer's disposer runs on shutdown. Called directly: this harness loads Arc's
      // generated classes in a separate classloader, so the package-private disposer cannot be
      // reached through the container here (a real Quarkus deployment shares one classloader).
      StreamRuneCryptoProducers.closeEngine(engineB);
      awaitNoListenBackend(replicaB);
    }
  }

  // ---- the channel the other backends get ----------------------------------------------------

  /** Vault, AWS KMS and the file-system backend wire the same channel from the same DataSource. */
  @Test
  void theOtherBackendsChannelListensOnAnAgroalDataSource() throws Exception {
    String app = "other-backends-" + UUID.randomUUID();
    AgroalDataSource pool = agroalPool(app, true, 4);
    toClose.add(pool);
    BlockingQueue<String> received = new LinkedBlockingQueue<>();

    CryptoForgetSignal signal =
        StreamRuneCryptoProducers.resolveForgetSignal(pool, Optional.empty(), "vault");
    toClose.add(signal);
    signal.subscribe(received::add);

    awaitListening(app);
    awaitDelivered(received, "token-" + UUID.randomUUID());
  }

  /**
   * An application datasource without auto-commit must not leave LISTEN inside a transaction that
   * never commits: PostgreSQL registers a LISTEN only on commit, and delivers notifications only
   * between transactions.
   */
  @Test
  void listensWhenTheApplicationDataSourceDoesNotAutoCommit() throws Exception {
    String app = "no-autocommit-" + UUID.randomUUID();
    AgroalDataSource pool = agroalPool(app, false, 4);
    toClose.add(pool);
    BlockingQueue<String> received = new LinkedBlockingQueue<>();

    CryptoForgetSignal signal =
        StreamRuneCryptoProducers.resolveForgetSignal(pool, Optional.empty(), "aws-kms");
    toClose.add(signal);
    signal.subscribe(received::add);

    awaitListening(app);
    awaitDelivered(received, "token-" + UUID.randomUUID());
  }

  /** The LISTEN connection is the channel's own; it never holds one of the application's. */
  @Test
  void theListenConnectionIsNotTakenFromTheApplicationPool() throws Exception {
    String app = "single-slot-" + UUID.randomUUID();
    AgroalDataSource pool = agroalPool(app, true, 1);
    toClose.add(pool);

    CryptoForgetSignal signal =
        StreamRuneCryptoProducers.resolveForgetSignal(pool, Optional.empty(), "filesystem");
    toClose.add(signal);
    signal.subscribe(token -> {});
    awaitListening(app);

    try (Connection conn = pool.getConnection()) {
      assertTrue(conn.isValid(2), "the application's only pooled connection is still free");
    }
  }

  /**
   * The LISTEN backend is terminated (a failover, an idle-session kill). The channel must not keep
   * handing the dead connection back to itself: it reconnects on a fresh one and delivers again.
   */
  @Test
  void reconnectsAfterTheListenBackendIsTerminated() throws Exception {
    String app = "terminated-" + UUID.randomUUID();
    AgroalDataSource pool = agroalPool(app, true, 4);
    toClose.add(pool);
    BlockingQueue<String> received = new LinkedBlockingQueue<>();

    CryptoForgetSignal signal =
        StreamRuneCryptoProducers.resolveForgetSignal(pool, Optional.empty(), "vault");
    toClose.add(signal);
    signal.subscribe(received::add);
    awaitListening(app);

    try (var conn = plain.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                    + " WHERE application_name = ? AND query = ?")) {
      ps.setString(1, app);
      ps.setString(2, "LISTEN " + CHANNEL);
      ps.execute();
    }

    awaitDelivered(received, "after-terminate-" + UUID.randomUUID());
  }

  // ---- helpers -------------------------------------------------------------------------------

  /** A pool shaped like the one quarkus-agroal builds from {@code quarkus.datasource.*}. */
  private static AgroalDataSource agroalPool(String applicationName, boolean autoCommit, int size)
      throws SQLException {
    var config =
        new AgroalDataSourceConfigurationSupplier()
            .connectionPoolConfiguration(
                pool ->
                    pool.maxSize(size)
                        .acquisitionTimeout(Duration.ofSeconds(2))
                        .connectionFactoryConfiguration(
                            factory ->
                                factory
                                    .jdbcUrl(PG.getJdbcUrl())
                                    .autoCommit(autoCommit)
                                    .principal(new NamePrincipal(PG.getUsername()))
                                    .credential(new SimplePassword(PG.getPassword()))
                                    .jdbcProperty("ApplicationName", applicationName)));
    return AgroalDataSource.from(config.get());
  }

  private static int listenBackends(String applicationName) throws SQLException {
    try (var conn = plain.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = ? AND query = ?")) {
      ps.setString(1, applicationName);
      ps.setString(2, "LISTEN " + CHANNEL);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static void awaitListening(String applicationName) throws Exception {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (System.nanoTime() < deadline) {
      if (listenBackends(applicationName) > 0) {
        return;
      }
      Thread.sleep(100);
    }
    fail(
        "no backend of '"
            + applicationName
            + "' ever issued LISTEN "
            + CHANNEL
            + ": that replica would keep serving a forgotten subject's cached plaintext until the"
            + " cache write-TTL");
  }

  private static void awaitNoListenBackend(String applicationName) throws Exception {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (listenBackends(applicationName) > 0) {
      if (System.nanoTime() > deadline) {
        fail("closing the engine left the LISTEN backend of '" + applicationName + "' open");
      }
      Thread.sleep(100);
    }
  }

  /**
   * Publishes {@code token} from another replica until it arrives. Re-publishing covers the window
   * in which a reconnecting listener has not re-issued LISTEN yet.
   */
  private static void awaitDelivered(BlockingQueue<String> received, String token)
      throws Exception {
    var publisher = new PostgresCryptoForgetSignal(plain);
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (System.nanoTime() < deadline) {
      publisher.publish(token);
      String got = received.poll(250, TimeUnit.MILLISECONDS);
      while (got != null) {
        if (token.equals(got)) {
          return;
        }
        got = received.poll();
      }
    }
    fail("the forget token published by another replica never reached this replica's listener");
  }

  private static void awaitNoLongerServed(CryptoEngine engine, SubjectId subject, byte[] ciphertext)
      throws InterruptedException {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        engine.decrypt(subject, ciphertext);
        Thread.sleep(100);
      } catch (KeyNotFoundException _) {
        return;
      }
    }
    fail("the Quarkus replica kept serving the forgotten subject's cached plaintext");
  }

  private static List<Class<?>> cryptoDeployment() {
    var classes = new ArrayList<Class<?>>();
    classes.add(StreamRuneCryptoProducers.class);
    classes.addAll(List.of(StreamRuneCryptoProducers.class.getDeclaredClasses()));
    classes.add(AgroalInfrastructure.class);
    return classes;
  }

  /**
   * What a Quarkus application with quarkus-agroal supplies: the datasource as an
   * application-scoped {@link AgroalDataSource} bean (so a client proxy is injected wherever a
   * {@code DataSource} is asked for), and the bound crypto properties.
   */
  @ApplicationScoped
  public static class AgroalInfrastructure {

    @Produces
    @ApplicationScoped
    public AgroalDataSource dataSource() {
      return applicationPool;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusCryptoProperties properties() {
      SmallRyeConfig config =
          new SmallRyeConfigBuilder()
              .withMapping(StreamRuneQuarkusCryptoProperties.class)
              .withConverter(
                  Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
              .withDefaultValues(Map.of(POSTGRES, "true"))
              .build();
      return config.getConfigMapping(StreamRuneQuarkusCryptoProperties.class);
    }
  }
}
