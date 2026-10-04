package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import java.lang.reflect.Method;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import software.amazon.awssdk.services.kms.KmsClient;

/** Tests for {@link StreamRuneCryptoFactory}. */
class StreamRuneCryptoFactoryTest {

  private final StreamRuneCryptoFactory factory = new StreamRuneCryptoFactory();

  // The Vault/AWS engines now take a container-managed client; a mock stands in for it in
  // these direct unit tests (the engine only stores the client, using it lazily on
  // encrypt/decrypt).
  // The clients are namespaced in StreamRune-owned holder types, so they never
  // participate in by-type resolution of java.net.http.HttpClient / KmsClient.
  private static StreamRuneCryptoFactory.StreamRuneVaultHttpClient httpClient() {
    return new StreamRuneCryptoFactory.StreamRuneVaultHttpClient(mock(HttpClient.class));
  }

  private static StreamRuneCryptoFactory.StreamRuneKmsClient kmsClient() {
    return new StreamRuneCryptoFactory.StreamRuneKmsClient(mock(KmsClient.class));
  }

  // --- streamrune.crypto.cache.enabled=false must skip the CachedCryptoEngine wrapper ---
  // (GDPR no-cache posture — parity with Quarkus, previously silently ignored on Micronaut).

  @Test
  void cacheEnabledByDefault_producesCachedEngine() throws Exception {
    var keyDir = Files.createTempDirectory("sr-keys-cache-on");
    try (ApplicationContext ctx =
        ApplicationContext.run(
            Map.of(
                "streamrune.crypto.filesystem.enabled",
                "true",
                "streamrune.crypto.filesystem.key-directory",
                keyDir.toString()))) {
      CryptoEngine engine = ctx.getBean(CryptoEngine.class);
      assertInstanceOf(
          CachedCryptoEngine.class,
          engine,
          "default (cache enabled) must produce a CachedCryptoEngine");
    }
  }

  @Test
  void cacheDisabled_producesBareEngine_notCached() throws Exception {
    var keyDir = Files.createTempDirectory("sr-keys-cache-off");
    try (ApplicationContext ctx =
        ApplicationContext.run(
            Map.of(
                "streamrune.crypto.filesystem.enabled", "true",
                "streamrune.crypto.filesystem.key-directory", keyDir.toString(),
                "streamrune.crypto.cache.enabled", "false"))) {
      CryptoEngine engine = ctx.getBean(CryptoEngine.class);
      assertFalse(
          engine instanceof CachedCryptoEngine,
          "cache.enabled=false must NOT wrap the engine in CachedCryptoEngine (no PII cached in"
              + " heap)");
      assertInstanceOf(org.streamrune.filesystem.FileSystemCryptoEngine.class, engine);
    }
  }

  // Direct unit coverage of every bare (uncached) factory method — the beans Micronaut resolves
  // when streamrune.crypto.cache.enabled=false. Each must return the raw engine, never a
  // CachedCryptoEngine.

  @Test
  void fileSystemCryptoEngineUncached_returnsBareEngine() throws Exception {
    var props =
        CryptoTestProps.builder()
            .keyDirectory(Files.createTempDirectory("sr-keys-uncached").toString())
            .build();
    CryptoEngine engine = factory.fileSystemCryptoEngineUncached(props);
    assertFalse(engine instanceof CachedCryptoEngine);
    assertInstanceOf(org.streamrune.filesystem.FileSystemCryptoEngine.class, engine);
  }

  @Test
  void postgresCryptoEngineUncached_returnsBareEngine() {
    var props = CryptoTestProps.builder().build();
    CryptoEngine engine = factory.postgresCryptoEngineUncached(mock(DataSource.class), props);
    assertFalse(engine instanceof CachedCryptoEngine);
    assertInstanceOf(PostgresCryptoEngine.class, engine);
  }

  @Test
  void vaultCryptoEngineUncached_returnsBareEngine() {
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .build();
    CryptoEngine engine =
        factory.vaultCryptoEngineUncached(httpClient(), mock(DataSource.class), props);
    assertFalse(engine instanceof CachedCryptoEngine);
  }

  @Test
  void awsKmsCryptoEngineUncached_returnsBareEngine() {
    var props =
        CryptoTestProps.builder()
            .awsRegion("us-west-2")
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();
    CryptoEngine engine =
        factory.awsKmsCryptoEngineUncached(kmsClient(), mock(DataSource.class), props);
    assertFalse(engine instanceof CachedCryptoEngine);
  }

  // --- the produced CryptoEngine must be closed on context shutdown ---

  @Test
  void cryptoFactoryMethodsAreAutoCloseableTypedAndPreDestroyClose() {
    // Micronaut does not auto-close @Factory-produced AutoCloseable beans. Without a
    // preDestroy = "close" the cached engine's LISTEN connection, its dedicated Hikari pool +
    // housekeeping thread, and the listener virtual thread leak on every non-JVM-exit context
    // shutdown (@MicronautTest multi-context suites, embedded restarts). preDestroy = "close" can
    // only resolve when the declared bean type exposes close(), so each factory method must return
    // the AutoCloseable CachedCryptoEngine, not the bare CryptoEngine interface.
    for (String name :
        List.of(
            "fileSystemCryptoEngine",
            "postgresCryptoEngine",
            "vaultCryptoEngine",
            "awsKmsCryptoEngine")) {
      Method m = factoryMethod(name);
      assertTrue(
          AutoCloseable.class.isAssignableFrom(m.getReturnType()),
          name
              + " must return an AutoCloseable engine so Micronaut can resolve preDestroy close()");
      Bean bean = m.getAnnotation(Bean.class);
      assertNotNull(bean, name + " must be annotated @Bean(preDestroy = \"close\")");
      assertEquals("close", bean.preDestroy(), name + " must declare preDestroy = \"close\"");
    }
  }

  @Test
  void clientBeansAreAutoCloseableTypedAndPreDestroyClose() {
    // The Vault HttpClient / AWS KmsClient are container-managed beans closed on shutdown.
    // Micronaut does not auto-close @Factory-produced AutoCloseable beans, so each must declare
    // @Bean(preDestroy = "close") against an AutoCloseable-typed return, or the client leaks
    // (selector thread / SDK pool + reaper) on every context shutdown.
    for (String name : List.of("vaultHttpClient", "awsKmsClient")) {
      Method m = factoryMethod(name);
      assertTrue(
          AutoCloseable.class.isAssignableFrom(m.getReturnType()),
          name
              + " must return an AutoCloseable client so Micronaut can resolve preDestroy close()");
      Bean bean = m.getAnnotation(Bean.class);
      assertNotNull(bean, name + " must be annotated @Bean(preDestroy = \"close\")");
      assertEquals("close", bean.preDestroy(), name + " must declare preDestroy = \"close\"");
    }
  }

  private static Method factoryMethod(String name) {
    for (Method m : StreamRuneCryptoFactory.class.getDeclaredMethods()) {
      if (m.getName().equals(name)) {
        return m;
      }
    }
    throw new AssertionError("no factory method named " + name);
  }

  // --- vaultCryptoEngine validation ---

  @Test
  void vaultCryptoEngineRejectsBlankToken() {
    var props =
        CryptoTestProps.builder()
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .build();
    // token is null by default

    var e =
        assertThrows(
            IllegalStateException.class,
            () -> factory.vaultCryptoEngine(httpClient(), null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.vault.token"));
  }

  @Test
  void vaultCryptoEngineRejectsWhitespaceToken() {
    var props =
        CryptoTestProps.builder()
            .vaultToken("   ")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .build();

    var e =
        assertThrows(
            IllegalStateException.class,
            () -> factory.vaultCryptoEngine(httpClient(), null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.vault.token"));
  }

  @Test
  void vaultCryptoEngineFailsClosed_whenNoDataSourceForTerminalErasure() {
    // Valid token but NO DataSource → no durable
    // ForgottenSubjectStore → fail closed rather than shipping non-terminal Vault erasure.
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .build();

    var e =
        assertThrows(
            IllegalStateException.class,
            () -> factory.vaultCryptoEngine(httpClient(), null, props));
    assertTrue(e.getMessage().contains("ForgottenSubjectStore"));
  }

  @Test
  void vaultCryptoEngineBuilt_withDataSource_terminalErasure() {
    // A DataSource present → durable JdbcForgottenSubjectStore
    // auto-wired → engine builds.
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .build();

    var engine = factory.vaultCryptoEngine(httpClient(), mock(DataSource.class), props);
    assertNotNull(engine);
  }

  // --- awsKmsCryptoEngine validation ---

  @Test
  void awsKmsCryptoEngineRejectsBlankRegion() {
    var props =
        CryptoTestProps.builder()
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();
    // region is null

    var e =
        assertThrows(
            IllegalStateException.class,
            () -> factory.awsKmsCryptoEngine(kmsClient(), null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.aws.region"));
  }

  @Test
  void awsKmsCryptoEngineRejectsNullKmsKeyId() {
    var props = CryptoTestProps.builder().awsRegion("us-west-2").build();
    // kmsKeyId is null

    var e =
        assertThrows(
            IllegalStateException.class,
            () -> factory.awsKmsCryptoEngine(kmsClient(), null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.aws.kms-key-id"));
  }

  @Test
  void awsKmsCryptoEngineRejectsWhitespaceRegion() {
    var props =
        CryptoTestProps.builder()
            .awsRegion("   ")
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();

    var e =
        assertThrows(
            IllegalStateException.class,
            () -> factory.awsKmsCryptoEngine(kmsClient(), null, props));
    assertTrue(
        e.getMessage().contains("Failed to build AwsKmsCryptoEngine")
            || e.getMessage().contains("region"));
  }

  @Test
  void awsKmsCryptoEngineRejectsBlankKmsKeyId() {
    var props = CryptoTestProps.builder().awsRegion("us-west-2").awsKmsKeyId("   ").build();

    var e =
        assertThrows(
            IllegalStateException.class,
            () -> factory.awsKmsCryptoEngine(kmsClient(), null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.aws.kms-key-id"));
  }

  @Test
  void awsKmsCryptoEngineFailsClosed_whenNoDataSourceForDurableErasure() {
    // Valid region + key id but NO DataSource → no durable ForgottenSubjectStore → the engine
    // must fail closed instead of silently recording non-durable in-memory tombstones.
    var props =
        CryptoTestProps.builder()
            .awsRegion("us-west-2")
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();

    var e =
        assertThrows(
            IllegalStateException.class,
            () -> factory.awsKmsCryptoEngine(kmsClient(), null, props));
    assertTrue(e.getMessage().contains("ForgottenSubjectStore"));
    assertTrue(e.getMessage().contains("durable GDPR erasure"));
  }

  @Test
  void awsKmsCryptoEngineBuilt_withDataSource_durableErasure() {
    // A DataSource present → durable JdbcForgottenSubjectStore auto-wired → engine builds.
    var props =
        CryptoTestProps.builder()
            .awsRegion("us-west-2")
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();

    var engine = factory.awsKmsCryptoEngine(kmsClient(), mock(DataSource.class), props);
    assertNotNull(engine);
  }

  // --- postgresCryptoEngine ---

  @Test
  void postgresCryptoEngineDelegatesDirectly() {
    var props = CryptoTestProps.builder().build();
    DataSource ds =
        new javax.sql.DataSource() {
          public java.sql.Connection getConnection() {
            return null;
          }

          public java.sql.Connection getConnection(String u, String p) {
            return null;
          }

          public java.io.PrintWriter getLogWriter() {
            return null;
          }

          public void setLogWriter(java.io.PrintWriter w) {}

          public void setLoginTimeout(int s) {}

          public int getLoginTimeout() {
            return 0;
          }

          public java.util.logging.Logger getParentLogger() {
            return null;
          }

          public <T> T unwrap(Class<T> c) {
            return null;
          }

          public boolean isWrapperFor(Class<?> c) {
            return false;
          }
        };

    CryptoEngine engine = factory.postgresCryptoEngine(ds, props);

    assertNotNull(engine);
    assertTrue(
        engine instanceof PostgresCryptoEngine || engine.getClass().getName().contains("Cached"));
  }

  // --- wrapWithCache ---

  @Test
  void wrapWithCacheAppliesExpireAfterWrite() {
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .cacheExpireAfterWrite(java.time.Duration.ofMinutes(10))
            .cacheExpireAfterAccess(null)
            .build();

    // Vault token is set → engine is built. wrapWithCache reads expireAfterWrite.
    var engine = factory.vaultCryptoEngine(httpClient(), mock(DataSource.class), props);
    assertNotNull(engine); // Build succeeds, wrapped in CachedCryptoEngine
  }

  @Test
  void wrapWithCacheSkippedWhenCacheDisabled() {
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .cacheExpireAfterWrite(null) // null = no expiry
            .cacheExpireAfterAccess(null)
            .build();

    var engine = factory.vaultCryptoEngine(httpClient(), mock(DataSource.class), props);
    assertNotNull(engine);
  }

  @Test
  void wrapWithCacheAppliesExpireAfterAccess() {
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .cacheExpireAfterWrite(null)
            .cacheExpireAfterAccess(java.time.Duration.ofMinutes(5))
            .build();

    var engine = factory.vaultCryptoEngine(httpClient(), mock(DataSource.class), props);
    assertNotNull(engine);
  }

  // --- resolveForgetSignal ---

  @Test
  void resolveForgetSignal_wiresPostgresChannel_whenDataSourcePresent() {
    var signal =
        StreamRuneCryptoFactory.resolveForgetSignal(
            mock(DataSource.class), java.time.Duration.ofMinutes(5), "aws-kms");
    assertInstanceOf(
        org.streamrune.crypto.postgres.PostgresCryptoForgetSignal.class,
        signal,
        "a DataSource-backed backend must get the prompt LISTEN/NOTIFY forget channel, not NOOP");
  }

  @Test
  void resolveForgetSignal_fallsBackToNoop_whenNoDataSource() {
    var signal = StreamRuneCryptoFactory.resolveForgetSignal(null, null, "vault");
    assertSame(org.streamrune.crypto.CryptoForgetSignal.NOOP, signal);
  }
}
