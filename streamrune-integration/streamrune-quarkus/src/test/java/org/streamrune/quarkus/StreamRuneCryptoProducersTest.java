package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.inject.Disposes;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import software.amazon.awssdk.services.kms.KmsClient;

/**
 * Tests for {@link StreamRuneCryptoProducers}. Properties are bound through a real SmallRyeConfig
 * ({@link TestProperties#crypto}), so these tests exercise the documented {@code
 * streamrune.crypto.*} keys end to end.
 */
class StreamRuneCryptoProducersTest {

  private final StreamRuneCryptoProducers producers = new StreamRuneCryptoProducers();

  private static StreamRuneQuarkusCryptoProperties props(Map<String, String> overrides) {
    return TestProperties.crypto(overrides);
  }

  private static Map<String, String> vaultEnabled(Map<String, String> extra) {
    var map = new HashMap<String, String>();
    map.put("streamrune.crypto.vault.enabled", "true");
    map.putAll(extra);
    return map;
  }

  private static Map<String, String> awsEnabled(Map<String, String> extra) {
    var map = new HashMap<String, String>();
    map.put("streamrune.crypto.aws.enabled", "true");
    map.putAll(extra);
    return map;
  }

  // --- build-property gates must match the documented config keys ---

  /**
   * The gates live on the nested producer CLASSES, not on the producer methods. Arc consults
   * {@code @VetoedProducer} — what a false {@code @IfBuildProperty} becomes — only where it
   * collects producers; the loop that collects {@code @Disposes} methods never looks at it. A
   * method-level gate therefore deletes a producer and keeps its disposer, and Arc fails
   * augmentation. A class-level gate takes both, because a vetoed class stops being a bean class
   * before any of its methods are examined. Pinned end-to-end against a real Arc container in
   * {@code QuarkusConditionalDisposerTest}; pinned here so the intent is stated where the
   * annotation is read.
   */
  @Test
  void buildPropertyGatesLiveOnTheProducerClassesAndMatchDocumentedKeys() {
    assertClassGate(
        StreamRuneCryptoProducers.FileSystemCryptoProducer.class,
        "streamrune.crypto.filesystem.enabled");
    assertClassGate(
        StreamRuneCryptoProducers.PostgresCryptoProducer.class,
        "streamrune.crypto.postgres.enabled");
    assertClassGate(
        StreamRuneCryptoProducers.VaultCryptoProducer.class, "streamrune.crypto.vault.enabled");
    assertClassGate(
        StreamRuneCryptoProducers.AwsKmsCryptoProducer.class, "streamrune.crypto.aws.enabled");
  }

  private static void assertClassGate(Class<?> producerClass, String expectedKey) {
    var gate = producerClass.getAnnotation(IfBuildProperty.class);
    assertNotNull(gate, producerClass.getSimpleName() + " must be gated by @IfBuildProperty");
    assertEquals(expectedKey, gate.name());
    assertEquals("true", gate.stringValue());
    assertFalse(gate.enableIfMissing());
  }

  /**
   * The other half of the same invariant: no producer method may carry its own gate, because that
   * is exactly the shape that can outlive — or be outlived by — its disposer.
   */
  @Test
  void noProducerMethodCarriesItsOwnBuildPropertyGate() {
    for (Class<?> producerClass : StreamRuneCryptoProducers.class.getDeclaredClasses()) {
      for (var method : producerClass.getDeclaredMethods()) {
        if (method.isAnnotationPresent(jakarta.enterprise.inject.Produces.class)) {
          assertNull(
              method.getAnnotation(IfBuildProperty.class),
              producerClass.getSimpleName()
                  + "#"
                  + method.getName()
                  + " must be gated by its declaring class, not by a method-level"
                  + " @IfBuildProperty that its disposer cannot follow");
        }
      }
    }
  }

  // --- fileSystemCryptoEngine ---

  @Test
  void fileSystemCryptoEngineBuilt_cachedByDefault() {
    var engine =
        producers.buildFileSystemCryptoEngine(
            mock(DataSource.class),
            props(
                Map.of(
                    "streamrune.crypto.filesystem.enabled", "true",
                    "streamrune.crypto.filesystem.key-directory", "/var/lib/streamrune/keys")));
    assertNotNull(engine);
    assertInstanceOf(CachedCryptoEngine.class, engine);
  }

  @Test
  void fileSystemCryptoEngineBuilt_uncachedWhenCacheDisabled() {
    var engine =
        producers.buildFileSystemCryptoEngine(
            mock(DataSource.class),
            props(
                Map.of(
                    "streamrune.crypto.filesystem.enabled", "true",
                    "streamrune.crypto.cache.enabled", "false")));
    assertNotNull(engine);
    assertFalse(engine instanceof CachedCryptoEngine);
  }

  @Test
  void fileSystemCryptoEngineBuilt_withExpiryConfig() {
    var engine =
        producers.buildFileSystemCryptoEngine(
            mock(DataSource.class),
            props(
                Map.of(
                    "streamrune.crypto.filesystem.enabled", "true",
                    "streamrune.crypto.cache.expire-after-write", "PT30M",
                    "streamrune.crypto.cache.expire-after-access", "PT10M")));
    assertNotNull(engine);
    assertInstanceOf(CachedCryptoEngine.class, engine);
  }

  @Test
  void fileSystemCryptoEngineBuilt_blankExpiryDisablesExpiryInsteadOfFailing() {
    var engine =
        producers.buildFileSystemCryptoEngine(
            mock(DataSource.class),
            props(
                Map.of(
                    "streamrune.crypto.filesystem.enabled", "true",
                    "streamrune.crypto.cache.expire-after-write", "  ",
                    "streamrune.crypto.cache.expire-after-access", "  ")));
    assertNotNull(engine);
    assertInstanceOf(CachedCryptoEngine.class, engine);
  }

  @Test
  void fileSystemCryptoEngineThrows_whenRuntimeDisabledAfterBuildTimeEnabled() {
    // The bean only exists because the key was true at build time; flipping it to
    // false at runtime must fail fast, not produce a broken null engine.
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> producers.buildFileSystemCryptoEngine(mock(DataSource.class), props(Map.of())));
    assertTrue(ex.getMessage().contains("streamrune.crypto.filesystem.enabled"));
  }

  // --- postgresCryptoEngine ---

  @Test
  void postgresCryptoEngineBuilt() {
    var engine =
        producers.buildPostgresCryptoEngine(
            mock(DataSource.class), props(Map.of("streamrune.crypto.postgres.enabled", "true")));
    assertNotNull(engine);
    assertInstanceOf(CachedCryptoEngine.class, engine);
  }

  @Test
  void postgresCryptoEngineThrows_whenRuntimeDisabled() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> producers.buildPostgresCryptoEngine(mock(DataSource.class), props(Map.of())));
    assertTrue(ex.getMessage().contains("streamrune.crypto.postgres.enabled"));
  }

  // --- vaultCryptoEngine ---

  @Test
  void vaultCryptoEngineBuilt_withToken() {
    var engine =
        producers.buildVaultCryptoEngine(
            mock(DataSource.class),
            props(vaultEnabled(Map.of("streamrune.crypto.vault.token", "s.test"))));
    assertNotNull(engine);
    assertInstanceOf(CachedCryptoEngine.class, engine);
  }

  @Test
  void vaultCryptoEngineBuilt_uncachedWhenCacheDisabled() {
    var engine =
        producers.buildVaultCryptoEngine(
            mock(DataSource.class),
            props(
                vaultEnabled(
                    Map.of(
                        "streamrune.crypto.vault.token", "s.test",
                        "streamrune.crypto.cache.enabled", "false"))));
    assertNotNull(engine);
    assertFalse(engine instanceof CachedCryptoEngine);
  }

  @Test
  void vaultCryptoEngineFailsFast_whenTokenMissing() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildVaultCryptoEngine(
                    mock(DataSource.class), props(vaultEnabled(Map.of()))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.vault.token"));
  }

  @Test
  void vaultCryptoEngineFailsFast_whenTokenBlank() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildVaultCryptoEngine(
                    mock(DataSource.class),
                    props(vaultEnabled(Map.of("streamrune.crypto.vault.token", "  ")))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.vault.token"));
  }

  @Test
  void vaultCryptoEngineThrows_whenRuntimeDisabled() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildVaultCryptoEngine(
                    mock(DataSource.class),
                    props(Map.of("streamrune.crypto.vault.token", "s.test"))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.vault.enabled"));
  }

  @Test
  void vaultCryptoEngineRejectsInvalidExpireWrite_namingTheCacheKey() {
    // "5m" is the Quarkus-idiomatic shorthand, but these properties are parsed with the strict
    // ISO-8601 java.time.Duration format — the failure must point at the cache key and the
    // expected format, not at the active engine's config.
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildVaultCryptoEngine(
                    mock(DataSource.class),
                    props(
                        vaultEnabled(
                            Map.of(
                                "streamrune.crypto.vault.token", "s.test",
                                "streamrune.crypto.cache.expire-after-write", "5m")))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.cache.expire-after-write"));
    assertTrue(ex.getMessage().contains("ISO-8601"));
    assertTrue(ex.getMessage().contains("'5m'"));
  }

  @Test
  void vaultCryptoEngineRejectsInvalidExpireAccess_namingTheCacheKey() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildVaultCryptoEngine(
                    mock(DataSource.class),
                    props(
                        vaultEnabled(
                            Map.of(
                                "streamrune.crypto.vault.token", "s.test",
                                "streamrune.crypto.cache.expire-after-access", "xyz")))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.cache.expire-after-access"));
    assertTrue(ex.getMessage().contains("ISO-8601"));
  }

  @Test
  void vaultCryptoEngineFailsClosed_whenNoDataSourceForTerminalErasure() {
    // Valid config but NO DataSource → no durable
    // ForgottenSubjectStore → fail closed rather than shipping non-terminal Vault erasure (build()
    // throws, rewrapped here).
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildVaultCryptoEngine(
                    null, props(vaultEnabled(Map.of("streamrune.crypto.vault.token", "s.test")))));
    assertTrue(ex.getMessage().contains("Failed to build VaultCryptoEngine"));
    assertTrue(ex.getCause().getMessage().contains("ForgottenSubjectStore"));
  }

  // --- awsKmsCryptoEngine ---

  @Test
  void awsKmsCryptoEngineBuilt_withRegionAndKeyId_andDataSource_durableErasure() {
    // A DataSource present → durable JdbcForgottenSubjectStore auto-wired → engine builds.
    var engine =
        producers.buildAwsKmsCryptoEngine(
            mock(DataSource.class),
            props(
                awsEnabled(
                    Map.of(
                        "streamrune.crypto.aws.region", "us-west-2",
                        "streamrune.crypto.aws.kms-key-id",
                            "arn:aws:kms:us-west-2:123456789012:key/test-key-id"))));
    assertNotNull(engine);
    assertInstanceOf(CachedCryptoEngine.class, engine);
  }

  @Test
  void awsKmsCryptoEngineBuilt_uncachedWhenCacheDisabled() {
    var engine =
        producers.buildAwsKmsCryptoEngine(
            mock(DataSource.class),
            props(
                awsEnabled(
                    Map.of(
                        "streamrune.crypto.aws.region", "us-west-2",
                        "streamrune.crypto.aws.kms-key-id", "key-1",
                        "streamrune.crypto.cache.enabled", "false"))));
    assertNotNull(engine);
    assertFalse(engine instanceof CachedCryptoEngine);
  }

  @Test
  void awsKmsCryptoEngineFailsClosed_whenNoDataSourceForDurableErasure() {
    // Valid config but NO DataSource → no durable ForgottenSubjectStore → fail closed rather
    // than silently recording non-durable in-memory tombstones (build() throws, rewrapped here).
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildAwsKmsCryptoEngine(
                    null,
                    props(
                        awsEnabled(
                            Map.of(
                                "streamrune.crypto.aws.region", "us-west-2",
                                "streamrune.crypto.aws.kms-key-id", "key-1")))));
    assertTrue(ex.getMessage().contains("Failed to build AwsKmsCryptoEngine"));
    assertTrue(ex.getCause().getMessage().contains("ForgottenSubjectStore"));
    assertTrue(ex.getCause().getMessage().contains("durable GDPR erasure"));
  }

  @Test
  void awsKmsCryptoEngineFailsFast_whenRegionMissing() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildAwsKmsCryptoEngine(
                    mock(DataSource.class),
                    props(awsEnabled(Map.of("streamrune.crypto.aws.kms-key-id", "key-1")))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.aws.region"));
  }

  @Test
  void awsKmsCryptoEngineFailsFast_whenKmsKeyIdMissing() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildAwsKmsCryptoEngine(
                    mock(DataSource.class),
                    props(awsEnabled(Map.of("streamrune.crypto.aws.region", "us-west-2")))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.aws.kms-key-id"));
  }

  @Test
  void awsKmsCryptoEngineFailsFast_whenKmsKeyIdBlank() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildAwsKmsCryptoEngine(
                    mock(DataSource.class),
                    props(
                        awsEnabled(
                            Map.of(
                                "streamrune.crypto.aws.region", "us-west-2",
                                "streamrune.crypto.aws.kms-key-id", "  ")))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.aws.kms-key-id"));
  }

  @Test
  void awsKmsCryptoEngineThrows_whenRuntimeDisabled() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.buildAwsKmsCryptoEngine(
                    mock(DataSource.class),
                    props(
                        Map.of(
                            "streamrune.crypto.aws.region", "us-west-2",
                            "streamrune.crypto.aws.kms-key-id", "key-1"))));
    assertTrue(ex.getMessage().contains("streamrune.crypto.aws.enabled"));
  }

  // --- disposer: the produced CryptoEngine must be closed on context shutdown ---

  /**
   * CDI/Arc disposes a {@code @Produces} bean only via a matching {@code @Disposes} method. Without
   * one the CachedCryptoEngine's LISTEN connection, its dedicated Hikari pool + housekeeping
   * thread, and the listener virtual thread leak on every non-JVM-exit context shutdown (dev-mode
   * reload, multi-context test suites).
   *
   * <p>Every backend declares its own, in the same class as its producer — that co-location is what
   * makes the class-level gate able to remove both at once.
   */
  @Test
  void everyBackendDeclaresItsOwnEngineDisposer() throws Exception {
    for (Class<?> producerClass :
        List.of(
            StreamRuneCryptoProducers.FileSystemCryptoProducer.class,
            StreamRuneCryptoProducers.PostgresCryptoProducer.class,
            StreamRuneCryptoProducers.VaultCryptoProducer.class,
            StreamRuneCryptoProducers.AwsKmsCryptoProducer.class)) {
      var m = producerClass.getDeclaredMethod("disposeCryptoEngine", CryptoEngine.class);
      assertTrue(
          Arrays.stream(m.getParameterAnnotations()[0])
              .anyMatch(a -> a.annotationType() == Disposes.class),
          producerClass.getSimpleName()
              + "#disposeCryptoEngine's CryptoEngine parameter must be @Disposes so Arc invokes"
              + " it");
    }
  }

  @Test
  void disposerClosesAnAutoCloseableEngine() {
    // The produced engine is a CachedCryptoEngine (AutoCloseable); close() releases the forget
    // signal's LISTEN connection/pool/thread. The disposer must invoke it.
    CryptoEngine engine =
        mock(CryptoEngine.class, withSettings().extraInterfaces(AutoCloseable.class));

    StreamRuneCryptoProducers.closeEngine(engine);

    try {
      verify((AutoCloseable) engine).close();
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void disposerToleratesANonAutoCloseableEngine() {
    // An uncached (cache-disabled) engine may not be AutoCloseable; disposing it must be a no-op,
    // never a failure.
    CryptoEngine engine = mock(CryptoEngine.class);
    assertDoesNotThrow(() -> StreamRuneCryptoProducers.closeEngine(engine));
  }

  // --- the inline-built Vault HttpClient / AWS KmsClient must be container-closed ---

  @Test
  void vaultHttpClientDisposer_isDisposes_andClosesTheClient() throws Exception {
    // The Vault HttpClient is a container-managed bean (parity with Spring's vaultHttpClient
    // bean); Arc closes it on shutdown via the @Disposes method, instead of leaking a selector
    // thread + connection pool per dev-mode reload / @QuarkusTest context.
    var m =
        StreamRuneCryptoProducers.VaultCryptoProducer.class.getDeclaredMethod(
            "closeVaultHttpClient", StreamRuneCryptoProducers.StreamRuneVaultHttpClient.class);
    assertTrue(
        Arrays.stream(m.getParameterAnnotations()[0])
            .anyMatch(a -> a.annotationType() == Disposes.class),
        "closeVaultHttpClient's HttpClient parameter must be @Disposes so Arc closes the client");
    m.setAccessible(true);
    HttpClient client = mock(HttpClient.class);

    m.invoke(
        new StreamRuneCryptoProducers.VaultCryptoProducer(),
        new StreamRuneCryptoProducers.StreamRuneVaultHttpClient(client));

    verify(client).close();
  }

  @Test
  void awsKmsClientDisposer_isDisposes_andClosesTheClient() throws Exception {
    // The AWS KmsClient is a container-managed bean (parity with Spring's kmsClient bean);
    // Arc closes it on shutdown, instead of leaking the SDK's connection pool + reaper thread.
    var m =
        StreamRuneCryptoProducers.AwsKmsCryptoProducer.class.getDeclaredMethod(
            "closeAwsKmsClient", StreamRuneCryptoProducers.StreamRuneKmsClient.class);
    assertTrue(
        Arrays.stream(m.getParameterAnnotations()[0])
            .anyMatch(a -> a.annotationType() == Disposes.class),
        "closeAwsKmsClient's KmsClient parameter must be @Disposes so Arc closes the client");
    m.setAccessible(true);
    KmsClient client = mock(KmsClient.class);

    m.invoke(
        new StreamRuneCryptoProducers.AwsKmsCryptoProducer(),
        new StreamRuneCryptoProducers.StreamRuneKmsClient(client));

    verify(client).close();
  }

  // --- resolveForgetSignal ---

  @Test
  void resolveForgetSignal_wiresPostgresChannel_whenDataSourcePresent() {
    var signal =
        StreamRuneCryptoProducers.resolveForgetSignal(
            mock(DataSource.class), java.util.Optional.of("PT5M"), "aws-kms");
    assertInstanceOf(
        org.streamrune.crypto.postgres.PostgresCryptoForgetSignal.class,
        signal,
        "a DataSource-backed backend must get the prompt LISTEN/NOTIFY forget channel, not NOOP");
  }

  @Test
  void resolveForgetSignal_fallsBackToNoop_whenNoDataSource() {
    var signal =
        StreamRuneCryptoProducers.resolveForgetSignal(null, java.util.Optional.empty(), "vault");
    assertSame(org.streamrune.crypto.CryptoForgetSignal.NOOP, signal);
  }
}
