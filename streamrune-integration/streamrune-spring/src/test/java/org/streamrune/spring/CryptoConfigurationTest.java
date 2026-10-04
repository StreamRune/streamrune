package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.UserConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.crypto.CryptoEngine;

/** Tests for crypto engine auto-configuration beans. */
class CryptoConfigurationTest {

  @Test
  void awsKmsCryptoEngineCreatedWhenDataSourcePresent_durableErasure() {
    // With a DataSource present the durable JdbcForgottenSubjectStore is auto-wired and the
    // engine builds (the store is lazy — no DB access at construction).
    var props =
        CryptoTestProps.builder()
            .awsRegion("us-west-2")
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();

    var config = new AwsKmsCryptoEngineConfiguration();
    CryptoEngine engine =
        config.awsKmsCryptoEngine(config.streamRuneKmsClient(props), mock(DataSource.class), props);
    assertNotNull(engine);
  }

  @Test
  void awsKmsCryptoEngineFailsClosed_whenNoDataSourceForDurableErasure() {
    // Without a DataSource there is no durable ForgottenSubjectStore, so the engine must FAIL
    // CLOSED instead of silently recording in-memory tombstones that vanish on restart.
    var props =
        CryptoTestProps.builder()
            .awsRegion("us-west-2")
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();

    var config = new AwsKmsCryptoEngineConfiguration();
    var kmsClient = config.streamRuneKmsClient(props);
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class, () -> config.awsKmsCryptoEngine(kmsClient, null, props));
    assertTrue(e.getMessage().contains("ForgottenSubjectStore"));
    assertTrue(e.getMessage().contains("durable GDPR erasure"));
  }

  @Test
  void awsKmsCryptoEngineRejectsBlankRegion() {
    var props =
        CryptoTestProps.builder()
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();
    // region stays null

    var config = new AwsKmsCryptoEngineConfiguration();
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> config.streamRuneKmsClient(props));
    assertTrue(e.getMessage().contains("streamrune.crypto.aws.region"));
  }

  @Test
  void awsKmsCryptoEngineRejectsWhitespaceRegion() {
    var props =
        CryptoTestProps.builder()
            .awsRegion("   ")
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .build();

    var config = new AwsKmsCryptoEngineConfiguration();
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> config.streamRuneKmsClient(props));
    assertTrue(e.getMessage().contains("streamrune.crypto.aws.region"));
  }

  @Test
  void awsKmsCryptoEngineRejectsNullKmsKeyId() {
    var props = CryptoTestProps.builder().awsRegion("us-west-2").build();
    // kmsKeyId stays null

    var config = new AwsKmsCryptoEngineConfiguration();
    var kmsClient = config.streamRuneKmsClient(props);
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class, () -> config.awsKmsCryptoEngine(kmsClient, null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.aws.kms-key-id"));
  }

  @Test
  void awsKmsCryptoEngineRejectsWhitespaceKmsKeyId() {
    var props = CryptoTestProps.builder().awsRegion("us-west-2").awsKmsKeyId("   ").build();

    var config = new AwsKmsCryptoEngineConfiguration();
    var kmsClient = config.streamRuneKmsClient(props);
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class, () -> config.awsKmsCryptoEngine(kmsClient, null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.aws.kms-key-id"));
  }

  @Test
  void fileSystemCryptoEngineCreatedWhenKeyDirectoryProvided() {
    var props = CryptoTestProps.builder().keyDirectory("/var/lib/streamrune/keys").build();

    var config = new FileSystemCryptoEngineConfiguration();
    CryptoEngine engine = config.fileSystemCryptoEngine(mock(DataSource.class), props);
    assertNotNull(engine);
  }

  @Test
  void fileSystemCryptoEngineRejectsBlankKeyDirectory() {
    var props = CryptoTestProps.builder().keyDirectory(null).build();

    var config = new FileSystemCryptoEngineConfiguration();
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> config.fileSystemCryptoEngine(null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.filesystem.key-directory"));
  }

  @Test
  void fileSystemCryptoEngineRejectsWhitespaceKeyDirectory() {
    var props = CryptoTestProps.builder().keyDirectory("   ").build();

    var config = new FileSystemCryptoEngineConfiguration();
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> config.fileSystemCryptoEngine(null, props));
    assertTrue(e.getMessage().contains("streamrune.crypto.filesystem.key-directory"));
  }

  @Test
  void vaultCryptoEngineCreatedWhenDataSourcePresent_terminalErasure() {
    // With a DataSource present the durable
    // JdbcForgottenSubjectStore
    // is auto-wired (lazy — no DB access at construction) and the engine builds.
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .build();

    var config = new VaultCryptoEngineConfiguration();
    CryptoEngine engine =
        config.vaultCryptoEngine(config.streamRuneVaultHttpClient(), mock(DataSource.class), props);
    assertNotNull(engine);
  }

  @Test
  void vaultCryptoEngineFailsClosed_whenNoDataSourceForTerminalErasure() {
    // Without a DataSource there is no durable
    // ForgottenSubjectStore,
    // so the engine must FAIL CLOSED rather than shipping non-terminal Vault erasure (a post-forget
    // encrypt would otherwise resurrect the subject under a fresh Vault transit key).
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .build();

    var config = new VaultCryptoEngineConfiguration();
    var httpClient = config.streamRuneVaultHttpClient();
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class, () -> config.vaultCryptoEngine(httpClient, null, props));
    assertTrue(e.getMessage().contains("ForgottenSubjectStore"));
  }

  @Test
  void vaultCryptoEngineRejectsBlankToken() {
    var props =
        CryptoTestProps.builder()
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .build();
    // token stays null

    var config = new VaultCryptoEngineConfiguration();
    var httpClient = config.streamRuneVaultHttpClient();
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> config.vaultCryptoEngine(httpClient, mock(DataSource.class), props));
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

    var config = new VaultCryptoEngineConfiguration();
    var httpClient = config.streamRuneVaultHttpClient();
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> config.vaultCryptoEngine(httpClient, mock(DataSource.class), props));
    assertTrue(e.getMessage().contains("streamrune.crypto.vault.token"));
  }

  // ── streamrune.crypto.cache.enabled=false must skip the CachedCryptoEngine wrapper ──
  // (GDPR no-cache posture — parity with Quarkus, previously silently ignored on Spring).

  @Test
  void cacheEnabledByDefault_wrapsInCachedCryptoEngine() {
    var props = CryptoTestProps.builder().keyDirectory("/var/lib/streamrune/keys").build();
    assertTrue(props.cache().enabled(), "cache.enabled defaults to true");

    var config = new FileSystemCryptoEngineConfiguration();
    CryptoEngine engine = config.fileSystemCryptoEngine(mock(DataSource.class), props);
    assertInstanceOf(
        org.streamrune.crypto.CachedCryptoEngine.class,
        engine,
        "default (cache enabled) must wrap in CachedCryptoEngine");
  }

  @Test
  void fileSystemCryptoEngineSkipsCache_whenCacheDisabled() {
    var props =
        CryptoTestProps.builder()
            .keyDirectory("/var/lib/streamrune/keys")
            .cacheEnabled(false)
            .build();

    var config = new FileSystemCryptoEngineConfiguration();
    CryptoEngine engine = config.fileSystemCryptoEngine(mock(DataSource.class), props);
    assertFalse(
        engine instanceof org.streamrune.crypto.CachedCryptoEngine,
        "cache.enabled=false must NOT wrap in CachedCryptoEngine (no PII cached in heap)");
    assertInstanceOf(org.streamrune.filesystem.FileSystemCryptoEngine.class, engine);
  }

  @Test
  void postgresCryptoEngineSkipsCache_whenCacheDisabled() {
    var props = CryptoTestProps.builder().cacheEnabled(false).build();

    var config = new PostgresCryptoEngineConfiguration();
    CryptoEngine engine = config.postgresCryptoEngine(mock(DataSource.class), props);
    assertFalse(
        engine instanceof org.streamrune.crypto.CachedCryptoEngine,
        "cache.enabled=false must NOT wrap the Postgres engine in CachedCryptoEngine");
  }

  @Test
  void vaultCryptoEngineSkipsCache_whenCacheDisabled() {
    var props =
        CryptoTestProps.builder()
            .vaultToken("s.test")
            .vaultAddress("http://localhost:8200")
            .vaultEngineMount("transit")
            .cacheEnabled(false)
            .build();

    var config = new VaultCryptoEngineConfiguration();
    CryptoEngine engine =
        config.vaultCryptoEngine(config.streamRuneVaultHttpClient(), mock(DataSource.class), props);
    assertFalse(
        engine instanceof org.streamrune.crypto.CachedCryptoEngine,
        "cache.enabled=false must NOT wrap the Vault engine in CachedCryptoEngine");
  }

  @Test
  void awsKmsCryptoEngineSkipsCache_whenCacheDisabled() {
    var props =
        CryptoTestProps.builder()
            .awsRegion("us-west-2")
            .awsKmsKeyId("arn:aws:kms:us-west-2:123456789012:key/test-key-id")
            .cacheEnabled(false)
            .build();

    var config = new AwsKmsCryptoEngineConfiguration();
    CryptoEngine engine =
        config.awsKmsCryptoEngine(config.streamRuneKmsClient(props), mock(DataSource.class), props);
    assertFalse(
        engine instanceof org.streamrune.crypto.CachedCryptoEngine,
        "cache.enabled=false must NOT wrap the AWS-KMS engine in CachedCryptoEngine");
  }

  @Test
  void bareEngineDisposesCleanly_underInferredDestroyMethod() {
    // Disposal safety: with cache disabled the crypto configs return the BARE delegate, which
    // (unlike CachedCryptoEngine) has no close() method. The configs therefore use an INFERRED
    // destroy method (plain @Bean) rather than @Bean(destroyMethod = "close"): Spring closes the
    // AutoCloseable CachedCryptoEngine on the enabled path but silently skips a bare engine with no
    // close(). This mirrors the production @Bean pattern; an explicit destroyMethod = "close" would
    // instead fail bean creation with "Invalid destruction signature" (proven while developing this
    // fix), which would break the no-cache posture at startup.
    new ApplicationContextRunner()
        .withConfiguration(UserConfigurations.of(BareEngineConfig.class))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(CryptoEngine.class))
                  .isInstanceOf(org.streamrune.filesystem.FileSystemCryptoEngine.class)
                  .isNotInstanceOf(org.streamrune.crypto.CachedCryptoEngine.class);
            });
    // Leaving run(...) closes the context; a throwing destroy method would surface here.
  }

  // ── Enabling a framework crypto backend on Spring must BOOT the context. ──
  // Regression guard for the advertised zero-config crypto path. The older tests above only invoke
  // the config methods directly, so they never exercise the properties binding — these runs go
  // through the real auto-config boot path. The immutable properties record is registered once via
  // CryptoPropertiesConfiguration's @EnableConfigurationProperties and bound by its constructor.

  private final ApplicationContextRunner cryptoContextRunner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  CryptoPropertiesConfiguration.class,
                  FileSystemCryptoEngineConfiguration.class,
                  PostgresCryptoEngineConfiguration.class));

  @Test
  void fileSystemBackendBootsContext_andBindsCryptoProperties() {
    cryptoContextRunner
        .withPropertyValues(
            "streamrune.crypto.filesystem.enabled=true",
            "streamrune.crypto.filesystem.key-directory=/var/lib/streamrune/keys",
            "streamrune.crypto.cache.maximum-size=42")
        .run(
            ctx -> {
              // (a) the context STARTS — before the fix this failed with
              // "No ConfigurationProperties annotation found on 'StreamRuneCryptoProperties'".
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(CryptoEngine.class);
              // (b) the single properties bean exists and a value is bound from configuration
              // (relaxed binding: maximum-size -> maximumSize).
              assertThat(ctx).hasSingleBean(StreamRuneCryptoProperties.class);
              assertThat(ctx.getBean(StreamRuneCryptoProperties.class).cache().maximumSize())
                  .isEqualTo(42);
            });
  }

  @Test
  void postgresBackendBootsContext_andBindsCryptoProperties() {
    cryptoContextRunner
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withPropertyValues(
            "streamrune.crypto.postgres.enabled=true",
            // no-cache posture keeps the LISTEN/NOTIFY forget channel off the mock DataSource.
            "streamrune.crypto.cache.enabled=false",
            "streamrune.crypto.postgres.table-name=custom_encryption_keys",
            "streamrune.crypto.cache.maximum-size=42")
        .run(
            ctx -> {
              // (a) the context STARTS (same boot break as the filesystem backend before the fix).
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(CryptoEngine.class);
              // (b) the single properties bean exists with values bound from configuration
              // (relaxed binding: table-name -> tableName, maximum-size -> maximumSize).
              assertThat(ctx).hasSingleBean(StreamRuneCryptoProperties.class);
              var props = ctx.getBean(StreamRuneCryptoProperties.class);
              assertThat(props.postgres().tableName()).isEqualTo("custom_encryption_keys");
              assertThat(props.cache().maximumSize()).isEqualTo(42);
            });
  }

  @Test
  void defaultsAreBoundWhenNothingIsConfigured() {
    cryptoContextRunner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          var props = ctx.getBean(StreamRuneCryptoProperties.class);
          assertThat(props.cache().enabled()).isTrue();
          assertThat(props.cache().maximumSize()).isEqualTo(10_000);
          assertThat(props.cache().expireAfterWrite()).isEqualTo(java.time.Duration.ofMinutes(5));
          assertThat(props.cache().expireAfterAccess()).isNull();
          assertThat(props.filesystem().keyDirectory()).isEqualTo("/var/lib/streamrune/keys");
          assertThat(props.postgres().tableName()).isEqualTo("encryption_keys");
          assertThat(props.vault().address()).isEqualTo("http://localhost:8200");
          assertThat(props.vault().engineMount()).isEqualTo("transit");
          assertThat(props.vault().token()).isNull();
          assertThat(props.aws().region()).isNull();
          assertThat(props.aws().kmsKeyId()).isNull();
        });
  }

  @Test
  void everyKeyBindsThroughTheConstructors() {
    cryptoContextRunner
        .withPropertyValues(
            "streamrune.crypto.cache.enabled=false",
            "streamrune.crypto.cache.maximum-size=500",
            "streamrune.crypto.cache.expire-after-write=1h",
            "streamrune.crypto.cache.expire-after-access=30m",
            "streamrune.crypto.filesystem.key-directory=/custom/keys",
            "streamrune.crypto.postgres.table-name=custom_keys",
            "streamrune.crypto.vault.address=http://vault:8200",
            "streamrune.crypto.vault.token=s.test",
            "streamrune.crypto.vault.engine-mount=custom-transit",
            "streamrune.crypto.aws.region=us-west-2",
            "streamrune.crypto.aws.kms-key-id=key-123")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var props = ctx.getBean(StreamRuneCryptoProperties.class);
              assertThat(props.cache().enabled()).isFalse();
              assertThat(props.cache().maximumSize()).isEqualTo(500);
              assertThat(props.cache().expireAfterWrite()).isEqualTo(java.time.Duration.ofHours(1));
              assertThat(props.cache().expireAfterAccess())
                  .isEqualTo(java.time.Duration.ofMinutes(30));
              assertThat(props.filesystem().keyDirectory()).isEqualTo("/custom/keys");
              assertThat(props.postgres().tableName()).isEqualTo("custom_keys");
              assertThat(props.vault().address()).isEqualTo("http://vault:8200");
              assertThat(props.vault().token()).isEqualTo("s.test");
              assertThat(props.vault().engineMount()).isEqualTo("custom-transit");
              assertThat(props.aws().region()).isEqualTo("us-west-2");
              assertThat(props.aws().kmsKeyId()).isEqualTo("key-123");
            });
  }

  @Test
  void propertiesAreImmutableRecordsWithoutSetters() {
    for (Class<?> type :
        new Class<?>[] {
          StreamRuneCryptoProperties.class,
          StreamRuneCryptoProperties.Cache.class,
          StreamRuneCryptoProperties.FileSystem.class,
          StreamRuneCryptoProperties.Postgres.class,
          StreamRuneCryptoProperties.Vault.class,
          StreamRuneCryptoProperties.Aws.class
        }) {
      assertTrue(type.isRecord(), type.getSimpleName() + " must be a record");
      for (var m : type.getMethods()) {
        assertFalse(
            m.getName().startsWith("set"),
            type.getSimpleName() + " must not expose the setter " + m.getName());
      }
    }
  }

  @org.springframework.context.annotation.Configuration
  static class BareEngineConfig {
    // Mirrors the production crypto configs: inferred destroy method, may return a bare engine.
    @org.springframework.context.annotation.Bean
    CryptoEngine cryptoEngine() {
      return org.streamrune.filesystem.FileSystemCryptoEngine.builder()
          .keyDirectory(java.nio.file.Path.of("/var/lib/streamrune/keys"))
          .build();
    }
  }
}
