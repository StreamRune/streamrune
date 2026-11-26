package org.streamrune.micronaut;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Internal;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.aws.AwsKmsCryptoEngine;
import org.streamrune.crypto.filesystem.FileSystemCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoBackend;
import org.streamrune.crypto.vault.VaultCryptoEngine;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;

/**
 * Micronaut factory for StreamRune crypto engine components. Registers crypto engine beans in the
 * Micronaut context, conditionally enabled via configuration properties.
 */
@Factory
public class StreamRuneCryptoFactory {

  private static final Logger log = LoggerFactory.getLogger(StreamRuneCryptoFactory.class);

  /** Creates the crypto factory. */
  public StreamRuneCryptoFactory() {}

  // ── container-managed Vault HttpClient / AWS KmsClient ─────────────────────────────────
  //
  // The Vault HttpClient and AWS KmsClient were built inline in the delegate builders and owned by
  // nobody — Micronaut does not auto-close @Factory-produced AutoCloseable beans, and neither the
  // CachedCryptoEngine (which does not close its delegate) nor the bare engine (no preDestroy at
  // all) closed them. Every @MicronautTest context / embedded restart leaked a JDK HttpClient
  // (selector thread + pool) and a KmsClient (SDK connection pool + reaper thread). Register both
  // as @Bean(preDestroy = "close") so the container closes them on shutdown — parity with Spring's
  // vaultHttpClient / kmsClient beans and the Quarkus @Produces + @Disposes pair.

  // ── the managed clients are namespaced in StreamRune-owned holder types ────────────
  //
  // Declaring plain @Singleton HttpClient / KmsClient beans put the framework into by-type
  // resolution of types applications routinely declare themselves: an app with its own HttpClient
  // bean (an outbound REST client) or KmsClient bean got "Multiple possible bean candidates found"
  // at boot — for the framework's own engine wiring AND for every application injection point of
  // that type — the moment the operator enabled the documented crypto property. Holding each client
  // inside a type only StreamRune declares keeps the shutdown-close property while
  // registering no bean of the ubiquitous type at all, so nothing can collide and no application
  // client can be selected here by accident (which would route Vault traffic, bearer token
  // included, over a client with unknown proxy/TLS/timeout settings).

  /** Container-managed Vault {@link HttpClient}, closed on context shutdown. */
  @Singleton
  @Requires(property = "streamrune.crypto.vault.enabled", value = "true")
  @Bean(preDestroy = "close")
  StreamRuneVaultHttpClient vaultHttpClient() {
    return new StreamRuneVaultHttpClient(HttpClient.newHttpClient());
  }

  /** Container-managed AWS {@link KmsClient}, closed on context shutdown. */
  @Singleton
  @Requires(property = "streamrune.crypto.aws.enabled", value = "true")
  @Bean(preDestroy = "close")
  StreamRuneKmsClient awsKmsClient(StreamRuneMicronautCryptoProperties props) {
    var ap = props.aws();
    if (ap.region() == null || ap.region().isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.region must be set when streamrune.crypto.aws.enabled is true");
    }
    return new StreamRuneKmsClient(KmsClient.builder().region(Region.of(ap.region())).build());
  }

  // ── the crypto backend: the stores and signals of the selected storage backend ─────────────────
  //
  // Every engine takes its forgotten-subject store, its forget signal and (the key-store engine)
  // the engine itself from the CryptoBackend of the storage backend streamrune.backend names, so
  // they live where the event store lives. A bean only when an engine is enabled.

  /**
   * The crypto backend of the selected storage backend, when a crypto engine is enabled ({@link
   * AnyCryptoEngineEnabled}).
   *
   * @param backend the selected storage backend
   * @return its crypto backend
   */
  @Singleton
  @Requires(condition = AnyCryptoEngineEnabled.class)
  CryptoBackend streamRuneCryptoBackend(
      StreamRuneBackend backend, io.micronaut.context.env.Environment environment) {
    return cryptoBackendOf(
        backend,
        isTrue(environment, org.streamrune.integration.BackendStartup.POSTGRES_KEY_STORE_PROPERTY),
        isTrue(environment, org.streamrune.integration.BackendStartup.SQLITE_KEY_STORE_PROPERTY));
  }

  /** Whether {@code property} equals {@code true}, the comparison the engines' requires make. */
  private static boolean isTrue(io.micronaut.context.env.Environment environment, String property) {
    return "true".equals(environment.getProperty(property, String.class).orElse(null));
  }

  /**
   * The crypto backend of {@code backend}, selected by its name: {@link PostgresCryptoBackend} for
   * {@code postgres}; for {@code sqlite} the SQLite crypto backend, loaded by name, which fails
   * naming the module {@code org.streamrune:streamrune-sqlite-crypto} when it is missing.
   * Package-private for direct testing.
   *
   * @param backend the storage backend
   * @param postgresKeyStore whether {@code streamrune.crypto.postgres.enabled} is on
   * @param sqliteKeyStore whether {@code streamrune.crypto.sqlite.enabled} is on
   * @throws IllegalStateException when the key-store engine of another backend is enabled, or the
   *     SQLite crypto backend cannot be loaded
   */
  static CryptoBackend cryptoBackendOf(
      StreamRuneBackend backend, boolean postgresKeyStore, boolean sqliteKeyStore) {
    org.streamrune.integration.BackendStartup.requireKeyStoreEngineOfBackend(
        backend.name(), postgresKeyStore, sqliteKeyStore);
    return org.streamrune.integration.BackendSelection.resolveCrypto(
        backend.name(), CryptoBackend.class, CryptoBackend::name, PostgresCryptoBackend::new);
  }

  /**
   * Matches when one of the crypto engines is enabled: {@code
   * streamrune.crypto.filesystem.enabled}, {@code streamrune.crypto.postgres.enabled}, {@code
   * streamrune.crypto.sqlite.enabled}, {@code streamrune.crypto.vault.enabled} or {@code
   * streamrune.crypto.aws.enabled} is {@code true} — the value each engine's own
   * {@code @Requires(property = ..., value = "true")} matches. Public only because Micronaut
   * instantiates it for {@code @Requires(condition = ...)}.
   *
   * @hidden
   */
  @Internal
  public static final class AnyCryptoEngineEnabled
      implements io.micronaut.context.condition.Condition {

    /** The enabling property of each crypto engine. */
    static final java.util.List<String> ENGINE_PROPERTIES =
        java.util.List.of(
            "streamrune.crypto.filesystem.enabled",
            "streamrune.crypto.postgres.enabled",
            "streamrune.crypto.sqlite.enabled",
            "streamrune.crypto.vault.enabled",
            "streamrune.crypto.aws.enabled");

    /** Creates the condition; Micronaut instantiates it. */
    public AnyCryptoEngineEnabled() {}

    @Override
    public boolean matches(io.micronaut.context.condition.ConditionContext context) {
      if (!(context.getBeanContext()
          instanceof io.micronaut.core.value.PropertyResolver properties)) {
        return false;
      }
      for (String property : ENGINE_PROPERTIES) {
        if ("true".equals(properties.getProperty(property, String.class).orElse(null))) {
          return true;
        }
      }
      return false;
    }
  }

  /** StreamRune-owned holder for the Vault {@link HttpClient}. */
  record StreamRuneVaultHttpClient(HttpClient client) implements AutoCloseable {
    @Override
    public void close() {
      client.close();
    }
  }

  /** StreamRune-owned holder for the AWS {@link KmsClient}. */
  record StreamRuneKmsClient(KmsClient client) implements AutoCloseable {
    @Override
    public void close() {
      client.close();
    }
  }

  // ── cache.enabled gates a cached vs. bare bean per backend ────────────────────────────
  //
  // streamrune.crypto.cache.enabled=false must skip the CachedCryptoEngine wrapper entirely so no
  // decrypted PII lingers in heap (the deliberate no-cache GDPR posture) — parity with Quarkus,
  // where the property was already honored. This cannot be a single method that conditionally
  // returns the bare delegate: Micronaut requires the factory method's DECLARED return type to be
  // the
  // AutoCloseable CachedCryptoEngine so Micronaut can resolve @Bean(preDestroy = "close") at
  // compile time (Micronaut does NOT auto-close @Factory-produced AutoCloseable beans, and cannot
  // resolve preDestroy = "close" against the bare CryptoEngine interface). So each backend has two
  // mutually exclusive beans, gated on cache.enabled:
  //   * the CACHED bean (default; @Requires notEquals "false") returns CachedCryptoEngine + closes;
  //   * the BARE bean (@Requires value "false") returns the raw CryptoEngine (no cache, nothing to
  //     close — the bare delegate holds no CachedCryptoEngine LISTEN connection / Hikari pool).
  // The shared build*Delegate helpers keep the delegate construction (and its validation) DRY.

  @Singleton
  @Requires(property = "streamrune.crypto.filesystem.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", notEquals = "false")
  @Bean(preDestroy = "close")
  CachedCryptoEngine fileSystemCryptoEngine(
      CryptoBackend cryptoBackend,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props,
      @Nullable StreamRuneMetrics metrics) {
    // Wire the LISTEN/NOTIFY forget channel when a DataSource is present so a
    // deleteKey on any replica promptly evicts a forgotten subject's cached plaintext on peers.
    return wrapWithCache(
        buildFileSystemDelegate(props, metrics),
        props.cache(),
        resolveForgetSignal(
            cryptoBackend, dataSource, props.cache().expireAfterWrite(), "filesystem"));
  }

  /** Bare (no-cache) file-system engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.filesystem.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine fileSystemCryptoEngineUncached(
      StreamRuneMicronautCryptoProperties props, @Nullable StreamRuneMetrics metrics) {
    return buildFileSystemDelegate(props, metrics);
  }

  @Singleton
  @Requires(property = "streamrune.crypto.postgres.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", notEquals = "false")
  @Bean(preDestroy = "close")
  CachedCryptoEngine postgresCryptoEngine(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      StreamRuneMicronautCryptoProperties props,
      @Nullable StreamRuneMetrics metrics) {
    // A Postgres DataSource is present, so wire the LISTEN/NOTIFY forget channel for
    // prompt cross-replica cache invalidation on deleteKey. Micronaut does NOT auto-close
    // @Factory-produced AutoCloseable beans, so @Bean(preDestroy = "close") is required for
    // the container to stop the listener and release its dedicated connection on context shutdown.
    return wrapWithCache(
        buildPostgresDelegate(cryptoBackend, dataSource, props, metrics),
        props.cache(),
        cryptoBackend.forgetSignal(dataSource));
  }

  /** Bare (no-cache) Postgres engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.postgres.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine postgresCryptoEngineUncached(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      StreamRuneMicronautCryptoProperties props,
      @Nullable StreamRuneMetrics metrics) {
    return buildPostgresDelegate(cryptoBackend, dataSource, props, metrics);
  }

  /**
   * The SQLite key-store engine ({@code streamrune.crypto.sqlite.enabled=true} with {@code
   * streamrune.backend=sqlite}): the crypto backend's key store with the default key table (SQLite
   * has no table-name property), cached with the backend's in-process forget signal.
   */
  @Singleton
  @Requires(property = "streamrune.crypto.sqlite.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", notEquals = "false")
  @Bean(preDestroy = "close")
  CachedCryptoEngine sqliteCryptoEngine(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      StreamRuneMicronautCryptoProperties props,
      @Nullable StreamRuneMetrics metrics) {
    return wrapWithCache(
        buildSqliteDelegate(cryptoBackend, dataSource, metrics),
        props.cache(),
        cryptoBackend.forgetSignal(dataSource));
  }

  /** Bare (no-cache) SQLite engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.sqlite.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine sqliteCryptoEngineUncached(
      CryptoBackend cryptoBackend, DataSource dataSource, @Nullable StreamRuneMetrics metrics) {
    return buildSqliteDelegate(cryptoBackend, dataSource, metrics);
  }

  /** The SQLite key-store engine, uncached, with the default key table. */
  static CryptoEngine buildSqliteDelegate(
      CryptoBackend cryptoBackend, DataSource dataSource, @Nullable StreamRuneMetrics metrics) {
    return cryptoBackend.keyStoreEngine(
        dataSource,
        new CryptoBackend.KeyStoreEngineSettings(
            null, StreamRuneMicronautModule.metricsOrNoop(metrics)));
  }

  /**
   * Auto-wires the crypto backend's durable forgotten-subject store ({@link
   * CryptoBackend#forgottenSubjects}) when a {@link DataSource} is present, so Vault GDPR erasure
   * is terminal (a forgotten subject cannot be re-encrypted) and survives restarts. Without a
   * DataSource there is no durable store, so {@code build()} fails closed rather than shipping
   * non-terminal Vault erasure. Requires the crypto Flyway migration ({@code forgotten_subjects}
   * table).
   */
  @Singleton
  @Requires(property = "streamrune.crypto.vault.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", notEquals = "false")
  @Bean(preDestroy = "close")
  CachedCryptoEngine vaultCryptoEngine(
      CryptoBackend cryptoBackend,
      StreamRuneVaultHttpClient httpClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props) {
    // The same DataSource wires the LISTEN/NOTIFY forget channel.
    return wrapWithCache(
        buildVaultDelegate(cryptoBackend, httpClient.client(), dataSource, props),
        props.cache(),
        resolveForgetSignal(cryptoBackend, dataSource, props.cache().expireAfterWrite(), "vault"));
  }

  /** Bare (no-cache) Vault engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.vault.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine vaultCryptoEngineUncached(
      CryptoBackend cryptoBackend,
      StreamRuneVaultHttpClient httpClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props) {
    return buildVaultDelegate(cryptoBackend, httpClient.client(), dataSource, props);
  }

  /**
   * Auto-wires the crypto backend's durable forgotten-subject store ({@link
   * CryptoBackend#forgottenSubjects}) when a {@link DataSource} is present, so AWS-KMS GDPR erasure
   * survives restarts out of the box. Without a DataSource there is no durable store, so {@code
   * build()} fails closed rather than recording in-memory tombstones that vanish on restart.
   * Requires the crypto Flyway migration ({@code forgotten_subjects} table).
   */
  @Singleton
  @Requires(property = "streamrune.crypto.aws.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", notEquals = "false")
  @Bean(preDestroy = "close")
  CachedCryptoEngine awsKmsCryptoEngine(
      CryptoBackend cryptoBackend,
      StreamRuneKmsClient kmsClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props,
      @Nullable StreamRuneMetrics metrics) {
    // The same DataSource that backs the durable tombstone
    // store also wires the LISTEN/NOTIFY forget channel for prompt cross-replica cache eviction.
    return wrapWithCache(
        buildAwsKmsDelegate(cryptoBackend, kmsClient.client(), dataSource, props, true, metrics),
        props.cache(),
        resolveForgetSignal(
            cryptoBackend, dataSource, props.cache().expireAfterWrite(), "aws-kms"));
  }

  /** Bare (no-cache) AWS-KMS engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.aws.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine awsKmsCryptoEngineUncached(
      CryptoBackend cryptoBackend,
      StreamRuneKmsClient kmsClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props,
      @Nullable StreamRuneMetrics metrics) {
    // The no-cache posture keeps no data key in heap either.
    return buildAwsKmsDelegate(
        cryptoBackend, kmsClient.client(), dataSource, props, false, metrics);
  }

  // ── delegate builders (shared by the cached + bare beans above) ──────────────────────────────

  // The metrics count discarded encryptions and retried decryptions; NOOP without a metrics bean.
  private CryptoEngine buildFileSystemDelegate(
      StreamRuneMicronautCryptoProperties props, @Nullable StreamRuneMetrics metrics) {
    try {
      return FileSystemCryptoEngine.builder()
          .keyDirectory(Path.of(props.filesystem().keyDirectory()))
          .metrics(StreamRuneMicronautModule.metricsOrNoop(metrics))
          .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build FileSystemCryptoEngine. Check streamrune.crypto.filesystem.key-directory.",
          e);
    }
  }

  private CryptoEngine buildPostgresDelegate(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      StreamRuneMicronautCryptoProperties props,
      @Nullable StreamRuneMetrics metrics) {
    // Honor the configured key-table name (validated as a SQL identifier in the engine
    // builder). Default keeps the migration-created "encryption_keys" table.
    return cryptoBackend.keyStoreEngine(
        dataSource,
        new CryptoBackend.KeyStoreEngineSettings(
            props.postgres().tableName(), StreamRuneMicronautModule.metricsOrNoop(metrics)));
  }

  private CryptoEngine buildVaultDelegate(
      CryptoBackend cryptoBackend,
      HttpClient httpClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props) {
    var vp = props.vault();
    if (vp.token() == null || vp.token().isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.vault.token must be set when streamrune.crypto.vault.enabled is true");
    }
    var builder =
        VaultCryptoEngine.builder()
            .httpClient(httpClient)
            .vaultAddress(vp.address())
            .token(vp.token())
            .engineMount(vp.engineMount());
    if (dataSource != null) {
      builder.forgottenSubjectStore(cryptoBackend.forgottenSubjects(dataSource));
    }
    return builder.build();
  }

  private CryptoEngine buildAwsKmsDelegate(
      CryptoBackend cryptoBackend,
      KmsClient kmsClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props,
      boolean dataKeyCacheOn,
      @Nullable StreamRuneMetrics metrics) {
    var ap = props.aws();
    if (ap.region() == null || ap.region().isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.region must be set when streamrune.crypto.aws.enabled is true");
    }
    if (ap.kmsKeyId() == null || ap.kmsKeyId().isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.kms-key-id must be set when streamrune.crypto.aws.enabled is true");
    }
    var dataKeyCache = ap.dataKeyCache();
    int maximumSize = dataKeyCache.maximumSize();
    if (maximumSize < 0) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.data-key-cache.maximum-size must be 0 or more (0 disables the"
              + " data-key cache), got "
              + maximumSize);
    }
    Duration expireAfterWrite = dataKeyCache.expireAfterWrite();
    if (expireAfterWrite == null || expireAfterWrite.isZero() || expireAfterWrite.isNegative()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.data-key-cache.expire-after-write must be a positive duration,"
              + " got "
              + expireAfterWrite);
    }
    var builder =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId(ap.kmsKeyId())
            .metrics(StreamRuneMicronautModule.metricsOrNoop(metrics))
            .dataKeyCacheMaximumSize(dataKeyCacheOn ? maximumSize : 0)
            .dataKeyCacheExpireAfterWrite(expireAfterWrite);
    if (dataSource != null) {
      builder.forgottenSubjectStore(cryptoBackend.forgottenSubjects(dataSource));
    }
    return builder.build();
  }

  /**
   * Resolves the cross-replica forget channel for a non-Postgres engine — the crypto backend's
   * prompt signal ({@link CryptoBackend#forgetSignal(DataSource)}; on PostgreSQL LISTEN/NOTIFY)
   * when a DataSource is present, else {@link CryptoForgetSignal#NOOP} with a startup WARN naming
   * the cache write-TTL ceiling as the only cross-replica erasure bound. Package-private for direct
   * testing.
   */
  static CryptoForgetSignal resolveForgetSignal(
      CryptoBackend cryptoBackend,
      @Nullable DataSource dataSource,
      @Nullable Duration configuredWriteTtl,
      String backend) {
    if (dataSource != null) {
      return cryptoBackend.forgetSignal(dataSource);
    }
    Duration writeTtl =
        configuredWriteTtl != null
            ? configuredWriteTtl
            : CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE;
    log.warn(
        "No DataSource available to wire the cross-replica crypto-forget channel for the {} crypto"
            + " backend; after a GDPR erasure, peer replicas will keep serving a forgotten subject's"
            + " cached plaintext for up to the cache write-TTL ceiling ({}) — the only cross-replica"
            + " erasure bound. Provide a DataSource on the selected backend's database so its forget"
            + " signal evicts promptly.",
        backend,
        writeTtl);
    return CryptoForgetSignal.NOOP;
  }

  private CachedCryptoEngine wrapWithCache(
      CryptoEngine delegate,
      StreamRuneMicronautCryptoProperties.CacheConfig cache,
      CryptoForgetSignal forgetSignal) {
    var builder =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .maximumSize(cache.maximumSize())
            .forgetSignal(forgetSignal);
    Duration write = cache.expireAfterWrite();
    if (write != null) {
      builder.expireAfterWrite(write);
    }
    Duration access = cache.expireAfterAccess();
    if (access != null) {
      builder.expireAfterAccess(access);
    }
    return builder.build();
  }
}
