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
import org.streamrune.aws.AwsKmsCryptoEngine;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.postgres.JdbcForgottenSubjectStore;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoForgetSignal;
import org.streamrune.filesystem.FileSystemCryptoEngine;
import org.streamrune.vault.VaultCryptoEngine;
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
      @Nullable DataSource dataSource, StreamRuneMicronautCryptoProperties props) {
    // Wire the LISTEN/NOTIFY forget channel when a DataSource is present so a
    // deleteKey on any replica promptly evicts a forgotten subject's cached plaintext on peers.
    return wrapWithCache(
        buildFileSystemDelegate(props),
        props.cache(),
        resolveForgetSignal(dataSource, props.cache().expireAfterWrite(), "filesystem"));
  }

  /** Bare (no-cache) file-system engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.filesystem.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine fileSystemCryptoEngineUncached(StreamRuneMicronautCryptoProperties props) {
    return buildFileSystemDelegate(props);
  }

  @Singleton
  @Requires(property = "streamrune.crypto.postgres.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", notEquals = "false")
  @Bean(preDestroy = "close")
  CachedCryptoEngine postgresCryptoEngine(
      DataSource dataSource, StreamRuneMicronautCryptoProperties props) {
    // A Postgres DataSource is present, so wire the LISTEN/NOTIFY forget channel for
    // prompt cross-replica cache invalidation on deleteKey. Micronaut does NOT auto-close
    // @Factory-produced AutoCloseable beans, so @Bean(preDestroy = "close") is required for
    // the container to stop the listener and release its dedicated connection on context shutdown.
    return wrapWithCache(
        buildPostgresDelegate(dataSource, props),
        props.cache(),
        new PostgresCryptoForgetSignal(dataSource));
  }

  /** Bare (no-cache) Postgres engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.postgres.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine postgresCryptoEngineUncached(
      DataSource dataSource, StreamRuneMicronautCryptoProperties props) {
    return buildPostgresDelegate(dataSource, props);
  }

  /**
   * Auto-wires a durable {@link JdbcForgottenSubjectStore} when a {@link DataSource} is present, so
   * Vault GDPR erasure is terminal (a forgotten subject cannot be re-encrypted) and survives
   * restarts. Without a DataSource there is no durable store, so {@code build()} fails closed
   * rather than shipping non-terminal Vault erasure. Requires the crypto Flyway migration ({@code
   * forgotten_subjects} table).
   */
  @Singleton
  @Requires(property = "streamrune.crypto.vault.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", notEquals = "false")
  @Bean(preDestroy = "close")
  CachedCryptoEngine vaultCryptoEngine(
      StreamRuneVaultHttpClient httpClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props) {
    // The same DataSource wires the LISTEN/NOTIFY forget channel.
    return wrapWithCache(
        buildVaultDelegate(httpClient.client(), dataSource, props),
        props.cache(),
        resolveForgetSignal(dataSource, props.cache().expireAfterWrite(), "vault"));
  }

  /** Bare (no-cache) Vault engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.vault.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine vaultCryptoEngineUncached(
      StreamRuneVaultHttpClient httpClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props) {
    return buildVaultDelegate(httpClient.client(), dataSource, props);
  }

  /**
   * Auto-wires a durable {@link JdbcForgottenSubjectStore} when a {@link DataSource} is present, so
   * AWS-KMS GDPR erasure survives restarts out of the box. Without a DataSource there is no durable
   * store, so {@code build()} fails closed rather than recording in-memory tombstones that vanish
   * on restart. Requires the crypto Flyway migration ({@code forgotten_subjects} table).
   */
  @Singleton
  @Requires(property = "streamrune.crypto.aws.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", notEquals = "false")
  @Bean(preDestroy = "close")
  CachedCryptoEngine awsKmsCryptoEngine(
      StreamRuneKmsClient kmsClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props) {
    // The same DataSource that backs the durable tombstone
    // store also wires the LISTEN/NOTIFY forget channel for prompt cross-replica cache eviction.
    return wrapWithCache(
        buildAwsKmsDelegate(kmsClient.client(), dataSource, props),
        props.cache(),
        resolveForgetSignal(dataSource, props.cache().expireAfterWrite(), "aws-kms"));
  }

  /** Bare (no-cache) AWS-KMS engine when {@code streamrune.crypto.cache.enabled=false}. */
  @Singleton
  @Requires(property = "streamrune.crypto.aws.enabled", value = "true")
  @Requires(property = "streamrune.crypto.cache.enabled", value = "false")
  CryptoEngine awsKmsCryptoEngineUncached(
      StreamRuneKmsClient kmsClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props) {
    return buildAwsKmsDelegate(kmsClient.client(), dataSource, props);
  }

  // ── delegate builders (shared by the cached + bare beans above) ──────────────────────────────

  private CryptoEngine buildFileSystemDelegate(StreamRuneMicronautCryptoProperties props) {
    try {
      return FileSystemCryptoEngine.builder()
          .keyDirectory(Path.of(props.filesystem().keyDirectory()))
          .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build FileSystemCryptoEngine. Check streamrune.crypto.filesystem.key-directory.",
          e);
    }
  }

  private CryptoEngine buildPostgresDelegate(
      DataSource dataSource, StreamRuneMicronautCryptoProperties props) {
    // Honor the configured key-table name (validated as a SQL identifier in the engine
    // builder). Default keeps the migration-created "encryption_keys" table.
    return PostgresCryptoEngine.builder()
        .dataSource(dataSource)
        .tableName(props.postgres().tableName())
        .build();
  }

  private CryptoEngine buildVaultDelegate(
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
      builder.forgottenSubjectStore(new JdbcForgottenSubjectStore(dataSource));
    }
    return builder.build();
  }

  private CryptoEngine buildAwsKmsDelegate(
      KmsClient kmsClient,
      @Nullable DataSource dataSource,
      StreamRuneMicronautCryptoProperties props) {
    var ap = props.aws();
    if (ap.region() == null || ap.region().isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.region must be set when streamrune.crypto.aws.enabled is true");
    }
    if (ap.kmsKeyId() == null || ap.kmsKeyId().isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.kms-key-id must be set when streamrune.crypto.aws.enabled is true");
    }
    var builder = AwsKmsCryptoEngine.builder().kmsClient(kmsClient).kmsKeyId(ap.kmsKeyId());
    if (dataSource != null) {
      builder.forgottenSubjectStore(new JdbcForgottenSubjectStore(dataSource));
    }
    return builder.build();
  }

  /**
   * Resolves the cross-replica forget channel for a non-Postgres backend — the prompt {@link
   * PostgresCryptoForgetSignal} (LISTEN/NOTIFY) when a DataSource is present, else {@link
   * CryptoForgetSignal#NOOP} with a startup WARN naming the cache write-TTL ceiling as the only
   * cross-replica erasure bound. Package-private for direct testing.
   */
  static CryptoForgetSignal resolveForgetSignal(
      @Nullable DataSource dataSource, @Nullable Duration configuredWriteTtl, String backend) {
    if (dataSource != null) {
      return new PostgresCryptoForgetSignal(dataSource);
    }
    Duration writeTtl =
        configuredWriteTtl != null
            ? configuredWriteTtl
            : CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE;
    log.warn(
        "No DataSource available to wire the cross-replica crypto-forget channel for the {} crypto"
            + " backend; after a GDPR erasure, peer replicas will keep serving a forgotten subject's"
            + " cached plaintext for up to the cache write-TTL ceiling ({}) — the only cross-replica"
            + " erasure bound. Provide a Postgres DataSource for prompt (LISTEN/NOTIFY) eviction.",
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
    if (write != null) builder.expireAfterWrite(write);
    Duration access = cache.expireAfterAccess();
    if (access != null) builder.expireAfterAccess(access);
    return builder.build();
  }
}
