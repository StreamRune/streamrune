package org.streamrune.micronaut;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.bind.annotation.Bindable;
import java.time.Duration;

/**
 * Immutable configuration properties for the StreamRune crypto engine in Micronaut applications.
 *
 * <p>Maps to the {@code streamrune.crypto.} configuration prefix. Like {@link
 * StreamRuneMicronautProperties} this is a record that Micronaut binds through its canonical
 * constructor, with {@link Bindable#defaultValue() defaults} applied for every key that is not set;
 * there are no setters and no way to change a value after binding. Each nested group is itself an
 * immutable record bound under its own prefix.
 *
 * @param cache cache configuration ({@code streamrune.crypto.cache.*})
 * @param filesystem file system key store configuration ({@code streamrune.crypto.filesystem.*})
 * @param postgres PostgreSQL key store configuration ({@code streamrune.crypto.postgres.*})
 * @param vault HashiCorp Vault configuration ({@code streamrune.crypto.vault.*})
 * @param aws AWS KMS configuration ({@code streamrune.crypto.aws.*})
 */
@ConfigurationProperties("streamrune.crypto")
public record StreamRuneMicronautCryptoProperties(
    CacheConfig cache,
    FileSystemConfig filesystem,
    PostgresConfig postgres,
    VaultConfig vault,
    AwsConfig aws) {

  /**
   * Cache configuration for crypto engine key caching. Binds {@code streamrune.crypto.cache.*}.
   *
   * @param enabled when {@code false} the decrypted-plaintext cache wrapper ({@link
   *     org.streamrune.crypto.CachedCryptoEngine}) is not applied, so no decrypted PII is retained
   *     in heap — the deliberate no-cache GDPR posture, shared with the Spring and Quarkus
   *     integrations; default {@code true}
   * @param maximumSize the maximum cache size
   * @param expireAfterWrite the duration after which written entries expire (default 5 minutes)
   * @param expireAfterAccess the duration after which accessed entries expire; {@code null} means
   *     no access-based expiry (the default)
   */
  @ConfigurationProperties("cache")
  public record CacheConfig(
      @Bindable(defaultValue = "true") boolean enabled,
      @Bindable(defaultValue = "10000") int maximumSize,
      @Bindable(defaultValue = "5m") Duration expireAfterWrite,
      @Nullable Duration expireAfterAccess) {}

  /**
   * File system key store configuration. Binds {@code streamrune.crypto.filesystem.*}.
   *
   * @param keyDirectory the directory path for encryption keys
   */
  @ConfigurationProperties("filesystem")
  public record FileSystemConfig(
      @Bindable(defaultValue = "/var/lib/streamrune/keys") String keyDirectory) {}

  /**
   * PostgreSQL key store configuration. Binds {@code streamrune.crypto.postgres.*}.
   *
   * @param tableName the key-table name (default {@code encryption_keys}), shared with the Spring
   *     and Quarkus integrations ({@code streamrune.crypto.postgres.table-name}). Must be a plain
   *     SQL identifier; the engine rejects anything else at construction. The shipped Flyway
   *     migration always creates the default-named table, so a custom name requires the operator to
   *     create that table.
   */
  @ConfigurationProperties("postgres")
  public record PostgresConfig(@Bindable(defaultValue = "encryption_keys") String tableName) {}

  /**
   * HashiCorp Vault key store configuration. Binds {@code streamrune.crypto.vault.*}.
   *
   * @param address the Vault server address
   * @param token the Vault authentication token; required when the Vault backend is enabled
   * @param engineMount the Vault transit engine mount path
   */
  @ConfigurationProperties("vault")
  public record VaultConfig(
      @Bindable(defaultValue = "http://localhost:8200") String address,
      @Nullable String token,
      @Bindable(defaultValue = "transit") String engineMount) {}

  /**
   * AWS KMS key store configuration. Binds {@code streamrune.crypto.aws.*}.
   *
   * @param region the AWS region; required when the AWS KMS backend is enabled
   * @param kmsKeyId the AWS KMS key ID; required when the AWS KMS backend is enabled
   */
  @ConfigurationProperties("aws")
  public record AwsConfig(@Nullable String region, @Nullable String kmsKeyId) {}
}
