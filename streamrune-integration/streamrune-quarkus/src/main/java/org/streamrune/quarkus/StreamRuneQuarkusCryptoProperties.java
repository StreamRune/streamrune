package org.streamrune.quarkus;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Optional;

/**
 * Configuration properties for StreamRune crypto engine auto-configuration in Quarkus applications.
 *
 * <p>Maps to the {@code streamrune.crypto.} configuration prefix. Each backing-engine section is
 * gated by an {@code enabled} flag — set to {@code true} to activate that engine.
 */
@ConfigMapping(prefix = "streamrune.crypto")
public interface StreamRuneQuarkusCryptoProperties {

  /** File-system backing store config (stores keys as files on disk). */
  FileSystemConfig filesystem();

  /** PostgreSQL backing store config (stores keys in a database table). */
  PostgresConfig postgres();

  /** HashiCorp Vault backing store config (uses Vault Transit engine). */
  VaultConfig vault();

  /** AWS KMS backing store config (uses AWS KMS for key management). */
  AwsConfig aws();

  /** Shared caching config applied to whichever engine is active. */
  CacheConfig cache();

  /** File-system engine configuration. */
  interface FileSystemConfig {
    @WithDefault("false")
    boolean enabled();

    /**
     * Directory in which per-subject AES-256 keys are stored. Key: {@code
     * streamrune.crypto.filesystem.key-directory}, shared with the Spring and Micronaut
     * integrations.
     */
    @WithDefault("/var/lib/streamrune/keys")
    String keyDirectory();
  }

  /** PostgreSQL engine configuration. */
  interface PostgresConfig {
    @WithDefault("false")
    boolean enabled();

    /**
     * Key-table name (default {@code encryption_keys}), shared with the Spring and Micronaut
     * integrations ({@code streamrune.crypto.postgres.table-name}). Must be a plain SQL identifier;
     * the engine rejects anything else at construction. The shipped Flyway migration always creates
     * the default-named table, so a custom name requires the operator to create that table.
     */
    @WithDefault("encryption_keys")
    String tableName();
  }

  /** HashiCorp Vault engine configuration. */
  interface VaultConfig {
    @WithDefault("false")
    boolean enabled();

    /** Base URL of the Vault server (e.g. {@code http://localhost:8200}). */
    @WithDefault("http://localhost:8200")
    String address();

    /**
     * Vault token with permission to call the transit engine. Should be sourced from {@code
     * ${VAULT_TOKEN}} or a secrets mount. Required when {@code enabled=true}; optional in the
     * mapping so that applications not using Vault start without it.
     */
    Optional<String> token();

    /** Mount point of the Vault transit engine (default: {@code transit}). */
    @WithDefault("transit")
    String engineMount();
  }

  /** AWS KMS engine configuration. */
  interface AwsConfig {
    @WithDefault("false")
    boolean enabled();

    /**
     * AWS region for the KMS client (e.g. {@code eu-central-1}). Required when {@code
     * enabled=true}.
     */
    Optional<String> region();

    /**
     * ARN or ID of the KMS symmetric key used for encrypt/decrypt operations. Required when {@code
     * enabled=true}.
     */
    @WithName("kms-key-id")
    Optional<String> kmsKeyId();

    /**
     * The engine's cache of plaintext data keys ({@code streamrune.crypto.aws.data-key-cache.*});
     * off whenever {@code streamrune.crypto.cache.enabled=false}.
     */
    @WithName("data-key-cache")
    DataKeyCacheConfig dataKeyCache();

    /** The AWS KMS engine's data-key cache. */
    interface DataKeyCacheConfig {
      /** Entries per data-key map; {@code 0} disables the data-key cache. */
      @WithDefault("1000")
      int maximumSize();

      /**
       * How long a cached data key lives after it was stored (default five minutes); must be
       * positive. A duration like every other StreamRune duration: {@code PT5M}, or {@code 5m} in a
       * Quarkus application.
       */
      @WithDefault("PT5M")
      Duration expireAfterWrite();
    }
  }

  /** Caching layer applied over the active crypto engine. */
  interface CacheConfig {
    /** Whether caching is enabled. When {@code false}, no cache wrapper is applied. */
    @WithDefault("true")
    boolean enabled();

    /** Maximum number of decrypted entries held in the cache. */
    @WithDefault("10000")
    int maximumSize();

    /**
     * Time after which an entry is expired, counted from when it was written (default five minutes,
     * the {@link org.streamrune.crypto.CachedCryptoEngine#DEFAULT_EXPIRE_AFTER_WRITE} ceiling the
     * engine applies without one). A duration: {@code PT5M}, or {@code 5m} in a Quarkus
     * application.
     */
    @WithDefault("PT5M")
    Duration expireAfterWrite();

    /**
     * Time after which an entry is expired, counted from its last access; unset, entries expire by
     * write age only. A duration: {@code PT10M}, or {@code 10m} in a Quarkus application.
     */
    Optional<Duration> expireAfterAccess();
  }
}
