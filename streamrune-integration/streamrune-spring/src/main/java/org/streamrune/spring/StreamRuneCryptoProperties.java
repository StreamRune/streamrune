package org.streamrune.spring;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Immutable crypto configuration under the {@code streamrune.crypto} prefix.
 *
 * <p>Bound via constructor binding, exactly like {@link StreamRuneProperties}: each record has one
 * (canonical) constructor and declares its defaults with {@link DefaultValue}. There are no setters
 * and no way to change a value after binding. Do not add extra constructors — Spring Boot cannot
 * deduce a bind constructor for a record with several and would silently fall back to JavaBean
 * binding, which ignores every configured value.
 *
 * <p>The per-backend {@code streamrune.crypto.<backend>.enabled} switches are not components: they
 * are read only through {@code @ConditionalOnProperty} on the backend configurations.
 *
 * @param cache crypto cache configuration
 * @param filesystem file system key store configuration
 * @param postgres PostgreSQL key store configuration
 * @param vault HashiCorp Vault configuration
 * @param aws AWS KMS configuration
 */
@ConfigurationProperties(prefix = "streamrune.crypto")
public record StreamRuneCryptoProperties(
    @DefaultValue Cache cache,
    @DefaultValue FileSystem filesystem,
    @DefaultValue Postgres postgres,
    @DefaultValue Vault vault,
    @DefaultValue Aws aws) {

  /**
   * @param enabled when {@code false} the decrypted-plaintext cache wrapper ({@code
   *     CachedCryptoEngine}) is not applied, so no decrypted PII is retained in heap — the
   *     deliberate no-cache GDPR posture, shared with the Micronaut and Quarkus integrations
   * @param maximumSize the maximum cache size
   * @param expireAfterWrite the duration after which written entries expire
   * @param expireAfterAccess the duration after which accessed entries expire; {@code null}
   *     disables access-based expiry (the default)
   */
  public record Cache(
      @DefaultValue("true") boolean enabled,
      @DefaultValue("10000") int maximumSize,
      @DefaultValue("5m") Duration expireAfterWrite,
      Duration expireAfterAccess) {}

  /**
   * @param keyDirectory the directory path for encryption keys
   */
  public record FileSystem(@DefaultValue("/var/lib/streamrune/keys") String keyDirectory) {}

  /**
   * @param tableName the key-table name; must be a plain SQL identifier (the engine rejects
   *     anything else at construction). The shipped migration always creates the default-named
   *     table, so a custom name requires the operator to create that table
   */
  public record Postgres(@DefaultValue("encryption_keys") String tableName) {}

  /**
   * @param address the Vault server address
   * @param token the Vault authentication token; required when the Vault backend is enabled
   * @param engineMount the Vault transit engine mount path
   */
  public record Vault(
      @DefaultValue("http://localhost:8200") String address,
      String token,
      @DefaultValue("transit") String engineMount) {}

  /**
   * @param region the AWS region; required when the AWS KMS backend is enabled
   * @param kmsKeyId the AWS KMS key id; required when the AWS KMS backend is enabled
   */
  public record Aws(String region, String kmsKeyId) {}
}
