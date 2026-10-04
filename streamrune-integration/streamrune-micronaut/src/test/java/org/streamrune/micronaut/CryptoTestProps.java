package org.streamrune.micronaut;

import java.time.Duration;

/**
 * Builds the immutable {@link StreamRuneMicronautCryptoProperties} for unit tests that call the
 * factory methods directly. Every field starts at its bound default.
 */
final class CryptoTestProps {

  private boolean cacheEnabled = true;
  private int cacheMaximumSize = 10_000;
  private Duration cacheExpireAfterWrite = Duration.ofMinutes(5);
  private Duration cacheExpireAfterAccess;
  private String keyDirectory = "/var/lib/streamrune/keys";
  private String tableName = "encryption_keys";
  private String vaultAddress = "http://localhost:8200";
  private String vaultToken;
  private String vaultEngineMount = "transit";
  private String awsRegion;
  private String awsKmsKeyId;

  private CryptoTestProps() {}

  static CryptoTestProps builder() {
    return new CryptoTestProps();
  }

  CryptoTestProps cacheEnabled(boolean v) {
    this.cacheEnabled = v;
    return this;
  }

  CryptoTestProps cacheExpireAfterWrite(Duration v) {
    this.cacheExpireAfterWrite = v;
    return this;
  }

  CryptoTestProps cacheExpireAfterAccess(Duration v) {
    this.cacheExpireAfterAccess = v;
    return this;
  }

  CryptoTestProps keyDirectory(String v) {
    this.keyDirectory = v;
    return this;
  }

  CryptoTestProps vaultAddress(String v) {
    this.vaultAddress = v;
    return this;
  }

  CryptoTestProps vaultToken(String v) {
    this.vaultToken = v;
    return this;
  }

  CryptoTestProps vaultEngineMount(String v) {
    this.vaultEngineMount = v;
    return this;
  }

  CryptoTestProps awsRegion(String v) {
    this.awsRegion = v;
    return this;
  }

  CryptoTestProps awsKmsKeyId(String v) {
    this.awsKmsKeyId = v;
    return this;
  }

  StreamRuneMicronautCryptoProperties build() {
    return new StreamRuneMicronautCryptoProperties(
        new StreamRuneMicronautCryptoProperties.CacheConfig(
            cacheEnabled, cacheMaximumSize, cacheExpireAfterWrite, cacheExpireAfterAccess),
        new StreamRuneMicronautCryptoProperties.FileSystemConfig(keyDirectory),
        new StreamRuneMicronautCryptoProperties.PostgresConfig(tableName),
        new StreamRuneMicronautCryptoProperties.VaultConfig(
            vaultAddress, vaultToken, vaultEngineMount),
        new StreamRuneMicronautCryptoProperties.AwsConfig(awsRegion, awsKmsKeyId));
  }
}
