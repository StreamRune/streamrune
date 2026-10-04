package org.streamrune.spring;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import org.streamrune.aws.AwsKmsCryptoEngine;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.postgres.JdbcForgottenSubjectStore;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;

@Configuration
@ConditionalOnClass(CryptoEngine.class)
@ConditionalOnProperty(prefix = "streamrune.crypto.aws", name = "enabled", havingValue = "true")
public class AwsKmsCryptoEngineConfiguration {

  /**
   * The framework's own KMS client, wrapped in a StreamRune-owned holder type.
   *
   * <p>This client is a bean so the container closes its connection pool and idle-connection-reaper
   * thread on shutdown. Registering it as a plain {@code @Bean KmsClient} broke every
   * AWS-integrated application that already owns a {@code KmsClient} bean the moment the operator
   * set {@code streamrune.crypto.aws.enabled=true} — a {@code NoUniqueBeanDefinitionException}, or
   * (when the app's bean was also named {@code kmsClient}, the obvious name) a {@code
   * BeanDefinitionOverrideException} that silently replaced one of the two.
   *
   * <p>Holding the client inside a type only StreamRune declares keeps the shutdown-close property
   * while keeping the framework out of by-type resolution of {@code KmsClient} entirely. See {@link
   * VaultCryptoEngineConfiguration#streamRuneVaultHttpClient()} for the full rationale, including
   * why {@code @ConditionalOnMissingBean} is the wrong direction.
   */
  @Bean
  StreamRuneKmsClient streamRuneKmsClient(StreamRuneCryptoProperties props) {
    String region = props.aws().region();
    if (region == null || region.isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.region must be set when streamrune.crypto.aws.enabled is true");
    }
    return new StreamRuneKmsClient(KmsClient.builder().region(Region.of(region)).build());
  }

  /**
   * StreamRune-owned holder for the AWS {@link KmsClient}. Package-private: it is wiring, not API.
   */
  record StreamRuneKmsClient(KmsClient client) implements AutoCloseable {
    @Override
    public void close() {
      client.close();
    }
  }

  /**
   * Builds the KMS-backed engine. AWS-KMS erasure is durable only if a persistent {@link
   * org.streamrune.crypto.ForgottenSubjectStore} is wired. When a {@link DataSource} is present, a
   * durable {@link JdbcForgottenSubjectStore} is auto-wired so KMS erasure survives restarts out of
   * the box; when no DataSource is available, {@code build()} fails closed (no silent in-memory
   * tombstone). Requires the crypto Flyway migration ({@code forgotten_subjects} table) on that
   * DataSource's database.
   */
  // Inferred destroy method (not explicit destroyMethod = "close"). Cache enabled → the bean
  // is an AutoCloseable CachedCryptoEngine and is still closed on shutdown; cache.enabled=false →
  // the bare AwsKmsCryptoEngine has no close() and inference skips it (an explicit
  // destroyMethod = "close" would fail bean creation with "Invalid destruction signature").
  @Bean
  CryptoEngine awsKmsCryptoEngine(
      StreamRuneKmsClient kmsClient,
      @Nullable DataSource dataSource,
      StreamRuneCryptoProperties props) {
    String kmsKeyId = props.aws().kmsKeyId();
    if (kmsKeyId == null || kmsKeyId.isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.kms-key-id must be set when streamrune.crypto.aws.enabled is true");
    }
    var builder = AwsKmsCryptoEngine.builder().kmsClient(kmsClient.client()).kmsKeyId(kmsKeyId);
    if (dataSource != null) {
      builder.forgottenSubjectStore(new JdbcForgottenSubjectStore(dataSource));
    }
    // No DataSource → no durable store → build() fails closed rather than silently recording
    // in-memory tombstones that would resurrect erased PII on restart.
    var delegate = builder.build();

    var cacheProps = props.cache();
    // Honor streamrune.crypto.cache.enabled=false (parity with Quarkus) — return the bare
    // engine so no decrypted PII is retained in heap (the deliberate no-cache GDPR posture).
    if (!cacheProps.enabled()) {
      return delegate;
    }
    // The DataSource that backs the durable tombstone store
    // also wires the LISTEN/NOTIFY forget channel, so a deleteKey on any replica promptly evicts
    // the forgotten subject's cached plaintext on every peer (not just via the write-TTL ceiling).
    return CachedCryptoEngine.builder()
        .delegate(delegate)
        .maximumSize(cacheProps.maximumSize())
        .expireAfterWrite(cacheProps.expireAfterWrite())
        .expireAfterAccess(cacheProps.expireAfterAccess())
        .forgetSignal(
            SpringCryptoForgetSignals.resolve(dataSource, cacheProps.expireAfterWrite(), "aws-kms"))
        .build();
  }
}
