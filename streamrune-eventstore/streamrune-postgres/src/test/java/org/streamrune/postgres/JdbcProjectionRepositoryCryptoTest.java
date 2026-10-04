package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * {@code @Encrypted} on a projection read model must not be a silent plaintext no-op with the
 * framework's own repository. The GDPR gap is that the auto-configured {@link
 * JdbcProjectionRepository} built a JavaTime-only {@code ObjectMapper} with no {@code
 * CryptoShreddingModule}, so PII on a read model was written to the {@code data} column as
 * plaintext, GDPR-forget never reached it, and nothing logged or failed.
 *
 * <p>This test pins both the residual and the fix: the crypto-BLIND convenience constructor still
 * writes plaintext (documented residual, symmetric with the saga store's crypto-blind path), while
 * the crypto-AWARE factory encrypts the column and a subsequent forget redacts it.
 */
@Testcontainers
class JdbcProjectionRepositoryCryptoTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_proj_crypto");

  /** A read model carrying one {@code @Encrypted} PII field keyed off {@code customerId}. */
  record CustomerView(String customerId, @Encrypted(subjectId = "customerId") String email) {
    @JsonCreator
    CustomerView(
        @JsonProperty("customerId") String customerId, @JsonProperty("email") String email) {
      this.customerId = customerId;
      this.email = email;
    }
  }

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void setup() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  /** Reads the raw JSONB {@code data} column of a projection view table, bypassing the mapper. */
  private String rawColumn(ProjectionName projection, String id) throws Exception {
    String table = projection.value().replaceAll("[^a-zA-Z0-9_]", "_") + "_view";
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT data::text FROM " + table + " WHERE id = ?")) {
      ps.setString(1, id);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getString(1);
      }
    }
  }

  @Test
  void cryptoBlindDefaultRepository_writesEncryptedFieldAsPlaintext_andForgetLeavesIt()
      throws Exception {
    // The repro: the auto-configured / convenience repository (no CryptoEngine) persists the
    // @Encrypted field as READABLE PLAINTEXT — GDPR-forget can never reach a plaintext column.
    var engine = new InMemoryCryptoEngine();
    var repo = new JdbcProjectionRepository(dataSource);
    var projection = ProjectionName.of("blind_customer");

    repo.save(projection, "c-blind", new CustomerView("c-blind", "alice@example.com"));

    // The email is stored verbatim in the column — nothing was encrypted.
    assertThat(rawColumn(projection, "c-blind")).contains("alice@example.com");

    // Forget (delete the subject's key) leaves the plaintext untouched — the read model is OUTSIDE
    // GDPR forget scope, the marketed contract.
    engine.deleteKey(SubjectId.of("c-blind"));
    assertThat(rawColumn(projection, "c-blind")).contains("alice@example.com");

    var read = repo.findById(projection, "c-blind", CustomerView.class).orElseThrow();
    assertThat(read.email()).isEqualTo("alice@example.com");
  }

  @Test
  void cryptoAwareRepository_encryptsColumn_andForgetRedacts() throws Exception {
    // The fix: constructed via the new crypto-aware factory (mirroring
    // PostgresSagaStore.createObjectMapper), the SAME repository encrypts the @Encrypted field at
    // rest and GDPR-forget crypto-shreds it to the [REDACTED] tombstone.
    var engine = new InMemoryCryptoEngine();
    var repo =
        new JdbcProjectionRepository(
            dataSource,
            JdbcProjectionRepository.createObjectMapper(
                engine, org.streamrune.core.StreamRuneMetrics.NOOP));
    var projection = ProjectionName.of("secure_customer");

    repo.save(projection, "c-secure", new CustomerView("c-secure", "bob@example.com"));

    // The plaintext PII is NOT in the column — it is ciphertext at rest.
    assertThat(rawColumn(projection, "c-secure")).doesNotContain("bob@example.com");

    // Pre-forget it round-trips through decryption.
    assertThat(repo.findById(projection, "c-secure", CustomerView.class).orElseThrow().email())
        .isEqualTo("bob@example.com");

    // GDPR forget: deleting the key makes the ciphertext undecryptable → the read model now reads
    // as the [REDACTED] tombstone. Forget reached the read model via crypto-shredding.
    engine.deleteKey(SubjectId.of("c-secure"));
    assertThat(repo.findById(projection, "c-secure", CustomerView.class).orElseThrow().email())
        .isEqualTo(CryptoShreddingModule.REDACTED);
  }

  @Test
  void factoryCreatedRepository_encryptsColumn_andForgetRedacts() throws Exception {
    // An earlier change wired the optional CryptoEngine into the three
    // AUTO-CONFIG projection-repository beans, but the factory's own createProjectionRepository()
    // — a documented, public path — still returned the crypto-BLIND 1-arg repository even though
    // the factory HOLDS a configured CryptoEngine. So an @Encrypted read-model field built through
    // the factory was written to the data column as PLAINTEXT PII and GDPR-forget never reached it
    // — the exact same gap, on the sibling path. The factory must build the repository's mapper
    // from its own cryptoEngine (and metrics), mirroring the three auto-configs.
    var engine = new InMemoryCryptoEngine();
    var factory =
        new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
            .cryptoEngine(engine);
    var repo = factory.createProjectionRepository();
    var projection = ProjectionName.of("factory_customer");

    repo.save(projection, "c-factory", new CustomerView("c-factory", "carol@example.com"));

    // The plaintext PII must NOT be in the column — it is ciphertext at rest.
    assertThat(rawColumn(projection, "c-factory")).doesNotContain("carol@example.com");

    // Pre-forget it round-trips through decryption.
    assertThat(repo.findById(projection, "c-factory", CustomerView.class).orElseThrow().email())
        .isEqualTo("carol@example.com");

    // GDPR forget reaches the factory-built read model too: deleting the key redacts it.
    engine.deleteKey(SubjectId.of("c-factory"));
    assertThat(repo.findById(projection, "c-factory", CustomerView.class).orElseThrow().email())
        .isEqualTo(CryptoShreddingModule.REDACTED);
  }
}
