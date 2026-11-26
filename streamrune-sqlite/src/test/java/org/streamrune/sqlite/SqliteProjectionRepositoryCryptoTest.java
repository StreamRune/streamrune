package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * {@code @Encrypted} on a read model is encrypted at rest when the repository's mapper is the
 * crypto-aware one, and GDPR forget reaches it through crypto-shredding, outside a batch and inside
 * one; a crypto-blind mapper writes it as plaintext, the documented residual.
 */
class SqliteProjectionRepositoryCryptoTest {

  /** A read model carrying one {@code @Encrypted} PII field keyed off {@code customerId}. */
  record CustomerView(String customerId, @Encrypted(subjectId = "customerId") String email) {
    @JsonCreator
    CustomerView(
        @JsonProperty("customerId") String customerId, @JsonProperty("email") String email) {
      this.customerId = customerId;
      this.email = email;
    }
  }

  @TempDir Path dir;

  private SqliteDatabase database;

  @BeforeEach
  void open() {
    database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("crypto.db")));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
  }

  @AfterEach
  void close() {
    database.close();
  }

  private SqliteProjectionRepository repository(InMemoryCryptoEngine engine) {
    return new SqliteProjectionRepository(
        database,
        SqliteProjectionRepository.createObjectMapper(engine, StreamRuneMetrics.NOOP),
        new SqliteReadModelCheckpoints(database));
  }

  /** The raw {@code data} column, bypassing the mapper. */
  private String rawColumn(ProjectionName projection, String id) throws Exception {
    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT data FROM " + projection.value() + "_view WHERE id = ?")) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getString(1);
      }
    }
  }

  @Test
  void cryptoBlindRepository_writesEncryptedFieldAsPlaintext_andForgetLeavesIt() throws Exception {
    var engine = new InMemoryCryptoEngine();
    var repo = repository(null);
    var projection = ProjectionName.of("blind_customer");

    repo.save(projection, "c-blind", new CustomerView("c-blind", "alice@example.com"));

    assertThat(rawColumn(projection, "c-blind")).contains("alice@example.com");
    engine.deleteKey(SubjectId.of("c-blind"));
    assertThat(rawColumn(projection, "c-blind")).contains("alice@example.com");
    assertThat(repo.findById(projection, "c-blind", CustomerView.class).orElseThrow().email())
        .isEqualTo("alice@example.com");
  }

  @Test
  void cryptoAwareRepository_encryptsColumn_andForgetRedacts() throws Exception {
    var engine = new InMemoryCryptoEngine();
    var repo = repository(engine);
    var projection = ProjectionName.of("secure_customer");

    repo.save(projection, "c-secure", new CustomerView("c-secure", "bob@example.com"));

    assertThat(rawColumn(projection, "c-secure")).doesNotContain("bob@example.com");
    assertThat(repo.findById(projection, "c-secure", CustomerView.class).orElseThrow().email())
        .isEqualTo("bob@example.com");
    engine.deleteKey(SubjectId.of("c-secure"));
    assertThat(repo.findById(projection, "c-secure", CustomerView.class).orElseThrow().email())
        .isEqualTo(CryptoShreddingModule.REDACTED);
  }

  /** The repository handed to a batch serialises with the same crypto-aware mapper. */
  @Test
  void aBatchWritesThroughTheCryptoAwareMapperToo() throws Exception {
    var engine = new InMemoryCryptoEngine();
    var repo = repository(engine);
    var projection = ProjectionName.of("batch_customer");

    repo.executeAtomically(
        projection,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> {
          tx.save(projection, "c-batch", new CustomerView("c-batch", "carol@example.com"));
          assertThat(tx.findById(projection, "c-batch", CustomerView.class).orElseThrow().email())
              .isEqualTo("carol@example.com");
        },
        new SqliteReadModelCheckpoints(database));

    assertThat(rawColumn(projection, "c-batch")).doesNotContain("carol@example.com");
    engine.deleteKey(SubjectId.of("c-batch"));
    assertThat(repo.findById(projection, "c-batch", CustomerView.class).orElseThrow().email())
        .isEqualTo(CryptoShreddingModule.REDACTED);
  }
}
