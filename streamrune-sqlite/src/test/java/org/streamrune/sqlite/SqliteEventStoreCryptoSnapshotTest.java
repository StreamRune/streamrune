package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandFailureClassification;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.runtime.jdbc.StreamKeys;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * Snapshots with {@code @Encrypted} fields behave exactly as on Postgres (mirrors {@code
 * PostgresEventStoreCryptoSnapshotTest}): a post-forget state with the {@code [REDACTED]} tombstone
 * snapshots again; a crypto failure while reading a snapshot is never discarded as a stale cache, a
 * transient one stays a plain {@link EventStoreException} and a deterministic one is an {@link
 * EventDeserializationException}; a corrupt ciphertext with no crypto failure in its chain is
 * discarded and the stream replayed.
 */
class SqliteEventStoreCryptoSnapshotTest {

  /** Aggregate state carrying one {@code @Encrypted} PII field, keyed off {@code customerId}. */
  record CustomerState(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements AggregateState {

    @JsonCreator
    CustomerState(
        @JsonProperty("customerId") String customerId, @JsonProperty("email") String email) {
      this.customerId = customerId;
      this.email = email;
    }
  }

  /** Nested record with an {@code @Encrypted} field: forces Jackson to WRAP a decrypt failure. */
  record Secret(String subject, @Encrypted(subjectId = "subject") String value) {
    @JsonCreator
    Secret(@JsonProperty("subject") String subject, @JsonProperty("value") String value) {
      this.subject = subject;
      this.value = value;
    }
  }

  record NestedSecretState(String id, Secret secret) implements AggregateState {
    @JsonCreator
    NestedSecretState(@JsonProperty("id") String id, @JsonProperty("secret") Secret secret) {
      this.id = id;
      this.secret = secret;
    }
  }

  /** State whose constructor rejects the {@code [REDACTED]} tombstone a forget substitutes. */
  record ValidatedCustomerState(
      String customerId, @Encrypted(subjectId = "customerId") String email)
      implements AggregateState {

    @JsonCreator
    ValidatedCustomerState(
        @JsonProperty("customerId") String customerId, @JsonProperty("email") String email) {
      if (email != null && !email.contains("@")) {
        throw new IllegalArgumentException("not an email: " + email);
      }
      this.customerId = customerId;
      this.email = email;
    }
  }

  /** Encrypts to identity bytes, so a save works, and fails every decrypt as an outage. */
  static final class DecryptFailingEngine implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return plaintext;
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      throw new CryptoOperationException("KMS transient outage during snapshot decrypt");
    }

    @Override
    public void deleteKey(SubjectId subjectId) {}

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  @TempDir Path dir;

  private SqliteDatabase database;

  @BeforeEach
  void open() {
    database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("crypto-snapshot.db")));
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
  }

  @AfterEach
  void close() {
    database.close();
  }

  private static StreamId stream(String id) {
    return StreamId.of(AggregateType.of("customer"), AggregateId.of(id));
  }

  private static EventTypeRegistry registryFor(Class<?> stateClass) {
    return new EventTypeRegistry() {
      @Override
      public Class<?> resolveEventType(EventType eventType) {
        return Object.class;
      }

      @Override
      public Class<?> resolveStateType(String stateType) {
        return stateClass;
      }

      @Override
      public Collection<Class<?>> registeredTypes() {
        return List.of(stateClass);
      }
    };
  }

  private EventStore store(
      Class<?> stateClass, CryptoEngine engine, List<SnapshotMigration> steps) {
    return new SqliteEventStoreFactory(database, registryFor(stateClass))
        .cryptoEngine(engine)
        .snapshotMigrations(steps)
        .create();
  }

  @Test
  void saveSnapshotSucceedsForPostForgetAggregateWithRedactedField() {
    var cryptoEngine = new InMemoryCryptoEngine();
    EventStore store = store(CustomerState.class, cryptoEngine, List.of());
    StreamId streamId = stream("customer-snapshot-1");

    store.saveSnapshot(
        streamId, new Version(1), new CustomerState("customer-1", "alice@example.com"));
    assertThat(((CustomerState) store.load(streamId).snapshotState()).email())
        .isEqualTo("alice@example.com");

    cryptoEngine.deleteKey(SubjectId.of("customer-1"));
    var redactedState = (CustomerState) store.load(streamId).snapshotState();
    assertThat(redactedState.email()).isEqualTo(CryptoShreddingModule.REDACTED);

    store.saveSnapshot(streamId, new Version(1), redactedState);

    var resnapshotted = (CustomerState) store.load(streamId).snapshotState();
    assertThat(resnapshotted.customerId()).isEqualTo("customer-1");
    assertThat(resnapshotted.email()).isEqualTo(CryptoShreddingModule.REDACTED);
  }

  /** The email is ciphertext in the row: the plaintext never reaches the database file. */
  @Test
  void anEncryptedFieldIsCiphertextAtRest() throws Exception {
    EventStore store = store(CustomerState.class, new InMemoryCryptoEngine(), List.of());
    store.saveSnapshot(
        stream("at-rest"), new Version(1), new CustomerState("c-rest", "secret@example.com"));

    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement();
        var rs = st.executeQuery("SELECT state_payload FROM snapshot_store")) {
      rs.next();
      assertThat(rs.getString(1)).contains("c-rest").doesNotContain("secret@example.com");
    }
  }

  @Test
  void snapshotCryptoDecryptFailurePropagatesAndIsNotDiscarded() {
    EventStore store = store(NestedSecretState.class, new DecryptFailingEngine(), List.of());
    StreamId streamId = stream("nested-secret-1");
    store.saveSnapshot(
        streamId, new Version(1), new NestedSecretState("agg-1", new Secret("subj-1", "x")));

    assertThatThrownBy(() -> store.load(streamId))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasRootCauseInstanceOf(CryptoOperationException.class)
        .satisfies(
            t ->
                assertThat(CommandFailureClassification.BREAKER_ELIGIBLE.test(t))
                    .as("a transient key-store outage carries infrastructure-health evidence")
                    .isTrue());
  }

  @Test
  void postForgetTombstoneRejectionIsTypedDeterministicAndBreakerExcluded() {
    var cryptoEngine = new InMemoryCryptoEngine();
    EventStore store = store(ValidatedCustomerState.class, cryptoEngine, List.of());
    StreamId streamId = stream("customer-validated-1");
    store.saveSnapshot(
        streamId, new Version(1), new ValidatedCustomerState("customer-v1", "bob@example.com"));

    cryptoEngine.deleteKey(SubjectId.of("customer-v1"));

    assertThatThrownBy(() -> store.load(streamId))
        .isInstanceOf(EventDeserializationException.class)
        .satisfies(
            t -> {
              assertThat(CommandFailureClassification.DLQ_ELIGIBLE.test(t)).isTrue();
              assertThat(CommandFailureClassification.BREAKER_ELIGIBLE.test(t)).isFalse();
            });
  }

  private static SnapshotMigration throwingStep(RuntimeException failure) {
    return new SnapshotMigration() {
      @Override
      public int fromVersion() {
        return 1;
      }

      @Override
      public int toVersion() {
        return 2;
      }

      @Override
      public AggregateState migrate(AggregateState state) {
        throw failure;
      }
    };
  }

  @Test
  void migrationDeterministicCryptoFailureIsTypedAndBreakerExcluded() {
    EventStore store =
        store(
            CustomerState.class,
            new InMemoryCryptoEngine(),
            List.of(
                throwingStep(
                    new CryptoMappingException("constructor rejected the [REDACTED] tombstone"))));
    StreamId streamId = stream("customer-migrate-det-1");
    store.saveSnapshot(
        streamId, new Version(1), new CustomerState("customer-m1", "dave@example.com"), 1);

    assertThatThrownBy(() -> store.load(streamId, 2))
        .isInstanceOf(EventDeserializationException.class)
        .satisfies(
            t -> {
              assertThat(CommandFailureClassification.DLQ_ELIGIBLE.test(t)).isTrue();
              assertThat(CommandFailureClassification.BREAKER_ELIGIBLE.test(t)).isFalse();
            });
  }

  @Test
  void migrationTransientCryptoFailureStaysPlainInfrastructure() {
    EventStore store =
        store(
            CustomerState.class,
            new InMemoryCryptoEngine(),
            List.of(
                throwingStep(
                    new RuntimeException(
                        "step failed", new CryptoOperationException("KMS outage in step")))));
    StreamId streamId = stream("customer-migrate-trans-1");
    store.saveSnapshot(
        streamId, new Version(1), new CustomerState("customer-m2", "erin@example.com"), 1);

    assertThatThrownBy(() -> store.load(streamId, 2))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasRootCauseInstanceOf(CryptoOperationException.class)
        .satisfies(t -> assertThat(CommandFailureClassification.BREAKER_ELIGIBLE.test(t)).isTrue());
  }

  @Test
  void rootLevelCorruptCiphertextSnapshotDiscardsAndReplays() throws Exception {
    EventStore store = store(CustomerState.class, new InMemoryCryptoEngine(), List.of());
    StreamId streamId = stream("customer-corrupt-b64");
    store.saveSnapshot(
        streamId, new Version(1), new CustomerState("customer-b64", "carol@example.com"));

    // '#' is not a Base64 character.
    try (Connection conn = database.dataSource().getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE snapshot_store SET state_payload = json_set(state_payload, '$.email',"
                    + " '###corrupt###') WHERE aggregate_type = ? AND aggregate_id = ?")) {
      StreamKeys.bind(ps, 1, streamId);
      assertThat(ps.executeUpdate()).isEqualTo(1);
    }

    AggregateHistory history = store.load(streamId);
    assertThat(history.snapshotState()).isNull();
    assertThat(history.events()).isEmpty();
    assertThat(history.version()).isEqualTo(Version.initial());
  }
}
