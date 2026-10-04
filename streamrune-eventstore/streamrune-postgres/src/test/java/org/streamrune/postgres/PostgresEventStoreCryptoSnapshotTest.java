package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandFailureClassification;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Regression test for Defect 6: after a subject is crypto-shredded (GDPR forget), snapshotting an
 * aggregate whose {@code @Encrypted} field already carries the {@code [REDACTED]} tombstone must
 * SUCCEED instead of failing forever.
 *
 * <p>Before the fix, {@link PostgresEventStore#saveSnapshot} serialized the state via {@code
 * objectMapper.writeValueAsString(state)}, which routed the already-redacted field through {@code
 * CryptoShreddingModule}'s {@code EncryptingPropertyWriter}. That writer always called {@code
 * cryptoEngine.encrypt(...)}, which throws {@code SubjectForgottenException} on a deleted key —
 * propagating out of {@code saveSnapshot} and permanently preventing that aggregate from ever being
 * snapshotted again (every load replays the full event stream from scratch). The fix lets the
 * property writer recognize its own tombstone marker and write it through literally.
 */
@Testcontainers
class PostgresEventStoreCryptoSnapshotTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

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

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return CustomerState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            // CustomerState (below) genuinely is the resolved state type here — it is what
            // PostgresEventStore deserializes snapshots into (see the @Test method). Report
            // it for real instead of inheriting the throwing default, which means the @Encrypted
            // startup guard now correctly sees CustomerState.email and requires a CryptoEngine —
            // see the .cryptoEngine(...) added below.
            return java.util.List.of(CustomerState.class);
          }
        };
    // Run Flyway migrations via the factory, same pattern as PostgresSagaStoreCryptoShreddingTest.
    // A CryptoEngine must be wired here too: typeRegistry now truthfully reports
    // CustomerState (which has an @Encrypted field), so CryptoConfigValidator requires a non-null
    // engine at this factory.create() call, exactly as it would for a real application with this
    // schema. InMemoryCryptoEngine needs no crypto migration tables (requiredCryptoTables() is
    // empty), so this doesn't change schema provisioning.
    new PostgresEventStoreFactory(dataSource, typeRegistry)
        .cryptoEngine(new InMemoryCryptoEngine())
        .create();
  }

  @BeforeEach
  void cleanTables() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM snapshot_store");
      stmt.execute("DELETE FROM event_stream");
    }
  }

  @Test
  void saveSnapshotSucceedsForPostForgetAggregateWithRedactedField() throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .cryptoEngine(cryptoEngine)
            .build();

    var streamId = TestStreams.stream("customer-snapshot-1");
    var state = new CustomerState("customer-1", "alice@example.com");

    // Pre-forget: snapshotting with real PII succeeds and round-trips through decryption.
    store.saveSnapshot(streamId, new Version(1), state);
    AggregateHistory beforeForget = store.load(streamId);
    assertThat(((CustomerState) beforeForget.snapshotState()).email())
        .isEqualTo("alice@example.com");

    // Forget the subject (GDPR erasure).
    cryptoEngine.deleteKey(SubjectId.of("customer-1"));

    // Loading now yields the [REDACTED] tombstone for the encrypted field.
    AggregateHistory afterForget = store.load(streamId);
    var redactedState = (CustomerState) afterForget.snapshotState();
    assertThat(redactedState.email()).isEqualTo(CryptoShreddingModule.REDACTED);

    // The regression: re-snapshotting that already-redacted state must succeed instead of
    // throwing SubjectForgottenException forever.
    store.saveSnapshot(streamId, new Version(1), redactedState);

    AggregateHistory afterResnapshot = store.load(streamId);
    var resnapshotted = (CustomerState) afterResnapshot.snapshotState();
    assertThat(resnapshotted.customerId()).isEqualTo("customer-1");
    assertThat(resnapshotted.email()).isEqualTo(CryptoShreddingModule.REDACTED);
  }

  // ---- Fail-closed guard: a crypto decrypt failure must NEVER be swallowed by the new
  // discard-and-replay path (which is exclusively for a plain Jackson/IO deserialize failure). ----

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

  /**
   * Encrypts to identity bytes (so save works) but fails every decrypt with a transient crypto
   * error.
   */
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

  @Test
  void snapshotCryptoDecryptFailurePropagatesAndIsNotDiscarded() {
    // A decrypt failure on a NESTED @Encrypted field is wrapped by Jackson into a
    // JsonMappingException (IOException) — the SAME exception type the discard catch sees for a
    // plain unmappable payload. Fail-closed requires it to still PROPAGATE (a corrupt/undecryptable
    // ciphertext must never masquerade as a stale cache and be silently dropped).
    EventTypeRegistry nestedRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return NestedSecretState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of(NestedSecretState.class);
          }
        };
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(nestedRegistry)
            .cryptoEngine(new DecryptFailingEngine())
            .build();

    var streamId = TestStreams.stream("nested-secret-1");
    store.saveSnapshot(
        streamId,
        new Version(1),
        new NestedSecretState("agg-1", new Secret("subj-1", "top-secret")));

    assertThatThrownBy(() -> store.load(streamId))
        .satisfies(
            t -> {
              boolean cryptoInChain = false;
              for (Throwable c = t; c != null; c = c.getCause()) {
                if (c instanceof CryptoOperationException) {
                  cryptoInChain = true;
                  break;
                }
              }
              assertThat(cryptoInChain)
                  .as("crypto decrypt failure must propagate, not be swallowed by snapshot-discard")
                  .isTrue();
              // Guard: a TRANSIENT key-store outage is infrastructure evidence — it must
              // stay the PLAIN EventStoreException (never the deterministic
              // EventDeserializationException subtype), so the circuit breaker still counts it.
              assertThat(t).isExactlyInstanceOf(EventStoreException.class);
              assertThat(CommandFailureClassification.BREAKER_ELIGIBLE.test(t))
                  .as("a transient key-store outage carries infrastructure-health evidence")
                  .isTrue();
            });
  }

  // ---- A DETERMINISTIC crypto failure in the snapshot path (CryptoMappingException —
  // e.g. a record constructor rejecting the post-forget [REDACTED] tombstone) must surface as the
  // typed EventDeserializationException, exactly like the event path (toEnvelope). It still
  // PROPAGATES (fail-closed preserved: never discarded as a stale cache), but classifies as a
  // deterministic per-aggregate stored-data failure: DLQ-eligible, breaker-EXCLUDED — so client
  // retries against one post-forget aggregate can no longer open the bus-global circuit. ----

  /**
   * State whose compact constructor validates the email format — and therefore rejects the {@code
   * [REDACTED]} tombstone substituted after a GDPR forget. A population that grows with every
   * erasure.
   */
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
      public java.util.Collection<Class<?>> registeredTypes() {
        return java.util.List.of(stateClass);
      }
    };
  }

  @Test
  void postForgetTombstoneRejectionIsTypedDeterministicAndBreakerExcluded() {
    var cryptoEngine = new InMemoryCryptoEngine();
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(registryFor(ValidatedCustomerState.class))
            .cryptoEngine(cryptoEngine)
            .build();

    var streamId = TestStreams.stream("customer-validated-1");
    store.saveSnapshot(
        streamId, new Version(1), new ValidatedCustomerState("customer-v1", "bob@example.com"));

    // GDPR forget: the next load decrypts email to "[REDACTED]", which the compact constructor
    // rejects — CryptoShreddingModule surfaces that as a deterministic CryptoMappingException.
    cryptoEngine.deleteKey(SubjectId.of("customer-v1"));

    assertThatThrownBy(() -> store.load(streamId))
        .isInstanceOf(EventDeserializationException.class)
        .satisfies(
            t -> {
              assertThat(CommandFailureClassification.DLQ_ELIGIBLE.test(t))
                  .as("a deterministic stored-data failure must stay visible in the DLQ")
                  .isTrue();
              assertThat(CommandFailureClassification.BREAKER_ELIGIBLE.test(t))
                  .as(
                      "one aggregate's deterministic post-forget failure must never open the"
                          + " bus-global circuit")
                  .isFalse();
            });
  }

  // ---- The same two-way routing applies when a registered
  // SnapshotMigration step fails with a crypto cause. Deterministic (CryptoMappingException, e.g.
  // a step's convertValue hitting a tombstone-rejecting constructor) → typed
  // EventDeserializationException; transient (bare CryptoOperationException — key-store outage) →
  // plain EventStoreException. Both still PROPAGATE (fail-closed: a crypto-failing migration is
  // never discarded as a stale cache). ----

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
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .cryptoEngine(new InMemoryCryptoEngine())
            .migrations(
                List.of(
                    throwingStep(
                        new CryptoMappingException(
                            "step cannot reconstruct the record: constructor rejected the"
                                + " [REDACTED] tombstone"))))
            .build();

    var streamId = TestStreams.stream("customer-migrate-det-1");
    store.saveSnapshot(
        streamId, new Version(1), new CustomerState("customer-m1", "dave@example.com"), 1);

    assertThatThrownBy(() -> store.load(streamId, 2))
        .isInstanceOf(EventDeserializationException.class)
        .satisfies(
            t -> {
              assertThat(CommandFailureClassification.DLQ_ELIGIBLE.test(t)).isTrue();
              assertThat(CommandFailureClassification.BREAKER_ELIGIBLE.test(t))
                  .as("a deterministic migration-step crypto refusal is per-aggregate stored data")
                  .isFalse();
            });
  }

  @Test
  void migrationTransientCryptoFailureStaysPlainInfrastructure() {
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .cryptoEngine(new InMemoryCryptoEngine())
            .migrations(
                List.of(
                    throwingStep(
                        new RuntimeException(
                            "step failed",
                            new CryptoOperationException("KMS transient outage in step")))))
            .build();

    var streamId = TestStreams.stream("customer-migrate-trans-1");
    store.saveSnapshot(
        streamId, new Version(1), new CustomerState("customer-m2", "erin@example.com"), 1);

    assertThatThrownBy(() -> store.load(streamId, 2))
        .isExactlyInstanceOf(EventStoreException.class)
        .satisfies(
            t -> {
              boolean cryptoInChain = false;
              for (Throwable c = t; c != null; c = c.getCause()) {
                if (c instanceof CryptoOperationException) {
                  cryptoInChain = true;
                  break;
                }
              }
              assertThat(cryptoInChain)
                  .as("the transient crypto evidence must stay visible in the chain")
                  .isTrue();
              assertThat(CommandFailureClassification.BREAKER_ELIGIBLE.test(t))
                  .as("a transient key-store outage carries infrastructure-health evidence")
                  .isTrue();
            });
  }

  @Test
  void rootLevelCorruptCiphertextSnapshotDiscardsAndReplays() throws Exception {
    // The snapshot-deserialize catch was IOException-only, but a corrupt Base64 value in
    // a ROOT-level @Encrypted field throws a raw IllegalArgumentException (Jackson wraps only
    // NESTED property failures into JsonMappingException, and CryptoShreddingModule's
    // Base64.getDecoder().decode() runs before any crypto wrapping). The raw IAE escaped the catch
    // and propagated out of load() — permanently wedging the aggregate and defeating the
    // discard-and-replay guard that exists for exactly this case: an unreadable snapshot is a
    // DERIVED CACHE, never the source of truth. There is no CryptoOperationException anywhere in
    // the chain (the failure happens before the engine is called), so the fail-closed crypto guard
    // does not apply — the snapshot must be discarded and the state rebuilt from events.
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .cryptoEngine(new InMemoryCryptoEngine())
            .build();

    var streamId = TestStreams.stream("customer-corrupt-b64");
    store.saveSnapshot(
        streamId, new Version(1), new CustomerState("customer-b64", "carol@example.com"));

    // Corrupt the stored ciphertext in place: '#' is not a Base64 character.
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE snapshot_store SET state_payload = jsonb_set(state_payload, '{email}',"
                    + " '\"###corrupt###\"') WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      ps.executeUpdate();
    }

    AggregateHistory history = store.load(streamId);
    assertThat(history.snapshotState())
        .as("the corrupt derived-cache snapshot must be discarded, not wedge the aggregate")
        .isNull();
    assertThat(history.events()).isEmpty();
    assertThat(history.version()).isEqualTo(Version.initial());
  }
}
