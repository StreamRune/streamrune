package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Pins that a {@link PostgresSagaStore} built with a crypto-aware {@link ObjectMapper} (one with
 * {@link CryptoShreddingModule} registered) actually encrypts {@code @Encrypted} saga-state fields
 * at rest, and that deleting the subject's key (GDPR forget) redacts the field on subsequent loads.
 *
 * <p>This is the regression test for the defect where all three framework auto-configs built {@link
 * PostgresSagaStore} via the crypto-blind 1-arg constructor: {@code @Encrypted} saga-state fields
 * were silently persisted as plaintext PII, and forgetting the subject's key had no effect on this
 * table.
 */
@Testcontainers
class PostgresSagaStoreCryptoShreddingTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  /** Saga state carrying one {@code @Encrypted} PII field, keyed off {@code customerId}. */
  record CustomerSagaState(
      SagaStatus status, String customerId, @Encrypted(subjectId = "customerId") String email)
      implements SagaState {

    @JsonCreator
    CustomerSagaState(
        @JsonProperty("status") SagaStatus status,
        @JsonProperty("customerId") String customerId,
        @JsonProperty("email") String email) {
      this.status = status;
      this.customerId = customerId;
      this.email = email;
    }
  }

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    EventTypeRegistry typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            // CustomerSagaState (with its @Encrypted email field) never flows through this
            // registry: PostgresSagaStore serializes saga state directly via the crypto-aware
            // ObjectMapper below, not via EventTypeRegistry resolution. Report none explicitly
            // rather than inheriting the throwing default.
            return java.util.List.of();
          }
        };
    // Apply the shipped event-store baseline (creates saga_state) via the event store factory,
    // same as PostgresSagaStoreTest.
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void cleanTable() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_state");
    }
  }

  private static ObjectMapper cryptoAwareMapper(org.streamrune.core.crypto.CryptoEngine engine) {
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.registerModule(new CryptoShreddingModule(engine));
    return mapper;
  }

  private String readRawStateJson(SagaId sagaId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT state_json FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, sagaId.value());
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).as("saga row exists").isTrue();
        return rs.getString("state_json");
      }
    }
  }

  @Test
  void withCryptoAwareMapper_encryptedFieldIsCiphertextAtRest_andRedactedAfterForget()
      throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var sagaStore = new PostgresSagaStore(dataSource, cryptoAwareMapper(cryptoEngine));

    var sagaId = SagaId.of("saga-crypto-1");
    var sagaType = SagaType.of("CustomerSaga");
    var state = new CustomerSagaState(SagaStatus.RUNNING, "customer-1", "alice@example.com");

    sagaStore.create(sagaId, sagaType, state, SagaStatus.RUNNING);

    // (a) the persisted column must contain ciphertext, never the plaintext PII substring.
    String storedJson = readRawStateJson(sagaId);
    assertThat(storedJson).doesNotContain("alice@example.com");

    // Sanity: pre-forget load decrypts back to plaintext.
    var loaded = sagaStore.load(sagaId, sagaType, CustomerSagaState.class).orElseThrow();
    assertThat(loaded.state().email()).isEqualTo("alice@example.com");

    // Update path must also encrypt.
    var updatedState =
        new CustomerSagaState(SagaStatus.RUNNING, "customer-1", "alice2@example.com");
    sagaStore.update(sagaId, sagaType, updatedState, SagaStatus.RUNNING, 1L);
    String storedJsonAfterUpdate = readRawStateJson(sagaId);
    assertThat(storedJsonAfterUpdate).doesNotContain("alice2@example.com");

    // (b) after forgetting the subject, loading yields [REDACTED] for that field.
    cryptoEngine.deleteKey(SubjectId.of("customer-1"));
    var afterForget = sagaStore.load(sagaId, sagaType, CustomerSagaState.class).orElseThrow();
    assertThat(afterForget.state().email()).isEqualTo(CryptoShreddingModule.REDACTED);
  }

  @Test
  void withCryptoBlindMapper_encryptedFieldIsStoredAsPlaintext_demonstratingTheDefect()
      throws Exception {
    // A mapper WITHOUT CryptoShreddingModule must not silently encrypt: this pins the documented
    // plaintext behaviour so regressions in the opposite direction (accidentally always
    // encrypting) are also caught. The crypto-blind mapper is now built explicitly — the 1-arg
    // PostgresSagaStore(DataSource) constructor that used to hide this choice was deleted before
    // 1.0.0, precisely because the docs taught it while the GDPR guide warned against it.
    // createObjectMapper(null) is the documented crypto-blind form; it registers JavaTimeModule
    // only, exactly what the deleted constructor did.
    var sagaStore = new PostgresSagaStore(dataSource, PostgresSagaStore.createObjectMapper(null));

    var sagaId = SagaId.of("saga-plaintext-1");
    var sagaType = SagaType.of("CustomerSaga");
    var state = new CustomerSagaState(SagaStatus.RUNNING, "customer-2", "bob@example.com");

    sagaStore.create(sagaId, sagaType, state, SagaStatus.RUNNING);

    String storedJson = readRawStateJson(sagaId);
    assertThat(storedJson).contains("bob@example.com");
  }

  // ----- A state-conversion failure must not be laundered as an infrastructure one ---

  /**
   * A saga state whose {@code @Encrypted} field has a null {@code subjectId} is the natural shape
   * of a partially-populated saga mid-flow, and {@code CryptoShreddingModule} refuses to fall back
   * to plaintext for it — it throws {@code CryptoOperationException}. That failure is
   * DETERMINISTIC: the same state re-serializes to the same failure on every attempt.
   *
   * <p>All three write paths used to wrap it in {@code EventStoreException} via a blanket {@code
   * catch (Exception)} spanning serialization AND the JDBC call, making it indistinguishable from a
   * lost connection. {@code SagaRunner} deliberately treats {@code SagaStore} failures as
   * infrastructure and lets them propagate so the subscription retries the batch — correct for a
   * dropped connection, catastrophic here: the poll loop retries forever with capped backoff, no
   * event is quarantined, no saga is FAULTED, no dead-letter row is written, and the subscription
   * still reports RUNNING. Every saga of every type on that subscription stops advancing.
   *
   * <p>The store cannot decide deterministic-vs-transient on its own ({@code
   * CryptoOperationException} is also what a Vault/KMS outage produces), so it does the one thing
   * it can: stop laundering. Serialization gets its own narrow catch and a distinct {@link
   * SagaStateSerializationException} that is NOT an {@code EventStoreException}, carrying the
   * original cause so the caller can classify.
   */
  private static void assertNotLaunderedAsInfrastructure(
      String path, org.junit.jupiter.api.function.Executable call) {
    Throwable thrown = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, call);
    assertThat(thrown)
        .as(
            "%s: a deterministic state-conversion failure must not be disguised as an"
                + " EventStoreException — SagaRunner treats those as infrastructure and retries the"
                + " batch forever",
            path)
        .isNotInstanceOf(EventStoreException.class)
        .isInstanceOf(SagaStateSerializationException.class)
        .hasRootCauseInstanceOf(org.streamrune.core.crypto.CryptoOperationException.class);
  }

  @Test
  void create_nullSubjectIdOnEncryptedField_isNotLaunderedIntoAnInfrastructureFailure() {
    var sagaStore =
        new PostgresSagaStore(dataSource, cryptoAwareMapper(new InMemoryCryptoEngine()));
    var partiallyPopulated = new CustomerSagaState(SagaStatus.RUNNING, null, "alice@example.com");

    assertNotLaunderedAsInfrastructure(
        "create",
        () ->
            sagaStore.create(
                SagaId.of("saga-err1-create"),
                SagaType.of("CustomerSaga"),
                partiallyPopulated,
                SagaStatus.RUNNING));
  }

  @Test
  void update_nullSubjectIdOnEncryptedField_isNotLaunderedIntoAnInfrastructureFailure() {
    // The exact line: a correlated event whose evolve() leaves customerId null, persisted
    // by SagaStepExecutor.persistForwardStatus -> sagaStore.update.
    var sagaStore =
        new PostgresSagaStore(dataSource, cryptoAwareMapper(new InMemoryCryptoEngine()));
    var sagaId = SagaId.of("saga-err1-update");
    var sagaType = SagaType.of("CustomerSaga");
    sagaStore.create(
        sagaId,
        sagaType,
        new CustomerSagaState(SagaStatus.RUNNING, "customer-err1", "alice@example.com"),
        SagaStatus.RUNNING);

    var partiallyPopulated = new CustomerSagaState(SagaStatus.RUNNING, null, "alice@example.com");
    assertNotLaunderedAsInfrastructure(
        "update",
        () -> sagaStore.update(sagaId, sagaType, partiallyPopulated, SagaStatus.RUNNING, 1L));
  }

  @Test
  void claimCompensating_nullSubjectIdOnEncryptedField_isNotLaunderedIntoAnInfrastructureFailure() {
    var sagaStore =
        new PostgresSagaStore(dataSource, cryptoAwareMapper(new InMemoryCryptoEngine()));
    var sagaId = SagaId.of("saga-err1-claim");
    var sagaType = SagaType.of("CustomerSaga");
    sagaStore.create(
        sagaId,
        sagaType,
        new CustomerSagaState(SagaStatus.RUNNING, "customer-err1b", "alice@example.com"),
        SagaStatus.RUNNING);

    var partiallyPopulated = new CustomerSagaState(SagaStatus.RUNNING, null, "alice@example.com");
    assertNotLaunderedAsInfrastructure(
        "claimCompensating",
        () -> sagaStore.claimCompensating(sagaId, sagaType, partiallyPopulated, 1L));
  }

  @Test
  void load_undeserializableStateJson_isNotLaunderedIntoAnInfrastructureFailure() throws Exception {
    // The read side has the identical shape and the identical wedge: doLoad's SECOND try (which
    // deserializes after the connection is released) wrapped every mapping failure in
    // EventStoreException too, so a saga row whose stored JSON no longer maps — a shape change, an
    // unknown enum constant, a payload written by another type — retries forever instead of being
    // quarantined.
    var sagaStore =
        new PostgresSagaStore(dataSource, cryptoAwareMapper(new InMemoryCryptoEngine()));
    var sagaId = SagaId.of("saga-err1-load");
    var sagaType = SagaType.of("CustomerSaga");
    sagaStore.create(
        sagaId,
        sagaType,
        new CustomerSagaState(SagaStatus.RUNNING, "customer-err1c", "alice@example.com"),
        SagaStatus.RUNNING);

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE saga_state SET state_json = ?::jsonb WHERE saga_id = ?")) {
      ps.setString(1, "{\"status\":\"NOT_A_REAL_STATUS\",\"customerId\":\"c\",\"email\":\"e\"}");
      ps.setString(2, sagaId.value());
      ps.executeUpdate();
    }

    Throwable thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            RuntimeException.class,
            () -> sagaStore.load(sagaId, sagaType, CustomerSagaState.class));
    assertThat(thrown)
        .as(
            "load: a deterministic deserialization failure must not be disguised as an"
                + " EventStoreException")
        .isNotInstanceOf(EventStoreException.class)
        .isInstanceOf(SagaStateSerializationException.class);
  }

  @Test
  void jdbcFailuresStayEventStoreExceptions() {
    // The other half of the split: a genuine STORAGE failure must keep its infrastructure
    // classification, so SagaRunner's retry-the-batch behaviour is unchanged for the case it was
    // designed for. A closed DataSource is a pure connection failure with no serialization
    // involved.
    var closedDataSource = new PGSimpleDataSource();
    closedDataSource.setUrl("jdbc:postgresql://127.0.0.1:1/nonexistent");
    closedDataSource.setUser("nobody");
    closedDataSource.setPassword("nobody");
    var sagaStore =
        new PostgresSagaStore(closedDataSource, cryptoAwareMapper(new InMemoryCryptoEngine()));
    var good = new CustomerSagaState(SagaStatus.RUNNING, "customer-ok", "alice@example.com");

    assertThat(
            org.junit.jupiter.api.Assertions.assertThrows(
                RuntimeException.class,
                () ->
                    sagaStore.create(
                        SagaId.of("saga-err1-infra"),
                        SagaType.of("CustomerSaga"),
                        good,
                        SagaStatus.RUNNING)))
        .isInstanceOf(EventStoreException.class);
  }

  @Test
  void createObjectMapper_withCryptoEngine_registersCryptoShreddingModule() {
    var mapper = PostgresSagaStore.createObjectMapper(new InMemoryCryptoEngine());
    assertThat(mapper.getRegisteredModuleIds()).contains("CryptoShreddingModule");
  }

  @Test
  void createObjectMapper_withNullCryptoEngine_isCryptoBlind() {
    var mapper = PostgresSagaStore.createObjectMapper(null);
    assertThat(mapper.getRegisteredModuleIds()).doesNotContain("CryptoShreddingModule");
  }
}
