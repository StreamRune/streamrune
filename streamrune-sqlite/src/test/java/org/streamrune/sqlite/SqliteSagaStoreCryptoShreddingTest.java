package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * A {@link SqliteSagaStore} built with a crypto-aware {@link ObjectMapper} (one with {@link
 * CryptoShreddingModule}) encrypts {@code @Encrypted} saga-state fields at rest, and forgetting the
 * subject's key redacts them on later loads; a crypto-blind mapper stores them as plaintext. A
 * state-conversion failure is a {@link SagaStateSerializationException}, never an {@link
 * EventStoreException}, and a storage failure stays an {@link EventStoreException}.
 */
class SqliteSagaStoreCryptoShreddingTest {

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

  private static final SagaType TYPE = SagaType.of("CustomerSaga");

  @TempDir Path dir;

  private SqliteDatabase database;

  @BeforeEach
  void open() {
    database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("crypto-sagas.db")));
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
  }

  @AfterEach
  void close() {
    database.close();
  }

  private static ObjectMapper cryptoAwareMapper(CryptoEngine engine) {
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.registerModule(new CryptoShreddingModule(engine));
    return mapper;
  }

  private String readRawStateJson(SagaId sagaId) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT state_json FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, sagaId.value());
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("saga row exists").isTrue();
        return rs.getString(1);
      }
    }
  }

  @Test
  void withCryptoAwareMapper_encryptedFieldIsCiphertextAtRest_andRedactedAfterForget()
      throws Exception {
    var cryptoEngine = new InMemoryCryptoEngine();
    var sagaStore = new SqliteSagaStore(database, cryptoAwareMapper(cryptoEngine));
    var sagaId = SagaId.of("saga-crypto-1");
    var state = new CustomerSagaState(SagaStatus.RUNNING, "customer-1", "alice@example.com");

    sagaStore.create(sagaId, TYPE, state, SagaStatus.RUNNING);

    assertThat(readRawStateJson(sagaId)).doesNotContain("alice@example.com");
    var loaded = sagaStore.load(sagaId, TYPE, CustomerSagaState.class).orElseThrow();
    assertThat(loaded.state().email()).isEqualTo("alice@example.com");

    // Every other write that persists the state encrypts it too.
    sagaStore.update(
        sagaId,
        TYPE,
        new CustomerSagaState(SagaStatus.RUNNING, "customer-1", "alice2@example.com"),
        SagaStatus.RUNNING,
        1L);
    assertThat(readRawStateJson(sagaId)).doesNotContain("alice2@example.com");
    sagaStore.claimCompensating(
        sagaId,
        TYPE,
        new CustomerSagaState(SagaStatus.COMPENSATING, "customer-1", "alice3@example.com"),
        2L);
    assertThat(readRawStateJson(sagaId)).doesNotContain("alice3@example.com");
    var genesis = SagaId.of("saga-crypto-genesis");
    sagaStore.createGenesisPending(
        genesis,
        TYPE,
        new CustomerSagaState(SagaStatus.STARTED, "customer-1", "carol@example.com"),
        SagaStatus.STARTED,
        false);
    assertThat(readRawStateJson(genesis)).doesNotContain("carol@example.com");
    var faulted = SagaId.of("saga-crypto-faulted");
    sagaStore.createFaulted(
        faulted, TYPE, new CustomerSagaState(SagaStatus.STARTED, "customer-1", "dan@example.com"));
    assertThat(readRawStateJson(faulted)).doesNotContain("dan@example.com");

    cryptoEngine.deleteKey(SubjectId.of("customer-1"));
    var afterForget = sagaStore.load(sagaId, TYPE, CustomerSagaState.class).orElseThrow();
    assertThat(afterForget.state().email()).isEqualTo(CryptoShreddingModule.REDACTED);
  }

  @Test
  void withCryptoBlindMapper_encryptedFieldIsStoredAsPlaintext() throws Exception {
    var sagaStore = new SqliteSagaStore(database, SqliteSagaStore.createObjectMapper(null));
    var sagaId = SagaId.of("saga-plaintext-1");

    sagaStore.create(
        sagaId,
        TYPE,
        new CustomerSagaState(SagaStatus.RUNNING, "customer-2", "bob@example.com"),
        SagaStatus.RUNNING);

    assertThat(readRawStateJson(sagaId)).contains("bob@example.com");
  }

  private static void assertNotLaunderedAsInfrastructure(String path, Executable call) {
    Throwable thrown = assertThrows(RuntimeException.class, call);
    assertThat(thrown)
        .as("%s: a deterministic state-conversion failure is not an EventStoreException", path)
        .isNotInstanceOf(EventStoreException.class)
        .isInstanceOf(SagaStateSerializationException.class)
        .hasRootCauseInstanceOf(CryptoOperationException.class);
  }

  @Test
  void everyWriteOfAStateWithANullSubjectIdIsAStateConversionFailure() {
    var sagaStore = new SqliteSagaStore(database, cryptoAwareMapper(new InMemoryCryptoEngine()));
    var sagaId = SagaId.of("saga-null-subject");
    sagaStore.create(
        sagaId,
        TYPE,
        new CustomerSagaState(SagaStatus.RUNNING, "customer-3", "alice@example.com"),
        SagaStatus.RUNNING);
    var partial = new CustomerSagaState(SagaStatus.RUNNING, null, "alice@example.com");

    assertNotLaunderedAsInfrastructure(
        "create", () -> sagaStore.create(SagaId.of("other"), TYPE, partial, SagaStatus.RUNNING));
    assertNotLaunderedAsInfrastructure(
        "createGenesisPending",
        () ->
            sagaStore.createGenesisPending(
                SagaId.of("other"), TYPE, partial, SagaStatus.STARTED, false));
    assertNotLaunderedAsInfrastructure(
        "createFaulted", () -> sagaStore.createFaulted(SagaId.of("other"), TYPE, partial));
    assertNotLaunderedAsInfrastructure(
        "update", () -> sagaStore.update(sagaId, TYPE, partial, SagaStatus.RUNNING, 1L));
    assertNotLaunderedAsInfrastructure(
        "claimCompensating", () -> sagaStore.claimCompensating(sagaId, TYPE, partial, 1L));
    assertNotLaunderedAsInfrastructure(
        "applyEvent",
        () ->
            sagaStore.applyEvent(
                sagaId,
                TYPE,
                partial,
                SagaStatus.RUNNING,
                1L,
                org.streamrune.core.saga.SagaStore.AppliedEvent.live(
                    org.streamrune.core.types.GlobalOffset.of(1))));
    // Nothing was written by any of them.
    assertThat(sagaStore.load(sagaId, TYPE, CustomerSagaState.class).orElseThrow().version())
        .isEqualTo(1L);
    assertThat(sagaStore.load(SagaId.of("other"), TYPE, CustomerSagaState.class)).isEmpty();
  }

  @Test
  void load_undeserializableStateJson_isAStateConversionFailure() throws Exception {
    var sagaStore = new SqliteSagaStore(database, cryptoAwareMapper(new InMemoryCryptoEngine()));
    var sagaId = SagaId.of("saga-bad-json");
    sagaStore.create(
        sagaId,
        TYPE,
        new CustomerSagaState(SagaStatus.RUNNING, "customer-4", "alice@example.com"),
        SagaStatus.RUNNING);
    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement("UPDATE saga_state SET state_json = ? WHERE saga_id = ?")) {
      ps.setString(1, "{\"status\":\"NOT_A_REAL_STATUS\",\"customerId\":\"c\",\"email\":\"e\"}");
      ps.setString(2, sagaId.value());
      ps.executeUpdate();
    }

    Throwable thrown =
        assertThrows(
            RuntimeException.class, () -> sagaStore.load(sagaId, TYPE, CustomerSagaState.class));

    assertThat(thrown)
        .isNotInstanceOf(EventStoreException.class)
        .isInstanceOf(SagaStateSerializationException.class);
  }

  @Test
  void storageFailuresStayEventStoreExceptions() {
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    SqliteDatabase handle = SqliteDatabase.open(source);
    SqliteSagaStore failing =
        new SqliteSagaStore(handle, cryptoAwareMapper(new InMemoryCryptoEngine()));
    source.failWith(() -> new IllegalStateException("the pool is closed"));
    var good = new CustomerSagaState(SagaStatus.RUNNING, "customer-ok", "alice@example.com");

    assertThat(
            assertThrows(
                RuntimeException.class,
                () -> failing.create(SagaId.of("saga-infra"), TYPE, good, SagaStatus.RUNNING)))
        .isInstanceOf(EventStoreException.class);
    assertThat(
            assertThrows(
                RuntimeException.class,
                () -> failing.load(SagaId.of("saga-infra"), TYPE, CustomerSagaState.class)))
        .isInstanceOf(EventStoreException.class)
        .hasRootCauseInstanceOf(IllegalStateException.class);
    assertThat(
            assertThrows(
                RuntimeException.class, () -> failing.loadRowFacts(SagaId.of("saga-infra"), TYPE)))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("saga-infra");
    handle.close();
  }

  @Test
  void createObjectMapper_withCryptoEngine_registersCryptoShreddingModule() {
    assertThat(
            SqliteSagaStore.createObjectMapper(new InMemoryCryptoEngine()).getRegisteredModuleIds())
        .contains("CryptoShreddingModule");
  }

  @Test
  void createObjectMapper_withNullCryptoEngine_isCryptoBlind() {
    assertThat(SqliteSagaStore.createObjectMapper(null).getRegisteredModuleIds())
        .doesNotContain("CryptoShreddingModule");
  }
}
