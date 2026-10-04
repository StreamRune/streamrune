package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.CommandFailureClassification;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Every deterministic read-path deserialization failure must surface as the typed {@link
 * EventDeserializationException} — never as a raw user-code exception — while a transient
 * crypto-backend outage during decryption must stay a plain {@link EventStoreException}
 * (infrastructure), because it heals by itself.
 *
 * <p>The upcast branch of {@code PostgresEventStore.toEnvelope} used {@code
 * objectMapper.convertValue}, whose unchecked {@code IllegalArgumentException} (malformed upcaster
 * output) escaped the {@code catch (IOException)} raw. A top-level {@code IllegalArgumentException}
 * is classified as a caller-input permanent rejection, so the failure was silently excluded from
 * the DLQ and the breaker, and the caller saw an exception indistinguishable from their own bad
 * input. A user upcaster's own throw (NPE/CCE) escaped the same way (the same read-path wedge).
 */
@Testcontainers
class PostgresEventStoreReadClassificationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;

  record TestEvent(String value) implements DomainEvent {}

  /** V2 of TestEvent with an additional field. */
  record TestEventV2(String value, String extra) implements DomainEvent {}

  /** Event with a root-level {@code @Encrypted} field, for the transient-crypto read test. */
  record SecretEvent(String subject, @Encrypted(subjectId = "subject") String value)
      implements DomainEvent {
    @JsonCreator
    SecretEvent(@JsonProperty("subject") String subject, @JsonProperty("value") String value) {
      this.subject = subject;
      this.value = value;
    }
  }

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    new PostgresEventStoreFactory(dataSource, registryFor(TestEvent.class)).create();
  }

  @BeforeEach
  void cleanTables() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM snapshot_store");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  static EventTypeRegistry registryFor(Class<?> eventClass) {
    return new EventTypeRegistry() {
      @Override
      public Class<?> resolveEventType(EventType eventType) {
        return eventClass;
      }

      @Override
      public Class<?> resolveStateType(String stateType) {
        return eventClass;
      }

      @Override
      public java.util.Collection<Class<?>> registeredTypes() {
        return List.of(eventClass);
      }
    };
  }

  /** Inserts a raw V1 event row for {@code TestEvent}, reserving a real global offset. */
  private void insertV1Row(String streamId, String payloadJson) throws Exception {
    try (var conn = dataSource.getConnection()) {
      long offset;
      try (var res =
              conn.prepareStatement(
                  "UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1"
                      + " RETURNING next_value");
          var rs = res.executeQuery()) {
        rs.next();
        offset = rs.getLong(1);
      }
      try (var ps =
          conn.prepareStatement(
              "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                  + " event_type, payload, metadata, schema_version)"
                  + " VALUES (?, 'test', ?, ?, ?, ?::jsonb, ?::jsonb, ?)")) {
        ps.setLong(1, offset);
        ps.setString(2, streamId);
        ps.setLong(3, 1);
        ps.setString(4, "TestEvent");
        ps.setString(5, payloadJson);
        ps.setString(
            6,
            "{\"eventId\":\"evt_test123\",\"commandId\":\"cmd_test123\",\"traceId\":null,"
                + "\"spanId\":null,\"correlationId\":\"corr-1\",\"causationId\":null,"
                + "\"userId\":null,\"timestamp\":\"2025-01-01T00:00:00Z\"}");
        ps.setInt(7, 1);
        ps.executeUpdate();
      }
    }
  }

  private static EventUpcaster upcasterTo2(
      java.util.function.UnaryOperator<Map<String, Object>> transform) {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return new EventType("TestEvent");
      }

      @Override
      public int currentVersion() {
        return 2;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        return transform.apply(eventData);
      }
    };
  }

  private static boolean chainContains(Throwable t, Class<? extends Throwable> type) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (type.isInstance(c)) {
        return true;
      }
    }
    return false;
  }

  @Test
  void malformedUpcasterOutputSurfacesAsTypedDeserializationFailure() throws Exception {
    // The upcaster returns a shape convertValue cannot bind to TestEventV2 — Jackson
    // throws an unchecked IllegalArgumentException. It must surface wrapped in the typed
    // EventDeserializationException (cause chain preserved), not as a raw IAE that classifies as
    // the caller's own bad input.
    insertV1Row("stream-malformed-upcast", "{\"value\": \"original\"}");
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(registryFor(TestEventV2.class))
            .upcasters(
                List.of(
                    upcasterTo2(
                        data -> {
                          var out = new HashMap<String, Object>(data);
                          out.put("value", Map.of("nested", "garbage"));
                          out.put("extra", List.of(1, 2, 3));
                          return out;
                        })))
            .build();

    assertThatThrownBy(() -> store.load(TestStreams.stream("stream-malformed-upcast")))
        .isInstanceOf(EventDeserializationException.class)
        .hasMessageContaining("stream-malformed-upcast")
        .satisfies(
            t ->
                assertThat(chainContains(t, IllegalArgumentException.class))
                    .as("the original IllegalArgumentException must stay in the cause chain")
                    .isTrue())
        .satisfies(
            t ->
                assertThat(CommandFailureClassification.isPermanentRejection(t))
                    .as(
                        "a stored-data defect is not the caller's bad input — it must stay"
                            + " DLQ-eligible instead of being silently excluded")
                    .isFalse());
  }

  @Test
  void upcasterThrowSurfacesAsTypedDeserializationFailure() throws Exception {
    // A user upcaster's own deterministic throw (here an NPE) must not
    // propagate raw — the read-poison classifier needs a typed deterministic frame to bound the
    // retry instead of retrying the read forever.
    insertV1Row("stream-throwing-upcast", "{\"value\": \"original\"}");
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(registryFor(TestEventV2.class))
            .upcasters(
                List.of(
                    upcasterTo2(
                        data -> {
                          throw new NullPointerException("upcaster bug");
                        })))
            .build();

    assertThatThrownBy(() -> store.load(TestStreams.stream("stream-throwing-upcast")))
        .isInstanceOf(EventDeserializationException.class)
        .hasMessageContaining("stream-throwing-upcast")
        .satisfies(
            t ->
                assertThat(chainContains(t, NullPointerException.class))
                    .as("the upcaster's own exception must stay in the cause chain")
                    .isTrue());
  }

  @Test
  void unmappablePayloadOnNonUpcastBranchIsTypedToo() throws Exception {
    // The sibling non-upcast branch (plain readValue) fails with a Jackson IOException for an
    // unmappable stored payload — equally deterministic, so it carries the same typed frame.
    insertV1Row("stream-unmappable", "{\"value\": {\"not\": \"a-string\"}}");
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(registryFor(TestEvent.class))
            .build();

    assertThatThrownBy(() -> store.load(TestStreams.stream("stream-unmappable")))
        .isInstanceOf(EventDeserializationException.class)
        .hasMessageContaining("stream-unmappable");
  }

  /** Encrypts to identity bytes (so append works) but fails every decrypt transiently. */
  static final class DecryptFailingEngine implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return plaintext;
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      throw new CryptoOperationException("KMS transient outage during event decrypt");
    }

    @Override
    public void deleteKey(SubjectId subjectId) {}

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  @Test
  void transientCryptoOutageDuringReadStaysAPlainInfrastructureFailure() throws Exception {
    // The counter-case: a Vault/KMS outage while decrypting a root-level @Encrypted event field is
    // TRANSIENT. It must NOT be wrapped as the deterministic EventDeserializationException — it
    // stays a plain EventStoreException with the CryptoOperationException preserved in the chain,
    // so read-path classifiers keep retrying instead of terminally halting a projection.
    var registry = registryFor(SecretEvent.class);
    var writer =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(registry)
            .cryptoEngine(new DecryptFailingEngine())
            .build();
    var streamId = TestStreams.stream("stream-crypto-outage");
    var metadata =
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now());
    writer.append(
        streamId,
        List.of(
            new EventEnvelope(
                GlobalOffset.initial(),
                streamId,
                new Version(1),
                new EventType("SecretEvent"),
                new SecretEvent("subj-1", "top-secret"),
                metadata)),
        Version.initial());

    assertThatThrownBy(() -> writer.load(streamId))
        .isInstanceOf(EventStoreException.class)
        .isNotInstanceOf(EventDeserializationException.class)
        .satisfies(
            t ->
                assertThat(chainContains(t, CryptoOperationException.class))
                    .as("the engine's transient failure must stay in the cause chain")
                    .isTrue());
  }
}
