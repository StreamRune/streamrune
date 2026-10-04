package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * Parity with PostgresEventStore: the serializing-mode double must raise the SAME typed frames as
 * {@code PostgresEventStore} on every read/deserialization failure, or a unit test against the
 * double witnesses classification behavior production does not have.
 *
 * <p>Event path (mirrors {@code PostgresEventStoreReadClassificationTest}): a deterministic failure
 * — a user upcaster's own throw, malformed upcaster output, or the crypto layer's deterministic
 * {@link CryptoMappingException} (a record constructor rejecting the post-forget {@code [REDACTED]}
 * tombstone) — surfaces as the typed {@link EventDeserializationException}; a transient key-store
 * outage (bare {@link CryptoOperationException}) stays a PLAIN {@link EventStoreException}. Before
 * the fix, the catch was {@code JsonProcessingException}-only, so every root-level RuntimeException
 * escaped raw.
 *
 * <p>Snapshot path (mirrors {@code PostgresEventStoreCryptoSnapshotTest}): same two-way routing,
 * with propagate-vs-discard unchanged — crypto causes always propagate (fail-closed), everything
 * else discards the derived cache and replays.
 */
class InMemoryEventStoreReadClassificationTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  // ── fixtures ──

  record VEvent(String value) implements DomainEvent {}

  /** V2 shape the upcaster targets. */
  record VEventV2(String value, String extra) implements DomainEvent {}

  /** Event with a root-level {@code @Encrypted} field. */
  record SecretEvent(String subject, @Encrypted(subjectId = "subject") String value)
      implements DomainEvent {
    @JsonCreator
    SecretEvent(@JsonProperty("subject") String subject, @JsonProperty("value") String value) {
      this.subject = subject;
      this.value = value;
    }
  }

  /** Event whose constructor validates the plaintext — and so rejects the [REDACTED] tombstone. */
  record ValidatedSecretEvent(String subject, @Encrypted(subjectId = "subject") String email)
      implements DomainEvent {
    @JsonCreator
    ValidatedSecretEvent(
        @JsonProperty("subject") String subject, @JsonProperty("email") String email) {
      if (email != null && !email.contains("@")) {
        throw new IllegalArgumentException("not an email: " + email);
      }
      this.subject = subject;
      this.email = email;
    }
  }

  /** State with a root-level {@code @Encrypted} field. */
  record SecretState(String subject, @Encrypted(subjectId = "subject") String value)
      implements AggregateState {
    @JsonCreator
    SecretState(@JsonProperty("subject") String subject, @JsonProperty("value") String value) {
      this.subject = subject;
      this.value = value;
    }
  }

  /** State whose constructor validates the plaintext — and so rejects the [REDACTED] tombstone. */
  record ValidatedState(String subject, @Encrypted(subjectId = "subject") String email)
      implements AggregateState {
    @JsonCreator
    ValidatedState(@JsonProperty("subject") String subject, @JsonProperty("email") String email) {
      if (email != null && !email.contains("@")) {
        throw new IllegalArgumentException("not an email: " + email);
      }
      this.subject = subject;
      this.email = email;
    }
  }

  /** Encrypts to identity bytes (so append/save works) but fails every decrypt transiently. */
  static final class DecryptFailingEngine implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return plaintext;
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      throw new CryptoOperationException("KMS transient outage during decrypt");
    }

    @Override
    public void deleteKey(SubjectId subjectId) {}

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  private static EventEnvelope envelope(StreamId id, long v, EventType type, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        id,
        new Version(v),
        type,
        event,
        new EventMetadata(
            EventId.of("evt_" + v),
            CommandId.of("cmd_" + v),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  private static EventUpcaster upcasterTo2(
      java.util.function.UnaryOperator<Map<String, Object>> transform) {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return new EventType("VEvent");
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
    int remaining = 50;
    for (Throwable c = t; c != null && remaining-- > 0; c = c.getCause()) {
      if (type.isInstance(c)) {
        return true;
      }
    }
    return false;
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

  // ── event path ──

  @Test
  void upcasterThrowSurfacesAsTypedDeserializationFailure() {
    // A user upcaster's own deterministic throw (here an NPE) escaped the
    // JsonProcessingException-only catch raw. It must surface as the typed frame, exactly like
    // PostgresEventStore.toEnvelope.
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("VEvent", VEventV2.class).build();
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("stream-throwing-upcast"));
    // Append while chainless (stamped schema_version 1), then register the v2 chain — the read
    // takes the upcast branch (see withUpcasters' javadoc).
    store.append(
        s, List.of(envelope(s, 1, new EventType("VEvent"), new VEvent("x"))), Version.initial());
    store.withUpcasters(
        List.of(
            upcasterTo2(
                data -> {
                  throw new NullPointerException("upcaster bug");
                })));

    var thrown = assertThrows(EventDeserializationException.class, () -> store.load(s));
    assertTrue(
        chainContains(thrown, NullPointerException.class),
        "the upcaster's own exception must stay in the cause chain");
  }

  @Test
  void malformedUpcasterOutputSurfacesAsTypedDeserializationFailure() {
    // convertValue's unchecked IllegalArgumentException on a malformed upcaster
    // output escaped raw — classifying as the CALLER's permanent rejection though the defect is
    // in stored data.
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("VEvent", VEventV2.class).build();
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("stream-malformed-upcast"));
    store.append(
        s, List.of(envelope(s, 1, new EventType("VEvent"), new VEvent("x"))), Version.initial());
    store.withUpcasters(
        List.of(
            upcasterTo2(
                data -> {
                  var out = new HashMap<String, Object>(data);
                  out.put("value", Map.of("nested", "garbage"));
                  out.put("extra", List.of(1, 2, 3));
                  return out;
                })));

    var thrown = assertThrows(EventDeserializationException.class, () -> store.load(s));
    assertTrue(
        chainContains(thrown, IllegalArgumentException.class),
        "the original IllegalArgumentException must stay in the cause chain");
  }

  @Test
  void transientCryptoOutageOnEventReadStaysPlainInfrastructureFailure() {
    // A bare CryptoOperationException on a ROOT-level @Encrypted field propagates raw out of
    // readValue (no Jackson wrapping happens before the tree-stage decrypt). It must surface as
    // the PLAIN EventStoreException — transient infrastructure, never the deterministic subtype.
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("Secret", SecretEvent.class).build();
    var store = InMemoryEventStore.serializing(registry, new DecryptFailingEngine());
    StreamId s = StreamId.of(TYPE, AggregateId.of("stream-secret-transient"));
    store.append(
        s,
        List.of(envelope(s, 1, new EventType("Secret"), new SecretEvent("subj-1", "top-secret"))),
        Version.initial());

    var thrown = assertThrows(EventStoreException.class, () -> store.load(s));
    assertSame(
        EventStoreException.class,
        thrown.getClass(),
        "transient key-store outage must stay the PLAIN EventStoreException");
    assertTrue(
        chainContains(thrown, CryptoOperationException.class),
        "the transient crypto evidence must stay visible in the chain");
  }

  @Test
  void postForgetTombstoneRejectionOnEventReadIsTypedDeterministic() {
    // Event path: after a GDPR forget the @Encrypted field decrypts to
    // "[REDACTED]", the constructor rejects it, and CryptoShreddingModule surfaces the
    // deterministic CryptoMappingException — which must arrive wrapped in the typed
    // EventDeserializationException, not raw.
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("Validated", ValidatedSecretEvent.class)
            .build();
    var engine = new InMemoryCryptoEngine();
    var store = InMemoryEventStore.serializing(registry, engine);
    StreamId s = StreamId.of(TYPE, AggregateId.of("stream-validated-forget"));
    store.append(
        s,
        List.of(
            envelope(
                s,
                1,
                new EventType("Validated"),
                new ValidatedSecretEvent("subj-2", "bob@example.com"))),
        Version.initial());
    engine.deleteKey(SubjectId.of("subj-2"));

    var thrown = assertThrows(EventDeserializationException.class, () -> store.load(s));
    assertTrue(
        chainContains(thrown, CryptoMappingException.class),
        "the deterministic crypto mapping refusal must stay visible in the chain");
  }

  // ── snapshot path ──

  @Test
  void postForgetTombstoneRejectionOnSnapshotIsTypedDeterministicAndPropagates() {
    // Snapshot path: the deterministic CryptoMappingException escaped the
    // JsonProcessingException-only catch in deserializeSnapshotState raw, bypassing BOTH the
    // fail-closed propagate (untyped) and the discard. It must propagate as the typed
    // EventDeserializationException — still thrown, never discarded.
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerState("ValidatedState", ValidatedState.class)
            .build();
    var engine = new InMemoryCryptoEngine();
    var store = InMemoryEventStore.serializing(registry, engine);
    StreamId s = StreamId.of(TYPE, AggregateId.of("snap-validated-forget"));
    store.saveSnapshot(s, new Version(1), new ValidatedState("subj-3", "carol@example.com"));
    engine.deleteKey(SubjectId.of("subj-3"));

    var thrown = assertThrows(EventDeserializationException.class, () -> store.load(s));
    assertTrue(
        chainContains(thrown, CryptoMappingException.class),
        "the deterministic crypto mapping refusal must stay visible in the chain");
  }

  @Test
  void transientCryptoOutageOnSnapshotStaysPlainInfrastructureFailure() {
    // The bare CryptoOperationException from a ROOT-level @Encrypted decrypt escaped raw (the
    // resolveSnapshot catch only sees EventStoreException). It must propagate wrapped in the
    // PLAIN EventStoreException — fail-closed preserved, still transient for every classifier.
    var registry =
        SimpleEventTypeRegistry.builder().registerState("SecretState", SecretState.class).build();
    var store = InMemoryEventStore.serializing(registry, new DecryptFailingEngine());
    StreamId s = StreamId.of(TYPE, AggregateId.of("snap-secret-transient"));
    store.saveSnapshot(s, new Version(1), new SecretState("subj-4", "top-secret"));

    var thrown = assertThrows(EventStoreException.class, () -> store.load(s));
    assertSame(
        EventStoreException.class,
        thrown.getClass(),
        "transient key-store outage must stay the PLAIN EventStoreException");
    assertTrue(
        chainContains(thrown, CryptoOperationException.class),
        "the transient crypto evidence must stay visible in the chain");
  }

  // The discard-and-replay baseline for NON-crypto failures is pinned by
  // InMemoryEventStoreSerializingTest.undeserializableEventFailsOnReadLikePostgres and
  // InMemoryEventStoreMigrationTest.throwingMigrationDiscardsSnapshotAndReplaysFromInitial — the
  // widened catches here must not (and do not) change propagate-vs-discard, only the frame type.

  record CartState(int items) implements AggregateState {}

  // ── snapshot migration path ──

  @Test
  void migrationDeterministicCryptoFailureIsTypedDeserializationFailure() {
    var registry =
        SimpleEventTypeRegistry.builder().registerState("CartState", CartState.class).build();
    var store =
        InMemoryEventStore.serializing(registry, null)
            .withMigrations(
                List.of(
                    throwingStep(
                        new CryptoMappingException(
                            "step cannot reconstruct the record: constructor rejected the"
                                + " [REDACTED] tombstone"))));
    StreamId s = StreamId.of(TYPE, AggregateId.of("snap-migrate-det"));
    store.saveSnapshot(s, new Version(1), new CartState(1), 1);

    var thrown = assertThrows(EventDeserializationException.class, () -> store.load(s, 2));
    assertTrue(
        chainContains(thrown, CryptoMappingException.class),
        "the deterministic crypto mapping refusal must stay visible in the chain");
  }

  @Test
  void migrationTransientCryptoFailureStaysPlainInfrastructureFailure() {
    var registry =
        SimpleEventTypeRegistry.builder().registerState("CartState", CartState.class).build();
    var store =
        InMemoryEventStore.serializing(registry, null)
            .withMigrations(
                List.of(
                    throwingStep(
                        new RuntimeException(
                            "step failed",
                            new CryptoOperationException("KMS transient outage in step")))));
    StreamId s = StreamId.of(TYPE, AggregateId.of("snap-migrate-trans"));
    store.saveSnapshot(s, new Version(1), new CartState(1), 1);

    var thrown = assertThrows(EventStoreException.class, () -> store.load(s, 2));
    assertSame(
        EventStoreException.class,
        thrown.getClass(),
        "transient key-store outage must stay the PLAIN EventStoreException");
    assertTrue(
        chainContains(thrown, CryptoOperationException.class),
        "the transient crypto evidence must stay visible in the chain");
  }
}
