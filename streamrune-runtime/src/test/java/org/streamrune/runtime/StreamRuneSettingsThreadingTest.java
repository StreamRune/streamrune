package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.crypto.CryptoEngine;
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
import org.streamrune.core.upcasting.UpcasterChain;
import org.streamrune.crypto.CryptoShreddingModule;

/**
 * Proves that builder settings (cryptoEngine, upcasters, event-type registry) actually reach the
 * event store the facade constructs, by wiring them into a JSON-serializing store via {@link
 * StreamRune.ConfiguredEventStoreFactory} and observing their effect end to end.
 */
class StreamRuneSettingsThreadingTest {

  private static final AggregateType TYPE = AggregateType.of("user");

  record RegisterUser(String userId, String email) implements Command {}

  record UserRegistered(String userId, @Encrypted(subjectId = "userId") String email)
      implements DomainEvent {}

  record UserState(String email) implements AggregateState {}

  static class RegistrationDecider implements Decider<RegisterUser, UserState, UserRegistered> {
    @Override
    public UserState initialState() {
      return new UserState(null);
    }

    @Override
    public List<UserRegistered> decide(RegisterUser command, UserState state) {
      return List.of(new UserRegistered(command.userId(), command.email()));
    }

    @Override
    public UserState evolve(UserState state, UserRegistered event) {
      return new UserState(event.email());
    }
  }

  static class XorCryptoEngine implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      byte[] result = new byte[plaintext.length];
      for (int i = 0; i < plaintext.length; i++) {
        result[i] = (byte) (plaintext[i] ^ 0x5A);
      }
      return result;
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      return encrypt(subjectId, ciphertext);
    }

    @Override
    public void deleteKey(SubjectId subjectId) {}

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  /** Renames the legacy v1 field {@code name} to {@code fullName} (current version 2). */
  static class RenameNameToFullNameUpcaster implements EventUpcaster {
    @Override
    public org.streamrune.core.types.EventType eventType() {
      return new org.streamrune.core.types.EventType("ProfileCreated");
    }

    @Override
    public int currentVersion() {
      return 2;
    }

    @Override
    public Map<String, Object> upcast(Map<String, Object> data, int fromVersion) {
      var result = new HashMap<>(data);
      result.put("fullName", result.remove("name"));
      return result;
    }
  }

  record ProfileCreated(String fullName) implements DomainEvent {}

  private void withCtx(Runnable r) {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-1"), Instant.now(), Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, ctx).run(r);
  }

  @Test
  void cryptoEngineThreadedThroughBuilderEncryptsAtRestAndDecryptsOnLoad() {
    var engine = new XorCryptoEngine();
    var storeRef = new AtomicReference<JsonEventStore>();

    var streamRune =
        StreamRune.builder()
            .eventStoreFactory(
                settings -> {
                  var mapper = new ObjectMapper();
                  mapper.registerModule(new CryptoShreddingModule(settings.cryptoEngine()));
                  var store =
                      new JsonEventStore(
                          mapper, settings.typeRegistry(), new UpcasterChain(settings.upcasters()));
                  storeRef.set(store);
                  return store;
                })
            .cryptoEngine(engine)
            .registerEventType("UserRegistered", UserRegistered.class)
            .locker(new LocalStripedLocker(16))
            .register(
                TYPE,
                RegisterUser.class,
                cmd -> AggregateId.of(cmd.userId()),
                new RegistrationDecider())
            .build();

    try (streamRune) {
      withCtx(() -> streamRune.execute(new RegisterUser("user-1", "ada@example.com")));
    }

    // At rest: the @Encrypted field must not be stored in plaintext
    String rawJson = storeRef.get().rawJson(StreamId.of(TYPE, AggregateId.of("user-1")), 0);
    assertFalse(rawJson.contains("ada@example.com"), "email must be encrypted at rest: " + rawJson);

    // On load: the store's crypto-aware mapper decrypts the field back to plaintext
    AggregateHistory history = storeRef.get().load(StreamId.of(TYPE, AggregateId.of("user-1")));
    var event = assertInstanceOf(UserRegistered.class, history.events().getFirst().event());
    assertEquals("ada@example.com", event.email());
  }

  @Test
  void upcastersAndRegistryThreadedThroughBuilderApplyOnLoad() {
    var storeRef = new AtomicReference<JsonEventStore>();

    var streamRune =
        StreamRune.builder()
            .eventStoreFactory(
                settings -> {
                  var store =
                      new JsonEventStore(
                          new ObjectMapper(),
                          settings.typeRegistry(),
                          new UpcasterChain(settings.upcasters()));
                  storeRef.set(store);
                  return store;
                })
            .upcasters(List.of(new RenameNameToFullNameUpcaster()))
            .registerEventType("ProfileCreated", ProfileCreated.class)
            .build();

    try (streamRune) {
      // Simulate a legacy event persisted at schema version 1 with the old field name
      storeRef
          .get()
          .seedLegacy(
              StreamId.of(TYPE, AggregateId.of("profile-1")),
              "ProfileCreated",
              Map.of("name", "Ada"),
              1);

      AggregateHistory history =
          storeRef.get().load(StreamId.of(TYPE, AggregateId.of("profile-1")));
      var event = assertInstanceOf(ProfileCreated.class, history.events().getFirst().event());
      assertEquals("Ada", event.fullName(), "upcaster must rename 'name' to 'fullName' on load");
    }
  }

  /**
   * Test event store that persists events as JSON, resolving classes via the {@link
   * EventTypeRegistry} and applying the {@link UpcasterChain} on load — the same contract {@code
   * PostgresEventStore} fulfills. Exists to observe that the facade threads its settings into store
   * construction.
   */
  static final class JsonEventStore implements EventStore {

    private record Row(
        long offset,
        StreamId streamId,
        Version version,
        EventType type,
        int schemaVersion,
        String json,
        EventMetadata metadata) {}

    private final ObjectMapper mapper;
    private final EventTypeRegistry registry;
    private final UpcasterChain upcasters;
    private final List<Row> rows = new ArrayList<>();
    private long offsetSeq = 0;

    JsonEventStore(ObjectMapper mapper, EventTypeRegistry registry, UpcasterChain upcasters) {
      this.mapper = mapper;
      this.registry = registry;
      this.upcasters = upcasters;
    }

    synchronized void seedLegacy(
        StreamId streamId, String eventTypeName, Map<String, Object> payload, int schemaVersion) {
      try {
        long streamVersion = rows.stream().filter(r -> r.streamId().equals(streamId)).count();
        rows.add(
            new Row(
                ++offsetSeq,
                streamId,
                new Version(streamVersion + 1),
                new EventType(eventTypeName),
                schemaVersion,
                mapper.writeValueAsString(payload),
                metadata()));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    synchronized String rawJson(StreamId streamId, int index) {
      return rows.stream().filter(r -> r.streamId().equals(streamId)).toList().get(index).json();
    }

    @Override
    public synchronized AggregateHistory load(StreamId streamId) {
      var envelopes =
          rows.stream().filter(r -> r.streamId().equals(streamId)).map(this::toEnvelope).toList();
      Version version = envelopes.isEmpty() ? Version.initial() : envelopes.getLast().version();
      return new AggregateHistory(null, envelopes, version, Version.initial());
    }

    @Override
    public synchronized AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      long current = rows.stream().filter(r -> r.streamId().equals(streamId)).count();
      if (current != expectedVersion.value()) {
        throw new OptimisticLockException(streamId.value(), expectedVersion.value(), current);
      }
      var offsets = new ArrayList<GlobalOffset>();
      for (var envelope : events) {
        try {
          long offset = ++offsetSeq;
          rows.add(
              new Row(
                  offset,
                  streamId,
                  new Version(++current),
                  envelope.eventType(),
                  upcasters.currentVersion(envelope.eventType()),
                  mapper.writeValueAsString(envelope.event()),
                  envelope.metadata()));
          offsets.add(GlobalOffset.of(offset));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      return new AppendResult(offsets, new Version(current));
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      // Snapshots are not exercised by these tests (snapshotPolicy defaults to never())
    }

    @Override
    public synchronized List<EventEnvelope> readGlobalStream(
        GlobalOffset afterOffset, int maxCount) {
      return rows.stream()
          .filter(r -> r.offset() > afterOffset.value())
          .limit(maxCount)
          .map(this::toEnvelope)
          .toList();
    }

    @Override
    public synchronized List<EventEnvelope> readStream(
        StreamId streamId, Version afterVersion, int maxCount) {
      return rows.stream()
          .filter(r -> r.streamId().equals(streamId) && r.version().value() > afterVersion.value())
          .limit(maxCount)
          .map(this::toEnvelope)
          .toList();
    }

    private EventEnvelope toEnvelope(Row row) {
      try {
        Map<String, Object> data =
            mapper.readValue(row.json(), new TypeReference<Map<String, Object>>() {});
        Map<String, Object> upcast = upcasters.upcast(row.type(), data, row.schemaVersion());
        Class<?> eventClass = registry.resolveEventType(row.type());
        var event = (DomainEvent) mapper.convertValue(upcast, eventClass);
        return new EventEnvelope(
            GlobalOffset.of(row.offset()),
            row.streamId(),
            row.version(),
            row.type(),
            event,
            row.metadata());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    private static EventMetadata metadata() {
      return new EventMetadata(
          EventId.of(java.util.UUID.randomUUID().toString()),
          CommandId.of("cmd-seed"),
          null,
          null,
          CorrelationId.of("corr-seed"),
          null,
          null,
          Instant.now());
    }
  }
}
