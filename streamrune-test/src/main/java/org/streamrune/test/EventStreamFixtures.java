package org.streamrune.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.SerializerFactoryConfig;
import com.fasterxml.jackson.databind.ser.BasicSerializerFactory;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.Serializers;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.CryptoShreddingModule;

/**
 * Event streams kept in files: record the events of an {@link InMemoryEventStore} once, check the
 * file in, and replay it into a fresh store in a later test — a projection or a saga then runs over
 * the recorded history instead of one rebuilt by hand.
 *
 * <pre>{@code
 * Path file = Path.of("src/test/resources/orders.json");
 * EventStreamFixtures.write(store, file);   // once, then check the file in
 *
 * var replayed = InMemoryEventStore.serializing(typeRegistry, null);
 * EventStreamFixtures.appendTo(replayed, EventStreamFixtures.read(file));
 * }</pre>
 *
 * <p><b>The file.</b> A JSON array, one object per event in global order: {@code streamId} ({@code
 * type:id}), {@code version}, {@code eventType}, {@code schemaVersion}, {@code payload} (the event
 * as a JSON object, as the store's own mapper writes it) and {@code metadata} (the components of
 * {@link EventMetadata}: the event, command and correlation ids, the timestamp in ISO-8601, the
 * baggage, the saga origin). It is written with sorted map keys, two-space indentation and {@code
 * \n} line ends, so a stream written twice is byte for byte the same file. The ids and timestamps
 * are the file's: a stream appended from it and written again reproduces it exactly.
 *
 * <p><b>Reading it back.</b> {@link #read} parses the file and keeps each payload as JSON; {@link
 * #appendTo} binds it the way the store binds a stored event — the store's {@link
 * EventTypeRegistry} resolves the type (an unregistered one fails with the registry's {@code
 * UnknownEventTypeException}), the store's upcaster chain upcasts a payload stored below the type's
 * current schema version, and a payload above it is refused ({@code EventSchemaAheadException}) —
 * and appends each event to its stream at its version. The target is an {@link InMemoryEventStore}
 * in serializing mode ({@link InMemoryEventStore#serializing}), and a fresh one: an event whose
 * stream already holds that version fails with the store's {@code OptimisticLockException}. A file
 * holds payloads as the store reads them, so the fields of an {@code @Encrypted} event are in the
 * clear: append such a file to a store without a crypto engine. A fixture file therefore holds
 * personal data as plain text — record it from synthetic data only, and keep it in test resources.
 */
public final class EventStreamFixtures {

  /**
   * One event as a fixture file holds it.
   *
   * @param streamId the stream the event belongs to
   * @param version the event's version in its stream
   * @param eventType the name the event type is registered under
   * @param schemaVersion the schema version of {@code payload}: the version of the event type the
   *     store that wrote the file read it at
   * @param payload the event as a JSON object
   * @param metadata the event's metadata
   */
  public record StoredEvent(
      StreamId streamId,
      Version version,
      EventType eventType,
      int schemaVersion,
      JsonNode payload,
      EventMetadata metadata) {

    /** Refuses a missing part and a schema version below 1. */
    public StoredEvent {
      Objects.requireNonNull(streamId, "streamId is required");
      Objects.requireNonNull(version, "version is required");
      Objects.requireNonNull(eventType, "eventType is required");
      Objects.requireNonNull(payload, "payload is required");
      Objects.requireNonNull(metadata, "metadata is required");
      if (schemaVersion < 1) {
        throw new IllegalArgumentException("schemaVersion must be 1 or more, got " + schemaVersion);
      }
    }
  }

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  private static final ObjectWriter WRITER =
      MAPPER.writer(
          new DefaultPrettyPrinter()
              .withObjectIndenter(new DefaultIndenter("  ", "\n"))
              .withArrayIndenter(new DefaultIndenter("  ", "\n")));

  private EventStreamFixtures() {}

  /**
   * Writes every event of {@code store}, in global order, to {@code file}, replacing it. The
   * payloads of a serializing store are written with a copy of its own {@code ObjectMapper}, so a
   * payload type the application serializes its own way reads back: the copy sorts map keys, writes
   * dates in ISO-8601 and leaves out the encrypting writers of a {@link CryptoShreddingModule}. A
   * store that keeps its events by reference has no mapper: its payloads are written with the
   * file's own, {@code JavaTimeModule} and the same two formats.
   *
   * @param store the store whose events to record
   * @param file the file to write; its parent directories are created
   * @throws IOException if the file cannot be written
   */
  public static void write(InMemoryEventStore store, Path file) throws IOException {
    Objects.requireNonNull(store, "store is required");
    Objects.requireNonNull(file, "file is required");
    ObjectMapper payloads = payloadMapper(store);
    List<StoredEvent> events = new ArrayList<>();
    for (EventEnvelope event : store.readGlobalStream(GlobalOffset.initial(), Integer.MAX_VALUE)) {
      events.add(
          new StoredEvent(
              event.streamId(),
              event.version(),
              event.eventType(),
              store.schemaVersionOf(event.eventType()),
              payloads.valueToTree(event.event()),
              event.metadata()));
    }
    Path parent = file.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.writeString(file, WRITER.writeValueAsString(events) + "\n", StandardCharsets.UTF_8);
  }

  /**
   * Reads the events of a fixture file, in the order the file lists them.
   *
   * @param file the file
   * @return the events, their payloads as JSON
   * @throws IOException if the file cannot be read or is not an event stream
   */
  public static List<StoredEvent> read(Path file) throws IOException {
    Objects.requireNonNull(file, "file is required");
    try {
      return List.copyOf(
          MAPPER.readValue(file.toFile(), new TypeReference<List<StoredEvent>>() {}));
    } catch (IOException e) {
      throw new IOException(
          "Cannot read the event stream fixture " + file + ": " + e.getMessage(), e);
    }
  }

  /**
   * Appends {@code events} to {@code store}, each to its stream at its version, in the order given:
   * the store binds every payload as it binds a stored event (see the class description).
   *
   * @param store a store in serializing mode, holding none of the streams yet
   * @param events the events, typically {@link #read} from a file
   * @throws IllegalArgumentException if {@code store} keeps its events by reference
   */
  public static void appendTo(InMemoryEventStore store, List<StoredEvent> events) {
    Objects.requireNonNull(store, "store is required");
    Objects.requireNonNull(events, "events is required");
    if (!store.serializes()) {
      throw new IllegalArgumentException(
          "appendTo needs an InMemoryEventStore in serializing mode"
              + " (InMemoryEventStore.serializing(typeRegistry, cryptoEngine)): the store's"
              + " EventTypeRegistry resolves the event type of each event of the file");
    }
    for (StoredEvent event : events) {
      EventEnvelope envelope =
          store.bindStoredForm(
              event.streamId(),
              event.version(),
              event.eventType(),
              event.schemaVersion(),
              json(event.payload()),
              json(event.metadata()));
      store.append(event.streamId(), List.of(envelope), new Version(event.version().value() - 1));
    }
  }

  /**
   * The mapper the payloads of {@code store} are written with (see {@link #write}): the file's own
   * for a store without a mapper, else a copy of the store's that writes {@code @Encrypted} fields
   * in the clear.
   */
  private static ObjectMapper payloadMapper(InMemoryEventStore store) {
    if (!store.serializes()) {
      return MAPPER;
    }
    ObjectMapper copy = store.objectMapper().copy();
    if (copy.getSerializerFactory() instanceof BasicSerializerFactory factory
        && encrypts(factory.getFactoryConfig())) {
      copy.setSerializerFactory(factory.withConfig(withoutEncryption(factory.getFactoryConfig())));
    }
    return copy.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  private static boolean encrypts(SerializerFactoryConfig config) {
    for (BeanSerializerModifier modifier : config.serializerModifiers()) {
      if (encrypting(modifier)) {
        return true;
      }
    }
    return false;
  }

  /** A serializer modifier a {@link CryptoShreddingModule} registered: it encrypts. */
  private static boolean encrypting(BeanSerializerModifier modifier) {
    return modifier.getClass().getEnclosingClass() == CryptoShreddingModule.class;
  }

  /**
   * {@code config} without the modifiers that encrypt. Each {@code with...} call puts its argument
   * first, so adding the parts in reverse keeps their order.
   */
  private static SerializerFactoryConfig withoutEncryption(SerializerFactoryConfig config) {
    SerializerFactoryConfig clear = new SerializerFactoryConfig();
    for (Serializers serializers : reversed(config.serializers())) {
      clear = clear.withAdditionalSerializers(serializers);
    }
    for (Serializers serializers : reversed(config.keySerializers())) {
      clear = clear.withAdditionalKeySerializers(serializers);
    }
    for (BeanSerializerModifier modifier : reversed(config.serializerModifiers())) {
      if (!encrypting(modifier)) {
        clear = clear.withSerializerModifier(modifier);
      }
    }
    return clear;
  }

  private static <T> List<T> reversed(Iterable<T> items) {
    List<T> list = new ArrayList<>();
    items.forEach(list::add);
    return list.reversed();
  }

  private static String json(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
