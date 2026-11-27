package org.streamrune.runtime.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.Internal;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.EncryptedTypeGuard;

/**
 * The append side of an event store, shared by the backends: the argument rules of an append, a
 * keyed append and a snapshot save; the serialization of event rows and snapshot states; the outbox
 * entries an append derives and their trace links; the audit entries it writes; the head check; the
 * replay of a lost inbox claim; the mapping of a failed append to the exception it reports; and the
 * spans of an append, its serialization and a snapshot save.
 *
 * <p><b>What stays in the store.</b> The transaction: borrowing the connection, the statements and
 * their order, the commit, the rollback and the close. The store also keeps its own "untraced
 * when…" gates (whether to call {@link #tracedAppend} or {@link #tracedSaveSnapshot} at all), the
 * per-event serialization loop around {@link #serializeRow}, and the fallback exception of an
 * unkeyed append no mapping classified.
 *
 * <p><b>A committed append is never a failure.</b> {@link #appendConflict} and {@link
 * #keyedAppendFailure} are called only for an append that did not commit: once the commit returned,
 * the events are durable, and a failure of anything after it (closing the connection) returns the
 * append's result instead. They return the exception and never throw it, so the store decides
 * whether, and where, to throw.
 *
 * <p><b>Helper rules.</b> It never opens, commits, rolls back or closes a connection, and never
 * wraps a {@link SQLException}, except in the two mapping methods, which return the mapped
 * exception with the {@link SQLException} as its cause. It logs through the store's {@link Logger},
 * so log lines keep the store's category, and traces through the store's already-guarded {@link
 * StreamRuneTracing}. It keeps no state of its own beyond its constructor arguments.
 *
 * @hidden
 */
@Internal
public final class EventAppendSupport {

  private final ObjectMapper objectMapper;
  private final EventTypeRegistry typeRegistry;
  private final StreamRuneTracing tracing;
  private final Logger log;

  /**
   * One event's serialized row, ready for the insert.
   *
   * @param version the stream version the event is stored at
   * @param eventType the {@code event_type} column
   * @param payloadJson the serialized event
   * @param metadataJson the serialized metadata
   * @param schemaVersion the {@code schema_version} column
   */
  public record EventRow(
      long version, String eventType, String payloadJson, String metadataJson, int schemaVersion) {}

  /**
   * A snapshot state, validated and serialized, ready for the upsert.
   *
   * @param stateType the {@code state_type} column: the state class's simple name
   * @param stateJson the serialized state
   */
  public record SnapshotPayload(String stateType, String stateJson) {}

  /** A backend's check of serialized JSON before it is stored; it throws to refuse the JSON. */
  @FunctionalInterface
  public interface JsonGuard {

    /** A guard that accepts every JSON text, for a backend that stores any. */
    JsonGuard NONE = (json, what) -> {};

    /**
     * Checks {@code json}; throws to refuse it.
     *
     * @param json the serialized JSON
     * @param what what the JSON represents, for the error message
     */
    void check(String json, String what);
  }

  /** The store's serialization of an append's rows, which {@link #serialize} runs. */
  @FunctionalInterface
  public interface RowsSerializer {

    /**
     * Serializes the rows.
     *
     * @return the rows, in event order
     * @throws JsonProcessingException if an event or its metadata cannot be serialized
     */
    List<EventRow> serialize() throws JsonProcessingException;
  }

  /**
   * The append support of one store instance.
   *
   * @param objectMapper the mapper of events, metadata and snapshot states
   * @param typeRegistry resolves a snapshot's state type
   * @param guardedTracing the store's tracing, already guarded; never guarded here
   * @param log the store's logger, so every log line keeps the store's category
   */
  public EventAppendSupport(
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      StreamRuneTracing guardedTracing,
      Logger log) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper is required");
    this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry is required");
    this.tracing = Objects.requireNonNull(guardedTracing, "guardedTracing is required");
    this.log = Objects.requireNonNull(log, "log is required");
  }

  // ── Spans ─────────────────────────────────────────────────────────────────────────────────────

  /**
   * Runs {@code append} inside the span {@value TraceAttributes#EVENT_STORE_APPEND}, which carries
   * the stream, its aggregate type, the event count, the expected version and the outbox entry
   * count. The outcome is {@code outcome}'s answer for the result; a version conflict ({@link
   * OptimisticLockException}) is the outcome {@code CONFLICT}, and a saga's forward command the
   * fence refuses ({@link SagaForwardFencedException}) the outcome {@code FENCED}; neither is an
   * error. Any other failure is recorded as the span's error. Always traces: the store decides
   * whether to call it.
   *
   * @param streamId the stream appended to
   * @param eventCount the number of events
   * @param expectedVersion the version the append expects
   * @param outboxEntryCount the number of outbox entries
   * @param append the append
   * @param outcome the outcome of a successful append
   * @param <R> the append's result
   * @return the append's result
   */
  public <R> R tracedAppend(
      StreamId streamId,
      int eventCount,
      Version expectedVersion,
      int outboxEntryCount,
      Supplier<R> append,
      Function<R, String> outcome) {
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_APPEND,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
                .put(TraceAttributes.AGGREGATE_TYPE, streamId.aggregateType().value())
                .put(TraceAttributes.EVENT_COUNT, (long) eventCount)
                .put(TraceAttributes.EXPECTED_VERSION, expectedVersion.value())
                .put(TraceAttributes.OUTBOX_ENTRY_COUNT, (long) outboxEntryCount)
                .build());
    try {
      R result = append.get();
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, outcome.apply(result));
      scope.recordSuccess();
      return result;
    } catch (OptimisticLockException conflict) {
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, "CONFLICT");
      throw conflict;
    } catch (SagaForwardFencedException fenced) {
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, "FENCED");
      throw fenced;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * Runs the store's serialization of an append's rows, inside the span {@value
   * TraceAttributes#EVENT_STORE_APPEND_SERIALIZE}, carrying the event count, when {@code traced}:
   * the append then runs inside its own span. Serialization runs before the store borrows a
   * connection, so a slow encryption of an {@code @Encrypted} field never holds one.
   *
   * @param eventCount the number of events, for the span
   * @param traced whether the append runs inside its span
   * @param rows the store's serialization
   * @return the rows
   * @throws JsonProcessingException if {@code rows} throws it
   */
  public List<EventRow> serialize(int eventCount, boolean traced, RowsSerializer rows)
      throws JsonProcessingException {
    if (!traced) {
      return rows.serialize();
    }
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_APPEND_SERIALIZE,
            SpanKind.INTERNAL,
            SpanAttributes.builder().put(TraceAttributes.EVENT_COUNT, (long) eventCount).build());
    try {
      List<EventRow> serialized = rows.serialize();
      scope.recordSuccess();
      return serialized;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * Runs {@code write} inside the span {@value TraceAttributes#EVENT_STORE_SNAPSHOT_SAVE}, carrying
   * the stream and the version, with the outcome {@code SAVED} when it stored the snapshot and
   * {@code SKIPPED} when it did not; a failure is recorded as the span's error. Always traces: the
   * store decides whether to call it.
   *
   * @param streamId the stream
   * @param version the stream version the snapshot was taken at
   * @param write the save
   * @return what {@code write} returned
   */
  public boolean tracedSaveSnapshot(StreamId streamId, Version version, BooleanSupplier write) {
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
                .put(TraceAttributes.STREAM_VERSION, version.value())
                .build());
    try {
      boolean saved = write.getAsBoolean();
      scope.setAttribute(TraceAttributes.SNAPSHOT_OUTCOME, saved ? "SAVED" : "SKIPPED");
      scope.recordSuccess();
      return saved;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  // ── Serialization ─────────────────────────────────────────────────────────────────────────────

  /**
   * Serializes one event into its row, in this order: the payload, the metadata, {@code guard} on
   * the payload ({@code "event payload"}), {@code guard} on the metadata ({@code "event
   * metadata"}), then {@code schemaVersion}. A failure to serialize the metadata therefore wins
   * over a guard that would refuse the payload. A store that computes the schema version before it
   * serializes passes a supplier of the value it computed; a store that computes it after passes
   * the computation itself. Each keeps its own loop over the events.
   *
   * @param envelope the event
   * @param version the stream version the event is stored at
   * @param schemaVersion the event type's current schema version
   * @param guard the backend's check of the serialized JSON
   * @return the row
   * @throws JsonProcessingException if the payload or the metadata cannot be serialized
   */
  public EventRow serializeRow(
      EventEnvelope envelope, long version, IntSupplier schemaVersion, JsonGuard guard)
      throws JsonProcessingException {
    String payloadJson = objectMapper.writeValueAsString(envelope.event());
    String metadataJson = objectMapper.writeValueAsString(envelope.metadata());
    guard.check(payloadJson, "event payload");
    guard.check(metadataJson, "event metadata");
    int currentSchemaVersion = schemaVersion.getAsInt();
    return new EventRow(
        version, envelope.eventType().value(), payloadJson, metadataJson, currentSchemaVersion);
  }

  /**
   * Validates and serializes a snapshot state: its simple name must be a valid state type, resolve
   * in the registry, and resolve to the state's own class, since snapshots are stored by simple
   * name; then the state is serialized and {@code guard} checks it ({@code "snapshot state"}).
   *
   * @param state the state to save
   * @param shownStream the stream, already sanitized for a message
   * @param guard the backend's check of the serialized JSON
   * @return the state type and the serialized state
   * @throws EventStoreException if the type does not resolve, resolves to another class, or the
   *     state cannot be serialized
   */
  public SnapshotPayload serializeSnapshot(
      AggregateState state, String shownStream, JsonGuard guard) {
    String stateType = state.getClass().getSimpleName();
    EventStore.requireStateType(stateType);
    Class<?> resolved;
    try {
      resolved = typeRegistry.resolveStateType(stateType);
    } catch (RuntimeException e) {
      throw new EventStoreException(
          "Snapshot state type '"
              + stateType
              + "' does not resolve in the EventTypeRegistry — register "
              + state.getClass().getName()
              + " under its simple name so the snapshot can be rehydrated on load",
          e);
    }
    if (resolved != state.getClass()) {
      throw new EventStoreException(
          "Snapshot state type '"
              + stateType
              + "' resolves to "
              + resolved.getName()
              + " in the EventTypeRegistry, but the state being saved is "
              + state.getClass().getName()
              + " — snapshots are stored by simple class name, so two state classes sharing a"
              + " simple name would rehydrate as the wrong type");
    }
    String stateJson;
    try {
      stateJson = objectMapper.writeValueAsString(state);
    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize snapshot for stream: " + shownStream, e);
    }
    guard.check(stateJson, "snapshot state");
    return new SnapshotPayload(stateType, stateJson);
  }

  /**
   * Logs, at INFO on the store's logger, a snapshot save the purge generation refused: a GDPR
   * forget purged the stream's snapshot after the saved state was loaded.
   *
   * @param shownStream the stream, already sanitized for a log line
   * @param version the stream version of the refused snapshot
   * @param snapshotPurgeGeneration the purge generation the save expected
   * @return {@code false}, what the refused save returns
   */
  public boolean snapshotNotStored(
      String shownStream, Version version, long snapshotPurgeGeneration) {
    log.info(
        "Snapshot of stream {} at version {} not stored: its snapshot_store row carries"
            + " a purge generation other than {}, so a GDPR forget purged the stream's"
            + " snapshot after the state being saved was loaded; the next command loads and"
            + " snapshots again",
        shownStream,
        version.value(),
        snapshotPurgeGeneration);
    return false;
  }

  // ── Argument rules ────────────────────────────────────────────────────────────────────────────

  /**
   * The rules an append checks before anything else: the four arguments are required; outbox
   * entries need a configured outbox store; and an empty append is a no-op only without outbox
   * entries, which commit with the events and cannot be appended alone.
   *
   * @param streamId the stream
   * @param events the events
   * @param expectedVersion the version the append expects
   * @param outboxEntries the outbox entries
   * @param outboxStoreConfigured whether the store has an outbox store
   * @param outboxNotConfiguredMessage the store's message for entries without an outbox store
   * @return the result of an empty append, or empty when the append goes on
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalStateException if entries are given and no outbox store is configured
   * @throws IllegalArgumentException if entries are given with no events
   */
  public static Optional<AppendResult> preamble(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      List<OutboxEntry> outboxEntries,
      boolean outboxStoreConfigured,
      String outboxNotConfiguredMessage) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(events, "events is required");
    Objects.requireNonNull(expectedVersion, "expectedVersion is required");
    Objects.requireNonNull(outboxEntries, "outboxEntries is required");
    if (!outboxEntries.isEmpty() && !outboxStoreConfigured) {
      throw new IllegalStateException(outboxNotConfiguredMessage);
    }
    if (!events.isEmpty()) {
      return Optional.empty();
    }
    // The no-op returns before any transaction opens; entries in hand would be lost silently.
    if (!outboxEntries.isEmpty()) {
      throw new IllegalArgumentException(
          "outboxEntries were passed with an empty events list — outbox entries are persisted"
              + " atomically with the events of this append and cannot be appended alone. To"
              + " emit an outbox entry that has no accompanying domain event, persist it"
              + " directly with OutboxStore.save(entry), one entry per call — that write commits"
              + " on its own connection and is atomic with nothing else.");
    }
    return Optional.of(new AppendResult(List.of(), expectedVersion));
  }

  /**
   * The argument rules of a keyed append: the stream, the events, the expected version and the key
   * are required, and the command type must be a valid one. A store built without a command inbox
   * refuses the keyed append before these, with a message naming its own class.
   *
   * @param streamId the stream
   * @param events the events
   * @param expectedVersion the version the append expects
   * @param idempotencyKey the command's key
   * @param commandType the command's type
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalArgumentException if the command type is too long
   */
  public static void requireKeyedArguments(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(events, "events is required");
    Objects.requireNonNull(expectedVersion, "expectedVersion is required");
    Objects.requireNonNull(idempotencyKey, "idempotencyKey is required");
    CommandInbox.requireCommandType(commandType);
  }

  /**
   * The rules a snapshot save checks before the version: the stream is required, the state must be
   * present, the snapshot version at least {@code 1} and the purge generation not negative.
   *
   * @param streamId the stream
   * @param state the state
   * @param snapshotVersion the snapshot version
   * @param snapshotPurgeGeneration the purge generation the save expects
   * @throws NullPointerException if {@code streamId} is {@code null}
   * @throws IllegalArgumentException if another argument breaks its rule
   */
  public static void requireSnapshotArguments(
      StreamId streamId, AggregateState state, int snapshotVersion, long snapshotPurgeGeneration) {
    Objects.requireNonNull(streamId, "streamId is required");
    if (state == null) {
      throw new IllegalArgumentException("state must not be null");
    }
    if (snapshotVersion < 1) {
      throw new IllegalArgumentException("snapshotVersion must be >= 1");
    }
    if (snapshotPurgeGeneration < 0) {
      throw new IllegalArgumentException(
          "snapshotPurgeGeneration must not be negative, got " + snapshotPurgeGeneration);
    }
  }

  /**
   * Refuses a configuration that would persist {@code @Encrypted} components as plaintext: the
   * registry is required, and {@link EncryptedTypeGuard} checks its types against the engine.
   *
   * @param typeRegistry the registry the store resolves types with
   * @param cryptoEngine the configured engine, or {@code null}
   * @throws IllegalArgumentException if {@code typeRegistry} is {@code null}
   * @throws IllegalStateException if {@code cryptoEngine} is {@code null} and a registered type
   *     carries {@code @Encrypted}
   * @throws UnsupportedOperationException if {@code cryptoEngine} is {@code null} and the registry
   *     cannot list its types
   */
  public static void validateCryptoConfig(
      EventTypeRegistry typeRegistry, CryptoEngine cryptoEngine) {
    if (typeRegistry == null) {
      throw new IllegalArgumentException("typeRegistry is required");
    }
    EncryptedTypeGuard.check(typeRegistry, cryptoEngine);
  }

  // ── Outbox entries and audit entries ──────────────────────────────────────────────────────────

  /**
   * The outbox entries the configured mapper derives from {@code events}, each with the trace link
   * of the event it was derived from, whatever the mapper set (see {@link #withTraceLink}); none
   * without an outbox store or without a mapper.
   *
   * @param events the events
   * @param outboxStoreConfigured whether the store has an outbox store
   * @param mapperOrNull the store's mapper, or {@code null}
   * @return the entries, in event order
   */
  public static List<OutboxEntry> outboxEntriesOf(
      List<EventEnvelope> events, boolean outboxStoreConfigured, OutboxEventMapper mapperOrNull) {
    return outboxStoreConfigured && mapperOrNull != null
        ? events.stream()
            .flatMap(e -> withTraceLink(mapperOrNull.toOutbox(e), traceLinkOf(e)).stream())
            .toList()
        : List.of();
  }

  /**
   * The link to the span of the command that wrote {@code event}: its {@code spanContext}, via
   * {@link SpanLink#of}; {@link SpanLink#NONE} when the command was not traced or there is no
   * event.
   *
   * @param eventOrNull the event, or {@code null}
   * @return the link
   */
  public static SpanLink traceLinkOf(EventEnvelope eventOrNull) {
    return eventOrNull == null ? SpanLink.NONE : SpanLink.of(eventOrNull.metadata());
  }

  /**
   * {@code entries}, each with {@code link} as its trace link, replacing any link a mapper or a
   * caller set, so every stored link is the span context of the source event. {@code entries}
   * itself when every entry already carries {@code link}, the untraced case with no link set.
   *
   * @param entries the entries
   * @param link the link each entry takes
   * @return the linked entries
   */
  public static List<OutboxEntry> withTraceLink(List<OutboxEntry> entries, SpanLink link) {
    List<OutboxEntry> linked = null;
    for (int i = 0; i < entries.size(); i++) {
      OutboxEntry entry = entries.get(i);
      if (!entry.traceLink().equals(link)) {
        if (linked == null) {
          linked = new ArrayList<>(entries);
        }
        linked.set(i, entry.withTraceLink(link));
      }
    }
    return linked == null ? entries : linked;
  }

  /**
   * The event audit entries of an append. The stream and the versions are the append's arguments,
   * as the inserted events' are, never the envelope's, so the audit rows mirror {@code
   * event_stream} even when a caller's envelopes name another stream or version; the rest comes
   * from each event's metadata.
   *
   * @param streamId the stream appended to
   * @param events the events, in order
   * @param expectedVersion the version the append expects; the first event is stored after it
   * @return the entries, in event order
   */
  public static List<EventAuditEntry> auditEntries(
      StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
    List<EventAuditEntry> entries = new ArrayList<>(events.size());
    long version = expectedVersion.value();
    for (EventEnvelope envelope : events) {
      version++;
      EventMetadata metadata = envelope.metadata();
      entries.add(
          new EventAuditEntry(
              metadata.eventId(),
              envelope.eventType(),
              streamId,
              new Version(version),
              metadata.commandId(),
              metadata.correlationId(),
              metadata.causationId(),
              metadata.userId(),
              metadata.timestamp()));
    }
    return entries;
  }

  // ── Head check, lost claims and failures ──────────────────────────────────────────────────────

  /**
   * The strict head check of an append: the stream's stored head must be the version the append
   * expects, below it as well as above it.
   *
   * @param streamId the stream
   * @param expectedVersion the version the append expects
   * @param actualHead the stream's stored head, read inside the append's transaction
   * @throws OptimisticLockException if they differ
   */
  public static void requireExpectedHead(
      StreamId streamId, Version expectedVersion, long actualHead) {
    if (actualHead != expectedVersion.value()) {
      throw new OptimisticLockException(
          "Version conflict on stream '"
              + LogSanitizer.sanitizeForLog(streamId.value())
              + "': expected version "
              + expectedVersion.value()
              + " but actual head is "
              + actualHead);
    }
  }

  /**
   * The claim-lost dead end: the key's claim was lost, yet the recorded row is gone (retention
   * pruned it in between); a retry claims the key anew. The key renders through {@link
   * LogSanitizer#sanitizeForLog}: it is caller-supplied, and this message is logged and persisted.
   *
   * @param idempotencyKey the key
   * @return the exception
   */
  public static EventStoreException inboxConflictWithoutRow(IdempotencyKey idempotencyKey) {
    return new EventStoreException(
        "Inbox conflict but no row found for key: "
            + LogSanitizer.sanitizeForLog(idempotencyKey.value()));
  }

  /**
   * The result of a keyed append whose claim another execution won: the recorded row's offsets and
   * final version, already applied. Fails closed unless the row was recorded by the same command on
   * the same stream. The store calls it after its append connection is returned, so a claim loser
   * never holds one connection while the inbox borrows another.
   *
   * <p>Without the check, the claim loser would be handed the winner's offsets and final version
   * with {@code alreadyApplied=true}: a fabricated success for a command whose events were never
   * appended and never will be (the key stays claimed), carrying another stream's metadata back to
   * the caller. The command bus's sequential pre-check refuses these same inputs, so this
   * concurrent path reaches the same verdict through the same shared guard ({@link
   * CommandInbox.InboxResult#requireBoundTo}). The refusal is an {@link IllegalArgumentException},
   * not an {@link EventStoreException}, on purpose: the bus classifies it as deterministic (its
   * default dead-letter eligibility excludes it), so a colliding key is neither retried nor
   * dead-lettered against a conflict that can never resolve.
   *
   * @param inbox the store's command inbox
   * @param key the key
   * @param commandType the command type presenting the key
   * @param streamId the stream the presenting command targets
   * @return the recorded result
   * @throws EventStoreException if no row is recorded under the key
   * @throws IllegalArgumentException if the row was recorded for another command or stream
   */
  public static IdempotentAppendResult replayLostClaim(
      CommandInbox inbox, IdempotencyKey key, String commandType, StreamId streamId) {
    CommandInbox.InboxResult prior =
        inbox.find(key).orElseThrow(() -> inboxConflictWithoutRow(key));
    prior.requireBoundTo(commandType, streamId);
    return new IdempotentAppendResult(prior.globalOffsets(), prior.finalVersion(), true);
  }

  /**
   * The exception an unkeyed append that did not commit reports for {@code e}, or {@code null} when
   * none of these applies and the store reports its own: a collision on {@code global_offset} is a
   * desynced counter ({@link EventStoreException}), a retryable conflict a version conflict ({@link
   * OptimisticLockException}), a lock failure the classifier's exception. The mapped exception has
   * {@code e} as its cause. Never call it for an append that committed.
   *
   * @param e the failure
   * @param classifier the backend's classifier
   * @param shownStream the stream, already sanitized for a message
   * @param expectedVersion the version the append expected
   * @return the exception to throw, or {@code null}
   */
  public static RuntimeException appendConflict(
      SQLException e,
      AppendFailureClassifier classifier,
      String shownStream,
      Version expectedVersion) {
    if (classifier.isGlobalOffsetCollision(e)) {
      return new EventStoreException(classifier.globalOffsetDesyncMessage(shownStream), e);
    }
    if (classifier.isRetryableConflict(e)) {
      return new OptimisticLockException(
          "Version conflict on stream '"
              + shownStream
              + "' at expected version "
              + expectedVersion.value(),
          e);
    }
    return classifier.lockFailure(e, "append to stream '" + shownStream + "'");
  }

  /**
   * The exception a keyed append that did not commit reports for {@code e}: a collision on {@code
   * global_offset} is a desynced counter, a retryable conflict a concurrent modification ({@link
   * OptimisticLockException}), a lock failure the classifier's exception, anything else a plain
   * {@link EventStoreException}. The mapped exception has {@code e} as its cause. Never {@code
   * null}; never call it for an append that committed.
   *
   * @param e the failure
   * @param classifier the backend's classifier
   * @param shownStream the stream, already sanitized for a message
   * @return the exception to throw
   */
  public static RuntimeException keyedAppendFailure(
      SQLException e, AppendFailureClassifier classifier, String shownStream) {
    if (classifier.isGlobalOffsetCollision(e)) {
      return new EventStoreException(classifier.globalOffsetDesyncMessage(shownStream), e);
    }
    if (classifier.isRetryableConflict(e)) {
      return new OptimisticLockException("Concurrent modification of stream: " + shownStream, e);
    }
    EventStoreException lockFailure =
        classifier.lockFailure(e, "idempotent append to stream '" + shownStream + "'");
    if (lockFailure != null) {
      return lockFailure;
    }
    return new EventStoreException("Failed idempotent append to stream: " + shownStream, e);
  }
}
