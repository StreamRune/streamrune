package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The spans of {@link PostgresEventStore} and {@link PgAdvisoryLocker} against PostgreSQL, through
 * {@link RecordingTracing}: names, kinds, exact attribute keys, outcomes, errors and nesting, the
 * span tree of a command, each area switched off, and none for NOOP or a read that returns nothing.
 * This module's test classpath holds no OpenTelemetry jar, so every test here also shows that the
 * store and the locker build and run without one.
 */
@Testcontainers
class PostgresEventStoreTracingTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("tracing_test");

  private static final AggregateType LAMP = AggregateType.of("lamp");
  private static final StreamId STREAM = StreamId.of(LAMP, AggregateId.of("l-1"));

  static PGSimpleDataSource dataSource;

  record Switched(String lampId) implements DomainEvent {}

  /** Serializes to {@code {"x": ...}}, which {@link Switched} cannot bind. */
  record Renamed(String x) implements DomainEvent {}

  record LampState(int switches) implements AggregateState {}

  record Switch(String lampId) implements Command {}

  static final EventTypeRegistry REGISTRY =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          return Switched.class;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          return LampState.class;
        }

        @Override
        public Collection<Class<?>> registeredTypes() {
          return List.of(Switched.class, LampState.class);
        }
      };

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    new PostgresEventStoreFactory(dataSource, REGISTRY).create();
  }

  @BeforeEach
  void clean() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM snapshot_store");
      stmt.execute("DELETE FROM command_inbox");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  private static PostgresEventStore store(StreamRuneTracing tracing) {
    return PostgresEventStore.builder()
        .dataSource(dataSource)
        .typeRegistry(REGISTRY)
        .commandInbox(new PostgresCommandInbox(dataSource))
        .tracing(tracing)
        .build();
  }

  private static EventEnvelope event(DomainEvent payload) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        STREAM,
        Version.initial(),
        EventType.of("Switched"),
        payload,
        new EventMetadata(
            EventId.of("evt-1"),
            CommandId.of("cmd-1"),
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"),
            Map.of()));
  }

  private static EventEnvelope switched() {
    return event(new Switched("l-1"));
  }

  private static void assertFailed(RecordingTracing.RecordedSpan span) {
    assertThat(span.error).as(span.name).isNotNull();
    assertThat(span.ok).isFalse();
    assertThat(span.closed).isTrue();
  }

  @Test
  void append_startsTheAppendSpan_withTheSerializeSpanUnderIt() {
    var tracing = new RecordingTracing();

    store(tracing).append(STREAM, List.of(switched(), switched()), Version.initial());

    var append = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    var serialize = tracing.only(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    assertThat(append.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(append.attributes())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                TraceAttributes.STREAM_ID,
                "lamp:l-1",
                TraceAttributes.AGGREGATE_TYPE,
                "lamp",
                TraceAttributes.EVENT_COUNT,
                2L,
                TraceAttributes.EXPECTED_VERSION,
                0L,
                TraceAttributes.OUTBOX_ENTRY_COUNT,
                0L,
                TraceAttributes.APPEND_OUTCOME,
                "APPENDED"));
    assertThat(append.ok).isTrue();
    assertThat(append.closed).isTrue();
    assertThat(append.parent).isNull();
    assertThat(serialize.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(serialize.attributes())
        .containsExactlyEntriesOf(Map.of(TraceAttributes.EVENT_COUNT, 2L));
    assertThat(serialize.parent).isSameAs(append);
    assertThat(serialize.ok).isTrue();
  }

  @Test
  void aConflict_isTheOutcomeConflict_andARecordedKeyTheOutcomeDuplicate() {
    var tracing = new RecordingTracing();
    PostgresEventStore store = store(tracing);
    IdempotencyKey key = IdempotencyKey.of("key-1");
    store.appendWithKey(STREAM, List.of(switched()), Version.initial(), key, "Switch");
    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_APPEND)
                .attribute(TraceAttributes.APPEND_OUTCOME))
        .isEqualTo("APPENDED");
    tracing.clear();

    store.appendWithKey(STREAM, List.of(switched()), Version.initial(), key, "Switch");
    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_APPEND)
                .attribute(TraceAttributes.APPEND_OUTCOME))
        .isEqualTo("DUPLICATE");
    tracing.clear();

    assertThatThrownBy(() -> store.append(STREAM, List.of(switched()), Version.initial()))
        .isInstanceOf(OptimisticLockException.class);
    var conflict = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(conflict.attribute(TraceAttributes.APPEND_OUTCOME)).isEqualTo("CONFLICT");
    assertThat(conflict.error).isNull();
    assertThat(conflict.ok).isFalse();
    assertThat(conflict.closed).isTrue();
  }

  @Test
  void aForwardCommandTheFenceRefuses_isTheOutcomeFenced_notAnError() {
    var tracing = new RecordingTracing();
    PostgresEventStore store = store(tracing);
    // No saga_state row: the fence refuses the forward command of a saga not in its forward phase.
    SagaOrigin forwardOf =
        SagaOrigin.dispatchedBy(SagaType.of("order-saga"), SagaId.of("saga-fenced"));

    assertThatThrownBy(
            () ->
                store.appendWithKey(
                    STREAM,
                    List.of(switched()),
                    Version.initial(),
                    IdempotencyKey.of("k-fenced"),
                    "Switch",
                    forwardOf))
        .isInstanceOf(SagaForwardFencedException.class);

    var append = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(append.attribute(TraceAttributes.APPEND_OUTCOME)).isEqualTo("FENCED");
    assertThat(append.error).isNull();
    assertThat(append.closed).isTrue();
  }

  @Test
  void load_startsTheLoadSpan_withTheSnapshotAndDeserializeSpansUnderIt_andAnOutcomePerSnapshot() {
    var tracing = new RecordingTracing();
    PostgresEventStore store = store(tracing);
    store.append(STREAM, List.of(switched(), switched()), Version.initial());
    tracing.clear();

    store.load(STREAM, 1);

    // No stored snapshot: no snapshot span, and the load says it used none.
    var load = tracing.only(TraceAttributes.EVENT_STORE_LOAD);
    var deserialize = tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(load.attributes())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                TraceAttributes.STREAM_ID,
                "lamp:l-1",
                TraceAttributes.AGGREGATE_TYPE,
                "lamp",
                TraceAttributes.EVENT_COUNT,
                2L,
                TraceAttributes.SNAPSHOT_USED,
                false));
    assertThat(deserialize.attributes())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(TraceAttributes.EVENT_COUNT, 2L, TraceAttributes.UPCAST_COUNT, 0L));
    assertThat(deserialize.parent).isSameAs(load);
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD, TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);

    store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 0);
    tracing.clear();
    store.load(STREAM, 1);
    var hit = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    assertThat(hit.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(hit.attributes())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                TraceAttributes.STREAM_ID, "lamp:l-1",
                TraceAttributes.STREAM_VERSION, 1L,
                TraceAttributes.SNAPSHOT_OUTCOME, "HIT"));
    assertThat(hit.parent).isSameAs(tracing.only(TraceAttributes.EVENT_STORE_LOAD));
    assertThat(
            tracing.only(TraceAttributes.EVENT_STORE_LOAD).attribute(TraceAttributes.SNAPSHOT_USED))
        .isEqualTo(true);
    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE)
                .attribute(TraceAttributes.EVENT_COUNT))
        .isEqualTo(1L);

    // Stored at snapshot version 1, expected at 2, no migration chain: unusable, full replay.
    tracing.clear();
    store.load(STREAM, 2);
    var discarded = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    assertThat(discarded.attribute(TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("DISCARDED");
    assertThat(discarded.attribute(TraceAttributes.STREAM_VERSION)).isEqualTo(0L);
  }

  @Test
  void anUndeserializableSnapshot_isDiscarded_andTheStreamReplaysUnderTheLoad() throws Exception {
    var tracing = new RecordingTracing();
    PostgresEventStore store = store(tracing);
    store.append(STREAM, List.of(switched(), switched()), Version.initial());
    store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 0);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("UPDATE snapshot_store SET state_payload = '{\"unknown\": 1}'");
    }
    tracing.clear();

    store.load(STREAM, 1);

    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD)
                .attribute(TraceAttributes.SNAPSHOT_OUTCOME))
        .isEqualTo("DISCARDED");
    var deserialize = tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(deserialize.attribute(TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(deserialize.parent).isSameAs(tracing.only(TraceAttributes.EVENT_STORE_LOAD));
  }

  @Test
  void aLoadThatIgnoresSnapshots_startsNoSnapshotSpan_andCountsUpcasts() {
    store(StreamRuneTracing.NOOP).append(STREAM, List.of(switched()), Version.initial());
    var tracing = new RecordingTracing();
    PostgresEventStore upcasting =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(REGISTRY)
            .upcasters(
                List.of(
                    new EventUpcaster() {
                      @Override
                      public EventType eventType() {
                        return EventType.of("Switched");
                      }

                      @Override
                      public int currentVersion() {
                        return 2;
                      }

                      @Override
                      public Map<String, Object> upcast(
                          Map<String, Object> eventData, int fromVersion) {
                        return eventData;
                      }
                    }))
            .tracing(tracing)
            .build();
    upcasting.append(STREAM, List.of(switched()), new Version(1));
    tracing.clear();

    upcasting.load(STREAM, EventStore.IGNORE_SNAPSHOT);

    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD, TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    var deserialize = tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(deserialize.attribute(TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(deserialize.attribute(TraceAttributes.UPCAST_COUNT)).isEqualTo(1L);
  }

  @Test
  void aFailedDeserialization_marksTheDeserializeTheLoadAndTheReadSpansFailed() {
    var tracing = new RecordingTracing();
    PostgresEventStore store = store(tracing);
    store.append(STREAM, List.of(event(new Renamed("x"))), Version.initial());
    tracing.clear();

    assertThatThrownBy(() -> store.load(STREAM, 1))
        .isInstanceOf(EventDeserializationException.class);
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_LOAD));
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE));
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD).attributes())
        .doesNotContainKeys(TraceAttributes.EVENT_COUNT, TraceAttributes.SNAPSHOT_USED);
    tracing.clear();

    tracing.inside(
        "request",
        () ->
            assertThatThrownBy(() -> store.readGlobalStream(GlobalOffset.initial(), 10))
                .isInstanceOf(EventDeserializationException.class));
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_READ));
  }

  @Test
  void aFailedAppend_marksTheAppendSpanFailed() {
    var tracing = new RecordingTracing();
    PostgresEventStore store = store(tracing);

    // A NUL character cannot be stored in jsonb: refused after the events were serialized.
    assertThatThrownBy(
            () -> store.append(STREAM, List.of(event(new Switched("\u0000"))), Version.initial()))
        .isInstanceOf(RuntimeException.class);

    var append = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertFailed(append);
    assertThat(append.attributes()).doesNotContainKey(TraceAttributes.APPEND_OUTCOME);
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE));
  }

  @Test
  void saveSnapshot_isTheOutcomeSaved_orSkippedAfterAPurge() {
    var tracing = new RecordingTracing();
    PostgresEventStore store = store(tracing);
    store.append(STREAM, List.of(switched()), Version.initial());
    tracing.clear();

    store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 0);

    var save = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);
    assertThat(save.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(save.attributes())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                TraceAttributes.STREAM_ID, "lamp:l-1",
                TraceAttributes.STREAM_VERSION, 1L,
                TraceAttributes.SNAPSHOT_OUTCOME, "SAVED"));
    tracing.clear();

    // A purge generation other than the row's: a purge intervened, nothing is written.
    assertThat(store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 7)).isFalse();
    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE)
                .attribute(TraceAttributes.SNAPSHOT_OUTCOME))
        .isEqualTo("SKIPPED");
    tracing.clear();

    assertThatThrownBy(() -> store.saveSnapshot(STREAM, new Version(1), new OtherState(), 1, 0))
        .isInstanceOf(RuntimeException.class);
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE));
  }

  record OtherState() implements AggregateState {}

  @Test
  void aReadThatReturnsEvents_startsTheReadSpanUnderTheCurrentSpan_andAnEmptyReadStartsNone() {
    var tracing = new RecordingTracing();
    PostgresEventStore store = store(tracing);
    store.append(STREAM, List.of(switched(), switched()), Version.initial());
    tracing.clear();

    tracing.inside(
        "request",
        () -> {
          assertThat(store.readGlobalStream(GlobalOffset.of(2), 10)).isEmpty();
          assertThat(store.readStream(STREAM, new Version(2), 10)).isEmpty();
        });
    assertThat(tracing.names()).containsExactly("request");
    tracing.clear();

    // A read with no span current, the poll of a subscription or a projection runner: no span.
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 10)).hasSize(2);
    assertThat(store.readStream(STREAM, new Version(1), 10)).hasSize(1);
    assertThat(tracing.names()).isEmpty();

    Instant before = Instant.now();
    var request =
        tracing.inside("request", () -> store.readGlobalStream(GlobalOffset.initial(), 10));
    var global = tracing.only(TraceAttributes.EVENT_STORE_READ);
    assertThat(global.parent).isSameAs(request);
    // The span starts when the query started, before the events were known to exist.
    assertThat(global.startTime)
        .isNotNull()
        .isAfterOrEqualTo(before)
        .isBeforeOrEqualTo(Instant.now());
    assertThat(global.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(global.attributes())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(TraceAttributes.EVENT_COUNT, 2L, TraceAttributes.READ_KIND, "GLOBAL"));
    tracing.clear();

    tracing.inside("request", () -> store.readStream(STREAM, new Version(1), 10));
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_READ).startTime).isNotNull();
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_READ).attributes())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(TraceAttributes.EVENT_COUNT, 1L, TraceAttributes.READ_KIND, "STREAM"));
  }

  @Test
  void withTheEventStoreAreaOff_orNoop_noSpanStarts_andTheArgumentChecksAreUnchanged() {
    var off = RecordingTracing.without(TracingArea.EVENT_STORE);
    for (StreamRuneTracing tracing : List.of(off, StreamRuneTracing.NOOP)) {
      PostgresEventStore store = store(tracing);
      store.append(STREAM, List.of(switched()), Version.initial());
      store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 0);
      store.load(STREAM, 1);
      store.readGlobalStream(GlobalOffset.initial(), 10);
      store.readStream(STREAM, Version.initial(), 10);
      clearQuietly();
    }
    assertThat(off.names()).isEmpty();

    var traced = new RecordingTracing();
    PostgresEventStore store = store(traced);
    assertThatThrownBy(() -> store.append(null, List.of(switched()), Version.initial()))
        .hasMessage("streamId is required");
    assertThatThrownBy(() -> store.append(STREAM, null, Version.initial(), List.of()))
        .hasMessage("events is required");
    assertThatThrownBy(() -> store.append(STREAM, List.of(switched()), Version.initial(), null))
        .hasMessage("outboxEntries is required");
    assertThatThrownBy(() -> store.append(STREAM, List.of(switched()), null))
        .isInstanceOf(NullPointerException.class);
    assertThat(store.append(STREAM, List.of(), Version.initial()).globalOffsets()).isEmpty();
    IdempotencyKey key = IdempotencyKey.of("k");
    assertThatThrownBy(
            () -> store.appendWithKey(null, List.of(switched()), Version.initial(), key, "S"))
        .hasMessage("streamId is required");
    assertThatThrownBy(() -> store.appendWithKey(STREAM, null, Version.initial(), key, "S"))
        .hasMessage("events is required");
    assertThatThrownBy(() -> store.appendWithKey(STREAM, List.of(switched()), null, key, "S"))
        .isInstanceOf(NullPointerException.class);
    assertThat(store.appendWithKey(STREAM, List.of(), Version.initial(), key, "S").alreadyApplied())
        .isFalse();
    assertThatThrownBy(
            () ->
                PostgresEventStore.builder()
                    .dataSource(dataSource)
                    .typeRegistry(REGISTRY)
                    .tracing(traced)
                    .build()
                    .appendWithKey(STREAM, List.of(switched()), Version.initial(), key, "S"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> store.saveSnapshot(STREAM, null, new LampState(1), 1, 0))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> PostgresEventStore.builder().tracing(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new PostgresEventStoreFactory(dataSource, REGISTRY).tracing(null))
        .isInstanceOf(NullPointerException.class);
    assertThat(traced.names()).isEmpty();
  }

  private void clearQuietly() {
    try {
      clean();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void theFactory_passesItsTracingToTheStore() {
    var tracing = new RecordingTracing();
    EventStore store =
        new PostgresEventStoreFactory(dataSource, REGISTRY)
            .autoInitializeSchema(false)
            .tracing(tracing)
            .create();

    store.append(STREAM, List.of(switched()), Version.initial());

    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_APPEND, TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
  }

  // ==================== The span tree of a command ====================

  static final class LampDecider implements Decider<Switch, LampState, Switched> {
    @Override
    public LampState initialState() {
      return new LampState(0);
    }

    @Override
    public List<Switched> decide(Switch command, LampState state) {
      return List.of(new Switched(command.lampId()));
    }

    @Override
    public LampState evolve(LampState state, Switched event) {
      return new LampState(state.switches() + 1);
    }
  }

  private static void runCommand(RecordingTracing tracing) {
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store(tracing))
            .locker(new PgAdvisoryLocker(dataSource).traced(tracing))
            .snapshotPolicy(SnapshotPolicy.everyNEvents(100))
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .tracing(tracing)
            .build()) {
      bus.execute(new Switch("l-1"));
    }
  }

  @Test
  void aCommand_nestsTheLockTheLoadAndTheAppendSpans() {
    var tracing = new RecordingTracing();

    runCommand(tracing);

    var command = tracing.only("command/Switch");
    var lock = tracing.only(TraceAttributes.LOCK_ACQUIRE);
    var load = tracing.only(TraceAttributes.EVENT_STORE_LOAD);
    var append = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(tracing.names())
        .containsExactly(
            "command/Switch",
            TraceAttributes.LOCK_ACQUIRE,
            TraceAttributes.EVENT_STORE_LOAD,
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE,
            TraceAttributes.EVENT_STORE_APPEND,
            TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    assertThat(command.parent).isNull();
    assertThat(lock.parent).isSameAs(command);
    assertThat(load.parent).isSameAs(command);
    assertThat(append.parent).isSameAs(command);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE).parent).isSameAs(load);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE).parent).isSameAs(append);
    assertThat(tracing.spans).allMatch(s -> s.closed && s.ok);
    assertThat(lock.attribute(TraceAttributes.LOCK_OUTCOME)).isEqualTo("ACQUIRED");
  }

  @Test
  void eachAreaSwitchedOff_removesItsSpansAlone() {
    var noStore = RecordingTracing.without(TracingArea.EVENT_STORE);
    runCommand(noStore);
    assertThat(noStore.names()).containsExactly("command/Switch", TraceAttributes.LOCK_ACQUIRE);
    clearQuietly();

    var noLock = RecordingTracing.without(TracingArea.LOCK);
    runCommand(noLock);
    assertThat(noLock.names())
        .contains("command/Switch", TraceAttributes.EVENT_STORE_APPEND)
        .doesNotContain(TraceAttributes.LOCK_ACQUIRE);
  }

  // ==================== PgAdvisoryLocker ====================

  @Test
  void lockAcquire_carriesTheStreamAndTheOutcome_andALockHeldElsewhereIsTheOutcomeTimeout()
      throws Exception {
    var tracing = new RecordingTracing();
    PgAdvisoryLocker untraced = new PgAdvisoryLocker(dataSource);
    PgAdvisoryLocker locker = untraced.traced(tracing);

    try (AutoCloseable held = locker.acquireLock(STREAM, Duration.ofSeconds(5))) {
      var acquired = tracing.only(TraceAttributes.LOCK_ACQUIRE);
      assertThat(acquired.kind).isEqualTo(SpanKind.INTERNAL);
      assertThat(acquired.attributes())
          .containsExactlyInAnyOrderEntriesOf(
              Map.of(
                  TraceAttributes.STREAM_ID, "lamp:l-1", TraceAttributes.LOCK_OUTCOME, "ACQUIRED"));
      assertThat(acquired.ok).isTrue();
      tracing.clear();

      // Another session holds the lock: no wait, and a bounded wait that elapses.
      for (Duration timeout : List.of(Duration.ZERO, Duration.ofMillis(50))) {
        assertThatThrownBy(() -> locker.acquireLock(STREAM, timeout))
            .isInstanceOf(LockException.class);
        var timedOut = tracing.only(TraceAttributes.LOCK_ACQUIRE);
        assertThat(timedOut.attribute(TraceAttributes.LOCK_OUTCOME)).isEqualTo("TIMEOUT");
        assertThat(timedOut.error).isNull();
        assertThat(timedOut.closed).isTrue();
        tracing.clear();
      }
    }
  }

  @Test
  void aLockThatCannotReachTheDatabase_marksTheLockSpanFailed_withoutAnOutcome() {
    var tracing = new RecordingTracing();
    PGSimpleDataSource unreachable = new PGSimpleDataSource();
    unreachable.setUrl("jdbc:postgresql://127.0.0.1:1/none");
    unreachable.setConnectTimeout(1);
    PgAdvisoryLocker locker = new PgAdvisoryLocker(unreachable).traced(tracing);

    assertThatThrownBy(() -> locker.acquireLock(STREAM, Duration.ofSeconds(1)))
        .isInstanceOf(LockException.class);

    var failed = tracing.only(TraceAttributes.LOCK_ACQUIRE);
    assertFailed(failed);
    assertThat(failed.attributes()).containsOnlyKeys(TraceAttributes.STREAM_ID);
  }

  @Test
  void withTheLockAreaOff_orNoop_noLockSpanStarts() throws Exception {
    var off = RecordingTracing.without(TracingArea.LOCK);
    for (StreamRuneTracing tracing : List.of(off, StreamRuneTracing.NOOP)) {
      new PgAdvisoryLocker(dataSource)
          .traced(tracing)
          .acquireLock(STREAM, Duration.ofSeconds(1))
          .close();
    }
    assertThat(off.names()).isEmpty();
    assertThatThrownBy(() -> new PgAdvisoryLocker(dataSource).traced(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () -> new PgAdvisoryLocker(dataSource).traced(off).acquireLock(null, Duration.ZERO))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void theLockerWithTracing_sharesItsPool_andClosingEitherClosesItOnce() throws Exception {
    var closes = new java.util.concurrent.atomic.AtomicInteger();
    var pool = new ClosingDataSource(dataSource, closes);
    PgAdvisoryLocker owner = PgAdvisoryLocker.withDedicatedPool(pool);
    PgAdvisoryLocker traced = owner.traced(new RecordingTracing());

    traced.acquireLock(STREAM, Duration.ofSeconds(1)).close();
    traced.close();
    owner.close();

    assertThat(closes).hasValue(1);
  }

  /** The application's pool, a {@link javax.sql.DataSource} that counts its closes. */
  static final class ClosingDataSource extends org.postgresql.ds.PGSimpleDataSource
      implements AutoCloseable {
    private final PGSimpleDataSource delegate;
    private final java.util.concurrent.atomic.AtomicInteger closes;

    ClosingDataSource(
        PGSimpleDataSource delegate, java.util.concurrent.atomic.AtomicInteger closes) {
      this.delegate = delegate;
      this.closes = closes;
    }

    @Override
    public java.sql.Connection getConnection() throws java.sql.SQLException {
      return delegate.getConnection();
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }

  // ==================== Without OpenTelemetry ====================

  @Test
  void thisModulesClasspath_holdsNoOpenTelemetry() {
    assertThatThrownBy(() -> Class.forName("io.opentelemetry.api.OpenTelemetry"))
        .isInstanceOf(ClassNotFoundException.class);
    assertThatThrownBy(() -> Class.forName("io.opentelemetry.context.Context"))
        .isInstanceOf(ClassNotFoundException.class);
  }
}
