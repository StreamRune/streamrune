package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests proving that {@link PostgresEventStore}, when configured with an {@link
 * OutboxEventMapper} + {@link PostgresOutboxStore}, writes outbox entries in the SAME transaction
 * as the domain events — and that the builder enforces both-or-neither configuration.
 */
@Testcontainers
class PostgresEventStoreOutboxIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_outbox_mapper_it");

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;

  record MappedEvent(String value) implements DomainEvent {}

  record SkippedEvent(String value) implements DomainEvent {}

  /** Maps only {@link MappedEvent}; returns empty for everything else. */
  static final OutboxEventMapper TEST_MAPPER =
      envelope -> {
        if ("MappedEvent".equals(envelope.eventType().name())) {
          return List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(envelope.metadata().eventId().value()),
                  "{\"k\":1}",
                  "MappedEvent",
                  envelope.streamId()));
        }
        return List.of();
      };

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
            return switch (eventType.name()) {
              case "MappedEvent" -> MappedEvent.class;
              case "SkippedEvent" -> SkippedEvent.class;
              default -> throw new IllegalArgumentException("Unknown event type: " + eventType);
            };
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of(MappedEvent.class, SkippedEvent.class);
          }
        };

    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void cleanUp() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
      stmt.execute("DELETE FROM event_stream");
    }
  }

  // ── Helper: build a minimal EventEnvelope ─────────────────────────────────

  private static EventEnvelope envelope(
      StreamId streamId, String eventTypeName, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(1),
        new EventType(eventTypeName),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("outbox-it"),
            null,
            null,
            Instant.now()));
  }

  // ── (a) mapper maps → outbox row written in same tx ───────────────────────

  @Test
  void append_with_mapper_writes_outbox_entry_in_same_transaction() {
    var outboxStore = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .outboxStore(outboxStore)
            .outboxEventMapper(TEST_MAPPER)
            .build();

    var streamId = TestStreams.stream("outbox-mapped-" + System.nanoTime());
    var env = envelope(streamId, "MappedEvent", new MappedEvent("hello"));

    store.append(streamId, List.of(env), Version.initial());

    var pending = outboxStore.loadPending(10);
    assertEquals(1, pending.size(), "exactly one outbox entry must be written");
    assertEquals("MappedEvent", pending.get(0).payloadType());
    assertEquals(streamId, pending.get(0).streamId());
    // PostgreSQL normalizes JSONB spacing on store; compare without whitespace
    assertEquals(
        "{\"k\":1}",
        pending.get(0).payload().replaceAll("\\s", ""),
        "payload must round-trip through JSONB (normalization ignored)");
  }

  // ── (b) mapper skips → no outbox row ──────────────────────────────────────

  @Test
  void append_with_mapper_writes_no_outbox_entry_when_mapper_returns_empty() {
    var outboxStore = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .outboxStore(outboxStore)
            .outboxEventMapper(TEST_MAPPER)
            .build();

    var streamId = TestStreams.stream("outbox-skipped-" + System.nanoTime());
    var env = envelope(streamId, "SkippedEvent", new SkippedEvent("nope"));

    store.append(streamId, List.of(env), Version.initial());

    var pending = outboxStore.loadPending(10);
    assertTrue(pending.isEmpty(), "skipped event must produce no outbox entry");
  }

  // ── (c) builder fail-fast: mapper without store, or store without mapper ──

  @Test
  void builder_throws_when_outboxEventMapper_set_without_outboxStore() {
    var builder =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .outboxEventMapper(TEST_MAPPER); // no outboxStore

    assertThrows(
        IllegalStateException.class,
        builder::build,
        "build() must throw when mapper is set without outboxStore");
  }

  @Test
  void builder_throws_when_outboxStore_set_without_outboxEventMapper() {
    var outboxStore = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    var builder =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .outboxStore(outboxStore); // no mapper

    assertThrows(
        IllegalStateException.class,
        builder::build,
        "build() must throw when outboxStore is set without outboxEventMapper");
  }

  // ── (d) mapper throws → append rolls back entirely (no event row, no outbox row) ───────

  @Test
  void append_mapperThrows_persistsNeitherEventNorOutbox() throws Exception {
    var throwingMapper =
        (OutboxEventMapper)
            envelope -> {
              throw new RuntimeException("mapper intentionally exploding");
            };

    var outboxStore = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .outboxStore(outboxStore)
            .outboxEventMapper(throwingMapper)
            .build();

    var streamId = TestStreams.stream("outbox-mapper-throws-" + System.nanoTime());
    var env = envelope(streamId, "MappedEvent", new MappedEvent("boom"));

    assertThatThrownBy(() -> store.append(streamId, List.of(env), Version.initial()))
        .isInstanceOf(RuntimeException.class);

    // (b) no event_stream row for this stream id
    int eventRows;
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        eventRows = rs.getInt(1);
      }
    }
    assertEquals(0, eventRows, "event_stream must have no row for the failed append");

    // (c) no outbox_events row for this aggregate
    int outboxRows;
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        outboxRows = rs.getInt(1);
      }
    }
    assertEquals(0, outboxRows, "outbox_events must have no row for the failed append");
  }

  // ── (e) atomicity: failed append must not leave orphan outbox rows ─────────

  /** Returns total count of rows in outbox_events regardless of status. */
  private int countAllOutboxRows() throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT COUNT(*) FROM outbox_events");
        var rs = ps.executeQuery()) {
      rs.next();
      return rs.getInt(1);
    }
  }

  @Test
  void append_failure_rolls_back_outbox_entries() throws Exception {
    var outboxStore = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .outboxStore(outboxStore)
            .outboxEventMapper(TEST_MAPPER)
            .build();

    var streamId = TestStreams.stream("outbox-atomicity-" + System.nanoTime());

    // First append succeeds — produces exactly one outbox entry.
    store.append(
        streamId,
        List.of(envelope(streamId, "MappedEvent", new MappedEvent("ok"))),
        Version.initial());
    int countAfterSuccess = countAllOutboxRows();
    assertEquals(1, countAfterSuccess, "baseline: one outbox row after the successful append");

    // Second append with a stale expectedVersion triggers OptimisticLockException.
    // The mapper WOULD produce another outbox entry — unless the tx rolls back.
    assertThatThrownBy(
            () ->
                store.append(
                    streamId,
                    List.of(envelope(streamId, "MappedEvent", new MappedEvent("conflict"))),
                    Version.initial())) // stale: stream is already at version 1
        .isInstanceOf(OptimisticLockException.class);

    // The total outbox row count must remain at 1 — no orphan row from the rolled-back tx.
    int countAfterFailure = countAllOutboxRows();
    assertEquals(
        1, countAfterFailure, "outbox must contain no orphan entry from the rolled-back append");
  }

  // ── (f) Strict-channel fail-closed: a strict channel refuses a null-aggregate entry inside
  // append ────

  @Test
  void append_strictNullAggregateMapper_rollsBack_surfacesOrderingViolationUnwrapped()
      throws Exception {
    // The mapper forgets aggregateId on a STRICT channel. saveAll throws before its try
    // block, PostgresEventStore.append rolls back in its finally and rethrows the RuntimeException
    // as is — the command fails with the named type, not an EventStoreException wrapping it, and
    // neither the event nor the outbox row is persisted. A rerun fails identically (fail-closed).
    OutboxEventMapper forgetful =
        envelope ->
            List.of(
                OutboxEntry.pending(
                    OutboxEntryId.of(envelope.metadata().eventId().value()),
                    "{\"k\":1}",
                    "MappedEvent"));
    var strictOutbox =
        new PostgresOutboxStore(
            dataSource, Duration.ofMinutes(1), OutboxOrderingMode.STRICT_PER_AGGREGATE);
    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .outboxStore(strictOutbox)
            .outboxEventMapper(forgetful)
            .build();
    var streamId = TestStreams.stream("strict-null-agg-" + System.nanoTime());
    var env = envelope(streamId, "MappedEvent", new MappedEvent("boom"));

    for (int rerun = 0; rerun < 2; rerun++) {
      var ex =
          assertThrows(
              OutboxOrderingViolationException.class,
              () -> store.append(streamId, List.of(env), Version.initial()));
      assertThat(ex.payloadType()).isEqualTo("MappedEvent");
    }
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        assertEquals(0, rs.getInt(1), "no event row");
      }
    }
    assertEquals(0, countAllOutboxRows(), "no outbox row");
  }
}
