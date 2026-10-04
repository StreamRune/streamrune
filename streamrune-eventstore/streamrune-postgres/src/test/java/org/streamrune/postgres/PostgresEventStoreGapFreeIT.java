package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Gate test for the lock-free, gap-free global read backed by the gapless commit-ordered counter
 * ({@code global_offset_sequence}) and the contiguous-prefix guard in {@link
 * PostgresEventStore#readGlobalStream}. Forces the exact race that guard exists for — two appends
 * committing out of {@code global_offset} order — and asserts the reader never skips the
 * late-committing lower offset and never returns the higher offset ahead of it.
 *
 * <p>Two raw connections insert directly into {@code event_stream} so the test controls commit
 * timing (the public {@code append} commits atomically and cannot be held open). Connection A takes
 * the lower offset and is held open; connection B takes the higher offset and commits first.
 */
@Testcontainers
class PostgresEventStoreGapFreeIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_gapfree");

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;
  PostgresEventStore store;

  record TestEvent(String value) implements DomainEvent {}

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return TestEvent.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of(TestEvent.class);
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    store = PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      // Reset the gapless offset counter so each test's offsets restart at 1 (deterministic).
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  private static final String RESERVE_OFFSET =
      "UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1"
          + " RETURNING next_value";

  private static final String INSERT =
      "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version, event_type,"
          + " payload, metadata, schema_version)"
          + " VALUES (?, 'test', ?, 1, 'TestEvent', ?::jsonb, ?::jsonb, 1)";

  /**
   * Reserves the next global_offset on its own short auto-committed statement — NOT inside {@code
   * conn}'s transaction — then inserts one event using that offset on the given (possibly
   * uncommitted) connection; returns the assigned global_offset. Here the counter's row lock is
   * held only for the instant of the reservation UPDATE, so reserving on a separate,
   * immediately-committed connection lets two concurrent callers each get a distinct offset without
   * blocking on each other's still-open transaction — mirroring how the real counter is meant to be
   * used (reserve-then-release quickly, hold the transaction open only for the row insert/commit).
   * Reserving inside the long-held transaction itself would serialize the two test connections on
   * the counter's row lock, defeating the whole point of holding one of them open.
   */
  private long insert(java.sql.Connection conn, String stream, String value) throws Exception {
    long offset;
    try (var reserveConn = dataSource.getConnection();
        var res = reserveConn.prepareStatement(RESERVE_OFFSET);
        var rs = res.executeQuery()) {
      rs.next();
      offset = rs.getLong(1);
    }
    String payload = objectMapper.writeValueAsString(new TestEvent(value));
    String metadata =
        objectMapper.writeValueAsString(
            new EventMetadata(
                IdGenerator.generateEventId(),
                IdGenerator.generateCommandId(),
                null,
                null,
                CorrelationId.of("gapfree"),
                null,
                null,
                Instant.now()));
    try (var ps = conn.prepareStatement(INSERT)) {
      ps.setLong(1, offset);
      ps.setString(2, stream);
      ps.setString(3, payload);
      ps.setString(4, metadata);
      ps.executeUpdate();
    }
    return offset;
  }

  @Test
  void readGlobalStreamPagesOverPermanentHoles() throws Exception {
    // A restore or import that leaves the counter ahead of the data, or rows deleted out of band,
    // leave PERMANENT holes in event_stream that will never be filled. The reader must page OVER
    // such a hole and deliver every committed event, never treating the hole as the end of the
    // stream. (The gapless append forms no holes — offset assignment is serialized on the counter
    // row so a committed higher offset proves the lower ones settled — but an out-of-band hole
    // must not wedge global delivery.)
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(true);
      assertEquals(1, insert(conn, "hole-1", "e1"));
      assertEquals(2, insert(conn, "hole-2", "e2"));
      // Skip offset 3 to simulate a permanent hole (an out-of-band skip or deleted row).
      try (var st = conn.createStatement()) {
        st.execute("UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1");
      }
      assertEquals(4, insert(conn, "hole-3", "e3"));
      assertEquals(5, insert(conn, "hole-4", "e4"));
    }

    // Page the whole stream following the checkpoint; the permanent hole at offset 3 must not
    // stall delivery — every committed event is returned across successive polls.
    var delivered = new java.util.ArrayList<Long>();
    long cursor = 0;
    for (int i = 0; i < 10 && delivered.size() < 4; i++) {
      var batch = store.readGlobalStream(GlobalOffset.of(cursor), 500);
      if (batch.isEmpty()) {
        break;
      }
      batch.forEach(e -> delivered.add(e.globalOffset().value()));
      cursor = batch.get(batch.size() - 1).globalOffset().value();
    }
    assertEquals(
        java.util.List.of(1L, 2L, 4L, 5L),
        delivered,
        "reader must page over the permanent hole at offset 3 and deliver every committed event");
  }
}
