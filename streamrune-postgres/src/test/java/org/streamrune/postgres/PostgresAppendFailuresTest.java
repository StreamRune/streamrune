package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.jdbc.EventAppendSupport;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The PostgreSQL classifier of a failed append, on failures a real database raises: a second row at
 * a stream's version is a retryable conflict and no collision, a second row at a {@code
 * global_offset} is a collision, a lock the database did not grant is never singled out, and the
 * desync message is the store's operator-facing text.
 */
@Testcontainers
class PostgresAppendFailuresTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("append_failures");

  private static final String INSERT =
      "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version, event_type,"
          + " payload, metadata) VALUES (?, 'account', 'a-1', ?, 'Opened', '{}'::jsonb,"
          + " '{}'::jsonb)";

  private static final PostgresAppendFailures CLASSIFIER = PostgresAppendFailures.INSTANCE;

  static PGSimpleDataSource dataSource;

  record Opened(String value) {}

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    EventTypeRegistry registry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Opened.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Opened.class;
          }

          @Override
          public Collection<Class<?>> registeredTypes() {
            return List.of(Opened.class);
          }
        };
    new PostgresEventStoreFactory(dataSource, registry).create();
  }

  @BeforeEach
  void clean() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var st = conn.createStatement()) {
      st.execute("DELETE FROM event_stream");
    }
    insert(1, 1);
  }

  @Test
  void aSecondRowAtTheStreamsVersion_isRetryableAndNoCollision() {
    SQLException e = insertFailure(2, 1);

    assertThat(e.getSQLState()).isEqualTo("23505");
    assertThat(CLASSIFIER.isRetryableConflict(e)).isTrue();
    assertThat(CLASSIFIER.isGlobalOffsetCollision(e)).isFalse();
    assertThat(CLASSIFIER.lockFailure(e, "append to stream 'account:a-1'")).isNull();
    assertThat(EventAppendSupport.appendConflict(e, CLASSIFIER, "account:a-1", Version.initial()))
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Version conflict on stream 'account:a-1' at expected version 0")
        .hasCause(e);
  }

  @Test
  void aSecondRowAtAGlobalOffset_isACollision() {
    SQLException e = insertFailure(1, 2);

    assertThat(e.getSQLState()).isEqualTo("23505");
    assertThat(CLASSIFIER.isGlobalOffsetCollision(e)).isTrue();
    assertThat(CLASSIFIER.lockFailure(e, "append to stream 'account:a-1'")).isNull();
    assertThat(EventAppendSupport.keyedAppendFailure(e, CLASSIFIER, "account:a-1"))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(PostgresEventStore.globalOffsetDesyncMessage("account:a-1"))
        .hasCause(e);
  }

  @Test
  void aLockTheDatabaseDidNotGrant_isNeverSingledOut() throws SQLException {
    SQLException e;
    try (Connection holder = dataSource.getConnection();
        Connection waiter = dataSource.getConnection()) {
      holder.setAutoCommit(false);
      try (var st = holder.createStatement()) {
        st.execute("SELECT next_value FROM global_offset_sequence WHERE id = 1 FOR UPDATE");
      }
      try (var st = waiter.createStatement()) {
        st.execute("SET lock_timeout = '50ms'");
        e =
            catchThrowableOfType(
                SQLException.class,
                () ->
                    st.execute(
                        "UPDATE global_offset_sequence SET next_value = next_value WHERE id = 1"));
      } finally {
        holder.rollback();
      }
    }

    assertThat(e.getSQLState()).isEqualTo("55P03");
    assertThat(CLASSIFIER.lockFailure(e, "append to stream 'account:a-1'")).isNull();
    assertThat(CLASSIFIER.lockFailure(e, "idempotent append to stream 'account:a-1'")).isNull();
    assertThat(EventAppendSupport.appendConflict(e, CLASSIFIER, "account:a-1", Version.initial()))
        .as("the store wraps it as its plain append failure")
        .isNull();
    assertThat(EventAppendSupport.keyedAppendFailure(e, CLASSIFIER, "account:a-1"))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed idempotent append to stream: account:a-1")
        .hasCause(e);
  }

  @Test
  void theDesyncMessage_isTheStoresText() {
    assertThat(CLASSIFIER.globalOffsetDesyncMessage("account:a-1"))
        .isEqualTo(
            "global_offset collision while appending to stream 'account:a-1':"
                + " global_offset_sequence.next_value has fallen at or below max(global_offset),"
                + " so this append reserved offsets that are already committed. This is NOT a"
                + " version conflict and retrying cannot resolve it — the reservation rolls back"
                + " with the append, so every attempt reserves the same used offsets. Re-seed the"
                + " counter with: UPDATE global_offset_sequence SET next_value = (SELECT"
                + " COALESCE(max(global_offset), 0) FROM event_stream) WHERE id = 1");
  }

  private static void insert(long globalOffset, long version) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var ps = conn.prepareStatement(INSERT)) {
      ps.setLong(1, globalOffset);
      ps.setLong(2, version);
      ps.executeUpdate();
    }
  }

  private static SQLException insertFailure(long globalOffset, long version) {
    return catchThrowableOfType(SQLException.class, () -> insert(globalOffset, version));
  }
}
