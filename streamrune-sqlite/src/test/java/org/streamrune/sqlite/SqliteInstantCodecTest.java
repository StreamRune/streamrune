package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The SQLite instant codec on a file database: epoch microseconds in an {@code INTEGER}, and the
 * two required-read flavours the saga and outbox stores use.
 */
class SqliteInstantCodecTest {

  private static final Instant AT = Instant.parse("2026-11-27T10:15:30.123456Z");

  @TempDir Path dir;

  private DataSource dataSource;

  @BeforeEach
  void createTable() throws SQLException {
    dataSource = TestDatabases.dataSource(dir.resolve("codec.db"));
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement()) {
      st.executeUpdate("CREATE TABLE t (id INTEGER PRIMARY KEY, at INTEGER, flag INTEGER)");
    }
  }

  @Test
  void sagaRowsReadInstantDecodesMicros() throws SQLException {
    insert(SqliteTimestamps.toMicros(AT), 0);

    assertThat(instant(rs -> SqliteInstantCodec.SAGA_ROWS.readInstant(rs, "at"))).isEqualTo(AT);
  }

  @Test
  void sagaRowsReadInstantOfNullIsTheEpoch() throws SQLException {
    insert(null, 0);

    assertThat(instant(rs -> SqliteInstantCodec.SAGA_ROWS.readInstant(rs, "at")))
        .isEqualTo(Instant.EPOCH);
  }

  @Test
  void outboxRowsReadInstantOfNullIsNull() throws SQLException {
    insert(null, 0);

    assertThat(instant(rs -> SqliteInstantCodec.OUTBOX_ROWS.readInstant(rs, "at"))).isNull();
  }

  @Test
  void outboxRowsReadInstantDecodesMicros() throws SQLException {
    insert(SqliteTimestamps.toMicros(AT), 0);

    assertThat(instant(rs -> SqliteInstantCodec.OUTBOX_ROWS.readInstant(rs, "at"))).isEqualTo(AT);
  }

  @Test
  void readNullableInstantOfNullIsNull() throws SQLException {
    insert(null, 0);

    assertThat(instant(rs -> SqliteInstantCodec.SAGA_ROWS.readNullableInstant(rs, "at"))).isNull();
    assertThat(instant(rs -> SqliteInstantCodec.OUTBOX_ROWS.readNullableInstant(rs, "at")))
        .isNull();
  }

  @Test
  void readNullableInstantDecodesMicros() throws SQLException {
    insert(SqliteTimestamps.toMicros(AT), 0);

    assertThat(instant(rs -> SqliteInstantCodec.SAGA_ROWS.readNullableInstant(rs, "at")))
        .isEqualTo(AT);
  }

  @Test
  void readFlagIsNonZero() throws SQLException {
    insert(null, 0);
    assertThat(flag(rs -> SqliteInstantCodec.SAGA_ROWS.readFlag(rs, "flag"))).isFalse();

    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement()) {
      st.executeUpdate("UPDATE t SET flag = 2");
    }
    assertThat(flag(rs -> SqliteInstantCodec.OUTBOX_ROWS.readFlag(rs, "flag"))).isTrue();
  }

  @Test
  void bindInstantBindsMicros() throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement("INSERT INTO t (id, at, flag) VALUES (1, ?, 0)")) {
      SqliteInstantCodec.SAGA_ROWS.bindInstant(ps, 1, AT);
      ps.executeUpdate();
    }

    assertThat(micros()).isEqualTo(SqliteTimestamps.toMicros(AT));
  }

  private void insert(Long micros, int flag) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement("INSERT INTO t (id, at, flag) VALUES (1, ?, ?)")) {
      if (micros == null) {
        ps.setNull(1, Types.INTEGER);
      } else {
        ps.setLong(1, micros);
      }
      ps.setInt(2, flag);
      ps.executeUpdate();
    }
  }

  private Instant instant(RowReader<Instant> reader) throws SQLException {
    return read(reader);
  }

  private boolean flag(RowReader<Boolean> reader) throws SQLException {
    return read(reader);
  }

  private long micros() throws SQLException {
    RowReader<Long> reader = rs -> rs.getLong("at");
    return read(reader);
  }

  private <T> T read(RowReader<T> reader) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT at, flag FROM t WHERE id = 1")) {
      assertThat(rs.next()).isTrue();
      return reader.read(rs);
    }
  }

  @FunctionalInterface
  private interface RowReader<T> {
    T read(ResultSet rs) throws SQLException;
  }
}
