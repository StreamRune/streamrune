package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

class StreamKeysTest {

  private static final StreamId STREAM =
      StreamId.of(new AggregateType("Order"), new AggregateId("order-1"));

  @TempDir Path dir;

  private JdbcTestDatabase db;

  @BeforeEach
  void createTable() throws SQLException {
    db =
        JdbcTestDatabase.create(
            dir,
            "CREATE TABLE keyed (seq INTEGER NOT NULL, aggregate_type TEXT, aggregate_id TEXT)");
  }

  @Test
  void bindsTypeThenId() throws SQLException {
    insert(7, STREAM);

    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement("SELECT seq, aggregate_type, aggregate_id FROM keyed");
        ResultSet rs = ps.executeQuery()) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getLong(1)).as("the parameter before firstIndex is untouched").isEqualTo(7L);
      assertThat(rs.getString(2)).isEqualTo("Order");
      assertThat(rs.getString(3)).isEqualTo("order-1");
    }
  }

  @Test
  void bindsTwoNullsForANullStream() throws SQLException {
    insert(1, null);

    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement("SELECT aggregate_type, aggregate_id FROM keyed");
        ResultSet rs = ps.executeQuery()) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString(1)).isNull();
      assertThat(rs.wasNull()).isTrue();
      assertThat(rs.getString(2)).isNull();
      assertThat(rs.wasNull()).isTrue();
    }
  }

  @Test
  void readsNullWhenAggregateIdIsNull() throws SQLException {
    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement(
                "INSERT INTO keyed (seq, aggregate_type, aggregate_id) VALUES (1, 'Order', NULL)")) {
      ps.executeUpdate();
    }

    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement("SELECT aggregate_type, aggregate_id FROM keyed");
        ResultSet rs = ps.executeQuery()) {
      assertThat(rs.next()).isTrue();
      assertThat(StreamKeys.read(rs)).isNull();
    }
  }

  @Test
  void readsTheStreamByColumnName() throws SQLException {
    insert(1, STREAM);

    // The columns in the opposite order of the bind: read goes by name, not by position.
    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement("SELECT aggregate_id, seq, aggregate_type FROM keyed");
        ResultSet rs = ps.executeQuery()) {
      assertThat(rs.next()).isTrue();
      assertThat(StreamKeys.read(rs)).isEqualTo(STREAM);
    }
  }

  private void insert(long seq, StreamId stream) throws SQLException {
    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement(
                "INSERT INTO keyed (seq, aggregate_type, aggregate_id) VALUES (?, ?, ?)")) {
      ps.setLong(1, seq);
      StreamKeys.bind(ps, 2, stream);
      assertThat(ps.executeUpdate()).isEqualTo(1);
    }
  }
}
