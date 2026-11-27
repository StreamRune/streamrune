package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The PostgreSQL timestamp and flag codec on a real {@code TIMESTAMPTZ}/{@code BOOLEAN} table: the
 * decoding the Postgres stores use today ({@code getTimestamp(...).toInstant()}, {@code
 * getBoolean}, {@code setTimestamp(Timestamp.from(...))}).
 */
@Testcontainers
class PostgresInstantCodecTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("instant_codec_test");

  static PGSimpleDataSource dataSource;

  private static final PostgresInstantCodec CODEC = PostgresInstantCodec.INSTANCE;

  @BeforeAll
  static void init() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE codec_row (id INT PRIMARY KEY, at TIMESTAMPTZ, flag BOOLEAN NOT NULL)");
    }
  }

  @Test
  void readInstantRoundTripsMicros() throws Exception {
    Instant at = Instant.parse("2026-11-27T10:15:30.123456Z");
    try (var conn = dataSource.getConnection()) {
      insert(conn, 1, at, true);
      try (var ps = conn.prepareStatement("SELECT at FROM codec_row WHERE id = 1");
          var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(CODEC.readInstant(rs, "at")).isEqualTo(at);
        assertThat(CODEC.readNullableInstant(rs, "at")).isEqualTo(at);
      }
    }
  }

  @Test
  void readInstantOfNullThrowsNullPointer() throws Exception {
    try (var conn = dataSource.getConnection()) {
      insert(conn, 2, null, false);
      try (var ps = conn.prepareStatement("SELECT at FROM codec_row WHERE id = 2");
          var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThatThrownBy(() -> CODEC.readInstant(rs, "at"))
            .isInstanceOf(NullPointerException.class);
      }
    }
  }

  @Test
  void readNullableInstantOfNullIsNull() throws Exception {
    try (var conn = dataSource.getConnection()) {
      insert(conn, 3, null, false);
      try (var ps = conn.prepareStatement("SELECT at FROM codec_row WHERE id = 3");
          var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(CODEC.readNullableInstant(rs, "at")).isNull();
      }
    }
  }

  @Test
  void readFlagReadsBooleans() throws Exception {
    try (var conn = dataSource.getConnection()) {
      insert(conn, 4, null, true);
      insert(conn, 5, null, false);
      try (var ps =
              conn.prepareStatement("SELECT flag FROM codec_row WHERE id IN (4, 5) ORDER BY id");
          var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(CODEC.readFlag(rs, "flag")).isTrue();
        assertThat(rs.next()).isTrue();
        assertThat(CODEC.readFlag(rs, "flag")).isFalse();
      }
    }
  }

  @Test
  void bindInstantBindsATimestamp() throws Exception {
    Instant at = Instant.parse("2026-01-02T03:04:05.678901Z");
    try (var conn = dataSource.getConnection()) {
      try (var ps =
          conn.prepareStatement("INSERT INTO codec_row (id, at, flag) VALUES (6, ?, true)")) {
        CODEC.bindInstant(ps, 1, at);
        assertThat(ps.executeUpdate()).isEqualTo(1);
      }
      try (var ps =
              conn.prepareStatement(
                  "SELECT at, (at = '2026-01-02T03:04:05.678901Z'::timestamptz) AS exact"
                      + " FROM codec_row WHERE id = 6");
          var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getBoolean("exact")).as("stored as the same instant").isTrue();
        assertThat(rs.getTimestamp("at").toInstant()).isEqualTo(at.truncatedTo(ChronoUnit.MICROS));
      }
    }
  }

  private static void insert(Connection conn, int id, Instant at, boolean flag) throws Exception {
    try (var ps = conn.prepareStatement("INSERT INTO codec_row (id, at, flag) VALUES (?, ?, ?)")) {
      ps.setInt(1, id);
      ps.setTimestamp(2, at == null ? null : java.sql.Timestamp.from(at));
      ps.setBoolean(3, flag);
      ps.executeUpdate();
    }
  }
}
