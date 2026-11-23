package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.streamrune.sqlite.SqliteSchemaSeriesTest.execute;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The one timestamp representation of every SQLite table: epoch microseconds in an INTEGER. */
class SqliteTimestampsTest {

  private static final List<Instant> MICROSECOND_INSTANTS =
      List.of(
          Instant.parse("1969-07-20T20:17:40.123456Z"), // before 1970
          Instant.parse("1969-12-31T23:59:59.999999Z"),
          Instant.EPOCH,
          Instant.parse("1970-01-01T00:00:00.000001Z"),
          Instant.parse("2026-11-23T10:15:30.654321Z"),
          Instant.parse("2038-01-19T03:14:08.000001Z"), // after the 32-bit seconds limit
          Instant.parse("9999-12-31T23:59:59.999999Z"),
          Instant.parse("0001-01-01T00:00:00.000001Z"));

  @TempDir Path dir;

  @Test
  void microsecondInstantsRoundTripExactly() {
    for (Instant instant : MICROSECOND_INSTANTS) {
      assertThat(SqliteTimestamps.fromMicros(SqliteTimestamps.toMicros(instant)))
          .as(instant.toString())
          .isEqualTo(instant);
    }
    assertThat(SqliteTimestamps.toMicros(Instant.parse("1969-12-31T23:59:59.999999Z")))
        .isEqualTo(-1L);
    assertThat(SqliteTimestamps.toMicros(Instant.parse("1970-01-01T00:00:01.000002Z")))
        .isEqualTo(1_000_002L);
  }

  /** Sub-microsecond digits round half up to the nearest microsecond, as pgjdbc sends them. */
  @Test
  void nanosecondsRoundToTheNearestMicrosecondHalfUp() {
    assertThat(SqliteTimestamps.toMicros(Instant.ofEpochSecond(0, 499))).isZero();
    assertThat(SqliteTimestamps.toMicros(Instant.ofEpochSecond(0, 500))).isEqualTo(1L);
    assertThat(SqliteTimestamps.toMicros(Instant.ofEpochSecond(0, 999_999_500)))
        .isEqualTo(1_000_000L);
    assertThat(SqliteTimestamps.toMicros(Instant.ofEpochSecond(-1, 999_999_499))).isEqualTo(-1L);
    assertThat(SqliteTimestamps.toMicros(Instant.ofEpochSecond(-1, 999_999_500))).isZero();
  }

  @Test
  void instantsOutsideTheLongRangeAreRefused() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> SqliteTimestamps.toMicros(Instant.MAX))
        .withMessageContaining("instant");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> SqliteTimestamps.toMicros(Instant.MIN))
        .withMessageContaining("instant");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> SqliteTimestamps.toMicros(null))
        .withMessage("instant must not be null");
  }

  /**
   * Stored in a STRICT INTEGER column, the values read back equal, sort chronologically and answer
   * range queries, across 1970 and 2038.
   */
  @Test
  void storedValuesRoundTripSortAndAnswerRangeQueries() throws Exception {
    try (SqliteDatabase db = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("app.db")))) {
      DataSource ds = db.dataSource();
      execute(ds, "CREATE TABLE stamps (at INTEGER NOT NULL) STRICT");
      try (Connection connection = ds.getConnection();
          PreparedStatement insert = connection.prepareStatement("INSERT INTO stamps VALUES (?)")) {
        for (Instant instant : MICROSECOND_INSTANTS) {
          insert.setLong(1, SqliteTimestamps.toMicros(instant));
          insert.executeUpdate();
        }
      }

      assertThat(read(ds, "SELECT at FROM stamps ORDER BY at", List.of()))
          .containsExactlyElementsOf(MICROSECOND_INSTANTS.stream().sorted().toList());
      assertThat(
              read(
                  ds,
                  "SELECT at FROM stamps WHERE at >= ? AND at < ? ORDER BY at",
                  List.of(
                      SqliteTimestamps.toMicros(Instant.parse("1969-12-31T23:59:59.999999Z")),
                      SqliteTimestamps.toMicros(Instant.parse("2038-01-19T03:14:08.000001Z")))))
          .containsExactly(
              Instant.parse("1969-12-31T23:59:59.999999Z"),
              Instant.EPOCH,
              Instant.parse("1970-01-01T00:00:00.000001Z"),
              Instant.parse("2026-11-23T10:15:30.654321Z"));
    }
  }

  private static List<Instant> read(DataSource ds, String sql, List<Long> parameters)
      throws Exception {
    List<Instant> result = new ArrayList<>();
    try (Connection connection = ds.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int i = 0; i < parameters.size(); i++) {
        statement.setLong(i + 1, parameters.get(i));
      }
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          result.add(SqliteTimestamps.fromMicros(rs.getLong(1)));
        }
      }
    }
    return result;
  }

  @Test
  void plusSaturatingMovesByWholeMicrosecondsAndSaturatesAtTheRange() {
    assertThat(SqliteTimestamps.plusSaturating(10, Duration.ofNanos(2_999))).isEqualTo(12);
    assertThat(SqliteTimestamps.plusSaturating(10, Duration.ofSeconds(-1))).isEqualTo(-999_990);
    assertThat(SqliteTimestamps.plusSaturating(0, Duration.ofSeconds(Long.MAX_VALUE)))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(SqliteTimestamps.plusSaturating(0, Duration.ofSeconds(Long.MIN_VALUE)))
        .isEqualTo(Long.MIN_VALUE);
    assertThat(SqliteTimestamps.plusSaturating(Long.MAX_VALUE - 1, Duration.ofNanos(5_000)))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(SqliteTimestamps.plusSaturating(Long.MIN_VALUE + 1, Duration.ofNanos(-5_000)))
        .isEqualTo(Long.MIN_VALUE);
    assertThat(SqliteTimestamps.plusSaturating(Long.MAX_VALUE, Duration.ofNanos(-1_000)))
        .isEqualTo(Long.MAX_VALUE - 1);
  }
}
