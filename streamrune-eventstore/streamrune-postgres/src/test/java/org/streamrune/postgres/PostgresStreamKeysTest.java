package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Binding and reading the stream key's column pair; needs a PostgreSQL server, no schema. */
@Testcontainers
class PostgresStreamKeysTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("stream_keys_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void init() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  @Test
  void read_rebuildsThePairThroughTheLenientConstructors_andNullForNoStream() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      try (var rs =
          stmt.executeQuery(
              "SELECT 'Order'::varchar AS aggregate_type, 'o-1'::varchar AS aggregate_id")) {
        assertTrue(rs.next());
        var stream = PostgresStreamKeys.read(rs);
        assertEquals(
            "Order", stream.aggregateType().value(), "a stored value is never re-validated");
        assertEquals("o-1", stream.aggregateId().value());
      }
      try (var rs =
          stmt.executeQuery(
              "SELECT NULL::varchar AS aggregate_type, NULL::varchar AS aggregate_id")) {
        assertTrue(rs.next());
        assertNull(PostgresStreamKeys.read(rs));
      }
    }
  }

  @Test
  void bind_writesBothColumns_orTwoNulls() throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT ?::varchar, ?::varchar")) {
      PostgresStreamKeys.bind(ps, 1, StreamId.of(AggregateType.of("order"), AggregateId.of("o-1")));
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("order", rs.getString(1));
        assertEquals("o-1", rs.getString(2));
      }
      PostgresStreamKeys.bind(ps, 1, null);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertNull(rs.getString(2));
      }
    }
  }
}
