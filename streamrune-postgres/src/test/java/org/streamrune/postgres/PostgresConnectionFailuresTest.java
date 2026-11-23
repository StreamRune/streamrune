package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class PostgresConnectionFailuresTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("connection_failures");

  private static PGSimpleDataSource dataSource() {
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl());
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    return ds;
  }

  @ParameterizedTest
  @ValueSource(strings = {"57P01", "57P02", "57P03", "53300"})
  void thePostgresCodesAreConnectionFailures_andOnlyThroughThePostgresClassifier(String state) {
    SQLException failure = new SQLException("x", state);

    assertThat(PostgresConnectionFailures.CLASSIFIER.isConnectionFailure(failure)).isTrue();
    assertThat(ConnectionFailureClassifier.GENERIC.isConnectionFailure(failure)).isFalse();
  }

  @Test
  void thePostgresSagaStoreContributesThem() {
    var store = new PostgresSagaStore(dataSource(), new ObjectMapper());

    assertThat(store.connectionFailureClassifier()).isSameAs(PostgresConnectionFailures.CLASSIFIER);
  }

  @Test
  void aKilledBackendIsAConnectionFailure_aStatementTimeoutIsNot() throws Exception {
    var ds = dataSource();
    try (Connection victim = ds.getConnection();
        Connection killer = ds.getConnection()) {
      int pid;
      try (var st = victim.createStatement();
          var rs = st.executeQuery("SELECT pg_backend_pid()")) {
        rs.next();
        pid = rs.getInt(1);
      }
      try (var st = killer.createStatement()) {
        st.execute("SELECT pg_terminate_backend(" + pid + ")");
      }
      SQLException killed = null;
      try (Statement st = victim.createStatement()) {
        st.execute("SELECT 1");
      } catch (SQLException e) {
        killed = e;
      }
      assertThat((Throwable) killed).isNotNull();
      assertThat(PostgresConnectionFailures.CLASSIFIER.isConnectionFailure(killed)).isTrue();
    }

    try (Connection c = ds.getConnection();
        Statement st = c.createStatement()) {
      st.execute("SET statement_timeout = 1");
      SQLException timeout = null;
      try {
        st.execute("SELECT pg_sleep(1)");
      } catch (SQLException e) {
        timeout = e;
      }
      assertThat((Throwable) timeout).isNotNull();
      assertThat(PostgresConnectionFailures.CLASSIFIER.isConnectionFailure(timeout)).isFalse();
    }
  }
}
