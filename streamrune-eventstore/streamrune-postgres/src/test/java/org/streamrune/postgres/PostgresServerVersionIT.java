package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The version check against a REAL server, and the pin that the container the suite starts is the
 * one {@code -PpostgresImage} asked for. The second matters as much as the first: the {@code
 * postgres-17} CI job claims "the PostgreSQL-backed suites pass on 17", and that claim is only
 * worth its name if a broken Gradle-to-JVM property hand-off cannot quietly run them on 18.
 */
@Testcontainers
class PostgresServerVersionIT {

  private static final Pattern TAG_MAJOR = Pattern.compile("^(?:[\\w./-]*/)?postgres:(\\d+)\\b.*");

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("server_version_it");

  private static PGSimpleDataSource dataSource() {
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl());
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    return ds;
  }

  @Test
  void theServerTheSuiteRunsOnIsSupported() {
    assertDoesNotThrow(() -> PostgresServerVersion.requireSupported(dataSource()));
  }

  @Test
  void theContainerRunsTheMajorVersionTheImageNames() throws Exception {
    Matcher tag = TAG_MAJOR.matcher(PostgresTestImage.NAME);
    assumeTrue(tag.matches(), () -> "image has no numeric major tag: " + PostgresTestImage.NAME);
    int expected = Integer.parseInt(tag.group(1));

    try (var conn = dataSource().getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SHOW server_version_num")) {
      rs.next();
      int major = rs.getInt(1) / 10_000;
      assertEquals(
          expected,
          major,
          () -> "image " + PostgresTestImage.NAME + " started a PostgreSQL " + major + " server");
    }
    // And the driver-side view the refusal reads agrees with the server-side one.
    try (var conn = dataSource().getConnection()) {
      assertEquals(expected, conn.getMetaData().getDatabaseMajorVersion());
    }
  }
}
