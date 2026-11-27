package org.streamrune.quarkus.it;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** The PostgreSQL server of the application under test: one container per test run. */
public class PostgresResource implements QuarkusTestResourceLifecycleManager {

  private PostgreSQLContainer postgres;

  @Override
  public Map<String, String> start() {
    postgres = new PostgreSQLContainer(PostgresTestImage.NAME);
    postgres.start();
    return Map.of(
        "quarkus.datasource.jdbc.url", postgres.getJdbcUrl(),
        "quarkus.datasource.username", postgres.getUsername(),
        "quarkus.datasource.password", postgres.getPassword());
  }

  @Override
  public void stop() {
    if (postgres != null) {
      postgres.stop();
    }
  }
}
