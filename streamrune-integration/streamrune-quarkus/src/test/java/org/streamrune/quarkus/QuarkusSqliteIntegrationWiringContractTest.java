package org.streamrune.quarkus;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.IntegrationWiringContract;
import org.streamrune.integration.tck.SqliteTestDatabases;

/**
 * The integration wiring contract on Quarkus with the SQLite backend: each application is started
 * exactly as {@link QuarkusIntegrationWiringContractTest} starts one — a real Arc container over
 * the module's beans, with the build-time conditions of its configuration evaluated. Arc is one
 * container per JVM, so the starts run one at a time, in one thread. Each start on a database
 * selects {@code streamrune.backend=sqlite}, and its database is a HikariCP pool on a new SQLite
 * file with the connection settings the backend requires ({@link SqliteTestDatabases}). SQLite
 * serves one application instance, so the contract leaves out what needs several ({@link
 * #supportsMultiInstance()}).
 */
@Execution(ExecutionMode.SAME_THREAD)
class QuarkusSqliteIntegrationWiringContractTest extends IntegrationWiringContract {

  private final QuarkusIntegrationWiringContractTest contract =
      new QuarkusIntegrationWiringContractTest();

  /** Where each test's database files are made; JUnit deletes it after the test. */
  @TempDir Path files;

  private SqliteTestDatabases databases;

  @AfterEach
  void closeTheDatabases() {
    if (databases != null) {
      databases.close();
    }
  }

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    return contract.start(
        dataSource, SqliteTestDatabases.onSqlite(dataSource, properties), extraBeans);
  }

  @Override
  protected DataSource database() {
    if (databases == null) {
      databases = new SqliteTestDatabases(files);
    }
    return databases.create();
  }

  @Override
  protected String healthCheckName() {
    return contract.healthCheckName();
  }

  @Override
  protected Map<String, String> configurationDefaults(Application application) {
    return contract.configurationDefaults(application);
  }

  @Override
  protected String backendName() {
    return "sqlite";
  }

  @Override
  protected boolean supportsMultiInstance() {
    return false;
  }
}
