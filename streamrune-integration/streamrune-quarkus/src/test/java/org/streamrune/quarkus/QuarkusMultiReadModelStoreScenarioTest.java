package org.streamrune.quarkus;

import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.MultiReadModelStoreScenario;

/**
 * Two read-model stores on two databases, on Quarkus: each application is started exactly as {@link
 * QuarkusIntegrationWiringContractTest} starts one — a real Arc container over the module's beans,
 * the scenario's plain bean classes as {@code Singleton} class beans (Arc injects the constructor
 * of a constructor-injected one) and its suppliers as synthetic beans — and its databases are
 * created on that class's PostgreSQL container, which this class stops when it is done. Arc is one
 * container per JVM, so the starts run one at a time, in one thread.
 */
@Execution(ExecutionMode.SAME_THREAD)
class QuarkusMultiReadModelStoreScenarioTest extends MultiReadModelStoreScenario {

  private final QuarkusIntegrationWiringContractTest contract =
      new QuarkusIntegrationWiringContractTest();

  @AfterAll
  static void stopPostgres() {
    QuarkusIntegrationWiringContractTest.stopPostgres();
  }

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    return contract.start(dataSource, properties, extraBeans);
  }

  @Override
  protected DataSource postgresql() {
    return contract.database();
  }
}
