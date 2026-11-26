package org.streamrune.micronaut;

import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.MultiReadModelStoreScenario;

/**
 * Two read-model stores on two databases, on Micronaut: each application is started exactly as
 * {@link MicronautIntegrationWiringContractTest} starts one — the scenario's plain beans as runtime
 * singletons, collected with {@code getBeansOfType}, and its constructor-injected projection from
 * that class's compile-time {@code ApplicationBeans} — and its databases are created on that
 * class's PostgreSQL container, which this class stops when it is done. The adapter hands the
 * context the current start's {@code DataSource} through a static field, so the starts run one at a
 * time, in one thread.
 */
@Execution(ExecutionMode.SAME_THREAD)
class MicronautMultiReadModelStoreScenarioTest extends MultiReadModelStoreScenario {

  private final MicronautIntegrationWiringContractTest contract =
      new MicronautIntegrationWiringContractTest();

  @AfterAll
  static void stopPostgres() {
    MicronautIntegrationWiringContractTest.stopPostgres();
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
