package org.streamrune.spring;

import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.MultiReadModelStoreScenario;

/**
 * Two read-model stores on two databases, on Spring Boot: each application is started exactly as
 * {@link SpringIntegrationWiringContractTest} starts one — the same {@code SpringApplication}, the
 * same declaration of the scenario's beans — and its databases are created on that class's
 * PostgreSQL container, which this class stops when it is done.
 */
class SpringMultiReadModelStoreScenarioTest extends MultiReadModelStoreScenario {

  private final SpringIntegrationWiringContractTest contract =
      new SpringIntegrationWiringContractTest();

  @AfterAll
  static void stopPostgres() {
    SpringIntegrationWiringContractTest.stopPostgres();
  }

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    return contract.start(dataSource, properties, extraBeans);
  }

  @Override
  protected DataSource postgresql() {
    return contract.postgresql();
  }
}
