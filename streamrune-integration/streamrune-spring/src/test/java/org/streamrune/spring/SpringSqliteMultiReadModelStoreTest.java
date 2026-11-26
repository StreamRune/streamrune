package org.streamrune.spring;

import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.SingleInstanceMultiReadModelStoreScenario;

/**
 * Two read-model stores on two SQLite files, on Spring Boot: each application is started exactly as
 * {@link SpringIntegrationWiringContractTest} starts one — the same {@code SpringApplication}, the
 * same declaration of the contract's beans.
 */
class SpringSqliteMultiReadModelStoreTest extends SingleInstanceMultiReadModelStoreScenario {

  private final SpringIntegrationWiringContractTest contract =
      new SpringIntegrationWiringContractTest();

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    return contract.start(dataSource, properties, extraBeans);
  }
}
