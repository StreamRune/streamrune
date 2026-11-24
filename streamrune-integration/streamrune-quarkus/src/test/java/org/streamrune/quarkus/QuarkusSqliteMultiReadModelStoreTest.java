package org.streamrune.quarkus;

import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.streamrune.integration.tck.Application;
import org.streamrune.integration.tck.SingleInstanceMultiReadModelStoreScenario;

/**
 * Two read-model stores on two SQLite files, on Quarkus: each application is started exactly as
 * {@link QuarkusIntegrationWiringContractTest} starts one — a real Arc container over the module's
 * beans, with the build-time conditions of its configuration evaluated. Arc is one container per
 * JVM, so the starts run one at a time, in one thread.
 */
@Execution(ExecutionMode.SAME_THREAD)
class QuarkusSqliteMultiReadModelStoreTest extends SingleInstanceMultiReadModelStoreScenario {

  private final QuarkusIntegrationWiringContractTest contract =
      new QuarkusIntegrationWiringContractTest();

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    return contract.start(dataSource, properties, extraBeans);
  }
}
