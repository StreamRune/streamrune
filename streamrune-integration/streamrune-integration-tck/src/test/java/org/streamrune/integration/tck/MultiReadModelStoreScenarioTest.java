package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.streamrune.integration.tck.IntegrationWiringContractTest.StubDataSource;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.AmbiguousProjection;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.SecondaryDatabase;
import org.streamrune.integration.tck.MultiReadModelStoreScenario.SecondaryStoreBean;

/**
 * The part of {@link MultiReadModelStoreScenario} that runs without a database: its refusal rule,
 * against applications that keep it and applications that break it. The scenario's other tests read
 * two PostgreSQL databases; this module's tests start no container, so they run in the three
 * integrations only.
 */
class MultiReadModelStoreScenarioTest {

  @Test
  void aRefusalNamingTheProjectionAndBothStoresPasses() {
    var scenario =
        new Scenario(
            "projection 'p4_ambiguous' (TRANSACTIONAL_LOCAL) exposes no writeTarget() and several"
                + " read-model stores are transactional ('postgres', 'secondary')");
    assertDoesNotThrow(scenario::ambiguousTransactionalProjectionFailsStartupNamingBothStores);
    assertEquals(
        Set.of(SecondaryStoreBean.class, AmbiguousProjection.class),
        scenario.beans,
        "the application declares the second store and the ambiguous projection");
    assertEquals(MultiReadModelStoreScenario.PROPERTIES, scenario.properties);
  }

  @Test
  void aRefusalThatNamesOneStoreFails() {
    var scenario =
        new Scenario("projection 'p4_ambiguous' (TRANSACTIONAL_LOCAL) cannot use 'postgres'");
    var failure =
        assertThrows(
            AssertionFailedError.class,
            scenario::ambiguousTransactionalProjectionFailsStartupNamingBothStores);
    assertTrue(failure.getMessage().contains("'secondary'"), failure.getMessage());
  }

  @Test
  void anApplicationThatStartsFails() {
    var scenario = new Scenario(null);
    assertThrows(
        AssertionFailedError.class,
        scenario::ambiguousTransactionalProjectionFailsStartupNamingBothStores);
  }

  @Test
  void theSecondaryStoreNeedsItsDatabaseNamed() {
    SecondaryDatabase.use(null);
    var failure = assertThrows(IllegalStateException.class, SecondaryDatabase::store);
    assertTrue(failure.getMessage().contains("SecondaryDatabase.use"), failure.getMessage());
  }

  /** The scenario over applications that start, or are refused with one message. */
  private static final class Scenario extends MultiReadModelStoreScenario {

    private final String refusal;
    private Set<Class<?>> beans;
    private Map<String, String> properties;

    Scenario(String refusal) {
      this.refusal = refusal;
    }

    @Override
    protected Application start(
        DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
      this.beans = extraBeans;
      this.properties = properties;
      return new Stub(refusal);
    }

    @Override
    protected DataSource postgresql() {
      return new StubDataSource();
    }
  }

  /** An application that started ({@code refusal == null}) or was refused with {@code refusal}. */
  private record Stub(String refusal) implements Application {

    @Override
    public boolean started() {
      return refusal == null;
    }

    @Override
    public String startupFailureMessage() {
      return refusal;
    }

    @Override
    public <T> T bean(Class<T> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public HttpResponse http(String method, String path, Map<String, String> headers, String body) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> health() {
      return Map.of();
    }

    @Override
    public List<String> startupLogLines() {
      return List.of();
    }

    @Override
    public void close() {}
  }
}
