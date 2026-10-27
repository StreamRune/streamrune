package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opentest4j.AssertionFailedError;
import org.streamrune.integration.tck.ReferenceApplication.Rule;

/**
 * The contract run against {@link ReferenceApplication}, which keeps every rule: each inherited
 * test passes. The tests declared here break one rule at a time and show that the contract's test
 * of that rule then fails, so no test of the contract passes an application that breaks its rule.
 */
class IntegrationWiringContractTest extends IntegrationWiringContract {

  private Set<Rule> broken = Set.of();
  private String checkName = "streamrune";

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    return ReferenceApplication.start(dataSource, properties, extraBeans, broken);
  }

  @Override
  protected DataSource postgresql() {
    return new StubDataSource();
  }

  @Override
  protected String healthCheckName() {
    return checkName;
  }

  @Override
  protected Map<String, String> configurationDefaults(Application application) {
    return ((ReferenceApplication) application).configurationDefaults();
  }

  @Override
  protected Duration patience() {
    return Duration.ofSeconds(2);
  }

  @ParameterizedTest
  @EnumSource(Rule.class)
  void theTestOfARuleFailsOnAnApplicationThatBreaksIt(Rule rule) {
    broken = Set.of(rule);
    assertThrows(AssertionFailedError.class, testOf(rule));
  }

  @Test
  void aCheckNameTheProductionGuideDoesNotStateFailsTheHealthTest() {
    checkName = "streamRuneCheck";
    var failure =
        assertThrows(AssertionFailedError.class, this::healthCarriesTheDocumentedNameAndKeys);
    assertTrue(failure.getMessage().contains("streamRuneCheck"), failure.getMessage());
  }

  @Test
  void theGuidesStateWhatTheContractReads() {
    assertTrue(documentedCheckNames().containsAll(Set.of("streamRune", "streamrune")));
    assertEquals("Saga type X is delivered on every replica", documentedUngatedSagaWarning());
    Map<String, String> reference = configurationReference();
    assertEquals("7d", reference.get("streamrune.dead-letter.retention-max-age"));
    assertEquals("—", reference.get("streamrune.crypto.cache.expire-after-access"));
  }

  @Test
  void healthDetailsAreReadFlatWhetherNestedOrNot() {
    var nested =
        Map.<String, Object>of(
            "details", Map.of("subscription.p", Map.of("role", "LEADING"), "eventStore.x", 1));
    var flat = Map.<String, Object>of("details", Map.of("subscription.p.role", "LEADING"));
    assertEquals("LEADING", details(nested).get("subscription.p.role"));
    assertEquals(1, details(nested).get("eventStore.x"));
    assertEquals("LEADING", details(flat).get("subscription.p.role"));
    assertEquals(Map.of(), details(Map.of()));
  }

  private Executable testOf(Rule rule) {
    return switch (rule) {
      case OUTBOX_RELAY -> this::outboxEnabledWithoutRelayRefusesStart;
      case ONE_LEADERSHIP -> this::twoLeadershipBeansRefuseStart;
      case SNAPSHOT_PURGE_DECISION -> this::forgetServiceWithoutPurgeDecisionRefusesStart;
      case DEAD_LETTER_RETENTION -> this::deadLetterRetentionAboveInboxRetentionRefusesStart;
      case FAIL_CLOSED_IDENTITY -> this::failClosedIdentityAnswers403WithoutAnIdentity;
      case HEALTH_DETAILS -> this::healthCarriesTheDocumentedNameAndKeys;
      case UNGATED_SAGA_WARNING -> this::sagaRunnerWithoutLeadershipLogsTheDocumentedWarn;
      case RUNNERS_STOP_FIRST -> this::shutdownStopsRunnersBeforeTheEventStoreCloses;
      case DOCUMENTED_DEFAULTS -> this::configurationDefaultsEqualTheOracle;
      case ROUTE_CALLER_AUDITED -> this::aCommandDispatchedFromARouteIsAuditedAsTheCaller;
      case BUS_ANSWERS_BY_RUNNER_RULES ->
          this::theCommandBusAnswersAKeyedReSendByTheRetryRunnersRules;
    };
  }

  /** A data source whose connections do nothing: the stand-in for a PostgreSQL database. */
  static class StubDataSource implements DataSource {

    @Override
    public Connection getConnection() {
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> method.getReturnType() == boolean.class ? false : null);
    }

    @Override
    public Connection getConnection(String username, String password) {
      return getConnection();
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      return java.util.logging.Logger.getGlobal();
    }

    @Override
    public <T> T unwrap(Class<T> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
      return false;
    }
  }
}
