package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.integration.OutboxWiringMessages;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SnapshotPurgeSetting;
import org.streamrune.integration.tck.ContractBeans.CommandInboxBean;
import org.streamrune.integration.tck.ContractBeans.ContractProjection;
import org.streamrune.integration.tck.ContractBeans.ContractSagaState;
import org.streamrune.integration.tck.ContractBeans.CryptoEngineBean;
import org.streamrune.integration.tck.ContractBeans.DeadLetterQueueBean;
import org.streamrune.integration.tck.ContractBeans.FirstLeadership;
import org.streamrune.integration.tck.ContractBeans.IdentifiedCallers;
import org.streamrune.integration.tck.ContractBeans.OutboxStoreBean;
import org.streamrune.integration.tck.ContractBeans.SecondLeadership;
import org.streamrune.integration.tck.ContractBeans.UngatedSagaRunner;

/**
 * The wiring rules the Spring, Quarkus and Micronaut integrations keep alike, as tests that run in
 * each integration's own test sources against its own container. A subclass implements the hooks
 * below over its framework, and inherits the tests: each starts an application through {@link
 * #start} and asserts what the application's user observes — a refused start and its message, an
 * HTTP answer, the health check, a startup log line, the order of shutdown, the bound defaults.
 *
 * <p>A rule that crosses the three integrations gets its test here, so an integration that drifts
 * from the others shows as the red test of its subclass. Each integration keeps its own tests of
 * how it implements a rule; these are the floor the three share.
 *
 * <p>An application starts without a database unless its rule needs one: the test kit's in-memory
 * event store, and the beans of {@link ContractBeans} in the place of the stores the rule involves.
 * The health check and the shutdown order run on PostgreSQL ({@link #postgresql()}).
 */
public abstract class IntegrationWiringContract {

  /**
   * The stream the SSE requests of {@link #failClosedIdentityAnswers403WithoutAnIdentity} ask for.
   */
  static final String STREAM = "/api/sse/order/order-1";

  /**
   * Starts an application of this integration, the way an application with this configuration and
   * these beans starts.
   *
   * <p>Without a {@code dataSource} the application has no database: its event store and offset
   * store are the test kit's in-memory ones, declared as an application declares its own. With one,
   * it is the application's {@code DataSource}: the integration builds its PostgreSQL event store
   * and stores on it, and the container closes it when the application closes, at the point where
   * it closes an application's connection pool.
   *
   * @param dataSource the application's {@code DataSource}, or {@code null} for an application
   *     without a database
   * @param properties the application's configuration, keys and values as a properties file writes
   *     them
   * @param extraBeans the beans the application declares, classes of {@link ContractBeans} read as
   *     {@link ContractBeans#beanType(Class)} and {@link ContractBeans#create(Class)} read them
   * @return the application, started or refused: a refused start is an application whose {@link
   *     Application#started()} is {@code false}, not an exception
   */
  protected abstract Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans);

  /**
   * A {@code DataSource} on an empty PostgreSQL database, one that no earlier call returned.
   *
   * @return the data source
   */
  protected abstract DataSource postgresql();

  /**
   * The name under which this integration's health endpoint reports the StreamRune check, as the
   * production guide's table of health checks states it.
   *
   * @return the check name
   */
  protected abstract String healthCheckName();

  /**
   * The configuration defaults of a started application, read with {@link ConfigurationOracle}: the
   * integration's properties beans, taken from the application, merged ({@link
   * ConfigurationOracle#merge}) with the keys the integration reads outside them, its bean
   * conditions and its lookups, as its {@code ConfigurationDocLintTest} reads them.
   *
   * @param application a started application with no configuration of its own
   * @return each key with its default as {@link DefaultRendering} writes it
   */
  protected abstract Map<String, String> configurationDefaults(Application application);

  /**
   * How long a test waits for a running application to reach the state it asserts: its projection
   * runner to report its subscription, or to poll the database.
   *
   * @return twenty seconds, unless a subclass waits otherwise
   */
  protected Duration patience() {
    return Duration.ofSeconds(20);
  }

  /**
   * With the outbox enabled, an outbox store, and neither an {@code OutboxPublisher} nor an
   * application's {@code OutboxPoller}, nothing would relay the entries the event store writes: the
   * start is refused with the message of {@link OutboxWiringMessages#noPublisher()}.
   */
  @Test
  public void outboxEnabledWithoutRelayRefusesStart() {
    try (Application application =
        start(null, Map.of("streamrune.outbox.enabled", "true"), Set.of(OutboxStoreBean.class))) {
      assertRefused(application, OutboxWiringMessages.noPublisher());
    }
  }

  /**
   * Two {@code SubscriptionLeadership} beans with none marked the default: the integration threads
   * one leadership into every runner and does not pick one, so the start is refused, and the
   * refusal names both.
   */
  @Test
  public void twoLeadershipBeansRefuseStart() {
    try (Application application =
        start(null, Map.of(), Set.of(FirstLeadership.class, SecondLeadership.class))) {
      assertRefused(
          application,
          FirstLeadership.class.getSimpleName(),
          SecondLeadership.class.getSimpleName());
    }
  }

  /**
   * With a crypto engine the integration builds a {@code ForgetSubjectService}; without a snapshot
   * purger and without {@code streamrune.gdpr.snapshot-purge=none} a forget would leave a subject's
   * data in the snapshots, so the start is refused with the message that says what to declare. With
   * {@code none} the same application starts.
   */
  @Test
  public void forgetServiceWithoutPurgeDecisionRefusesStart() {
    try (Application application = start(null, Map.of(), Set.of(CryptoEngineBean.class))) {
      assertRefused(
          application,
          "has no snapshot purger",
          "Declare a SnapshotPurger bean",
          SnapshotPurgeSetting.PROPERTY + "=none");
    }
    try (Application application =
        start(
            null, Map.of(SnapshotPurgeSetting.PROPERTY, "none"), Set.of(CryptoEngineBean.class))) {
      assertStarted(application);
    }
  }

  /**
   * With a command inbox and a command dead-letter queue, a dead-letter retention longer than the
   * inbox retention would let an entry outlive the inbox row that deduplicates its replay: the
   * start is refused, and the refusal names both properties.
   */
  @Test
  public void deadLetterRetentionAboveInboxRetentionRefusesStart() {
    try (Application application =
        start(
            null,
            Map.of("streamrune.dead-letter.retention-max-age", "PT720H"),
            Set.of(CommandInboxBean.class, DeadLetterQueueBean.class))) {
      assertRefused(
          application,
          "streamrune.dead-letter.retention-max-age (PT720H)",
          "must not exceed streamrune.inbox.retention-max-age (PT168H)");
    }
  }

  /**
   * The request identity fails closed: without an authenticated principal and without {@code
   * streamrune.security.trust-user-id-header=true}, an {@code X-User-Id} header is no identity, and
   * the SSE endpoint, whose authorizer admits identified callers only, answers {@code 403}. Behind
   * a trusted gateway the same request carries its identity and its stream opens, so the {@code
   * 403} is the missing identity's.
   *
   * <p>The Spring adapter runs it in the {@code AUTHENTICATED_PRINCIPAL} mode (spring-security-core
   * is on that module's test classpath, and an unauthenticated request is anonymous), the Quarkus
   * and Micronaut adapters in the {@code ANONYMOUS} mode; both fail closed.
   */
  @Test
  public void failClosedIdentityAnswers403WithoutAnIdentity() {
    Map<String, String> request =
        Map.of(RequestIdentityPolicy.USER_ID_HEADER, "alice", "Accept", "text/event-stream");
    try (Application application =
        start(null, Map.of("streamrune.sse.enabled", "true"), Set.of(IdentifiedCallers.class))) {
      assertStarted(application);
      HttpResponse response = application.http("GET", STREAM, request, null);
      assertEquals(
          403,
          response.status(),
          () -> "a request without an identity must be refused: " + response);
    }
    try (Application application =
        start(
            null,
            Map.of(
                "streamrune.sse.enabled",
                "true",
                RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY,
                "true"),
            Set.of(IdentifiedCallers.class))) {
      assertStarted(application);
      HttpResponse response = application.http("GET", STREAM, request, null);
      assertEquals(
          200,
          response.status(),
          () ->
              "behind a trusted gateway the header is the identity and the stream opens: "
                  + response);
      String type = response.header("Content-Type");
      assertTrue(
          type != null && type.startsWith("text/event-stream"),
          () -> "the stream is an event stream: " + response);
    }
  }

  /**
   * The health endpoint reports the StreamRune check under the name the production guide states,
   * {@code UP}, with the head of the event store ({@code eventStore.lastGlobalOffset}) and one
   * detail per subscription carrying its lease role ({@code subscription.<name>.role}): the role
   * the contract's projection runner plays under its lease. With a {@code DataSource} the
   * single-active-consumer lease is on by default and the only replica holds it: {@code LEADING}
   * (the runner reports {@code STANDBY} until it takes the lease). With the lease switched off the
   * runner takes none and delivers on every replica: {@code UNGATED}.
   */
  @Test
  public void healthCarriesTheDocumentedNameAndKeys() {
    List<String> documented = documentedCheckNames();
    assertTrue(
        documented.contains(healthCheckName()),
        () ->
            "the production guide's health table must state the check name "
                + healthCheckName()
                + ": it states "
                + documented);
    assertTheProjectionReports("LEADING", Map.of());
    assertTheProjectionReports(
        "UNGATED", Map.of("streamrune.subscription.single-active-consumer.enabled", "false"));
  }

  private void assertTheProjectionReports(String expectedRole, Map<String, String> properties) {
    String role = "subscription." + ContractProjection.NAME + ".role";
    try (Application application =
        start(postgresql(), properties, Set.of(ContractProjection.class))) {
      assertStarted(application);
      Map<String, Object> health =
          awaitHealth(
              application, details -> expectedRole.equals(String.valueOf(details.get(role))));
      assertEquals(
          "UP", String.valueOf(health.get("status")), () -> "the check must be UP: " + health);
      Map<String, Object> details = details(health);
      assertTrue(
          details.containsKey("eventStore.lastGlobalOffset"),
          () -> "the check must carry eventStore.lastGlobalOffset: " + health);
      assertTrue(
          details.containsKey(role),
          () -> "the check must carry " + role + " for the running projection: " + health);
      assertEquals(
          expectedRole,
          String.valueOf(details.get(role)),
          () ->
              "with "
                  + (properties.isEmpty() ? "the default properties" : properties)
                  + " the running projection reports the role "
                  + expectedRole
                  + ": "
                  + health);
    }
  }

  /**
   * With a real {@code SubscriptionLeadership} bean, a {@code SagaRunner} that carries no lease
   * delivers its saga type on every replica, and the start logs the {@code WARN} the production
   * guide quotes, naming the saga type. The runner is reported, not refused.
   */
  @Test
  public void sagaRunnerWithoutLeadershipLogsTheDocumentedWarn() {
    String documented = documentedUngatedSagaWarning();
    String expected =
        documented.replace("Saga type X", "Saga type " + ContractSagaState.class.getName());
    try (Application application =
        start(null, Map.of(), Set.of(FirstLeadership.class, UngatedSagaRunner.class))) {
      assertStarted(application);
      List<String> lines = application.startupLogLines();
      assertTrue(
          lines.stream().anyMatch(line -> line.startsWith("WARN") && line.contains(expected)),
          () ->
              "the start must log the WARN \""
                  + expected
                  + "\"; it logged:\n  "
                  + String.join("\n  ", lines));
    }
  }

  /**
   * Closing the application stops the integration's runners before the container closes the
   * database the event store reads: the projection runner polls the database while the application
   * runs; when the container closes the {@code DataSource}, every connection taken from it has been
   * returned, so no runner is in the middle of a poll; and no thread asks it for a connection
   * afterwards.
   *
   * <p>The assertion is a necessary condition of the order, not the order itself: a runner stopped
   * within one idle interval after the close, with its poll thread asleep, passes unobserved; the
   * 20 ms interval keeps that window small.
   *
   * <p>It runs twice: with the single-active-consumer lease (the default), so the leadership the
   * integration builds also releases its leases before the database closes; and without it, where a
   * projection runner the integration does not stop keeps polling, because no closed leadership
   * parks it in standby.
   */
  @Test
  public void shutdownStopsRunnersBeforeTheEventStoreCloses() throws InterruptedException {
    Map<String, String> polling =
        Map.of("streamrune.polling-interval-ms", "20", "streamrune.polling-jitter-ms", "0");
    assertRunnersStopBeforeTheDatabaseCloses(polling);
    Map<String, String> withoutLease = new LinkedHashMap<>(polling);
    withoutLease.put("streamrune.subscription.single-active-consumer.enabled", "false");
    assertRunnersStopBeforeTheDatabaseCloses(withoutLease);
  }

  private void assertRunnersStopBeforeTheDatabaseCloses(Map<String, String> properties)
      throws InterruptedException {
    RecordingDataSource database = new RecordingDataSource(postgresql());
    Application application = start(database, properties, Set.of(ContractProjection.class));
    try {
      assertStarted(application);
      long afterStart = database.connectionsGranted();
      await(
          () -> database.connectionsGranted() > afterStart + 5,
          "the projection runner must poll the database while the application runs " + properties);
    } finally {
      application.close();
    }
    assertTrue(
        database.isClosed(),
        "the container must close the DataSource when the application closes " + properties);
    assertEquals(
        0,
        database.connectionsInUseAtClose(),
        "every connection must be returned before the container closes the DataSource: a runner"
            + " still running a poll is not stopped "
            + properties);
    // A runner still alive asks for its next page within a few polling intervals.
    Thread.sleep(500);
    assertEquals(
        List.of(),
        database.requestsAfterClose(),
        "no thread may ask the closed DataSource for a connection: every runner must be stopped"
            + " before the container closes it "
            + properties);
  }

  /**
   * A started application with no configuration of its own has the defaults the configuration
   * reference states, the one table that holds the three integrations to the same defaults: every
   * key of the reference, a projection's keys aside, is a key of the application's configuration,
   * with the stated default.
   */
  @Test
  public void configurationDefaultsEqualTheOracle() {
    try (Application application = start(null, Map.of(), Set.of())) {
      assertStarted(application);
      Map<String, String> bound = new TreeMap<>(configurationDefaults(application));
      assertEquals("7d", bound.get("streamrune.dead-letter.retention-max-age"), () -> "" + bound);
      assertEquals("7d", bound.get("streamrune.inbox.retention-max-age"), () -> "" + bound);
      assertEquals("5m", bound.get("streamrune.crypto.cache.expire-after-write"), () -> "" + bound);

      Map<String, String> stated = configurationReference();
      List<String> problems = new ArrayList<>();
      stated.forEach(
          (key, statedDefault) -> {
            String bindsDefault = bound.get(key);
            if (bindsDefault == null) {
              if (!key.contains("<")) {
                problems.add(key + ": the reference states " + statedDefault + ", not bound");
              }
            } else if (!bindsDefault.equals(statedDefault)) {
              problems.add(
                  key + ": the reference states " + statedDefault + ", bound " + bindsDefault);
            }
          });
      if (!problems.isEmpty()) {
        fail(
            problems.size()
                + " key(s) of docs/guide/configuration.md the application binds differently:\n  "
                + String.join("\n  ", problems));
      }
    }
  }

  // ---- assertions --------------------------------------------------------------------------

  static void assertStarted(Application application) {
    assertTrue(
        application.started(),
        () ->
            "the application must start; its start failed with:\n"
                + application.startupFailureMessage());
  }

  static void assertRefused(Application application, String... fragments) {
    assertFalse(application.started(), "the integration must refuse the start");
    String message = application.startupFailureMessage();
    assertNotNull(message, "a refused start must say why");
    for (String fragment : fragments) {
      assertTrue(
          message.contains(fragment),
          () -> "the refusal must say \"" + fragment + "\"; it says:\n" + message);
    }
  }

  /** Polls {@code condition} until it holds or {@link #patience()} has passed. */
  private void await(java.util.function.BooleanSupplier condition, String what)
      throws InterruptedException {
    long deadline = System.nanoTime() + patience().toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        fail(what + " (waited " + patience() + ")");
      }
      Thread.sleep(20);
    }
  }

  /**
   * The health check once its flattened details satisfy {@code ready}, or its last answer when
   * {@link #patience()} passes first.
   */
  private Map<String, Object> awaitHealth(
      Application application, Predicate<Map<String, Object>> ready) {
    long deadline = System.nanoTime() + patience().toNanos();
    Map<String, Object> health = application.health();
    while (!ready.test(details(health)) && System.nanoTime() < deadline) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      health = application.health();
    }
    return health;
  }

  /**
   * The details of a health check, nested maps flattened into dotted keys: Spring and Micronaut
   * nest a subscription's entries under {@code subscription.<name>}, Quarkus writes {@code
   * subscription.<name>.role} as one key, and both read as the latter.
   */
  static Map<String, Object> details(Map<String, Object> health) {
    Map<String, Object> flat = new LinkedHashMap<>();
    if (health.get("details") instanceof Map<?, ?> details) {
      flatten("", details, flat);
    }
    return flat;
  }

  private static void flatten(String prefix, Map<?, ?> map, Map<String, Object> into) {
    map.forEach(
        (key, value) -> {
          String name = prefix + key;
          if (value instanceof Map<?, ?> nested) {
            flatten(name + ".", nested, into);
          } else {
            into.put(name, value);
          }
        });
  }

  // ---- what the guides state ---------------------------------------------------------------

  /** The {@code Check name} column of the production guide's table of health checks. */
  static List<String> documentedCheckNames() {
    List<String> names = new ArrayList<>();
    int column = -1;
    for (String line : guide("docs/guide/production.md").split("\n", -1)) {
      List<String> cells = cells(line);
      if (cells.contains("Check name")) {
        column = cells.indexOf("Check name");
      } else if (column >= 0 && cells.size() > column) {
        String cell = cells.get(column);
        if (!cell.startsWith("-")) {
          names.add(cell.replace("`", ""));
        }
      } else {
        column = -1;
      }
    }
    return names;
  }

  /**
   * The {@code WARN} the production guide quotes for a saga runner without a lease, its saga type
   * written as {@code X}.
   */
  static String documentedUngatedSagaWarning() {
    Matcher quoted =
        Pattern.compile("\"(Saga type X is delivered on every replica)\"")
            .matcher(guide("docs/guide/production.md"));
    assertTrue(quoted.find(), "the production guide must quote the WARN of an ungated saga runner");
    return quoted.group(1);
  }

  /** Every key the configuration reference states a default for, with that default. */
  static Map<String, String> configurationReference() {
    Map<String, String> stated = new TreeMap<>();
    boolean inTable = false;
    for (String line : guide("docs/guide/configuration.md").split("\n", -1)) {
      List<String> cells = cells(line);
      if (cells.size() >= 2 && cells.get(0).equals("Property") && cells.get(1).equals("Default")) {
        inTable = true;
      } else if (inTable && cells.size() >= 2) {
        String key = cells.get(0).replace("`", "");
        if (key.startsWith("streamrune.")) {
          stated.put(key, cells.get(1).replace("`", ""));
        }
      } else {
        inTable = false;
      }
    }
    assertTrue(
        stated.size() > 50, () -> "the configuration reference's tables must be found: " + stated);
    return stated;
  }

  private static List<String> cells(String line) {
    String row = line.strip();
    if (!row.startsWith("|")) {
      return List.of();
    }
    List<String> cells = new ArrayList<>();
    for (String cell : row.substring(1).split("\\|", -1)) {
      cells.add(cell.strip());
    }
    if (!cells.isEmpty() && cells.getLast().isEmpty()) {
      cells.removeLast();
    }
    return cells;
  }

  private static String guide(String file) {
    Path path = ConfigurationDocs.repositoryRoot().resolve(file);
    try {
      return Files.readString(path, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
