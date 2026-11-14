package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.MisspelledPropertyNames;
import org.streamrune.integration.OutboxWiringMessages;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SnapshotPurgeSetting;
import org.streamrune.integration.tck.ContractBeans.AcceptingOutboxPublisher;
import org.streamrune.integration.tck.ContractBeans.ApplicationCommandInterceptor;
import org.streamrune.integration.tck.ContractBeans.ApplicationLocker;
import org.streamrune.integration.tck.ContractBeans.ApplicationTracing;
import org.streamrune.integration.tck.ContractBeans.CommandInboxBean;
import org.streamrune.integration.tck.ContractBeans.ContractCommand;
import org.streamrune.integration.tck.ContractBeans.ContractCommandRegistration;
import org.streamrune.integration.tck.ContractBeans.ContractInlineProjection;
import org.streamrune.integration.tck.ContractBeans.ContractProjection;
import org.streamrune.integration.tck.ContractBeans.ContractSagaState;
import org.streamrune.integration.tck.ContractBeans.CryptoEngineBean;
import org.streamrune.integration.tck.ContractBeans.DeadLetterQueueBean;
import org.streamrune.integration.tck.ContractBeans.DispatchingRoute;
import org.streamrune.integration.tck.ContractBeans.FirstLeadership;
import org.streamrune.integration.tck.ContractBeans.IdentifiedCallers;
import org.streamrune.integration.tck.ContractBeans.OpenTelemetryBean;
import org.streamrune.integration.tck.ContractBeans.OutboxStoreBean;
import org.streamrune.integration.tck.ContractBeans.RecordingAuditStore;
import org.streamrune.integration.tck.ContractBeans.SecondLeadership;
import org.streamrune.integration.tck.ContractBeans.UngatedSagaRunner;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.GraalJitGuard;
import org.streamrune.runtime.TracingCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;

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
   * start is refused with the message of {@code OutboxWiringMessages.noPublisher}.
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
   * A value the integration cannot read as the type of its property refuses the start, and the
   * refusal names the property and the value: no integration binds the property's default in its
   * place. Durations, numbers and a duration bound outside the properties types alike.
   */
  @Test
  public void anUnreadableValueRefusesStartNamingThePropertyAndTheValue() {
    for (Map.Entry<String, String> unreadable :
        List.of(
            Map.entry("streamrune.outbox.flush-interval", "soon"),
            Map.entry("streamrune.inbox.retention-max-age", "a week"),
            Map.entry("streamrune.saga.compensation-retry-interval", "often"),
            Map.entry("streamrune.stripe-count", "many"))) {
      try (Application application =
          start(null, Map.of(unreadable.getKey(), unreadable.getValue()), Set.of())) {
        assertRefused(application, unreadable.getKey(), unreadable.getValue());
      }
    }
  }

  /**
   * A name that reads like a StreamRune property but that no integration binds — a duration
   * property with a {@code -ms} suffix, the outbox retry multiplier under a shorter name — refuses
   * the start and the refusal names it: no integration runs on the default of the property the
   * application meant.
   */
  @Test
  public void aMisspelledPropertyNameRefusesStart() {
    List<String> names = new ArrayList<>();
    for (String duration :
        List.of(
            "streamrune.retry-initial-delay",
            "streamrune.polling-interval",
            "streamrune.polling-jitter",
            "streamrune.lock-timeout",
            "streamrune.dead-letter.retry-interval",
            "streamrune.outbox.flush-interval",
            "streamrune.outbox.retry-initial-delay",
            "streamrune.sse.polling-interval")) {
      names.add(duration + "-ms");
    }
    names.addAll(MisspelledPropertyNames.OTHER_NAMES.keySet());
    for (String name : names) {
      try (Application application = start(null, Map.of(name, "5"), Set.of())) {
        assertRefused(application, name);
      }
    }
  }

  /**
   * A duration written as a number without its unit refuses the start on every integration, and the
   * refusal names the property and the value: Spring would read it as milliseconds, Quarkus as
   * seconds, and Micronaut cannot read it. Zero too.
   */
  @Test
  public void aDurationWithoutItsUnitRefusesStart() {
    for (Map.Entry<String, String> bare :
        List.of(
            Map.entry("streamrune.outbox.flush-interval", "5000"),
            Map.entry("streamrune.inbox.retention-max-age", "0"),
            Map.entry("streamrune.polling-jitter", "0"),
            Map.entry("streamrune.saga.compensation-retry-interval", "60"))) {
      try (Application application =
          start(null, Map.of(bare.getKey(), bare.getValue()), Set.of())) {
        assertRefused(
            application,
            bare.getKey(),
            bare.getValue(),
            "a duration needs its unit, such as 5000ms, 30s or 0s");
      }
    }
  }

  /**
   * A bare zero dead-letter retention beside a finite inbox retention never starts: whichever way
   * an integration reads the {@code 0}, the refusal names the property. An integration that bound
   * the default in its place would start and prune the rows the application asked to keep.
   */
  @Test
  public void aBareZeroDeadLetterRetentionRefusesStart() {
    try (Application application =
        start(
            null,
            Map.of("streamrune.dead-letter.retention-max-age", "0"),
            Set.of(CommandInboxBean.class, DeadLetterQueueBean.class))) {
      assertRefused(application, "streamrune.dead-letter.retention-max-age");
    }
  }

  /** A bare negative lock timeout never starts: the refusal names the property. */
  @Test
  public void aBareNegativeLockTimeoutRefusesStart() {
    try (Application application = start(null, Map.of("streamrune.lock-timeout", "-5"), Set.of())) {
      assertRefused(application, "streamrune.lock-timeout");
    }
  }

  /**
   * A switch is {@code true} or {@code false}, in lower case, on every integration: any other
   * spelling refuses the start, naming the property and the value. The frameworks read {@code
   * TRUE}, {@code yes} or {@code 1} differently from one another, and a bean condition can compare
   * the text where the bound settings read a boolean: {@code streamrune.crypto.cache.enabled=FALSE}
   * could keep the decrypted-plaintext cache while the settings read it as off, and {@code
   * streamrune.validation-enabled=TRUE} could leave out the Bean Validation interceptor while they
   * read it as on.
   */
  @Test
  public void aSwitchSpelledOtherThanTrueOrFalseRefusesStart() {
    for (Map.Entry<String, String> spelling :
        List.of(
            Map.entry("streamrune.crypto.cache.enabled", "FALSE"),
            Map.entry("streamrune.validation-enabled", "TRUE"),
            Map.entry("streamrune.saga.enabled", "yes"),
            Map.entry("streamrune.outbox.enabled", "1"),
            Map.entry("streamrune.tracing.enabled", "off"))) {
      try (Application application =
          start(null, Map.of(spelling.getKey(), spelling.getValue()), Set.of())) {
        assertRefused(
            application,
            spelling.getKey(),
            spelling.getValue(),
            "a switch is true or false, in lower case");
      }
    }
  }

  /** The same switches spelled {@code true} and {@code false} start. */
  @Test
  public void aSwitchSpelledTrueOrFalseStarts() {
    try (Application application =
        start(
            null,
            Map.of(
                "streamrune.crypto.cache.enabled", "false",
                "streamrune.validation-enabled", "true",
                "streamrune.tracing.enabled", "false"),
            Set.of())) {
      assertStarted(application);
    }
  }

  /**
   * Zero written with its unit, {@code 0s}, is zero on every integration: a zero dead-letter
   * retention beside a finite inbox retention reaches the check that refuses it, where a value the
   * integration bound as its default would start.
   */
  @Test
  public void zeroWithItsUnitReachesTheRetentionCheck() {
    try (Application application =
        start(
            null,
            Map.of("streamrune.dead-letter.retention-max-age", "0s"),
            Set.of(CommandInboxBean.class, DeadLetterQueueBean.class))) {
      assertRefused(
          application,
          "streamrune.dead-letter.retention-max-age (PT0S (pruning disabled))",
          "must not exceed streamrune.inbox.retention-max-age (PT168H)");
    }
  }

  /** A negative lock timeout refuses the start on every integration, naming the property. */
  @Test
  public void aNegativeLockTimeoutRefusesStart() {
    try (Application application =
        start(null, Map.of("streamrune.lock-timeout", "-5s"), Set.of())) {
      assertRefused(application, "streamrune.lock-timeout must not be negative", "PT-5S");
    }
  }

  /**
   * The command bus answers a keyed command that fails again under its key by the dead-letter retry
   * runner's rules — whether the entry stored for the key is still replayed on the schedule — and
   * the integration builds the bus and the runner from the same keys: the runner, which compares
   * its {@code maxRetries} and inbox window with its bus's when it is built, logs no warning about
   * them, with the defaults and with values of the application's own.
   */
  @Test
  public void theCommandBusAnswersAKeyedReSendByTheRetryRunnersRules() {
    List<Map<String, String>> configurations =
        List.of(
            Map.of(),
            Map.of(
                "streamrune.dead-letter.max-retries",
                "3",
                "streamrune.inbox.retention-max-age",
                "PT240H"));
    for (Map<String, String> properties : configurations) {
      try (Application application =
          start(null, properties, Set.of(CommandInboxBean.class, DeadLetterQueueBean.class))) {
        assertStarted(application);
        DeadLetterRetryRunner runner = application.bean(DeadLetterRetryRunner.class);
        assertEquals(
            Integer.parseInt(properties.getOrDefault("streamrune.dead-letter.max-retries", "5")),
            runner.policy().maxRetries(),
            "the runner replays by the configured maxRetries");
        List<String> lines = application.startupLogLines();
        assertTrue(
            lines.stream().noneMatch(line -> line.contains(BUS_RULES_DIFFER)),
            () ->
                "the bus and the runner must answer by the same rules; the start logged:\n  "
                    + String.join("\n  ", lines));
      }
    }
  }

  /**
   * The opt-out of the JVM check, {@value GraalJitGuard#ALLOW_PROPERTY}, is one key in the three
   * integrations, and it reaches the command bus the integration builds: unset, the bus is built
   * without it; set to {@code true}, with it. The JVM the tests run on is one the check accepts, so
   * both start.
   */
  @Test
  public void theOptOutOfTheJvmCheckReachesTheCommandBus() {
    for (boolean allow : List.of(false, true)) {
      Map<String, String> properties =
          allow ? Map.of(GraalJitGuard.ALLOW_PROPERTY, "true") : Map.of();
      try (Application application = start(null, properties, Set.of())) {
        assertStarted(application);
        assertEquals(
            allow,
            application.bean(VirtualThreadCommandBus.class).allowsGraalJit(),
            "the bus is built with the configured opt-out");
      }
    }
  }

  /** What the retry runner's warning names when its bus answers a keyed re-send by other rules. */
  private static final String BUS_RULES_DIFFER =
      "VirtualThreadCommandBus.Builder.deadLetterRetryPolicy";

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
   * A command a route of the application dispatches through the integration's command bus is
   * audited as the request's caller, though the route binds nothing around the dispatch: the
   * request filter identifies the caller, and the integration hands that identity to the command
   * the route dispatches, on whatever thread its framework runs the route. Behind a trusted gateway
   * the {@code X-User-Id} header is the caller.
   */
  @Test
  public void aCommandDispatchedFromARouteIsAuditedAsTheCaller() {
    try (Application application =
        start(
            null,
            Map.of(RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY, "true"),
            Set.of(
                DispatchingRoute.class,
                ContractCommandRegistration.class,
                RecordingAuditStore.class))) {
      assertStarted(application);
      HttpResponse response =
          application.http(
              "POST",
              DispatchingRoute.PATH + "/order-1",
              Map.of(RequestIdentityPolicy.USER_ID_HEADER, "alice"),
              null);
      assertEquals(200, response.status(), () -> "the route must execute the command: " + response);
      List<AuditEntry> entries = application.bean(RecordingAuditStore.class).entries();
      assertEquals(1, entries.size(), () -> "the command must be audited once: " + entries);
      assertEquals(
          UserId.of("alice"),
          entries.getFirst().userId(),
          () -> "the audit row must name the request's caller: " + entries);
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
        Map.of("streamrune.polling-interval", "PT0.02S", "streamrune.polling-jitter", "PT0S");
    assertRunnersStopBeforeTheDatabaseCloses(polling);
    Map<String, String> withoutLease = new LinkedHashMap<>(polling);
    withoutLease.put("streamrune.subscription.single-active-consumer.enabled", "false");
    assertRunnersStopBeforeTheDatabaseCloses(withoutLease);
  }

  private void assertRunnersStopBeforeTheDatabaseCloses(Map<String, String> properties)
      throws InterruptedException {
    RecordingDataSource database = new RecordingDataSource(postgresql());
    try (Application application = start(database, properties, Set.of(ContractProjection.class))) {
      assertStarted(application);
      long afterStart = database.connectionsGranted();
      await(
          () -> database.connectionsGranted() > afterStart + 5,
          "the projection runner must poll the database while the application runs " + properties);
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
      assertEquals(
          "7d", bound.get("streamrune.dead-letter.retention-max-age"), () -> String.valueOf(bound));
      assertEquals(
          "7d", bound.get("streamrune.inbox.retention-max-age"), () -> String.valueOf(bound));
      assertEquals(
          "5m",
          bound.get("streamrune.crypto.cache.expire-after-write"),
          () -> String.valueOf(bound));

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

  // ---- tracing -----------------------------------------------------------------------------

  /**
   * The configuration of a start whose background runners poll often enough for a test, and whose
   * projection runner leads without a lease: an adapter may give an application without a database
   * a {@code DataSource} that reaches none.
   */
  private static final Map<String, String> PROMPT_RUNNERS =
      Map.of(
          "streamrune.polling-interval", "50ms",
          "streamrune.polling-jitter", "10ms",
          "streamrune.outbox.flush-interval", "50ms",
          "streamrune.subscription.single-active-consumer.enabled", "false");

  /** The stream of the outbox entries the tracing rules relay. */
  private static final StreamId OUTBOX_STREAM =
      StreamId.of(AggregateType.of(ContractCommandRegistration.TYPE), AggregateId.of("outbox-1"));

  /** The span name of a projection's batch, for {@link ContractProjection}. */
  private static final String PROJECTION_SPAN =
      TraceAttributes.projectionProcessSpanName(ProjectionName.of(ContractProjection.NAME));

  /** The span name of an inline projection's batch, for {@link ContractInlineProjection}. */
  private static final String INLINE_PROJECTION_SPAN =
      TraceAttributes.projectionProcessSpanName(ProjectionName.of(ContractInlineProjection.NAME));

  /** The beans of a start that traces every area the integration wires on its own. */
  private static final Set<Class<?>> TRACED_APPLICATION =
      Set.of(
          OpenTelemetryBean.class,
          ContractCommandRegistration.class,
          ContractProjection.class,
          ContractInlineProjection.class,
          OutboxStoreBean.class,
          AcceptingOutboxPublisher.class);

  /**
   * With an {@code OpenTelemetry} bean, a command on the integration's PostgreSQL event store
   * produces the tree of a command: {@code command/…} holds {@code lock/acquire}, {@code
   * eventstore/load} and {@code eventstore/append}, the append holds {@code
   * eventstore/append/serialize}. The integration's command bus has no interceptor bean for tracing
   * in its chain and adds the interceptor itself.
   */
  @Test
  public void aCommandOnTheIntegrationsStoreIsTracedThroughTheOpenTelemetryBean() {
    OpenTelemetryBean.reset();
    try (Application application =
        start(
            postgresql(),
            Map.of(),
            Set.of(OpenTelemetryBean.class, ContractCommandRegistration.class))) {
      assertStarted(application);
      application.bean(VirtualThreadCommandBus.class).execute(new ContractCommand("traced-1"));

      List<SpanData> spans = OpenTelemetryBean.spans();
      SpanData command = onlySpan(spans, s -> s.getName().startsWith("command/"));
      assertTrue(
          command.getName().endsWith(ContractCommand.class.getSimpleName()), command::getName);
      for (String child :
          List.of(
              TraceAttributes.LOCK_ACQUIRE,
              TraceAttributes.EVENT_STORE_LOAD,
              TraceAttributes.EVENT_STORE_APPEND)) {
        SpanData span = onlySpan(spans, s -> s.getName().equals(child));
        assertEquals(
            command.getSpanId(), span.getParentSpanId(), child + " must be a child of the command");
      }
      SpanData append =
          onlySpan(spans, s -> s.getName().equals(TraceAttributes.EVENT_STORE_APPEND));
      SpanData serialize =
          onlySpan(spans, s -> s.getName().equals(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE));
      assertEquals(append.getSpanId(), serialize.getParentSpanId());
    }
  }

  /**
   * A command the integration's bus runs through {@code executeAsync} continues the caller's trace,
   * with no bean and no setting: its {@code command/…} span is a child of the span current on the
   * submitting thread, as a synchronous command's is.
   */
  @Test
  public void anAsyncCommandContinuesTheCallersTrace() throws Exception {
    OpenTelemetryBean.reset();
    try (Application application =
        start(null, Map.of(), Set.of(OpenTelemetryBean.class, ContractCommandRegistration.class))) {
      assertStarted(application);
      io.opentelemetry.api.trace.Span caller =
          OpenTelemetryBean.view().getTracer("caller").spanBuilder("caller").startSpan();
      try (io.opentelemetry.context.Scope ignored = caller.makeCurrent()) {
        application
            .bean(VirtualThreadCommandBus.class)
            .executeAsync(new ContractCommand("async-1"))
            .get(30, java.util.concurrent.TimeUnit.SECONDS);
      } finally {
        caller.end();
      }

      List<SpanData> spans = OpenTelemetryBean.spans();
      SpanData callerSpan = onlySpan(spans, s -> s.getName().equals("caller"));
      SpanData command = onlySpan(spans, s -> s.getName().startsWith("command/"));
      assertEquals(callerSpan.getTraceId(), command.getTraceId(), "the caller's trace");
      assertEquals(callerSpan.getSpanId(), command.getParentSpanId(), "a child of the caller");
    }
  }

  /**
   * {@code streamrune.tracing.event-store.enabled=false} removes every {@code eventstore/…} span of
   * the integration's PostgreSQL event store, and the command and its lock are still traced.
   */
  @Test
  public void theEventStoreSwitchRemovesTheStoresSpansOnly() {
    OpenTelemetryBean.reset();
    try (Application application =
        start(
            postgresql(),
            Map.of("streamrune.tracing.event-store.enabled", "false"),
            Set.of(OpenTelemetryBean.class, ContractCommandRegistration.class))) {
      assertStarted(application);
      application.bean(VirtualThreadCommandBus.class).execute(new ContractCommand("untraced-1"));

      List<String> names = spanNames();
      assertTrue(names.stream().anyMatch(n -> n.startsWith("command/")), names::toString);
      assertTrue(names.contains(TraceAttributes.LOCK_ACQUIRE), names::toString);
      assertTrue(names.stream().noneMatch(n -> n.startsWith("eventstore/")), names::toString);
    }
  }

  /**
   * Each area the integration wires has its switch: with every area on, a command, its lock, the
   * projection runner, the inline projection and the outbox relay are traced; {@code
   * streamrune.tracing.<area>.enabled= false} removes exactly that area's spans. The application's
   * {@code StreamRuneTracing} bean, which the application hands to the components it builds itself,
   * answers every area's switch, the subscription and saga areas included.
   */
  @Test
  public void eachAreaSwitchRemovesExactlyThatArea() throws InterruptedException {
    Map<TracingArea, Predicate<String>> areaSpans = new LinkedHashMap<>();
    areaSpans.put(TracingArea.COMMAND, n -> n.startsWith("command/"));
    areaSpans.put(TracingArea.LOCK, TraceAttributes.LOCK_ACQUIRE::equals);
    areaSpans.put(
        TracingArea.PROJECTION, n -> n.equals(PROJECTION_SPAN) || n.equals(INLINE_PROJECTION_SPAN));
    areaSpans.put(TracingArea.OUTBOX, TraceAttributes.OUTBOX_RELAY::equals);

    List<TracingArea> runs = new ArrayList<>();
    runs.add(null);
    runs.addAll(List.of(TracingArea.values()));
    for (TracingArea off : runs) {
      OpenTelemetryBean.reset();
      Map<String, String> properties = new LinkedHashMap<>(PROMPT_RUNNERS);
      properties.put("streamrune.outbox.enabled", "true");
      if (off != null) {
        properties.put(areaKey(off), "false");
      }
      try (Application application = start(null, properties, TRACED_APPLICATION)) {
        assertStarted(application);
        StreamRuneTracing tracing = application.bean(StreamRuneTracing.class);
        for (TracingArea area : TracingArea.values()) {
          assertEquals(
              area != off, tracing.enabled(area), () -> area + " with " + areaKey(off) + " off");
        }
        application.bean(VirtualThreadCommandBus.class).execute(new ContractCommand("areas-1"));
        OutboxStore outbox = application.bean(OutboxStore.class);
        outbox.save(
            OutboxEntry.pending(
                OutboxEntryId.of("areas-entry"), "{}", "ContractPayload", OUTBOX_STREAM));
        OffsetStore offsets = application.bean(OffsetStore.class);
        await(
            () ->
                outbox.findById(OutboxEntryId.of("areas-entry")).orElseThrow().status()
                        == OutboxStatus.DELIVERED
                    && offsets.getLastOffset(ProjectionName.of(ContractProjection.NAME)).value()
                        > 0,
            "the outbox relay must deliver the entry and the projection runner process the event");

        for (Map.Entry<TracingArea, Predicate<String>> area : areaSpans.entrySet()) {
          if (area.getKey() != off) {
            await(
                () -> spanNames().stream().anyMatch(area.getValue()),
                () ->
                    area.getKey()
                        + " must be traced with "
                        + areaKey(off)
                        + " off: "
                        + spanNames());
          }
        }
        if (off != TracingArea.PROJECTION) {
          for (String projectionSpan : List.of(INLINE_PROJECTION_SPAN, PROJECTION_SPAN)) {
            await(
                () -> spanNames().contains(projectionSpan),
                () -> projectionSpan + " with " + areaKey(off) + " off: " + spanNames());
          }
        }
        if (off != null && areaSpans.containsKey(off)) {
          // The spans of the work just awaited end right after it: give them the time to.
          Thread.sleep(200);
          List<String> names = spanNames();
          assertTrue(
              names.stream().noneMatch(areaSpans.get(off)),
              () -> areaKey(off) + "=false must remove its spans: " + names);
        }
      }
    }
  }

  /**
   * {@code streamrune.tracing.enabled=false}: StreamRune starts no span at all, though the
   * application has an {@code OpenTelemetry} bean, and its tracing bean is {@code
   * StreamRuneTracing.NOOP}.
   */
  @Test
  public void theMasterSwitchOffStartsNoSpan() throws InterruptedException {
    OpenTelemetryBean.reset();
    Map<String, String> properties = new LinkedHashMap<>(PROMPT_RUNNERS);
    properties.put("streamrune.outbox.enabled", "true");
    properties.put("streamrune.tracing.enabled", "false");
    try (Application application = start(null, properties, TRACED_APPLICATION)) {
      assertStarted(application);
      assertSame(StreamRuneTracing.NOOP, application.bean(StreamRuneTracing.class));
      application.bean(VirtualThreadCommandBus.class).execute(new ContractCommand("off-1"));
      OutboxStore outbox = application.bean(OutboxStore.class);
      outbox.save(
          OutboxEntry.pending(
              OutboxEntryId.of("off-entry"), "{}", "ContractPayload", OUTBOX_STREAM));
      await(
          () ->
              outbox.findById(OutboxEntryId.of("off-entry")).orElseThrow().status()
                  == OutboxStatus.DELIVERED,
          "the outbox relay must deliver the entry");
      assertEquals(List.of(), spanNames());
    }
  }

  /**
   * An OpenTelemetry SDK that is present but switched off by the SDK's own setting ({@code
   * otel.sdk.disabled=true}) is no OpenTelemetry to StreamRune: the tracing bean is {@code
   * StreamRuneTracing.NOOP}, so no span starts and the command bus, which then holds no
   * command-span interceptor, writes no span context into an event.
   */
  @Test
  public void anOpenTelemetrySwitchedOffBySdkSettingsStartsNoSpan() {
    for (Map.Entry<String, String> off :
        List.of(Map.entry("otel.sdk.disabled", "true"), Map.entry("otel.sdk.disabled", "TRUE"))) {
      OpenTelemetryBean.reset();
      try (Application application =
          start(
              null,
              Map.ofEntries(off),
              Set.of(OpenTelemetryBean.class, ContractCommandRegistration.class))) {
        assertStarted(application);
        assertSame(
            StreamRuneTracing.NOOP, application.bean(StreamRuneTracing.class), off::toString);
        VirtualThreadCommandBus bus = application.bean(VirtualThreadCommandBus.class);
        assertTrue(
            bus.interceptors().stream().noneMatch(TracingCommandInterceptor.class::isInstance),
            off::toString);
        bus.execute(new ContractCommand("switched-off-1"));
        assertEquals(List.of(), spanNames(), off::toString);
      }
    }
  }

  /**
   * The application's own {@code StreamRuneTracing} bean wins over the one the integration would
   * build from the {@code OpenTelemetry} bean: the integration's components trace through it.
   */
  @Test
  public void theApplicationsTracingBeanWins() {
    OpenTelemetryBean.reset();
    ApplicationTracing.reset();
    try (Application application =
        start(
            null,
            Map.of(),
            Set.of(
                OpenTelemetryBean.class,
                ApplicationTracing.class,
                ContractCommandRegistration.class))) {
      assertStarted(application);
      application.bean(VirtualThreadCommandBus.class).execute(new ContractCommand("own-1"));

      List<String> names = ApplicationTracing.spanNames();
      assertTrue(names.stream().anyMatch(n -> n.startsWith("command/")), names::toString);
      assertTrue(names.contains(TraceAttributes.LOCK_ACQUIRE), names::toString);
      assertEquals(List.of(), spanNames());
    }
  }

  /**
   * A tracing that throws fails nothing and is counted: the application's own {@code
   * StreamRuneTracing} bean, whose every {@code start} throws, costs the spans and no command, and
   * each failure is counted in the application's metrics as {@code streamrune.tracing.failures}, by
   * the operation that threw. One whose {@code enabled} throws as well fails no startup: the
   * integration asks it through the guard, as every component does.
   */
  @Test
  public void aTracingThatThrowsFailsNothingAndIsCounted() {
    ContractBeans.ThrowingApplicationTracing.reset();
    ContractBeans.ApplicationMetrics.reset();
    Set<Class<?>> beans =
        Set.of(
            ContractBeans.ThrowingApplicationTracing.class,
            ContractBeans.ApplicationMetrics.class,
            ContractCommandRegistration.class);
    try (Application application = start(null, Map.of(), beans)) {
      assertStarted(application);
      VirtualThreadCommandBus bus = application.bean(VirtualThreadCommandBus.class);
      assertEquals(1, bus.execute(new ContractCommand("throwing-1")).events().size());
      assertEquals(1, bus.execute(new ContractCommand("throwing-1")).events().size());

      int thrown = ContractBeans.ThrowingApplicationTracing.thrown();
      assertTrue(thrown >= 2, () -> "the tracing must have thrown for each command: " + thrown);
      assertEquals(
          thrown,
          ContractBeans.ApplicationMetrics.recorded()
              .total(
                  org.streamrune.core.metrics.MetricNames.TRACING_FAILURES,
                  org.streamrune.core.metrics.MetricNames.TAG_OPERATION,
                  "start"),
          "every failure of the tracing is counted once, in the application's metrics");
    }

    ContractBeans.ThrowingApplicationTracing.reset();
    ContractBeans.ApplicationMetrics.reset();
    ContractBeans.ThrowingApplicationTracing.enabledThrows(true);
    try (Application application = start(null, Map.of(), beans)) {
      assertStarted(application);
      VirtualThreadCommandBus bus = application.bean(VirtualThreadCommandBus.class);
      assertEquals(1, bus.execute(new ContractCommand("throwing-2")).events().size());
      int enabledThrown = ContractBeans.ThrowingApplicationTracing.thrown();
      assertTrue(enabledThrown > 0, "enabled() must have thrown while the application started");
      assertEquals(
          enabledThrown,
          ContractBeans.ApplicationMetrics.recorded()
              .total(
                  org.streamrune.core.metrics.MetricNames.TRACING_FAILURES,
                  org.streamrune.core.metrics.MetricNames.TAG_OPERATION,
                  "enabled"),
          "every enabled() that throws is counted, whichever component asked");
    } finally {
      ContractBeans.ThrowingApplicationTracing.reset();
    }
  }

  /**
   * A {@code TracingCommandInterceptor} the application declares is the one the command bus runs:
   * one command is one {@code command/…} span, never two.
   */
  @Test
  public void theApplicationsCommandInterceptorIsNotDoubled() {
    OpenTelemetryBean.reset();
    try (Application application =
        start(
            null,
            Map.of(),
            Set.of(
                OpenTelemetryBean.class,
                ApplicationCommandInterceptor.class,
                ContractCommandRegistration.class))) {
      assertStarted(application);
      VirtualThreadCommandBus bus = application.bean(VirtualThreadCommandBus.class);
      List<CommandInterceptor> tracingInterceptors =
          bus.interceptors().stream().filter(TracingCommandInterceptor.class::isInstance).toList();
      assertEquals(
          List.of(application.bean(TracingCommandInterceptor.class)),
          tracingInterceptors,
          "the bus runs the application's interceptor, and no other");
      bus.execute(new ContractCommand("once-1"));

      List<String> names = spanNames();
      assertEquals(
          1, names.stream().filter(n -> n.startsWith("command/")).count(), names::toString);
    }
  }

  /**
   * An {@code AggregateLocker} the application declares in the place of the integration's default,
   * a framework locker built without a tracing, is traced by the integration's command bus on its
   * own locks: one command is one {@code lock/acquire} span, and {@code
   * streamrune.tracing.lock.enabled=false} leaves none.
   */
  @Test
  public void anApplicationsLockerIsTracedOnItsOwnLocks() {
    for (boolean lockArea : List.of(true, false)) {
      OpenTelemetryBean.reset();
      Map<String, String> properties =
          lockArea ? Map.of() : Map.of("streamrune.tracing.lock.enabled", "false");
      try (Application application =
          start(
              null,
              properties,
              Set.of(
                  OpenTelemetryBean.class,
                  ApplicationLocker.class,
                  ContractCommandRegistration.class))) {
        assertStarted(application);
        application.bean(VirtualThreadCommandBus.class).execute(new ContractCommand("locked-1"));

        List<String> names = spanNames();
        assertEquals(
            lockArea ? 1 : 0,
            names.stream().filter(TraceAttributes.LOCK_ACQUIRE::equals).count(),
            () -> "lock area " + (lockArea ? "on" : "off") + ": " + names);
      }
    }
  }

  /**
   * Without an {@code OpenTelemetry} bean the integration takes the global OpenTelemetry, which the
   * OpenTelemetry Java agent or an SDK registered globally sets: a command is traced through it.
   */
  @Test
  public void withoutABeanTheGlobalOpenTelemetryTraces() {
    OpenTelemetryBean.reset();
    GlobalOpenTelemetry.resetForTest();
    GlobalOpenTelemetry.set(OpenTelemetryBean.view());
    try (Application application =
        start(null, Map.of(), Set.of(ContractCommandRegistration.class))) {
      assertStarted(application);
      application.bean(VirtualThreadCommandBus.class).execute(new ContractCommand("global-1"));

      List<String> names = spanNames();
      assertTrue(names.stream().anyMatch(n -> n.startsWith("command/")), names::toString);
      assertTrue(names.contains(TraceAttributes.LOCK_ACQUIRE), names::toString);
    } finally {
      GlobalOpenTelemetry.resetForTest();
    }
  }

  /**
   * Without an {@code OpenTelemetry} bean and with no global OpenTelemetry registered, the
   * integration's tracing is {@code StreamRuneTracing.NOOP}: nothing is traced, at no cost, and a
   * command runs.
   */
  @Test
  public void withoutOpenTelemetryTheTracingIsNoop() {
    OpenTelemetryBean.reset();
    GlobalOpenTelemetry.resetForTest();
    try (Application application =
        start(null, Map.of(), Set.of(ContractCommandRegistration.class))) {
      assertStarted(application);
      assertSame(StreamRuneTracing.NOOP, application.bean(StreamRuneTracing.class));
      application.bean(VirtualThreadCommandBus.class).execute(new ContractCommand("noop-1"));
      assertEquals(List.of(), spanNames());
    } finally {
      GlobalOpenTelemetry.resetForTest();
    }
  }

  /** The key of an area's switch, {@code streamrune.tracing.<area>.enabled}. */
  static String areaKey(TracingArea area) {
    if (area == null) {
      return "no area";
    }
    return "streamrune.tracing."
        + area.name().toLowerCase(Locale.ROOT).replace('_', '-')
        + ".enabled";
  }

  private static List<String> spanNames() {
    return OpenTelemetryBean.spans().stream().map(SpanData::getName).toList();
  }

  private static SpanData onlySpan(List<SpanData> spans, Predicate<SpanData> which) {
    List<SpanData> matching = spans.stream().filter(which).toList();
    assertEquals(
        1,
        matching.size(),
        () ->
            "exactly one span must match; spans: "
                + spans.stream().map(SpanData::getName).toList());
    return matching.getFirst();
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
    await(condition, () -> what);
  }

  /** Polls {@code condition} until it holds or {@link #patience()} has passed. */
  private void await(
      java.util.function.BooleanSupplier condition, java.util.function.Supplier<String> what)
      throws InterruptedException {
    long deadline = System.nanoTime() + patience().toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        fail(what.get() + " (waited " + patience() + ")");
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
      if (cells.size() >= 2
          && "Property".equals(cells.getFirst())
          && "Default".equals(cells.get(1))) {
        inTable = true;
      } else if (inTable && cells.size() >= 2) {
        String key = cells.getFirst().replace("`", "");
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
      return Files.readString(path);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
