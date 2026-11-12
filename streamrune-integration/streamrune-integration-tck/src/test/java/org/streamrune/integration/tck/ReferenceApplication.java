package org.streamrune.integration.tck;

import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.TracerProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.EventStore;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.CommandDeadLetterRetentionValidator;
import org.streamrune.integration.OutboxWiringMessages;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SnapshotPurgeSetting;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.integration.SubscriptionLeadershipResolution;
import org.streamrune.integration.tck.ContractBeans.ContractCommand;
import org.streamrune.integration.tck.ContractBeans.ContractCommandRegistration;
import org.streamrune.integration.tck.ContractBeans.ContractInlineProjection;
import org.streamrune.integration.tck.ContractBeans.ContractProjection;
import org.streamrune.integration.tck.ContractBeans.DispatchingRoute;
import org.streamrune.integration.tck.ContractBeans.RecordingAuditStore;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.GraalJitGuard;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;
import org.streamrune.runtime.OpenTelemetryTracing;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.SagaLeaseWiring;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.TracingProjectionDecorator;
import org.streamrune.runtime.TracingSettings;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * An application that keeps every rule of {@link IntegrationWiringContract} with a few lines each,
 * through the integration-api pieces the three integrations share: what the contract's own tests
 * run it against. {@link Rule} names the rules; an application started with one of them in {@code
 * broken} does not keep it, which is how the contract's tests show that each test fails on an
 * application that breaks its rule.
 */
final class ReferenceApplication implements Application {

  /** The rules the reference application keeps unless told to break one. */
  enum Rule {
    OUTBOX_RELAY,
    ONE_LEADERSHIP,
    SNAPSHOT_PURGE_DECISION,
    DEAD_LETTER_RETENTION,
    FAIL_CLOSED_IDENTITY,
    HEALTH_DETAILS,
    UNGATED_SAGA_WARNING,
    RUNNERS_STOP_FIRST,
    DOCUMENTED_DEFAULTS,
    ROUTE_CALLER_AUDITED,
    BUS_ANSWERS_BY_RUNNER_RULES,
    JVM_CHECK_OPT_OUT_REACHES_THE_BUS,
    STORE_TRACED,
    EVENT_STORE_SWITCH,
    AREA_SWITCHES,
    MASTER_SWITCH,
    APPLICATION_TRACING_WINS,
    ONE_COMMAND_INTERCEPTOR,
    GLOBAL_OPENTELEMETRY,
    NOOP_WITHOUT_OPENTELEMETRY
  }

  private final List<Object> beans;
  private final String failure;
  private final List<String> logLines = new CopyOnWriteArrayList<>();
  private final Map<String, String> properties;
  private final Set<Rule> broken;
  private final DataSource dataSource;
  private HttpServer server;
  private Thread runner;
  private VirtualThreadCommandBus commandBus;
  private StreamRuneTracing tracing = StreamRuneTracing.NOOP;
  private EventStore eventStore;
  private MultiProjectionRunner projectionRunner;
  private OutboxPoller outboxPoller;

  private ReferenceApplication(
      DataSource dataSource,
      Map<String, String> properties,
      Set<Class<?>> extraBeans,
      Set<Rule> broken) {
    this.dataSource = dataSource;
    this.properties = properties;
    this.broken = broken.isEmpty() ? EnumSet.noneOf(Rule.class) : EnumSet.copyOf(broken);
    this.beans =
        extraBeans.stream()
            .map(ContractBeans::create)
            .collect(Collectors.toCollection(ArrayList::new));
    String refusal = null;
    try {
      validate();
      tracing = tracing();
      if (beansOf(StreamRuneTracing.class).isEmpty()) {
        beans.add(tracing);
      }
      eventStore = eventStore();
      buildDeadLetterRetry();
      if (commandBus == null) {
        commandBus = commandBusBuilder().build();
      }
      beans.add(commandBus);
      startTracedRunners();
      startRunner();
      startServer();
    } catch (RuntimeException e) {
      refusal = e.getMessage();
    }
    this.failure = refusal;
  }

  static ReferenceApplication start(
      DataSource dataSource,
      Map<String, String> properties,
      Set<Class<?>> extraBeans,
      Set<Rule> broken) {
    return new ReferenceApplication(dataSource, properties, extraBeans, broken);
  }

  private boolean keeps(Rule rule) {
    return !broken.contains(rule);
  }

  private <T> List<T> beansOf(Class<T> type) {
    return beans.stream().filter(type::isInstance).map(type::cast).toList();
  }

  private void validate() {
    if (keeps(Rule.OUTBOX_RELAY)
        && Boolean.parseBoolean(properties.get("streamrune.outbox.enabled"))
        && !beansOf(OutboxStore.class).isEmpty()
        && beansOf(OutboxPublisher.class).isEmpty()) {
      throw new IllegalStateException(OutboxWiringMessages.noPublisher());
    }
    List<SubscriptionLeadership> leaderships = beansOf(SubscriptionLeadership.class);
    if (keeps(Rule.ONE_LEADERSHIP) && leaderships.size() > 1) {
      throw SubscriptionLeadershipResolution.ambiguous(
          leaderships.stream().map(l -> l.getClass().getName()).toList(), "as the default");
    }
    if (keeps(Rule.SNAPSHOT_PURGE_DECISION) && !beansOf(CryptoEngine.class).isEmpty()) {
      SnapshotPurgeSetting.parse(properties.get(SnapshotPurgeSetting.PROPERTY))
          .withoutSnapshotPurge(List.of());
    }
    if (keeps(Rule.DEAD_LETTER_RETENTION)
        && !beansOf(CommandInbox.class).isEmpty()
        && !beansOf(DeadLetterQueue.class).isEmpty()) {
      CommandDeadLetterRetentionValidator.validate(
          Duration.parse(
              properties.getOrDefault("streamrune.dead-letter.retention-max-age", "PT168H")),
          Duration.ofDays(7));
    }
    @SuppressWarnings("unchecked")
    List<SagaRunner<?>> runners = (List<SagaRunner<?>>) (List<?>) beansOf(SagaRunner.class);
    SubscriptionLeadership leadership =
        leaderships.isEmpty() ? SubscriptionLeadership.NOOP : leaderships.getFirst();
    if (keeps(Rule.UNGATED_SAGA_WARNING)
        && SagaLeaseWiring.warnUngatedUnderLeadership(runners, leadership) > 0) {
      // The contract's only saga runner is of ContractSagaState: its saga type is that class.
      logLines.add(
          "WARN org.streamrune.runtime.SagaLeaseWiring: Saga type "
              + ContractBeans.ContractSagaState.class.getName()
              + " is delivered on every replica: its SagaRunner carries no lease.");
    }
  }

  /**
   * The tracing an integration builds: the application's {@code StreamRuneTracing} bean, or the one
   * over its {@code OpenTelemetry} bean or else the global OpenTelemetry, under the {@code
   * streamrune.tracing.*} switches; {@code NOOP} with the master switch off or without a tracer
   * provider.
   */
  private StreamRuneTracing tracing() {
    List<StreamRuneTracing> own = beansOf(StreamRuneTracing.class);
    if (keeps(Rule.APPLICATION_TRACING_WINS) && !own.isEmpty()) {
      return own.getFirst();
    }
    TracingSettings settings = settings(keeps(Rule.AREA_SWITCHES));
    if (keeps(Rule.MASTER_SWITCH) && !settings.enabled()) {
      return StreamRuneTracing.NOOP;
    }
    List<OpenTelemetry> beansOfOpenTelemetry = beansOf(OpenTelemetry.class);
    OpenTelemetry source =
        !beansOfOpenTelemetry.isEmpty()
            ? beansOfOpenTelemetry.getFirst()
            : keeps(Rule.GLOBAL_OPENTELEMETRY) ? GlobalOpenTelemetry.get() : OpenTelemetry.noop();
    if (keeps(Rule.NOOP_WITHOUT_OPENTELEMETRY)
        && source.getTracerProvider() == TracerProvider.noop()) {
      return StreamRuneTracing.NOOP;
    }
    return OpenTelemetryTracing.of(
        source, keeps(Rule.MASTER_SWITCH) ? settings : settings(keeps(Rule.AREA_SWITCHES), true));
  }

  private TracingSettings settings(boolean areas) {
    return settings(areas, !"false".equals(properties.get("streamrune.tracing.enabled")));
  }

  private TracingSettings settings(boolean areas, boolean enabled) {
    var settings = TracingSettings.builder().enabled(enabled);
    if (areas) {
      for (TracingArea area : TracingArea.values()) {
        settings.area(
            area, !"false".equals(properties.get(IntegrationWiringContract.areaKey(area))));
      }
    }
    return settings.build();
  }

  /**
   * The event store: with a database, the store an integration builds on it, traced (a serializing
   * in-memory store stands in for PostgreSQL); without one, the application's in-memory store,
   * which the integration does not build and does not trace.
   */
  private EventStore eventStore() {
    if (dataSource == null) {
      return new InMemoryEventStore();
    }
    var store =
        InMemoryEventStore.serializing(
            SimpleEventTypeRegistry.builder()
                .registerEvent(ContractBeans.ContractCommandExecuted.class)
                .build(),
            null);
    if (!keeps(Rule.STORE_TRACED)) {
      return store;
    }
    StreamRuneTracing storeTracing =
        keeps(Rule.EVENT_STORE_SWITCH) || beansOf(OpenTelemetry.class).isEmpty()
            ? tracing
            : OpenTelemetryTracing.of(
                beansOf(OpenTelemetry.class).getFirst(), TracingSettings.allEnabled());
    return store.withTracing(storeTracing);
  }

  /**
   * The builder of the application's command bus, as an integration starts it: on the event store,
   * with the opt-out of the JVM check that {@value GraalJitGuard#ALLOW_PROPERTY} configures, unless
   * the rule is broken, the application's command registrations and interceptors, the inline
   * projections decorated, and the tracing, which adds the command interceptor unless the
   * application declared one.
   */
  private VirtualThreadCommandBus.Builder commandBusBuilder() {
    var bus = VirtualThreadCommandBus.builder().eventStore(eventStore).tracing(tracing);
    if (keeps(Rule.JVM_CHECK_OPT_OUT_REACHES_THE_BUS)) {
      bus.allowGraalJit(Boolean.parseBoolean(properties.get(GraalJitGuard.ALLOW_PROPERTY)));
    }
    List<CommandInterceptor> interceptors = new ArrayList<>(beansOf(CommandInterceptor.class));
    if (!keeps(Rule.ONE_COMMAND_INTERCEPTOR) && !interceptors.isEmpty()) {
      interceptors.addFirst(new OpenTelemetryCommandInterceptor(tracing));
    }
    for (ContractInlineProjection inline : beansOf(ContractInlineProjection.class)) {
      ProjectionName name = ProjectionName.of(ContractInlineProjection.NAME);
      interceptors.add(
          InlineProjectionInterceptor.builder()
              .register(name, TracingProjectionDecorator.traced(name, inline, tracing))
              .build());
    }
    bus.interceptors(interceptors);
    for (DeciderRegistration<?, ?, ?> registration : beansOf(DeciderRegistration.class)) {
      register(bus, registration);
    }
    return bus;
  }

  private static <
          C extends org.streamrune.core.Command,
          S extends org.streamrune.core.AggregateState,
          E extends org.streamrune.core.DomainEvent>
      void register(
          VirtualThreadCommandBus.Builder bus, DeciderRegistration<C, S, E> registration) {
    bus.register(
        registration.aggregateType(),
        registration.commandType(),
        registration.idExtractor(),
        registration.decider());
  }

  /**
   * Without a database, the projection runner over the application's stores and the outbox relay
   * with the outbox enabled, both traced, as an integration builds them.
   */
  private void startTracedRunners() {
    if (dataSource == null && !beansOf(ContractProjection.class).isEmpty()) {
      OffsetStore offsets = new InMemoryOffsetStore();
      beans.add(offsets);
      projectionRunner =
          MultiProjectionRunner.builder()
              .eventStore(eventStore)
              .offsetStore(offsets)
              .subscriptionConfig(
                  new SubscriptionConfig(true, Duration.ofMillis(50), Duration.ofMillis(10)))
              .tracing(tracing)
              .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
              .register(
                  ProjectionName.of(ContractProjection.NAME),
                  beansOf(ContractProjection.class).getFirst(),
                  ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
              .build();
      projectionRunner.start();
    }
    if (Boolean.parseBoolean(properties.get("streamrune.outbox.enabled"))
        && !beansOf(OutboxStore.class).isEmpty()
        && !beansOf(OutboxPublisher.class).isEmpty()) {
      outboxPoller =
          OutboxPoller.builder()
              .outboxStore(beansOf(OutboxStore.class).getFirst())
              .publisher(beansOf(OutboxPublisher.class).getFirst())
              .pollInterval(Duration.ofMillis(50))
              .tracing(tracing)
              .build();
      outboxPoller.start();
    }
  }

  /**
   * With a command inbox and a dead-letter queue, builds the command bus and the retry runner from
   * the same keys, as an integration does, and keeps what the runner logs when it is built: the bus
   * answers a keyed re-send by the runner's {@code maxRetries} and inbox window, unless the rule is
   * broken.
   */
  private void buildDeadLetterRetry() {
    List<CommandInbox> inboxes = beansOf(CommandInbox.class);
    List<DeadLetterQueue> queues = beansOf(DeadLetterQueue.class);
    if (inboxes.isEmpty() || queues.isEmpty()) {
      return;
    }
    var policy =
        new DeadLetterRetryPolicy(
            Integer.parseInt(properties.getOrDefault("streamrune.dead-letter.max-retries", "5")),
            Duration.parse(
                properties.getOrDefault("streamrune.dead-letter.retry-interval", "PT60S")),
            2.0,
            false,
            DeadLetterRetryPolicy.UnkeyedReplay.OPERATOR_ONLY);
    Duration window =
        Duration.parse(properties.getOrDefault("streamrune.inbox.retention-max-age", "PT168H"));
    var bus =
        commandBusBuilder()
            .commandInbox(inboxes.getFirst())
            .deadLetterQueue(queues.getFirst())
            .objectMapper(DeadLetterRetryRunner.createObjectMapper(null));
    if (keeps(Rule.BUS_ANSWERS_BY_RUNNER_RULES)) {
      bus.deadLetterRetryPolicy(policy).inboxRetentionMaxAge(window);
    }
    commandBus = bus.build();
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(DeadLetterRetryRunner.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      beans.add(
          DeadLetterRetryRunner.builder()
              .deadLetterQueue(queues.getFirst())
              .commandBus(commandBus)
              .objectMapper(DeadLetterRetryRunner.createObjectMapper(null))
              .policy(policy)
              .inboxRetentionMaxAge(window)
              .build());
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    for (var event : appender.list) {
      if (event.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN)) {
        logLines.add(
            event.getLevel() + " " + event.getLoggerName() + ": " + event.getFormattedMessage());
      }
    }
  }

  /** With a database and a projection, polls the database the way a projection runner does. */
  private void startRunner() {
    if (dataSource == null || beansOf(ContractProjection.class).isEmpty()) {
      return;
    }
    runner =
        Thread.ofVirtual()
            .name("reference-projection-runner")
            .start(
                () -> {
                  while (!Thread.currentThread().isInterrupted()) {
                    try (var connection = dataSource.getConnection()) {
                      Thread.sleep(5);
                    } catch (InterruptedException e) {
                      return;
                    } catch (Exception e) {
                      // a closed DataSource: the poll fails and the next one is tried
                    }
                  }
                });
  }

  private void startServer() {
    boolean sse = Boolean.parseBoolean(properties.get("streamrune.sse.enabled"));
    boolean route = !beansOf(DispatchingRoute.class).isEmpty();
    if (!sse && !route) {
      return;
    }
    boolean trusted =
        keeps(Rule.FAIL_CLOSED_IDENTITY)
            ? Boolean.parseBoolean(
                properties.get(RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY))
            : true;
    RequestIdentityPolicy policy = RequestIdentityPolicy.of(null, trusted);
    SseAuthorizer authorizer =
        beansOf(SseAuthorizer.class).stream().findFirst().orElse(SseAuthorizer.DENY_ALL);
    try {
      server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    server.createContext(
        "/api/sse/",
        exchange -> {
          String[] path = exchange.getRequestURI().getPath().split("/");
          var caller =
              new StreamRuneContext.RequestContext(
                  null,
                  policy.resolve(
                      exchange.getRequestHeaders().get(RequestIdentityPolicy.USER_ID_HEADER),
                      (header, principal) -> {}),
                  CorrelationId.of("reference-request"),
                  Instant.now(),
                  Map.of(),
                  null);
          StreamId stream = StreamId.of(AggregateType.of(path[3]), AggregateId.of(path[4]));
          if (authorizer.isAuthorized(caller, stream)) {
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write(": keepalive\n\n".getBytes(StandardCharsets.UTF_8));
          } else {
            exchange.sendResponseHeaders(403, -1);
          }
          exchange.close();
        });
    server.createContext(
        DispatchingRoute.PATH + "/",
        exchange -> {
          String id =
              exchange.getRequestURI().getPath().substring(DispatchingRoute.PATH.length() + 1);
          UserId caller =
              policy.resolve(
                  exchange.getRequestHeaders().get(RequestIdentityPolicy.USER_ID_HEADER),
                  (header, principal) -> {});
          // The command's audit row, as the integration's audit interceptor writes it: it names
          // the caller the request edge identified, unless the rule is broken.
          beansOf(RecordingAuditStore.class)
              .getFirst()
              .save(
                  new AuditEntry(
                      CommandId.of("reference-" + id),
                      ContractCommand.class.getSimpleName(),
                      AggregateType.of(ContractCommandRegistration.TYPE),
                      AggregateId.of(id),
                      keeps(Rule.ROUTE_CALLER_AUDITED) ? caller : null,
                      Instant.now(),
                      AuditOutcome.SUCCESS,
                      null,
                      1,
                      CorrelationId.of("reference-request")));
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    server.start();
  }

  @Override
  public boolean started() {
    return failure == null;
  }

  @Override
  public String startupFailureMessage() {
    return failure;
  }

  @Override
  public <T> T bean(Class<T> type) {
    return beansOf(type).getFirst();
  }

  @Override
  public HttpResponse http(String method, String path, Map<String, String> headers, String body) {
    return ContractHttp.send(server.getAddress().getPort(), method, path, headers, body);
  }

  @Override
  public Map<String, Object> health() {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("eventStore.lastGlobalOffset", 0L);
    if (keeps(Rule.HEALTH_DETAILS) && !beansOf(ContractProjection.class).isEmpty()) {
      // The runner's role under its lease: on by default with a database, the only replica leads.
      boolean leased =
          dataSource != null
              && !"false"
                  .equals(properties.get("streamrune.subscription.single-active-consumer.enabled"));
      details.put(
          "subscription." + ContractProjection.NAME,
          Map.of("status", "UP", "role", leased ? "LEADING" : "UNGATED"));
    }
    Map<String, Object> health = new LinkedHashMap<>();
    health.put("status", "UP");
    health.put("details", details);
    return health;
  }

  @Override
  public List<String> startupLogLines() {
    return new ArrayList<>(logLines);
  }

  /** The bound defaults: the configuration reference's, one of them changed when it is broken. */
  Map<String, String> configurationDefaults() {
    Map<String, String> defaults = new LinkedHashMap<>();
    IntegrationWiringContract.configurationReference()
        .forEach(
            (key, value) -> {
              if (!key.contains("<")) {
                defaults.put(key, value);
              }
            });
    if (!keeps(Rule.DOCUMENTED_DEFAULTS)) {
      defaults.put("streamrune.snapshot-every-n-events", "50");
      defaults.remove("streamrune.lock-timeout");
    }
    return defaults;
  }

  @Override
  public void close() {
    if (server != null) {
      server.stop(0);
    }
    if (outboxPoller != null) {
      outboxPoller.close();
    }
    if (projectionRunner != null) {
      projectionRunner.close();
    }
    if (commandBus != null) {
      commandBus.close();
    }
    if (keeps(Rule.RUNNERS_STOP_FIRST)) {
      stopRunner();
      closeDataSource();
    } else {
      closeDataSource();
      stopRunnerLater();
    }
  }

  private void stopRunner() {
    if (runner != null) {
      runner.interrupt();
      try {
        runner.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void stopRunnerLater() {
    if (runner != null) {
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  Thread.sleep(200);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                stopRunner();
              });
    }
  }

  private void closeDataSource() {
    if (dataSource instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
