package org.streamrune.integration.tck;

import com.sun.net.httpserver.HttpServer;
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
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.integration.CommandDeadLetterRetentionValidator;
import org.streamrune.integration.OutboxWiringMessages;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SnapshotPurgeSetting;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.integration.SubscriptionLeadershipResolution;
import org.streamrune.integration.tck.ContractBeans.ContractProjection;
import org.streamrune.runtime.SagaLeaseWiring;
import org.streamrune.runtime.SagaRunner;

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
    DOCUMENTED_DEFAULTS
  }

  private final List<Object> beans;
  private final String failure;
  private final List<String> logLines = new CopyOnWriteArrayList<>();
  private final Map<String, String> properties;
  private final Set<Rule> broken;
  private final DataSource dataSource;
  private HttpServer server;
  private Thread runner;

  private ReferenceApplication(
      DataSource dataSource,
      Map<String, String> properties,
      Set<Class<?>> extraBeans,
      Set<Rule> broken) {
    this.dataSource = dataSource;
    this.properties = properties;
    this.broken = broken.isEmpty() ? EnumSet.noneOf(Rule.class) : EnumSet.copyOf(broken);
    this.beans = extraBeans.stream().map(ContractBeans::create).collect(Collectors.toList());
    String refusal = null;
    try {
      validate();
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
        && !beansOf(OutboxStore.class).isEmpty()) {
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
    SubscriptionLeadership leadership = leaderships.isEmpty() ? null : leaderships.getFirst();
    if (keeps(Rule.UNGATED_SAGA_WARNING)
        && SagaLeaseWiring.warnUngatedUnderLeadership(runners, leadership) > 0) {
      // The contract's only saga runner is of ContractSagaState: its saga type is that class.
      logLines.add(
          "WARN org.streamrune.runtime.SagaLeaseWiring: Saga type "
              + ContractBeans.ContractSagaState.class.getName()
              + " is delivered on every replica: its SagaRunner carries no lease.");
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
    if (!Boolean.parseBoolean(properties.get("streamrune.sse.enabled"))) {
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
