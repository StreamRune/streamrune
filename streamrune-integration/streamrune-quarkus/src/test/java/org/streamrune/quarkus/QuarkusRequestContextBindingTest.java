package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.arc.Arc;
import io.quarkus.resteasy.reactive.common.runtime.ArcThreadSetupAction;
import io.quarkus.runtime.configuration.DurationConverter;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.eclipse.microprofile.health.Readiness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Authorization;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.audit.Auditable;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.tck.ContractBeans.RecordingAuditStore;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.SagaDeadLetterReplayer;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The caller of a request reaches the command bus and the query bus of a Quarkus application from a
 * resource method that binds nothing itself.
 *
 * <p>{@link StreamRuneRequestFilter} cannot wrap the resource method: it stores the request's
 * context in the request-scoped {@link StreamRuneRequestContextHolder}, and {@link
 * StreamRuneContext#CURRENT} is not bound while the method runs. The integration's command bus and
 * query bus take the holder's context for a dispatch that starts with {@code CURRENT} unbound
 * inside an active CDI request context, so a command, a command sent through {@code executeAsync},
 * an {@code @Auditable} query and a saga dead-letter replay dispatched from the resource run as the
 * request's caller: their events' metadata and their audit rows name the caller, and the query's
 * handler sees it. A dispatch with no request stays anonymous.
 *
 * <p>The application is the integration's beans in a real Arc container ({@link
 * RealArcTestContainer}), the in-memory stores of the test kit, and the resource below served by
 * Quarkus REST ({@link QuarkusRestTestServer}) as a Quarkus application serves it: each request
 * runs inside a CDI request context of its own, and the filter writes the container's
 * request-scoped holder. The identity is the {@code X-User-Id} header behind a trusted gateway.
 */
@Execution(ExecutionMode.SAME_THREAD)
class QuarkusRequestContextBindingTest {

  private RealArcTestContainer arc;
  private QuarkusRestTestServer server;
  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  @AfterEach
  void close() throws Exception {
    client.shutdownNow();
    if (server != null) {
      server.close();
    }
    if (arc != null) {
      arc.close();
    }
  }

  @Test
  void aResourceThatBindsNothingDispatchesAsTheRequestsCaller() throws Exception {
    boot();
    String id = UUID.randomUUID().toString();

    HttpResponse<String> answer = post("/binding/dispatch/" + id, "alice");

    assertThat(answer.statusCode()).as(answer.body()).isEqualTo(200);
    assertThat(answer.body())
        .as("precondition: the resource ran with CURRENT unbound inside an active request context")
        .startsWith("bound=false active=true ");
    assertThat(answer.body()).as("the query handler's caller").endsWith(" query=alice");
    assertThat(metadataUserIds(id)).as("the command's event").containsExactly("alice");
    assertThat(metadataUserIds(id + "-async"))
        .as("the event of the command sent through executeAsync")
        .containsExactly("alice");
    assertThat(auditUserIdsOf(id)).as("the command's audit row").containsExactly("alice");
    assertThat(auditUserIdsOf(id + "-async"))
        .as("the audit row of the command sent through executeAsync")
        .containsExactly("alice");
    assertThat(queryAuditUserIds()).as("the query's audit row").containsExactly("alice");
  }

  @Test
  void aSagaReplayFromAResourceThatBindsNothingRecordsTheOperator() throws Exception {
    boot();
    String flowId = faultedFlow();

    HttpResponse<String> answer = post("/binding/replay/" + flowId, "bob");

    assertThat(answer.statusCode()).as(answer.body()).isEqualTo(200);
    assertThat(answer.body()).as("the replay fed the faulted step").isEqualTo("[REPLAYED]");
    List<EventEnvelope> finished =
        stored().stream().filter(envelope -> envelope.event() instanceof Finished).toList();
    assertThat(finished).hasSize(1);
    var metadata = finished.getFirst().metadata();
    assertThat(metadata.userId()).as("the operator of the replay").isEqualTo(UserId.of("bob"));
    assertThat(metadata.sagaOrigin()).as("the saga's origin").isNotNull();
    assertThat(metadata.sagaOrigin().sagaId()).isEqualTo(SagaHarness.sagaIdOf(flowId));
    assertThat(auditUserIdsOf(flowId).getLast())
        .as("the audit row of the replayed command, the last one on the flow")
        .isEqualTo("bob");
  }

  @Test
  void aDispatchWithNoRequestStaysAnonymous() {
    boot();
    String id = UUID.randomUUID().toString();
    var bus = arc.container().instance(VirtualThreadCommandBus.class).get();

    bus.execute(new Touch(id));
    String queryUser = arc.container().instance(QueryBus.class).get().dispatch(new WhoAmI());

    assertThat(queryUser).isEmpty();
    assertThat(metadataUserIds(id)).hasSize(1).containsOnlyNulls();
    assertThat(auditUserIdsOf(id)).hasSize(1).containsOnlyNulls();
    assertThat(queryAuditUserIds()).hasSize(1).containsOnlyNulls();
  }

  // ---- the application --------------------------------------------------------------------------

  private void boot() {
    Map<String, String> properties =
        Map.of(RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY, "true");
    QuarkusIntegrationWiringContractTest.ApplicationInfrastructure.config =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withMapping(StreamRuneQuarkusCryptoProperties.class)
            .withConverter(Duration.class, 100, new DurationConverter())
            .withSources(new PropertiesConfigSource(properties, "application.properties", 250))
            .build();
    QuarkusIntegrationWiringContractTest.ApplicationInfrastructure.dataSource =
        new QuarkusIntegrationWiringContractTest.NoDatabase();
    List<Class<?>> beans = new ArrayList<>(QuarkusIntegrationWiringContractTest.FRAMEWORK_BEANS);
    beans.add(Readiness.class);
    beans.add(QuarkusIntegrationWiringContractTest.ApplicationInfrastructure.class);
    beans.add(Fixtures.class);
    try {
      arc =
          RealArcTestContainer.bootApplication(
              beans, properties, List.of(RecordingAuditStore.class), List.of());
      arc.container()
          .instance(QueryBus.class)
          .get()
          .register(
              WhoAmI.class,
              query -> {
                UserId user = Authorization.currentUserId();
                return user == null ? "" : user.value();
              });
      server =
          QuarkusRestTestServer.start(
              new BindingResource(
                  arc.container().instance(VirtualThreadCommandBus.class).get(),
                  arc.container().instance(QueryBus.class).get(),
                  arc.container().instance(SagaHarness.class).get()),
              List.of(
                  new StreamRuneRequestFilter(
                      arc.container().instance(StreamRuneRequestContextHolder.class).get(),
                      arc.container().instance(StreamRuneQuarkusProperties.class).get(),
                      arc.container().instance(RequestIdentityPolicy.class).get(),
                      arc.container().select(UserRoleResolver.class))),
              new ArcThreadSetupAction(arc.container().requestContext()));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private HttpResponse<String> post(String path, String caller) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
            .timeout(Duration.ofSeconds(30))
            .header(RequestIdentityPolicy.USER_ID_HEADER, caller)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  /** Opens a flow outside any request whose saga faults on its {@code Prepared}. */
  private String faultedFlow() {
    SagaHarness saga = arc.container().instance(SagaHarness.class).get();
    String flowId = "flow-" + UUID.randomUUID();
    saga.saga.poisonPreparedFor = flowId;
    arc.container().instance(VirtualThreadCommandBus.class).get().execute(new OpenFlow(flowId));
    saga.deliver();
    assertThat(saga.deadLetters.all()).as("precondition: the saga faulted").hasSize(1);
    return flowId;
  }

  private List<EventEnvelope> stored() {
    return arc.container()
        .instance(EventStore.class)
        .get()
        .readGlobalStream(GlobalOffset.initial(), 1000);
  }

  private List<String> metadataUserIds(String aggregateId) {
    return stored().stream()
        .filter(envelope -> envelope.aggregateId().value().equals(aggregateId))
        .map(envelope -> envelope.metadata().userId())
        .map(user -> user == null ? null : user.value())
        .toList();
  }

  /** The users of the audit rows of the commands on {@code aggregateId}, in the order audited. */
  private List<String> auditUserIdsOf(String aggregateId) {
    return arc.container().instance(RecordingAuditStore.class).get().entries().stream()
        .filter(
            entry -> entry.aggregateId() != null && entry.aggregateId().value().equals(aggregateId))
        .map(entry -> entry.userId() == null ? null : entry.userId().value())
        .toList();
  }

  private List<String> queryAuditUserIds() {
    return arc.container().instance(RecordingAuditStore.class).get().entries().stream()
        .filter(entry -> entry.commandType().contains(WhoAmI.class.getSimpleName()))
        .map(entry -> entry.userId() == null ? null : entry.userId().value())
        .toList();
  }

  /**
   * Dispatches a command, a command through {@code executeAsync} and an {@code @Auditable} query,
   * or replays a faulted saga, and binds nothing around any of them.
   */
  @Path("/binding")
  public static final class BindingResource {

    private final VirtualThreadCommandBus commandBus;
    private final QueryBus queryBus;
    private final SagaHarness saga;

    BindingResource(VirtualThreadCommandBus commandBus, QueryBus queryBus, SagaHarness saga) {
      this.commandBus = commandBus;
      this.queryBus = queryBus;
      this.saga = saga;
    }

    @POST
    @Path("/dispatch/{id}")
    public String dispatch(@PathParam("id") String id) {
      boolean bound = StreamRuneContext.CURRENT.isBound();
      boolean active = Arc.container().requestContext().isActive();
      commandBus.execute(new Touch(id));
      commandBus.executeAsync(new Touch(id + "-async")).join();
      String queryUser = queryBus.dispatch(new WhoAmI());
      return "bound=" + bound + " active=" + active + " query=" + queryUser;
    }

    @POST
    @Path("/replay/{flowId}")
    public String replay(@PathParam("flowId") String flowId) {
      saga.saga.poisonPreparedFor = null;
      return saga.replayer.replayAll(SagaHarness.sagaIdOf(flowId)).toString();
    }
  }

  /** The application's stores, its deciders and the saga a resource replays. */
  @ApplicationScoped
  public static class Fixtures {

    private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    private final InMemorySagaStore sagaStore = new InMemorySagaStore();

    @Produces
    @Singleton
    public EventStore eventStore() {
      return new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(sagaStore);
    }

    @Produces
    @Singleton
    public OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }

    @Produces
    @Singleton
    public CommandInbox commandInbox() {
      return inbox;
    }

    @Produces
    @Singleton
    public SagaHarness sagaHarness(VirtualThreadCommandBus commandBus, EventStore eventStore) {
      return new SagaHarness(commandBus, eventStore, sagaStore);
    }

    @Produces
    @Singleton
    public DeciderRegistration<Touch, Kept, Touched> touchRegistration() {
      return new DeciderRegistration<>(
          AggregateType.of("binding_probe"),
          Touch.class,
          command -> AggregateId.of(command.id()),
          new TouchDecider());
    }

    @Produces
    @Singleton
    public DeciderRegistration<FlowCommand, Kept, FlowEvent> flowRegistration() {
      return new DeciderRegistration<>(
          AggregateType.of("binding_flow"),
          FlowCommand.class,
          command -> AggregateId.of(command.flowId()),
          new FlowDecider());
    }
  }

  // ---- commands, a query and the saga -----------------------------------------------------------

  @Auditable
  public record WhoAmI() implements Query<String> {}

  public record Touch(String id) implements Command {}

  public record Touched() implements DomainEvent {}

  public record Kept() implements AggregateState {}

  static final class TouchDecider implements Decider<Touch, Kept, Touched> {
    @Override
    public Kept initialState() {
      return new Kept();
    }

    @Override
    public List<Touched> decide(Touch command, Kept state) {
      return List.of(new Touched());
    }

    @Override
    public Kept evolve(Kept state, Touched event) {
      return state;
    }
  }

  public sealed interface FlowCommand extends Command permits OpenFlow, Prepare, Finish {
    String flowId();
  }

  public record OpenFlow(String flowId) implements FlowCommand {}

  public record Prepare(String flowId) implements FlowCommand {}

  public record Finish(String flowId) implements FlowCommand {}

  public sealed interface FlowEvent extends DomainEvent permits Opened, Prepared, Finished {}

  public record Opened(String flowId) implements FlowEvent {}

  public record Prepared(String flowId) implements FlowEvent {}

  public record Finished(String flowId) implements FlowEvent {}

  static final class FlowDecider implements Decider<FlowCommand, Kept, FlowEvent> {
    @Override
    public Kept initialState() {
      return new Kept();
    }

    @Override
    public List<FlowEvent> decide(FlowCommand command, Kept state) {
      return switch (command) {
        case OpenFlow open -> List.of(new Opened(open.flowId()));
        case Prepare prepare -> List.of(new Prepared(prepare.flowId()));
        case Finish finish -> List.of(new Finished(finish.flowId()));
      };
    }

    @Override
    public Kept evolve(Kept state, FlowEvent event) {
      return state;
    }
  }

  public record FlowState(SagaStatus status, String flowId) implements SagaState {}

  /**
   * Opens on {@code Opened} and dispatches {@code Prepare}; on {@code Prepared} dispatches {@code
   * Finish}. Refuses the {@code Prepared} of {@link #poisonPreparedFor}, so that step faults into a
   * dead-letter entry a resource replays once the poison is lifted.
   */
  static final class FlowSaga implements SagaOrchestrator<FlowState> {
    volatile String poisonPreparedFor;

    @Override
    public Class<FlowState> stateType() {
      return FlowState.class;
    }

    @Override
    public FlowState initialState(SagaId sagaId) {
      return new FlowState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Opened;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaHarness.sagaIdOf(((Opened) event.event()).flowId());
    }

    @Override
    public FlowState evolve(FlowState state, EventEnvelope event) {
      return switch (event.event()) {
        case Opened opened -> new FlowState(SagaStatus.RUNNING, opened.flowId());
        case Prepared prepared -> {
          if (prepared.flowId().equals(poisonPreparedFor)) {
            throw new IllegalStateException("evolve refuses this event");
          }
          yield state;
        }
        case Finished _ -> new FlowState(SagaStatus.COMPLETED, state.flowId());
        default -> state;
      };
    }

    @Override
    public List<SagaCommand> handle(FlowState state, EventEnvelope event) {
      AggregateId flow = AggregateId.of(state.flowId());
      return switch (event.event()) {
        case Opened _ -> List.of(SagaCommand.of(new Prepare(state.flowId()), flow));
        case Prepared _ -> List.of(SagaCommand.of(new Finish(state.flowId()), flow));
        default -> List.of();
      };
    }

    @Override
    public List<SagaCommand> compensate(
        FlowState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  /**
   * The saga over the integration's command bus: a runner fed from the event store as a
   * subscription would, its dead-letter store and the replayer a resource calls.
   */
  public static final class SagaHarness {
    final FlowSaga saga = new FlowSaga();
    final InMemorySagaDeadLetterStore deadLetters;
    final SagaDeadLetterReplayer replayer;
    private final EventStore eventStore;
    private final SagaRunner<FlowState> runner;
    private final List<GlobalOffset> checkpoint = new CopyOnWriteArrayList<>();

    SagaHarness(
        VirtualThreadCommandBus commandBus, EventStore eventStore, InMemorySagaStore sagaStore) {
      this.eventStore = eventStore;
      this.deadLetters = new InMemorySagaDeadLetterStore(sagaStore);
      this.runner =
          SagaRunner.<FlowState>builder()
              .orchestrator(saga)
              .sagaStore(sagaStore)
              .commandBus(commandBus)
              .sagaDeadLetterStore(deadLetters)
              .build();
      this.replayer =
          SagaDeadLetterReplayer.builder()
              .sagaDeadLetterStore(deadLetters)
              .sagaStore(sagaStore)
              .eventStore(eventStore)
              .sagaRunner(runner)
              .build();
    }

    static SagaId sagaIdOf(String flowId) {
      return SagaId.of("binding-flow-saga-" + flowId);
    }

    /** Feeds the runner every event stored since the last call, until no new event appears. */
    synchronized void deliver() {
      while (true) {
        GlobalOffset after = checkpoint.isEmpty() ? GlobalOffset.initial() : checkpoint.getLast();
        List<EventEnvelope> batch = eventStore.readGlobalStream(after, 100);
        if (batch.isEmpty()) {
          return;
        }
        runner.asEventListener().onEvents(batch);
        checkpoint.add(batch.getLast().globalOffset());
      }
    }
  }
}
