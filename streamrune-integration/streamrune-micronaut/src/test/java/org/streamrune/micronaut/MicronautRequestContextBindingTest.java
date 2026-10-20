package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.HttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.filters.AuthenticationFetcher;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Authorization;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.RequireRole;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.Auditable;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.UserId;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStore;
import reactor.core.publisher.Mono;

/**
 * The caller of a request reaches the command bus, the query bus and {@link
 * StreamRuneContextHelper} on every thread Micronaut runs a route on, through a real embedded Netty
 * server and Micronaut Security.
 *
 * <p>{@link StreamRuneContextFilter} binds {@link StreamRuneContext#CURRENT} around the rest of the
 * filter chain, on its own thread. Micronaut does not always run the route inside that call: with
 * the default {@code streamed} server type a route whose body arrives after the request head runs
 * on a Netty event loop once the body is read, and a controller annotated {@code @ExecuteOn} runs
 * on that executor's thread under either server type. On those threads {@code CURRENT} is not
 * bound. Each case below first checks where the route ran (the precondition), then that every
 * dispatch ran as the authenticated caller:
 *
 * <ul>
 *   <li>the ungated command's events carry the user in their metadata, and its audit row names the
 *       user;
 *   <li>the {@code @RequireRole("ADMIN")} command is allowed for the ADMIN caller, and audited as
 *       that caller;
 *   <li>the command sent through {@code executeAsync} carries the user too;
 *   <li>the {@code @Auditable} query's handler sees the user, and its audit row names the user;
 *   <li>{@link StreamRuneContextHelper#get()} answers the request's context.
 * </ul>
 *
 * <p>An unauthenticated request on the same path stays anonymous, and a dispatch with no request at
 * all (a background thread) stays anonymous: nothing is invented.
 */
class MicronautRequestContextBindingTest {

  static final String SPEC = "MicronautRequestContextBindingTest";

  /** Long enough that the server has routed the request before the body arrives. */
  private static final long BODY_DELAY_MILLIS = 300;

  /** More than one read of the socket: the request head arrives without the whole body. */
  private static final int LARGE_BODY_BYTES = 1024 * 1024;

  // ---- the default server type (streamed) -------------------------------------------------------

  @Test
  void aBodyThatArrivesAfterTheHeadDoesNotLoseTheCaller() throws Exception {
    try (EmbeddedServer server = boot(null)) {
      var answer = postWithDelayedBody(server, "/binding/plain", "bob");

      assertThat(answer.status()).as(answer.body()).isEqualTo(200);
      assertDispatchedAs(server, "bob", false);
    }
  }

  @Test
  void aBodyThatArrivesAfterTheHeadDoesNotLoseTheCallerOfAnExecuteOnController() throws Exception {
    try (EmbeddedServer server = boot(null)) {
      var answer = postWithDelayedBody(server, "/binding/blocking", "bob");

      assertThat(answer.status()).as(answer.body()).isEqualTo(200);
      assertDispatchedAs(server, "bob", false);
    }
  }

  @Test
  void aLargeBodyDoesNotLoseTheCaller() {
    try (EmbeddedServer server = boot(null);
        HttpClient client = HttpClient.create(server.getURL())) {
      var request =
          HttpRequest.POST("/binding/plain", "x".repeat(LARGE_BODY_BYTES))
              .contentType(MediaType.TEXT_PLAIN_TYPE)
              .header(HeaderAuthenticationFixture.HEADER, "bob");

      assertThat(client.toBlocking().exchange(request, String.class).code()).isEqualTo(200);
      assertDispatchedAs(server, "bob", false);
    }
  }

  @Test
  void anUnauthenticatedRequestWhoseBodyArrivesAfterTheHeadStaysAnonymous() throws Exception {
    try (EmbeddedServer server = boot(null)) {
      var answer = postWithDelayedBody(server, "/binding/plain", null);

      assertThat(answer.status()).as(answer.body()).isEqualTo(200);
      assertDispatchedAs(server, null, false);
    }
  }

  // ---- full_content -----------------------------------------------------------------------------

  @Test
  void anExecuteOnControllerDoesNotLoseTheCallerUnderFullContent() {
    try (EmbeddedServer server = boot("full_content");
        HttpClient client = HttpClient.create(server.getURL())) {
      var request =
          HttpRequest.POST("/binding/blocking", "payload")
              .contentType(MediaType.TEXT_PLAIN_TYPE)
              .header(HeaderAuthenticationFixture.HEADER, "bob");

      assertThat(client.toBlocking().exchange(request, String.class).code()).isEqualTo(200);
      assertDispatchedAs(server, "bob", false);
    }
  }

  @Test
  void aPlainControllerUnderFullContentRunsInsideTheFiltersBinding() throws Exception {
    // The control row: the route runs inside the filter's binding, which the buses use as it is.
    try (EmbeddedServer server = boot("full_content")) {
      var answer = postWithDelayedBody(server, "/binding/plain", "bob");

      assertThat(answer.status()).as(answer.body()).isEqualTo(200);
      assertDispatchedAs(server, "bob", true);
    }
  }

  // ---- no request -------------------------------------------------------------------------------

  @Test
  void aDispatchWithNoRequestStaysAnonymous() {
    try (EmbeddedServer server = boot(null)) {
      ApplicationContext context = server.getApplicationContext();
      var bus = context.getBean(VirtualThreadCommandBus.class);
      String id = UUID.randomUUID().toString();
      String asyncId = UUID.randomUUID().toString();

      bus.execute(new ProbeCommand.Touch(id));
      bus.executeAsync(new ProbeCommand.Touch(asyncId)).join();
      String queryUser = context.getBean(QueryBus.class).dispatch(new WhoAmI());

      assertThat(StreamRuneContextHelper.get()).isNull();
      assertThat(queryUser).isEmpty();
      assertThat(metadataUserIds(context, id)).hasSize(1).containsOnlyNulls();
      assertThat(metadataUserIds(context, asyncId)).hasSize(1).containsOnlyNulls();
      assertThat(auditUserIds(context, id)).hasSize(1).containsOnlyNulls();
      assertThat(queryAuditUserIds(context)).hasSize(1).containsOnlyNulls();
    }
  }

  // ---- helpers ----------------------------------------------------------------------------------

  private static EmbeddedServer boot(String serverType) {
    Map<String, Object> props = new HashMap<>();
    props.put("spec.name", SPEC);
    props.put("micronaut.server.port", "-1");
    props.put("micronaut.security.enabled", "true");
    props.put(
        "micronaut.security.intercept-url-map",
        List.of(Map.of("pattern", "/**", "access", List.of("isAnonymous()"))));
    if (serverType != null) {
      props.put("micronaut.server.netty.server-type", serverType);
    }
    EmbeddedServer server = ApplicationContext.run(EmbeddedServer.class, props);
    server
        .getApplicationContext()
        .getBean(QueryBus.class)
        .register(
            WhoAmI.class,
            query -> {
              UserId user = Authorization.currentUserId();
              return user == null ? "" : user.value();
            });
    return server;
  }

  private static RawHttp.Answer postWithDelayedBody(
      EmbeddedServer server, String path, String authenticatedAs) throws Exception {
    List<String> headers =
        authenticatedAs == null
            ? List.of()
            : List.of(HeaderAuthenticationFixture.HEADER + ": " + authenticatedAs);
    return RawHttp.postWithDelayedBody(
        server.getPort(), path, headers, "payload", BODY_DELAY_MILLIS);
  }

  /**
   * Asserts that the one request the controller served dispatched everything as {@code user}
   * ({@code null}: anonymous), and that its route ran inside the filter's {@code ScopedValue}
   * binding exactly when {@code ranInsideTheFiltersBinding}.
   */
  private static void assertDispatchedAs(
      EmbeddedServer server, String user, boolean ranInsideTheFiltersBinding) {
    ApplicationContext context = server.getApplicationContext();
    List<Observation> observations = context.getBean(Observations.class).all;
    assertThat(observations).hasSize(1);
    Observation seen = observations.getFirst();
    assertThat(seen.scopeBound())
        .as(
            "precondition: the route ran %s the filter's ScopedValue binding (thread %s)",
            ranInsideTheFiltersBinding ? "inside" : "outside", seen.thread())
        .isEqualTo(ranInsideTheFiltersBinding);

    assertThat(seen.helperUser()).as("StreamRuneContextHelper.get()").isEqualTo(user);
    assertThat(seen.queryUser())
        .as("the query handler's caller")
        .isEqualTo(user == null ? "" : user);
    assertThat(seen.promote())
        .as("the @RequireRole(\"ADMIN\") command")
        .isEqualTo(user == null ? "denied" : "allowed");

    List<String> expectedCommandUsers = user == null ? nulls(1) : List.of(user, user);
    assertThat(metadataUserIds(context, seen.id()))
        .as("the events' metadata userId")
        .isEqualTo(expectedCommandUsers);
    assertThat(metadataUserIds(context, seen.asyncId()))
        .as("the metadata userId of the command sent through executeAsync")
        .isEqualTo(user == null ? nulls(1) : List.of(user));
    assertThat(auditUserIds(context, seen.id()))
        .as("the commands' audit rows (the denied one included)")
        .isEqualTo(user == null ? nulls(2) : List.of(user, user));
    assertThat(queryAuditUserIds(context))
        .as("the query's audit row")
        .isEqualTo(user == null ? nulls(1) : List.of(user));
  }

  private static List<String> nulls(int count) {
    return java.util.Collections.nCopies(count, (String) null);
  }

  private static List<String> metadataUserIds(ApplicationContext context, String aggregateId) {
    return context.getBean(EventStore.class).readGlobalStream(GlobalOffset.initial(), 1000).stream()
        .filter(envelope -> envelope.aggregateId().value().equals(aggregateId))
        .map(EventEnvelope::metadata)
        .map(metadata -> metadata.userId() == null ? null : metadata.userId().value())
        .toList();
  }

  private static List<String> auditUserIds(ApplicationContext context, String aggregateId) {
    return context.getBean(RecordingAuditStore.class).entries.stream()
        .filter(
            entry -> entry.aggregateId() != null && entry.aggregateId().value().equals(aggregateId))
        .map(entry -> entry.userId() == null ? null : entry.userId().value())
        .toList();
  }

  private static List<String> queryAuditUserIds(ApplicationContext context) {
    return context.getBean(RecordingAuditStore.class).entries.stream()
        .filter(entry -> entry.commandType().contains(WhoAmI.class.getSimpleName()))
        .map(entry -> entry.userId() == null ? null : entry.userId().value())
        .toList();
  }

  // ---- fixtures ---------------------------------------------------------------------------------

  /**
   * What the controller saw on the thread its route ran on.
   *
   * @param scopeBound whether {@link StreamRuneContext#CURRENT} was bound there
   * @param helperUser the user {@link StreamRuneContextHelper#get()} answered; {@code null} when
   *     none
   * @param queryUser the user the query handler saw; {@code ""} when none
   * @param promote {@code allowed} or {@code denied}: the {@code @RequireRole} command's outcome
   * @param id the aggregate the synchronous commands addressed
   * @param asyncId the aggregate the {@code executeAsync} command addressed
   * @param thread the name of the thread the route ran on
   */
  record Observation(
      boolean scopeBound,
      String helperUser,
      String queryUser,
      String promote,
      String id,
      String asyncId,
      String thread) {}

  @Singleton
  @Requires(property = "spec.name", value = SPEC)
  static class Observations {
    final List<Observation> all = new CopyOnWriteArrayList<>();
  }

  /**
   * Dispatches an ungated command, a {@code @RequireRole("ADMIN")} command, a command through
   * {@code executeAsync} and an {@code @Auditable} query, and records what it saw. The route takes
   * the body, so it cannot run before the body has arrived.
   */
  @Controller("/binding")
  @Requires(property = "spec.name", value = SPEC)
  static class BindingController {

    private final VirtualThreadCommandBus commandBus;
    private final QueryBus queryBus;
    private final Observations observations;

    BindingController(
        VirtualThreadCommandBus commandBus, QueryBus queryBus, Observations observations) {
      this.commandBus = commandBus;
      this.queryBus = queryBus;
      this.observations = observations;
    }

    @Post(value = "/plain", consumes = MediaType.TEXT_PLAIN)
    HttpResponse<String> plain(@Body String payload) {
      return dispatchAll(payload);
    }

    @Post(value = "/blocking", consumes = MediaType.TEXT_PLAIN)
    @ExecuteOn(TaskExecutors.BLOCKING)
    HttpResponse<String> blocking(@Body String payload) {
      return dispatchAll(payload);
    }

    private HttpResponse<String> dispatchAll(String payload) {
      boolean scopeBound = StreamRuneContext.CURRENT.isBound();
      StreamRuneContext.RequestContext helper = StreamRuneContextHelper.get();
      String id = UUID.randomUUID().toString();
      String asyncId = UUID.randomUUID().toString();
      commandBus.execute(new ProbeCommand.Touch(id));
      String promote;
      try {
        commandBus.execute(new ProbeCommand.Promote(id));
        promote = "allowed";
      } catch (AuthorizationException denied) {
        promote = "denied";
      }
      commandBus.executeAsync(new ProbeCommand.Touch(asyncId)).join();
      String queryUser = queryBus.dispatch(new WhoAmI());
      observations.all.add(
          new Observation(
              scopeBound,
              helper == null || helper.userId() == null ? null : helper.userId().value(),
              queryUser,
              promote,
              id,
              asyncId,
              Thread.currentThread().getName()));
      return HttpResponse.ok("read " + payload.length());
    }
  }

  @Factory
  @Requires(property = "spec.name", value = SPEC)
  static class Fixtures {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    DeciderRegistration<ProbeCommand, ProbeState, ProbeEvent> probeRegistration() {
      return new DeciderRegistration<>(
          AggregateType.of("binding_probe"),
          ProbeCommand.class,
          command ->
              switch (command) {
                case ProbeCommand.Touch touch -> AggregateId.of(touch.id());
                case ProbeCommand.Promote promote -> AggregateId.of(promote.id());
              },
          new ProbeDecider());
    }
  }

  /** The application's audit store: the module audits commands and queries into it. */
  @Singleton
  @Requires(property = "spec.name", value = SPEC)
  static class RecordingAuditStore implements AuditStore {
    final List<AuditEntry> entries = new CopyOnWriteArrayList<>();

    @Override
    public void save(AuditEntry entry) {
      entries.add(entry);
    }
  }

  /**
   * Real Micronaut Security authentication: a request carrying {@value #HEADER} is authenticated as
   * that name; {@code bob} has the role {@code ADMIN}, anyone else none.
   */
  @Singleton
  @Requires(property = "spec.name", value = SPEC)
  static class HeaderAuthenticationFixture implements AuthenticationFetcher<HttpRequest<?>> {
    static final String HEADER = "X-Test-Authenticated-As";

    @Override
    public Publisher<Authentication> fetchAuthentication(HttpRequest<?> request) {
      String name = request.getHeaders().get(HEADER);
      if (name == null) {
        return Mono.empty();
      }
      return Mono.just(
          Authentication.build(name, name.equals("bob") ? List.of("ADMIN") : List.of()));
    }
  }

  @Auditable
  record WhoAmI() implements Query<String> {}

  sealed interface ProbeCommand extends Command permits ProbeCommand.Touch, ProbeCommand.Promote {
    record Touch(String id) implements ProbeCommand {}

    @RequireRole("ADMIN")
    record Promote(String id) implements ProbeCommand {}
  }

  record ProbeState() implements AggregateState {}

  sealed interface ProbeEvent extends DomainEvent permits ProbeEvent.Touched {
    record Touched() implements ProbeEvent {}
  }

  static class ProbeDecider implements Decider<ProbeCommand, ProbeState, ProbeEvent> {
    @Override
    public ProbeState initialState() {
      return new ProbeState();
    }

    @Override
    public List<ProbeEvent> decide(ProbeCommand command, ProbeState state) {
      return List.of(new ProbeEvent.Touched());
    }

    @Override
    public ProbeState evolve(ProbeState state, ProbeEvent event) {
      return state;
    }
  }
}
