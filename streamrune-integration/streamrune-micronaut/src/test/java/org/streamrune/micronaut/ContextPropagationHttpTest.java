package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStore;

/**
 * End-to-end HTTP tests for {@link StreamRuneContextFilter}: requests without StreamRune headers
 * must not fail, and header-derived context must reach command handlers — observable in the event
 * metadata written by the command bus. The application runs in the trusted-gateway mode ({@code
 * streamrune.security.trust-user-id-header=true}), the only mode in which {@code X-User-Id} is the
 * user; the other modes are pinned by {@link MicronautRequestIdentityFailClosedTest}.
 */
@MicronautTest
@Property(name = "spec.name", value = "ContextPropagationHttpTest")
@Property(name = "micronaut.security.enabled", value = "false")
@Property(name = "streamrune.security.trust-user-id-header", value = "true")
class ContextPropagationHttpTest {

  private static final Argument<Map<String, String>> MAP =
      Argument.mapOf(String.class, String.class);

  @Inject
  @Client("/")
  HttpClient client;

  @Test
  void requestWithoutHeadersSucceedsWithDefaults() {
    var response = client.toBlocking().exchange(HttpRequest.GET("/context-probe"), MAP);

    assertEquals(200, response.code(), "missing headers must not cause a 500");
    Map<String, String> body = response.body();
    assertNotNull(body);
    assertEquals("true", body.get("hasContext"), "request context must be available");
    assertFalse(body.get("traceId").isBlank(), "missing X-Trace-Id must be generated");
    assertEquals("", body.get("userId"), "missing X-User-Id must mean anonymous");
    assertFalse(body.get("correlationId").isBlank());
  }

  @Test
  void headersArePropagatedIntoRequestContext() {
    var request =
        HttpRequest.GET("/context-probe")
            .header("X-Trace-Id", "trace-42")
            .header("X-User-Id", "user-42")
            .header("X-Correlation-Id", "corr-42")
            .header("X-User-Role", "ADMIN");

    var response = client.toBlocking().exchange(request, MAP);

    assertEquals(200, response.code());
    Map<String, String> body = response.body();
    assertNotNull(body);
    assertEquals("trace-42", body.get("traceId"));
    assertEquals("user-42", body.get("userId"));
    assertEquals("corr-42", body.get("correlationId"));
    assertEquals("ADMIN", body.get("role"));
  }

  @Test
  void contextReachesCommandHandlerMetadata() {
    var request =
        HttpRequest.GET("/context-probe/command")
            .header("X-Trace-Id", "trace-cmd")
            .header("X-User-Id", "user-cmd")
            .header("X-Correlation-Id", "corr-cmd");

    io.micronaut.http.HttpResponse<Map<String, String>> response;
    try {
      response = client.toBlocking().exchange(request, MAP);
    } catch (io.micronaut.http.client.exceptions.HttpClientResponseException e) {
      throw new AssertionError(
          "server error: " + e.getResponse().getBody(String.class).orElse("<no body>"), e);
    }

    assertEquals(200, response.code());
    Map<String, String> body = response.body();
    assertNotNull(body);
    assertEquals(
        "user-cmd",
        body.get("metaUserId"),
        "X-User-Id must reach the event metadata written by the command bus");
    assertEquals("trace-cmd", body.get("metaTraceId"));
    assertEquals("corr-cmd", body.get("metaCorrelationId"));
  }

  // === test fixtures ===

  @Controller("/context-probe")
  @Requires(property = "spec.name", value = "ContextPropagationHttpTest")
  static class ProbeController {

    private final VirtualThreadCommandBus commandBus;
    private final EventStore eventStore;

    ProbeController(VirtualThreadCommandBus commandBus, EventStore eventStore) {
      this.commandBus = commandBus;
      this.eventStore = eventStore;
    }

    @Get
    Map<String, String> probe() {
      var scoped = StreamRuneContext.CURRENT.isBound() ? StreamRuneContext.CURRENT.get() : null;
      var ctx = scoped != null ? scoped : StreamRuneContextHelper.get();
      Map<String, String> result = new HashMap<>();
      result.put("hasContext", String.valueOf(ctx != null));
      if (ctx != null) {
        result.put("traceId", ctx.traceId() != null ? ctx.traceId().value() : "");
        result.put("userId", ctx.userId() != null ? ctx.userId().value() : "");
        result.put("correlationId", ctx.correlationId().value());
        result.put("role", ctx.baggage().getOrDefault("role", ""));
      }
      return result;
    }

    @Get("/command")
    Map<String, String> command() {
      commandBus.execute(new ProbeCommand.Touch(UUID.randomUUID().toString()));
      var events = eventStore.readGlobalStream(GlobalOffset.initial(), 1000);
      var metadata = events.getLast().metadata();
      Map<String, String> result = new HashMap<>();
      result.put("metaUserId", metadata.userId() != null ? metadata.userId().value() : "");
      result.put("metaTraceId", metadata.traceId() != null ? metadata.traceId().value() : "");
      result.put(
          "metaCorrelationId",
          metadata.correlationId() != null ? metadata.correlationId().value() : "");
      return result;
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "ContextPropagationHttpTest")
  static class Fixtures {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    DeciderRegistration<ProbeCommand, ProbeState, ProbeEvent> probeRegistration() {
      return new DeciderRegistration<>(
          AggregateType.of("probe"),
          ProbeCommand.class,
          cmd ->
              switch (cmd) {
                case ProbeCommand.Touch t -> AggregateId.of(t.id());
              },
          new ProbeDecider());
    }
  }

  sealed interface ProbeCommand extends org.streamrune.core.Command permits ProbeCommand.Touch {
    record Touch(String id) implements ProbeCommand {}
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
