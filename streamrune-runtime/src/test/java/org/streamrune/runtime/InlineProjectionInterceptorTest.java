package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.runtime.WindowedProjectionTestSupport.evt;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class InlineProjectionInterceptorTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  record TestCommand() implements Command {}

  @Test
  void after_runsAllProjectionsInRegistrationOrder() {
    var calls = new ArrayList<String>();
    Projection a = batch -> calls.add("a:" + batch.size());
    Projection b = batch -> calls.add("b:" + batch.size());

    var interceptor =
        InlineProjectionInterceptor.builder().register("a", a).register("b", b).build();

    interceptor.after(ctxWith(List.of(evt(Instant.parse("2026-01-01T00:00:00Z")))));

    assertThat(calls).containsExactly("a:1", "b:1");
  }

  @Test
  void projectionThrows_remainingProjectionsStillRun_andFirstFailurePropagates() {
    var calls = new ArrayList<String>();
    Projection a =
        batch -> {
          throw new RuntimeException("boom-a");
        };
    Projection b = batch -> calls.add("b");

    var interceptor =
        InlineProjectionInterceptor.builder().register("a", a).register("b", b).build();

    assertThatThrownBy(
            () -> interceptor.after(ctxWith(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))))))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("boom-a");

    // One broken projection must not starve its siblings of the committed events.
    assertThat(calls).containsExactly("b");
  }

  @Test
  void multipleFailures_firstPropagatesWithLaterOnesSuppressed() {
    var calls = new ArrayList<String>();
    Projection a =
        batch -> {
          throw new RuntimeException("boom-a");
        };
    Projection b = batch -> calls.add("b");
    Projection c =
        batch -> {
          throw new IllegalStateException("boom-c");
        };

    var interceptor =
        InlineProjectionInterceptor.builder()
            .register("a", a)
            .register("b", b)
            .register("c", c)
            .build();

    assertThatThrownBy(
            () -> interceptor.after(ctxWith(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))))))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("boom-a")
        .satisfies(
            thrown -> {
              assertThat(thrown.getSuppressed()).hasSize(1);
              assertThat(thrown.getSuppressed()[0])
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessage("boom-c");
            });

    assertThat(calls).containsExactly("b");
  }

  @Test
  void emptyEvents_stillCallsProcess() {
    var calls = new ArrayList<Integer>();
    Projection p = batch -> calls.add(batch.size());

    var interceptor = InlineProjectionInterceptor.builder().register("p", p).build();

    interceptor.after(ctxWith(List.of()));

    assertThat(calls).containsExactly(0);
  }

  @Test
  void builder_noProjections_throws() {
    assertThatThrownBy(() -> InlineProjectionInterceptor.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one");
  }

  @Test
  void anonymousRegister_acceptsUnnamedProjection() {
    var calls = new ArrayList<Integer>();
    Projection p = batch -> calls.add(batch.size());
    var interceptor = InlineProjectionInterceptor.builder().register(p).build();

    interceptor.after(ctxWith(List.of(evt(Instant.parse("2026-01-01T00:00:00Z")))));

    assertThat(calls)
        .as("the anonymously registered projection must receive the committed events")
        .containsExactly(1);
  }

  @Test
  void shortCircuitedResult_skipsProjections() {
    var calls = new ArrayList<String>();
    Projection p = batch -> calls.add("p:" + batch.size());

    var interceptor = InlineProjectionInterceptor.builder().register("p", p).build();

    var shortCircuit =
        new CommandBus.CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("test")),
            Version.initial(),
            List.of(),
            List.of(),
            "RejectingInterceptor",
            CommandBus.ShortCircuitReason.VETOED);
    interceptor.after(
        new CommandContext(
            new TestCommand(),
            "TestCommand",
            new CommandId("c-1"),
            null,
            null,
            shortCircuit,
            Instant.now()));

    assertThat(calls).isEmpty();
  }

  @Test
  void nullResult_skipsProjections() {
    var calls = new ArrayList<String>();
    Projection p = batch -> calls.add("p:" + batch.size());

    var interceptor = InlineProjectionInterceptor.builder().register("p", p).build();

    interceptor.after(
        new CommandContext(
            new TestCommand(),
            "TestCommand",
            new CommandId("c-1"),
            null,
            null,
            null,
            Instant.now()));

    assertThat(calls).isEmpty();
  }

  private static CommandContext ctxWith(List<EventEnvelope> events) {
    var result =
        new CommandBus.CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("test")),
            Version.initial(),
            List.of(),
            events);
    return new CommandContext(
        new TestCommand(), "TestCommand", new CommandId("c-1"), null, null, result, Instant.now());
  }
}
