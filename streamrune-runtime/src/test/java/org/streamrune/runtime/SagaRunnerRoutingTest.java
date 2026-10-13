package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Which saga an event reaches. A start event names its saga through {@code extractSagaId}. Any
 * other event that carries a saga origin of the runner's own saga type reaches the saga that origin
 * names, and the application is not asked. Every other event is handed to {@code
 * correlateByEventData}, which sees the stream id and the payload and nothing of the metadata; its
 * default answer is "no saga of this type".
 *
 * <p>The correlation id takes no part: every envelope here carries, as its correlation id, the id
 * of a saga the event must not reach.
 */
class SagaRunnerRoutingTest {

  private static final AggregateType TYPE = AggregateType.of("routing");
  private static final StreamId STREAM = StreamId.of(TYPE, AggregateId.of("order-1"));
  private static final SagaType OWN_TYPE = SagaType.fromClass(RouteState.class);

  /** The saga a client names in its correlation id; no event of this class may reach it. */
  private static final SagaId NAMED_BY_CORRELATION_ID = SagaId.of("saga-victim");

  record Started(String id) implements DomainEvent {}

  record Replied(String id) implements DomainEvent {}

  record RouteState(SagaStatus status, int replies) implements SagaState {}

  /** A saga that overrides no routing hook for non-start events. */
  static class DefaultRoutingSaga implements SagaOrchestrator<RouteState> {
    @Override
    public Class<RouteState> stateType() {
      return RouteState.class;
    }

    @Override
    public RouteState initialState(SagaId sagaId) {
      return new RouteState(SagaStatus.STARTED, 0);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Started;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((Started) event.event()).id());
    }

    @Override
    public RouteState evolve(RouteState state, EventEnvelope event) {
      return event.event() instanceof Replied
          ? new RouteState(SagaStatus.RUNNING, state.replies() + 1)
          : new RouteState(SagaStatus.RUNNING, state.replies());
    }

    @Override
    public List<SagaCommand> handle(RouteState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        RouteState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  /** A saga whose event-data hook is the given function; every call is recorded. */
  static final class HookedSaga extends DefaultRoutingSaga {
    private final BiFunction<StreamId, DomainEvent, Optional<SagaId>> hook;
    final List<StreamId> streamsSeen = new ArrayList<>();
    final List<DomainEvent> eventsSeen = new ArrayList<>();

    HookedSaga(BiFunction<StreamId, DomainEvent, Optional<SagaId>> hook) {
      this.hook = hook;
    }

    @Override
    public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
      streamsSeen.add(streamId);
      eventsSeen.add(event);
      return hook.apply(streamId, event);
    }
  }

  /** The decider form of {@link DefaultRoutingSaga}. */
  static final class RouteDecider implements SagaDecider<RouteState> {
    @Override
    public Class<RouteState> stateType() {
      return RouteState.class;
    }

    @Override
    public RouteState initialState(SagaId sagaId) {
      return new RouteState(SagaStatus.STARTED, 0);
    }

    @Override
    public RouteState evolve(RouteState state, EventEnvelope event) {
      return event.event() instanceof Replied
          ? new RouteState(SagaStatus.RUNNING, state.replies() + 1)
          : new RouteState(SagaStatus.RUNNING, state.replies());
    }

    @Override
    public List<SagaCommand> handle(RouteState state, EventEnvelope event) {
      return List.of();
    }
  }

  static final class NoopBus implements CommandBus {
    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return ok();
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      return ok();
    }

    private static CommandResult ok() {
      return new CommandResult(List.of(), STREAM, Version.initial(), List.of());
    }
  }

  private ForwardingSagaStore sagaStore;
  private InMemorySagaDeadLetterStore deadLetters;

  @BeforeEach
  void setUp() {
    var inMemory = new InMemorySagaStore();
    sagaStore = new ForwardingSagaStore(inMemory);
    deadLetters = new InMemorySagaDeadLetterStore(inMemory);
  }

  private SagaRunner<RouteState> runner(SagaOrchestrator<RouteState> orchestrator) {
    return SagaRunner.<RouteState>builder()
        .orchestrator(orchestrator)
        .sagaStore(sagaStore)
        .commandBus(new NoopBus())
        .sagaDeadLetterStore(deadLetters)
        .build();
  }

  /** Metadata with the given origin and, as its correlation id, the saga no event may reach. */
  private static EventMetadata metadata(SagaOrigin origin) {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of(NAMED_BY_CORRELATION_ID.value()),
        null,
        null,
        Instant.now(),
        Map.of(),
        origin);
  }

  private static EventEnvelope envelope(long offset, DomainEvent event, SagaOrigin origin) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        STREAM,
        Version.initial(),
        EventType.fromClass(event.getClass()),
        event,
        metadata(origin));
  }

  private static SagaOrigin ownOrigin(String sagaId) {
    return SagaOrigin.dispatchedBy(OWN_TYPE, SagaId.of(sagaId));
  }

  private Optional<LoadedSaga<RouteState>> row(String sagaId) {
    return sagaStore.load(SagaId.of(sagaId), OWN_TYPE, RouteState.class);
  }

  private void start(SagaRunner<RouteState> runner, String id, long offset) {
    runner.asEventListener().onEvents(List.of(envelope(offset, new Started(id), null)));
  }

  @Test
  void aStartEventWithAnOrigin_takesTheStartPath() {
    var runner = runner(new DefaultRoutingSaga());
    var start = envelope(1L, new Started("a"), ownOrigin("saga-other"));

    assertThat(runner.resolveTarget(start))
        .contains(new SagaRunner.ReplayTarget(SagaId.of("saga-a"), true));

    runner.asEventListener().onEvents(List.of(start));
    assertThat(row("saga-a")).isPresent();
    assertThat(row("saga-other")).isEmpty();
  }

  @Test
  void anEventOfThisTypesOrigin_goesToTheSagaItNames_andTheHookIsNotCalled() {
    var saga =
        new HookedSaga(
            (streamId, event) -> {
              throw new IllegalStateException("the hook must not be asked");
            });
    var runner = runner(saga);
    start(runner, "a", 1L);
    var reply = envelope(2L, new Replied("a"), ownOrigin("saga-a"));

    assertThat(runner.resolveTarget(reply))
        .contains(new SagaRunner.ReplayTarget(SagaId.of("saga-a"), false));

    runner.asEventListener().onEvents(List.of(reply));
    assertThat(row("saga-a").orElseThrow().state().replies()).isEqualTo(1);
    assertThat(saga.eventsSeen).isEmpty();
    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void anEventOfAnotherTypesOrigin_goesToTheHook() {
    var saga = new HookedSaga((streamId, event) -> Optional.of(SagaId.of("saga-h")));
    var runner = runner(saga);
    var reply =
        envelope(
            2L,
            new Replied("a"),
            SagaOrigin.dispatchedBy(SagaType.of("other.State"), SagaId.of("saga-a")));

    assertThat(runner.resolveTarget(reply))
        .contains(new SagaRunner.ReplayTarget(SagaId.of("saga-h"), false));
    assertThat(saga.streamsSeen).containsExactly(STREAM);
    assertThat(saga.eventsSeen).containsExactly(new Replied("a"));
  }

  /** The comparison of saga types is exact: a type that merely begins alike is another type. */
  @Test
  void anOriginWhoseSagaTypeOnlyResemblesThisRunners_isAnotherTypesOrigin() {
    var runner = runner(new DefaultRoutingSaga());
    start(runner, "a", 1L);
    var before = row("saga-a").orElseThrow();
    String own = OWN_TYPE.value();
    var longer = SagaOrigin.dispatchedBy(SagaType.of(own + "2"), SagaId.of("saga-a"));
    var shorter =
        SagaOrigin.dispatchedBy(
            SagaType.of(own.substring(0, own.length() - 1)), SagaId.of("saga-a"));
    var otherCase = SagaOrigin.dispatchedBy(SagaType.of(own.toUpperCase()), SagaId.of("saga-a"));

    for (SagaOrigin origin : List.of(longer, shorter, otherCase)) {
      var reply = envelope(2L, new Replied("a"), origin);
      assertThat(runner.resolveTarget(reply)).as("origin %s", origin).isEmpty();
      runner.asEventListener().onEvents(List.of(reply));
    }
    assertThat(row("saga-a").orElseThrow()).isEqualTo(before);
  }

  @Test
  void anEventWithoutAnOrigin_andTheDefaultHook_belongsToNoSaga() {
    var runner = runner(new DefaultRoutingSaga());
    start(runner, "victim", 1L);
    var before = row("saga-victim").orElseThrow();
    int callsBefore = sagaStore.calls().size();
    var reply = envelope(2L, new Replied("victim"), null);

    assertThat(runner.resolveTarget(reply)).isEmpty();
    runner.asEventListener().onEvents(List.of(reply));

    assertThat(sagaStore.calls()).as("nothing loaded, nothing written").hasSize(callsBefore);
    assertThat(deadLetters.all()).isEmpty();
    assertThat(row("saga-victim").orElseThrow()).isEqualTo(before);
  }

  @Test
  void aHookThatThrows_isANullSagaQuarantine() {
    var saga =
        new HookedSaga(
            (streamId, event) -> {
              throw new IllegalStateException("hook broken");
            });
    var runner = runner(saga);
    start(runner, "a", 1L);
    var poison = envelope(2L, new Replied("x"), null);
    var ownReply = envelope(3L, new Replied("a"), ownOrigin("saga-a"));

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison, ownReply)))
        .doesNotThrowAnyException();

    assertThat(deadLetters.all()).hasSize(1);
    var entry = deadLetters.all().get(0);
    assertThat(entry.sagaId()).isNull();
    assertThat(entry.eventOffset()).isEqualTo(GlobalOffset.of(2L));
    assertThat(entry.errorType()).contains("IllegalStateException");
    assertThat(row("saga-a").orElseThrow().state().replies())
        .as("the batch continues with the next event")
        .isEqualTo(1);
  }

  @Test
  void aHookThatReturnsNull_isANullSagaQuarantine() {
    var runner = runner(new HookedSaga((streamId, event) -> null));
    start(runner, "a", 1L);
    var poison = envelope(2L, new Replied("x"), null);
    var ownReply = envelope(3L, new Replied("a"), ownOrigin("saga-a"));

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison, ownReply)))
        .doesNotThrowAnyException();

    assertThat(deadLetters.all()).hasSize(1);
    var entry = deadLetters.all().get(0);
    assertThat(entry.sagaId()).isNull();
    assertThat(entry.eventOffset()).isEqualTo(GlobalOffset.of(2L));
    assertThat(entry.errorType()).contains("NullPointerException");
    assertThat(entry.errorMessage()).contains("correlateByEventData returned null");
    assertThat(row("saga-a").orElseThrow().state().replies()).isEqualTo(1);
  }

  @Test
  void theBuilderNeedsNoEventDataRouter() {
    var runner =
        SagaRunner.<RouteState>builder()
            .decider(new RouteDecider())
            .startWhen(event -> event.event() instanceof Started)
            .extractSagaId(event -> SagaId.of("saga-" + ((Started) event.event()).id()))
            .sagaStore(sagaStore)
            .commandBus(new NoopBus())
            .sagaDeadLetterStore(deadLetters)
            .build();
    start(runner, "a", 1L);

    assertThat(runner.resolveTarget(envelope(2L, new Replied("a"), null))).isEmpty();
    assertThat(runner.resolveTarget(envelope(2L, new Replied("a"), ownOrigin("saga-a"))))
        .contains(new SagaRunner.ReplayTarget(SagaId.of("saga-a"), false));
  }

  @Test
  void theBuildersEventDataRouter_routesAnEventWithoutAnOrigin_byStreamAndPayload() {
    var streams = new ArrayList<StreamId>();
    var runner =
        SagaRunner.<RouteState>builder()
            .decider(new RouteDecider())
            .startWhen(event -> event.event() instanceof Started)
            .extractSagaId(event -> SagaId.of("saga-" + ((Started) event.event()).id()))
            .correlateByEventData(
                (streamId, event) -> {
                  streams.add(streamId);
                  return event instanceof Replied replied
                      ? Optional.of(SagaId.of("saga-" + replied.id()))
                      : Optional.empty();
                })
            .sagaStore(sagaStore)
            .commandBus(new NoopBus())
            .sagaDeadLetterStore(deadLetters)
            .build();

    assertThat(runner.resolveTarget(envelope(2L, new Replied("b"), null)))
        .contains(new SagaRunner.ReplayTarget(SagaId.of("saga-b"), false));
    assertThat(streams).containsExactly(STREAM);
  }
}
