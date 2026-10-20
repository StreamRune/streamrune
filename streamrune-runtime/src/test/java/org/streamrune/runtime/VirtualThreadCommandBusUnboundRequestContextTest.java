package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.UserId;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;

/**
 * {@link VirtualThreadCommandBus.Builder#unboundRequestContext}: where a command dispatched with
 * {@link StreamRuneContext#CURRENT} unbound takes its request context from. The source's answer is
 * bound for the whole dispatch — the interceptors and the events' metadata see it, and the
 * correlation id is its own — on every entry point; it is asked on the dispatching thread, never
 * while {@code CURRENT} is bound, and a {@code null} answer leaves the dispatch without a context.
 */
class VirtualThreadCommandBusUnboundRequestContextTest {

  private static final AggregateType TYPE = AggregateType.of("unbound_probe");

  private static final StreamRuneContext.RequestContext FROM_SOURCE =
      context("corr-source", "alice");

  record Touch(String id) implements Command {}

  record Touched() implements DomainEvent {}

  record Nothing() implements AggregateState {}

  static final class TouchDecider implements Decider<Touch, Nothing, Touched> {
    @Override
    public Nothing initialState() {
      return new Nothing();
    }

    @Override
    public List<Touched> decide(Touch command, Nothing state) {
      return List.of(new Touched());
    }

    @Override
    public Nothing evolve(Nothing state, Touched event) {
      return state;
    }
  }

  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final InMemoryEventStore store = new InMemoryEventStore().withCommandInbox(inbox);
  private final AtomicInteger asked = new AtomicInteger();
  private final List<Thread> askedOn = new CopyOnWriteArrayList<>();
  private final List<StreamRuneContext.RequestContext> seenByInterceptor =
      new CopyOnWriteArrayList<>();
  private final List<Boolean> sagaDispatchSeenByInterceptor = new CopyOnWriteArrayList<>();

  private VirtualThreadCommandBus bus(Supplier<StreamRuneContext.RequestContext> source) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .commandInbox(inbox)
        .register(TYPE, Touch.class, touch -> AggregateId.of(touch.id()), new TouchDecider())
        .interceptors(
            new CommandInterceptor() {
              @Override
              public boolean before(CommandContext ctx) {
                seenByInterceptor.add(StreamRuneContext.capture());
                sagaDispatchSeenByInterceptor.add(StreamRuneContext.isSagaDispatch());
                return true;
              }
            })
        .unboundRequestContext(source)
        .build();
  }

  private Supplier<StreamRuneContext.RequestContext> answering(
      StreamRuneContext.RequestContext answer) {
    return () -> {
      asked.incrementAndGet();
      askedOn.add(Thread.currentThread());
      return answer;
    };
  }

  @Test
  void anUnboundExecuteRunsUnderTheSourcesContext() {
    try (var bus = bus(answering(FROM_SOURCE))) {
      bus.execute(new Touch("t-1"));
    }

    assertThat(seenByInterceptor).containsExactly(FROM_SOURCE);
    EventMetadata metadata = onlyMetadata("t-1");
    assertThat(metadata.userId()).isEqualTo(UserId.of("alice"));
    assertThat(metadata.correlationId()).isEqualTo(CorrelationId.of("corr-source"));
    assertThat(asked).hasValue(1);
    assertThat(askedOn).containsExactly(Thread.currentThread());
    assertThat(StreamRuneContext.CURRENT.isBound()).isFalse();
  }

  @Test
  void anUnboundKeyedExecuteRunsUnderTheSourcesContext() {
    try (var bus = bus(answering(FROM_SOURCE))) {
      bus.execute(new Touch("t-2"), IdempotencyKey.of("key-2"));
    }

    assertThat(seenByInterceptor).containsExactly(FROM_SOURCE);
    assertThat(onlyMetadata("t-2").userId()).isEqualTo(UserId.of("alice"));
    assertThat(asked).hasValue(1);
  }

  @Test
  void anUnboundExecuteAsyncAsksOnTheSubmittingThreadAndRunsUnderTheAnswer() throws Exception {
    try (var bus = bus(answering(FROM_SOURCE))) {
      bus.executeAsync(new Touch("t-3")).get(5, TimeUnit.SECONDS);
    }

    assertThat(askedOn).containsExactly(Thread.currentThread());
    assertThat(seenByInterceptor).containsExactly(FROM_SOURCE);
    assertThat(onlyMetadata("t-3").userId()).isEqualTo(UserId.of("alice"));
  }

  @Test
  void aBoundContextIsNeverReplacedAndTheSourceIsNotAsked() throws Exception {
    var bound = context("corr-bound", "bob");
    try (var bus = bus(answering(FROM_SOURCE))) {
      ScopedValue.where(StreamRuneContext.CURRENT, bound)
          .call(
              () -> {
                bus.execute(new Touch("t-4"));
                bus.execute(new Touch("t-5"), IdempotencyKey.of("key-5"));
                return bus.executeAsync(new Touch("t-6")).get(5, TimeUnit.SECONDS);
              });
    }

    assertThat(asked).hasValue(0);
    assertThat(seenByInterceptor).containsExactly(bound, bound, bound);
    assertThat(onlyMetadata("t-4").userId()).isEqualTo(UserId.of("bob"));
  }

  @Test
  void aContextBoundToNullIsNoContextAndTheSourceIsNotAsked() throws Exception {
    try (var bus = bus(answering(FROM_SOURCE))) {
      ScopedValue.where(StreamRuneContext.CURRENT, null)
          .call(
              () -> {
                bus.execute(new Touch("t-n1"));
                return bus.executeAsync(new Touch("t-n2")).get(5, TimeUnit.SECONDS);
              });
    }

    assertThat(asked).hasValue(0);
    for (String id : List.of("t-n1", "t-n2")) {
      EventMetadata metadata = onlyMetadata(id);
      assertThat(metadata.userId()).isNull();
      assertThat(metadata.correlationId()).as("generated by the bus").isNotNull();
    }
  }

  // ---- saga dispatch: the operator of a replay run from a request ----

  private static final SagaOrigin ORIGIN =
      new SagaOrigin(SagaType.of("com.shop.OrderSaga$State"), SagaId.of("saga-1"), 0);

  @Test
  void aSagaDispatchOnAnUnboundThreadTakesItsAmbientUserFromTheSource() {
    try (var bus = bus(answering(FROM_SOURCE))) {
      SagaCommandDispatch.dispatch(
          bus,
          ORIGIN,
          EventId.of("evt-1"),
          new Touch("t-s1"),
          IdempotencyKey.of("saga:1:0"),
          false);
    }

    EventMetadata metadata = onlyMetadata("t-s1");
    assertThat(metadata.userId()).as("the operator").isEqualTo(UserId.of("alice"));
    assertThat(metadata.correlationId())
        .as("the saga's own, not the request's")
        .isEqualTo(ORIGIN.correlationId());
    assertThat(metadata.sagaOrigin()).isEqualTo(ORIGIN);
    assertThat(sagaDispatchSeenByInterceptor).as("still a saga dispatch").containsExactly(true);
    assertThat(asked).hasValue(1);
  }

  @Test
  void aSagaDispatchUnderABoundContextKeepsItAndDoesNotAskTheSource() throws Exception {
    var bound = context("corr-bound", "bob");
    try (var bus = bus(answering(FROM_SOURCE))) {
      ScopedValue.where(StreamRuneContext.CURRENT, bound)
          .call(
              () ->
                  SagaCommandDispatch.dispatch(
                      bus,
                      ORIGIN,
                      EventId.of("evt-2"),
                      new Touch("t-s2"),
                      IdempotencyKey.of("saga:1:1"),
                      false));
    }

    assertThat(onlyMetadata("t-s2").userId()).isEqualTo(UserId.of("bob"));
    assertThat(asked).hasValue(0);
  }

  @Test
  void aSagaDispatchWithNoRequestStaysWithoutAUser() {
    try (var bus = bus(answering(null))) {
      SagaCommandDispatch.dispatch(
          bus,
          ORIGIN,
          EventId.of("evt-3"),
          new Touch("t-s3"),
          IdempotencyKey.of("saga:1:2"),
          false);
    }

    EventMetadata metadata = onlyMetadata("t-s3");
    assertThat(metadata.userId()).isNull();
    assertThat(metadata.correlationId()).isEqualTo(ORIGIN.correlationId());
    assertThat(sagaDispatchSeenByInterceptor).containsExactly(true);
  }

  @Test
  void aNullAnswerLeavesTheDispatchWithoutAContext() throws Exception {
    try (var bus = bus(answering(null))) {
      bus.execute(new Touch("t-9"));
      bus.executeAsync(new Touch("t-10")).get(5, TimeUnit.SECONDS);
    }

    assertThat(asked).hasValue(2);
    assertThat(seenByInterceptor).containsOnlyNulls().hasSize(2);
    EventMetadata metadata = onlyMetadata("t-9");
    assertThat(metadata.userId()).isNull();
    assertThat(metadata.correlationId()).as("generated by the bus").isNotNull();
  }

  @Test
  void withoutASourceAnUnboundDispatchRunsWithoutAContext() {
    try (var bus = bus(null)) {
      bus.execute(new Touch("t-11"));
    }

    assertThat(seenByInterceptor).containsOnlyNulls().hasSize(1);
    assertThat(onlyMetadata("t-11").userId()).isNull();
  }

  @Test
  void aSourceThatThrowsFailsTheDispatchBeforeAnyInterceptor() {
    var failure = new IllegalStateException("no request");
    try (var bus =
        bus(
            () -> {
              throw failure;
            })) {
      assertThatThrownBy(() -> bus.execute(new Touch("t-12"))).isSameAs(failure);
      assertThatThrownBy(() -> bus.executeAsync(new Touch("t-13")).join())
          .isInstanceOf(CompletionException.class)
          .hasCause(failure);
    }

    assertThat(seenByInterceptor).isEmpty();
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 100)).isEmpty();
  }

  @Test
  void theStreamRuneFacadeHandsTheSourceToItsBus() {
    var seen = new AtomicReference<UserId>();
    try (var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .register(TYPE, Touch.class, touch -> AggregateId.of(touch.id()), new TouchDecider())
            .unboundRequestContext(answering(FROM_SOURCE))
            .build()) {
      streamRune.execute(new Touch("t-14"));
      seen.set(onlyMetadata("t-14").userId());
    }

    assertThat(seen.get()).isEqualTo(UserId.of("alice"));
    assertThat(asked).hasValue(1);
  }

  private EventMetadata onlyMetadata(String aggregateId) {
    var envelopes =
        store.readGlobalStream(GlobalOffset.initial(), 100).stream()
            .filter(envelope -> envelope.aggregateId().value().equals(aggregateId))
            .toList();
    assertThat(envelopes).hasSize(1);
    return envelopes.getFirst().metadata();
  }

  private static StreamRuneContext.RequestContext context(String correlationId, String userId) {
    return new StreamRuneContext.RequestContext(
        null, UserId.of(userId), CorrelationId.of(correlationId), Instant.now(), Map.of());
  }
}
