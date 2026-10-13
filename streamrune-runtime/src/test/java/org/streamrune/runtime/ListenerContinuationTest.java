package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Audit;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Audited;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Opened;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Reserved;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryEventSubscription;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.SagaEnvelopes;

/**
 * A listener that reacts to a stored event by dispatching a command inside {@link
 * StreamRuneContext#continuing(EventEnvelope)} — a process manager — on the production command bus.
 *
 * <p>The first half enumerates the crash points of one hop of such a listener and what a rerun
 * leaves behind:
 *
 * <ol>
 *   <li>after the event is read and before the command commits: nothing is stored, and the
 *       redelivery binds the same cause, because the cause is a function of the stored event;
 *   <li>the dispatch fails: nothing is dead-lettered for an event a saga caused, the failure leaves
 *       the listener, and the subscription delivers the event again;
 *   <li>after the command and its idempotency key commit and before the subscription's checkpoint:
 *       the redelivery is answered from the command inbox and stores no second event;
 *   <li>a dispatch without a key is executed again on redelivery, each time with the same saga
 *       origin and causation id;
 *   <li>a listener that swallows the failure loses the command: the checkpoint moves on and no
 *       dead-letter entry exists.
 * </ol>
 *
 * <p>Each runs on both subscriptions a listener can be fed by here: {@link
 * InMemoryEventSubscription}, and {@link PollingEventSubscription} over an {@link
 * InMemoryOffsetStore}, driven one poll cycle at a time.
 *
 * <p>The second half lets a real saga dispatch the command whose reply the listener continues.
 */
class ListenerContinuationTest {

  // ==== The listener's hop and its crash points ====

  /** The command the listener dispatches. */
  public record Capture(String paymentId) implements Command {}

  /** The event of {@link Capture}. */
  public record Captured(String paymentId) implements DomainEvent {}

  /** The event the listener reacts to. */
  public record PaymentInitiated(String paymentId) implements DomainEvent {}

  private record Payment(int captures) implements AggregateState {}

  private record FulfillmentState(SagaStatus status) implements SagaState {}

  private static final AggregateType PAYMENT = AggregateType.of("payment");
  private static final AggregateType ORDER = AggregateType.of("order");
  private static final SagaId SAGA_ID = SagaId.of("order-saga-42");
  private static final SagaOrigin ONE_HOP_FROM_THE_SAGA =
      new SagaOrigin(SagaType.fromClass(FulfillmentState.class), SAGA_ID, 1);
  private static final String SUBSCRIPTION = "payment-process-manager";

  /** The production bus with an inbox and a dead letter queue, over the in-memory event store. */
  private static final class Harness implements AutoCloseable {
    final ObjectMapper mapper = new ObjectMapper();
    final AtomicInteger failuresLeft = new AtomicInteger();
    final AtomicBoolean rejecting = new AtomicBoolean();
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    final InMemoryDeadLetterQueue dlq = new InMemoryDeadLetterQueue();
    final VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(mapper)
            .commandInbox(inbox)
            .deadLetterQueue(dlq)
            // The default dead-letter rule: the decider's infrastructure fault is eligible, its
            // domain rejection is not.
            .register(
                PAYMENT,
                Capture.class,
                capture -> AggregateId.of(capture.paymentId()),
                new Decider<Capture, Payment, Captured>() {
                  @Override
                  public Payment initialState() {
                    return new Payment(0);
                  }

                  @Override
                  public List<Captured> decide(Capture command, Payment state) {
                    if (rejecting.get()) {
                      throw new DomainException("the payment is already refunded");
                    }
                    if (failuresLeft.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                      throw new IllegalStateException("payment provider unreachable");
                    }
                    return List.of(new Captured(command.paymentId()));
                  }

                  @Override
                  public Payment evolve(Payment state, Captured event) {
                    return new Payment(state.captures() + 1);
                  }
                })
            .build();

    /** Stores the reply to a saga's own command: origin at hop 0. */
    EventEnvelope appendSagaCaused() {
      StreamId stream = StreamId.of(ORDER, AggregateId.of("o-1"));
      return append(
          SagaEnvelopes.causedBy(
              FulfillmentState.class, SAGA_ID, stream, new PaymentInitiated("pay-1")));
    }

    /** Stores an event a user's request caused: a user, a trace, a client's correlation id. */
    EventEnvelope appendRequestCaused() {
      StreamId stream = StreamId.of(ORDER, AggregateId.of("o-1"));
      return append(
          new EventEnvelope(
              GlobalOffset.initial(),
              stream,
              Version.initial(),
              EventType.fromClass(PaymentInitiated.class),
              new PaymentInitiated("pay-1"),
              new EventMetadata(
                  EventId.of("evt-request-1"),
                  CommandId.of("cmd-request-1"),
                  TraceId.of("trace-7"),
                  null,
                  CorrelationId.of("client-corr-7"),
                  null,
                  UserId.of("alice"),
                  Instant.now(),
                  Map.of("role", "ADMIN"))));
    }

    private EventEnvelope append(EventEnvelope envelope) {
      var appended = eventStore.append(envelope.streamId(), List.of(envelope), Version.initial());
      return eventStore
          .readGlobalStream(GlobalOffset.of(appended.globalOffsets().get(0).value() - 1), 1)
          .get(0);
    }

    List<EventEnvelope> captured() {
      return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
          .filter(e -> e.event() instanceof Captured)
          .toList();
    }

    @Override
    public void close() {
      bus.close();
    }
  }

  /**
   * The listener under test: on {@link PaymentInitiated} it dispatches {@link Capture} inside the
   * continuation of that event. The hooks place a crash before the dispatch or after it.
   */
  private static final class ProcessManager implements EventListener {
    final Harness harness;
    final List<StreamRuneContext.DispatchCause> boundCauses = new CopyOnWriteArrayList<>();
    final AtomicInteger deliveries = new AtomicInteger();
    final AtomicInteger swallowed = new AtomicInteger();
    final AtomicInteger rejections = new AtomicInteger();
    IntPredicate crashBeforeDispatch = delivery -> false;
    IntPredicate crashAfterDispatch = delivery -> false;
    boolean keyed = true;
    boolean swallowFailures;
    boolean takeRejectionsAsTheAnswer;

    ProcessManager(Harness harness) {
      this.harness = harness;
    }

    @Override
    public void onEvents(List<EventEnvelope> batch) {
      for (EventEnvelope event : batch) {
        if (!(event.event() instanceof PaymentInitiated initiated)) {
          continue;
        }
        int delivery = deliveries.incrementAndGet();
        if (crashBeforeDispatch.test(delivery)) {
          throw new IllegalStateException("crashed before the dispatch");
        }
        if (swallowFailures) {
          try {
            dispatch(event, initiated);
          } catch (RuntimeException lost) {
            swallowed.incrementAndGet();
          }
        } else if (takeRejectionsAsTheAnswer) {
          try {
            dispatch(event, initiated);
          } catch (DomainException answer) {
            rejections.incrementAndGet();
          }
        } else {
          dispatch(event, initiated);
        }
        if (crashAfterDispatch.test(delivery)) {
          throw new IllegalStateException("crashed before the checkpoint");
        }
      }
    }

    private void dispatch(EventEnvelope event, PaymentInitiated initiated) {
      StreamRuneContext.continuing(event)
          .run(
              () -> {
                boundCauses.add(StreamRuneContext.cause());
                Capture command = new Capture(initiated.paymentId());
                if (keyed) {
                  harness.bus.execute(command, keyOf(event));
                } else {
                  harness.bus.execute(command);
                }
              });
    }
  }

  /** The key a listener derives from the event it reacts to. */
  private static IdempotencyKey keyOf(EventEnvelope event) {
    return IdempotencyKey.of("capture:" + event.metadata().eventId().value());
  }

  /** A subscription driven one delivery cycle at a time. */
  private interface Feed extends AutoCloseable {

    /** Runs one delivery cycle; a listener failure ends the cycle and leaves the checkpoint. */
    void cycle();

    /** The offset the subscription has acknowledged. */
    long checkpoint();

    @Override
    void close();
  }

  private static Feed inMemoryFeed(Harness harness, EventListener listener) {
    InMemoryEventSubscription subscription =
        new InMemoryEventSubscription(harness.eventStore, listener);
    subscription.start();
    // Appends wake nobody: the listener runs when the scenario polls, one cycle at a time.
    subscription.dropNotifications(true);
    return new Feed() {
      @Override
      public void cycle() {
        subscription.poll();
      }

      @Override
      public long checkpoint() {
        return subscription.lastDeliveredOffset();
      }

      @Override
      public void close() {
        subscription.close();
      }
    };
  }

  private static Feed pollingFeed(Harness harness, EventListener listener) {
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    PollingEventSubscription subscription =
        PollingEventSubscription.builder()
            .subscriptionName(SUBSCRIPTION)
            .eventStore(harness.eventStore)
            .offsetStore(offsets)
            .listener(listener)
            .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ZERO))
            .build();
    return new Feed() {
      @Override
      public void cycle() {
        try {
          subscription.pollOnce();
        } catch (RuntimeException listenerFailure) {
          // The poll loop logs the failure, backs off and polls again; the checkpoint stays.
        }
      }

      @Override
      public long checkpoint() {
        return offsets.getLastOffset(ProjectionName.of(SUBSCRIPTION)).value();
      }

      @Override
      public void close() {
        subscription.close();
      }
    };
  }

  /** One scenario: the store and bus, the listener, and the feed that delivers to it. */
  private interface Scenario {
    void run(Harness harness, ProcessManager listener, Feed feed);
  }

  private interface FeedFactory {
    Feed open(Harness harness, EventListener listener);
  }

  /** Runs {@code scenario} once per subscription, each on a fresh harness and listener. */
  private static Stream<DynamicTest> onBothSubscriptions(Scenario scenario) {
    Map<String, FeedFactory> feeds =
        Map.of(
            "in-memory subscription", ListenerContinuationTest::inMemoryFeed,
            "polling subscription", ListenerContinuationTest::pollingFeed);
    return feeds.entrySet().stream()
        .map(
            entry ->
                dynamicTest(
                    entry.getKey(),
                    () -> {
                      try (Harness harness = new Harness()) {
                        ProcessManager listener = new ProcessManager(harness);
                        try (Feed feed = entry.getValue().open(harness, listener)) {
                          scenario.run(harness, listener, feed);
                        }
                      }
                    }));
  }

  private static StreamRuneContext.DispatchCause causeOf(EventEnvelope event) {
    SagaOrigin origin = event.metadata().sagaOrigin();
    return new StreamRuneContext.DispatchCause(
        CausationId.of(event.metadata().eventId().value()),
        origin == null
            ? null
            : new SagaOrigin(origin.sagaType(), origin.sagaId(), origin.hop() + 1));
  }

  /** {@code captured} is the listener's event for the saga-caused {@code initiated}. */
  private static void assertContinuesTheSagasReply(
      EventEnvelope initiated, EventEnvelope captured) {
    EventMetadata metadata = captured.metadata();
    assertThat(metadata.sagaOrigin()).isEqualTo(ONE_HOP_FROM_THE_SAGA);
    assertThat(metadata.causationId())
        .isEqualTo(CausationId.of(initiated.metadata().eventId().value()));
    assertThat(metadata.correlationId()).isEqualTo(initiated.metadata().correlationId());
    assertThat(metadata.userId()).isNull();
    assertThat(metadata.baggage()).isEmpty();
  }

  @TestFactory
  Stream<DynamicTest> theHop_storesTheOriginOneHopFurtherAndTheEventsIdAsCausation() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope initiated = harness.appendSagaCaused();

          feed.cycle();

          assertThat(harness.captured())
              .singleElement()
              .satisfies(captured -> assertContinuesTheSagasReply(initiated, captured));
          assertThat(feed.checkpoint())
              .as("the cycle drained to the head: the listener's own event is acknowledged too")
              .isEqualTo(harness.captured().getFirst().globalOffset().value());
          assertThat(harness.dlq.all()).isEmpty();
        });
  }

  @TestFactory
  Stream<DynamicTest> crashAfterReadingTheEvent_beforeTheCommandCommits() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope initiated = harness.appendSagaCaused();
          listener.crashBeforeDispatch = delivery -> delivery == 1;

          feed.cycle();

          assertThat(harness.captured()).as("nothing is stored").isEmpty();
          assertThat(harness.dlq.all()).isEmpty();
          assertThat(feed.checkpoint()).as("the checkpoint stays").isZero();
          assertThat(listener.boundCauses).isEmpty();

          feed.cycle();

          assertThat(harness.captured())
              .singleElement()
              .satisfies(captured -> assertContinuesTheSagasReply(initiated, captured));
          assertThat(listener.boundCauses)
              .as("the cause is a function of the stored event")
              .containsExactly(causeOf(initiated));
          assertThat(listener.deliveries).hasValue(2);

          feed.cycle();
          assertThat(listener.deliveries).as("acknowledged: not delivered again").hasValue(2);
        });
  }

  @TestFactory
  Stream<DynamicTest> theDispatchFails_nothingIsDeadLettered_andTheRedeliveryStoresTheEvent() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope initiated = harness.appendSagaCaused();
          harness.failuresLeft.set(1);

          feed.cycle();

          assertThat(harness.captured()).isEmpty();
          assertThat(harness.dlq.all())
              .as("a command that carries a saga origin is not dead-lettered")
              .isEmpty();
          assertThat(feed.checkpoint()).as("the failure left the listener").isZero();

          feed.cycle();

          assertThat(harness.captured())
              .singleElement()
              .satisfies(captured -> assertContinuesTheSagasReply(initiated, captured));
          assertThat(harness.dlq.all()).isEmpty();
          assertThat(listener.boundCauses)
              .as("both attempts bound the same cause")
              .containsExactly(causeOf(initiated), causeOf(initiated));
        });
  }

  @TestFactory
  Stream<DynamicTest> crashAfterTheCommandCommits_beforeTheCheckpoint() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope initiated = harness.appendSagaCaused();
          listener.crashAfterDispatch = delivery -> delivery == 1;

          feed.cycle();

          assertThat(harness.captured()).hasSize(1);
          EventEnvelope committed = harness.captured().getFirst();
          assertThat(harness.inbox.find(keyOf(initiated))).isPresent();
          assertThat(feed.checkpoint()).as("the checkpoint stays").isZero();

          feed.cycle();

          assertThat(harness.captured())
              .as("the redelivery is answered from the command inbox")
              .containsExactly(committed);
          assertContinuesTheSagasReply(initiated, committed);
          assertThat(listener.deliveries).hasValue(2);
          assertThat(feed.checkpoint()).isEqualTo(committed.globalOffset().value());
        });
  }

  @TestFactory
  Stream<DynamicTest> aDispatchWithoutAKey_isExecutedAgainOnRedelivery_withTheSameProvenance() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope initiated = harness.appendSagaCaused();
          listener.keyed = false;
          listener.crashAfterDispatch = delivery -> delivery == 1;

          feed.cycle();
          feed.cycle();

          List<EventEnvelope> captured = harness.captured();
          assertThat(captured).as("no key, no deduplication: the application's choice").hasSize(2);
          assertThat(captured).allSatisfy(event -> assertContinuesTheSagasReply(initiated, event));
          assertThat(captured.get(0).metadata().eventId())
              .isNotEqualTo(captured.get(1).metadata().eventId());
        });
  }

  @TestFactory
  Stream<DynamicTest> aListenerThatSwallowsTheFailure_losesTheCommand() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope initiated = harness.appendSagaCaused();
          listener.swallowFailures = true;
          harness.failuresLeft.set(1);

          feed.cycle();

          assertThat(listener.swallowed).hasValue(1);
          assertThat(feed.checkpoint())
              .as("the subscription saw no failure and moved on")
              .isEqualTo(initiated.globalOffset().value());

          feed.cycle();

          assertThat(listener.deliveries).as("the event is not delivered again").hasValue(1);
          assertThat(harness.captured()).as("the command never ran to completion").isEmpty();
          assertThat(harness.dlq.all()).as("and no dead-letter entry holds it").isEmpty();
        });
  }

  /**
   * An event no saga caused: the failed command is dead-lettered like any other, so it has two
   * retry channels. Here the subscription's redelivery reaches the command inbox first and the
   * replay finds the command committed; {@code DeadLetterReplayProvenanceTest} runs the other
   * order, in which the stored event has no causation id.
   */
  @TestFactory
  Stream<DynamicTest> anEventWithoutAnOrigin_aFailedDispatchIsDeadLettered_andOneEventIsStored() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope requested = harness.appendRequestCaused();
          harness.failuresLeft.set(1);

          feed.cycle();

          assertThat(harness.captured()).isEmpty();
          assertThat(harness.dlq.all()).hasSize(1);
          assertThat(harness.dlq.all().getFirst().idempotencyKey()).isEqualTo(keyOf(requested));
          assertThat(harness.dlq.all().getFirst().userId()).isNull();

          feed.cycle();

          assertThat(harness.captured()).hasSize(1);
          EventMetadata metadata = harness.captured().getFirst().metadata();
          assertThat(metadata.sagaOrigin()).isNull();
          assertThat(metadata.causationId()).isEqualTo(CausationId.of("evt-request-1"));
          assertThat(metadata.correlationId()).isEqualTo(CorrelationId.of("client-corr-7"));
          assertThat(metadata.traceId()).isEqualTo(TraceId.of("trace-7"));
          assertThat(metadata.userId()).as("the user of the first event issued nothing").isNull();
          assertThat(metadata.baggage()).isEmpty();

          replayDeadLetters(harness);

          assertThat(harness.captured()).as("the replay hit the inbox").hasSize(1);
          assertThat(harness.dlq.all()).isEmpty();
        });
  }

  /**
   * The same event, and a listener that lets every failure leave it, as a listener must for an
   * event a saga caused. Each failed delivery publishes a dead-letter entry of its own, under the
   * command id of that execution. With an idempotency key the redelivery and the replays of all
   * entries end in one stored event.
   */
  @TestFactory
  Stream<DynamicTest> anEventWithoutAnOrigin_everyFailedDeliveryLeavesAnEntry_aKeyDrainsThemAll() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope requested = harness.appendRequestCaused();
          harness.failuresLeft.set(3);

          feed.cycle();
          feed.cycle();
          feed.cycle();

          assertThat(harness.captured()).isEmpty();
          assertThat(harness.dlq.all()).as("one entry per failed delivery").hasSize(3);
          assertThat(harness.dlq.all())
              .allSatisfy(entry -> assertThat(entry.idempotencyKey()).isEqualTo(keyOf(requested)));
          assertThat(harness.dlq.all().stream().map(entry -> entry.commandId()).distinct())
              .hasSize(3);

          feed.cycle();

          assertThat(harness.captured()).hasSize(1);
          assertThat(harness.dlq.all())
              .as("the entries outlive the successful delivery")
              .hasSize(3);

          replayDeadLetters(harness);

          assertThat(harness.dlq.all()).isEmpty();
          assertThat(harness.captured()).as("every replay hit the inbox").hasSize(1);
          assertThat(harness.captured().getFirst().metadata().causationId())
              .as("the redelivery committed first")
              .isEqualTo(CausationId.of("evt-request-1"));
        });
  }

  /**
   * Without a key nothing ties the redelivery and the replays together: the command is executed by
   * the successful delivery and once more by the replay of every entry, and the replays' events are
   * causation roots.
   */
  @TestFactory
  Stream<DynamicTest> anEventWithoutAnOrigin_withoutAKey_everyEntryRunsTheCommandAgain() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          harness.appendRequestCaused();
          listener.keyed = false;
          harness.failuresLeft.set(3);

          feed.cycle();
          feed.cycle();
          feed.cycle();
          assertThat(harness.dlq.all()).hasSize(3);

          feed.cycle();
          assertThat(harness.captured()).hasSize(1);

          replayDeadLetters(harness);

          assertThat(harness.dlq.all()).isEmpty();
          assertThat(harness.captured())
              .as("one execution by the delivery, one by each of the three replays")
              .hasSize(4);
          assertThat(harness.captured().stream().map(event -> event.metadata().causationId()))
              .containsExactly(CausationId.of("evt-request-1"), null, null, null);
          assertThat(harness.captured())
              .allSatisfy(event -> assertThat(event.metadata().sagaOrigin()).isNull());
        });
  }

  /** One batch of the command dead-letter retry runner over the harness's queue. */
  private static void replayDeadLetters(Harness harness) {
    try (DeadLetterRetryRunner replay =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(harness.dlq)
            .commandBus(harness.bus)
            .objectMapper(harness.mapper)
            .policy(new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false))
            .pollInterval(Duration.ofMinutes(1))
            .registerCommand(Capture.class)
            .build()) {
      replay.processBatch();
    }
  }

  /**
   * A command the domain rejects fails on every delivery. The bus dead-letters a rejection under no
   * rule, and a subscription has no skip for the event, so a listener that lets it propagate is
   * handed the same event again for as long as it does; one that takes the rejection as the
   * command's answer and returns lets the subscription move on.
   */
  @TestFactory
  Stream<DynamicTest> aRejectedCommand_blocksTheSubscriptionUntilTheListenerTakesTheAnswer() {
    return onBothSubscriptions(
        (harness, listener, feed) -> {
          EventEnvelope initiated = harness.appendSagaCaused();
          harness.rejecting.set(true);

          feed.cycle();
          feed.cycle();
          feed.cycle();

          assertThat(listener.deliveries).as("delivered again on every cycle").hasValue(3);
          assertThat(feed.checkpoint()).isZero();
          assertThat(harness.captured()).isEmpty();
          assertThat(harness.dlq.all()).as("a rejection is never dead-lettered").isEmpty();

          listener.takeRejectionsAsTheAnswer = true;
          feed.cycle();

          assertThat(listener.rejections).hasValue(1);
          assertThat(feed.checkpoint()).isEqualTo(initiated.globalOffset().value());
          assertThat(harness.captured()).isEmpty();
          feed.cycle();
          assertThat(listener.deliveries).as("acknowledged: not delivered again").hasValue(4);
        });
  }

  // ==== A real saga's reply, continued by a listener ====

  /** Reacts to the saga's reply {@code Reserved} with a keyed command inside its continuation. */
  private static EventListener continuingTheReply(
      SagaDispatchProvenanceTest.Harness h, List<Boolean> sagaDispatchSeen) {
    return batch -> {
      for (EventEnvelope event : batch) {
        if (event.event() instanceof Reserved reserved) {
          StreamRuneContext.continuing(event)
              .run(
                  () -> {
                    sagaDispatchSeen.add(StreamRuneContext.isSagaDispatch());
                    h.bus.execute(
                        new Audit(reserved.orderId()),
                        IdempotencyKey.of("audit:" + event.metadata().eventId().value()));
                  });
        }
      }
    };
  }

  private static void assertTheAuditContinuesTheReservation(
      SagaDispatchProvenanceTest.Harness h, List<Boolean> sagaDispatchSeen) {
    assertThat(h.stored(Reserved.class)).hasSize(1);
    EventEnvelope reserved = h.stored(Reserved.class).getFirst();
    SagaOrigin sagaOrigin = reserved.metadata().sagaOrigin();
    assertThat(sagaOrigin.hop()).as("the reply to the saga's own command").isZero();

    assertThat(h.stored(Audited.class)).hasSize(1);
    EventMetadata audited = h.stored(Audited.class).getFirst().metadata();
    assertThat(audited.sagaOrigin())
        .isEqualTo(new SagaOrigin(sagaOrigin.sagaType(), sagaOrigin.sagaId(), 1));
    assertThat(audited.causationId())
        .isEqualTo(CausationId.of(reserved.metadata().eventId().value()));
    assertThat(audited.correlationId()).isEqualTo(reserved.metadata().correlationId());
    assertThat(audited.userId()).isNull();
    assertThat(sagaDispatchSeen).as("a continuation is not a saga dispatch").containsExactly(false);
  }

  /** The runner and the listener share one subscription; the trigger is appended unbound. */
  @Test
  void aSagasReply_continuedOnTheSubscriptionTheRunnerShares() {
    try (var h = new SagaDispatchProvenanceTest.Harness()) {
      var seen = new CopyOnWriteArrayList<Boolean>();
      EventListener processManager = continuingTheReply(h, seen);
      var subscription =
          new InMemoryEventSubscription(
              h.eventStore,
              batch -> {
                h.runner.asEventListener().onEvents(batch);
                processManager.onEvents(batch);
              });
      subscription.start();

      h.append(new Opened("42"));

      assertThat(subscription.deliveryFailureCount()).isZero();
      assertTheAuditContinuesTheReservation(h, seen);
      subscription.close();
    }
  }

  /**
   * The runner is driven by hand, so the reply's own append notifies the listener's subscription on
   * the dispatching thread, inside the saga's dispatch: the continuation still runs without the
   * saga's privileges and one hop from it.
   */
  @Test
  void aSagasReply_continuedOnTheThreadOfTheSagasOwnDispatch() {
    try (var h = new SagaDispatchProvenanceTest.Harness()) {
      var seen = new CopyOnWriteArrayList<Boolean>();
      var subscription = new InMemoryEventSubscription(h.eventStore, continuingTheReply(h, seen));
      subscription.start();

      h.open();

      assertThat(subscription.deliveryFailureCount()).isZero();
      assertTheAuditContinuesTheReservation(h, seen);
      subscription.close();
    }
  }

  /** The reply reaches the listener on the poll thread of a started polling subscription. */
  @Test
  void aSagasReply_continuedOnAPollingSubscriptionsOwnThread() {
    try (var h = new SagaDispatchProvenanceTest.Harness()) {
      var seen = new CopyOnWriteArrayList<Boolean>();
      try (PollingEventSubscription subscription =
          PollingEventSubscription.builder()
              .subscriptionName(SUBSCRIPTION)
              .eventStore(h.eventStore)
              .offsetStore(new InMemoryOffsetStore())
              .listener(continuingTheReply(h, seen))
              .config(new SubscriptionConfig(false, Duration.ofMillis(5), Duration.ZERO))
              .build()) {
        subscription.start();

        h.open();

        Awaitility.await()
            .atMost(Duration.ofSeconds(10))
            .until(() -> !h.stored(Audited.class).isEmpty());
        assertTheAuditContinuesTheReservation(h, seen);
      }
    }
  }
}
