package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Audit;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Audited;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Harness;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Opened;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Recheck;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Rechecked;
import org.streamrune.runtime.SagaDispatchProvenanceTest.Reserved;
import org.streamrune.test.InMemoryEventSubscription;

/**
 * A listener of an {@link InMemoryEventSubscription} that reacts to a saga's reply event by
 * executing a command of its own is not part of the saga's dispatch: the in-memory subscription
 * calls it outside the saga flag, the dispatch cause and the request context of the appending
 * thread, as a polling subscription's listener is. The command's events therefore carry no saga
 * origin and no causation id, whichever way the reply reached the listener.
 *
 * <p>The commands run on the real {@link VirtualThreadCommandBus}; the saga is driven by the real
 * {@link SagaRunner}.
 */
class InMemorySubscriptionSagaReplyTest {

  private static final String ORDER = "42";

  /** What the listener's own command stored, and what the listener found bound when it ran. */
  record Observed(
      SagaOrigin sagaOrigin,
      CausationId causationId,
      UserId userId,
      TraceId traceId,
      boolean sagaDispatch,
      StreamRuneContext.DispatchCause boundCause) {}

  private static final Observed NOTHING_OF_THE_SAGA =
      new Observed(null, null, null, null, false, null);

  /** What the listener found bound: the saga flag, the cause, and whether a context is bound. */
  record Seen(boolean sagaDispatch, StreamRuneContext.DispatchCause cause, boolean contextBound) {}

  /** Reacts to the saga's reply {@code Reserved} with a keyed command of its own. */
  private static EventListener auditing(Harness h, List<Seen> seen) {
    return batch -> {
      for (EventEnvelope event : batch) {
        if (event.event() instanceof Reserved reserved) {
          seen.add(
              new Seen(
                  StreamRuneContext.isSagaDispatch(),
                  StreamRuneContext.cause(),
                  StreamRuneContext.CURRENT.isBound()));
          h.bus.execute(
              new Audit(reserved.orderId()),
              IdempotencyKey.of("audit:" + event.globalOffset().value()));
        }
      }
    };
  }

  private static Observed observed(Harness h, List<Seen> seen) {
    assertThat(seen).as("the listener reacted to the reply once").hasSize(1);
    assertThat(h.stored(Audited.class)).hasSize(1);
    EventMetadata audited = h.stored(Audited.class).getFirst().metadata();
    return new Observed(
        audited.sagaOrigin(),
        audited.causationId(),
        audited.userId(),
        audited.traceId(),
        seen.getFirst().sagaDispatch(),
        seen.getFirst().cause());
  }

  /**
   * The saga runner and the listener share one subscription, and the trigger is appended by a
   * thread with nothing bound: the reply is appended inside the saga's dispatch and delivered once
   * that dispatch has returned.
   */
  @Test
  void theReplyDeliveredAfterTheSagasDispatchReturned_onOneSubscription() {
    try (var h = new Harness()) {
      var seen = new CopyOnWriteArrayList<Seen>();
      EventListener processManager = auditing(h, seen);
      var subscription =
          new InMemoryEventSubscription(
              h.eventStore,
              batch -> {
                h.runner.asEventListener().onEvents(batch);
                processManager.onEvents(batch);
              });
      subscription.start();

      h.append(new Opened(ORDER));

      assertThat(h.stored(Reserved.class)).hasSize(1);
      assertThat(subscription.deliveryFailureCount()).isZero();
      assertThat(observed(h, seen)).isEqualTo(NOTHING_OF_THE_SAGA);
      assertThat(seen.getFirst().contextBound())
          .as("nothing was bound where the trigger was appended, so nothing is bound here")
          .isFalse();
      subscription.close();
    }
  }

  /**
   * The saga runner is driven by hand, so the reply's own append notifies the listener's
   * subscription on the dispatching thread, inside the saga's dispatch.
   */
  @Test
  void theReplyDeliveredOnTheNotificationOfTheSagasOwnAppend() {
    try (var h = new Harness()) {
      var seen = new CopyOnWriteArrayList<Seen>();
      var subscription = new InMemoryEventSubscription(h.eventStore, auditing(h, seen));
      subscription.start();

      h.open();

      assertThat(h.stored(Reserved.class)).hasSize(1);
      assertThat(subscription.deliveryFailureCount()).isZero();
      assertThat(observed(h, seen)).isEqualTo(NOTHING_OF_THE_SAGA);
      assertThat(seen.getFirst().contextBound())
          .as("the saga dispatch's context is replaced by an empty one, since it cannot be unbound")
          .isTrue();
      subscription.close();
    }
  }

  /** The reply is delivered by a poll, after the saga's dispatch returned. */
  @Test
  void theReplyDeliveredByAPoll() {
    try (var h = new Harness()) {
      var seen = new CopyOnWriteArrayList<Seen>();
      var subscription = new InMemoryEventSubscription(h.eventStore, auditing(h, seen));
      subscription.start();
      subscription.dropNotifications(true);
      h.open();
      assertThat(h.stored(Reserved.class)).hasSize(1);
      assertThat(seen).isEmpty();
      subscription.dropNotifications(false);

      subscription.poll();

      assertThat(subscription.deliveryFailureCount()).isZero();
      assertThat(observed(h, seen)).isEqualTo(NOTHING_OF_THE_SAGA);
      assertThat(seen.getFirst().contextBound()).isFalse();
      subscription.close();
    }
  }

  /**
   * A command executed through the bus holds its stream's lock while its events are appended and
   * delivered. A listener that reacts with a command on that same stream still runs to completion
   * before the first command returns.
   */
  @Test
  void aListenersCommandOnTheAppendingCommandsOwnStream_completesBeforeThatCommandReturns() {
    try (var h = new Harness()) {
      var subscription =
          new InMemoryEventSubscription(
              h.eventStore,
              batch -> {
                for (EventEnvelope event : batch) {
                  if (event.event() instanceof Audited audited) {
                    h.bus.execute(new Recheck(audited.orderId()));
                  }
                }
              });
      subscription.start();

      h.bus.execute(new Audit(ORDER));

      assertThat(subscription.deliveryFailureCount()).isZero();
      assertThat(h.stored(Rechecked.class)).hasSize(1);
      assertThat(subscription.lastDeliveredOffset()).isEqualTo(2);
      subscription.close();
    }
  }
}
