package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Query;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/** Tests for {@link UnboundRequestContextQueryBus}. */
class UnboundRequestContextQueryBusTest {

  record Probe() implements Query<String> {}

  private static final StreamRuneContext.RequestContext FROM_SOURCE = context("source", "alice");

  private final AtomicInteger asked = new AtomicInteger();
  private final AtomicReference<StreamRuneContext.RequestContext> seen = new AtomicReference<>();

  private UnboundRequestContextQueryBus bus(StreamRuneContext.RequestContext answer) {
    var delegate = new SimpleQueryBus();
    var bus =
        new UnboundRequestContextQueryBus(
            delegate,
            () -> {
              asked.incrementAndGet();
              return answer;
            });
    bus.register(
        Probe.class,
        query -> {
          seen.set(StreamRuneContext.capture());
          return "ok";
        });
    return bus;
  }

  @Test
  void anUnboundDispatchRunsUnderTheSourcesContext() {
    assertThat(bus(FROM_SOURCE).dispatch(new Probe())).isEqualTo("ok");

    assertThat(seen.get()).isSameAs(FROM_SOURCE);
    assertThat(asked).hasValue(1);
    assertThat(StreamRuneContext.CURRENT.isBound()).isFalse();
  }

  @Test
  void aBoundContextIsNeverReplacedAndTheSourceIsNotAsked() {
    var bound = context("bound", "bob");
    var bus = bus(FROM_SOURCE);

    ScopedValue.where(StreamRuneContext.CURRENT, bound).run(() -> bus.dispatch(new Probe()));

    assertThat(seen.get()).isSameAs(bound);
    assertThat(asked).hasValue(0);
  }

  @Test
  void aContextBoundToNullIsKeptAndTheSourceIsNotAsked() {
    var bus = bus(FROM_SOURCE);
    seen.set(FROM_SOURCE);

    ScopedValue.where(StreamRuneContext.CURRENT, null).run(() -> bus.dispatch(new Probe()));

    assertThat(seen.get()).isNull();
    assertThat(asked).hasValue(0);
  }

  @Test
  void aNullAnswerDispatchesWithoutAContext() {
    seen.set(FROM_SOURCE);

    assertThat(bus(null).dispatch(new Probe())).isEqualTo("ok");

    assertThat(seen.get()).isNull();
    assertThat(asked).hasValue(1);
  }

  @Test
  void bothArgumentsAreRequired() {
    assertThatThrownBy(() -> new UnboundRequestContextQueryBus(null, () -> null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new UnboundRequestContextQueryBus(new SimpleQueryBus(), null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void delegateIsTheWrappedBus() {
    var delegate = new SimpleQueryBus();
    assertThat(new UnboundRequestContextQueryBus(delegate, () -> null).delegate())
        .isSameAs(delegate);
  }

  private static StreamRuneContext.RequestContext context(String correlationId, String userId) {
    return new StreamRuneContext.RequestContext(
        null, UserId.of(userId), CorrelationId.of(correlationId), Instant.now(), Map.of());
  }
}
