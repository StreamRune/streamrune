package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.runtime.EventSerializationBenchmark.LoadState;
import org.streamrune.runtime.EventSerializationBenchmark.OrderRecordedV3;

/**
 * Runs {@link EventSerializationBenchmark#loadWithUpcasting} twice against a real {@code @Setup}
 * state, outside the JMH harness. Part of {@code check} through {@code jmhSmokeTest}: every call
 * must return the whole stream as current-version events, whatever version they are stored at, so a
 * state that stamped the wrong version (and skipped or refused the upcasting) fails here.
 */
class EventSerializationBenchmarkSmokeTest {

  private final EventSerializationBenchmark benchmark = new EventSerializationBenchmark();

  @ParameterizedTest(name = "{0} upcast step(s)")
  @ValueSource(ints = {0, 1, 2})
  void loadWithUpcastingRunsTwiceAndReturnsCurrentEvents(int upcastSteps) {
    var state = new LoadState();
    state.upcastSteps = upcastSteps;
    state.appendEvents();

    benchmark.loadWithUpcasting(state);
    var events = benchmark.loadWithUpcasting(state).events();

    assertThat(events).hasSize(EventSerializationBenchmark.STREAM_LENGTH);
    assertThat(events)
        .extracting(EventEnvelope::event)
        .allSatisfy(event -> assertThat(event).isInstanceOf(OrderRecordedV3.class));
    assertThat(events.getLast().event())
        .isEqualTo(new OrderRecordedV3("order-99", "customer-99", 99, "EUR"));
  }
}
