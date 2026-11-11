package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.runtime.TracingBenchmark.TracedBusState;

/**
 * Runs {@link TracingBenchmark#commandBusThroughput} twice for each tracing parameter against a
 * real {@code @Setup} state, outside the JMH harness; see {@code StreamRuneBenchmarkSmokeTest}.
 */
class TracingBenchmarkSmokeTest {

  private final TracingBenchmark benchmark = new TracingBenchmark();

  @ParameterizedTest
  @ValueSource(strings = {"NOOP", "SAMPLER_OFF", "SAMPLER_ON"})
  void commandBusThroughputRunsTwice(String tracing) {
    TracedBusState state = new TracedBusState();
    state.tracing = tracing;
    state.setup();
    try {
      assertDoesNotThrow(() -> benchmark.commandBusThroughput(state));
      assertDoesNotThrow(() -> benchmark.commandBusThroughput(state));
    } finally {
      state.tearDown();
    }
  }
}
