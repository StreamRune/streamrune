package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.runtime.ProjectionRunnerBenchmark.CustomerOrders;
import org.streamrune.runtime.ProjectionRunnerBenchmark.RunnerState;

/**
 * Runs {@link ProjectionRunnerBenchmark#processBatch} twice against a real {@code @Setup} state,
 * outside the JMH harness. Part of {@code check} through {@code jmhSmokeTest}: the second call must
 * process the whole batch again, not find the projection caught up and measure an empty read.
 */
class ProjectionRunnerBenchmarkSmokeTest {

  private final ProjectionRunnerBenchmark benchmark = new ProjectionRunnerBenchmark();

  @ParameterizedTest(name = "batch of {0}")
  @ValueSource(ints = {10, 100})
  void processBatchRunsTwiceOverTheWholeBatch(int batchSize) {
    var state = new RunnerState();
    state.batchSize = batchSize;
    state.appendEvents();

    assertThat(benchmark.processBatch(state)).isEqualTo(GlobalOffset.of(batchSize));
    var afterFirst = state.repository.findAll(ProjectionRunnerBenchmark.NAME, CustomerOrders.class);
    assertThat(benchmark.processBatch(state)).isEqualTo(GlobalOffset.of(batchSize));

    var afterSecond =
        state.repository.findAll(ProjectionRunnerBenchmark.NAME, CustomerOrders.class);
    assertThat(afterSecond).hasSize(ProjectionRunnerBenchmark.CUSTOMERS);
    assertThat(afterSecond).containsExactlyInAnyOrderElementsOf(afterFirst);
    assertThat(afterSecond)
        .extracting(CustomerOrders::orders)
        .containsOnly((long) batchSize / ProjectionRunnerBenchmark.CUSTOMERS);
  }
}
