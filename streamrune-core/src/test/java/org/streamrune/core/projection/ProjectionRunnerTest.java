package org.streamrune.core.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class ProjectionRunnerTest {

  /** A runner that only knows the pair it was built with, as every runner before routing did. */
  static final class FixedPairRunner implements ProjectionRunner {
    final List<String> calls = new ArrayList<>();

    @Override
    public void run(
        ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode) {
      calls.add("run " + projectionName.value());
    }

    @Override
    public void reset(ProjectionName projectionName) {
      calls.add("reset " + projectionName.value());
    }
  }

  private static final OffsetStore CHECKPOINTS =
      new OffsetStore() {
        @Override
        public GlobalOffset getLastOffset(ProjectionName projectionName) {
          return GlobalOffset.initial();
        }

        @Override
        public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
          // not used
        }
      };

  @Test
  void runOnATargetRefusesByDefault_namingTheRunnerAndTheWayOut() {
    var runner = new FixedPairRunner();
    var target =
        new CheckpointTarget("search", CHECKPOINTS, AtomicBatchProcessor.nonAtomicAtLeastOnce());

    var refused =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                runner.run(
                    ProjectionName.of("orders"),
                    batch -> {},
                    ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
                    target));

    assertEquals(
        FixedPairRunner.class.getName()
            + " cannot run projection 'orders' on checkpoint target 'search': it reads and commits"
            + " every projection through the checkpoints and processor it was built with. Use a"
            + " runner that routes a registration to its target (ContinuousProjectionRunner,"
            + " PollingProjectionRunner), or build this runner with the target's checkpoints and"
            + " processor and call run(name, projection, mode).",
        refused.getMessage());
    assertEquals(List.of(), runner.calls, "a refused run must not fall back to the runner's pair");
  }

  @Test
  void resetOnATargetRefusesByDefault_namingTheRunnerAndTheWayOut() {
    var runner = new FixedPairRunner();
    var target =
        new CheckpointTarget("search", CHECKPOINTS, AtomicBatchProcessor.nonAtomicAtLeastOnce());

    var refused =
        assertThrows(
            UnsupportedOperationException.class,
            () -> runner.reset(ProjectionName.of("orders"), target));

    assertEquals(
        FixedPairRunner.class.getName()
            + " cannot reset projection 'orders' on checkpoint target 'search': it rewinds only the"
            + " checkpoints it was built with. Use a runner that routes a registration to its"
            + " target (ContinuousProjectionRunner, PollingProjectionRunner), or rewind the"
            + " target's checkpoints with OffsetStore.reset(name).",
        refused.getMessage());
    assertEquals(
        List.of(), runner.calls, "a refused reset must not rewind the runner's own checkpoints");
  }
}
