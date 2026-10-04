package org.streamrune.core.saga;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.types.AggregateId;

class SagaDeciderTest {

  record TestState(SagaId sagaId, SagaStatus status) implements SagaState {}

  record TestCmd() implements Command {}

  static class MinimalDecider implements SagaDecider<TestState> {
    @Override
    public Class<TestState> stateType() {
      return TestState.class;
    }

    @Override
    public TestState initialState(SagaId sagaId) {
      return new TestState(sagaId, SagaStatus.STARTED);
    }

    @Override
    public TestState evolve(TestState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(TestState state, EventEnvelope event) {
      return List.of();
    }
  }

  @Test
  void defaultCompensateReturnsEmptyList() {
    var decider = new MinimalDecider();
    List<SagaCommand> result =
        decider.compensate(
            new TestState(SagaId.of("s1"), SagaStatus.RUNNING),
            new RuntimeException("fail"),
            SagaCommand.of(new TestCmd(), AggregateId.of("agg-1")));
    assertTrue(result.isEmpty());
  }

  @Test
  void defaultTimeoutReturnsEmpty() {
    var decider = new MinimalDecider();
    assertTrue(decider.timeout().isEmpty());
  }

  @Test
  void timeoutCanBeOverridden() {
    var decider =
        new SagaDecider<TestState>() {
          @Override
          public Class<TestState> stateType() {
            return TestState.class;
          }

          @Override
          public TestState initialState(SagaId sagaId) {
            return new TestState(sagaId, SagaStatus.STARTED);
          }

          @Override
          public TestState evolve(TestState state, EventEnvelope event) {
            return state;
          }

          @Override
          public List<SagaCommand> handle(TestState state, EventEnvelope event) {
            return List.of();
          }

          @Override
          public Optional<Duration> timeout() {
            return Optional.of(Duration.ofMinutes(30));
          }
        };
    assertEquals(Duration.ofMinutes(30), decider.timeout().orElseThrow());
  }

  @Test
  void sagaTimeoutExceptionMessage() {
    var ex = new SagaTimeoutException(SagaId.of("order-123"), Duration.ofMinutes(5));
    assertTrue(ex.getMessage().contains("order-123"));
    assertTrue(ex.getMessage().contains("5"));
    assertEquals(SagaId.of("order-123"), ex.sagaId());
    assertEquals(Duration.ofMinutes(5), ex.timeout());
  }
}
