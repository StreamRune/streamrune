package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryProjectionRepository;

@ExtendWith(MockitoExtension.class)
class ValidatingProjectionDecoratorTest {

  @Mock Projection delegate;
  @Mock Validator validator;

  record TestEvent(String data) implements DomainEvent {}

  private EventEnvelope envelope(DomainEvent event) {
    return new EventEnvelope(
        new GlobalOffset(1L),
        StreamId.of(AggregateType.of("stream"), AggregateId.of("stream-1")),
        new Version(1),
        new EventType("TestEvent"),
        event,
        new EventMetadata(
            new EventId("evt-1"),
            new CommandId("cmd-1"),
            null,
            null,
            new CorrelationId("corr-1"),
            null,
            null,
            Instant.now()));
  }

  @Test
  void shouldPassValidEventsThroughToDelegate() {
    var event = new TestEvent("valid");
    var env = envelope(event);
    when(validator.validate(event)).thenReturn(Set.of());

    var decorator = new ValidatingProjectionDecorator(delegate, validator);
    decorator.process(List.of(env));

    verify(delegate).process(List.of(env));
  }

  @SuppressWarnings("unchecked")
  @Test
  void shouldFilterOutInvalidEvents() {
    var validEvent = new TestEvent("valid");
    var invalidEvent = new TestEvent("invalid");
    var validEnv = envelope(validEvent);
    var invalidEnv = envelope(invalidEvent);

    when(validator.validate(validEvent)).thenReturn(Set.of());
    ConstraintViolation<TestEvent> violation = mock(ConstraintViolation.class);
    when(validator.validate(invalidEvent)).thenReturn(Set.of(violation));

    var decorator = new ValidatingProjectionDecorator(delegate, validator);
    decorator.process(List.of(invalidEnv, validEnv));

    verify(delegate).process(List.of(validEnv));
  }

  @SuppressWarnings("unchecked")
  @Test
  void shouldNotCallDelegateWhenAllEventsInvalid() {
    var event = new TestEvent("bad");
    var env = envelope(event);
    ConstraintViolation<TestEvent> violation = mock(ConstraintViolation.class);
    when(validator.validate(event)).thenReturn(Set.of(violation));

    var decorator = new ValidatingProjectionDecorator(delegate, validator);
    decorator.process(List.of(env));

    verifyNoInteractions(delegate);
  }

  @Test
  void shouldPassFullBatchWhenAllValid() {
    var event1 = new TestEvent("a");
    var event2 = new TestEvent("b");
    var env1 = envelope(event1);
    var env2 = envelope(event2);

    when(validator.validate(event1)).thenReturn(Set.of());
    when(validator.validate(event2)).thenReturn(Set.of());

    var decorator = new ValidatingProjectionDecorator(delegate, validator);
    decorator.process(List.of(env1, env2));

    verify(delegate).process(List.of(env1, env2));
  }

  @Test
  void shouldInvokeDelegateExactlyOnce() {
    var event = new TestEvent("ok");
    var env = envelope(event);
    when(validator.validate(event)).thenReturn(Set.of());

    var decorator = new ValidatingProjectionDecorator(delegate, validator);
    decorator.process(List.of(env));

    verify(delegate, times(1)).process(any());
  }

  // This decorator overrides process(List, ProjectionRepository) only
  // to filter the batch before forwarding, so writesThroughRepository() must forward to the
  // delegate's own answer rather than reading true reflectively off this class.
  @Test
  void writesThroughRepository_forwardsTrueFromDelegate() {
    when(delegate.writesThroughRepository()).thenReturn(true);
    var decorator = new ValidatingProjectionDecorator(delegate, validator);
    org.junit.jupiter.api.Assertions.assertTrue(decorator.writesThroughRepository());
  }

  @Test
  void writesThroughRepository_forwardsFalseFromDelegate() {
    when(delegate.writesThroughRepository()).thenReturn(false);
    var decorator = new ValidatingProjectionDecorator(delegate, validator);
    org.junit.jupiter.api.Assertions.assertFalse(decorator.writesThroughRepository());
  }

  @Test
  void writeTarget_isForwardedToTheDelegate() {
    var repo = new InMemoryProjectionRepository();
    var writeThrough =
        new BaseProjection(repo, "orders") {
          @Override
          public void process(List<EventEnvelope> batch) {}
        };
    assertThat(new ValidatingProjectionDecorator(writeThrough, validator).writeTarget())
        .containsSame(repo);

    Projection lambda = batch -> {};
    assertThat(new ValidatingProjectionDecorator(lambda, validator).writeTarget()).isEmpty();
  }
}
