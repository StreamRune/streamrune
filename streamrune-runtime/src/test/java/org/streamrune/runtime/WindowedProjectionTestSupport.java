package org.streamrune.runtime;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

final class WindowedProjectionTestSupport {

  record Counted(int n) implements DomainEvent {}

  /**
   * Hands every fixture envelope its own strictly-increasing {@link GlobalOffset}. A real event
   * store never emits two events at the same global offset, and the fixture used to stamp {@link
   * GlobalOffset#initial()} on all of them — which made every envelope look like a redelivery of
   * every other one to any offset-aware consumer. Tests that feed envelopes through {@link
   * SimpleTestEventStore} are unaffected either way (it re-indexes from 1); tests that call {@code
   * Projection.process} directly depend on this.
   */
  private static final AtomicLong NEXT_OFFSET = new AtomicLong(1);

  static EventEnvelope evt(Instant t) {
    return new EventEnvelope(
        GlobalOffset.of(NEXT_OFFSET.getAndIncrement()),
        StreamId.of(AggregateType.of("stream"), AggregateId.of("s1")),
        new Version(1),
        new EventType("Counted"),
        new Counted(1),
        new EventMetadata(
            new EventId("e-" + t.toEpochMilli()),
            new CommandId("c-1"),
            null,
            null,
            new CorrelationId("corr-1"),
            null,
            null,
            t));
  }

  private WindowedProjectionTestSupport() {}
}
