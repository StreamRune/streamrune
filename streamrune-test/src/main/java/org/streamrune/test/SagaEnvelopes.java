package org.streamrune.test;

import java.time.Instant;
import java.util.Map;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Hand-built saga-caused metadata and envelopes for unit tests: what the command bus stores for an
 * event produced by a command a saga dispatched, without running a saga or a bus.
 *
 * <p>In production the command bus alone writes {@link EventMetadata#sagaOrigin()}. Use this helper
 * to feed a saga, a projection or a listener under test an event that looks like the reply to a
 * saga's own command.
 */
public final class SagaEnvelopes {

  private SagaEnvelopes() {}

  /**
   * Metadata of an event produced by a command the saga dispatched itself: the saga's origin at hop
   * 0, the correlation id saga dispatch binds (the saga id), generated event and command ids, the
   * current instant, and no trace, user, causation id or baggage.
   *
   * @param stateType the saga's state class; its saga type is derived as the runners derive it
   * @param sagaId the dispatching saga instance
   * @return metadata naming the saga as the origin
   */
  public static EventMetadata metadataCausedBy(
      Class<? extends SagaState> stateType, SagaId sagaId) {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of(sagaId.value()),
        null,
        null,
        Instant.now(),
        Map.of(),
        SagaOrigin.dispatchedBy(SagaType.fromClass(stateType), sagaId));
  }

  /**
   * An envelope for {@code event} as the first event of {@code streamId}, not yet stored (initial
   * global offset and version), carrying {@link #metadataCausedBy(Class, SagaId)}.
   *
   * @param stateType the saga's state class
   * @param sagaId the dispatching saga instance
   * @param streamId the stream the event belongs to
   * @param event the event payload
   * @return the saga-caused envelope
   */
  public static EventEnvelope causedBy(
      Class<? extends SagaState> stateType, SagaId sagaId, StreamId streamId, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        Version.initial(),
        EventType.fromClass(event.getClass()),
        event,
        metadataCausedBy(stateType, sagaId));
  }
}
