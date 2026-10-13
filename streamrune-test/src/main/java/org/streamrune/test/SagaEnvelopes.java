package org.streamrune.test;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
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
 * saga's own command ({@link #causedBy(Class, SagaId, StreamId, DomainEvent)}), or like the event
 * of a command a listener dispatched for such an event inside {@link
 * StreamRuneContext#continuing(EventEnvelope)} ({@link #continuationOf}): one hop further, with the
 * causing event's id as its causation id.
 *
 * <p>A saga runner routes by the origin's saga type and saga id; the hop count and the causation id
 * take no part in it, so an envelope of either kind reaches the saga the same way. Build the later
 * hop where the code under test reads the hop, the causation id or the stored shape.
 */
public final class SagaEnvelopes {

  private SagaEnvelopes() {}

  /**
   * Metadata of an event produced by a command the saga dispatched itself: the saga's origin at hop
   * 0, the correlation id saga dispatch binds (the origin's, {@code streamrune:saga:<sagaId>}),
   * generated event and command ids, the current instant, and no trace, user, causation id or
   * baggage.
   *
   * @param stateType the saga's state class; its saga type is derived as the runners derive it
   * @param sagaId the dispatching saga instance
   * @return metadata naming the saga as the origin
   */
  public static EventMetadata metadataCausedBy(
      Class<? extends SagaState> stateType, SagaId sagaId) {
    SagaOrigin origin = SagaOrigin.dispatchedBy(SagaType.fromClass(stateType), sagaId);
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        origin.correlationId(),
        null,
        null,
        Instant.now(),
        Map.of(),
        origin);
  }

  /**
   * An envelope for {@code event} as the first event of {@code streamId}, not yet stored (initial
   * global offset and version), carrying {@link #metadataCausedBy(Class, SagaId)}.
   *
   * <p>The envelope sits at global offset 0. A {@code SagaRunner} skips an event at or below the
   * offset its saga applied last, as a redelivery, so hand this envelope to a runner only after
   * appending it to an event store (the store assigns the offset; read it back), or build an
   * envelope of your own at a later offset around {@link #metadataCausedBy(Class, SagaId)}.
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

  /**
   * Metadata of an event produced by a command that was dispatched inside {@link
   * StreamRuneContext#continuing(EventEnvelope) StreamRuneContext.continuing(cause)}: what a
   * listener's command stores when it reacts to {@code cause}.
   *
   * <p>It is built by binding {@code continuing(cause)} and asking {@link InMemoryCommandBus} for
   * the metadata it writes under those bindings, so it follows the framework's rule and holds no
   * copy of it: the saga origin of {@code cause} one hop further (none when {@code cause} carries
   * none), the id of {@code cause} as the causation id, the origin's correlation id (the
   * correlation id of {@code cause} when it carries no origin), the trace of {@code cause}, no user
   * and no baggage, and generated event and command ids.
   *
   * @param cause the event the command reacts to, typically {@link #causedBy(Class, SagaId,
   *     StreamId, DomainEvent)} or another continuation
   * @return the metadata of the continuation's event
   * @throws NullPointerException if {@code cause} is {@code null}
   * @throws IllegalStateException if the saga origin of {@code cause} is at the hop limit
   */
  public static EventMetadata metadataOfContinuation(EventEnvelope cause) {
    return StreamRuneContext.continuing(cause)
        .call(
            () ->
                InMemoryCommandBus.metadataUnderTheBoundDispatch(IdGenerator.generateCommandId()));
  }

  /**
   * An envelope for {@code event} as the first event of {@code streamId}, not yet stored (initial
   * global offset and version), carrying {@link #metadataOfContinuation(EventEnvelope)}: the event
   * of a command a listener dispatched inside {@code StreamRuneContext.continuing(cause)}.
   *
   * <p>The note on the offset of {@link #causedBy(Class, SagaId, StreamId, DomainEvent)} applies.
   *
   * @param cause the event the command reacts to
   * @param streamId the stream the continuation's event belongs to
   * @param event the event payload
   * @return the continuation's envelope
   * @throws NullPointerException if an argument is {@code null}
   */
  public static EventEnvelope continuationOf(
      EventEnvelope cause, StreamId streamId, DomainEvent event) {
    Objects.requireNonNull(cause, "cause is required");
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(event, "event is required");
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        Version.initial(),
        EventType.fromClass(event.getClass()),
        event,
        metadataOfContinuation(cause));
  }

  /**
   * A saga-caused envelope {@code hop} continuations away from the saga's own dispatch: {@link
   * #causedBy(Class, SagaId, StreamId, DomainEvent)} for hop 0, and for a later hop the last of a
   * chain of {@link #continuationOf} envelopes that starts there, each caused by the one before.
   *
   * @param stateType the saga's state class
   * @param sagaId the dispatching saga instance
   * @param hop how many continuations lie between the saga's dispatch and the event; 0 or more
   * @param streamId the stream the event belongs to
   * @param event the event payload
   * @return the envelope, with the saga's origin at {@code hop} and, for a hop above 0, the id of
   *     the previous envelope of the chain as its causation id
   * @throws IllegalArgumentException if {@code hop} is negative
   */
  public static EventEnvelope causedBy(
      Class<? extends SagaState> stateType,
      SagaId sagaId,
      int hop,
      StreamId streamId,
      DomainEvent event) {
    if (hop < 0) {
      throw new IllegalArgumentException("hop must be 0 or more, got " + hop);
    }
    EventEnvelope envelope = causedBy(stateType, sagaId, streamId, event);
    for (int i = 0; i < hop; i++) {
      envelope = continuationOf(envelope, streamId, event);
    }
    return envelope;
  }
}
