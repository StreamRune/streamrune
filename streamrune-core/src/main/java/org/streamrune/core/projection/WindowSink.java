package org.streamrune.core.projection;

/**
 * Caller-provided callback invoked by {@code WindowedProjection} when a window closes (or re-emits
 * late under {@link LateDataPolicy#REOPEN}).
 *
 * <p><b>Delivery.</b> A final result ({@code isFinal = true}) that this callback accepted —
 * returned from without throwing — is emitted at most once per window by a given {@code
 * WindowedProjection} instance. When the callback throws on a final result, the window stays open
 * and the same final result, with an identical value, is emitted again on the next close scan (the
 * retry of the batch, or the next batch), so a sink that can fail after a side effect must tolerate
 * that duplicate. A non-final result ({@code isFinal = false}, REOPEN) may be re-delivered with an
 * identical value even after this callback accepted it: when this call, or a later callback for the
 * same late event, throws, that event is left un-applied and its re-delivery re-emits the same
 * value. A re-delivery repeats a value, never changes it, because values are recomputed from window
 * state the failed attempt did not modify — provided the projection's accumulator does not mutate
 * the state it is handed.
 *
 * <p>The {@link WindowResult#state()} passed in is the projection's live window state, not a copy:
 * treat it as read-only.
 */
@FunctionalInterface
public interface WindowSink<W> {
  void onWindow(WindowResult<W> result);
}
