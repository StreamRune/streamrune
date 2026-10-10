package org.streamrune.quarkus;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.AbstractMultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The last operator of a Server-Sent Events stream, directly in front of the subscriber that writes
 * the response: it ends the stream when that subscriber throws from {@code onItem}.
 *
 * <p><b>Why a downstream throws.</b> Quarkus REST serializes a frame inside its subscriber's {@code
 * onNext}, with the application's message body writer. A writer that throws an unchecked exception
 * (JSON-B's {@code JsonbException}, a writer of the application's own) throws it out of {@code
 * onNext}.
 *
 * <p><b>What would happen without this operator.</b> The exception would travel up to {@code
 * emitter.emit}. Mutiny's serialized emitter claims a work-in-progress flag around every {@code
 * emit} and does not release it when the downstream throws; from then on {@code fail} and {@code
 * complete} only record their signal. Quarkus REST's subscriber would never be told that the stream
 * has ended, so the HTTP response would stay open with nothing left to write to it or to end it.
 *
 * <p><b>What it does.</b> Reactive Streams §2.13 says a subscriber that throws from {@code onNext}
 * must be considered cancelled. The operator catches the exception, cancels the upstream — which
 * runs the emitter's termination hook, the stream's teardown — and signals the failure to the
 * downstream, which Quarkus REST answers by ending the response. The exception does not reach the
 * emitter, so the emitter stays able to terminate.
 *
 * @param <T> the type of the frames
 */
final class DownstreamFailureGuard<T> extends AbstractMultiOperator<T, T> {

  private static final Logger log = LoggerFactory.getLogger(DownstreamFailureGuard.class);

  DownstreamFailureGuard(Multi<T> upstream) {
    super(upstream);
  }

  @Override
  public void subscribe(MultiSubscriber<? super T> downstream) {
    upstream.subscribe().withSubscriber(new Guard<>(downstream));
  }

  private static final class Guard<T> extends MultiOperatorProcessor<T, T> {

    Guard(MultiSubscriber<? super T> downstream) {
      super(downstream);
    }

    @Override
    public void onItem(T item) {
      if (isDone()) {
        return;
      }
      try {
        downstream.onItem(item);
      } catch (Throwable failure) {
        log.warn("Ending an SSE stream whose frame could not be written: {}", failure.toString());
        cancelUpstream();
        downstream.onFailure(failure);
      }
    }
  }
}
