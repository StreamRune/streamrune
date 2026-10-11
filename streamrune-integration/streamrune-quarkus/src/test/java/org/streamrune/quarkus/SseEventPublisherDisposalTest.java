package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Disposes;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.runtime.SseEventPublisher;

/**
 * {@link SseEventPublisher} is {@link AutoCloseable} ("close() stops every worker"), and CDI/Arc
 * destroys a producer-method product only through a matching {@code @Disposes} method — {@link
 * StreamRuneProducers#sseEventPublisher()} declared none, so the still-registered per-subscriber
 * virtual-thread delivery workers (and their queued decrypted events) survived every context
 * teardown (dev-mode live reload, a {@code @QuarkusTest} suite standing up several contexts).
 *
 * <p>Two complementary checks, mirroring the established convention this codebase already uses for
 * the identical defect shape on the crypto producers ({@code
 * StreamRuneCryptoProducersTest#vaultHttpClientDisposer_isDisposes_andClosesTheClient} etc.):
 *
 * <ol>
 *   <li>{@link #containerBootProducesAWorkingSingletonSseEventPublisher()} boots a REAL Arc
 *       container and drives a genuine subscribe/publish/deliver round trip through the resolved
 *       bean — a hand-built {@code new StreamRuneProducers().sseEventPublisher()} call proves
 *       nothing about whether the container's DI graph actually wires and produces a working
 *       instance.
 *   <li>{@link #disposerParameterIsAnnotatedDisposes_andClosesThePublisher()} proves the disposer
 *       CONTRACT reflectively rather than through {@code RealArcTestContainer}'s own shutdown path:
 *       that harness loads Arc's generated bean classes into an isolated {@code URLClassLoader}
 *       (documented on the harness itself) so a package-private disposer — matching the {@code
 *       closeVaultHttpClient}/{@code disposeCryptoEngine} convention in {@code
 *       StreamRuneCryptoProducers} — hits {@code IllegalAccessError} calling back into the
 *       differently-loaded {@code StreamRuneProducers} class, a test-harness classloader artifact
 *       that does not occur in a real Quarkus deployment (where generated Arc classes share the
 *       application classloader). Reflection sidesteps that artifact exactly as the crypto producer
 *       tests already do for the same reason.
 * </ol>
 */
class SseEventPublisherDisposalTest {

  private static EventEnvelope envelope(StreamId stream) {
    var envelope = mock(EventEnvelope.class);
    when(envelope.streamId()).thenReturn(stream);
    when(envelope.globalOffset()).thenReturn(GlobalOffset.of(1));
    return envelope;
  }

  @Test
  void containerBootProducesAWorkingSingletonSseEventPublisher() throws Exception {
    StreamId streamId = StreamId.of(AggregateType.of("order"), AggregateId.of("order-1"));
    var delivered = new CountDownLatch(1);

    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                QuarkusBeanWiringTest.ApplicationInfrastructure.class,
                QuarkusBeanWiringTest.ApplicationEventStoreOverride.class))) {
      var instance = arc.container().instance(SseEventPublisher.class);
      assertTrue(instance.isAvailable(), "the framework @DefaultBean SseEventPublisher must exist");
      SseEventPublisher publisher = instance.get();

      publisher.subscribe(streamId, env -> delivered.countDown());
      publisher.publish(envelope(streamId));
      assertTrue(
          delivered.await(5, TimeUnit.SECONDS),
          "the real Arc-produced SseEventPublisher must actually deliver published events");
    }
  }

  @Test
  void disposerParameterIsAnnotatedDisposes_andClosesThePublisher() throws Exception {
    var m =
        StreamRuneProducers.class.getDeclaredMethod("closeSsePublisher", SseEventPublisher.class);
    var paramAnnotations = m.getParameterAnnotations()[0];
    assertTrue(
        Arrays.stream(paramAnnotations).anyMatch(a -> a.annotationType() == Disposes.class),
        "closeSsePublisher's SseEventPublisher parameter must be @Disposes so Arc closes it on"
            + " context shutdown");

    m.setAccessible(true);
    SseEventPublisher publisher = mock(SseEventPublisher.class);
    m.invoke(new StreamRuneProducers(), publisher);

    verify(publisher).close();
  }
}
