package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.test.InMemoryProjectionRepository;

@ExtendWith(MockitoExtension.class)
class CacheAwareProjectionTest {

  @Mock private Projection delegate;
  @Mock private CacheInvalidator invalidator;

  // -------------------------------------------------------------------------
  // 1. process_callsDelegateAndInvalidator_inOrder
  // -------------------------------------------------------------------------

  @Test
  void process_callsDelegateAndInvalidator_inOrder() {
    CacheAwareProjection cap = new CacheAwareProjection(delegate, invalidator);
    List<EventEnvelope> batch = List.of();

    cap.process(batch);

    InOrder order = inOrder(delegate, invalidator);
    order.verify(delegate).process(batch);
    order.verify(invalidator).onEventsProcessed(batch);
    order.verifyNoMoreInteractions();
  }

  // -------------------------------------------------------------------------
  // 2. process_delegateThrows_invalidatorNotCalled
  // -------------------------------------------------------------------------

  @Test
  void process_delegateThrows_invalidatorNotCalled() {
    doThrow(new RuntimeException("boom")).when(delegate).process(List.of());
    CacheAwareProjection cap = new CacheAwareProjection(delegate, invalidator);

    assertThrows(RuntimeException.class, () -> cap.process(List.of()));

    verify(invalidator, never()).onEventsProcessed(List.of());
  }

  // -------------------------------------------------------------------------
  // 3. process_emptyBatch_callsBoth
  // -------------------------------------------------------------------------

  @Test
  void process_emptyBatch_callsBoth() {
    CacheAwareProjection cap = new CacheAwareProjection(delegate, invalidator);
    List<EventEnvelope> emptyBatch = List.of();

    cap.process(emptyBatch);

    verify(delegate).process(emptyBatch);
    verify(invalidator).onEventsProcessed(emptyBatch);
  }

  // -------------------------------------------------------------------------
  // 4. nullDelegate_throws
  // -------------------------------------------------------------------------

  @Test
  void nullDelegate_throws() {
    NullPointerException ex =
        assertThrows(NullPointerException.class, () -> new CacheAwareProjection(null, invalidator));
    assertTrue(ex.getMessage().contains("delegate"), "message must mention 'delegate'");
  }

  // -------------------------------------------------------------------------
  // 5. nullInvalidator_throws
  // -------------------------------------------------------------------------

  @Test
  void nullInvalidator_throws() {
    NullPointerException ex =
        assertThrows(NullPointerException.class, () -> new CacheAwareProjection(delegate, null));
    assertTrue(ex.getMessage().contains("invalidator"), "message must mention 'invalidator'");
  }

  // -------------------------------------------------------------------------
  // 6. writesThroughRepository forwards to the delegate
  // -------------------------------------------------------------------------

  @Test
  void writesThroughRepository_forwardsTrueFromDelegate() {
    when(delegate.writesThroughRepository()).thenReturn(true);
    var cap = new CacheAwareProjection(delegate, invalidator);
    assertTrue(cap.writesThroughRepository());
  }

  @Test
  void writesThroughRepository_forwardsFalseFromDelegate() {
    when(delegate.writesThroughRepository()).thenReturn(false);
    var cap = new CacheAwareProjection(delegate, invalidator);
    assertFalse(cap.writesThroughRepository());
  }

  @Test
  void writeTarget_isForwardedToTheDelegate() {
    var repo = new InMemoryProjectionRepository();
    var writeThrough =
        new BaseProjection(repo, "orders") {
          @Override
          public void process(List<EventEnvelope> batch) {}
        };
    assertThat(new CacheAwareProjection(writeThrough, invalidator).writeTarget())
        .containsSame(repo);

    Projection lambda = batch -> {};
    assertThat(new CacheAwareProjection(lambda, invalidator).writeTarget()).isEmpty();
  }
}
