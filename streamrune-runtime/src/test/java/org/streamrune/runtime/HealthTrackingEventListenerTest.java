package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.SubscriptionName;

class HealthTrackingEventListenerTest {

  @Test
  void shouldRecordSuccessOnSuccessfulDelivery() {
    EventListener delegate = mock(EventListener.class);
    var contributor = mock(SubscriptionHealthContributor.class);
    var listener =
        new HealthTrackingEventListener(delegate, contributor, SubscriptionName.of("orders"));

    listener.onEvents(List.of());

    verify(delegate).onEvents(List.of());
    verify(contributor).recordSuccess("orders");
    verify(contributor, never()).recordError(any());
  }

  @Test
  void shouldRecordErrorOnDeliveryFailure() {
    EventListener delegate = mock(EventListener.class);
    doThrow(new RuntimeException("fail")).when(delegate).onEvents(any());
    var contributor = mock(SubscriptionHealthContributor.class);
    var listener =
        new HealthTrackingEventListener(delegate, contributor, SubscriptionName.of("orders"));

    assertThrows(RuntimeException.class, () -> listener.onEvents(List.of()));

    verify(contributor).recordError("orders");
    verify(contributor, never()).recordSuccess(any());
  }

  @Test
  void shouldRethrowExceptionAfterRecording() {
    EventListener delegate = mock(EventListener.class);
    var expected = new IllegalStateException("broken");
    doThrow(expected).when(delegate).onEvents(any());
    var contributor = mock(SubscriptionHealthContributor.class);
    var listener =
        new HealthTrackingEventListener(delegate, contributor, SubscriptionName.of("orders"));

    var thrown = assertThrows(IllegalStateException.class, () -> listener.onEvents(List.of()));
    assertSame(expected, thrown);
  }
}
