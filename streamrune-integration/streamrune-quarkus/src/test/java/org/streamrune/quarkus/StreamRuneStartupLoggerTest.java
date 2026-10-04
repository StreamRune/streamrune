package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.inject.Instance;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.Projection;

class StreamRuneStartupLoggerTest {

  @SuppressWarnings("unchecked")
  private <T> Instance<T> resolvable(T value) {
    Instance<T> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(value);
    return instance;
  }

  @SuppressWarnings("unchecked")
  private <T> Instance<T> unresolvable() {
    Instance<T> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(false);
    return instance;
  }

  @SuppressWarnings("unchecked")
  private Instance<Projection> projections(Projection... beans) {
    Instance<Projection> instance = mock(Instance.class);
    when(instance.stream()).thenReturn(Stream.of(beans));
    return instance;
  }

  @Test
  void logsSummaryWithAllComponentsPresent() {
    var logger =
        new StreamRuneStartupLogger(
            TestProperties.defaults(),
            resolvable(mock(EventStore.class)),
            resolvable(mock(CryptoEngine.class)),
            projections(mock(Projection.class), mock(Projection.class)));
    assertDoesNotThrow(() -> logger.onStart(new StartupEvent()));
  }

  @Test
  void logsSummaryWithNothingPresent() {
    var logger =
        new StreamRuneStartupLogger(
            TestProperties.defaults(), unresolvable(), unresolvable(), projections());
    assertDoesNotThrow(() -> logger.onStart(new StartupEvent()));
  }

  @Test
  void skipsLoggingWhenDisabled() {
    Instance<EventStore> eventStore = unresolvable();
    Instance<CryptoEngine> crypto = unresolvable();
    Instance<Projection> projections = projections();
    var logger =
        new StreamRuneStartupLogger(
            TestProperties.of(Map.of("streamrune.startup-log", "false")),
            eventStore,
            crypto,
            projections);
    logger.onStart(new StartupEvent());
    verifyNoInteractions(eventStore, crypto, projections);
  }
}
