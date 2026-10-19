package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.BackgroundRelayHealthContributor.Status;

/**
 * The forget channel of a cached crypto engine that listens for peers' forgets is reported as
 * {@code crypto-forget-listen}: DEGRADED while the replica does not hear them, UP while it does.
 * Without such an engine nothing new is reported.
 */
class CryptoForgetListenHealthRegistrarTest {

  /** A forget channel whose listening side the test switches. */
  static final class SwitchableForgetSignal implements CryptoForgetSignal {
    volatile boolean listening;

    @Override
    public void publish(String token) {}

    @Override
    public void subscribe(Consumer<String> onForget, Runnable onListening) {}

    @Override
    public boolean isSubscribed() {
      return true;
    }

    @Override
    public boolean isListening() {
      return listening;
    }
  }

  private static CryptoEngine cachedEngine(CryptoForgetSignal signal) {
    return CachedCryptoEngine.builder()
        .delegate(mock(CryptoEngine.class))
        .forgetSignal(signal)
        .build();
  }

  @Test
  void aListeningChannelReadsDegradedUntilTheReplicaHearsPeersForgets() {
    var signal = new SwitchableForgetSignal();
    var engine = cachedEngine(signal);
    var contributor = new BackgroundRelayHealthContributor();

    new CryptoForgetListenHealthRegistrar(List.of(engine), contributor)
        .registerCryptoForgetListenHealth();

    var component = contributor.components().getFirst();
    assertEquals("crypto-forget-listen", component.name());
    assertEquals(Status.DEGRADED, component.status());
    signal.listening = true;
    assertEquals(Status.UP, contributor.components().getFirst().status());
  }

  @Test
  void nothingIsReportedWithoutACryptoEngine() {
    var contributor = new BackgroundRelayHealthContributor();
    new CryptoForgetListenHealthRegistrar(List.of(), contributor)
        .registerCryptoForgetListenHealth();
    assertTrue(contributor.components().isEmpty());
  }

  @Test
  void nothingIsReportedForAnEngineWhoseChannelDoesNotListen() {
    var contributor = new BackgroundRelayHealthContributor();
    new CryptoForgetListenHealthRegistrar(
            List.of(cachedEngine(CryptoForgetSignal.NOOP), mock(CryptoEngine.class)), contributor)
        .registerCryptoForgetListenHealth();
    assertTrue(contributor.components().isEmpty());
  }

  /**
   * Compile-time bean fixture: a {@link CachedCryptoEngine} whose channel the test switches, and
   * the contributor the registrar reports into (runtime singletons are not reliably collected into
   * injected {@code List}s — see {@code SagaCompensationRetryLifecycleWiringTest}).
   */
  @Factory
  @Requires(property = "spec.name", value = "crypto-forget-listen-health")
  static class ListeningEngineFixture {
    static final SwitchableForgetSignal SIGNAL = new SwitchableForgetSignal();
    static final BackgroundRelayHealthContributor CONTRIBUTOR =
        new BackgroundRelayHealthContributor();

    @Singleton
    BackgroundRelayHealthContributor contributor() {
      return CONTRIBUTOR;
    }

    @Singleton
    CryptoEngine engine() {
      return cachedEngine(SIGNAL);
    }
  }

  /** The same fixture without an engine whose channel listens. */
  @Factory
  @Requires(property = "spec.name", value = "crypto-forget-listen-health-noop")
  static class NoopEngineFixture {
    static final BackgroundRelayHealthContributor CONTRIBUTOR =
        new BackgroundRelayHealthContributor();

    @Singleton
    BackgroundRelayHealthContributor contributor() {
      return CONTRIBUTOR;
    }

    @Singleton
    CryptoEngine engine() {
      return cachedEngine(CryptoForgetSignal.NOOP);
    }
  }

  /**
   * In a booted container the registrar is a bean, hears the startup event on its own, and reports
   * the channel of the engine bean — so a missing {@code @Singleton}, {@code @Requires} or
   * annotation-processing step shows here and not only in production.
   */
  @Test
  void aBootedContextReportsTheForgetChannelOfItsCryptoEngineBean() {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name", "crypto-forget-listen-health",
                "streamrune.gdpr.snapshot-purge", "none"))) {
      assertTrue(ctx.containsBean(CryptoForgetListenHealthRegistrar.class));
      var components = ListeningEngineFixture.CONTRIBUTOR.components();
      assertEquals(1, components.size(), "the startup event registered the channel once");
      assertEquals("crypto-forget-listen", components.getFirst().name());
      assertEquals(Status.DEGRADED, components.getFirst().status());
      ListeningEngineFixture.SIGNAL.listening = true;
      assertEquals(Status.UP, ListeningEngineFixture.CONTRIBUTOR.components().getFirst().status());
    }
  }

  @Test
  void aBootedContextWithAChannelThatDoesNotListenReportsNothing() {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name", "crypto-forget-listen-health-noop",
                "streamrune.gdpr.snapshot-purge", "none"))) {
      assertTrue(ctx.containsBean(CryptoForgetListenHealthRegistrar.class));
      assertTrue(NoopEngineFixture.CONTRIBUTOR.components().isEmpty());
    }
  }
}
