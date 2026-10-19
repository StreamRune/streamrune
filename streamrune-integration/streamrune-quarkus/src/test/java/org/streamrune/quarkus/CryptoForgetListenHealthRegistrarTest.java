package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
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

    new CryptoForgetListenHealthRegistrar(instanceOf(engine), relayHealth(contributor))
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
    new CryptoForgetListenHealthRegistrar(instanceOf(), relayHealth(contributor))
        .registerCryptoForgetListenHealth();
    assertTrue(contributor.components().isEmpty());
  }

  @Test
  void nothingIsReportedForAnEngineWhoseChannelDoesNotListen() {
    var contributor = new BackgroundRelayHealthContributor();
    new CryptoForgetListenHealthRegistrar(
            instanceOf(cachedEngine(CryptoForgetSignal.NOOP), mock(CryptoEngine.class)),
            relayHealth(contributor))
        .registerCryptoForgetListenHealth();
    assertTrue(contributor.components().isEmpty());
  }

  @SuppressWarnings("unchecked")
  private static Instance<CryptoEngine> instanceOf(CryptoEngine... engines) {
    Instance<CryptoEngine> instance = mock(Instance.class);
    when(instance.iterator()).thenAnswer(invocation -> List.of(engines).iterator());
    return instance;
  }

  @SuppressWarnings("unchecked")
  private static Instance<BackgroundRelayHealthContributor> relayHealth(
      BackgroundRelayHealthContributor contributor) {
    Instance<BackgroundRelayHealthContributor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.get()).thenReturn(contributor);
    return instance;
  }

  @Test
  @SuppressWarnings("unchecked")
  void registersNothingWithoutAHealthContributor() {
    Instance<BackgroundRelayHealthContributor> none = mock(Instance.class);
    when(none.isUnsatisfied()).thenReturn(true);
    var registrar =
        new CryptoForgetListenHealthRegistrar(
            instanceOf(cachedEngine(new SwitchableForgetSignal())), none);
    registrar.registerCryptoForgetListenHealth();
  }

  @AfterEach
  void resetTheSwitchedChannel() {
    ListeningEngineBeans.SIGNAL.listening = false;
  }

  /** Beans of a container: an engine whose channel the test switches, and the contributor. */
  @ApplicationScoped
  public static class ListeningEngineBeans {
    static final SwitchableForgetSignal SIGNAL = new SwitchableForgetSignal();
    static final BackgroundRelayHealthContributor CONTRIBUTOR =
        new BackgroundRelayHealthContributor();

    @Produces
    @Singleton
    public CryptoEngine engine() {
      return cachedEngine(SIGNAL);
    }

    @Produces
    @Singleton
    public BackgroundRelayHealthContributor contributor() {
      return CONTRIBUTOR;
    }
  }

  /** The same, with an engine whose channel does not listen. */
  @ApplicationScoped
  public static class NoopEngineBeans {
    static final BackgroundRelayHealthContributor CONTRIBUTOR =
        new BackgroundRelayHealthContributor();

    @Produces
    @Singleton
    public CryptoEngine engine() {
      return cachedEngine(CryptoForgetSignal.NOOP);
    }

    @Produces
    @Singleton
    public BackgroundRelayHealthContributor contributor() {
      return CONTRIBUTOR;
    }
  }

  /**
   * In a booted Arc container the registrar is a bean and observes the startup event: it reports
   * the channel of the engine bean, so a missing scope, observer or producer wiring shows here.
   */
  @Test
  void aBootedContainerReportsTheForgetChannelOfItsCryptoEngineBeanOnStartup() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                ListeningEngineBeans.class,
                CryptoForgetListenHealthRegistrar.class,
                CryptoEngine.class,
                BackgroundRelayHealthContributor.class))) {
      assertTrue(
          ListeningEngineBeans.CONTRIBUTOR.components().isEmpty(),
          "nothing is reported before the application starts");

      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());

      var components = ListeningEngineBeans.CONTRIBUTOR.components();
      assertEquals(1, components.size());
      assertEquals("crypto-forget-listen", components.getFirst().name());
      assertEquals(Status.DEGRADED, components.getFirst().status());
      ListeningEngineBeans.SIGNAL.listening = true;
      assertEquals(Status.UP, ListeningEngineBeans.CONTRIBUTOR.components().getFirst().status());
    }
  }

  @Test
  void aBootedContainerWithAChannelThatDoesNotListenReportsNothing() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                NoopEngineBeans.class,
                CryptoForgetListenHealthRegistrar.class,
                CryptoEngine.class,
                BackgroundRelayHealthContributor.class))) {
      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());

      assertTrue(NoopEngineBeans.CONTRIBUTOR.components().isEmpty());
    }
  }
}
