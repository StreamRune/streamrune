package org.streamrune.quarkus.it;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.interceptor.Interceptor;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Records the framework threads the startup observers started before the saga parallelism check
 * runs, for {@link QuarkusRefusedStartTest}: what the framework left running when that check
 * refuses the start.
 *
 * <p>Reads the virtual threads before every other startup observer, and again after the framework's
 * runners started and after the application's observers at the default priority, one step before
 * the check ({@code Interceptor.Priority.LIBRARY_AFTER + 500}). It writes the framework threads
 * among those that appeared, as {@code tid=name} pairs, to the system property {@link
 * #STARTED_THREADS}: a system property belongs to the JVM, which the application under test shares
 * with the test, whatever classloader each of them runs in. A start the check lets through is ended
 * at once with exit code 2, so the test fails instead of waiting for an application that runs. It
 * does nothing unless the configuration sets {@link #RECORD}.
 */
@ApplicationScoped
public class StartedThreadsProbe {

  /** The configuration key that switches the probe on. */
  static final String RECORD = "bootstrap-test.record-started-threads";

  /** The system property the started framework threads are written to. */
  static final String STARTED_THREADS = "bootstrap-test.started-threads";

  private Map<Long, String> before;

  public void beforeTheStartupObservers(
      @Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) StartupEvent event)
      throws IOException {
    if (recording()) {
      // A launch that records nothing leaves no value of an earlier one behind.
      System.clearProperty(STARTED_THREADS);
      before = VirtualThreads.alive();
    }
  }

  public void beforeTheSagaParallelismCheck(
      @Observes @Priority(Interceptor.Priority.LIBRARY_AFTER + 400) StartupEvent event)
      throws IOException {
    if (!recording()) {
      return;
    }
    Map<Long, String> started = new HashMap<>(VirtualThreads.alive());
    started.keySet().removeAll(before.keySet());
    started.values().removeIf(name -> !VirtualThreads.isFramework(name));
    System.setProperty(
        STARTED_THREADS,
        started.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(",")));
  }

  public void afterEveryStartupObserver(
      @Observes @Priority(Interceptor.Priority.PLATFORM_AFTER) StartupEvent event) {
    if (recording()) {
      Quarkus.asyncExit(2);
    }
  }

  private static boolean recording() {
    return ConfigProvider.getConfig().getOptionalValue(RECORD, Boolean.class).orElse(false);
  }
}
