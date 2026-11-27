package org.streamrune.quarkus.it;

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
 * Records the virtual threads the startup observers started before the saga parallelism check runs,
 * for {@link QuarkusRefusedStartTest}: what the framework left running when that check refuses the
 * start.
 *
 * <p>Reads the threads first before every other startup observer and then after the framework's
 * runners started and after the application's observers at the default priority, one step before
 * the check ({@code Interceptor.Priority.LIBRARY_AFTER + 500}). It writes the difference, as {@code
 * tid=name} pairs, to the system property {@link #STARTED_THREADS}: a system property belongs to
 * the JVM, which the application under test shares with the test, whatever classloader each of them
 * runs in. It records nothing unless the configuration sets {@link #RECORD}.
 */
@ApplicationScoped
public class StartedThreadsProbe {

  /** The configuration key that switches the probe on. */
  static final String RECORD = "bootstrap-test.record-started-threads";

  /** The system property the started threads are written to. */
  static final String STARTED_THREADS = "bootstrap-test.started-threads";

  private Map<Long, String> before;

  public void beforeTheStartupObservers(
      @Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) StartupEvent event)
      throws IOException {
    if (recording()) {
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
    System.setProperty(
        STARTED_THREADS,
        started.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(",")));
  }

  private static boolean recording() {
    return ConfigProvider.getConfig().getOptionalValue(RECORD, Boolean.class).orElse(false);
  }
}
