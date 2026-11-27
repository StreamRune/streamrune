package org.streamrune.quarkus.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A Quarkus application whose start the framework refuses after its runners started leaves none of
 * the framework's threads running.
 *
 * <p>The application is started as {@code quarkus-run.jar} would start it, in this JVM, with a
 * property the saga parallelism check refuses: a parallelism for a subscription no saga runner is
 * bound to. That check runs after the framework's runners and the leadership started, and Quarkus
 * fires no {@code ShutdownEvent} after a failed start: only the container's shutdown stops them. In
 * production the process exits; in dev mode and in a test JVM it does not, so any thread left
 * running would outlive the application. {@link StartedThreadsProbe} records the threads the
 * startup observers started before the check; each of them must end.
 */
@QuarkusMainTest
@TestProfile(QuarkusRefusedStartTest.RefusedStart.class)
@QuarkusTestResource(PostgresResource.class)
class QuarkusRefusedStartTest {

  private static final Duration SETTLE = Duration.ofSeconds(30);

  /** The refused property, and the probe switched on. */
  public static class RefusedStart implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "streamrune.saga.subscriptions.nobody.parallelism",
          "2",
          StartedThreadsProbe.RECORD,
          "true");
    }
  }

  @Test
  @Launch(
      value = {},
      exitCode = 1)
  void theRefusedStartLeavesNoThreadTheFrameworkStarted(LaunchResult result) throws Exception {
    assertThat(result.getOutput() + result.getErrorOutput()).contains("nobody");

    Map<Long, String> started = startedThreads();
    // The framework's runners had started when the check refused the start: the projection runner
    // runs both enabled projections, each on a thread of its own.
    assertThat(started.values()).contains("projection-orders", "projection-order-totals");

    long deadline = System.nanoTime() + SETTLE.toNanos();
    Map<Long, String> alive = alive(started);
    while (!alive.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(50);
      alive = alive(started);
    }
    assertThat(alive.values())
        .as(
            "threads still running after the refused start%s",
            VirtualThreads.stacks(alive.keySet()))
        .isEmpty();
  }

  /** The threads {@link StartedThreadsProbe} recorded, by thread id. */
  private static Map<Long, String> startedThreads() {
    String recorded = System.getProperty(StartedThreadsProbe.STARTED_THREADS);
    assertThat(recorded).as("the threads the probe recorded").isNotNull();
    Map<Long, String> threads = new HashMap<>();
    Arrays.stream(recorded.split(","))
        .filter(pair -> !pair.isEmpty())
        .map(pair -> pair.split("=", 2))
        .forEach(pair -> threads.put(Long.parseLong(pair[0]), pair[1]));
    return threads;
  }

  private static Map<Long, String> alive(Map<Long, String> threads) throws Exception {
    Map<Long, String> now = VirtualThreads.alive();
    Map<Long, String> alive = new HashMap<>(threads);
    alive.keySet().retainAll(now.keySet());
    return alive;
  }
}
