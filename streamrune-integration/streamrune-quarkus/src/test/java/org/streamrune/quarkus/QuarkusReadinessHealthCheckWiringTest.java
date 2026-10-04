package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.health.AsyncHealthCheckFactory;
import io.smallrye.health.SmallRyeHealthReporter;
import io.smallrye.health.api.Wellness;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.json.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;
import org.eclipse.microprofile.health.Startup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.runtime.BackgroundRelayHealthContributor;

/**
 * The StreamRune readiness check must sit in {@code /q/health/ready} beside every other readiness
 * check of the application — driven through a real Arc container ({@link RealArcTestContainer}) and
 * the real SmallRye Health reporter that serves the endpoint.
 *
 * <p>The reporter collects its checks through {@code @Readiness Instance<HealthCheck>} (and {@code
 * /q/health} through {@code @Any Instance<HealthCheck>}). Arc resolves an {@code Instance}
 * iteration through the same ambiguity resolution as a single-valued injection point, which drops
 * every {@code @DefaultBean} candidate as soon as one non-default candidate exists. Declared
 * {@code @DefaultBean}, the StreamRune check therefore vanished from both endpoints in every
 * deployment that has any other readiness check — the datasource check Quarkus registers by default
 * is one — and a dead background relay never turned readiness {@code DOWN}.
 *
 * <p>The reporter is booted the way Quarkus runs it: with {@code
 * io.smallrye.health.delayChecksInitializations=true} (a runtime default the Quarkus health
 * extension installs), so the per-check switches the Quarkus recorder hands over apply when the
 * checks are first collected. Its package is defined by the container's own classloader (see {@link
 * RealArcTestContainer#bootIsolatingPackages}), so the test reaches it reflectively.
 */
class QuarkusReadinessHealthCheckWiringTest {

  private static final String DELAY_CHECKS = "io.smallrye.health.delayChecksInitializations";

  private String previousDelay;

  @BeforeEach
  void delayCheckInitializationLikeQuarkus() {
    previousDelay = System.setProperty(DELAY_CHECKS, "true");
  }

  @AfterEach
  void restoreDelay() {
    if (previousDelay == null) {
      System.clearProperty(DELAY_CHECKS);
    } else {
      System.setProperty(DELAY_CHECKS, previousDelay);
    }
  }

  @Test
  void theStreamRuneCheckStaysInReadinessBesideAnotherReadinessCheck() throws Exception {
    try (var arc = bootHealth()) {
      Reporter reporter = Reporter.of(arc);

      JsonObject readiness = reporter.report("getReadiness");

      assertThat(checkNames(readiness))
          .as("readiness payload %s", readiness)
          .containsExactlyInAnyOrder("streamrune", "database");
      assertThat(readiness.getString("status"))
          .as("a dead background relay must turn /q/health/ready DOWN")
          .isEqualTo("DOWN");
      assertThat(checkNames(reporter.report("getHealth"))).contains("streamrune", "database");
    }
  }

  @Test
  void theQuarkusPerCheckSwitchStillTurnsTheStreamRuneCheckOff() throws Exception {
    try (var arc = bootHealth()) {
      Reporter reporter = Reporter.of(arc);
      // What the Quarkus health recorder hands the reporter for
      // quarkus.smallrye-health.check."org.streamrune.quarkus.StreamRuneHealthCheck".enabled=false
      Map<String, Boolean> checkSwitches = new HashMap<>();
      checkSwitches.put(StreamRuneHealthCheck.class.getName(), false);
      reporter.call("setHealthChecksConfigs", Map.class, checkSwitches);

      JsonObject readiness = reporter.report("getReadiness");

      assertThat(checkNames(readiness)).containsExactly("database");
      assertThat(readiness.getString("status")).isEqualTo("UP");
    }
  }

  private static RealArcTestContainer bootHealth() throws Exception {
    return RealArcTestContainer.bootIsolatingPackages(
        List.of(
            StreamRuneHealthCheck.class,
            DatabaseReadinessCheck.class,
            DeadRelayConfig.class,
            SmallRyeHealthReporter.class,
            AsyncHealthCheckFactory.class,
            Readiness.class,
            Liveness.class,
            Startup.class,
            Wellness.class),
        Set.of(SmallRyeHealthReporter.class.getPackageName()));
  }

  private static List<String> checkNames(JsonObject payload) {
    return payload.getJsonArray("checks").stream()
        .map(check -> ((JsonObject) check).getString("name"))
        .toList();
  }

  /** The container's SmallRye Health reporter, reached through the container's own class. */
  private record Reporter(Class<?> type, Object instance) {

    static Reporter of(RealArcTestContainer arc) throws ClassNotFoundException {
      Class<?> type =
          Class.forName(SmallRyeHealthReporter.class.getName(), true, arc.deploymentClassLoader());
      return new Reporter(type, arc.container().select(type).get());
    }

    Object call(String method, Class<?> parameterType, Object argument) throws Exception {
      return type.getMethod(method, parameterType).invoke(instance, argument);
    }

    /** Runs one of the reporter's {@code SmallRyeHealth get...()} methods and returns its JSON. */
    JsonObject report(String method) throws Exception {
      Object health = type.getMethod(method).invoke(instance);
      return (JsonObject) health.getClass().getMethod("getPayload").invoke(health);
    }
  }

  /** Stands in for the datasource readiness check Quarkus registers by default. */
  @Readiness
  @ApplicationScoped
  public static class DatabaseReadinessCheck implements HealthCheck {
    @Override
    public HealthCheckResponse call() {
      return HealthCheckResponse.named("database").up().build();
    }
  }

  /** A started outbox relay whose poll thread has died. */
  public static class DeadRelayConfig {
    @Produces
    @Singleton
    public BackgroundRelayHealthContributor deadRelay() {
      return new BackgroundRelayHealthContributor() {
        @Override
        public Status overallStatus() {
          return Status.DOWN;
        }

        @Override
        public List<ComponentHealth> components() {
          return List.of(new ComponentHealth("outbox-relay", Status.DOWN, true, false, 0));
        }
      };
    }
  }
}
