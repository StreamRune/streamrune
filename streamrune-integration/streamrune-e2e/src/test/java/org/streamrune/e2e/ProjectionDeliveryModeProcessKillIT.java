package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.e2e.ProjectionDeliveryModeChildMain.Count;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.runtime.PollingProjectionRunner;

/**
 * A real process dies between the read-model write and the checkpoint. A child JVM ({@link
 * ProjectionDeliveryModeChildMain}) runs one batch of three events and halts at the crash point;
 * the parent reads the durable state the crash left, reruns the same projection in-process under
 * the same mode with no crash point, and asserts the read model and the checkpoint.
 *
 * <table>
 * <caption>Crash points pinned, and the convergence each case proves</caption>
 * <tr><th>mode</th><th>crash point</th><th>durable state at the crash</th><th>rerun</th></tr>
 * <tr><td>TRANSACTIONAL_LOCAL</td><td>after the write, before COMMIT</td><td>no read-model
 * row, checkpoint at the start</td><td>applies once: count 3, checkpoint at the top</td></tr>
 * <tr><td>TRANSACTIONAL_LOCAL</td><td>in the after-commit hook</td><td>count 3, checkpoint at
 * the top</td><td>nothing to apply: count 3</td></tr>
 * <tr><td>AT_LEAST_ONCE_IDEMPOTENT</td><td>after the autocommit write, before the checkpoint
 * save</td><td>count 3, checkpoint at the start</td><td>re-applies the batch: count 6 (pinned — an
 * idempotent upsert would converge to the same row), checkpoint at the top</td></tr>
 * <tr><td>AT_LEAST_ONCE_IDEMPOTENT</td><td>after the checkpoint save</td><td>count 3, checkpoint at
 * the top</td><td>nothing to apply: count 3</td></tr>
 * </table>
 */
class ProjectionDeliveryModeProcessKillIT extends E2ETestBase {

  @ParameterizedTest(name = "{0} / {1}")
  @CsvSource({
    "TRANSACTIONAL_LOCAL, AFTER_WRITE, 0, 3",
    "TRANSACTIONAL_LOCAL, AFTER_COMMIT, 3, 3",
    "AT_LEAST_ONCE_IDEMPOTENT, AFTER_WRITE, 3, 6",
    "AT_LEAST_ONCE_IDEMPOTENT, AFTER_COMMIT, 3, 3"
  })
  void childCrashes_parentRerunConverges(
      String mode, String crashPoint, int appliedAfterCrash, int appliedAfterRerun)
      throws Exception {
    var name =
        ProjectionName.of(
            "kill_" + mode.toLowerCase(Locale.ROOT) + "_" + crashPoint.toLowerCase(Locale.ROOT));
    var store = eventStore(dataSource);
    var top =
        appendCountEvents(
                store,
                StreamId.of(AggregateType.of("kill"), AggregateId.of("kill-stream")),
                0,
                0,
                3)
            .getLast();

    var java = ProcessHandle.current().info().command().orElseThrow();
    // The child's output goes to a file under build/ and into the assertion messages, so a failing
    // child's stack trace reaches the test report.
    var childLogDir = Files.createDirectories(Path.of("build", "tmp", "process-kill-it"));
    var childLog = childLogDir.resolve(name.value() + ".log");
    var child =
        new ProcessBuilder(
                java,
                "-cp",
                System.getProperty("java.class.path"),
                ProjectionDeliveryModeChildMain.class.getName(),
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword(),
                mode,
                crashPoint,
                name.value())
            .redirectErrorStream(true)
            .redirectOutput(childLog.toFile())
            .start();
    try {
      boolean exited = child.waitFor(90, TimeUnit.SECONDS);
      assertThat(exited).as("the child must exit; its output:%n%s", childOutput(childLog)).isTrue();
      assertThat(child.exitValue())
          .as("the child halted at the crash point; its output:%n%s", childOutput(childLog))
          .isEqualTo(ProjectionDeliveryModeChildMain.HALTED);
    } finally {
      // A child left running after a timeout or a failed assertion would keep its PostgreSQL
      // connections, and possibly the checkpoint row lock, into the next test.
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor(30, TimeUnit.SECONDS);
      }
    }

    var repo = new JdbcProjectionRepository(dataSource);
    var offsets = new PostgresOffsetStore(dataSource);
    assertThat(appliedCount(repo, name))
        .as("durable read model right after the crash")
        .isEqualTo(appliedAfterCrash);
    assertThat(offsets.getLastOffset(name))
        .as("durable checkpoint right after the crash")
        .isEqualTo(crashPoint.equals("AFTER_COMMIT") ? top : GlobalOffset.initial());

    // Rerun, in-process, same mode, no crash point.
    var m = ProjectionDeliveryMode.valueOf(mode);
    AtomicBatchProcessor processor =
        m == ProjectionDeliveryMode.TRANSACTIONAL_LOCAL
            ? repo
            : AtomicBatchProcessor.nonAtomicAtLeastOnce();
    new PollingProjectionRunner(store, offsets, 100, 100, processor)
        .run(name, ProjectionDeliveryModeChildMain.rerunProjection(m, repo, name), m);

    assertThat(appliedCount(repo, name)).as("after the rerun").isEqualTo(appliedAfterRerun);
    assertThat(offsets.getLastOffset(name)).as("checkpoint after the rerun").isEqualTo(top);
  }

  private static String childOutput(Path childLog) throws IOException {
    return Files.exists(childLog) ? Files.readString(childLog, StandardCharsets.UTF_8) : "<none>";
  }

  /**
   * The applied count of the one row, 0 when there is none. {@code findById} creates the {@code
   * <name>_view} table on first use, so a crash before any committed write reads as "no row".
   */
  private static int appliedCount(JdbcProjectionRepository repo, ProjectionName name) {
    return repo.findById(name, "row", Count.class).map(Count::applied).orElse(0);
  }
}
