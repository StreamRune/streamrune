package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The write mutex on a machine with one or two processors, where the virtual-thread scheduler has
 * one or two carriers. A JVM's carrier count is fixed when it starts, so each case runs {@link
 * WriteStarvationChild} in a second JVM started with {@code
 * -Djdk.virtualThreadScheduler.parallelism}.
 *
 * <p>Without the mutex each waiting writer waits for SQLite's write lock inside the driver's native
 * call and keeps its carrier; with as many waiting writers as carriers, the batch whose updater
 * parked inside its transaction cannot be resumed, every writer fails after {@code busy_timeout} (5
 * s), and only then does the batch commit. With the mutex the writers park in Java, the batch
 * resumes when its updater's wait ends, and every writer commits right after it.
 */
class SqliteWriteMutexStarvationTest {

  /** How long the batch's updater parks inside its transaction. */
  static final long HOLD_MILLIS = 300;

  /**
   * The most any writer or the batch may take: well under the {@code busy_timeout} that a starved
   * writer waits out ({@link WriteStarvationChild#BUSY_MILLIS}), with room for a slow machine.
   */
  static final long PROMPT_MILLIS = WriteStarvationChild.BUSY_MILLIS / 2;

  @TempDir Path dir;

  @ParameterizedTest(name = "{0} carrier(s), {1} waiting writer(s)")
  @CsvSource({"1, 1", "1, 3", "2, 2", "2, 6"})
  @Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void writersBehindABatchThatParksCommitPromptlyOnFewCarriers(int carriers, int writers)
      throws Exception {
    Path db = dir.resolve("starved-" + carriers + "-" + writers + ".db");
    List<String> lines;
    try (ForkedJvm child =
        ForkedJvm.start(
            List.of(
                "-Djdk.virtualThreadScheduler.parallelism=" + carriers,
                "-Djdk.virtualThreadScheduler.maxPoolSize=" + carriers),
            WriteStarvationChild.class,
            db.toString(),
            Integer.toString(writers),
            Long.toString(HOLD_MILLIS))) {
      child.awaitLineStartingWith(WriteStarvationChild.DONE);
      assertThat(child.awaitExit()).isZero();
      lines = child.lines();
    }

    List<String> outcomes =
        lines.stream()
            .filter(
                line ->
                    line.startsWith(WriteStarvationChild.BATCH)
                        || line.startsWith(WriteStarvationChild.WRITER))
            .toList();
    assertThat(outcomes).as("the batch and every writer reported: %s", lines).hasSize(writers + 1);
    assertThat(outcomes)
        .as("nothing failed: %s", lines)
        .noneMatch(line -> line.contains(WriteStarvationChild.FAILED));
    for (String outcome : outcomes) {
      String[] words = outcome.split(" ");
      long millis = Long.parseLong(words[words.length - 1]);
      assertThat(millis)
          .as("'%s' did not wait out busy_timeout: %s", outcome, lines)
          .isLessThan(PROMPT_MILLIS);
    }
  }
}
