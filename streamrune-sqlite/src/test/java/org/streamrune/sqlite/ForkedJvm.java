package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A second JVM running {@link GuardChild} on the test class path. Every wait has a timeout, and
 * {@link #close()} kills a child that is still running.
 */
final class ForkedJvm implements AutoCloseable {

  static final Duration TIMEOUT = Duration.ofSeconds(60);

  /** Queued after the child's last output line, so that a wait ends when the child has exited. */
  private static final String END_OF_OUTPUT = new String("end of output");

  private final Process process;
  private final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
  private final List<String> output = new ArrayList<>();
  private final Writer stdin;

  private ForkedJvm(Process process) {
    this.process = process;
    this.stdin = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
    Thread reader =
        new Thread(
            () -> {
              try (BufferedReader out =
                  new BufferedReader(
                      new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = out.readLine()) != null) {
                  synchronized (output) {
                    output.add(line);
                  }
                  lines.add(line);
                }
              } catch (IOException e) {
                lines.add("reader failed: " + e);
              }
              lines.add(END_OF_OUTPUT);
            },
            "forked-jvm-reader");
    reader.setDaemon(true);
    reader.start();
  }

  /** Starts {@link GuardChild} with {@code mode} on {@code db}. */
  static ForkedJvm start(String mode, Path db) throws IOException {
    String java =
        ProcessHandle.current()
            .info()
            .command()
            .orElse(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    ProcessBuilder builder =
        new ProcessBuilder(
            java,
            "--enable-native-access=ALL-UNNAMED",
            "-cp",
            System.getProperty("java.class.path"),
            GuardChild.class.getName(),
            mode,
            db.toString());
    builder.redirectErrorStream(true);
    return new ForkedJvm(builder.start());
  }

  /**
   * Runs {@code probe} in a second JVM and returns its verdict line, starting with {@link
   * GuardChild#ACQUIRED} or {@link GuardChild#REFUSED}.
   */
  static String probe(Path db) throws Exception {
    try (ForkedJvm child = start("probe", db)) {
      String verdict = child.awaitLineStartingWith(GuardChild.ACQUIRED, GuardChild.REFUSED);
      child.awaitExit();
      return verdict;
    }
  }

  /** Waits for the first output line that starts with one of {@code prefixes}. */
  String awaitLineStartingWith(String... prefixes) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (true) {
      long left = deadline - System.nanoTime();
      String line = left > 0 ? lines.poll(left, TimeUnit.NANOSECONDS) : null;
      if (line == null || line == END_OF_OUTPUT) {
        return fail(
            "no line starting with %s within %s; output:%n%s",
            List.of(prefixes), TIMEOUT, output());
      }
      for (String prefix : prefixes) {
        if (line.startsWith(prefix)) {
          return line;
        }
      }
    }
  }

  /** Sends one command line to the child's standard input. */
  void send(String command) throws IOException {
    stdin.write(command + "\n");
    stdin.flush();
  }

  /** Waits for the child to exit and returns its exit code. */
  int awaitExit() throws InterruptedException {
    if (!process.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
      fail("the forked JVM did not exit within %s; output:%n%s", TIMEOUT, output());
    }
    return process.exitValue();
  }

  private String output() {
    synchronized (output) {
      return String.join(System.lineSeparator(), output);
    }
  }

  @Override
  public void close() throws InterruptedException {
    if (process.isAlive()) {
      process.destroyForcibly();
      process.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
  }
}
