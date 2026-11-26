package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.File;
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
 * A second JVM running {@link GuardChild}, or another test program, on the test class path. Every
 * wait has a timeout, and {@link #close()} kills a child that is still running.
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

  /** Starts {@link GuardChild} with {@code mode} on {@code db}, with native access enabled. */
  static ForkedJvm start(String mode, Path db) throws IOException {
    return start(
        List.of("--enable-native-access=ALL-UNNAMED"),
        System.getProperty("java.class.path"),
        mode,
        db);
  }

  /**
   * Starts {@link GuardChild} with {@code mode} on {@code db} in a JVM that denies native access to
   * the class path, where StreamRune runs, and allows it to the sqlite-jdbc driver alone: the
   * driver connects, and the instance guard cannot bind {@code flock}. The driver is loaded as a
   * named module for that, and slf4j-api with it, because a named module cannot read the class
   * path.
   */
  static ForkedJvm startWithNativeAccessDenied(String mode, Path db) throws IOException {
    List<String> modules = new ArrayList<>();
    List<String> rest = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      String name = Path.of(entry).getFileName().toString();
      (name.startsWith("sqlite-jdbc") || name.startsWith("slf4j-api") ? modules : rest).add(entry);
    }
    return start(
        List.of(
            "--illegal-native-access=deny",
            "--module-path",
            String.join(File.pathSeparator, modules),
            "--add-modules",
            "org.xerial.sqlitejdbc,org.slf4j",
            "--enable-native-access=org.xerial.sqlitejdbc"),
        String.join(File.pathSeparator, rest),
        mode,
        db);
  }

  private static ForkedJvm start(List<String> jvmArgs, String classPath, String mode, Path db)
      throws IOException {
    return start(jvmArgs, classPath, GuardChild.class, List.of(mode, db.toString()));
  }

  /** Starts {@code main} with {@code args} on the test class path, with native access enabled. */
  static ForkedJvm start(Class<?> main, String... args) throws IOException {
    return start(
        List.of("--enable-native-access=ALL-UNNAMED"),
        System.getProperty("java.class.path"),
        main,
        List.of(args));
  }

  private static ForkedJvm start(
      List<String> jvmArgs, String classPath, Class<?> main, List<String> args) throws IOException {
    String java =
        ProcessHandle.current()
            .info()
            .command()
            .orElse(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    List<String> command = new ArrayList<>();
    command.add(java);
    command.addAll(jvmArgs);
    command.addAll(List.of("-cp", classPath, main.getName()));
    command.addAll(args);
    ProcessBuilder builder = new ProcessBuilder(command);
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
      if (!process.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
        fail("the forked JVM did not stop within %s after it was killed", TIMEOUT);
      }
    }
  }
}
