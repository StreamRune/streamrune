package org.streamrune.core.release;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Runs the repository's CI scripts ({@code .github/scripts}) and reads its workflow and release
 * files, for the tests of this package. The shell scripts run under {@code bash} with the commands
 * they call replaced by stand-ins the test writes, so nothing reaches the network.
 */
final class RepositoryScripts {

  private RepositoryScripts() {}

  /** The output and exit code of one script run. */
  record Result(int exitCode, String output) {}

  /** The repository root: the nearest directory above the working directory with the settings. */
  static Path repositoryRoot() {
    Path dir = Path.of("").toAbsolutePath();
    while (dir != null) {
      if (Files.exists(dir.resolve("settings.gradle.kts"))
          && Files.isDirectory(dir.resolve(".github"))) {
        return dir;
      }
      dir = dir.getParent();
    }
    throw new IllegalStateException(
        "cannot locate the repository root above " + Path.of("").toAbsolutePath());
  }

  /** The text of a repository file. */
  static String read(String relativePath) {
    try {
      return Files.readString(repositoryRoot().resolve(relativePath), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Writes an executable stand-in script {@code name} into {@code dir} and returns its path. */
  static Path executable(Path dir, String name, String body) {
    try {
      Path file = dir.resolve(name);
      Files.writeString(file, "#!/usr/bin/env bash\n" + body, StandardCharsets.UTF_8);
      Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rwxr-xr-x"));
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Runs the repository script {@code script} under {@code bash} in {@code workDir} with the extra
   * environment {@code env} and the arguments {@code args}. Skips the test where there is no bash.
   */
  static Result runScript(String script, Path workDir, Map<String, String> env, String... args) {
    return run("bash", script, workDir, env, args);
  }

  /**
   * Runs the repository's Python script {@code script} with {@code python3 -I} in {@code workDir}
   * with the arguments {@code args}. Skips the test where there is no python3.
   */
  static Result runPython(String script, Path workDir, String... args) {
    return run("python3", script, workDir, Map.of(), args);
  }

  private static Result run(
      String interpreter, String script, Path workDir, Map<String, String> env, String... args) {
    List<String> command = new ArrayList<>();
    command.add(interpreter);
    if (interpreter.equals("python3")) {
      command.add("-I");
    }
    command.add(repositoryRoot().resolve(script).toString());
    command.addAll(List.of(args));
    ProcessBuilder builder = new ProcessBuilder(command).directory(workDir.toFile());
    builder.redirectErrorStream(true);
    builder.environment().remove("GITHUB_STEP_SUMMARY");
    builder.environment().putAll(env);
    Process process;
    try {
      process = builder.start();
    } catch (IOException e) {
      assumeTrue(false, interpreter + " is not available: " + e.getMessage());
      throw new AssertionError(e);
    }
    try {
      process.getOutputStream().close();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (!process.waitFor(60, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new AssertionError(script + " did not finish within 60 seconds:\n" + output);
      }
      return new Result(process.exitValue(), output);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
