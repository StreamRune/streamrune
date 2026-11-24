package org.streamrune.quarkus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * Another process that holds a SQLite file's instance guard: a forked JVM on this test class path
 * that opens a {@link SqliteDatabase} on the file and keeps it open until closed.
 */
final class SqliteGuardHolder implements AutoCloseable {

  private final Process process;

  private SqliteGuardHolder(Process process) {
    this.process = process;
  }

  /** Starts the holder on {@code file} and returns once it holds the guard. */
  static SqliteGuardHolder hold(Path file) throws IOException {
    String java = ProcessHandle.current().info().command().orElse("java");
    Process process =
        new ProcessBuilder(
                java,
                "--enable-native-access=ALL-UNNAMED",
                "-cp",
                System.getProperty("java.class.path"),
                SqliteGuardHolder.class.getName(),
                file.toString())
            .redirectErrorStream(true)
            .start();
    var holder = new SqliteGuardHolder(process);
    var out =
        new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    for (String line = out.readLine(); ; line = out.readLine()) {
      if (line == null) {
        holder.close();
        throw new IOException("the guard holder ended before it held the guard");
      }
      if (line.equals("HELD")) {
        return holder;
      }
    }
  }

  @Override
  public void close() {
    process.destroy();
    try {
      if (!process.waitFor(10, TimeUnit.SECONDS)) {
        process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Opens the database given as the only argument, prints {@code HELD} and waits until stdin closes
   * or the process is destroyed.
   *
   * @param args the database file
   * @throws IOException if stdin cannot be read
   */
  public static void main(String[] args) throws IOException {
    try (SqliteDatabase database = SqliteDatabase.open(SqliteDataSources.file(Path.of(args[0])))) {
      System.out.println("HELD");
      System.out.flush();
      while (System.in.read() >= 0) {
        // hold until stdin closes
      }
    }
  }
}
