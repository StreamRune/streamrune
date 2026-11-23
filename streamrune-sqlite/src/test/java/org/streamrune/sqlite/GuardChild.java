package org.streamrune.sqlite;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * The program a test runs in a second JVM, so that the guard meets another process as it does in
 * production.
 *
 * <ul>
 *   <li>{@code probe <db>} opens a {@link SqliteDatabase} on the file, prints {@code ACQUIRED} and
 *       closes it, or prints {@code REFUSED} and the guard's message.
 *   <li>{@code hold <db>} opens a {@link SqliteDatabase}, prints {@code READY} and reads commands
 *       from standard input: {@code close} closes the handle and exits, {@code halt} stops the JVM
 *       at once with {@link Runtime#halt(int)}, without closing anything, as a crash does. The end
 *       of standard input halts it too, so a parent that dies leaves no holder behind.
 * </ul>
 */
public final class GuardChild {

  static final String ACQUIRED = "ACQUIRED";
  static final String REFUSED = "REFUSED";
  static final String READY = "READY";
  static final String CLOSED = "CLOSED";

  private GuardChild() {}

  public static void main(String[] args) throws Exception {
    Path db = Path.of(args[1]);
    if (args[0].equals("probe")) {
      try (SqliteDatabase ignored = SqliteDatabase.open(TestDatabases.dataSource(db))) {
        print(ACQUIRED);
      } catch (SqliteGuardException e) {
        print(REFUSED + " " + e.getMessage());
      }
      return;
    }
    SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db));
    print(READY);
    BufferedReader in =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    String command = in.readLine();
    if ("close".equals(command)) {
      database.close();
      print(CLOSED);
      return;
    }
    Runtime.getRuntime().halt(3);
  }

  private static void print(String line) {
    System.out.println(line);
    System.out.flush();
  }
}
