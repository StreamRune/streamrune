import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/**
 * Tests {@link ApiSurfaceCheck} on small fixtures it compiles itself: the exposure rule, the
 * {@code @hidden} rule, the baseline difference and what the baseline records. Run by the root
 * build's {@code testApiSurfaceCheck} (part of {@code check}) with the source launcher, which
 * compiles {@code ApiSurfaceCheck.java} beside it on demand. Exits 1 on the first failed
 * expectation; argument: a scratch directory.
 */
public final class ApiSurfaceCheckTest {

  private static final String MARKER =
      """
      package org.streamrune.core;

      import java.lang.annotation.*;

      @Retention(RetentionPolicy.CLASS)
      @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
      public @interface Internal {}
      """;

  private static int checks;

  public static void main(String[] args) throws IOException {
    Path scratch = Path.of(args[0]);
    deleteTree(scratch);
    Files.createDirectories(scratch);
    exposureRule(scratch.resolve("exposure"));
    hiddenRule(scratch.resolve("hidden"));
    unresolvedReference(scratch.resolve("unresolved"));
    differenceAndRecording(scratch.resolve("difference"));
    System.out.println("ApiSurfaceCheckTest: " + checks + " expectations hold");
  }

  // ── The cases ──────────────────────────────────────────────────────────────────────────────

  /** A listed signature naming a type application code may not name is reported, with why. */
  private static void exposureRule(Path dir) throws IOException {
    Path classes =
        compile(
            dir,
            Map.of(
                "module-info.java",
                "module fx { exports org.streamrune.fx.api; exports org.streamrune.core; }",
                "org/streamrune/core/Internal.java",
                MARKER,
                "org/streamrune/fx/api/Secret.java",
                """
                package org.streamrune.fx.api;
                /** @hidden */
                @org.streamrune.core.Internal
                public final class Secret {}
                """,
                "org/streamrune/fx/hidden/Unexported.java",
                "package org.streamrune.fx.hidden; public class Unexported {}",
                "org/streamrune/fx/api/Base.java",
                "package org.streamrune.fx.api; class Base {}",
                "org/streamrune/fx/api/Leaky.java",
                "package org.streamrune.fx.api; public class Leaky extends Base {}",
                "org/streamrune/fx/api/Outer.java",
                """
                package org.streamrune.fx.api;
                public final class Outer {
                  protected static class Prot {}
                  public Prot prot() { return null; }
                }
                """,
                "org/streamrune/fx/api/Exposes.java",
                """
                package org.streamrune.fx.api;
                import java.util.List;
                import java.util.function.Supplier;
                public class Exposes {
                  public Exposes(Secret secret) {}
                  public List<? super Secret> wildcard() { return null; }
                  public <T extends Supplier<Secret[]>> void bound(T value) {}
                  public org.streamrune.fx.hidden.Unexported unexported() { return null; }
                }
                """));
    Result result = run(dir, "dump", classes, List.of(), List.of());
    expect(result.status == 1, "an exposure fails the run", result);
    String names = "names org.streamrune.fx.api.Secret, which is marked @Internal";
    expect(
        result.err.contains("<init>(org.streamrune.fx.api.Secret)\n      " + names),
        "a constructor parameter",
        result);
    expect(result.err.contains("wildcard()\n      " + names), "a wildcard bound", result);
    expect(result.err.contains("bound(T)\n      " + names), "a type-parameter bound", result);
    expect(
        result.err.contains(
            "names org.streamrune.fx.hidden.Unexported, whose package its module does not"
                + " export"),
        "a package the module does not export",
        result);
    expect(
        result.err.contains("names org.streamrune.fx.api.Base, which is not public"),
        "a package-private supertype",
        result);
    expect(
        result.err.contains("names org.streamrune.fx.api.Outer$Prot, which application code"),
        "a protected type nested in a final type",
        result);
    String api = Files.readString(dir.resolve("api.txt"));
    expect(
        !api.contains("class org.streamrune.fx.api.Secret"),
        "an @Internal type is left out",
        result);
    expect(
        !api.contains("class org.streamrune.fx.hidden.Unexported"),
        "an unexported type is left out",
        result);
  }

  /** Every @Internal without @hidden is found, in every spelling; another Internal is not. */
  private static void hiddenRule(Path dir) throws IOException {
    Path sources = dir.resolve("src");
    Path classes =
        compile(
            dir,
            Map.of(
                "org/streamrune/core/Internal.java",
                MARKER,
                "org/streamrune/fx/other/Internal.java",
                "package org.streamrune.fx.other; public @interface Internal {}",
                "org/streamrune/fx/WildcardImport.java",
                """
                package org.streamrune.fx;
                import org.streamrune.core.*;
                /** No tag. */
                @Internal
                public class WildcardImport {}
                """,
                "org/streamrune/fx/SameLine.java",
                """
                package org.streamrune.fx;
                import org.streamrune.core.Internal;
                public class SameLine {
                  /** No tag. */
                  @SuppressWarnings("unused") @Internal public void method() {}
                }
                """,
                "org/streamrune/fx/ModifierFirst.java",
                """
                package org.streamrune.fx;
                import org.streamrune.core.Internal;
                public class ModifierFirst {
                  /** No tag. */
                  public @Internal int field;
                }
                """,
                "org/streamrune/fx/Tagged.java",
                """
                package org.streamrune.fx;
                import org.streamrune.core.Internal;
                public class Tagged {
                  /**
                   * Tagged.
                   *
                   * @hidden
                   */
                  @Internal
                  public void method() {}
                }
                """,
                "org/streamrune/fx/OtherMarker.java",
                """
                package org.streamrune.fx;
                import org.streamrune.fx.other.Internal;
                /** Another framework's marker of the same simple name. */
                @Internal
                public class OtherMarker {}
                """));
    Result result = run(dir, "dump", classes, List.of(), List.of(sources));
    expect(result.status == 1, "a missing @hidden fails the run", result);
    expect(
        result.err.contains("org/streamrune/fx/WildcardImport.java:4 WildcardImport is marked"),
        "a class seen through a wildcard import",
        result);
    expect(
        result.err.contains("org/streamrune/fx/SameLine.java:5 method() is marked"),
        "a method with another annotation on the same line",
        result);
    expect(
        result.err.contains("org/streamrune/fx/ModifierFirst.java:5 field is marked"),
        "a field with the marker after a modifier",
        result);
    expect(!result.err.contains("Tagged.java"), "a tagged declaration passes", result);
    expect(!result.err.contains("OtherMarker"), "another framework's Internal is ignored", result);
    expect(!result.err.contains("source reading found"), "the counts agree", result);
  }

  /** A StreamRune type a listed signature names but the class path lacks fails the run. */
  private static void unresolvedReference(Path dir) throws IOException {
    Path classes =
        compile(
            dir,
            Map.of(
                "org/streamrune/lib/Gone.java",
                "package org.streamrune.lib; public class Gone {}",
                "org/streamrune/uses/UsesGone.java",
                """
                package org.streamrune.uses;
                public class UsesGone { public org.streamrune.lib.Gone gone() { return null; } }
                """));
    Files.delete(classes.resolve("org/streamrune/lib/Gone.class"));
    Result result = run(dir, "dump", classes, List.of(), List.of());
    expect(result.status == 1, "an unresolved reference fails the run", result);
    expect(
        result.err.contains(
            "names org.streamrune.lib.Gone, which is not on the compile class path"),
        "the unresolved type is named",
        result);
  }

  /** The baseline records annotation and enum details, and the difference is per type. */
  private static void differenceAndRecording(Path dir) throws IOException {
    Path before =
        compile(
            dir.resolve("before"),
            Map.of(
                "org/streamrune/d/R1.java",
                "package org.streamrune.d; public record R1(int a) {}",
                "org/streamrune/d/R2.java",
                "package org.streamrune.d; public record R2(int a) {}",
                "org/streamrune/d/Cfg.java",
                """
                package org.streamrune.d;
                import java.lang.annotation.*;
                @Retention(RetentionPolicy.RUNTIME)
                @Target({ElementType.TYPE, ElementType.METHOD})
                public @interface Cfg { int ttl() default 60; }
                """,
                "org/streamrune/d/Color.java",
                "package org.streamrune.d; public enum Color { RED, GREEN }",
                "org/streamrune/d/Op.java",
                """
                package org.streamrune.d;
                public enum Op {
                  PLUS { public int apply() { return 1; } };
                  public abstract int apply();
                }
                """,
                "org/streamrune/d/Holder.java",
                """
                package org.streamrune.d;
                public class Holder {
                  public class Inner { public Inner(int x) {} }
                  public void kept() {}
                }
                """));
    Path baseline = dir.resolve("api.txt");
    Result dump = run(dir, "dump", before, List.of(), List.of());
    expect(dump.status == 0, "a clean fixture dumps", dump);
    String api = Files.readString(baseline);
    expect(api.contains("  public abstract int ttl() default 60"), "an annotation default", dump);
    expect(
        api.contains(
            "@Retention(java.lang.annotation.RetentionPolicy.RUNTIME) @Target({java.lang"
                + ".annotation.ElementType.TYPE, java.lang.annotation.ElementType.METHOD})"
                + " public @interface org.streamrune.d.Cfg"),
        "the retention and targets of an annotation type",
        dump);
    expect(api.contains("  constants RED, GREEN"), "the order of enum constants", dump);
    expect(api.contains("public enum org.streamrune.d.Op\n"), "an enum with bodies, plain", dump);
    expect(!api.contains("Op$1"), "an enum constant body is not listed", dump);
    expect(
        api.contains("public class org.streamrune.d.Holder$Inner\n  public <init>(int)\n"),
        "an inner class constructor without the enclosing instance",
        dump);

    Files.writeString(baseline, api.replace("\n", "\r\n"));
    Result crlf = run(dir, "check", before, List.of(), List.of());
    expect(crlf.status == 0, "a baseline with CRLF line ends matches", crlf);
    Files.writeString(baseline, api);

    Path after =
        compile(
            dir.resolve("after"),
            Map.of(
                "org/streamrune/d/R1.java",
                "package org.streamrune.d; public record R1(int b) {}",
                "org/streamrune/d/R2.java",
                "package org.streamrune.d; public record R2(int a) {}",
                "org/streamrune/d/Cfg.java",
                """
                package org.streamrune.d;
                import java.lang.annotation.*;
                @Retention(RetentionPolicy.CLASS)
                @Target({ElementType.FIELD})
                public @interface Cfg { int ttl() default 300; }
                """,
                "org/streamrune/d/Color.java",
                "package org.streamrune.d; public enum Color { GREEN, RED }",
                "org/streamrune/d/Op.java",
                """
                package org.streamrune.d;
                public enum Op {
                  PLUS { public int apply() { return 1; } };
                  public abstract int apply();
                }
                """,
                "org/streamrune/d/Holder.java",
                """
                package org.streamrune.d;
                public class Holder {
                  public class Inner { public Inner(int x) {} }
                  public void kept() {}
                  public void added() {}
                }
                """));
    Result check = run(dir, "check", after, List.of(), List.of());
    expect(check.status == 1, "a changed API fails the check", check);
    int removed = check.err.indexOf("Removed or changed");
    int added = check.err.indexOf("Added:");
    expect(removed >= 0 && added > removed, "both sections, removed first", check);
    String removedPart = check.err.substring(removed, added);
    String addedPart = check.err.substring(added);
    expect(
        removedPart.contains("-   public int a()    (in org.streamrune.d.R1)"),
        "a removed member whose text another type still has",
        check);
    expect(removedPart.contains("default 60"), "a changed annotation default", check);
    expect(
        removedPart.contains("@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)"),
        "a changed retention",
        check);
    expect(removedPart.contains("constants RED, GREEN"), "a reordering of constants", check);
    expect(
        addedPart.contains("+   public void added()    (in org.streamrune.d.Holder)"),
        "an added member",
        check);
    expect(
        !check.err.contains("(in org.streamrune.d.R2)"), "an unchanged type is not listed", check);
  }

  // ── Support ────────────────────────────────────────────────────────────────────────────────

  private record Result(int status, String out, String err) {}

  private static Path compile(Path dir, Map<String, String> sources) throws IOException {
    Path src = dir.resolve("src");
    Path classes = dir.resolve("classes");
    List<Path> files = new ArrayList<>();
    for (Map.Entry<String, String> e : sources.entrySet()) {
      Path file = src.resolve(e.getKey());
      Files.createDirectories(file.getParent());
      Files.writeString(file, e.getValue());
      files.add(file);
    }
    Files.createDirectories(classes);
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    List<String> arguments = new ArrayList<>(List.of("-d", classes.toString(), "-proc:none"));
    files.forEach(f -> arguments.add(f.toString()));
    ByteArrayOutputStream errors = new ByteArrayOutputStream();
    int status = compiler.run(null, null, errors, arguments.toArray(String[]::new));
    if (status != 0) {
      throw new IllegalStateException("fixture does not compile:\n" + errors);
    }
    return classes;
  }

  private static Result run(
      Path dir, String mode, Path classes, List<Path> classpath, List<Path> sources)
      throws IOException {
    List<String> args =
        new ArrayList<>(
            List.of(
                "--mode", mode,
                "--module", ":fixture",
                "--baseline", dir.resolve("api.txt").toString(),
                "--report", dir.resolve("report.txt").toString(),
                "--classes", classes.toString()));
    classpath.forEach(c -> args.addAll(List.of("--classpath", c.toString())));
    sources.forEach(s -> args.addAll(List.of("--sources", s.toString())));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int status =
        ApiSurfaceCheck.run(
            args.toArray(String[]::new),
            new PrintStream(out, true, StandardCharsets.UTF_8),
            new PrintStream(err, true, StandardCharsets.UTF_8));
    return new Result(
        status, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
  }

  private static void expect(boolean holds, String what, Result result) {
    checks++;
    if (!holds) {
      System.err.println(
          "ApiSurfaceCheckTest: expectation failed: "
              + what
              + "\n--- exit "
              + result.status
              + "\n--- out\n"
              + result.out
              + "--- err\n"
              + result.err);
      System.exit(1);
    }
  }

  private static void deleteTree(Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(dir)) {
      for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(p);
      }
    }
  }
}
