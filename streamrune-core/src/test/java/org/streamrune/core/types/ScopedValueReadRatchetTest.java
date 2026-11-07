package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The framework reads the scoped values it binds through {@code
 * org.streamrune.core.ScopedValueReads} and never through {@link ScopedValue#get()} or {@link
 * ScopedValue#isBound()}: this ratchet fails the build when a production source calls either on one
 * of them, or declares a scoped value this list does not name.
 *
 * <p>Why: {@code get()} and {@code isBound()} answer from the JDK's per-thread cache of recent
 * reads before they walk the thread's bindings, and on GraalVM CE 25.0.1 with its Graal JIT a
 * compiled read of that cache on a virtual thread that was unmounted and remounted answers with
 * entries that are not the thread's bindings. {@code ScopedValueReads} walks the bindings every
 * time. These values decide authorization, the provenance of every stored event and the refusal of
 * a reserved correlation id, so none of them is read through the cache.
 *
 * <p>The scan reuses {@link LogSinkPolicyRatchetTest#productionSources()}, so it covers the tree
 * the log-sink ratchet covers, with the same Gradle input declaration. Comments and string literals
 * are blanked first ({@link LoggingFacadeRatchetTest#blankCommentsAndLiterals}).
 */
class ScopedValueReadRatchetTest {

  /** Every scoped value the production sources declare, by the name of its constant. */
  private static final Set<String> FRAMEWORK_SCOPED_VALUES =
      Set.of("CURRENT", "SAGA_OWNED", "CAUSE", "SAGA_FORWARD", "GENERATED", "DLQ_REPLAY");

  private static final Pattern CACHED_READ =
      Pattern.compile(
          "\\b(?:"
              + String.join("|", FRAMEWORK_SCOPED_VALUES)
              + ")\\s*\\.\\s*(?:get|isBound)\\s*\\(\\s*\\)");

  private static final Pattern DECLARATION =
      Pattern.compile(
          "\\bScopedValue\\s*<[^;=]*>\\s*(\\w+)\\s*=\\s*ScopedValue\\s*\\.\\s*newInstance");

  @Test
  void noProductionSourceReadsAFrameworkScopedValueThroughTheCache() {
    List<String> violations = new ArrayList<>();
    for (Path file : LogSinkPolicyRatchetTest.productionSources()) {
      violations.addAll(cachedReadsIn(file.toString(), LogSinkPolicyRatchetTest.readSource(file)));
    }
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " read(s) of a framework scoped value through ScopedValue.get() or isBound() in"
              + " production sources. Read it through org.streamrune.core.ScopedValueReads"
              + " (or StreamRuneContext.capture(), cause(), isSagaDispatch()):\n  "
              + String.join("\n  ", violations));
    }
  }

  @Test
  void everyScopedValueOfTheProductionSourcesIsNamedHere() {
    List<String> unknown = new ArrayList<>();
    for (Path file : LogSinkPolicyRatchetTest.productionSources()) {
      String code =
          LoggingFacadeRatchetTest.blankCommentsAndLiterals(
              String.join("\n", LogSinkPolicyRatchetTest.readSource(file)));
      Matcher m = DECLARATION.matcher(code);
      while (m.find()) {
        if (!FRAMEWORK_SCOPED_VALUES.contains(m.group(1))) {
          unknown.add(file.getFileName() + " " + m.group(1));
        }
      }
    }
    if (!unknown.isEmpty()) {
      fail(
          "scoped value(s) this ratchet does not name; add each to FRAMEWORK_SCOPED_VALUES so its"
              + " reads are checked too:\n  "
              + String.join("\n  ", unknown));
    }
  }

  @Test
  void theScannerRecognisesEveryCachedRead() {
    for (String bad :
        List.of(
            "if (StreamRuneContext.CURRENT.isBound()) {",
            "return CAUSE.get();",
            "var marker = SAGA_FORWARD . get ( );",
            "return GENERATED.isBound() ? GENERATED.get() : null;",
            "if (DLQ_REPLAY.isBound() && Boolean.TRUE.equals(DLQ_REPLAY.get())) {",
            "boolean owned = StreamRuneContext.SAGA_OWNED.isBound();")) {
      assertEquals(1, cachedReadsIn("Sample.java", List.of(bad)).size(), "must flag: " + bad);
    }
  }

  @Test
  void theScannerIgnoresCommentsLiteralsAndTheReadsThroughTheBindings() {
    List<String> source =
        List.of(
            "/** CURRENT.get() is not called here. */",
            "// CAUSE.isBound() neither",
            "String s = \"SAGA_OWNED.get()\";",
            "var context = ScopedValueReads.valueOrNull(StreamRuneContext.CURRENT);",
            "boolean bound = ScopedValueReads.isBound(CAUSE);",
            "Optional<String> other = cache.get();",
            "ScopedValue.where(CURRENT, context).call(() -> bus.execute(command));");
    assertEquals(List.of(), cachedReadsIn("Sample.java", source));
  }

  private static List<String> cachedReadsIn(String name, List<String> lines) {
    String code = LoggingFacadeRatchetTest.blankCommentsAndLiterals(String.join("\n", lines));
    List<String> out = new ArrayList<>();
    String[] codeLines = code.split("\n", -1);
    for (int i = 0; i < codeLines.length; i++) {
      if (CACHED_READ.matcher(codeLines[i]).find()) {
        out.add(Path.of(name).getFileName() + ":" + (i + 1) + " " + codeLines[i].strip());
      }
    }
    return out;
  }
}
