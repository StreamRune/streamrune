package org.streamrune.runtime.gdpr;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.streamrune.core.types.LogSanitizer;

/**
 * What the GDPR services call a pluggable component whose own {@code name()} cannot be used: a
 * {@link org.streamrune.core.gdpr.SubjectDataPurger} or a {@link
 * org.streamrune.core.gdpr.SubjectDataCollector} that throws from {@code name()} or returns {@code
 * null} or a blank string. One rule for both services, so an operator reads the same placeholder in
 * the erasure audit trail and in an export. Both services also refuse, at build time, two
 * components under one name ({@link #requireUnique}).
 *
 * <p>Package-private; internal collaborator of {@link ForgetSubjectService} and {@link
 * ExportSubjectDataService}.
 */
final class GdprNames {

  private GdprNames() {}

  /**
   * The class name of {@code component}: its simple name, or for an anonymous class (which has
   * none) its binary name without the package. Without the package because the sink caps a name at
   * {@link LogSanitizer#sanitizeForLog 64 characters} from the left, and a fully qualified name
   * would lose the end of it, the part that tells two components apart.
   *
   * <p>The result is returned as the class spells it. It goes through {@link
   * LogSanitizer#sanitizeForLog} wherever it is rendered into a log line, an audit summary or an
   * exception message; {@link #placeholder} applies that once up front, for a caller that also uses
   * the name as a key.
   */
  static String classNameOf(Object component) {
    Class<?> type = component.getClass();
    String simple = type.getSimpleName();
    if (!simple.isEmpty()) {
      return simple;
    }
    String binary = type.getName();
    return binary.substring(binary.lastIndexOf('.') + 1);
  }

  /**
   * {@link #classNameOf} rendered through {@link LogSanitizer#sanitizeForLog}: a name the service
   * made up, safe to use as a key and in every sink, however the class was named. A class name is a
   * plain Java identifier unless the class was generated or obfuscated, in which case the control,
   * line-separator, format and unpaired-surrogate characters in it are dropped and a name over 64
   * characters is cut.
   */
  static String placeholder(Object component) {
    return LogSanitizer.sanitizeForLog(classNameOf(component));
  }

  /**
   * Refuses two components under one name. {@code names.get(i)} is the name {@code
   * components.get(i)} goes by: its own {@code name()}, or the class name standing in for it, so
   * two instances of one class without a usable name share a name too. The message names every
   * shared name and the classes sharing it, all through {@link LogSanitizer#sanitizeForLog}, as the
   * exception is logged wherever startup fails.
   *
   * @param kind the component type, e.g. {@code "SubjectDataCollector"}
   * @param why what a shared name would break, completing "names must be unique: "
   * @throws IllegalArgumentException if two or more components share a name
   */
  static void requireUnique(String kind, String why, List<?> components, List<String> names) {
    Map<String, List<String>> classesByName = new LinkedHashMap<>();
    for (int i = 0; i < components.size(); i++) {
      classesByName
          .computeIfAbsent(names.get(i), name -> new ArrayList<>())
          .add(LogSanitizer.sanitizeForLog(classNameOf(components.get(i))));
    }
    String shared =
        classesByName.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(
                entry ->
                    "'"
                        + LogSanitizer.sanitizeForLog(entry.getKey())
                        + "' ("
                        + String.join(", ", entry.getValue())
                        + ")")
            .collect(Collectors.joining("; "));
    if (!shared.isEmpty()) {
      throw new IllegalArgumentException(
          kind
              + " names must be unique: "
              + why
              + ". Shared: "
              + shared
              + ". One without a usable name() goes by its class name, so two such instances of"
              + " one class share it; give each "
              + kind
              + " its own literal name().");
    }
  }
}
