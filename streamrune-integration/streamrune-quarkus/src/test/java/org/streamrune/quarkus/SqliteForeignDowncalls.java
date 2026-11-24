package org.streamrune.quarkus;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;

/**
 * The foreign downcalls of the SQLite backend's instance guard ({@code
 * org.streamrune.sqlite.FlockFileLock}), as reachability metadata registers them: {@code open(path,
 * flags, mode)} (variadic from the third argument), {@code flock(fd, operation)} and {@code
 * close(fd)}, each capturing {@code errno}, and {@code strerror(errnum)}.
 */
final class SqliteForeignDowncalls {

  /** One line per descriptor: condition, return type, parameter types, options. */
  static final List<String> EXPECTED =
      List.of(
          "org.streamrune.sqlite.FlockFileLock jint [void*, jint, jint]"
              + " {\"firstVariadicArg\":2,\"captureCallState\":true}",
          "org.streamrune.sqlite.FlockFileLock jint [jint, jint] {\"captureCallState\":true}",
          "org.streamrune.sqlite.FlockFileLock jint [jint] {\"captureCallState\":true}",
          "org.streamrune.sqlite.FlockFileLock void* [jint] {}");

  private SqliteForeignDowncalls() {}

  /** The downcalls of a reachability-metadata document, in the form of {@link #EXPECTED}. */
  static List<String> of(JsonNode metadata) {
    List<String> downcalls = new ArrayList<>();
    for (JsonNode downcall : metadata.path("foreign").path("downcalls")) {
      List<String> parameters = new ArrayList<>();
      downcall.path("parameterTypes").forEach(type -> parameters.add(type.asText()));
      downcalls.add(
          downcall.path("condition").path("typeReached").asText()
              + " "
              + downcall.path("returnType").asText()
              + " "
              + parameters
              + " "
              + (downcall.has("options") ? downcall.get("options").toString() : "{}"));
    }
    return downcalls;
  }
}
