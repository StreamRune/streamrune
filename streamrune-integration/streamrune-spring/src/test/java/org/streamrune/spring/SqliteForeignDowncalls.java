package org.streamrune.spring;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import org.streamrune.sqlite.SqliteNativeDowncalls;

/**
 * The foreign downcalls of the SQLite backend's instance guard as a reachability-metadata document
 * registers them, read in the one-line form of {@link SqliteNativeDowncalls}.
 */
final class SqliteForeignDowncalls {

  /**
   * The lines the metadata must hold, derived from the descriptors the guard binds with, so a
   * changed descriptor fails the parity test until the metadata follows.
   */
  static final List<String> EXPECTED = SqliteNativeDowncalls.reachabilityMetadata();

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
