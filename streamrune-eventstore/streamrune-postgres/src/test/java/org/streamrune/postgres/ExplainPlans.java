package org.streamrune.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Reads a statement's plan as {@code EXPLAIN (FORMAT JSON)} and walks it. Shared by the plan pins,
 * which seed enough rows, run {@code VACUUM ANALYZE} and disable the scans that would hide the
 * index on a small table before asking the planner.
 */
final class ExplainPlans {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ExplainPlans() {}

  /** Runs {@code EXPLAIN (FORMAT JSON) sql} on {@code conn} and returns the root {@code Plan}. */
  static JsonNode explainJson(Connection conn, String sql) throws SQLException {
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery("EXPLAIN (FORMAT JSON) " + sql)) {
      if (!rs.next()) {
        throw new IllegalStateException("EXPLAIN returned no row");
      }
      try {
        return MAPPER.readTree(rs.getString(1)).get(0).get("Plan");
      } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
        throw new IllegalStateException("EXPLAIN returned unreadable JSON", e);
      }
    }
  }

  /** Depth-first search for an Index Scan / Index Only Scan on {@code indexName}. */
  static JsonNode findIndexScan(JsonNode node, String indexName) {
    if (node == null) return null;
    String type = node.path("Node Type").asText("");
    if ((type.equals("Index Scan") || type.equals("Index Only Scan"))
        && indexName.equals(node.path("Index Name").asText(""))) {
      return node;
    }
    for (JsonNode child : node.path("Plans")) {
      JsonNode found = findIndexScan(child, indexName);
      if (found != null) return found;
    }
    return null;
  }

  /** True when any node of the plan, depth first over {@code Plans}, has {@code nodeType}. */
  static boolean containsNodeType(JsonNode node, String nodeType) {
    if (node == null) return false;
    if (nodeType.equals(node.path("Node Type").asText(""))) {
      return true;
    }
    for (JsonNode child : node.path("Plans")) {
      if (containsNodeType(child, nodeType)) return true;
    }
    return false;
  }
}
