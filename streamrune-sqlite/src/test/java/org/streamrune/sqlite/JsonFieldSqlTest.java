package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link JsonFieldSql} compiles a top-level JSON key to the SQLite expression that yields what
 * PostgreSQL's {@code data->>'key'} yields, addresses exactly that key whatever characters it
 * holds, and is an expression an index can be declared on.
 */
class JsonFieldSqlTest {

  @TempDir Path dir;

  private Connection conn;

  @BeforeEach
  void open() throws SQLException {
    conn = TestDatabases.dataSource(dir.resolve("json.db")).getConnection();
    execute("CREATE TABLE doc (id TEXT NOT NULL PRIMARY KEY, data TEXT NOT NULL) STRICT");
  }

  @AfterEach
  void close() throws SQLException {
    conn.close();
  }

  private void execute(String sql) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  private void insert(String id, String json) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO doc (id, data) VALUES (?, ?)")) {
      ps.setString(1, id);
      ps.setString(2, json);
      ps.executeUpdate();
    }
  }

  /** The expression's value on the row {@code id}; {@code null} for SQL NULL. */
  private String text(String id, String field) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT " + JsonFieldSql.text(field) + " FROM doc WHERE id = ?")) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("row %s", id).isTrue();
        Object value = rs.getObject(1);
        if (value != null) {
          assertThat(value).as("the value of %s is text or NULL", field).isInstanceOf(String.class);
        }
        return (String) value;
      }
    }
  }

  private List<String> explain(String sql, String... parameters) throws SQLException {
    var plan = new ArrayList<String>();
    try (PreparedStatement ps = conn.prepareStatement("EXPLAIN QUERY PLAN " + sql)) {
      for (int i = 0; i < parameters.length; i++) {
        ps.setString(i + 1, parameters[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          plan.add(rs.getString("detail"));
        }
      }
    }
    return plan;
  }

  /**
   * Keys with the characters that mean something in a JSON path ({@code .}, {@code "}, {@code [},
   * {@code $}, a space, a backslash) or in SQL ({@code '}) each read their own key, never a nested
   * path or a neighbour, and a key built to break out of the literal injects nothing.
   */
  @Test
  void fieldNamesWithPathMetacharactersAreLiteralKeys() throws SQLException {
    var keys = new LinkedHashMap<String, String>();
    keys.put("a.b", "dotted");
    keys.put("a\"b", "quoted");
    keys.put("a'b", "apostrophe");
    keys.put("$x", "dollar");
    keys.put("a[0]", "bracketed");
    keys.put("a b", "spaced");
    keys.put("a\\b", "backslashed");
    keys.put("a\\\"b", "backslash-quote");
    keys.put("\"", "lone-quote");
    keys.put("\\", "lone-backslash");
    keys.put("é😀", "non-ascii");
    keys.put("x') OR 1=1 --", "injection-1");
    keys.put("\" || '", "injection-2");
    // Decoys: what each key would read if its characters were taken as path syntax.
    String json =
        "{\"a\":{\"b\":\"nested\",\"[0]\":\"nested-bracket\"},\"$\":{\"x\":\"root\"},\"x\":\"x\","
            + "\"b\":\"b\","
            + "\"a.b\":\"dotted\",\"a\\\"b\":\"quoted\",\"a'b\":\"apostrophe\",\"$x\":\"dollar\","
            + "\"a[0]\":\"bracketed\",\"a b\":\"spaced\",\"a\\\\b\":\"backslashed\","
            + "\"a\\\\\\\"b\":\"backslash-quote\",\"\\\"\":\"lone-quote\","
            + "\"\\\\\":\"lone-backslash\",\"é😀\":\"non-ascii\","
            + "\"x') OR 1=1 --\":\"injection-1\",\"\\\" || '\":\"injection-2\"}";
    insert("k", json);
    insert("other", "{\"a.b\":\"another row\"}");

    for (Map.Entry<String, String> key : keys.entrySet()) {
      assertThat(text("k", key.getKey())).as("key %s", key.getKey()).isEqualTo(key.getValue());
    }
    // A filter on an injection key still selects one row by that key's value.
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT id FROM doc WHERE " + JsonFieldSql.text("x') OR 1=1 --") + " = ?")) {
      ps.setString(1, "injection-1");
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString(1)).isEqualTo("k");
        assertThat(rs.next()).as("only the row holding the key").isFalse();
      }
    }
    // Escaped, never raw: the apostrophe is doubled in the SQL literal, the quote and the
    // backslash are backslash-escaped in the path.
    assertThat(JsonFieldSql.text("a'b")).contains("'$.\"a''b\"'").doesNotContain("'$.\"a'b\"'");
    assertThat(JsonFieldSql.path("a\"b")).isEqualTo("$.\"a\\\"b\"");
    assertThat(JsonFieldSql.path("a\\b")).isEqualTo("$.\"a\\\\b\"");
    assertThat(JsonFieldSql.path("a.b")).isEqualTo("$.\"a.b\"");
  }

  /**
   * The expression yields what PostgreSQL's {@code ->>} yields for each JSON type: a string
   * unquoted and unescaped, a number in the spelling it is stored with, a boolean as {@code true}
   * or {@code false}; SQL NULL for JSON {@code null} and for a missing key, but the string {@code
   * "null"} as the text {@code null}.
   */
  @Test
  void theValueIsTheTextPostgresReturnsForEveryScalar() throws SQLException {
    insert("string", "{\"v\":\"alice\"}");
    insert("numeric-string", "{\"v\":\"5\"}");
    insert("integer", "{\"v\":5}");
    insert("big-integer", "{\"v\":123456789012345678901234567890}");
    insert("trailing-zero", "{\"v\":19.90}");
    insert("small", "{\"v\":0.00010}");
    insert("negative", "{\"v\":-3.50}");
    insert("true", "{\"v\":true}");
    insert("false", "{\"v\":false}");
    insert("null", "{\"v\":null}");
    insert("missing", "{\"w\":1}");
    insert("null-text", "{\"v\":\"null\"}");
    insert("empty", "{\"v\":\"\"}");
    insert("quote", "{\"v\":\"a\\\"b\"}");
    insert("backslash", "{\"v\":\"a\\\\b\"}");
    insert("escapes", "{\"v\":\"x\\n\\t\\r\\b\\f\\u00e9\\/\\u0001y\"}");
    insert("surrogates", "{\"v\":\"\\ud83d\\ude00\"}");
    insert("raw-non-ascii", "{\"v\":\"é～😀\"}");

    assertThat(text("string", "v")).isEqualTo("alice");
    assertThat(text("numeric-string", "v")).isEqualTo("5");
    assertThat(text("integer", "v")).isEqualTo("5");
    assertThat(text("big-integer", "v")).isEqualTo("123456789012345678901234567890");
    assertThat(text("trailing-zero", "v")).isEqualTo("19.90");
    assertThat(text("small", "v")).isEqualTo("0.00010");
    assertThat(text("negative", "v")).isEqualTo("-3.50");
    assertThat(text("true", "v")).isEqualTo("true");
    assertThat(text("false", "v")).isEqualTo("false");
    assertThat(text("null", "v")).isNull();
    assertThat(text("missing", "v")).isNull();
    assertThat(text("null-text", "v")).isEqualTo("null");
    assertThat(text("empty", "v")).isEmpty();
    assertThat(text("quote", "v")).isEqualTo("a\"b");
    assertThat(text("backslash", "v")).isEqualTo("a\\b");
    assertThat(text("escapes", "v")).isEqualTo("x\n\t\r\b\fé/\u0001y");
    assertThat(text("surrogates", "v")).isEqualTo("😀");
    assertThat(text("raw-non-ascii", "v")).isEqualTo("é～😀");
  }

  /**
   * The documented divergence: an object or an array is its compact JSON text, keys in their stored
   * order. PostgreSQL prints a jsonb container as {@code {"a": [true, null], "b": 1}} (a space
   * after each comma and colon, keys in jsonb's order), so a filter or a sort on a container field
   * differs between the two; {@code FieldEquals} promises nothing for containers.
   */
  @Test
  void aContainerIsItsCompactJsonText_notPostgresJsonbLayout() throws SQLException {
    insert("object", "{\"v\":{\"b\": 1, \"a\": [true, null]}}");
    insert("array", "{\"v\":[1, \"x\"]}");

    assertThat(text("object", "v")).isEqualTo("{\"b\":1,\"a\":[true,null]}");
    assertThat(text("array", "v")).isEqualTo("[1,\"x\"]");
  }

  /** Text compares byte-wise, which in UTF-8 is code-point order. */
  @Test
  void textOrdersByCodePoint() throws SQLException {
    for (String value : List.of("é", "z", "😀", "Z", "～", "a")) {
      insert(value, "{\"v\":\"" + value + "\"}");
    }
    var ordered = new ArrayList<String>();
    try (Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT id FROM doc ORDER BY " + JsonFieldSql.text("v") + " COLLATE BINARY")) {
      while (rs.next()) {
        ordered.add(rs.getString(1));
      }
    }
    assertThat(ordered).containsExactly("Z", "a", "z", "é", "～", "😀");
  }

  /**
   * An index declared on the expression serves a filter on it and an order by it: SQLite matches an
   * index on an expression only when the query's expression is the same text, which a bound path
   * parameter could never be.
   */
  @Test
  void expressionIsIndexable() throws SQLException {
    String customer = JsonFieldSql.text("customer");
    String createdAt = JsonFieldSql.text("createdAt");
    execute("CREATE INDEX doc_customer_ix ON doc (" + customer + ", " + createdAt + " DESC, id)");
    execute("CREATE INDEX doc_created_ix ON doc (" + createdAt + " DESC, id)");
    for (int i = 0; i < 50; i++) {
      insert("r" + i, "{\"customer\":\"c" + (i % 5) + "\",\"createdAt\":\"t" + i + "\"}");
    }
    execute("ANALYZE");

    List<String> filtered =
        explain(
            "SELECT id FROM doc WHERE "
                + customer
                + " = ? ORDER BY "
                + createdAt
                + " COLLATE BINARY DESC NULLS FIRST, id COLLATE BINARY",
            "c1");
    List<String> sorted =
        explain(
            "SELECT id FROM doc ORDER BY "
                + createdAt
                + " COLLATE BINARY DESC NULLS FIRST, id COLLATE BINARY LIMIT 10");

    assertThat(filtered)
        .anySatisfy(line -> assertThat(line).contains("INDEX doc_customer_ix (<expr>=?)"))
        .noneSatisfy(line -> assertThat(line).contains("TEMP B-TREE"));
    assertThat(sorted)
        .anySatisfy(line -> assertThat(line).contains("INDEX doc_created_ix"))
        .noneSatisfy(line -> assertThat(line).contains("TEMP B-TREE"));
  }

  /** A null key and a key holding U+0000, which no SQL text can carry, are refused. */
  @Test
  void aNullKeyAndAKeyHoldingUPlus0000AreRefused() {
    assertThatThrownBy(() -> JsonFieldSql.text(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("field is required");
    assertThatThrownBy(() -> JsonFieldSql.text("a\0b"))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("field cannot hold U+0000");
  }
}
