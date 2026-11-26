package org.streamrune.sqlite;

/**
 * Compiles a top-level read-model field to the SQLite expression that yields what PostgreSQL's
 * {@code data->>'field'} yields: the JSON value as text.
 *
 * <p><b>The expression.</b> {@code CASE json_type(data, P) WHEN 'text' THEN json_extract(data, P)
 * WHEN 'null' THEN NULL ELSE data -> P END}, where {@code P} is the field's JSON path:
 *
 * <ul>
 *   <li>a string: {@code json_extract}, which returns it unquoted and with its JSON escapes
 *       decoded, as {@code ->>} does;
 *   <li>a number, {@code true} or {@code false}: {@code ->}, which returns the JSON text as it is
 *       stored, so {@code 19.90} stays {@code 19.90} and a boolean is the word. {@code
 *       json_extract} would return a typed SQL value ({@code 19.9}, {@code 1}), which compares and
 *       sorts as a number. The repository writes numbers in the spelling PostgreSQL's {@code jsonb}
 *       prints them with, so this text is the one PostgreSQL returns;
 *   <li>JSON {@code null} and a missing key: SQL {@code NULL};
 *   <li>an object or an array: {@code ->}, its compact JSON text. PostgreSQL prints a {@code jsonb}
 *       container with a space after each comma and colon, and its keys in its own order, so a
 *       filter or a sort on a container field differs between the two.
 * </ul>
 *
 * The result is always TEXT or NULL, never a number, so it compares with a bound string and sorts
 * byte-wise under {@code COLLATE BINARY}: in UTF-8 that is code-point order, PostgreSQL's {@code
 * COLLATE "C"}.
 *
 * <p><b>The path is a literal.</b> SQLite allows no bound parameter in an index expression, and it
 * uses an index on an expression only for a query whose expression is the same text. So the path is
 * written into the statement as an SQL string literal, the same text in the index DDL and in every
 * read. It is escaped, never spliced raw: the key is wrapped in {@code $."..."}, a quotation mark
 * or a backslash in it is preceded by a backslash (SQLite's JSON path rules), and the path is put
 * in single quotes with each single quote doubled (SQL's rule). A key holding {@code .}, {@code [},
 * {@code $} or a space therefore addresses that exact key, never a nested path, and no key breaks
 * out of the literal.
 */
final class JsonFieldSql {

  private JsonFieldSql() {}

  /**
   * The expression yielding the text of the top-level JSON key {@code field} of the {@code data}
   * column, as PostgreSQL's {@code data->>'field'} does.
   *
   * @param field the JSON key, used exactly as given
   * @return the SQL expression
   * @throws IllegalArgumentException if {@code field} is {@code null} or holds U+0000
   */
  static String text(String field) {
    String path = literal(path(field));
    return "CASE json_type(data, "
        + path
        + ") WHEN 'text' THEN json_extract(data, "
        + path
        + ") WHEN 'null' THEN NULL ELSE data -> "
        + path
        + " END";
  }

  /**
   * The JSON path of the top-level key {@code field}: {@code $."field"} with each quotation mark
   * and backslash of the key preceded by a backslash.
   *
   * @param field the JSON key
   * @return the path, not yet an SQL literal
   * @throws IllegalArgumentException if {@code field} is {@code null} or holds U+0000
   */
  static String path(String field) {
    if (field == null) {
      throw new IllegalArgumentException("field is required");
    }
    if (field.indexOf('\0') >= 0) {
      throw new IllegalArgumentException("field cannot hold U+0000");
    }
    var path = new StringBuilder(field.length() + 5).append("$.\"");
    for (int i = 0; i < field.length(); i++) {
      char c = field.charAt(i);
      if (c == '"' || c == '\\') {
        path.append('\\');
      }
      path.append(c);
    }
    return path.append('"').toString();
  }

  /** {@code text} as an SQL string literal: in single quotes, each single quote doubled. */
  private static String literal(String text) {
    return "'" + text.replace("'", "''") + "'";
  }
}
