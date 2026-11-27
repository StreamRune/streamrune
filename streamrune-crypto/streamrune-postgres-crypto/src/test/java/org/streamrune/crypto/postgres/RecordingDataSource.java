package org.streamrune.crypto.postgres;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

/**
 * In-memory DataSource for unit testing — uses dynamic proxies for JDBC types.
 *
 * <p>SQL-aware: statements are routed by their {@code WHERE}/{@code FROM} clause so the {@code
 * encryption_keys} and {@code forgotten_subjects} tables are modelled separately, and each query
 * filters by the bound {@code subject_id} parameter like the real database does.
 *
 * <p>Transactional the way pgjdbc is: {@code setAutoCommit(false)} opens a transaction, {@code
 * commit()} keeps its writes and {@code rollback()} restores the state it started from. Switching
 * {@code setAutoCommit(true)} on an open transaction COMMITS it, which is the pgjdbc behaviour
 * behind both crash points. {@code close()} on an open transaction discards it, as the server does
 * when the session ends or the process dies, and so does {@code abort()}, after which the
 * connection is closed: pgjdbc throws SQLState 08003 from it. {@link #failBefore}/{@link
 * #failAfter} throw an {@link Error} at a chosen statement, and {@link #failNextRollback} makes the
 * next rollback throw, for the crash-point tests. {@link #returnConnectionsLikeAgroal} wraps each
 * connection the way the Agroal pool does (see {@link #agroalPooled}).
 *
 * <p>Recording only, never changing what a statement does: {@link #sqlSeen} lists every prepared
 * statement in order, {@link #keyBytesHandedOut} every {@code key_bytes} array a result set handed
 * to the engine, and {@link #keyBytesBound} every array the engine bound with {@code setBytes} (the
 * caller's own reference, before the fake copies it), so a test can check that the engine zeroes
 * its key buffers after use. {@link #calls} is the transcript of every physical connection —
 * opened, autoCommit switched, statement prepared, committed, rolled back, aborted, closed — and
 * {@link #statements} tags each prepared statement with its connection and autoCommit state, so a
 * test can tell one transaction from statements spread over several connections (each connection's
 * rollback restores a global snapshot, so the table contents alone cannot).
 */
final class RecordingDataSource implements DataSource {
  private final Map<String, byte[]> keys = new HashMap<>();
  // Generation of each stored key (the key_version column; every insert binds it).
  private final Map<String, Integer> keyVersions = new HashMap<>();
  // erased_key_generations: highest generation a completed erasure destroyed, per subject.
  private final Map<String, Integer> erasedGenerations = new HashMap<>();
  private final Set<String> forgotten = new HashSet<>();
  private final java.util.List<String> sqlSeen = new java.util.ArrayList<>();
  private final java.util.List<byte[]> keyBytesHandedOut = new java.util.ArrayList<>();
  private final java.util.List<byte[]> keyBytesBound = new java.util.ArrayList<>();
  private final java.util.List<Call> calls = new java.util.ArrayList<>();
  private final java.util.List<PreparedSql> statements = new java.util.ArrayList<>();
  private int connectionsOpened;

  /** One recorded call on physical connection number {@code connection} (1-based). */
  record Call(int connection, String call) {}

  /** A prepared statement, the connection that prepared it and whether autoCommit was on. */
  record PreparedSql(int connection, boolean autoCommit, String sql) {}

  private Runnable beforeInsert = () -> {};
  // Crash-point injection, one-shot so the rerun that proves convergence runs clean: the Error
  // thrown by the next statement whose SQL contains failSqlFragment, before or after it executes.
  private String failSqlFragment;
  private boolean failAfterExecute;
  private Error injectedError;
  private Throwable rollbackFailure;
  private java.sql.SQLException abortFailure;
  private boolean agroalReturn;
  private int physicalAborts;

  /** The committed-or-open content of the four tables, for a transaction's rollback point. */
  private record State(
      Map<String, byte[]> keys,
      Map<String, Integer> keyVersions,
      Map<String, Integer> erasedGenerations,
      Set<String> forgotten) {}

  State snapshot() {
    return new State(
        new HashMap<>(keys),
        new HashMap<>(keyVersions),
        new HashMap<>(erasedGenerations),
        new HashSet<>(forgotten));
  }

  void restore(State state) {
    keys.clear();
    keys.putAll(state.keys());
    keyVersions.clear();
    keyVersions.putAll(state.keyVersions());
    erasedGenerations.clear();
    erasedGenerations.putAll(state.erasedGenerations());
    forgotten.clear();
    forgotten.addAll(state.forgotten());
  }

  /** Every SQL string the engine prepared, in order — used to assert the configured table name. */
  java.util.List<String> sqlSeen() {
    return sqlSeen;
  }

  /**
   * The connection transcript, in order: {@code getConnection}, {@code setAutoCommit(<flag>)},
   * {@code prepare <sql>}, {@code commit}, {@code rollback}, {@code abort}, {@code close}.
   */
  java.util.List<Call> calls() {
    return calls;
  }

  /** Every prepared statement with its connection and autoCommit state, in order. */
  java.util.List<PreparedSql> statements() {
    return statements;
  }

  /** Forgets everything recorded so far (not the tables); connection numbers keep counting. */
  void resetRecording() {
    sqlSeen.clear();
    keyBytesHandedOut.clear();
    keyBytesBound.clear();
    calls.clear();
    statements.clear();
  }

  /** Every {@code key_bytes} array {@code getBytes} handed to the engine, in order. */
  java.util.List<byte[]> keyBytesHandedOut() {
    return keyBytesHandedOut;
  }

  /** Every array the engine passed to {@code setBytes}, as the engine's own reference, in order. */
  java.util.List<byte[]> keyBytesBound() {
    return keyBytesBound;
  }

  // The engine binds the SHA-256 hex of the subject id to the subject_id columns, so the fake's
  // maps are keyed by that hash too. These convenience methods accept the natural (raw) subject
  // id and hash it, so callers seed/inspect with plain ids exactly as before.
  void putKey(String subjectId, byte[] keyBytes) {
    keys.put(sha256Hex(subjectId), keyBytes);
    keyVersions.put(sha256Hex(subjectId), 1); // a first mint is generation 1
  }

  boolean containsKey(String subjectId) {
    return keys.containsKey(sha256Hex(subjectId));
  }

  byte[] getKey(String subjectId) {
    return keys.get(sha256Hex(subjectId));
  }

  boolean isForgotten(String subjectId) {
    return forgotten.contains(sha256Hex(subjectId));
  }

  /** The subject's erased_key_generations value, or {@code null} when it has no row. */
  Integer erasedGeneration(String subjectId) {
    return erasedGenerations.get(sha256Hex(subjectId));
  }

  /** Whether {@code literal} appears verbatim as a stored subject_id column value (any table). */
  boolean storesColumnLiteral(String literal) {
    return keys.containsKey(literal) || forgotten.contains(literal);
  }

  /** Hook executed right before an INSERT runs — simulates a concurrent writer. */
  void beforeInsert(Runnable hook) {
    this.beforeInsert = hook;
  }

  /** The next statement containing {@code sqlFragment} throws {@code error} instead of running. */
  void failBefore(String sqlFragment, Error error) {
    arm(sqlFragment, false, error);
  }

  /** The next statement containing {@code sqlFragment} runs, then throws {@code error}. */
  void failAfter(String sqlFragment, Error error) {
    arm(sqlFragment, true, error);
  }

  private void arm(String sqlFragment, boolean afterExecute, Error error) {
    this.failSqlFragment = sqlFragment;
    this.failAfterExecute = afterExecute;
    this.injectedError = error;
  }

  /** The next {@code rollback()} throws {@code failure} and leaves the transaction open. */
  void failNextRollback(Throwable failure) {
    this.rollbackFailure = failure;
  }

  /** The next {@code abort()} throws {@code failure} and leaves the connection as it was. */
  void failNextAbort(java.sql.SQLException failure) {
    this.abortFailure = failure;
  }

  void maybeFailAbort() throws java.sql.SQLException {
    if (abortFailure != null) {
      java.sql.SQLException failure = abortFailure;
      abortFailure = null;
      throw failure;
    }
  }

  /** From now on, every connection is handed out behind an Agroal-style pooled wrapper. */
  void returnConnectionsLikeAgroal() {
    this.agroalReturn = true;
  }

  /** How many physical connections were aborted. */
  int physicalAborts() {
    return physicalAborts;
  }

  void maybeFail(String sql, boolean afterExecute) {
    if (injectedError != null
        && failAfterExecute == afterExecute
        && sql.contains(failSqlFragment)) {
      Error error = injectedError;
      injectedError = null;
      throw error;
    }
  }

  void maybeFailRollback() throws Throwable {
    if (rollbackFailure != null) {
      Throwable failure = rollbackFailure;
      rollbackFailure = null;
      throw failure;
    }
  }

  @Override
  public Connection getConnection() {
    Connection physical = fakeConnection(this);
    return agroalReturn ? agroalPooled(physical) : physical;
  }

  @Override
  public Connection getConnection(String username, String password) {
    return getConnection();
  }

  @Override
  public java.io.PrintWriter getLogWriter() {
    return null;
  }

  @Override
  public void setLogWriter(java.io.PrintWriter out) {}

  @Override
  public void setLoginTimeout(int seconds) {}

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public java.util.logging.Logger getParentLogger() {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  private static Connection fakeConnection(RecordingDataSource ds) {
    int id = ++ds.connectionsOpened;
    ds.calls.add(new Call(id, "getConnection"));
    // Per-connection transaction state (see RecordingDataSource): whether autoCommit is off, and
    // the state the open transaction started from.
    boolean[] manualCommit = {false};
    RecordingDataSource.State[] txStart = {null};
    boolean[] closed = {false};
    InvocationHandler h =
        (proxy, method, args) -> {
          // Recording only: the transcript entry is written before the call runs.
          switch (method.getName()) {
            case "setAutoCommit" -> ds.calls.add(new Call(id, "setAutoCommit(" + args[0] + ")"));
            case "prepareStatement" -> {
              ds.calls.add(new Call(id, "prepare " + args[0]));
              ds.statements.add(new PreparedSql(id, !manualCommit[0], (String) args[0]));
            }
            case "commit", "rollback", "abort", "close" ->
                ds.calls.add(new Call(id, method.getName()));
            default -> {}
          }
          if (closed[0] && CLOSED_CONNECTION_REFUSES.contains(method.getName())) {
            throw new java.sql.SQLException("This connection has been closed.", "08003");
          }
          switch (method.getName()) {
            case "unwrap" -> {
              Class<?> iface = (Class<?>) args[0];
              if (iface.isInstance(proxy)) {
                return proxy;
              }
              throw new java.sql.SQLException("Cannot unwrap to " + iface.getName());
            }
            case "isWrapperFor" -> {
              return ((Class<?>) args[0]).isInstance(proxy);
            }
            case "isClosed" -> {
              return closed[0];
            }
            case "abort" -> {
              // pgjdbc closes the socket; the server discards the open transaction.
              ds.maybeFailAbort();
              ((java.util.concurrent.Executor) args[0])
                  .execute(
                      () -> {
                        if (!closed[0]) {
                          ds.physicalAborts++;
                          if (manualCommit[0]) {
                            ds.restore(txStart[0]);
                          }
                          manualCommit[0] = false;
                          txStart[0] = null;
                          closed[0] = true;
                        }
                      });
              return null;
            }
            case "prepareStatement" -> {
              return fakePreparedStatement((String) args[0], ds);
            }
            case "getAutoCommit" -> {
              return !manualCommit[0];
            }
            case "setAutoCommit" -> {
              boolean autoCommit = (Boolean) args[0];
              if (autoCommit == manualCommit[0]) {
                // A real change. Turning autoCommit ON commits the open transaction (pgjdbc):
                // its writes are already in the maps, so only the rollback point is dropped.
                manualCommit[0] = !autoCommit;
                txStart[0] = autoCommit ? null : ds.snapshot();
              }
              return null;
            }
            case "commit" -> {
              if (manualCommit[0]) {
                txStart[0] = ds.snapshot();
              }
              return null;
            }
            case "rollback" -> {
              ds.maybeFailRollback();
              if (manualCommit[0]) {
                ds.restore(txStart[0]);
              }
              return null;
            }
            case "close" -> {
              // Ending the session discards an open transaction.
              if (manualCommit[0]) {
                ds.restore(txStart[0]);
                manualCommit[0] = false;
                txStart[0] = null;
              }
              return null;
            }
            default -> {
              return null;
            }
          }
        };
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, h);
  }

  /** The calls a closed pgjdbc connection refuses with SQLState 08003. */
  private static final Set<String> CLOSED_CONNECTION_REFUSES =
      Set.of("prepareStatement", "getAutoCommit", "setAutoCommit", "commit", "rollback");

  /**
   * Agroal 3.0's pooled connection wrapper, as far as these tests need it. Calls reach the physical
   * connection and {@code unwrap} returns it. {@code close()} returns the connection to the pool
   * the way Agroal does: a changed autoCommit is reset with {@code setAutoCommit(true)}, with NO
   * rollback first ({@code ConnectionHandler.resetConnection}), and a {@code SQLException} there
   * only becomes a pool warning. On an open transaction that reset COMMITS it (pgjdbc). The
   * wrapper's {@code abort()} only marks the wrapper closed, as Agroal's does: it never reaches the
   * physical connection.
   */
  private static Connection agroalPooled(Connection physical) {
    boolean[] autoCommitDirty = {false};
    boolean[] wrapperClosed = {false};
    InvocationHandler h =
        (proxy, method, args) -> {
          switch (method.getName()) {
            case "close" -> {
              if (!wrapperClosed[0]) {
                wrapperClosed[0] = true;
                if (autoCommitDirty[0]) {
                  try {
                    physical.setAutoCommit(true);
                  } catch (java.sql.SQLException _) {
                    // Agroal fires a warning; a fatal SQLState then makes it destroy the
                    // connection.
                  }
                }
              }
              return null;
            }
            case "abort" -> {
              wrapperClosed[0] = true;
              return null;
            }
            case "setAutoCommit" -> {
              if (physical.getAutoCommit() != (Boolean) args[0]) {
                autoCommitDirty[0] = true;
              }
              physical.setAutoCommit((Boolean) args[0]);
              return null;
            }
            default -> {
              try {
                return method.invoke(physical, args);
              } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
              }
            }
          }
        };
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, h);
  }

  private static PreparedStatement fakePreparedStatement(String sql, RecordingDataSource ds) {
    ds.sqlSeen.add(sql);
    Map<Integer, Object> params = new HashMap<>();

    InvocationHandler h =
        (proxy, method, args) -> {
          String name = method.getName();
          if ("setString".equals(name) || "setInt".equals(name)) {
            params.put((Integer) args[0], args[1]);
            return null;
          } else if ("setBytes".equals(name)) {
            // Real drivers copy the bytes to the wire; the engine zeroes its buffer afterwards,
            // so the fake must not keep a reference to the caller's array (it only records it).
            ds.keyBytesBound.add((byte[]) args[1]);
            params.put((Integer) args[0], ((byte[]) args[1]).clone());
            return null;
          } else if ("executeUpdate".equals(name)) {
            ds.maybeFail(sql, false);
            int updated = executeUpdate(sql, params, ds);
            ds.maybeFail(sql, true);
            return updated;
          } else if ("executeQuery".equals(name)) {
            ResultSet rs = executeQuery(sql, params, ds);
            params.clear();
            return rs;
          } else if ("close".equals(name)) {
            return null;
          }
          return null;
        };

    return (PreparedStatement)
        Proxy.newProxyInstance(
            PreparedStatement.class.getClassLoader(), new Class<?>[] {PreparedStatement.class}, h);
  }

  private static int executeUpdate(
      String sql, Map<Integer, Object> params, RecordingDataSource ds) {
    String subjectId = (String) params.get(1);
    if (sql.startsWith("DELETE")) {
      params.clear();
      // Faithful row counts, as PostgreSQL reports them.
      if (sql.contains("forgotten_subjects")) {
        return ds.forgotten.remove(subjectId) ? 1 : 0;
      }
      ds.keyVersions.remove(subjectId);
      return ds.keys.remove(subjectId) != null ? 1 : 0;
    } else if (sql.startsWith("INSERT")) {
      if (sql.contains("forgotten_subjects")) {
        params.clear();
        return ds.forgotten.add(subjectId) ? 1 : 0;
      }
      if (sql.contains("erased_key_generations")) {
        // INSERT ... ON CONFLICT DO UPDATE SET max_erased_version = GREATEST(existing, new).
        int erased = (Integer) params.get(2);
        params.clear();
        ds.erasedGenerations.merge(subjectId, erased, Math::max);
        return 1;
      }
      ds.beforeInsert.run();
      byte[] keyBytes = (byte[]) params.get(2);
      Integer version = (Integer) params.get(3);
      params.clear();
      if (sql.contains("DO NOTHING")) {
        // Postgres ON CONFLICT DO NOTHING: 0 rows affected on conflict.
        if (ds.keys.putIfAbsent(subjectId, keyBytes) == null) {
          ds.keyVersions.put(subjectId, version);
          return 1;
        }
        return 0;
      }
      ds.keys.put(subjectId, keyBytes);
      ds.keyVersions.put(subjectId, version);
      return 1;
    }
    params.clear();
    return 0;
  }

  private static ResultSet executeQuery(
      String sql, Map<Integer, Object> params, RecordingDataSource ds) {
    String subjectId = (String) params.get(1);
    if (sql.contains("pg_advisory_xact_lock")) {
      // The fake models a single in-process connection, so there is no real concurrency for the
      // advisory lock to serialize against — just hand back a one-row result so the driver-side
      // rs.next() the engine performs succeeds, mirroring what Postgres itself would return.
      return fakeRowResultSet(true, null, null);
    }
    if (sql.contains("forgotten_subjects")) {
      // SELECT 1 FROM forgotten_subjects WHERE subject_id = ?
      return fakeRowResultSet(ds.forgotten.contains(subjectId), null, null);
    }
    if (sql.contains("max_erased_version")) {
      // SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?
      Integer erased = ds.erasedGenerations.get(subjectId);
      return fakeRowResultSet(erased != null, null, erased);
    }
    if (sql.contains("key_bytes")) {
      // SELECT key_bytes, key_version FROM encryption_keys WHERE subject_id = ?
      byte[] key = ds.keys.get(subjectId);
      return fakeRowResultSet(
          key != null, key, ds.keyVersions.get(subjectId), ds.keyBytesHandedOut);
    }
    if (sql.contains("key_version")) {
      // SELECT key_version FROM encryption_keys WHERE subject_id = ?  (deleteKey)
      boolean present = ds.keys.containsKey(subjectId);
      return fakeRowResultSet(present, null, ds.keyVersions.get(subjectId));
    }
    // SELECT 1 FROM encryption_keys WHERE subject_id = ?  (isKeyAvailable)
    return fakeRowResultSet(ds.keys.containsKey(subjectId), null, null);
  }

  /**
   * A single-row-or-empty ResultSet. {@code present} controls whether {@code next()} yields one
   * row; {@code keyBytes} (when non-null) is what {@code getBytes("key_bytes")} returns on that
   * row; {@code intValue} (when non-null) is what {@code getInt(...)} returns for any column.
   */
  private static ResultSet fakeRowResultSet(boolean present, byte[] keyBytes, Integer intValue) {
    return fakeRowResultSet(present, keyBytes, intValue, new java.util.ArrayList<>());
  }

  /** As above, recording each {@code key_bytes} array handed out into {@code handedOut}. */
  private static ResultSet fakeRowResultSet(
      boolean present, byte[] keyBytes, Integer intValue, java.util.List<byte[]> handedOut) {
    int[] remaining = {present ? 1 : 0};

    InvocationHandler h =
        (proxy, method, args) -> {
          String name = method.getName();
          if ("next".equals(name)) {
            if (remaining[0] > 0) {
              remaining[0]--;
              return true;
            }
            return false;
          } else if ("getBytes".equals(name)) {
            String col = (String) args[0];
            if ("key_bytes".equals(col) && keyBytes != null) {
              // Real drivers return a fresh array per call; the engine zeroes it after use,
              // so handing out the stored reference would wipe the fake's key.
              byte[] copy = keyBytes.clone();
              handedOut.add(copy);
              return copy;
            }
            return null;
          } else if ("getInt".equals(name)) {
            return intValue == null ? 0 : intValue;
          } else if ("close".equals(name)) {
            return null;
          }
          return null;
        };

    return (ResultSet)
        Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(), new Class<?>[] {ResultSet.class}, h);
  }

  private static String sha256Hex(String input) {
    try {
      var md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      return java.util.HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
