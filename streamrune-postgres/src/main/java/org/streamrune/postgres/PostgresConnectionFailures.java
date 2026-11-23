package org.streamrune.postgres;

import org.streamrune.core.ConnectionFailureClassifier;

/** What PostgreSQL reports, beyond the generic JDBC rules, when the server is unreachable. */
public final class PostgresConnectionFailures {
  private PostgresConnectionFailures() {}

  /**
   * 57P01 admin shutdown, 57P02 crash shutdown, 57P03 cannot connect now, 53300 too many
   * connections.
   */
  public static final ConnectionFailureClassifier CLASSIFIER =
      ConnectionFailureClassifier.genericAndSqlStates("57P01", "57P02", "57P03", "53300");
}
