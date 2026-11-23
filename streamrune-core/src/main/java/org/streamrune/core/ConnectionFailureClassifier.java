package org.streamrune.core;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * Tells whether a failure says the database, or the connection to it, is gone — as opposed to a
 * failure of one statement (a statement timeout, a lock timeout, a serialization failure, a
 * constraint violation), which says nothing about the backend.
 *
 * <p>It lives in core because the store modules contribute to it and depend on core only, and
 * because the SPI that carries it, {@link org.streamrune.core.saga.SagaStore}, is in core. Every
 * built-in classifier walks the cause chain, to a depth of {@value #MAX_CAUSE_DEPTH}, so that a
 * wrapped failure is found and a cause cycle ends the walk.
 */
@FunctionalInterface
public interface ConnectionFailureClassifier {

  /**
   * How many links of the cause chain the built-in classifiers inspect; the failure is the first.
   */
  int MAX_CAUSE_DEPTH = 50;

  /**
   * Whether the failure says the database, or the connection to it, is gone.
   *
   * <p>The failure is passed as it was thrown, usually a wrapper around the driver's exception: an
   * implementation looks through the cause chain itself, as the built-in classifiers do. Suppressed
   * exceptions and {@code SQLException.getNextException()} are not part of that chain, and the
   * built-in classifiers do not inspect them.
   *
   * @param failure the failure to classify; may be {@code null}, which is not a connection failure
   * @return {@code true} when the failure is connection-class
   */
  boolean isConnectionFailure(Throwable failure);

  /**
   * An SQLState of class {@code 08}, an {@link SQLTransientConnectionException} or an {@link
   * SQLNonTransientConnectionException}, anywhere in the cause chain.
   */
  ConnectionFailureClassifier GENERIC = genericAndSqlStates();

  /**
   * {@link #GENERIC}, or an {@link SQLException} in the cause chain whose SQLState is one of {@code
   * sqlStates}.
   *
   * @param sqlStates the extra SQLStates a backend reports when it is unreachable
   * @return the classifier
   */
  static ConnectionFailureClassifier genericAndSqlStates(String... sqlStates) {
    Set<String> extra = Set.copyOf(Arrays.asList(sqlStates));
    return failure -> {
      Throwable c = failure;
      for (int depth = 0; c != null && depth < MAX_CAUSE_DEPTH; depth++, c = c.getCause()) {
        if (c instanceof SQLTransientConnectionException
            || c instanceof SQLNonTransientConnectionException) {
          return true;
        }
        if (c instanceof SQLException sql) {
          String state = sql.getSQLState();
          if (state != null && (state.startsWith("08") || extra.contains(state))) {
            return true;
          }
        }
      }
      return false;
    };
  }

  /**
   * This classifier or the other.
   *
   * @param other the classifier to add
   * @return a classifier that is {@code true} when either is
   */
  default ConnectionFailureClassifier or(ConnectionFailureClassifier other) {
    Objects.requireNonNull(other, "other is required");
    return failure -> isConnectionFailure(failure) || other.isConnectionFailure(failure);
  }
}
