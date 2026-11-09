package org.streamrune.postgres;

import javax.sql.DataSource;

/**
 * The dedicated DataSource a {@link PostgresNotificationSubscription} holds its LISTEN connection
 * from, as {@link PostgresNotificationSubscription#createListenDataSource(DataSource)} builds it: a
 * pool of one connection that the subscription closes with itself.
 *
 * <p>The type names no pool library, so the pool behind it can change without changing a signature.
 */
public interface ListenDataSource extends DataSource, AutoCloseable {

  /** Closes the pool and its connection. Idempotent. */
  @Override
  void close();
}
