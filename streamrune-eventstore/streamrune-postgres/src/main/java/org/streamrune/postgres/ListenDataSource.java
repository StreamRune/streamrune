package org.streamrune.postgres;

import javax.sql.DataSource;

/**
 * The dedicated DataSource a LISTEN connection is held from, as {@link
 * PostgresNotificationSubscription#createListenDataSource(DataSource)} builds it: a pool of one
 * connection, for one listener.
 *
 * <p><b>Who closes it.</b> Whoever it is handed to owns it and closes it with itself:
 *
 * <ul>
 *   <li>{@link PostgresNotificationSubscription.Builder#listenDataSource(DataSource)}: the
 *       subscription closes it on {@code close()};
 *   <li>{@link PgNotificationListener.Builder#dataSource(DataSource)}, and so {@link
 *       HybridEventSubscription.Builder#dataSource(DataSource)}, whose listener it reaches: the
 *       listener closes it on {@code close()}, started or not, and does not wrap it in another
 *       pool.
 * </ul>
 *
 * <p>Hand one instance to one holder. One that is never handed to a holder is closed by the
 * application.
 *
 * <p>The type names no pool library, so the pool behind it can change without changing a signature.
 */
public interface ListenDataSource extends DataSource, AutoCloseable {

  /** Closes the pool and its connection. Idempotent. */
  @Override
  void close();
}
