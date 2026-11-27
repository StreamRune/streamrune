/**
 * Store internals shared by the PostgreSQL and SQLite backends: the logic both backends' stores
 * carried as identical copies, held once. Every type of this package is {@link
 * org.streamrune.core.Internal}: public only because the backend modules use it, not part of the
 * public API, and no public signature of a published type names it.
 *
 * <p>The helpers follow the same rules. They take the caller's {@link java.sql.Connection}, {@link
 * java.sql.PreparedStatement} or {@link java.sql.ResultSet} and never open, commit, roll back or
 * close a connection; they let every {@link java.sql.SQLException} propagate unwrapped; they never
 * decide a lock. They never know or branch on the backend: what differs between the backends stays
 * in the backend module, behind a seam such as {@code JdbcInstantCodec}. A helper that logs takes
 * the owning store's logger in a field named {@code log}, so the log category stays the store's;
 * none keeps static mutable state.
 */
package org.streamrune.runtime.jdbc;
