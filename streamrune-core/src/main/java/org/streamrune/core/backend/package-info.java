/**
 * The backend seam: one storage technology StreamRune runs on, and the bundle of stores it opens
 * over a {@link javax.sql.DataSource}.
 *
 * <p>A {@link org.streamrune.core.backend.StreamRuneBackend} names the technology ({@code
 * "postgres"}, {@code "sqlite"}), checks that a {@code DataSource} is its database, and opens a
 * {@link org.streamrune.core.backend.BackendStores} from a {@link
 * org.streamrune.core.backend.BackendContext}. The bundle has one explicit accessor per storage
 * port of core: event store factory, checkpoints, the default read-model store, saga store and saga
 * dead letters, outbox, command inbox, dead-letter queue, projection dead letters, the audit stores
 * and queries, the snapshot purger and subscription leadership. {@link
 * org.streamrune.core.backend.EventStoreOptions} and {@link
 * org.streamrune.core.backend.OutboxOptions} carry what the two configurable stores need.
 *
 * <p>Every type of this package references core and JDK types only, so a plain-Java application
 * opens a backend without any integration module. An option value a backend cannot honour is
 * reported as a {@link org.streamrune.core.backend.BackendOptionException} naming the option.
 */
package org.streamrune.core.backend;
