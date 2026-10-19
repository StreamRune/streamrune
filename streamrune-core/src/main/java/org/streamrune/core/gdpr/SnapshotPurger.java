package org.streamrune.core.gdpr;

/**
 * A {@link SubjectDataPurger} that deletes a data subject's aggregate snapshots.
 *
 * <p>A snapshot stores an aggregate's state as it was when the snapshot was taken. Every field of
 * that state not marked {@code @Encrypted} is stored in the clear — the name an aggregate copied
 * into its state, an age computed from a birth date — and deleting the subject's key does not touch
 * it. A forget that does not delete the snapshot leaves that data readable.
 *
 * <p>The forget service therefore requires one: {@code ForgetSubjectService.Builder.build()}
 * refuses a service whose purgers hold no {@code SnapshotPurger} unless the application states
 * {@code withoutSnapshotPurge()}, and refuses both together. The type is what the builder looks
 * for; a snapshot purger is otherwise an ordinary purger, run after the key is deleted, audited and
 * reported under its {@link #name()} like any other.
 *
 * <p>{@code PostgresSnapshotStorePurger} ({@code streamrune-postgres}) implements it for the {@code
 * snapshot_store} table of {@code PostgresEventStore}. An application whose event store keeps
 * snapshots elsewhere implements it for that store.
 */
public interface SnapshotPurger extends SubjectDataPurger {}
