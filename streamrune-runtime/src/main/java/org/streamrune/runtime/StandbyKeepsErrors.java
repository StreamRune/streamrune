package org.streamrune.runtime;

/**
 * A health registration whose run of errors survives a standby. {@link
 * SubscriptionHealthContributor} counts a standby subscription's errors from zero — they are pages
 * it failed to deliver, and a standby delivers none, so the run ends with the lease — but keeps the
 * count of a registration that implements this. A projection runner's views do: their errors are
 * batches the runner dead-lettered or skipped, holes in the shared read model that standing by does
 * not close, so the entry stays {@code DEGRADED} until the runner processes a clean batch.
 */
interface StandbyKeepsErrors {}
