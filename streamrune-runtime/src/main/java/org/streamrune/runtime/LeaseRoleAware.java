package org.streamrune.runtime;

import org.streamrune.core.subscription.LeaseRole;

/**
 * A subscription that delivers under a lease and can say which role it plays in this process.
 * {@link SubscriptionHealthContributor} reads it the way it reads {@link PushHealthAware}; a
 * registration that does not implement it reports {@link LeaseRole#NOT_REPORTED}.
 */
public interface LeaseRoleAware {

  /** {@link LeaseRole#UNGATED}, {@link LeaseRole#LEADING} or {@link LeaseRole#STANDBY}. */
  LeaseRole leaseRole();

  /**
   * Whether the run of errors this registration reports ends when it stands by: the health
   * contributor then counts it from zero while the role is {@link LeaseRole#STANDBY}. {@code true}
   * by default, for a subscription whose errors are the pages it failed to deliver: a standby
   * delivers none, so no page can end the run, and the run ends with the lease. A projection
   * runner's view answers {@code false}: its errors are batches it dead-lettered or skipped, holes
   * in the shared read model that standing by does not close.
   */
  default boolean errorsEndWithTheLease() {
    return true;
  }
}
