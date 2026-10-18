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
}
