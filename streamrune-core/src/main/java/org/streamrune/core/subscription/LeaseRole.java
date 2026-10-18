package org.streamrune.core.subscription;

/** The role a subscription plays under its single-active-consumer lease, as health reports it. */
public enum LeaseRole {
  /**
   * The subscription does not report a role (a projection runner's view, or a custom registration).
   */
  NOT_REPORTED,
  /** The subscription takes no lease: it delivers on every replica that runs it. */
  UNGATED,
  /** This process holds the subscription's lease and delivers. */
  LEADING,
  /**
   * Another process holds the lease, or none does; this one reads nothing and waits to take over.
   */
  STANDBY
}
