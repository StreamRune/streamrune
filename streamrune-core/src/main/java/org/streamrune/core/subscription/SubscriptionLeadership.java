package org.streamrune.core.subscription;

import java.util.Optional;

/**
 * Single-active-consumer coordination for named subscriptions/projections. An instance that holds a
 * live <em>lease</em> for a consumer name is the one runner permitted to advance that name's
 * offset; non-leaders run as standbys and take over when the leader's lease expires or is resigned.
 *
 * <p>A {@link Lease} carries a monotonic {@code epoch} — a <em>fencing token</em>. Each successful
 * acquisition (across all replicas) yields a strictly greater epoch than any prior holder's, so a
 * stale leader that resumes after a pause can be detected and rejected downstream: the leader
 * passes its epoch into every write path (e.g. {@link
 * org.streamrune.core.projection.AtomicBatchProcessor#executeAtomically}), and a transactional
 * store rejects any commit whose epoch is below the epoch it has already stamped. This prevents a
 * partitioned old leader from clobbering a newer one's progress.
 *
 * <p>The default {@link #NOOP} implementation is always leader at epoch {@code 0} (unfenced) —
 * correct for single-instance deployments and in-memory tests, where there is no second replica to
 * fence against. The PostgreSQL implementation coordinates across replicas and issues real
 * monotonic epochs.
 *
 * <p>Implementations are thread-safe: runners on different threads call these methods for different
 * (or the same) consumer names concurrently. {@link #close()} resigns all held names.
 */
public interface SubscriptionLeadership extends AutoCloseable {

  /**
   * A held lease and its monotonic fencing epoch.
   *
   * <p>The {@code epoch} is strictly increasing across successive acquisitions of the same consumer
   * name (globally, not per-instance). Callers forward it as a fencing token so transactional
   * stores can reject writes from a superseded leader. Epoch {@code 0} denotes the unfenced {@link
   * #NOOP} lease.
   */
  record Lease(long epoch) {}

  /**
   * Attempts to acquire (or renew) the lease for {@code consumerName}. Non-blocking.
   *
   * <p><b>Transient failures must not throw.</b> Runners call this bare on their tick paths (every
   * poll/cron fire) and treat a thrown exception as fatal — a projection runner whose acquire tick
   * throws terminates instead of standing by. An implementation whose coordination backend is
   * transiently unreachable (a database blip, a wrapped/routing DataSource throwing an unchecked
   * exception) must therefore degrade the failure to an empty return — a skipped tick — rather than
   * propagating it. Only a genuinely unrecoverable {@link Error} should escape.
   *
   * @return the held lease (with its fencing epoch), or empty if another replica currently holds a
   *     live lease for this name (or this attempt transiently failed and was skipped)
   */
  Optional<Lease> tryAcquire(String consumerName);

  /**
   * Returns the lease this instance currently holds for {@code consumerName}, or empty if it never
   * acquired one, resigned it, or was fenced out. Does not attempt acquisition.
   */
  Optional<Lease> current(String consumerName);

  /**
   * Releases the lease for {@code consumerName} if held (idempotent).
   *
   * <p><b>Must not throw on transient failures.</b> Runners call this from their shutdown {@code
   * finally} blocks and {@code close()} paths — often at exactly the moment the coordination
   * backend is unhealthy. An implementation that cannot reach its backend must swallow and log the
   * failure (the lease then expires naturally at its TTL) rather than propagate it; only a
   * genuinely unrecoverable {@link Error} should escape.
   */
  void resign(String consumerName);

  /**
   * Resigns all held names and releases any resources. Redeclared to narrow {@link AutoCloseable}'s
   * {@code throws Exception} to none so lifecycle adapters can call it without a checked-exception
   * wrapper.
   */
  @Override
  void close();

  /**
   * Always-leader, epoch-{@code 0} (unfenced) no-op. Single-instance deployments and tests: every
   * {@code consumerName} is granted the same epoch-0 lease, and {@code resign}/{@code close} do
   * nothing. Because the epoch never advances, downstream fencing is a no-op.
   */
  SubscriptionLeadership NOOP =
      new SubscriptionLeadership() {
        private static final Optional<Lease> EPOCH_ZERO = Optional.of(new Lease(0L));

        @Override
        public Optional<Lease> tryAcquire(String consumerName) {
          return EPOCH_ZERO;
        }

        @Override
        public Optional<Lease> current(String consumerName) {
          return EPOCH_ZERO;
        }

        @Override
        public void resign(String consumerName) {}

        @Override
        public void close() {}
      };
}
