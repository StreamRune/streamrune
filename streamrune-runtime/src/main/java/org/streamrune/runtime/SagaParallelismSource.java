package org.streamrune.runtime;

import org.streamrune.core.Internal;

/**
 * Where a saga runner's parallelism comes from, highest precedence first. Integration plumbing,
 * read through {@link SagaParallelismDefault}.
 *
 * @hidden
 */
@Internal
public enum SagaParallelismSource {
  /** {@code streamrune.saga.subscriptions.<subscription-name>.parallelism}. */
  SUBSCRIPTION_PROPERTY,
  /** {@code SagaRunner.Builder.parallelism}. */
  BUILDER,
  /** {@code streamrune.saga.parallelism}. */
  GLOBAL_PROPERTY,
  /** The built-in default of a hand-wired runner. */
  DEFAULT
}
