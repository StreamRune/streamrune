package org.streamrune.runtime;

import java.util.Map;
import java.util.Objects;
import org.streamrune.core.Internal;
import org.streamrune.core.types.LogSanitizer;

/**
 * A deployment's saga parallelism settings, as an integration reads them from its configuration
 * properties and hands them to {@link SagaParallelismDefault}. Integration plumbing: an application
 * names a runner's parallelism on {@link SagaRunner.Builder#parallelism}.
 *
 * @param global the deployment's global parallelism ({@code streamrune.saga.parallelism}), 1 to 64
 * @param bySubscription the parallelism of named subscriptions ({@code
 *     streamrune.saga.subscriptions.<name>.parallelism}): subscription name to 1 to 64; copied,
 *     unmodifiable
 * @hidden
 */
@Internal
public record SagaParallelismSettings(int global, Map<String, Integer> bySubscription) {

  public SagaParallelismSettings {
    SagaRunner.requireParallelismInRange("streamrune.saga.parallelism", global);
    Objects.requireNonNull(bySubscription, "bySubscription");
    bySubscription = Map.copyOf(bySubscription);
    bySubscription.forEach(
        (name, value) ->
            SagaRunner.requireParallelismInRange(
                "streamrune.saga.subscriptions."
                    + LogSanitizer.sanitizeForLog(name)
                    + ".parallelism",
                value));
  }
}
