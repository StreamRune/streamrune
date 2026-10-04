package org.streamrune.runtime;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Test double: an {@link AtomicBatchProcessor} with a naming rule, the way {@code
 * JdbcProjectionRepository} accepts only table-safe names. Its registration-time {@link
 * #checkProjectionName} rejects any name containing a {@code '-'}. It records every name it was
 * asked about and counts its commits, so a test can prove a runner refused a name before processing
 * anything.
 */
final class NameRuleProcessor implements AtomicBatchProcessor {

  final List<ProjectionName> checked = new CopyOnWriteArrayList<>();
  final AtomicInteger commits = new AtomicInteger();

  @Override
  public void checkProjectionName(ProjectionName projectionName) {
    checked.add(projectionName);
    if (projectionName.value().contains("-")) {
      throw new IllegalArgumentException(
          "no read-model table for projection name '" + projectionName.value() + "'");
    }
  }

  @Override
  public void executeAtomically(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      OffsetStore offsetStore) {
    commits.incrementAndGet();
    projectionUpdater.update(null);
    offsetStore.saveOffset(projectionName, newOffset);
  }
}
