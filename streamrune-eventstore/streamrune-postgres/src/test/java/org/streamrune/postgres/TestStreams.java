package org.streamrune.postgres;

import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

/** Typed streams for the module's tests: one aggregate type, ids built through the lenient door. */
final class TestStreams {

  /** The aggregate type of every stream {@link #stream(String)} builds. */
  static final AggregateType TYPE = AggregateType.of("test");

  private TestStreams() {}

  /** The stream {@code test:<id>}; the id goes through the lenient constructor, as stored. */
  static StreamId stream(String id) {
    return StreamId.of(TYPE, new AggregateId(id));
  }
}
