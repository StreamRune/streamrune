package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CorrelationId;

/** Tests for {@link StreamRuneContextHelper}. */
class StreamRuneContextHelperTest {

  @AfterEach
  void cleanup() {
    StreamRuneContextHelper.remove();
  }

  @Test
  void getReturnsNullWhenUnset() {
    assertNull(StreamRuneContextHelper.get());
  }

  @Test
  void setAndGetRoundTrip() {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-1"), Instant.now(), Map.of());
    StreamRuneContextHelper.set(ctx);
    assertSame(ctx, StreamRuneContextHelper.get());
  }

  @Test
  void removeClearsContext() {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-1"), Instant.now(), Map.of());
    StreamRuneContextHelper.set(ctx);
    StreamRuneContextHelper.remove();
    assertNull(StreamRuneContextHelper.get());
  }
}
