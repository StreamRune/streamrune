package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.streamrune.core.LockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

class LockAcquisitionExceptionTest {

  private static final StreamId CART_1 =
      StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-1"));

  @Test
  void isASubtypeOfTheCanonicalLockException() {
    var ex = new LockAcquisitionException(CART_1, "timeout");
    assertInstanceOf(LockException.class, ex);
  }

  @Test
  void constructsWithStreamIdAndMessage() {
    var ex = new LockAcquisitionException(CART_1, "timeout");
    assertEquals(CART_1, ex.streamId());
    assertEquals(AggregateId.of("cart-1"), ex.streamId().aggregateId());
    assertEquals("timeout", ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  void constructsWithCause() {
    var cause = new InterruptedException();
    var ex = new LockAcquisitionException(CART_1, "timeout", cause);
    assertSame(cause, ex.getCause());
    assertEquals(CART_1, ex.streamId());
    assertEquals("timeout", ex.getMessage());
  }

  @Test
  void toStringNamesTheTypedStream() {
    var ex = new LockAcquisitionException(CART_1, "timeout");
    assertEquals("LockAcquisitionException{streamId=cart:cart-1, message=timeout}", ex.toString());
  }

  @Test
  void twoTypesSharingAnIdValueAreDifferentStreams() {
    var inventory = StreamId.of(AggregateType.of("inventory"), AggregateId.of("cart-1"));
    var ex = new LockAcquisitionException(inventory, "timeout");
    assertNotEquals(CART_1, ex.streamId());
    assertTrue(ex.toString().contains("streamId=inventory:cart-1"), ex.toString());
  }
}
