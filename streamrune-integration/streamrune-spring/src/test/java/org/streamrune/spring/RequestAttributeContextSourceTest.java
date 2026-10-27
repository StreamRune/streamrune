package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * {@link RequestAttributeContextSource} answers the context {@link ScopedValueFilter} stored on the
 * request the calling thread serves, and {@code null} wherever no such request is current.
 */
class RequestAttributeContextSourceTest {

  private final RequestAttributeContextSource source = new RequestAttributeContextSource();

  private static final StreamRuneContext.RequestContext ALICE =
      new StreamRuneContext.RequestContext(
          null, UserId.of("alice"), CorrelationId.of("corr-1"), Instant.now(), null);

  @AfterEach
  void resetTheThread() {
    RequestContextHolder.resetRequestAttributes();
  }

  @Test
  void answersTheContextTheFilterStoredOnTheCurrentRequest() {
    var request = new MockHttpServletRequest();
    request.setAttribute(RequestAttributeContextSource.ATTRIBUTE, ALICE);
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));

    assertSame(ALICE, source.get());
  }

  @Test
  void answersNullOnAThreadThatServesNoRequest() {
    assertNull(source.get());
  }

  @Test
  void answersNullForARequestTheFilterDidNotRunFor() {
    var request = new MockHttpServletRequest();
    request.setAttribute(RequestAttributeContextSource.ATTRIBUTE, "not a request context");
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));

    assertNull(source.get());

    RequestContextHolder.setRequestAttributes(
        new ServletRequestAttributes(new MockHttpServletRequest()));

    assertNull(source.get());
  }

  /**
   * Attributes a thread still holds after its request completed refuse a request-scope read: the
   * thread serves no request any more.
   */
  @Test
  void answersNullOnceTheRequestHasCompleted() {
    var request = new MockHttpServletRequest();
    request.setAttribute(RequestAttributeContextSource.ATTRIBUTE, ALICE);
    var attributes = new ServletRequestAttributes(request);
    RequestContextHolder.setRequestAttributes(attributes);
    attributes.requestCompleted();

    assertNull(source.get());
  }
}
