package org.streamrune.spring;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;

/**
 * Releases the Server-Sent Events stream of a request whose handling Spring MVC reports as failed.
 *
 * <p>{@link SseController} hands Spring MVC an emitter that already holds the stream's opening
 * frame. Spring MVC writes that frame while it takes the emitter over, and only then attaches the
 * emitter's completion, timeout and error callbacks to the request. When the write fails — the
 * client reset its connection after sending the request — the failure is thrown out of the
 * hand-over: no callback of the emitter runs, and the request stays in asynchronous mode with
 * nothing left to end it. Spring MVC does pass the failure to its exception resolvers, on the
 * request's own thread, and that is where this resolver stands: it calls {@link
 * SseController#releaseStreamOf}, which unsubscribes the client and fails the emitter, the result
 * that ends the asynchronous request.
 *
 * <p>The resolver never resolves anything. It returns {@code null} for every exception, so Spring
 * MVC goes on to the resolvers after it exactly as if this one were absent:
 * {@code @ExceptionHandler} methods, {@code ResponseStatusException}, the defaults, the
 * application's own. For a request that opened no stream it reads one request attribute and
 * returns.
 *
 * <p><b>First of all resolvers.</b> The auto-configuration declares it as a {@link
 * HandlerExceptionResolver} bean. The dispatcher servlet consults the resolver beans of the context
 * in their order and stops at the first that resolves, so a resolver of the application that
 * resolves the failure ends the search. This one is {@link PriorityOrdered} with the highest
 * precedence: it is sorted ahead of Spring MVC's own list ({@code handlerExceptionResolver}) and
 * ahead of every resolver ordered through {@code Ordered} or {@code @Order}, whatever its value,
 * and has released the stream before any of them is asked. Because it resolves nothing, standing
 * first changes the outcome of no resolution. The bean is not an autowire candidate: an application
 * that injects a {@link HandlerExceptionResolver} keeps getting the one it got without it.
 */
final class SseHandoverFailureResolver implements HandlerExceptionResolver, PriorityOrdered {

  @Override
  public ModelAndView resolveException(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception failure) {
    SseController.releaseStreamOf(request, failure);
    return null;
  }

  @Override
  public int getOrder() {
    return Ordered.HIGHEST_PRECEDENCE;
  }
}
