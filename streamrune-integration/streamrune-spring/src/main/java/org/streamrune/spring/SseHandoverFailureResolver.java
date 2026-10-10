package org.streamrune.spring;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

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
 * {@code @ExceptionHandler} methods, {@code ResponseStatusException}, the defaults. For a request
 * that opened no stream it reads one request attribute and returns.
 *
 * <p>The auto-configuration installs it as the first resolver of Spring MVC's own list, through
 * {@link WebMvcConfigurer#extendHandlerExceptionResolvers}. It is deliberately not a bean of type
 * {@link HandlerExceptionResolver}: the dispatcher servlet replaces its default resolvers by the
 * resolver beans it finds, and this one must only ever be added to them.
 */
final class SseHandoverFailureResolver implements HandlerExceptionResolver {

  @Override
  public ModelAndView resolveException(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception failure) {
    SseController.releaseStreamOf(request, failure);
    return null;
  }

  /**
   * The Spring MVC configurer that puts a {@link SseHandoverFailureResolver} ahead of the resolvers
   * Spring MVC has configured.
   *
   * @return the configurer
   */
  static WebMvcConfigurer asFirstResolver() {
    return new WebMvcConfigurer() {
      @Override
      public void extendHandlerExceptionResolvers(List<HandlerExceptionResolver> resolvers) {
        resolvers.addFirst(new SseHandoverFailureResolver());
      }
    };
  }
}
