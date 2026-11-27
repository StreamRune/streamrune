package org.streamrune.quarkus.it;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

/** Answers with the request context the StreamRune request filter built for the request. */
@Path("/caller")
public class CallerResource {

  @Inject StreamRuneRequestContextHolder holder;

  /**
   * The correlation id and the user id of the request context, one per line.
   *
   * @return the two values, {@code -} for an absent one
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String caller() {
    StreamRuneContext.RequestContext context = holder.context();
    if (context == null) {
      return "no request context";
    }
    return context.correlationId().value()
        + "\n"
        + (context.userId() == null ? "-" : context.userId().value());
  }
}
