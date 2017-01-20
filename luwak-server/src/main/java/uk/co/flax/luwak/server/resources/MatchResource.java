package uk.co.flax.luwak.server.resources;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Created by Tom.Ridd on 20/01/2017.
 */
@Path("/match")
public class MatchResource {

  @GET
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public LuwakMatchResult getLuwakMatches(@HeaderParam("query") String monitorQueryJson) {
    return new LuwakMatchResult(monitorQueryJson);
  }
}
