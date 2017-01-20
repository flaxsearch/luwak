package uk.co.flax.luwak.server.resources;

import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.QueryMatch;
import uk.co.flax.luwak.matchers.SimpleMatcher;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by Tom.Ridd on 20/01/2017.
 */
@Path("/match")
public class MatchResource {
  private final Monitor monitor;

  public MatchResource(Monitor monitor) {
    this.monitor = monitor;
  }

  @POST
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Matches<QueryMatch> getLuwakMatches(InputDocument inputDocument) throws IOException {
    return monitor.match(inputDocument, SimpleMatcher.FACTORY);
  }
}
