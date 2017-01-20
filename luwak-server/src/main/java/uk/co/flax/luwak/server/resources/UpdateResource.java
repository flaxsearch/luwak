package uk.co.flax.luwak.server.resources;

import uk.co.flax.luwak.Monitor;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Created by Tom.Ridd on 20/01/2017.
 */
@Path("/update")
public class UpdateResource {
  private final Monitor monitor;

  public UpdateResource(Monitor monitor) {
    this.monitor = monitor;
  }

  @POST
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public LuwakUpdateResult postLuwakUpdate(String inputDocumentJson) {

    return new LuwakUpdateResult(inputDocumentJson, true);
  }
}
