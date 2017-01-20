package uk.co.flax.luwak.server;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/claim")
@Produces(MediaType.APPLICATION_JSON)
public class ClaimResource {

    @GET
    public String getClaim() {
        return "{\"claim\":\"hello\"}";
    }
}

