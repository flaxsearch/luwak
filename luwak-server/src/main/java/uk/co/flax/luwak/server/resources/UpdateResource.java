package uk.co.flax.luwak.server.resources;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;

import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.QueryError;
import uk.co.flax.luwak.UpdateException;

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
    public String postLuwakUpdate(MonitorQuery monitorQuery) throws IOException, UpdateException {
        monitor.update(monitorQuery);
        return "OK";
    }

    @POST
    @Path("/multi")
    public String addMultiple(List<MonitorQuery> queries) throws IOException {
        try {
            monitor.update(queries);
        } catch (UpdateException e) {
            StringBuilder sb = new StringBuilder();
            for (QueryError error : e.errors) {
                sb.append(error.toString()).append("\n");
            }
            throw new WebApplicationException(sb.toString());
        }
        return "OK";
    }
}
