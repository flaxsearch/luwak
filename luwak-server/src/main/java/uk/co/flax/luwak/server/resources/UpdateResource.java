package uk.co.flax.luwak.server.resources;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;

import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
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
    public LuwakUpdateResult postLuwakUpdate(MonitorQuery monitorQuery) throws IOException, UpdateException {
        monitor.update(monitorQuery);
        return new LuwakUpdateResult(monitorQuery, true);
    }

    @POST
    @Path("/multi")
    public LuwakUpdateResult addMultiple(List<MonitorQuery> queries) throws Exception {
        monitor.update(queries);
        return new LuwakUpdateResult(null, true);
    }
}
