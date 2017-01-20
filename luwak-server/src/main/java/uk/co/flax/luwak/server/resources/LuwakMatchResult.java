package uk.co.flax.luwak.server.resources;

/**
 * Created by Tom.Ridd on 20/01/2017.
 */
public class LuwakMatchResult {
  String description = "Result list of a luwak match";
  String monitorQuery;

  public LuwakMatchResult(String query) {
    monitorQuery = query;
  }
  public String getDescription() {
    return description;
  }
  public String getMonitorQuery() {
    return monitorQuery;
  }
}
