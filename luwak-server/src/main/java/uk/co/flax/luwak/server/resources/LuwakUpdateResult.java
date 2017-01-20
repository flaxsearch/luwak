package uk.co.flax.luwak.server.resources;

import uk.co.flax.luwak.MonitorQuery;

/**
 * Created by Tom.Ridd on 20/01/2017.
 */
public class LuwakUpdateResult {
  private boolean success;
  private MonitorQuery inputDocument;

  public LuwakUpdateResult(MonitorQuery inputDocument, boolean success) {
    this.success = success;
    this.inputDocument = inputDocument;
  }

  public boolean isSuccess() {
    return success;
  }

  public MonitorQuery getInputDocument() {
    return inputDocument;
  }
}
